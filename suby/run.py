import platform
from pathlib import Path
from shlex import split as shlex_split
from subprocess import PIPE, Popen
from threading import Thread
from time import sleep
from typing import Any, Callable, List, Optional, Tuple, Union

from cantok import AbstractToken, CancellationError, DefaultToken, TimeoutToken
from emptylog import EmptyLogger, LoggerProtocol

from suby.callbacks import stderr_with_flush, stdout_with_flush
from suby.errors import RunningCommandError, WrongCommandError
from suby.subprocess_result import SubprocessResult


def run(
    *arguments: Union[str, Path],
    catch_output: bool = False,
    catch_exceptions: bool = False,
    logger: LoggerProtocol = EmptyLogger(),
    stdout_callback: Callable[[str], Any] = stdout_with_flush,
    stderr_callback: Callable[[str], Any] = stderr_with_flush,
    timeout: Optional[Union[int, float]] = None,
    split: bool = True,
    token: AbstractToken = DefaultToken(),
) -> SubprocessResult:
    """
    About reading from strout and stderr: https://stackoverflow.com/a/28319191/14522393
    """
    if timeout is not None and isinstance(token, DefaultToken):
        token = TimeoutToken(timeout)
    elif timeout is not None:
        token += TimeoutToken(timeout)

    converted_arguments = convert_arguments(arguments, split)
    arguments_string_representation = ' '.join([argument if ' ' not in argument else f'"{argument}"' for argument in converted_arguments])

    stdout_buffer: List[str] = []
    stderr_buffer: List[str] = []
    result = SubprocessResult()

    logger.info(f'The beginning of the execution of the command "{arguments_string_representation}".')

    try:
        with Popen(list(converted_arguments), stdout=PIPE, stderr=PIPE, bufsize=1, universal_newlines=True) as process:
            stderr_reading_thread = run_stderr_thread(process, stderr_buffer, result, catch_output, stderr_callback)
            if not isinstance(token, DefaultToken):
                killing_thread = run_killing_thread(process, token, result)

            for line in process.stdout:  # type: ignore[union-attr]
                stdout_buffer.append(line)
                if not catch_output:
                    stdout_callback(line)

            stderr_reading_thread.join()
            if not isinstance(token, DefaultToken):
                killing_thread.join()

    except FileNotFoundError as e:  # pragma: no cover
        if not catch_exceptions:
            message = f'Error when executing the command "{arguments_string_representation}".'
            logger.exception(message)
            raise RunningCommandError(message, result) from e
        return result


    fill_result(stdout_buffer, stderr_buffer, process.returncode, result)

    if process.returncode != 0:
        if not catch_exceptions:
            if result.killed_by_token:
                logger.error(f'The execution of the "{arguments_string_representation}" command was canceled using a cancellation token.')
                try:
                    token.check()
                except CancellationError as e:
                    e.result = result  # type: ignore[attr-defined]
                    raise e
            else:
                message = f'Error when executing the command "{arguments_string_representation}".'
                logger.error(message)
                raise RunningCommandError(message, result)
        elif result.killed_by_token:
            logger.error(f'The execution of the "{arguments_string_representation}" command was canceled using a cancellation token.')
        else:
            logger.error(f'Error when executing the command "{arguments_string_representation}".')

    else:
        logger.info(f'The command "{arguments_string_representation}" has been successfully executed.')

    return result


def convert_arguments(arguments: Tuple[Union[str, Path], ...], split: bool) -> List[str]:
    converted_arguments = []

    for argument in arguments:
        if isinstance(argument, Path):
            converted_arguments.append(str(argument))
        elif isinstance(argument, str):
            if split:
                try:
                    for sub_argument in split_argument(argument):
                        converted_arguments.append(sub_argument)
                except Exception as e:  # pragma: no cover
                    raise WrongCommandError(f'The expression "{argument}" cannot be parsed.') from e  # pragma: no cover
            else:
                converted_arguments.append(argument)
        else:
            raise TypeError(f'Only strings and pathlib.Path objects can be positional arguments when calling the suby function. You passed "{argument}" ({type(argument).__name__}).')

    return converted_arguments


def split_argument(argument: str) -> List[str]:
    # https://stackoverflow.com/a/35900070/14522393
    if platform.system() == 'Windows':
        return [argument]  # pragma: no cover
    return shlex_split(argument)


def run_killing_thread(process: Popen, token: AbstractToken, result: SubprocessResult) -> Thread:  # type: ignore[type-arg]
    thread = Thread(target=killing_loop, args=(process, token, result))
    thread.start()
    return thread


def run_stderr_thread(process: Popen, stderr_buffer: List[str], result: SubprocessResult, catch_output: bool, stderr_callback: Callable[[str], Any]) -> Thread:  # type: ignore[type-arg]
    thread = Thread(target=read_stderr, args=(process, stderr_buffer, result, catch_output, stderr_callback))
    thread.start()
    return thread


def killing_loop(process: Popen, token: AbstractToken, result: SubprocessResult) -> None:  # type: ignore[type-arg]
    while True:
        if not token:
            process.kill()
            result.killed_by_token = True
            break
        if process.poll() is not None:
            break
        sleep(0.0001)


def read_stderr(process: Popen, stderr_buffer: List[str], result: SubprocessResult, catch_output: bool, stderr_callback: Callable[[str], Any]) -> None:  # type: ignore[type-arg]
    for line in process.stderr:  # type: ignore[union-attr]
        stderr_buffer.append(line)
        if not catch_output:
            stderr_callback(line)


def fill_result(stdout_buffer: List[str], stderr_buffer: List[str], returncode: int, result: SubprocessResult) -> None:
    result.stdout = ''.join(stdout_buffer)
    result.stderr = ''.join(stderr_buffer)
    result.returncode = returncode
