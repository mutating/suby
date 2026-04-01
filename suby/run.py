from pathlib import Path
from platform import system
from shlex import split as shlex_split
from subprocess import PIPE, Popen
from threading import Thread
from time import sleep
from typing import Any, Callable, List, Optional, Tuple, Union

from cantok import AbstractToken, CancellationError, DefaultToken, TimeoutToken
from emptylog import EmptyLogger, LoggerProtocol

from suby.callbacks import stderr_with_flush, stdout_with_flush
from suby.errors import RunningCommandError, WrongCommandError
from suby.process_waiting import has_event_driven_wait, wait_for_process_exit
from suby.subprocess_result import SubprocessResult


def run(  # noqa: PLR0913, PLR0915
    *arguments: Union[str, Path],
    catch_output: bool = False,
    catch_exceptions: bool = False,
    logger: LoggerProtocol = EmptyLogger(),  # noqa: B008
    stdout_callback: Callable[[str], Any] = stdout_with_flush,
    stderr_callback: Callable[[str], Any] = stderr_with_flush,
    timeout: Optional[Union[int, float]] = None,
    split: bool = True,
    double_backslash: bool = system() == 'Windows',
    token: AbstractToken = DefaultToken(),  # noqa: B008
) -> SubprocessResult:
    """
    About reading from strout and stderr: https://stackoverflow.com/a/28319191/14522393
    """
    use_event_driven_timeout = timeout is not None and isinstance(token, DefaultToken) and has_event_driven_wait()

    if timeout is not None and isinstance(token, DefaultToken):
        token = TimeoutToken(timeout)
    elif timeout is not None:
        token += TimeoutToken(timeout)

    converted_arguments = convert_arguments(arguments, split, double_backslash)
    if not converted_arguments:
        raise WrongCommandError('You must pass at least one positional argument with the command to run.')
    arguments_string_representation = ' '.join([argument if ' ' not in argument else f'"{argument}"' for argument in converted_arguments])

    stdout_buffer: List[str] = []
    stderr_buffer: List[str] = []
    result = SubprocessResult()

    logger.info(f'The beginning of the execution of the command "{arguments_string_representation}".')

    try:
        with Popen(list(converted_arguments), stdout=PIPE, stderr=PIPE, bufsize=1, universal_newlines=True) as process:
            stderr_reading_thread = run_stderr_thread(process, stderr_buffer, catch_output, stderr_callback)
            if use_event_driven_timeout:
                timeout_thread = run_timeout_thread(process, timeout, result)  # type: ignore[arg-type]
            elif not isinstance(token, DefaultToken):
                killing_thread = run_killing_thread(process, token, result)

            for line in process.stdout:  # type: ignore[union-attr]
                stdout_buffer.append(line)
                if not catch_output:
                    stdout_callback(line)

            stderr_reading_thread.join()
            if use_event_driven_timeout:
                timeout_thread.join()
            elif not isinstance(token, DefaultToken):
                killing_thread.join()

    except OSError as e:  # pragma: no cover
        fill_startup_failure_result(result, e)
        if not catch_exceptions:
            message = f'Error when executing the command "{arguments_string_representation}".'
            logger.exception(message)
            raise RunningCommandError(message, result) from e
        logger.exception(f'Error when executing the command "{arguments_string_representation}".')
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
                    raise
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


def convert_arguments(arguments: Tuple[Union[str, Path], ...], split: bool, double_backslash: bool) -> List[str]:
    converted_arguments = []

    for argument in arguments:
        if isinstance(argument, Path):
            converted_arguments.append(str(argument))
        elif isinstance(argument, str):
            if split:
                try:
                    for sub_argument in split_argument(argument, double_backslash):
                        converted_arguments.append(sub_argument)
                except Exception as e:
                    raise WrongCommandError(f'The expression "{argument}" cannot be parsed.') from e
            else:
                converted_arguments.append(argument)
        else:
            raise TypeError(f'Only strings and pathlib.Path objects can be positional arguments when calling the suby function. You passed "{argument}" ({type(argument).__name__}).')

    return converted_arguments


def split_argument(argument: str, double_backslash: bool) -> List[str]:
    if double_backslash:
        argument = argument.replace('\\', '\\\\')
    return shlex_split(argument)


def run_timeout_thread(process: Popen, timeout: Union[int, float], result: SubprocessResult) -> Thread:  # type: ignore[type-arg]
    thread = Thread(target=timeout_wait, args=(process, timeout, result))
    thread.start()
    return thread


def run_killing_thread(process: Popen, token: AbstractToken, result: SubprocessResult) -> Thread:  # type: ignore[type-arg]
    thread = Thread(target=killing_loop, args=(process, token, result))
    thread.start()
    return thread


def run_stderr_thread(process: Popen, stderr_buffer: List[str], catch_output: bool, stderr_callback: Callable[[str], Any]) -> Thread:  # type: ignore[type-arg]
    thread = Thread(target=read_stderr, args=(process, stderr_buffer, catch_output, stderr_callback))
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


def timeout_wait(process: Popen, timeout: Union[int, float], result: SubprocessResult) -> None:  # type: ignore[type-arg]
    wait_for_process_exit(process, timeout)
    if process.poll() is None:
        try:
            process.kill()
        except OSError:
            pass
        else:
            result.killed_by_token = True


def read_stderr(process: Popen, stderr_buffer: List[str], catch_output: bool, stderr_callback: Callable[[str], Any]) -> None:  # type: ignore[type-arg]
    for line in process.stderr:  # type: ignore[union-attr]
        stderr_buffer.append(line)
        if not catch_output:
            stderr_callback(line)


def fill_result(stdout_buffer: List[str], stderr_buffer: List[str], returncode: int, result: SubprocessResult) -> None:
    result.stdout = ''.join(stdout_buffer)
    result.stderr = ''.join(stderr_buffer)
    result.returncode = returncode


def fill_startup_failure_result(result: SubprocessResult, error: OSError) -> None:
    result.stdout = ''
    result.stderr = str(error)
    result.returncode = 1
