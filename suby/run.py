from __future__ import annotations

import os
import stat
from collections.abc import Mapping as RuntimeMapping
from dataclasses import dataclass, field
from functools import partial
from inspect import (
    isasyncgenfunction,
    isclass,
    iscoroutinefunction,
    isgeneratorfunction,
)
from math import isfinite
from pathlib import Path
from platform import system
from shlex import split as shlex_split
from subprocess import PIPE, Popen
from threading import Event, Lock, Thread
from typing import (
    IO,
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Mapping,
    Optional,
    Tuple,
    Type,
    TypedDict,
    Union,
    cast,
)

from cantok import (
    AbstractToken,
    CancellationError,
    DefaultToken,
    TimeoutToken,
)
from cantok import ConditionCancellationError as CantokConditionCancellationError
from cantok import TimeoutCancellationError as CantokTimeoutCancellationError
from emptylog import EmptyLogger, LoggerProtocol
from sigmatch import PossibleCallMatcher, SignatureMismatchError

from suby.callbacks import stderr_with_flush, stdout_with_flush
from suby.errors import (
    ConditionCancellationError,
    EnvironmentVariablesConflict,
    RunningCommandError,
    TimeoutCancellationError,
    WrongCommandError,
    WrongDirectoryError,
)
from suby.process_waiting import has_event_driven_wait, wait_for_process_exit
from suby.subprocess_result import SubprocessResult

StreamCallback = Callable[[str], Any]  # type: ignore[misc, unused-ignore]
STREAM_CALLBACK_MATCHER = PossibleCallMatcher('.')
_CUSTOM_TOKEN_POLL_TIMEOUT_SECONDS = 0.0001
_CANCELLATION_ERROR_TYPES: Mapping[Type[CancellationError], Type[CancellationError]] = {
    CantokConditionCancellationError: ConditionCancellationError,
    CantokTimeoutCancellationError: TimeoutCancellationError,
}


class _TextPopenKwargs(TypedDict, total=False):
    stdout: int
    stderr: int
    bufsize: int
    text: Literal[True]
    encoding: str
    errors: str
    env: Mapping[str, str]
    cwd: str


class _FailureState:
    def __init__(self) -> None:
        self.error: Optional[Exception] = None
        self._lock = Lock()

    def set(self, error: Exception) -> bool:
        with self._lock:
            if self.error is None:
                self.error = error
                return True
            return False


@dataclass
class _ExecutionState:
    stdout_buffer: List[str] = field(default_factory=list)
    stderr_buffer: List[str] = field(default_factory=list)
    result: SubprocessResult = field(default_factory=SubprocessResult)
    failure_state: _FailureState = field(default_factory=_FailureState)
    process_exit_event: Event = field(default_factory=Event)
    wake_event: Event = field(default_factory=Event)


@dataclass
class _ReaderThreads:
    stdout: Thread
    stderr: Thread
    process_waiter: Thread


def run(  # noqa: PLR0913, PLR0915
    *arguments: Union[str, Path],
    catch_output: bool = False,
    catch_exceptions: bool = False,
    logger: LoggerProtocol = EmptyLogger(),  # noqa: B008
    stdout_callback: StreamCallback = stdout_with_flush,
    stderr_callback: StreamCallback = stderr_with_flush,
    timeout: Optional[Union[int, float]] = None,
    directory: Optional[Union[str, Path]] = None,
    split: bool = True,
    double_backslash: bool = system() == 'Windows',
    env: Optional[Mapping[str, str]] = None,
    add_env: Optional[Mapping[str, str]] = None,
    delete_env: Optional[Union[List[str], Tuple[str, ...]]] = None,
    token: AbstractToken = DefaultToken(),  # noqa: B008
) -> SubprocessResult:
    """
    About reading from strout and stderr: https://stackoverflow.com/a/28319191/14522393
    """
    validate_timeout(timeout)
    use_event_driven_timeout = timeout is not None and isinstance(token, DefaultToken) and has_event_driven_wait()

    if timeout is not None and isinstance(token, DefaultToken):
        token = TimeoutToken(timeout)
    elif timeout is not None:
        token += TimeoutToken(timeout)

    converted_arguments = convert_arguments(arguments, split, double_backslash)
    if not converted_arguments:
        raise WrongCommandError('You must pass at least one positional argument with the command to run.')
    arguments_string_representation = ' '.join([argument if ' ' not in argument else f'"{argument}"' for argument in converted_arguments])
    subprocess_env = build_subprocess_env(env, add_env, delete_env)
    subprocess_directory = prepare_directory(directory)
    popen_kwargs: _TextPopenKwargs = {
        'stdout': PIPE,
        'stderr': PIPE,
        'bufsize': 1,
        'text': True,
        'encoding': 'utf-8',
        'errors': 'strict',
    }
    if subprocess_env is not None:
        popen_kwargs['env'] = subprocess_env
    if subprocess_directory is not None:
        popen_kwargs['cwd'] = subprocess_directory

    check_output_stream_callback('stdout_callback', stdout_callback)
    check_output_stream_callback('stderr_callback', stderr_callback)

    state = _ExecutionState()

    logger.info(f'The beginning of the execution of the command "{arguments_string_representation}".')

    try:
        with Popen(list(converted_arguments), **popen_kwargs) as process:
            reader_threads = _ReaderThreads(
                stdout=run_stdout_thread(process, catch_output, stdout_callback, token, state),
                stderr=run_stderr_thread(process, catch_output, stderr_callback, token, state),
                process_waiter=run_process_waiter_thread(process, state),
            )
            if use_event_driven_timeout:
                timeout_thread = run_timeout_thread(process, timeout, state.result)  # type: ignore[arg-type]  # pragma: no cover (Windows or (Linux and <py39))

            while True:
                raise_failure_if_needed(process, reader_threads, state)
                if state.process_exit_event.is_set():
                    break
                state.wake_event.wait(get_manual_token_poll_timeout_seconds(use_event_driven_timeout, token))
                state.wake_event.clear()
                if should_poll_token_manually(use_event_driven_timeout, token):
                    try:
                        if not token:
                            kill_process_if_running(process)
                            state.result.killed_by_token = True
                    except Exception as error:  # noqa: BLE001
                        state.failure_state.set(error)
                        raise_failure_if_needed(process, reader_threads, state)

            join_reader_threads(reader_threads)
            raise_failure_if_needed(process, reader_threads, state)
            if use_event_driven_timeout:
                timeout_thread.join()  # pragma: no cover (Windows or (Linux and <py39))

    except FileNotFoundError as e:
        fill_startup_failure_result(state.result, e)
        message = format_startup_failure_message(arguments_string_representation, e)
        if not catch_exceptions:
            logger.exception(message)
            raise RunningCommandError(message, state.result) from e
        logger.exception(message)
        return state.result

    except PermissionError as e:
        fill_startup_failure_result(state.result, e)
        message = format_startup_failure_message(arguments_string_representation, e)
        if not catch_exceptions:
            logger.exception(message)
            raise RunningCommandError(message, state.result) from e
        logger.exception(message)
        return state.result

    except OSError as e:
        fill_startup_failure_result(state.result, e)
        message = format_startup_failure_message(arguments_string_representation, e)
        if not catch_exceptions:
            logger.exception(message)
            raise RunningCommandError(message, state.result) from e
        logger.exception(message)
        return state.result

    fill_result(state, process.returncode)

    if state.result.returncode != 0:
        if not catch_exceptions:
            if state.result.killed_by_token:
                logger.error(f'The execution of the "{arguments_string_representation}" command was canceled using a cancellation token.')
                try:
                    token.check()
                except CancellationError as e:
                    normalize_cancellation_error(e)
                    e.result = state.result  # type: ignore[attr-defined]
                    raise
            else:
                message = f'Error when executing the command "{arguments_string_representation}".'
                logger.error(message)
                raise RunningCommandError(message, state.result)
        elif state.result.killed_by_token:
            logger.error(f'The execution of the "{arguments_string_representation}" command was canceled using a cancellation token.')
        else:
            logger.error(f'Error when executing the command "{arguments_string_representation}".')

    else:
        logger.info(f'The command "{arguments_string_representation}" has been successfully executed.')

    return state.result


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


def check_output_stream_callback(parameter_name: str, callback: Any) -> None:  # type: ignore[misc]
    def build_message(reason: str) -> str:
        return (
            f'{parameter_name} must be a synchronous, non-generator callable that can be invoked as callback(line), '  # type: ignore[misc, unused-ignore]
            f'where line is a str output line; got {callback!r} ({type(callback).__name__}). {reason}'  # type: ignore[misc]
        )

    if not callable(callback):  # type: ignore[misc]
        raise SignatureMismatchError(build_message('The value is not callable.'))

    inspected_callback = callback  # type: ignore[misc]
    partial_type: type[object] = type(partial(len))
    while isinstance(inspected_callback, partial_type):  # type: ignore[misc]
        inspected_callback = object.__getattribute__(inspected_callback, 'func')  # type: ignore[misc]

    if isclass(inspected_callback):  # type: ignore[misc]
        raise SignatureMismatchError(build_message('callback classes are not supported; pass a function or a callable instance instead.'))

    try:
        inspected_call = object.__getattribute__(inspected_callback, '__call__')  # type: ignore[misc]
    except AttributeError:
        inspected_call = None
    for inspected in (inspected_callback, inspected_call):  # type: ignore[misc]
        if inspected is None:  # type: ignore[misc]
            continue
        if iscoroutinefunction(inspected):  # type: ignore[misc]
            raise SignatureMismatchError(build_message('async callbacks are not supported because suby invokes stream callbacks synchronously.'))
        if isasyncgenfunction(inspected):  # type: ignore[misc]
            raise SignatureMismatchError(build_message('async generator callbacks are not supported because suby invokes stream callbacks synchronously and ignores callback return values.'))
        if isgeneratorfunction(inspected):  # type: ignore[misc]
            raise SignatureMismatchError(build_message('generator callbacks are not supported because suby invokes stream callbacks synchronously and ignores callback return values.'))

    if not STREAM_CALLBACK_MATCHER.match(callback, raise_exception=False):  # type: ignore[misc]
        raise SignatureMismatchError(build_message('The callable signature does not accept one positional output line.'))


def prepare_directory(directory: Optional[Union[str, Path]]) -> Optional[str]:
    if directory is None:
        return None

    if isinstance(directory, str):
        raw_text = directory
    elif isinstance(directory, Path):
        raw_text = str(directory)
    else:
        raise TypeError(f'directory must be a string or pathlib.Path object, got {directory!r} ({type(directory).__name__}).')

    if raw_text == '':
        raise WrongDirectoryError('directory must not be an empty string; use "." for the current directory.')
    if '\x00' in raw_text:
        raise WrongDirectoryError(f'directory must not contain null bytes, got {raw_text!r}.')

    path = Path(raw_text)
    if path.is_absolute():
        cwd_path = path
    else:
        try:
            current_directory = Path.cwd()
        except OSError as error:
            raise WrongDirectoryError(f'Could not determine the current working directory while resolving directory {raw_text!r}.') from error
        cwd_path = current_directory / path

    try:
        directory_stat = cwd_path.stat()
    except FileNotFoundError as error:
        if has_file_parent(cwd_path):
            raise WrongDirectoryError(f'The directory {raw_text!r} cannot be resolved because an intermediate component is not a directory.') from error  # pragma: no cover (!Windows)
        raise WrongDirectoryError(f'The directory {raw_text!r} does not exist.') from error
    except NotADirectoryError as error:
        raise WrongDirectoryError(f'The directory {raw_text!r} cannot be resolved because an intermediate component is not a directory.') from error  # pragma: no cover (Windows)
    except PermissionError as error:
        raise WrongDirectoryError(f'Permission denied when accessing directory {raw_text!r}.') from error
    except OSError as error:
        raise WrongDirectoryError(f'Could not access directory {raw_text!r}: {error}.') from error

    if not stat.S_ISDIR(directory_stat.st_mode):
        raise WrongDirectoryError(f'The path {raw_text!r} exists but is not a directory.')
    if os.name != 'nt' and not os.access(cwd_path, os.X_OK):
        raise WrongDirectoryError(f'Permission denied when accessing directory {raw_text!r}.')  # pragma: no cover (Windows)

    return str(cwd_path)


def has_file_parent(path: Path) -> bool:
    for parent in path.parents:
        try:
            parent_stat = parent.stat()
        except OSError:
            continue
        return not stat.S_ISDIR(parent_stat.st_mode)
    return False


def build_subprocess_env(
    env: Optional[Mapping[str, str]],
    add_env: Optional[Mapping[str, str]],
    delete_env: Optional[Union[List[str], Tuple[str, ...]]],
) -> Optional[Dict[str, str]]:
    validate_environment_mapping('env', env)
    validate_environment_mapping('add_env', add_env)
    validate_delete_env(delete_env)

    if env is None and is_empty_collection(add_env) and is_empty_collection(delete_env):
        return None

    use_case_insensitive_names = system() == 'Windows'
    raise_environment_variables_conflict_if_needed(env, add_env, delete_env, use_case_insensitive_names)

    prepared_env: Dict[str, str] = {}
    if env is None:
        apply_environment_mapping(prepared_env, os.environ, use_case_insensitive_names)
    else:
        apply_environment_mapping(prepared_env, env, use_case_insensitive_names)

    if add_env is not None:
        apply_environment_mapping(prepared_env, add_env, use_case_insensitive_names)

    if delete_env is not None:
        for name in delete_env:
            prepared_env.pop(normalize_environment_variable_name(name, use_case_insensitive_names), None)

    return prepared_env


def validate_environment_mapping(name: str, value: Optional[Mapping[str, str]]) -> None:
    if value is None:
        return
    if not isinstance(value, RuntimeMapping):
        raise TypeError(f'{name} must be a mapping of str to str, got {type(value).__name__}.')
    for key, item_value in value.items():
        if not isinstance(key, str):
            raise TypeError(f'{name} keys must be str, got {key!r} ({type(key).__name__}).')
        validate_environment_variable_name_content(f'{name} keys', key)
        if not isinstance(item_value, str):
            raise TypeError(f'{name} values must be str, got {item_value!r} ({type(item_value).__name__}) for key {key!r}.')
        validate_environment_variable_value_content(f'{name} values', item_value, key)


def validate_delete_env(delete_env: Optional[Union[List[str], Tuple[str, ...]]]) -> None:
    if delete_env is None:
        return
    if not isinstance(delete_env, (list, tuple)):
        raise TypeError(f'delete_env must be a list or tuple of str, got {type(delete_env).__name__}.')
    for item in delete_env:
        if not isinstance(item, str):
            raise TypeError(f'delete_env items must be str, got {item!r} ({type(item).__name__}).')
        validate_environment_variable_name_content('delete_env items', item)


def validate_environment_variable_name_content(description: str, name: str) -> None:
    if '=' in name or '\x00' in name:
        raise TypeError(f"{description} must not contain '=' or null bytes, got {name!r}.")


def validate_environment_variable_value_content(description: str, value: str, key: str) -> None:
    if '\x00' in value:
        raise TypeError(f'{description} must not contain null bytes, got {value!r} for key {key!r}.')


def is_empty_collection(value: Optional[Union[Mapping[str, str], List[str], Tuple[str, ...]]]) -> bool:
    return value is None or len(value) == 0


def apply_environment_mapping(
    target: Dict[str, str],
    source: Mapping[str, str],
    use_case_insensitive_names: bool,
) -> None:
    for key, value in source.items():
        target[normalize_environment_variable_name(key, use_case_insensitive_names)] = value


def raise_environment_variables_conflict_if_needed(
    env: Optional[Mapping[str, str]],
    add_env: Optional[Mapping[str, str]],
    delete_env: Optional[Union[List[str], Tuple[str, ...]]],
    use_case_insensitive_names: bool,
) -> None:
    if delete_env is None:
        return

    explicit_names = set()
    for mapping in (env, add_env):
        if mapping is None:
            continue
        for key in mapping:
            explicit_names.add(normalize_environment_variable_name(key, use_case_insensitive_names))

    conflict_names = []
    seen_conflict_names = set()
    for name in delete_env:
        normalized_name = normalize_environment_variable_name(name, use_case_insensitive_names)
        if normalized_name not in explicit_names or normalized_name in seen_conflict_names:
            continue
        seen_conflict_names.add(normalized_name)
        conflict_names.append(name)

    if conflict_names:
        raise EnvironmentVariablesConflict(
            f'Environment variables cannot be both set via env/add_env and deleted via delete_env: {", ".join(conflict_names)}.',
        )


def normalize_environment_variable_name(name: str, use_case_insensitive_names: bool) -> str:
    if use_case_insensitive_names:
        return name.upper()
    return name


def run_timeout_thread(process: Popen[str], timeout: Union[int, float], result: SubprocessResult) -> Thread:
    thread = Thread(target=timeout_wait, args=(process, timeout, result))
    thread.start()
    return thread


def run_stdout_thread(process: Popen[str], catch_output: bool, stdout_callback: StreamCallback, token: AbstractToken, state: _ExecutionState) -> Thread:
    thread = Thread(target=read_stream, args=(process, cast(IO[str], process.stdout), state.stdout_buffer, catch_output, stdout_callback, token, state))
    thread.start()
    return thread


def run_stderr_thread(process: Popen[str], catch_output: bool, stderr_callback: StreamCallback, token: AbstractToken, state: _ExecutionState) -> Thread:
    thread = Thread(target=read_stream, args=(process, cast(IO[str], process.stderr), state.stderr_buffer, catch_output, stderr_callback, token, state))
    thread.start()
    return thread


def run_process_waiter_thread(process: Popen[str], state: _ExecutionState) -> Thread:
    thread = Thread(target=wait_for_process_exit_and_signal, args=(process, state))
    thread.start()
    return thread


def timeout_wait(process: Popen[str], timeout: Union[int, float], result: SubprocessResult) -> None:
    wait_for_process_exit(process, timeout)
    if process.poll() is None:
        try:
            process.kill()
        except OSError:
            pass
        else:
            result.killed_by_token = True


def should_poll_token_manually(use_event_driven_timeout: bool, token: AbstractToken) -> bool:
    return not use_event_driven_timeout and not isinstance(token, DefaultToken)


def get_manual_token_poll_timeout_seconds(
    use_event_driven_timeout: bool,
    token: AbstractToken,
) -> Optional[float]:
    if should_poll_token_manually(use_event_driven_timeout, token):
        return _CUSTOM_TOKEN_POLL_TIMEOUT_SECONDS
    return None


def read_stream(  # noqa: PLR0913
    process: Popen[str],
    stream: IO[str],
    buffer: List[str],
    catch_output: bool,
    callback: StreamCallback,
    token: AbstractToken,
    state: _ExecutionState,
) -> None:
    while True:
        if state.failure_state.error is not None:
            return
        try:
            if not isinstance(token, DefaultToken) and not token:
                kill_process_if_running(process)
                state.result.killed_by_token = True
                return
            line = stream.readline()
            if not line:
                return
            if state.failure_state.error is not None:
                return
            buffer.append(line)
            if should_call_stream_callback(catch_output, callback):
                callback(line)
        except Exception as error:  # noqa: BLE001
            if state.failure_state.set(error):
                state.wake_event.set()
            return


def should_call_stream_callback(catch_output: bool, callback: StreamCallback) -> bool:
    if not catch_output:
        return True
    return callback not in (stdout_with_flush, stderr_with_flush)


def wait_for_process_exit_and_signal(process: Popen[str], state: _ExecutionState) -> None:
    wait_for_process_exit(process, None)
    if process.returncode is None:
        try:
            process.wait()
        except OSError:
            pass
    state.process_exit_event.set()
    state.wake_event.set()


def fill_result(state: _ExecutionState, returncode: Optional[int]) -> None:
    state.result.stdout = ''.join(state.stdout_buffer)
    state.result.stderr = ''.join(state.stderr_buffer)
    state.result.returncode = 1 if returncode is None else returncode
    if state.result.returncode == 0:
        state.result.killed_by_token = False


def fill_startup_failure_result(result: SubprocessResult, _error: OSError) -> None:
    result.stdout = ''
    result.stderr = ''
    result.returncode = 1


def format_startup_failure_message(arguments_string_representation: str, error: OSError) -> str:
    if isinstance(error, FileNotFoundError):
        return f'The executable for the command "{arguments_string_representation}" was not found.'
    if isinstance(error, PermissionError):
        return f'Permission denied when starting the command "{arguments_string_representation}".'
    return f'OS error when starting the command "{arguments_string_representation}".'


def validate_timeout(timeout: Optional[Union[int, float]]) -> None:
    if timeout is None:
        return
    if not isfinite(timeout):
        raise ValueError('You cannot specify NaN or infinite timeout values.')


def raise_failure_if_needed(process: Popen[str], reader_threads: _ReaderThreads, state: _ExecutionState) -> None:
    if state.failure_state.error is None:
        return
    raise_background_failure(process, reader_threads, state, state.failure_state.error)


def raise_background_failure(process: Popen[str], reader_threads: _ReaderThreads, state: _ExecutionState, error: Exception) -> None:
    kill_process_if_running(process)
    join_reader_threads(reader_threads)
    try:
        process.wait()
    except OSError:
        pass
    fill_result(state, process.returncode if process.returncode is not None else 1)
    attach_result_to_exception(error, state.result)
    raise error


def join_reader_threads(reader_threads: _ReaderThreads) -> None:
    reader_threads.stdout.join()
    reader_threads.stderr.join()
    reader_threads.process_waiter.join()


def kill_process_if_running(process: Popen[str]) -> None:
    if process.poll() is not None:
        return
    try:
        process.kill()
    except OSError:
        pass


def attach_result_to_exception(error: BaseException, result: SubprocessResult) -> None:
    try:
        error_dict = cast(Dict[str, object], object.__getattribute__(error, '__dict__'))
    except (AttributeError, TypeError):
        error_dict = {}

    if 'result' in error_dict:
        return

    if any('result' in cast(Mapping[str, object], cls.__dict__) for cls in type(error).__mro__):
        return

    try:
        error.result = result  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001
        pass


def normalize_cancellation_error(error: CancellationError) -> None:
    normalized_error_type = _CANCELLATION_ERROR_TYPES.get(type(error))

    if normalized_error_type is not None:
        error.__class__ = normalized_error_type
