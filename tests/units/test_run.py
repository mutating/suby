import importlib
import importlib.util
import json
import re
import subprocess
import sys
import time
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from os import environ
from pathlib import Path, PurePath
from threading import Event, Thread
from time import perf_counter
from typing import Any, List, cast
from unittest.mock import patch

import pytest
from cantok import (
    AbstractToken,
    CancellationError,
    ConditionCancellationError,
    ConditionToken,
    DefaultToken,
    SimpleToken,
    TimeoutCancellationError,
)
from emptylog import MemoryLogger
from full_match import match

import suby
from suby import RunningCommandError, WrongCommandError, run
from suby.subprocess_result import SubprocessResult

_run_module = importlib.import_module('suby.run')

_WINDOWS_MAXIMUM_COMMAND_LINE_LENGTH = 32766
_PROMPT_EXCEPTION_SECONDS = 120.0


def _assert_kill_returncode_matches_platform(returncode: int) -> None:
    if sys.platform == 'win32':
        assert returncode != 0
    else:
        assert returncode == -9


def _windows_print_payload_for_command_line_length(target_command_line_length: int) -> str:
    payload_length = target_command_line_length

    while payload_length >= 0:
        payload = 'x' * payload_length
        command_line = subprocess.list2cmdline([sys.executable, '-c', f'print("{payload}")'])
        if len(command_line) <= target_command_line_length:
            return payload
        payload_length -= len(command_line) - target_command_line_length

    raise AssertionError('Failed to build a Windows command line payload within the requested length.')


def _load_run_module_with_default_system(system_name: str):
    """Load a fresh suby.run module with platform.system() patched before run() defaults are bound."""
    module_path = Path(cast(str, _run_module.__file__))
    module_name = f'test_suby_run_default_system_{system_name.lower()}'
    spec = importlib.util.spec_from_file_location(module_name, module_path)

    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        with patch('platform.system', return_value=system_name):
            spec.loader.exec_module(module)
    finally:
        sys.modules.pop(module_name, None)

    return module


@pytest.mark.parametrize(
    ('command', 'run_kwargs', 'expected_stdout', 'expected_stderr'),
    [
        ((Path(sys.executable), '-c "print(\'kek\')"'), {}, 'kek\n', ''),
        ((sys.executable, '-c "print(\'kek\')"'), {}, 'kek\n', ''),
        (('python -c "print(\'kek\')"',), {}, 'kek\n', ''),
        ((sys.executable, '-c "print(\'kek\')"'), {'token': SimpleToken()}, 'kek\n', ''),
        (('python -c "print(\'kek\')"',), {'token': SimpleToken()}, 'kek\n', ''),
        ((sys.executable, '-c "import sys; sys.stderr.write(\'kek\')"'), {}, '', 'kek'),
        (('python -c "import sys; sys.stderr.write(\'kek\')"',), {}, '', 'kek'),
    ],
)
def test_normal_way(command, run_kwargs, expected_stdout, expected_stderr, assert_no_suby_thread_leaks):
    """A regular run() call captures stdout/stderr into the result object and returns exit code 0."""
    with assert_no_suby_thread_leaks():
        result = run(*command, **run_kwargs)

    assert result.stdout == expected_stdout
    assert result.stderr == expected_stderr
    assert result.returncode == 0


@pytest.mark.parametrize(
    'command',
    [
        (sys.executable, '-c "raise ValueError"'),
        ('python -c "raise ValueError"',),
    ],
)
def test_catch_exception(command, assert_no_suby_thread_leaks):
    """With catch_exceptions=True, subprocess failures are returned in stderr and returncode instead of raising."""
    with assert_no_suby_thread_leaks():
        result = run(*command, catch_exceptions=True)

    assert 'ValueError' in result.stderr
    assert result.returncode != 0


@pytest.mark.parametrize(
    'command',
    [
        (sys.executable, '-c "import time; time.sleep({sleep_time})"'),
        ('python -c "import time; time.sleep({sleep_time})"',),
    ],
)
@pytest.mark.parametrize(
    ('catch_exceptions', 'expected_exception'),
    [
        (True, None),
        (False, TimeoutCancellationError),
    ],
)
def test_timeout_returns_or_raises_killed_result_according_to_catch_exceptions(
    command,
    catch_exceptions,
    expected_exception,
    assert_no_suby_thread_leaks,
):
    """A timeout kills the process in both modes; catch_exceptions controls whether the result is returned or attached to TimeoutCancellationError."""
    sleep_time = 100000
    timeout = 0.001
    command = [subcommand.format(sleep_time=sleep_time) if isinstance(subcommand, str) else subcommand for subcommand in command]

    start_time = perf_counter()
    with assert_no_suby_thread_leaks():
        if expected_exception is None:
            result = run(*command, timeout=timeout, catch_exceptions=catch_exceptions)
        else:
            with pytest.raises(expected_exception) as exc_info:
                run(*command, timeout=timeout, catch_exceptions=catch_exceptions)
            result = exc_info.value.result
    end_time = perf_counter()

    assert result.returncode != 0
    assert result.stdout == ''
    assert result.stderr == ''

    assert (end_time - start_time) < sleep_time
    assert (end_time - start_time) >= timeout


@pytest.mark.parametrize(
    ('command', 'error_text'),
    [
        ((sys.executable, '-c "raise ValueError"'), f'Error when executing the command "{sys.executable} -c "raise ValueError"".'),
        (('python -c "raise ValueError"',), 'Error when executing the command "python -c "raise ValueError"".'),
    ],
)
def test_exception_in_subprocess_without_catching(command, error_text, assert_no_suby_thread_leaks):
    """With catch_exceptions=False, a non-zero subprocess exit raises RunningCommandError with the captured result."""
    with assert_no_suby_thread_leaks(), pytest.raises(RunningCommandError, match=re.escape(error_text)) as exc_info:
        run(*command)

    assert exc_info.value.result.stdout == ''
    assert 'ValueError' in exc_info.value.result.stderr
    assert exc_info.value.result.returncode != 0


@pytest.mark.parametrize(
    'command',
    [
        (sys.executable, '-c "print(\'kek1\', end=\'\'); import sys; sys.stderr.write(\'kek2\')"'),
        ('python -c "print(\'kek1\', end=\'\'); import sys; sys.stderr.write(\'kek2\')"',),
    ],
)
@pytest.mark.parametrize(
    ('catch_output', 'expected_console_stdout', 'expected_console_stderr'),
    [
        (False, 'kek1', 'kek2'),
        (True, '', ''),
    ],
)
def test_catch_output_controls_console_forwarding(
    command,
    catch_output,
    expected_console_stdout,
    expected_console_stderr,
    assert_no_suby_thread_leaks,
):
    """catch_output=False forwards child output to console streams, while catch_output=True suppresses console forwarding."""
    stderr_buffer = StringIO()
    stdout_buffer = StringIO()

    with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer), assert_no_suby_thread_leaks():
        result = run(*command, catch_output=catch_output)

        stderr = stderr_buffer.getvalue()
        stdout = stdout_buffer.getvalue()

        assert result.returncode == 0
        assert stderr == expected_console_stderr
        assert stdout == expected_console_stdout


@pytest.mark.parametrize(
    ('command', 'first_log_message', 'second_log_message'),
    [
        ((sys.executable, '-c "print(\'kek\', end=\'\')"'), f'The beginning of the execution of the command "{sys.executable} -c "print(\'kek\', end=\'\')"".', f'The command "{sys.executable} -c "print(\'kek\', end=\'\')"" has been successfully executed.'),
        (('python -c "print(\'kek\', end=\'\')"',), 'The beginning of the execution of the command "python -c "print(\'kek\', end=\'\')"".', 'The command "python -c "print(\'kek\', end=\'\')"" has been successfully executed.'),
    ],
)
def test_logging_normal_way(command, first_log_message, second_log_message):
    """Successful execution logs one INFO record at start and one INFO record on completion."""
    logger = MemoryLogger()

    run(*command, logger=logger, catch_output=True)

    assert len(logger.data.info) == 2
    assert len(logger.data.error) == 0
    assert len(logger.data) == 2

    assert logger.data.info[0].message == first_log_message
    assert logger.data.info[1].message == second_log_message


@pytest.mark.parametrize(
    ('command', 'run_kwargs', 'expected_exception', 'first_log_message', 'second_log_message'),
    [
        (
            (sys.executable, f'-c "import time; time.sleep({500_000})"'),
            {'catch_exceptions': True, 'catch_output': True, 'timeout': 0.0001},
            None,
            f'The beginning of the execution of the command "{sys.executable} -c "import time; time.sleep(500000)"".',
            f'The execution of the "{sys.executable} -c "import time; time.sleep(500000)"" command was canceled using a cancellation token.',
        ),
        (
            (f'python -c "import time; time.sleep({500_000})"',),
            {'catch_exceptions': True, 'catch_output': True, 'timeout': 0.0001},
            None,
            'The beginning of the execution of the command "python -c "import time; time.sleep(500000)"".',
            'The execution of the "python -c "import time; time.sleep(500000)"" command was canceled using a cancellation token.',
        ),
        (
            (sys.executable, f'-c "import time; time.sleep({500_000})"'),
            {'catch_output': True, 'timeout': 0.0001},
            TimeoutCancellationError,
            f'The beginning of the execution of the command "{sys.executable} -c "import time; time.sleep(500000)"".',
            f'The execution of the "{sys.executable} -c "import time; time.sleep(500000)"" command was canceled using a cancellation token.',
        ),
        (
            (f'python -c "import time; time.sleep({500_000})"',),
            {'catch_output': True, 'timeout': 0.0001},
            TimeoutCancellationError,
            'The beginning of the execution of the command "python -c "import time; time.sleep(500000)"".',
            'The execution of the "python -c "import time; time.sleep(500000)"" command was canceled using a cancellation token.',
        ),
    ],
)
def test_logging_with_expired_timeout(command, run_kwargs, expected_exception, first_log_message, second_log_message):
    """Timeout cancellation logs INFO at startup and ERROR with the cancellation message afterwards."""
    logger = MemoryLogger()

    if expected_exception is None:
        run(*command, logger=logger, **run_kwargs)
    else:
        with pytest.raises(expected_exception):
            run(*command, logger=logger, **run_kwargs)

    assert len(logger.data.info) == 1
    assert len(logger.data.error) == 1
    assert len(logger.data) == 2

    assert logger.data.info[0].message == first_log_message
    assert logger.data.error[0].message == second_log_message


@pytest.mark.parametrize(
    ('command', 'first_log_message', 'second_log_message'),
    [
        ((sys.executable, '-c 1/0'), f'The beginning of the execution of the command "{sys.executable} -c 1/0".', f'Error when executing the command "{sys.executable} -c 1/0".'),
        (('python -c 1/0',), 'The beginning of the execution of the command "python -c 1/0".', 'Error when executing the command "python -c 1/0".'),
    ],
)
@pytest.mark.parametrize('catch_exceptions', [True, False])
def test_runtime_failure_logs_startup_info_and_failure_error_in_both_exception_modes(
    command,
    first_log_message,
    second_log_message,
    catch_exceptions,
):
    """Runtime subprocess failure logs the same startup INFO and failure ERROR messages whether exceptions are caught or raised."""
    logger = MemoryLogger()

    if catch_exceptions:
        run(*command, logger=logger, catch_exceptions=True, catch_output=True)
    else:
        with pytest.raises(RunningCommandError):
            run(*command, logger=logger, catch_output=True)

    assert len(logger.data.info) == 1
    assert len(logger.data.error) == 1
    assert len(logger.data) == 2

    assert logger.data.info[0].message == first_log_message
    assert logger.data.error[0].message == second_log_message


@pytest.mark.parametrize(
    'command',
    [
        (sys.executable, '-c "import time; time.sleep({sleep_time})"'),
        ('python -c "import time; time.sleep({sleep_time})"',),
    ],
)
@pytest.mark.parametrize(
    ('catch_exceptions', 'expected_exception'),
    [
        (True, None),
        (False, ConditionCancellationError),
    ],
)
def test_condition_token_cancellation_returns_or_raises_killed_result_according_to_catch_exceptions(
    command,
    catch_exceptions,
    expected_exception,
    assert_no_suby_thread_leaks,
):
    """ConditionToken cancellation kills the process in both modes; catch_exceptions controls whether the result is returned or attached to ConditionCancellationError."""
    sleep_time = 100000
    timeout = 0.1
    command = [subcommand.format(sleep_time=sleep_time) if isinstance(subcommand, str) else subcommand for subcommand in command]

    start_time = perf_counter()
    token = ConditionToken(lambda: perf_counter() - start_time > timeout)

    with assert_no_suby_thread_leaks():
        if expected_exception is None:
            result = run(*command, catch_exceptions=catch_exceptions, token=token)
        else:
            with pytest.raises(expected_exception) as exc_info:
                run(*command, catch_exceptions=catch_exceptions, token=token)
            assert exc_info.value.token is token
            result = exc_info.value.result

    end_time = perf_counter()

    assert result.returncode != 0
    assert result.stdout == ''
    assert result.stderr == ''
    assert result.killed_by_token == True

    assert end_time - start_time >= timeout
    assert end_time - start_time < sleep_time


@pytest.mark.parametrize(
    ('command', 'run_timeout', 'expected_exception', 'expected_token_identity'),
    [
        ((sys.executable, '-c "import time; time.sleep({sleep_time})"'), 3, ConditionCancellationError, True),
        (('python -c "import time; time.sleep({sleep_time})"',), 3, ConditionCancellationError, True),
        ((sys.executable, '-c "import time; time.sleep({sleep_time})"'), 0.05, TimeoutCancellationError, False),
        (('python -c "import time; time.sleep({sleep_time})"',), 0.05, TimeoutCancellationError, False),
    ],
)
def test_token_plus_timeout_without_catching_raises_expected_cancellation(
    command,
    run_timeout,
    expected_exception,
    expected_token_identity,
    assert_no_suby_thread_leaks,
):
    """When token and timeout are both configured, the earlier cancellation source determines which exception is raised."""
    sleep_time = 100000
    timeout = 0.1
    command = [subcommand.format(sleep_time=sleep_time) if isinstance(subcommand, str) else subcommand for subcommand in command]

    start_time = perf_counter()
    token = ConditionToken(lambda: perf_counter() - start_time > timeout)

    with assert_no_suby_thread_leaks(), pytest.raises(expected_exception) as exc_info:
        run(*command, token=token, timeout=run_timeout)

    if expected_token_identity:
        assert exc_info.value.token is token
    else:
        assert exc_info.value.token is not token
    result = exc_info.value.result

    end_time = perf_counter()

    assert result.returncode != 0
    assert result.stdout == ''
    assert result.stderr == ''
    assert result.killed_by_token == True

    assert end_time - start_time >= min(timeout, run_timeout)
    assert end_time - start_time < sleep_time


@pytest.mark.parametrize(
    ('command', 'callback_kwarg', 'expected_stdout', 'expected_stderr', 'expected_callback_output'),
    [
        (
            (sys.executable, '-c "print(\'kek\')"'),
            'stdout_callback',
            'kek\n',
            '',
            'kek\n',
        ),
        (
            ('python -c "print(\'kek\')"',),
            'stdout_callback',
            'kek\n',
            '',
            'kek\n',
        ),
        (
            (sys.executable, '-c "import sys; sys.stderr.write(\'kek\')"'),
            'stderr_callback',
            '',
            'kek',
            'kek',
        ),
        (
            ('python -c "import sys; sys.stderr.write(\'kek\')"',),
            'stderr_callback',
            '',
            'kek',
            'kek',
        ),
    ],
)
def test_replace_output_callback(command, callback_kwarg, expected_stdout, expected_stderr, expected_callback_output):
    """A custom stdout/stderr callback receives each line and suppresses the default console forwarding."""
    accumulator = []

    stderr_buffer = StringIO()
    stdout_buffer = StringIO()

    with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
        result = run(*command, **{callback_kwarg: accumulator.append})

    assert accumulator == [expected_callback_output]

    assert result.returncode == 0
    assert result.stdout == expected_stdout
    assert result.stderr == expected_stderr

    assert stderr_buffer.getvalue() == ''
    assert stdout_buffer.getvalue() == ''


@pytest.mark.parametrize(
    ('arguments', 'exception_message'),
    [
        ([None], 'Only strings and pathlib.Path objects can be positional arguments when calling the suby function. You passed "None" (NoneType).'),
        ([1], 'Only strings and pathlib.Path objects can be positional arguments when calling the suby function. You passed "1" (int).'),
        (['python', 1], 'Only strings and pathlib.Path objects can be positional arguments when calling the suby function. You passed "1" (int).'),
    ],
)
def test_pass_wrong_positional_argument(arguments, exception_message):
    """Passing a positional argument that is neither str nor Path raises TypeError with a descriptive message."""
    with pytest.raises(TypeError, match=match(exception_message)):
        run(*arguments)


@pytest.mark.parametrize(
    'command',
    [
        (Path(sys.executable), '-c', 'print(\'kek\')'),
        (sys.executable, '-c', 'print(\'kek\')'),
        ('python', '-c', 'print(\'kek\')'),
    ],
)
def test_split_false_with_multiple_args_executes_successfully(command):
    """With split=False, multiple already-separated command arguments execute successfully, including a Path executable."""
    result = run(*command, split=False)

    assert result.stdout == 'kek\n'
    assert result.stderr == ''
    assert result.returncode == 0


@pytest.mark.parametrize(
    ('command', 'exception_message'),
    [
        ((Path(sys.executable), '-c "'), 'The expression "-c "" cannot be parsed.'),
        ((sys.executable, '-c "'), 'The expression "-c "" cannot be parsed.'),
        (('python -c "',), 'The expression "python -c "" cannot be parsed.'),
    ],
)
def test_wrong_command(command, exception_message):
    """Malformed shlex input such as an unterminated quote raises WrongCommandError."""
    with pytest.raises(WrongCommandError, match=match(exception_message)):
        run(*command)


def test_empty_command_raises_wrong_command_error():
    """Calling run() without positional command arguments raises WrongCommandError."""
    with pytest.raises(WrongCommandError, match=match('You must pass at least one positional argument with the command to run.')):
        run()


def test_single_string_is_split_on_all_platforms():
    """A single command string is split into executable and arguments on every supported platform.

    This guards the old Windows behavior where one command string was passed as a single argv token and failed to start.
    """
    result = run('python -c pass')

    assert result.returncode == 0
    assert result.stdout == ''
    assert result.stderr == ''


def test_envs_for_subprocess_are_same_as_parent():
    """A child process inherits the parent environment variables."""
    subprocess_env = json.loads(run('python -c "import os, json; print(json.dumps(dict(os.environ)))"').stdout)
    parent_env = dict(environ)

    # why: https://stackoverflow.com/questions/1780483/lines-and-columns-environmental-variables-lost-in-a-script
    subprocess_env.pop('LINES', None)
    subprocess_env.pop('COLUMNS', None)
    parent_env.pop('LINES', None)
    parent_env.pop('COLUMNS', None)

    assert subprocess_env == parent_env


def test_executable_path_with_backslashes_passed_as_string():
    """Backslashes in a string executable path survive command parsing and the command starts successfully.

    This matters on Windows because sys.executable is usually a path like C:\\Python\\python.exe, and shlex with
    posix=True would otherwise treat backslashes as escapes.
    """
    result = run(sys.executable, '-c pass')

    assert result.returncode == 0


def test_executable_path_with_spaces_passed_as_unquoted_string_fails(tmp_path):
    """An unquoted executable path with spaces is split into multiple tokens and therefore fails to start.

    The supported alternatives are quoting that path in a single command string or passing it as a separate Path object.
    """
    space_dir = tmp_path / 'dir with space'
    space_dir.mkdir()
    script = space_dir / 'script.py'
    script.write_text('pass')

    with pytest.raises(RunningCommandError):
        # shlex splits on the space → python receives 'dir', 'with', 'space/script.py'
        # as separate arguments instead of the script path
        run(f'python {script}')


def test_argument_with_trailing_backslash(tmp_path):
    """With split=False, an argument ending in a trailing backslash is passed to the subprocess unchanged.

    This covers the Windows list2cmdline edge case where a quoted argument ending in \\ can otherwise be parsed as an
    escaped closing quote and mangle the argv tail.
    """
    dir_with_trailing_backslash = str(tmp_path) + '\\'  # tmp_path on Windows has spaces

    result = run(
        sys.executable,
        '-c', 'import sys; print(sys.argv[1])',
        dir_with_trailing_backslash,
        split=False,
        catch_output=True,
    )

    assert result.stdout.strip() == dir_with_trailing_backslash


@pytest.mark.skipif(sys.platform != 'win32', reason='Windows-only test')
def test_double_backslash_enabled_by_default_on_windows():
    """On Windows, the default double_backslash=True preserves backslashes and lets a sys.executable command string run."""
    result = run(f'{sys.executable} -c "print(\'kek\')"', catch_output=True)

    assert result.returncode == 0
    assert result.stdout == 'kek\n'


@pytest.mark.skipif(sys.platform != 'win32', reason='Windows-only test')
def test_double_backslash_can_be_disabled_on_windows():
    """On Windows, double_backslash=False lets shlex consume backslashes and turns the executable path invalid."""
    # r'C:\fake\python.exe' becomes 'C:fakepython.exe' because shlex in POSIX mode treats \f and \p as escapes.
    with pytest.raises(RunningCommandError, match=match('The executable for the command "C:fakepython.exe -c pass" was not found.')):
        run(r'C:\fake\python.exe -c pass', double_backslash=False)


@pytest.mark.parametrize(
    ('system_name', 'expected_double_backslash'),
    [
        (''.join(['Win', 'dows']), True),
        ('Linux', False),
        ('Zindows', False),
    ],
)
def test_default_double_backslash_is_bound_from_exact_platform_system_name(
    system_name,
    expected_double_backslash,
):
    """The default double_backslash value reaches convert_arguments() from an exact platform.system() == 'Windows' check at import time."""
    module = _load_run_module_with_default_system(system_name)

    with patch.object(module, 'convert_arguments', wraps=module.convert_arguments) as mock_convert_arguments, \
         patch.object(module, 'Popen', side_effect=FileNotFoundError('mocked missing command')):
        module.run('python -c pass', catch_exceptions=True)

    mock_convert_arguments.assert_called_once()
    assert mock_convert_arguments.call_args.args == (('python -c pass',), True, expected_double_backslash)


@pytest.mark.parametrize(
    ('run_kwargs', 'expected_output'),
    [
        ({}, 'hello world'),
        ({'double_backslash': True}, 'hello\\'),
    ],
)
@pytest.mark.skipif(sys.platform == 'win32', reason='non-Windows test')
def test_double_backslash_argument_processing_on_non_windows(run_kwargs, expected_output):
    """On non-Windows, default double_backslash=False merges backslash-space into one space, while double_backslash=True preserves the backslash."""
    result = run(
        sys.executable,
        '-c "import sys; print(sys.argv[1])"',
        r'hello\ world',
        catch_output=True,
        **run_kwargs,
    )

    assert result.returncode == 0
    assert result.stdout.strip() == expected_output


def test_run_returns_subprocess_result():
    """run() returns a SubprocessResult object."""
    result = run('python -c pass')

    assert isinstance(result, SubprocessResult)


def test_run_starts_popen_with_line_buffered_utf8_text_pipes(assert_no_suby_thread_leaks):
    """run() starts Popen with line-buffered UTF-8 text pipes for both stdout and stderr."""
    with assert_no_suby_thread_leaks(), \
         patch.object(_run_module, 'Popen', wraps=_run_module.Popen) as mock_popen:
        result = run(sys.executable, '-c', 'pass', split=False, catch_output=True)

    mock_popen.assert_called_once_with(
        [sys.executable, '-c', 'pass'],
        stdout=_run_module.PIPE,
        stderr=_run_module.PIPE,
        bufsize=1,
        text=True,
        encoding='utf-8',
        errors='strict',
    )
    assert result.returncode == 0


def test_missing_command_with_catch_exceptions_returns_filled_result():
    """A missing executable with catch_exceptions=True returns an empty-output result with empty stderr and returncode=1."""
    result = run('command_that_definitely_does_not_exist_12345', catch_exceptions=True)

    assert result.stdout == ''
    assert result.stderr == ''
    assert result.returncode == 1
    assert result.killed_by_token is False


def test_missing_command_without_catch_exceptions_attaches_filled_result():
    """A missing executable raises RunningCommandError with an empty-output result, returncode=1, and FileNotFoundError as __cause__."""
    with pytest.raises(
        RunningCommandError,
        match=match('The executable for the command "command_that_definitely_does_not_exist_12345" was not found.'),
    ) as exc_info:
        run('command_that_definitely_does_not_exist_12345')

    assert isinstance(exc_info.value.__cause__, FileNotFoundError)
    assert exc_info.value.result.stdout == ''
    assert exc_info.value.result.stderr == ''
    assert exc_info.value.result.returncode == 1
    assert exc_info.value.result.killed_by_token is False


def test_missing_command_with_catch_exceptions_logs_exception():
    """A missing executable with catch_exceptions=True logs startup INFO and a startup-specific exception message."""
    logger = MemoryLogger()

    result = run('command_that_definitely_does_not_exist_12345', catch_exceptions=True, logger=logger)

    assert result.returncode == 1
    assert len(logger.data.info) == 1
    assert len(logger.data.error) == 0
    assert len(logger.data.exception) == 1
    assert logger.data.info[0].message == 'The beginning of the execution of the command "command_that_definitely_does_not_exist_12345".'
    assert logger.data.exception[0].message == 'The executable for the command "command_that_definitely_does_not_exist_12345" was not found.'


@pytest.mark.skipif(sys.platform != 'win32', reason='Windows-only FileNotFoundError message regression')
def test_missing_command_name_is_preserved_in_running_command_error_on_windows():
    """Windows startup FileNotFoundError keeps the missing command name in the re-raised RunningCommandError."""
    missing_command = 'command_that_definitely_does_not_exist_12345'

    with pytest.raises(
        RunningCommandError,
        match=match(f'The executable for the command "{missing_command}" was not found.'),
    ) as exc_info:
        run(missing_command)

    assert isinstance(exc_info.value.__cause__, FileNotFoundError)
    assert missing_command not in str(exc_info.value.__cause__)
    assert exc_info.value.result.stdout == ''
    assert exc_info.value.result.stderr == ''


@pytest.mark.skipif(sys.platform != 'win32', reason='Windows-only FileNotFoundError message regression')
def test_missing_command_name_is_preserved_in_exception_log_on_windows():
    """Windows startup FileNotFoundError keeps the missing command name in logger.exception()."""
    missing_command = 'command_that_definitely_does_not_exist_12345'
    logger = MemoryLogger()

    result = run(missing_command, catch_exceptions=True, logger=logger)

    assert result.stdout == ''
    assert result.stderr == ''
    assert result.returncode == 1
    assert len(logger.data.exception) == 1
    assert logger.data.exception[0].message == f'The executable for the command "{missing_command}" was not found.'


@pytest.mark.parametrize(
    ('startup_error', 'expected_message'),
    [
        (
            FileNotFoundError('missing executable'),
            'The executable for the command "python -c pass" was not found.',
        ),
        (
            PermissionError('permission denied'),
            'Permission denied when starting the command "python -c pass".',
        ),
        (
            OSError('generic startup failure'),
            'OS error when starting the command "python -c pass".',
        ),
    ],
)
def test_format_startup_failure_message_varies_text_by_startup_error_type(startup_error, expected_message):
    """format_startup_failure_message() returns a different startup message for each OSError subclass."""
    message = _run_module.format_startup_failure_message('python -c pass', startup_error)

    assert message == expected_message


@pytest.mark.parametrize(
    ('startup_error', 'command', 'expected_message'),
    [
        (
            FileNotFoundError('missing executable'),
            'missing-tool --flag',
            'The executable for the command "missing-tool --flag" was not found.',
        ),
        (
            PermissionError('permission denied'),
            'locked-tool --flag',
            'Permission denied when starting the command "locked-tool --flag".',
        ),
        (
            OSError('generic startup failure'),
            'broken-tool --flag',
            'OS error when starting the command "broken-tool --flag".',
        ),
    ],
)
def test_startup_oserrors_from_popen_are_wrapped_in_running_command_error_with_dedicated_message(
    startup_error,
    command,
    expected_message,
):
    """FileNotFoundError, PermissionError, and generic OSError from process startup are wrapped in RunningCommandError with startup-specific messages."""
    with patch.object(_run_module, 'Popen', side_effect=startup_error), \
         pytest.raises(
             RunningCommandError,
             match=match(expected_message),
         ) as exc_info:
        run(command)

    assert isinstance(exc_info.value, RunningCommandError)
    assert exc_info.value.__cause__ is startup_error
    assert exc_info.value.result.stdout == ''
    assert exc_info.value.result.stderr == ''
    assert exc_info.value.result.returncode == 1
    assert exc_info.value.result.killed_by_token is False


@pytest.mark.parametrize(
    ('startup_error', 'command', 'expected_message'),
    [
        (
            FileNotFoundError('missing executable'),
            'missing-tool --flag',
            'The executable for the command "missing-tool --flag" was not found.',
        ),
        (
            PermissionError('permission denied'),
            'locked-tool --flag',
            'Permission denied when starting the command "locked-tool --flag".',
        ),
        (
            OSError('generic startup failure'),
            'broken-tool --flag',
            'OS error when starting the command "broken-tool --flag".',
        ),
    ],
)
def test_startup_failure_log_message_matches_running_command_error_text(startup_error, command, expected_message):
    """With catch_exceptions=True, logger.exception() uses the same startup text as RunningCommandError."""
    logger = MemoryLogger()

    with patch.object(_run_module, 'Popen', side_effect=startup_error):
        result = run(command, catch_exceptions=True, logger=logger)

    assert result.stdout == ''
    assert result.stderr == ''
    assert result.returncode == 1
    assert result.killed_by_token is False
    assert len(logger.data.exception) == 1
    assert logger.data.exception[0].message == expected_message

    with patch.object(_run_module, 'Popen', side_effect=startup_error), \
         pytest.raises(RunningCommandError) as exc_info:
        run(command)

    assert isinstance(exc_info.value, RunningCommandError)
    assert str(exc_info.value) == logger.data.exception[0].message


def test_runtime_failure_has_none_cause_and_keeps_process_stderr(assert_no_suby_thread_leaks):
    """After a successful process start, runtime failures keep __cause__ unset and preserve real stderr."""
    logger = MemoryLogger()

    with assert_no_suby_thread_leaks(), \
         pytest.raises(
             RunningCommandError,
             match=match(f'Error when executing the command "{sys.executable} -c "raise ValueError"".'),
         ) as exc_info:
        run(sys.executable, '-c "raise ValueError"', logger=logger)

    assert isinstance(exc_info.value, RunningCommandError)
    assert exc_info.value.__cause__ is None
    assert exc_info.value.result.stdout == ''
    assert 'ValueError' in exc_info.value.result.stderr
    assert exc_info.value.result.returncode != 0
    assert len(logger.data.error) == 1
    assert logger.data.error[0].message == f'Error when executing the command "{sys.executable} -c "raise ValueError"".'


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX-only exec format semantics')
@pytest.mark.parametrize('catch_exceptions', [True, False])
def test_exec_format_error_is_normalized_in_both_exception_modes(
    tmp_path,
    catch_exceptions,
    assert_no_suby_thread_leaks,
):
    """A script without a shebang becomes a generic startup failure both when the result is returned and when RunningCommandError is raised."""
    script = tmp_path / 'script-without-shebang'
    script.write_text('echo hello\n')
    script.chmod(0o755)
    logger = MemoryLogger()

    if catch_exceptions:
        with assert_no_suby_thread_leaks():
            result = run(str(script), catch_exceptions=True, logger=logger)

        assert result.stdout == ''
        assert result.stderr == ''
        assert result.returncode == 1
        assert len(logger.data.exception) == 1
        assert logger.data.exception[0].message == f'OS error when starting the command "{script}".'
    else:
        with assert_no_suby_thread_leaks(), pytest.raises(
            RunningCommandError,
            match=match(f'OS error when starting the command "{script}".'),
        ) as exc_info:
            run(str(script))

        assert type(exc_info.value.__cause__) is OSError
        assert 'Exec format error' in str(exc_info.value.__cause__)


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX-only permission semantics')
@pytest.mark.parametrize('catch_exceptions', [True, False])
def test_permission_error_is_normalized_in_both_exception_modes(tmp_path, catch_exceptions):
    """Permission-denied startup fills the startup-failure result in both modes and logs the startup message when exceptions are caught."""
    script = tmp_path / 'script.sh'
    script.write_text('echo hello')
    script.chmod(0o644)
    logger = MemoryLogger()

    if catch_exceptions:
        result = run(str(script), catch_exceptions=True, logger=logger)
        assert len(logger.data.exception) == 1
        assert logger.data.exception[0].message == f'Permission denied when starting the command "{script}".'
    else:
        with pytest.raises(
            RunningCommandError,
            match=match(f'Permission denied when starting the command "{script}".'),
        ) as exc_info:
            run(str(script))
        result = exc_info.value.result

    assert result.stdout == ''
    assert result.stderr == ''
    assert result.returncode == 1
    assert result.killed_by_token is False


def test_multiple_strings_split_independently():
    """Each string argument is split independently, then all subprocess argument pieces are concatenated.

    For example, 'python -c' becomes ['python', '-c'] and '"print(777)"' becomes ['print(777)'].
    """
    result = run('python -c', '"print(777)"', catch_output=True)

    assert result.returncode == 0
    assert result.stdout == '777\n'


def test_argument_with_space_passed_with_split_false():
    """With split=False, a value containing spaces is passed as a single command-line argument."""
    result = run(
        sys.executable,
        '-c', 'import sys; print(sys.argv[1])',
        'hello world',
        split=False,
        catch_output=True,
    )

    assert result.returncode == 0
    assert result.stdout.strip() == 'hello world'


@pytest.mark.parametrize(
    'error_name',
    [
        'ConditionCancellationError',
        'RunningCommandError',
        'WrongCommandError',
        'TimeoutCancellationError',
    ],
)
def test_errors_are_importable_from_suby(error_name):
    """The public exception classes are re-exported from the suby package root."""
    assert hasattr(suby, error_name)


@pytest.mark.parametrize(
    'token_factory',
    [
        pytest.param(lambda: SimpleToken().cancel(), id='simple-token'),
        pytest.param(lambda: ConditionToken(lambda: True), id='condition-token'),
    ],
)
def test_pre_cancelled_token_returns_killed_result_with_catch_exceptions(token_factory):
    """With catch_exceptions=True, a token cancelled before startup returns killed_by_token=True and a non-zero returncode."""
    token = token_factory()

    result = run('python -c "import time; time.sleep(100)"', token=token, catch_exceptions=True)

    assert result.killed_by_token == True
    assert result.returncode != 0


def test_already_cancelled_simple_token_raises():
    """An already-cancelled SimpleToken raises CancellationError when exceptions are not caught."""
    token = SimpleToken()
    token.cancel()

    with pytest.raises(CancellationError):
        run('python -c "import time; time.sleep(100)"', token=token)


def test_timeout_exception_message():
    """TimeoutCancellationError uses the documented timeout-expired message."""
    with pytest.raises(TimeoutCancellationError, match=match('The timeout of 1 seconds has expired.')):
        run('python -c "import time; time.sleep(100)"', timeout=1)


def test_large_output():
    """A command that prints many lines has its full stdout collected in order."""
    lines = 1000

    result = run(f'python -c "for i in range({lines}): print(i)"', catch_output=True)

    assert result.returncode == 0
    assert result.stdout == ''.join(f'{i}\n' for i in range(lines))


def test_parallel_runs():
    """Concurrent run() calls from multiple Python threads return independent stdout results."""
    results: List[SubprocessResult] = [SubprocessResult() for _ in range(10)]

    def run_task(i: int):
        results[i] = run(f'python -c "print({i})"', catch_output=True)

    threads = [Thread(target=run_task, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    for i, result in enumerate(results):

        assert result.returncode == 0
        assert result.stdout == f'{i}\n'


def _python_print_argv_script() -> str:
    return 'import json, sys; print(json.dumps(sys.argv[1:]))'


@pytest.mark.parametrize(
    'command',
    [
        ('',),
        ('', ''),
        ('   ',),
        ('"',),
        ("'",),
        ('python -c "',),
    ],
)
def test_malformed_commands_are_rejected(command):
    """Empty, whitespace-only, quote-only, and unterminated quoted command strings are rejected."""
    with pytest.raises(WrongCommandError):
        run(*command)


def test_very_long_command_string_is_handled():
    """A very long command line still executes when it stays within the platform limit.

    Windows has a much lower CreateProcess command line limit than POSIX, so the payload is capped there to the
    longest command line accepted by the platform.
    """
    if sys.platform == 'win32':
        payload = _windows_print_payload_for_command_line_length(_WINDOWS_MAXIMUM_COMMAND_LINE_LENGTH)
    else:
        payload = 'x' * 100_000

    result = run(sys.executable, '-c', f'print("{payload}")', split=False, catch_output=True)

    assert result.stdout == payload + '\n'


@pytest.mark.skipif(sys.platform != 'win32', reason='Windows-only CreateProcess command line limit')
def test_very_long_command_string_handles_values_below_windows_limit():
    """A Windows command line just below the CreateProcess limit should still execute successfully."""
    payload = _windows_print_payload_for_command_line_length(_WINDOWS_MAXIMUM_COMMAND_LINE_LENGTH - 128)

    result = run(sys.executable, '-c', f'print("{payload}")', split=False, catch_output=True)

    assert result.stdout == payload + '\n'


@pytest.mark.skipif(sys.platform != 'win32', reason='Windows-only CreateProcess command line limit')
def test_too_long_command_string_is_normalized_on_windows():
    """Windows rejects command lines longer than 32767 chars, and suby should expose that as RunningCommandError."""
    payload = _windows_print_payload_for_command_line_length(_WINDOWS_MAXIMUM_COMMAND_LINE_LENGTH) + 'x'

    with pytest.raises(RunningCommandError) as exc_info:
        run(sys.executable, '-c', f'print("{payload}")', split=False, catch_output=True)

    assert isinstance(exc_info.value.__cause__, OSError)
    assert exc_info.value.result.stdout == ''
    assert exc_info.value.result.stderr == ''
    assert exc_info.value.result.returncode == 1
    assert exc_info.value.result.killed_by_token is False


@pytest.mark.parametrize(
    ('command', 'result_attr', 'expected_output'),
    [
        ('print("x" * {payload_size})', 'stdout', 'x' * 100_000 + '\n'),
        ('import sys; sys.stderr.write("x" * {payload_size})', 'stderr', 'x' * 100_000),
    ],
)
def test_very_large_output_is_handled_without_a_huge_command_line(command, result_attr, expected_output):
    """A very large stdout or stderr payload is captured even when the command line itself stays short."""
    payload_size = 100_000
    result = run(sys.executable, '-c', command.format(payload_size=payload_size), split=False, catch_output=True)

    assert getattr(result, result_attr) == expected_output


def test_command_with_nul_byte_is_rejected_consistently():
    """A command string containing NUL is rejected as RunningCommandError or ValueError, depending on platform."""
    with pytest.raises((RunningCommandError, ValueError)):
        run('abc\0def')


def test_empty_path_object_is_rejected_consistently():
    """An empty Path executable is rejected as RunningCommandError or PermissionError, depending on platform."""
    with pytest.raises((RunningCommandError, PermissionError)):
        run(Path(''))  # noqa: PTH201


def test_current_directory_path_object_is_rejected_consistently():
    """Executing Path('.') raises RunningCommandError and preserves the startup OSError as __cause__."""
    with pytest.raises(RunningCommandError) as exc_info:
        run(Path('.'))  # noqa: PTH201

    assert isinstance(exc_info.value.__cause__, OSError)


def test_path_with_spaces_and_special_characters_executes_via_path_object(tmp_path: Path):
    """A script path containing spaces and shell-like characters still executes when passed as a Path object."""
    script = tmp_path / 'dir with spaces #and(parens)'
    script.mkdir()
    executable = script / 'echo.py'
    executable.write_text('print("ok")')
    result = run(Path(sys.executable), executable, split=False, catch_output=True)

    assert result.stdout == 'ok\n'


def test_kill_process_if_running_ignores_process_lookup_error():
    """kill_process_if_running() ignores ProcessLookupError when the process exits between poll() and kill()."""
    class MockProcess:
        def poll(self):
            return None

        def kill(self):
            raise ProcessLookupError('already exited')

    _run_module.kill_process_if_running(MockProcess())  # type: ignore[arg-type]


def test_path_object_that_looks_like_flag_is_treated_as_plain_argument():
    """A Path object such as Path('-c') is treated as a literal subprocess argument, not a string to re-split."""
    result = run(
        Path(sys.executable),
        Path('-c'),
        Path(_python_print_argv_script()),
        split=False,
        catch_output=True,
    )

    assert result.returncode == 0


@pytest.mark.parametrize(
    'command',
    [
        b'python',
        bytearray(b'python'),
        PurePath('python'),
    ],
)
def test_non_string_command_arguments_are_rejected(command):
    """Unsupported bytes-like and PurePath command arguments are rejected with TypeError."""
    with pytest.raises(TypeError):
        run(command)  # type: ignore[arg-type]


def test_string_like_object_is_rejected():
    """Objects that only implement __str__ are rejected unless they are actual str or Path instances."""
    class StringLikeObject:
        def __str__(self) -> str:
            return 'python'

    with pytest.raises(TypeError):
        run(StringLikeObject())  # type: ignore[arg-type]


def test_split_false_does_not_split_single_string_command():
    """With split=False, a whole command string is treated as one executable name and fails to start."""
    with pytest.raises(
        RunningCommandError,
        match=match('The executable for the command ""python -c "print(1)""" was not found.'),
    ):
        run('python -c "print(1)"', split=False)


@pytest.mark.parametrize(
    ('arguments', 'run_kwargs', 'expected_stdout'),
    [
        (('hello world',), {}, '["hello world"]\n'),
        ((r'hello\ world',), {'double_backslash': True}, '["hello\\\\ world"]\n'),
        (('endswith\\',), {}, '["endswith\\\\"]\n'),
        ((r'folder with spaces\name',), {}, '["folder with spaces\\\\name"]\n'),
        ((r'value\"quoted',), {}, '["value\\\\\\"quoted"]\n'),
        ((r'hello\ world', 'two words'), {}, '["hello\\\\ world", "two words"]\n'),
    ],
)
def test_split_false_preserves_argument_shape(arguments, run_kwargs, expected_stdout):
    """With split=False, spaces, backslashes, quotes, and multi-argument argv shapes are passed through unchanged."""
    result = run(
        sys.executable,
        '-c',
        _python_print_argv_script(),
        *arguments,
        split=False,
        catch_output=True,
        **run_kwargs,
    )

    assert result.stdout == expected_stdout


def test_split_false_with_empty_executable_is_rejected():
    """With split=False, an empty executable is rejected as RunningCommandError or ValueError."""
    with pytest.raises((RunningCommandError, ValueError)):
        run('', split=False)


def test_double_backslash_does_not_change_path_objects():
    """double_backslash only rewrites string arguments before shlex splitting, not Path arguments."""
    arguments = (Path(r'folder\subdir\tool.exe'),)

    assert _run_module.convert_arguments(arguments, split=True, double_backslash=False) == [r'folder\subdir\tool.exe']
    assert _run_module.convert_arguments(arguments, split=True, double_backslash=True) == [r'folder\subdir\tool.exe']


@pytest.mark.skipif(sys.platform != 'win32', reason='Windows-only UNC path semantics')
def test_unc_path_returns_startup_failure_result_on_windows():
    """An unavailable UNC network path like \\\\server\\share\\tool.exe returns a startup-failure result on Windows."""
    result = run(r'\\server\share\python.exe -c pass', catch_exceptions=True)

    assert isinstance(result, SubprocessResult)
    assert result.stdout == ''
    assert result.stderr == ''
    assert result.returncode == 1
    assert result.killed_by_token is False


@pytest.mark.skipif(sys.platform != 'win32', reason='Windows-only UNC path parsing')
def test_unc_path_is_passed_to_popen_without_backslash_loss_on_windows():
    """A UNC network executable path should reach subprocess startup as one executable argument with backslashes intact."""
    unc_command = r'\\server\share\python.exe -c pass'
    expected_argv = [r'\\server\share\python.exe', '-c', 'pass']

    with patch.object(_run_module, 'Popen', side_effect=FileNotFoundError('mocked missing UNC executable')) as mock_popen:
        result = run(unc_command, catch_exceptions=True)

    assert result.stdout == ''
    assert result.stderr == ''
    assert result.returncode == 1
    assert result.killed_by_token is False
    assert mock_popen.call_args.args[0] == expected_argv


@pytest.mark.parametrize(
    ('filename', 'contents', 'mode', 'expected_cause'),
    [
        ('script.sh', 'echo hello\n', 0o644, PermissionError),
        ('script-without-shebang', 'echo hello\n', 0o755, OSError),
        ('script.py', '#!/definitely/missing/interpreter\nprint("hello")\n', 0o755, OSError),
    ],
)
@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX-only startup failure semantics')
@pytest.mark.usefixtures('assert_no_suby_thread_leaks')
def test_posix_file_startup_failures_are_normalized_via_running_command_error(
    tmp_path: Path,
    filename,
    contents,
    mode,
    expected_cause,
):
    """POSIX startup failures are converted to RunningCommandError while preserving the original OS error in __cause__."""
    script = tmp_path / filename
    script.write_text(contents)
    script.chmod(mode)

    with pytest.raises(RunningCommandError) as exc_info:
        run(str(script))

    assert isinstance(exc_info.value.__cause__, expected_cause)


def test_directory_as_executable_is_normalized():
    """Trying to execute the current directory is converted from a raw OS startup error to RunningCommandError."""
    with pytest.raises(RunningCommandError):
        run(str(Path.cwd()))


@pytest.mark.parametrize(
    'missing_command',
    [
        str(Path.cwd() / 'missing-parent-dir' / 'missing-command'),
        'definitely_missing_command_for_suby_tests',
    ],
)
def test_missing_commands_are_normalized(missing_command, assert_no_suby_thread_leaks):
    """Missing commands raise RunningCommandError and keep FileNotFoundError as __cause__."""
    with assert_no_suby_thread_leaks(), pytest.raises(
        RunningCommandError,
        match=match(f'The executable for the command "{missing_command}" was not found.'),
    ) as exc_info:
        run(missing_command)

    assert isinstance(exc_info.value.__cause__, FileNotFoundError)


@pytest.mark.parametrize(
    ('run_kwargs', 'command', 'error_message'),
    [
        (
            {'stdout_callback': lambda _: (_ for _ in ()).throw(RuntimeError('stdout callback exploded'))},
            'print("hello")',
            'stdout callback exploded',
        ),
        (
            {'stderr_callback': lambda _: (_ for _ in ()).throw(RuntimeError('stderr callback exploded'))},
            'import sys; sys.stderr.write("hello\\n")',
            'stderr callback exploded',
        ),
    ],
)
def test_callback_exception_is_raised_by_run(run_kwargs, command, error_message, assert_no_suby_thread_leaks):
    """An exception raised by a stdout/stderr callback becomes the exception raised by run()."""
    with assert_no_suby_thread_leaks(), pytest.raises(RuntimeError, match=error_message):
        run(sys.executable, '-c', command, split=False, **run_kwargs)


@pytest.mark.parametrize(
    ('run_kwargs', 'command', 'expected_stdout', 'expected_stderr', 'error_message'),
    [
        (
            {'stdout_callback': lambda _: (_ for _ in ()).throw(RuntimeError('stdout callback exploded'))},
            'import time; print("hello", flush=True); time.sleep(5)',
            'hello\n',
            str,
            'stdout callback exploded',
        ),
        (
            {'stderr_callback': lambda _: (_ for _ in ()).throw(RuntimeError('stderr callback exploded'))},
            'import sys, time; sys.stderr.write("hello\\n"); sys.stderr.flush(); time.sleep(5)',
            str,
            'hello\n',
            'stderr callback exploded',
        ),
    ],
)
@pytest.mark.usefixtures('assert_no_suby_thread_leaks')
def test_callback_exceptions_kill_process_and_attach_result(
    run_kwargs,
    command,
    expected_stdout,
    expected_stderr,
    error_message,
):
    """A callback failure kills the still-running process and attaches the partial SubprocessResult to the exception."""
    with pytest.raises(RuntimeError, match=error_message) as exc_info:
        run(sys.executable, '-c', command, split=False, **run_kwargs)
    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    if expected_stdout is str:
        assert isinstance(exc_info.value.result.stdout, str)  # type: ignore[attr-defined]
    else:
        assert exc_info.value.result.stdout == expected_stdout  # type: ignore[attr-defined]
    if expected_stderr is str:
        assert isinstance(exc_info.value.result.stderr, str)  # type: ignore[attr-defined]
    else:
        assert exc_info.value.result.stderr == expected_stderr  # type: ignore[attr-defined]
    assert isinstance(exc_info.value.result.returncode, int)  # type: ignore[attr-defined]
    assert exc_info.value.result.returncode != 0  # type: ignore[attr-defined]
    assert exc_info.value.result.killed_by_token is False  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    ('callback_kwarg', 'command', 'expected_stdout', 'expected_stderr', 'error_message'),
    [
        (
            'stdout_callback',
            'print("hello", flush=True)',
            'hello\n',
            '',
            'stdout callback exploded after exit',
        ),
        (
            'stderr_callback',
            'import sys; sys.stderr.write("hello\\n"); sys.stderr.flush()',
            '',
            'hello\n',
            'stderr callback exploded after exit',
        ),
    ],
)
@pytest.mark.usefixtures('assert_no_suby_thread_leaks')
def test_callback_exceptions_after_process_exit_keep_success_returncode(
    callback_kwarg,
    command,
    expected_stdout,
    expected_stderr,
    error_message,
):
    """If a callback fails around process exit, the attached result keeps collected output and a coherent final return code."""
    def callback(_: str):
        time.sleep(0.1)
        raise RuntimeError(error_message)

    with pytest.raises(RuntimeError, match=error_message) as exc_info:
        run(sys.executable, '-c', command, split=False, **{callback_kwarg: callback})
    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert exc_info.value.result.stdout == expected_stdout  # type: ignore[attr-defined]
    assert exc_info.value.result.stderr == expected_stderr  # type: ignore[attr-defined]
    if exc_info.value.result.returncode != 0:  # type: ignore[attr-defined]
        _assert_kill_returncode_matches_platform(exc_info.value.result.returncode)  # type: ignore[attr-defined]
    assert exc_info.value.result.killed_by_token is False  # type: ignore[attr-defined]


def test_concurrent_stdout_and_stderr_callback_failures_raise_one_exception(assert_no_suby_thread_leaks):
    """If stdout and stderr callbacks fail concurrently, run() raises one of those callback exceptions."""
    def stdout_callback(_: str):
        raise RuntimeError('stdout callback exploded')

    def stderr_callback(_: str):
        raise RuntimeError('stderr callback exploded')

    with assert_no_suby_thread_leaks(), pytest.raises(RuntimeError, match=r'(stdout|stderr) callback exploded') as exc_info:
        run(
            sys.executable,
            '-c',
            'import sys, time; print("out", flush=True); sys.stderr.write("err\\n"); sys.stderr.flush(); time.sleep(5)',
            split=False,
            stdout_callback=stdout_callback,
            stderr_callback=stderr_callback,
        )

    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert isinstance(exc_info.value.result.stdout, str)  # type: ignore[attr-defined]
    assert isinstance(exc_info.value.result.stderr, str)  # type: ignore[attr-defined]


def test_second_reader_callback_failure_does_not_replace_first_recorded_failure(assert_no_suby_thread_leaks):
    """If one reader callback failure is already stored, a later callback failure exits through the non-winning set() branch."""
    first_failure_recorded = Event()
    original_failure_set = _run_module._FailureState.set

    def instrumented_failure_set(self, error):
        was_saved = original_failure_set(self, error)
        if was_saved and str(error) == 'stdout callback exploded first':
            first_failure_recorded.set()
        return was_saved

    def stdout_callback(_: str):
        raise RuntimeError('stdout callback exploded first')

    def stderr_callback(_: str):
        if not first_failure_recorded.wait(timeout=1):
            raise RuntimeError('coordinated second failure setup failed')
        raise RuntimeError('stderr callback exploded second')

    with assert_no_suby_thread_leaks(), \
         patch.object(_run_module._FailureState, 'set', new=instrumented_failure_set), \
         pytest.raises(RuntimeError, match='stdout callback exploded first') as exc_info:
        run(
            sys.executable,
            '-c',
            (
                'import sys, time\n'
                'print("out", flush=True)\n'
                'sys.stderr.write("err\\n")\n'
                'sys.stderr.flush()\n'
                'time.sleep(5)\n'
            ),
            split=False,
            stdout_callback=stdout_callback,
            stderr_callback=stderr_callback,
        )

    assert first_failure_recorded.is_set()
    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert exc_info.value.result.stdout == 'out\n'  # type: ignore[attr-defined]
    assert isinstance(exc_info.value.result.stderr, str)  # type: ignore[attr-defined]


def test_read_stream_second_failure_keeps_first_error_and_does_not_set_wake_event():
    """If callback stores one failure and then raises another, read_stream keeps the first one and skips wake_event.set()."""
    class FakeProcess:
        def poll(self):
            return 0

        def kill(self):
            return None

    state = _run_module._ExecutionState()
    first_error = RuntimeError('stdout callback exploded first')
    buffer = []

    def callback(_: str):
        assert state.failure_state.set(first_error) is True
        raise RuntimeError('stderr callback exploded second')

    _run_module.read_stream(
        FakeProcess(),
        StringIO('hello\n'),
        buffer,
        False,
        callback,
        DefaultToken(),
        state,
    )

    assert buffer == ['hello\n']
    assert state.failure_state.error is first_error
    assert state.wake_event.is_set() is False


def test_read_stream_cancelled_custom_token_kills_process_before_reading():
    """A reader thread polls a cancelled custom token before readline(), kills the process, and exits without buffering new output."""
    class FakeProcess:
        def __init__(self):
            self.was_killed = False

        def poll(self):
            return None

        def kill(self):
            self.was_killed = True

    token = SimpleToken()
    token.cancel()
    state = _run_module._ExecutionState()
    buffer: List[str] = []
    process = FakeProcess()

    _run_module.read_stream(
        process,
        StringIO('hello\n'),
        buffer,
        True,
        lambda _line: (_ for _ in ()).throw(RuntimeError('callback should not run')),
        token,
        state,
    )

    assert process.was_killed is True
    assert state.result.killed_by_token is True
    assert buffer == []
    assert state.failure_state.error is None
    assert state.wake_event.is_set() is False


def test_read_stream_ignores_line_if_failure_was_already_recorded_after_readline():
    """If another thread records a failure before the fetched line is handled, read_stream exits without buffering or callback delivery."""
    class FakeProcess:
        def poll(self):
            return 0

        def kill(self):
            return None

    class StreamThatRecordsFailureBeforeReturningLine:
        def __init__(self, state):
            self._state = state

        def readline(self):
            self._state.failure_state.set(RuntimeError('already recorded elsewhere'))
            return 'late-line\n'

    state = _run_module._ExecutionState()
    buffer: List[str] = []
    seen: List[str] = []

    _run_module.read_stream(
        FakeProcess(),
        StreamThatRecordsFailureBeforeReturningLine(state),
        buffer,
        False,
        seen.append,
        DefaultToken(),
        state,
    )

    assert buffer == []
    assert seen == []
    assert state.wake_event.is_set() is False


@pytest.mark.parametrize(
    ('callback_kwarg', 'command', 'error_message'),
    [
        (
            'stdout_callback',
            'import time; print("hello", flush=True); time.sleep(5)',
            'stdout callback exploded',
        ),
        (
            'stderr_callback',
            'import sys, time; sys.stderr.write("hello\\n"); sys.stderr.flush(); time.sleep(5)',
            'stderr callback exploded',
        ),
    ],
)
def test_timeout_and_callback_error_race_raises_either_exception_with_killed_result(
    callback_kwarg,
    command,
    error_message,
):
    """If timeout cancellation and a callback failure race, either exception may win but the returncode still follows the platform kill contract.

    TimeoutCancellationError always sets killed_by_token=True. If a callback RuntimeError wins, killed_by_token can be either
    False or True depending on whether the timeout helper killed the process before the callback failure was propagated.
    """
    returncodes = []

    for _ in range(5):
        def callback(_: str):
            raise RuntimeError(error_message)

        start = time.perf_counter()
        with pytest.raises((RuntimeError, TimeoutCancellationError)) as exc_info:
            run(
                sys.executable,
                '-c',
                command,
                split=False,
                timeout=0.2,
                **{callback_kwarg: callback},
            )
        elapsed = time.perf_counter() - start

        assert elapsed < _PROMPT_EXCEPTION_SECONDS
        result = cast(Any, exc_info.value).result

        assert isinstance(result, SubprocessResult)
        assert isinstance(result.stdout, str)
        assert isinstance(result.stderr, str)
        returncodes.append(result.returncode)

        if isinstance(exc_info.value, TimeoutCancellationError):
            assert result.killed_by_token is True

    for returncode in returncodes:
        _assert_kill_returncode_matches_platform(returncode)


def test_recorded_callback_failure_wins_even_if_timeout_kill_was_already_marked():
    """If timeout machinery already marked the result as killed, raising the recorded callback failure still preserves that killed result on the callback exception."""
    class FakeProcess:
        def __init__(self):
            self.returncode = None

        def poll(self):
            return None

        def kill(self):
            self.returncode = 1 if sys.platform == 'win32' else -9

        def wait(self):
            return self.returncode

    class DummyThread:
        def join(self):
            return None

    process = FakeProcess()
    state = _run_module._ExecutionState()
    state.result.killed_by_token = True
    state.stdout_buffer.append('hello\n')
    error = RuntimeError('stdout callback exploded after timeout kill')
    reader_threads = _run_module._ReaderThreads(stdout=DummyThread(), stderr=DummyThread(), process_waiter=DummyThread())

    with pytest.raises(RuntimeError, match='stdout callback exploded after timeout kill') as exc_info:
        _run_module.raise_background_failure(process, reader_threads, state, error)

    assert exc_info.value is error
    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert exc_info.value.result.stdout == 'hello\n'  # type: ignore[attr-defined]
    assert exc_info.value.result.killed_by_token is True  # type: ignore[attr-defined]
    _assert_kill_returncode_matches_platform(exc_info.value.result.returncode)  # type: ignore[attr-defined]

@pytest.mark.parametrize(
    ('callback_kwarg', 'command', 'expected_stdout', 'expected_stderr', 'error_message'),
    [
        (
            'stdout_callback',
            'import time; print("hello", flush=True); time.sleep(0.05)',
            ('', 'hello\n'),
            str,
            'stdout callback exploded after near-exit',
        ),
        (
            'stderr_callback',
            'import sys, time; sys.stderr.write("hello\\n"); sys.stderr.flush(); time.sleep(0.05)',
            str,
            ('', 'hello\n'),
            'stderr callback exploded after near-exit',
        ),
    ],
)
@pytest.mark.usefixtures('assert_no_suby_thread_leaks')
def test_timeout_and_near_exit_callback_error_race_raises_either_with_killed_result(
    callback_kwarg,
    command,
    expected_stdout,
    expected_stderr,
    error_message,
):
    """Near process exit, a timeout-versus-callback race still leaves a killed result when timeout cancellation wins.

    The return code is validated with a platform-specific branch because POSIX and Windows report killed
    processes differently.
    """
    returncodes = []
    killed_flags = []

    for _ in range(5):
        def callback(_: str):
            time.sleep(0.1)
            raise RuntimeError(error_message)

        start = time.perf_counter()
        with pytest.raises((RuntimeError, TimeoutCancellationError)) as exc_info:
            run(
                sys.executable,
                '-c',
                command,
                split=False,
                timeout=0.02,
                **{callback_kwarg: callback},
            )
        elapsed = time.perf_counter() - start

        assert elapsed < _PROMPT_EXCEPTION_SECONDS
        result = cast(Any, exc_info.value).result

        assert isinstance(result, SubprocessResult)
        if expected_stdout is str:
            assert isinstance(result.stdout, str)
        else:
            assert result.stdout in expected_stdout
        if expected_stderr is str:
            assert isinstance(result.stderr, str)
        else:
            assert result.stderr in expected_stderr
        returncodes.append(result.returncode)
        killed_flags.append(result.killed_by_token)

    for returncode in returncodes:
        _assert_kill_returncode_matches_platform(returncode)
    assert killed_flags == [True, True, True, True, True]


def test_existing_result_attribute_on_callback_exception_is_not_overwritten():
    """If a callback exception already has a result attribute, run() does not overwrite that object."""
    class ResultBearingError(RuntimeError):
        pass

    preserved_result = SubprocessResult()
    preserved_result.stdout = 'preserved'
    preserved_result.stderr = 'preserved'
    preserved_result.returncode = 777
    error = ResultBearingError('stdout callback exploded')
    error.result = preserved_result  # type: ignore[attr-defined]

    def callback(_: str):
        raise error

    with pytest.raises(ResultBearingError) as exc_info:
        run(sys.executable, '-c', 'print("hello")', split=False, stdout_callback=callback)

    assert exc_info.value.result is preserved_result  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    'exception_factory',
    [
        pytest.param(
            lambda: type(
                'ExplodingResultGetterError',
                (RuntimeError,),
                {'result': property(lambda _self: (_ for _ in ()).throw(RuntimeError('result getter exploded')))},
            )('stdout callback exploded'),
            id='getter',
        ),
        pytest.param(
            lambda: type(
                'ExplodingResultSetterError',
                (RuntimeError,),
                {
                    'result': property(
                        lambda _self: None,
                        lambda _self, _value: (_ for _ in ()).throw(RuntimeError('result setter exploded')),
                    ),
                },
            )('stdout callback exploded'),
            id='setter',
        ),
        pytest.param(
            lambda: type(
                'ExplodingResultAssignmentError',
                (RuntimeError,),
                {
                    '__setattr__': lambda self, name, value: (
                        (_ for _ in ()).throw(RuntimeError('result assignment exploded'))
                        if name == 'result'
                        else super(type(self), self).__setattr__(name, value)
                    ),
                },
            )('stdout callback exploded'),
            id='setattr',
        ),
    ],
)
def test_result_attachment_failures_do_not_mask_original_exception(exception_factory):
    """If reading or assigning exception.result fails, run() still raises the original callback exception."""
    error = exception_factory()

    def callback(_: str):
        raise error

    with pytest.raises(type(error), match='stdout callback exploded'):
        run(sys.executable, '-c', 'print("hello")', split=False, stdout_callback=callback)


def test_attach_result_to_exception_handles_exception_without_dict():
    """Attaching a result to a __slots__ exception without __dict__ should not raise a secondary error."""
    class SlotOnlyError(RuntimeError):
        __slots__ = ()

    error = SlotOnlyError('slot-only')
    result = SubprocessResult()
    result.stdout = 'hello'
    result.stderr = ''
    result.returncode = 0

    _run_module.attach_result_to_exception(error, result)

    assert isinstance(error, SlotOnlyError)


def test_attach_result_to_exception_handles_object_without_dict():
    """Attaching a result to a plain object without __dict__ should fail silently."""
    result = SubprocessResult()
    result.stdout = 'hello'
    result.stderr = ''
    result.returncode = 0

    _run_module.attach_result_to_exception(object(), result)  # type: ignore[arg-type]


def test_attach_result_to_exception_preserves_class_level_result_descriptor():
    """A result descriptor defined on the exception class itself is not invoked or shadowed by attach_result_to_exception()."""
    descriptor_calls = []

    class ResultDescriptor:
        def __get__(self, _instance, _owner):
            return 'class-level-result'

        def __set__(self, _instance, _value):
            descriptor_calls.append('set-called')

    class ResultDescriptorError(RuntimeError):
        result = ResultDescriptor()

    error = ResultDescriptorError('boom')
    result = SubprocessResult()

    _run_module.attach_result_to_exception(error, result)

    assert descriptor_calls == []
    assert 'result' not in error.__dict__
    assert error.result == 'class-level-result'


def test_attach_result_to_exception_preserves_inherited_result_descriptor():
    """A result descriptor inherited from a base exception class is also preserved and must not be shadowed on the instance."""
    descriptor_calls = []

    class ResultDescriptor:
        def __get__(self, _instance, _owner):
            return 'inherited-class-level-result'

        def __set__(self, _instance, _value):
            descriptor_calls.append('set-called')

    class BaseResultDescriptorError(RuntimeError):
        result = ResultDescriptor()

    class ChildResultDescriptorError(BaseResultDescriptorError):
        pass

    error = ChildResultDescriptorError('boom')
    result = SubprocessResult()

    _run_module.attach_result_to_exception(error, result)

    assert descriptor_calls == []
    assert 'result' not in error.__dict__
    assert error.result == 'inherited-class-level-result'


def test_raise_background_failure_ignores_wait_oserror_and_preserves_original_exception():
    """raise_background_failure ignores process.wait() OSError, fills the result, and re-raises the original error."""
    class FakeProcess:
        def __init__(self):
            self.returncode = None

        def poll(self):
            return None

        def kill(self):
            return None

        def wait(self):
            raise OSError('wait exploded')

    class DummyThread:
        def join(self):
            return None

    process = FakeProcess()
    reader_threads = _run_module._ReaderThreads(stdout=DummyThread(), stderr=DummyThread(), process_waiter=DummyThread())
    state = _run_module._ExecutionState()
    state.stdout_buffer.append('partial-out')
    state.stderr_buffer.append('partial-err')
    error = RuntimeError('original background failure')

    with pytest.raises(RuntimeError, match='original background failure') as exc_info:
        _run_module.raise_background_failure(process, reader_threads, state, error)

    assert exc_info.value is error
    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert exc_info.value.result.stdout == 'partial-out'  # type: ignore[attr-defined]
    assert exc_info.value.result.stderr == 'partial-err'  # type: ignore[attr-defined]
    assert exc_info.value.result.returncode == 1  # type: ignore[attr-defined]


def test_fill_result_uses_one_as_fallback_returncode_when_process_returncode_is_none():
    """fill_result() converts a missing process returncode to exactly 1 while preserving collected stdout/stderr buffers."""
    state = _run_module._ExecutionState()
    state.stdout_buffer.append('partial-out')
    state.stderr_buffer.append('partial-err')

    _run_module.fill_result(state, None)

    assert state.result.stdout == 'partial-out'
    assert state.result.stderr == 'partial-err'
    assert state.result.returncode == 1


def test_fill_result_clears_killed_flag_after_successful_completion():
    """fill_result() normalizes a successful returncode back to killed_by_token=False if a timeout thread set it earlier."""
    state = _run_module._ExecutionState()
    state.result.killed_by_token = True

    _run_module.fill_result(state, 0)

    assert state.result.returncode == 0
    assert state.result.killed_by_token is False


def test_slow_stdout_callback_does_not_prevent_completion():
    """A slow stdout callback may delay line handling, but it does not prevent the command from completing."""
    seen: List[str] = []

    def callback(text: str):
        time.sleep(0.05)
        seen.append(text)

    result = run(sys.executable, '-c', 'print("hello")', split=False, stdout_callback=callback)

    assert result.returncode == 0
    assert seen == ['hello\n']


def test_callback_that_prints_does_not_deadlock():
    """A callback that prints to stdout should not deadlock the reader threads."""
    def callback(text: str):
        print(text, end='')  # noqa: T201

    result = run(sys.executable, '-c', 'print("hello")', split=False, stdout_callback=callback, catch_output=False)

    assert result.returncode == 0


@pytest.mark.parametrize(
    ('run_kwargs', 'command'),
    [
        ({'stdout_callback': 1}, 'print("hello")'),
        ({'stderr_callback': 1}, 'import sys; sys.stderr.write("hello")'),
    ],
)
def test_callbacks_must_be_callable(run_kwargs, command):
    """stdout_callback and stderr_callback must be callable objects, otherwise run() raises TypeError."""
    with pytest.raises(TypeError):
        run(sys.executable, '-c', command, split=False, **run_kwargs)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ('callback_kwarg', 'command', 'expected_stdout', 'expected_stderr'),
    [
        (
            'stdout_callback',
            'print("hello")',
            'hello\n',
            '',
        ),
        (
            'stderr_callback',
            'import sys; sys.stderr.write("hello\\n")',
            '',
            'hello\n',
        ),
    ],
)
def test_catch_output_true_bypasses_callbacks_entirely(
    callback_kwarg,
    command,
    expected_stdout,
    expected_stderr,
    assert_no_suby_thread_leaks,
):
    """With catch_output=True, custom callbacks are bypassed entirely and output is only stored in the result."""
    seen: List[str] = []

    def callback(text: str) -> None:
        seen.append(text)
        raise RuntimeError('callback should not be called')

    with assert_no_suby_thread_leaks():
        result = run(
            sys.executable,
            '-c',
            command,
            split=False,
            catch_output=True,
            catch_exceptions=True,
            **{callback_kwarg: callback},
        )

    assert seen == []
    assert result.stdout == expected_stdout
    assert result.stderr == expected_stderr


@pytest.mark.parametrize(
    ('timeout', 'expected_error', 'error_message'),
    [
        (-1, ValueError, re.escape('You cannot specify a timeout less than zero.')),
        (float('nan'), ValueError, re.escape('You cannot specify NaN or infinite timeout values.')),
        (float('inf'), ValueError, re.escape('You cannot specify NaN or infinite timeout values.')),
        ('1', (TypeError, ValueError), None),
    ],
)
def test_invalid_timeout_values_are_rejected(timeout, expected_error, error_message):
    """Negative, NaN, infinite, and non-numeric timeout values are rejected before process execution."""
    with pytest.raises(expected_error, match=error_message):
        run(sys.executable, '-c', 'print("ok")', split=False, timeout=timeout, catch_output=True)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    'run_kwargs',
    [
        {},
        {'timeout': 0.5},
    ],
)
def test_non_cancelling_custom_token_is_handled(run_kwargs):
    """A custom token whose __bool__ never requests cancellation lets the command complete normally."""
    class NeverCancelsToken(AbstractToken):
        def _superpower(self) -> bool:
            return True

        def _get_superpower_exception_message(self) -> str:
            return 'never cancels'

        def _text_representation_of_superpower(self) -> str:
            return 'never cancels'

        def __bool__(self) -> bool:
            return True

        def __iadd__(self, other: object) -> 'NeverCancelsToken':
            return self

        def check(self):
            return None

    token = NeverCancelsToken()
    result = run(sys.executable, '-c', 'print("ok")', split=False, token=token, catch_output=True, **run_kwargs)

    assert result.returncode == 0


@pytest.mark.parametrize(
    ('use_event_driven_timeout', 'token_factory', 'expected_should_poll_manually', 'expected_poll_timeout_seconds'),
    [
        (False, DefaultToken, False, None),
        (True, DefaultToken, False, None),
        (False, SimpleToken, True, 0.0001),
        (True, SimpleToken, False, None),
    ],
)
def test_manual_token_polling_helpers_distinguish_default_and_custom_token_paths(
    use_event_driven_timeout,
    token_factory,
    expected_should_poll_manually,
    expected_poll_timeout_seconds,
):
    """Manual token polling is enabled only for custom tokens without event-driven timeout, and then the wake timeout is exactly 0.0001 seconds."""
    token = token_factory()

    assert _run_module.should_poll_token_manually(use_event_driven_timeout, token) is expected_should_poll_manually
    assert _run_module.get_manual_token_poll_timeout_seconds(use_event_driven_timeout, token) == expected_poll_timeout_seconds


def test_condition_token_with_unsuppressed_exception_raises_on_bool_before_run():
    """With suppress_exceptions=False, evaluating bool(token) propagates the token-condition exception."""
    def boom() -> bool:
        raise RuntimeError('token function exploded')

    token = ConditionToken(boom, suppress_exceptions=False)

    with pytest.raises(RuntimeError, match='token function exploded'):
        bool(token)


def test_condition_token_with_unsuppressed_exception_is_not_swallowed_by_run(assert_no_suby_thread_leaks):
    """With suppress_exceptions=False, run() propagates the token-condition exception and attaches a result."""
    def boom() -> bool:
        raise RuntimeError('silent token exploded')

    token = ConditionToken(boom, suppress_exceptions=False)

    start = time.perf_counter()
    with assert_no_suby_thread_leaks(), pytest.raises(RuntimeError, match='silent token exploded') as exc_info:
        run(sys.executable, '-c', 'import time; time.sleep(5)', split=False, token=token)
    elapsed = time.perf_counter() - start

    assert elapsed < _PROMPT_EXCEPTION_SECONDS
    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert exc_info.value.result.stdout == ''  # type: ignore[attr-defined]
    assert exc_info.value.result.stderr == ''  # type: ignore[attr-defined]
    assert isinstance(exc_info.value.result.returncode, int)  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    'delayed_error',
    [
        False,
        True,
    ],
)
def test_silent_process_timeout_and_token_error_raise_one_of_expected_exceptions(delayed_error, assert_no_suby_thread_leaks):
    """A process that produces no output raises either the token callback's RuntimeError or TimeoutCancellationError.

    delayed_error=False means the token condition raises immediately. delayed_error=True means it returns False a
    few times first, so timeout cancellation can win that race before the token error is observed.
    """
    calls = 0

    def token_condition() -> bool:
        nonlocal calls
        calls += 1
        if delayed_error and calls < 4:
            return False
        raise RuntimeError('token function exploded')

    token = ConditionToken(token_condition, suppress_exceptions=False)

    start = time.perf_counter()
    with assert_no_suby_thread_leaks(), pytest.raises((RuntimeError, TimeoutCancellationError)) as exc_info:
        run(
            sys.executable,
            '-c',
            'import time; time.sleep(5)',
            split=False,
            token=token,
            timeout=0.05,
        )
    elapsed = time.perf_counter() - start

    assert elapsed < _PROMPT_EXCEPTION_SECONDS
    result = cast(Any, exc_info.value).result

    assert isinstance(result, SubprocessResult)
    assert result.stdout == ''
    assert result.stderr == ''
    assert isinstance(result.returncode, int)


@pytest.mark.parametrize(
    ('command', 'expected_non_empty_stream', 'expected_empty_stream'),
    [
        (
            'import time\n'
            'from pathlib import Path\n'
            'print(0, flush=True)\n'
            'Path({marker_file}).touch()\n'
            'for i in range(1, 1000):\n'
            ' print(i, flush=True)\n'
            ' time.sleep(0.01)',
            'stdout',
            'stderr',
        ),
        (
            'import sys, time\n'
            'from pathlib import Path\n'
            'sys.stderr.write("0\\n")\n'
            'sys.stderr.flush()\n'
            'Path({marker_file}).touch()\n'
            'for i in range(1, 1000):\n'
            ' sys.stderr.write(f"{{i}}\\n")\n'
            ' sys.stderr.flush()\n'
            ' time.sleep(0.01)',
            'stderr',
            'stdout',
        ),
    ],
)
def test_token_cancellation_with_active_output_preserves_partial_output(
    command,
    expected_non_empty_stream,
    expected_empty_stream,
    tmp_path,
    assert_no_suby_thread_leaks,
):
    """Token cancellation keeps output produced after the child confirms that it has started writing."""
    marker_file = tmp_path / 'output-started.marker'
    cancellation_started_at: List[float] = []

    def should_cancel() -> bool:
        if not marker_file.exists():
            return False
        if not cancellation_started_at:
            cancellation_started_at.append(time.perf_counter())
            return False
        return time.perf_counter() - cancellation_started_at[0] > 0.2

    token = ConditionToken(should_cancel)

    with assert_no_suby_thread_leaks():
            result = run(
                sys.executable,
                '-c',
                command.format(marker_file=json.dumps(str(marker_file))),
                split=False,
                token=token,
                catch_exceptions=True,
                catch_output=True,
            )

    assert marker_file.exists()
    assert result.returncode != 0
    assert result.killed_by_token is True
    assert isinstance(getattr(result, expected_non_empty_stream), str)
    assert getattr(result, expected_non_empty_stream) != ''
    assert '0\n' in getattr(result, expected_non_empty_stream)
    assert isinstance(getattr(result, expected_empty_stream), str)


@pytest.mark.parametrize(
    ('command', 'expected_non_empty_stream', 'expected_empty_stream'),
    [
        (
            'import sys, time; sys.stdout.write("x" * 100000); sys.stdout.flush(); time.sleep(5)',
            'stdout',
            'stderr',
        ),
        (
            'import sys, time; sys.stderr.write("x" * 100000); sys.stderr.flush(); time.sleep(5)',
            'stderr',
            'stdout',
        ),
    ],
)
def test_token_cancellation_during_output_chunk_without_newline_still_kills_process(
    command,
    expected_non_empty_stream,
    expected_empty_stream,
    assert_no_suby_thread_leaks,
):
    """Token cancellation still kills a process while a reader thread is blocked on a large chunk without newlines.

    The already-written chunk may or may not be preserved depending on whether cancellation wins before readline()
    returns the unterminated data.
    """
    start = time.perf_counter()
    token = ConditionToken(lambda: time.perf_counter() - start > 0.5)

    with assert_no_suby_thread_leaks():
        result = run(
            sys.executable,
            '-c',
            command,
            split=False,
            token=token,
            catch_exceptions=True,
            catch_output=True,
        )

    elapsed = time.perf_counter() - start

    assert elapsed >= 0.5
    assert elapsed < 4
    assert result.returncode != 0
    assert result.killed_by_token is True
    assert isinstance(getattr(result, expected_non_empty_stream), str)
    assert '\n' not in getattr(result, expected_non_empty_stream)
    assert isinstance(getattr(result, expected_empty_stream), str)


def test_tiny_timeout_on_fast_process_is_still_well_formed():
    """Even an extremely small timeout returns a fully populated timeout result attached to the exception."""
    with pytest.raises(TimeoutCancellationError) as exc_info:
        run(sys.executable, '-c', 'import time; time.sleep(0.01)', split=False, timeout=0.000001, catch_output=True)

    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert exc_info.value.result.stdout == ''  # type: ignore[attr-defined]
    assert exc_info.value.result.stderr == ''  # type: ignore[attr-defined]
    assert exc_info.value.result.returncode != 0  # type: ignore[attr-defined]
    assert exc_info.value.result.killed_by_token is True  # type: ignore[attr-defined]


def test_stdout_without_newline_is_not_lost():
    """A final stdout chunk without a trailing newline is still preserved in result.stdout."""
    result = run(sys.executable, '-c', 'import sys; sys.stdout.write("hello")', split=False, catch_output=True)

    assert result.stdout == 'hello'


@pytest.mark.parametrize(
    ('command', 'expected_stream'),
    [
        ('for i in range(5000): print(i)', 'stdout'),
        ('import sys\nfor i in range(5000): sys.stderr.write(f"{i}\\n")', 'stderr'),
    ],
)
def test_large_output_is_collected_fully(command, expected_stream):
    """Large stdout or stderr streams are collected from the first line through the last line."""
    result = run(
        sys.executable,
        '-c',
        command,
        split=False,
        catch_exceptions=True,
        catch_output=True,
    )

    captured_output = getattr(result, expected_stream)

    assert captured_output is not None
    assert captured_output.startswith('0\n')
    assert captured_output.endswith('4999\n')


def test_stderr_heavy_process_does_not_starve_stdout():
    """Heavy stderr traffic should not starve stdout, meaning stdout is still drained and captured."""
    result = run(
        sys.executable,
        '-c',
        'import sys; print("out"); [sys.stderr.write("err\\n") for _ in range(1000)]',
        split=False,
        catch_output=True,
    )

    assert result.stdout is not None
    assert 'out\n' in result.stdout


def test_interleaved_stdout_and_stderr_are_both_collected():
    """Interleaved stdout and stderr output are both collected without losing lines from either stream."""
    result = run(
        sys.executable,
        '-c',
        'import sys\nfor i in range(10):\n print(f"out-{i}")\n sys.stderr.write(f"err-{i}\\n")',
        split=False,
        catch_output=True,
    )

    assert result.stdout is not None
    assert result.stderr is not None
    assert 'out-0\n' in result.stdout
    assert 'err-0\n' in result.stderr


@pytest.mark.parametrize('fd', [1, 2])
def test_non_utf8_output_is_rejected_consistently(fd):
    """Non-UTF-8 stdout/stderr bytes are rejected consistently under explicit UTF-8 strict decoding."""
    with pytest.raises(UnicodeDecodeError):
        run(sys.executable, '-c', f'import os; os.write({fd}, b"\\xff\\xfe\\xfd")', split=False, catch_output=True)


@pytest.mark.parametrize(
    ('fd', 'result_attr'),
    [
        (1, 'stdout'),
        (2, 'stderr'),
    ],
)
def test_utf8_output_accepts_non_ascii_text(fd, result_attr):
    """Explicit UTF-8 decoding preserves non-ASCII stdout/stderr bytes emitted by the child process."""
    result = run(
        sys.executable,
        '-c',
        f'import os; os.write({fd}, "привет\\n".encode("utf-8"))',
        split=False,
        catch_output=True,
    )

    assert getattr(result, result_attr) == 'привет\n'
    assert result.returncode == 0


def test_complex_kwargs_combination_is_well_formed():
    """run() should still succeed when all supported keyword arguments except timeout are passed together."""
    logger = MemoryLogger()
    result = run(
        sys.executable,
        '-c',
        'print("ok")',
        split=False,
        catch_output=True,
        catch_exceptions=True,
        logger=logger,
        stdout_callback=lambda _: None,
        stderr_callback=lambda _: None,
        token=DefaultToken(),
    )

    assert result.returncode == 0


def test_complex_kwargs_combination_with_timeout_integration_is_well_formed():
    """run() wires timeout infrastructure together with the rest of the keyword API and joins the timeout thread on the event-driven path."""
    class JoinedThread:
        def __init__(self):
            self.joined = False

        def join(self):
            self.joined = True

    logger = MemoryLogger()
    timeout_thread = JoinedThread()

    with patch.object(_run_module, 'has_event_driven_wait', return_value=True), \
         patch.object(_run_module, 'run_timeout_thread', return_value=timeout_thread) as mock_timeout_thread:
        result = run(
            sys.executable,
            '-c',
            'print("ok")',
            split=False,
            catch_output=True,
            catch_exceptions=True,
            logger=logger,
            stdout_callback=lambda _: None,
            stderr_callback=lambda _: None,
            timeout=1,
            token=DefaultToken(),
        )

    mock_timeout_thread.assert_called_once()
    called_process, called_timeout, called_result = mock_timeout_thread.call_args.args
    assert called_process.args == [sys.executable, '-c', 'print("ok")']  # type: ignore[attr-defined]
    assert called_timeout == 1
    assert called_result is result
    assert timeout_thread.joined is True
    assert isinstance(result, SubprocessResult)
    assert isinstance(result.stdout, str)
    assert isinstance(result.stderr, str)


def test_error_paths_return_consistent_subprocess_result_shapes():
    """Startup failure, runtime failure, and timeout cancellation all return SubprocessResult objects with the same field types."""
    results: List[SubprocessResult] = []

    results.append(run('definitely_missing_command_for_suby_shape', catch_exceptions=True))
    results.append(run(sys.executable, '-c', 'import sys; sys.exit(1)', split=False, catch_exceptions=True, catch_output=True))
    results.append(run(sys.executable, '-c', 'import time; time.sleep(1)', split=False, timeout=0, catch_exceptions=True, catch_output=True))

    for result in results:

        assert isinstance(result.stdout, str)
        assert isinstance(result.stderr, str)
        assert isinstance(result.returncode, int)
        assert isinstance(result.killed_by_token, bool)


def test_parallel_runs_with_shared_callback_do_not_drop_events():
    """Concurrent run() calls sharing one callback still deliver one callback event per subprocess."""
    seen: List[str] = []

    def worker(index: int):
        run(sys.executable, '-c', f'print({index})', split=False, stdout_callback=seen.append)

    threads = [Thread(target=worker, args=(index,)) for index in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(seen) == 10


def test_many_short_processes_complete_without_state_corruption():
    """Many sequential short-lived subprocesses should each return the expected stdout without corrupting state."""
    for _ in range(100):
        result = run(sys.executable, '-c', 'print("ok")', split=False, catch_output=True)

        assert result.stdout == 'ok\n'
