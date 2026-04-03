import importlib
import json
import re
import subprocess
import sys
import time
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from os import environ
from pathlib import Path, PurePath
from threading import Thread
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

_WINDOWS_MAXIMUM_COMMAND_LINE_LENGTH = 32767


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


@pytest.mark.parametrize(
    ('command', 'run_kwargs'),
    [
        ((Path(sys.executable), '-c "print(\'kek\')"'), {}),
        ((sys.executable, '-c "print(\'kek\')"'), {}),
        (('python -c "print(\'kek\')"',), {}),
        ((sys.executable, '-c "print(\'kek\')"'), {'token': SimpleToken()}),
        (('python -c "print(\'kek\')"',), {'token': SimpleToken()}),
    ],
)
def test_normal_way(command, run_kwargs, assert_no_suby_thread_leaks):
    """Checks that normal way."""
    with assert_no_suby_thread_leaks():
        result = run(*command, **run_kwargs)

    assert result.stdout == 'kek\n'
    assert result.stderr == ''
    assert result.returncode == 0


@pytest.mark.parametrize(
    'command',
    [
        (sys.executable, '-c "import sys; sys.stderr.write(\'kek\')"'),
        ('python -c "import sys; sys.stderr.write(\'kek\')"',),
    ],
)
def test_stderr_catching(command, assert_no_suby_thread_leaks):
    """Checks that stderr catching."""
    with assert_no_suby_thread_leaks():
        result = run(*command)

    assert result.stdout == ''
    assert result.stderr == 'kek'
    assert result.returncode == 0


@pytest.mark.parametrize(
    'command',
    [
        (sys.executable, '-c "raise ValueError"'),
        ('python -c "raise ValueError"',),
    ],
)
def test_catch_exception(command, assert_no_suby_thread_leaks):
    """Checks that catch exception."""
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
def test_timeout(command, assert_no_suby_thread_leaks):
    """Checks that timeout."""
    sleep_time = 100000
    timeout = 0.001
    command = [subcommand.format(sleep_time=sleep_time) if isinstance(subcommand, str) else subcommand for subcommand in command]

    start_time = perf_counter()
    with assert_no_suby_thread_leaks():
        result = run(*command, timeout=timeout, catch_exceptions=True)
    end_time = perf_counter()

    assert result.returncode != 0
    assert result.stdout == ''
    assert result.stderr == ''

    assert (end_time - start_time) < sleep_time
    assert (end_time - start_time) >= timeout


@pytest.mark.parametrize(
    'command',
    [
        (sys.executable, '-c "import time; time.sleep({sleep_time})"'),
        ('python -c "import time; time.sleep({sleep_time})"',),
    ],
)
def test_timeout_without_catching_exception(command, assert_no_suby_thread_leaks):
    """Checks that timeout without catching exception."""
    sleep_time = 100000
    timeout = 0.001
    command = [subcommand.format(sleep_time=sleep_time) if isinstance(subcommand, str) else subcommand for subcommand in command]

    with assert_no_suby_thread_leaks(), pytest.raises(TimeoutCancellationError):
        run(*command, timeout=timeout)

    start_time = perf_counter()
    with assert_no_suby_thread_leaks(), pytest.raises(TimeoutCancellationError) as exc_info:
        run(*command, timeout=timeout)
    end_time = perf_counter()

    assert exc_info.value.result.stdout == ''
    assert exc_info.value.result.stderr == ''
    assert exc_info.value.result.returncode != 0

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
    """Checks that exception in subprocess without catching."""
    with assert_no_suby_thread_leaks(), pytest.raises(RunningCommandError, match=re.escape(error_text)):
        run(*command)

    with assert_no_suby_thread_leaks(), pytest.raises(RunningCommandError) as exc_info:
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
def test_not_catching_output(command, assert_no_suby_thread_leaks):
    """Checks that not catching output."""
    stderr_buffer = StringIO()
    stdout_buffer = StringIO()

    with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer), assert_no_suby_thread_leaks():
        result = run(*command, catch_output=False)

        stderr = stderr_buffer.getvalue()
        stdout = stdout_buffer.getvalue()

        assert result.returncode == 0
        assert stderr == 'kek2'
        assert stdout == 'kek1'


@pytest.mark.parametrize(
    'command',
    [
        (sys.executable, '-c "print(\'kek1\', end=\'\'); import sys; sys.stderr.write(\'kek2\')"'),
        ('python -c "print(\'kek1\', end=\'\'); import sys; sys.stderr.write(\'kek2\')"',),
    ],
)
def test_catching_output(command, assert_no_suby_thread_leaks):
    """Checks that catching output."""
    stderr_buffer = StringIO()
    stdout_buffer = StringIO()

    with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
        with assert_no_suby_thread_leaks():
            result = run(*command, catch_output=True)

        assert result.returncode == 0
        assert stderr_buffer.getvalue() == ''
        assert stdout_buffer.getvalue() == ''


@pytest.mark.parametrize(
    ('command', 'first_log_message', 'second_log_message'),
    [
        ((sys.executable, '-c "print(\'kek\', end=\'\')"'), f'The beginning of the execution of the command "{sys.executable} -c "print(\'kek\', end=\'\')"".', f'The command "{sys.executable} -c "print(\'kek\', end=\'\')"" has been successfully executed.'),
        (('python -c "print(\'kek\', end=\'\')"',), 'The beginning of the execution of the command "python -c "print(\'kek\', end=\'\')"".', 'The command "python -c "print(\'kek\', end=\'\')"" has been successfully executed.'),
    ],
)
def test_logging_normal_way(command, first_log_message, second_log_message):
    """Checks that logging normal way."""
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
    """Checks that logging with expired timeout."""
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
def test_logging_with_exception(command, first_log_message, second_log_message):
    """Checks that logging with exception."""
    logger = MemoryLogger()

    run(*command, logger=logger, catch_exceptions=True, catch_output=True)

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
def test_logging_with_exception_without_catching_exceptions(command, first_log_message, second_log_message):
    """Checks that logging with exception without catching exceptions."""
    logger = MemoryLogger()

    with pytest.raises(RunningCommandError):
        run(*command, logger=logger, catch_output=True)

    assert len(logger.data.info) == 1
    assert len(logger.data.error) == 1

    assert logger.data.info[0].message == first_log_message
    assert logger.data.error[0].message == second_log_message


@pytest.mark.parametrize(
    'command',
    [
        (sys.executable, '-c "import time; time.sleep({sleep_time})"'),
        ('python -c "import time; time.sleep({sleep_time})"',),
    ],
)
def test_only_token(command, assert_no_suby_thread_leaks):
    """Checks that only token."""
    sleep_time = 100000
    timeout = 0.1
    command = [subcommand.format(sleep_time=sleep_time) if isinstance(subcommand, str) else subcommand for subcommand in command]

    start_time = perf_counter()
    token = ConditionToken(lambda: perf_counter() - start_time > timeout)

    with assert_no_suby_thread_leaks():
        result = run(*command, catch_exceptions=True, token=token)

    end_time = perf_counter()

    assert result.returncode != 0
    assert result.stdout == ''
    assert result.stderr == ''
    assert result.killed_by_token == True

    assert end_time - start_time >= timeout
    assert end_time - start_time < sleep_time


@pytest.mark.parametrize(
    'command',
    [
        (sys.executable, '-c "import time; time.sleep({sleep_time})"'),
        ('python -c "import time; time.sleep({sleep_time})"',),
    ],
)
def test_only_token_without_catching(command, assert_no_suby_thread_leaks):
    """Checks that only token without catching."""
    sleep_time = 100000
    timeout = 0.1
    command = [subcommand.format(sleep_time=sleep_time) if isinstance(subcommand, str) else subcommand for subcommand in command]

    start_time = perf_counter()
    token = ConditionToken(lambda: perf_counter() - start_time > timeout)

    with assert_no_suby_thread_leaks(), pytest.raises(ConditionCancellationError) as exc_info:
        run(*command, token=token)

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
    """Checks that token plus timeout without catching raises expected cancellation."""
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
    """Checks that replace output callback."""
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
    """Checks that pass wrong positional argument."""
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
def test_multiple_args_without_split(command):
    """Checks that multiple args without split."""
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
    """Checks that wrong command."""
    with pytest.raises(WrongCommandError, match=match(exception_message)):
        run(*command)


def test_empty_command_raises_wrong_command_error():
    """Checks that empty command raises WrongCommandError."""
    with pytest.raises(WrongCommandError, match=match('You must pass at least one positional argument with the command to run.')):
        run()


def test_single_string_is_split_on_all_platforms():
    # Under the old Windows behavior, a single string was NOT split by shlex —
    # it was passed as one token to the subprocess, which would fail.
    # This test verifies that shlex splitting works on all platforms.
    """Checks that single string is split on all platforms."""
    result = run('python -c pass')

    assert result.returncode == 0
    assert result.stdout == ''
    assert result.stderr == ''


def test_envs_for_subprocess_are_same_as_parent():
    """Checks that environmentironment variables for subprocess are same as parent."""
    subprocess_env = json.loads(run('python -c "import os, json; print(json.dumps(dict(os.environ)))"').stdout)

    # why: https://stackoverflow.com/questions/1780483/lines-and-columns-environmental-variables-lost-in-a-script
    subprocess_env.pop('LINES', None)
    subprocess_env.pop('COLUMNS', None)

    assert subprocess_env == environ


def test_executable_path_with_backslashes_passed_as_string():
    # On Windows, sys.executable is a path like C:\Python\python.exe.
    # shlex with posix=True treats \ as an escape character, silently eating backslashes.
    # This test verifies that backslashes in paths survive shlex splitting.
    """Checks that executable path with backslashes passed as string."""
    result = run(sys.executable, '-c pass')

    assert result.returncode == 0


def test_executable_path_with_spaces_passed_as_unquoted_string_fails(tmp_path):
    # When a path containing spaces is embedded in a command string without quotes,
    # shlex splits on the space and the command fails.
    # To pass such a path correctly, it must be either quoted in the string
    # or passed as a separate Path object.
    """Checks that executable path with spaces passed as unquoted string fails."""
    space_dir = tmp_path / 'dir with space'
    space_dir.mkdir()
    script = space_dir / 'script.py'
    script.write_text('pass')

    with pytest.raises(RunningCommandError):
        # shlex splits on the space → python receives 'dir', 'with', 'space/script.py'
        # as separate arguments instead of the script path
        run(f'python {script}')


def test_argument_with_trailing_backslash(tmp_path):
    # On Windows, subprocess uses list2cmdline to convert the arg list back into a
    # command string for CreateProcess. list2cmdline wraps args that contain spaces in
    # double quotes. If such an arg ends in \, the result is "arg\" — the \" is
    # interpreted by the Windows parser as an escaped quote, not a closing quote,
    # which mangles the argument and everything that follows.
    """Checks that argument with trailing backslash."""
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
    # sys.executable on Windows is a path like C:\Python\python.exe.
    # With double_backslash=True (the default on Windows), backslashes survive shlex splitting.
    """Checks that double backslash enabled by default on windows."""
    result = run(f'{sys.executable} -c "print(\'kek\')"', catch_output=True)

    assert result.returncode == 0
    assert result.stdout == 'kek\n'


@pytest.mark.skipif(sys.platform != 'win32', reason='Windows-only test')
def test_double_backslash_can_be_disabled_on_windows():
    # With double_backslash=False, shlex eats the backslashes in the path, making the executable path invalid.
    # r'C:\fake\python.exe' → shlex in posix mode: \f→f, \p→p → 'C:fakepython.exe'
    """Checks that double backslash can be disabled on windows."""
    with pytest.raises(RunningCommandError, match=match('Error when executing the command "C:fakepython.exe -c pass".')):
        run(r'C:\fake\python.exe -c pass', double_backslash=False)


@pytest.mark.parametrize(
    ('run_kwargs', 'expected_output'),
    [
        ({}, 'hello world'),
        ({'double_backslash': True}, 'hello\\'),
    ],
)
@pytest.mark.skipif(sys.platform == 'win32', reason='non-Windows test')
def test_double_backslash_argument_processing_on_non_windows(run_kwargs, expected_output):
    """Checks that double backslash argument processing on non windows."""
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
    """Checks that run returns SubprocessResult."""
    result = run('python -c pass')

    assert isinstance(result, SubprocessResult)


def test_missing_command_with_catch_exceptions_returns_filled_result():
    """Checks that missing command with catch exceptions returns filled result."""
    result = run('command_that_definitely_does_not_exist_12345', catch_exceptions=True)

    assert result.stdout == ''
    assert result.stderr != ''
    if sys.platform != 'win32':
        assert 'command_that_definitely_does_not_exist_12345' in result.stderr
    assert result.returncode == 1
    assert result.killed_by_token is False


def test_missing_command_without_catch_exceptions_attaches_filled_result():
    """Checks that missing command without catch exceptions attaches filled result."""
    with pytest.raises(RunningCommandError) as exc_info:
        run('command_that_definitely_does_not_exist_12345')

    assert exc_info.value.result.stdout == ''
    assert exc_info.value.result.stderr != ''
    if sys.platform != 'win32':
        assert 'command_that_definitely_does_not_exist_12345' in exc_info.value.result.stderr
    assert exc_info.value.result.returncode == 1
    assert exc_info.value.result.killed_by_token is False


def test_missing_command_stderr_shape_matches_current_platform():
    """POSIX includes the missing command in stderr, while Windows may only expose WinError text for now."""
    missing_command = 'command_that_definitely_does_not_exist_12345'
    result = run(missing_command, catch_exceptions=True)

    assert result.stdout == ''
    assert result.stderr != ''
    if sys.platform == 'win32':
        assert 'WinError' in result.stderr
    else:
        assert missing_command in result.stderr
    assert result.returncode == 1
    assert result.killed_by_token is False


def test_missing_command_with_catch_exceptions_logs_exception():
    """Checks that missing command with catch exceptions logs exception."""
    logger = MemoryLogger()

    result = run('command_that_definitely_does_not_exist_12345', catch_exceptions=True, logger=logger)

    assert result.returncode == 1
    assert len(logger.data.info) == 1
    assert len(logger.data.error) == 0
    assert len(logger.data.exception) == 1
    assert logger.data.info[0].message == 'The beginning of the execution of the command "command_that_definitely_does_not_exist_12345".'
    assert logger.data.exception[0].message == 'Error when executing the command "command_that_definitely_does_not_exist_12345".'


def test_missing_command_original_popen_raises_filenotfounderror():
    """Checks that missing command original popen raises FileNotFoundError."""
    with pytest.raises(RunningCommandError) as exc_info:
        run('command_that_definitely_does_not_exist_12345')

    assert isinstance(exc_info.value.__cause__, FileNotFoundError)


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX-only exec format semantics')
def test_exec_format_error_original_popen_raises_plain_oserror(tmp_path, assert_no_suby_thread_leaks):
    """Checks that exec format error original popen raises plain OSError."""
    script = tmp_path / 'script-without-shebang'
    script.write_text('echo hello\n')
    script.chmod(0o755)

    with assert_no_suby_thread_leaks(), pytest.raises(RunningCommandError) as exc_info:
        run(str(script))

    assert type(exc_info.value.__cause__) is OSError
    assert 'Exec format error' in str(exc_info.value.__cause__)


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX-only permission semantics')
def test_permission_error_with_catch_exceptions_returns_filled_result(tmp_path):
    """Checks that permission error with catch exceptions returns filled result."""
    script = tmp_path / 'script.sh'
    script.write_text('echo hello')
    script.chmod(0o644)

    result = run(str(script), catch_exceptions=True)

    assert result.stdout == ''
    assert result.stderr != ''
    assert 'Permission denied' in result.stderr
    assert result.returncode == 1
    assert result.killed_by_token is False


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX-only permission semantics')
def test_permission_error_without_catch_exceptions_attaches_filled_result(tmp_path):
    """Checks that permission error without catch exceptions attaches filled result."""
    script = tmp_path / 'script.sh'
    script.write_text('echo hello')
    script.chmod(0o644)

    with pytest.raises(RunningCommandError) as exc_info:
        run(str(script))

    assert exc_info.value.result.stdout == ''
    assert exc_info.value.result.stderr != ''
    assert 'Permission denied' in exc_info.value.result.stderr
    assert exc_info.value.result.returncode == 1
    assert exc_info.value.result.killed_by_token is False


def test_multiple_strings_split_independently():
    # 'python -c' splits to ['python', '-c'], '"print(777)"' splits to ['print(777)']
    # each string is split independently and the results are concatenated
    """Checks that multiple strings split independently."""
    result = run('python -c', '"print(777)"', catch_output=True)

    assert result.returncode == 0
    assert result.stdout == '777\n'


def test_argument_with_space_passed_with_split_false():
    # with split=False the string is passed as-is, spaces are not treated as delimiters
    """Checks that argument with space passed with split=False."""
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
        'RunningCommandError',
        'WrongCommandError',
        'TimeoutCancellationError',
    ],
)
def test_errors_are_importable_from_suby(error_name):
    """Checks that errors are importable from suby."""
    assert hasattr(suby, error_name)


def test_already_cancelled_simple_token_kills_process():
    """Checks that already cancelled SimpleToken kills process."""
    token = SimpleToken()
    token.cancel()

    result = run('python -c "import time; time.sleep(100)"', token=token, catch_exceptions=True)

    assert result.killed_by_token == True
    assert result.returncode != 0


def test_already_cancelled_simple_token_raises():
    """Checks that already cancelled SimpleToken raises."""
    token = SimpleToken()
    token.cancel()

    with pytest.raises(CancellationError):
        run('python -c "import time; time.sleep(100)"', token=token)


def test_immediately_satisfied_condition_token_kills_process():
    """Checks that immediately satisfied ConditionToken kills process."""
    token = ConditionToken(lambda: True)

    result = run('python -c "import time; time.sleep(100)"', token=token, catch_exceptions=True)

    assert result.killed_by_token == True
    assert result.returncode != 0


def test_timeout_exception_message():
    """Checks that timeout exception message."""
    with pytest.raises(TimeoutCancellationError, match=match('The timeout of 1 seconds has expired.')):
        run('python -c "import time; time.sleep(100)"', timeout=1)


def test_large_output():
    """Checks that large output."""
    lines = 1000

    result = run(f'python -c "for i in range({lines}): print(i)"', catch_output=True)

    assert result.returncode == 0
    assert result.stdout == ''.join(f'{i}\n' for i in range(lines))


def test_parallel_runs():
    """Checks that parallel runs."""
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
    """Checks that malformed commands are rejected."""
    with pytest.raises(WrongCommandError):
        run(*command)


def test_very_long_command_string_is_handled():
    """Checks that very long command string is handled.

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
    assert exc_info.value.result.stderr != ''
    assert exc_info.value.result.returncode == 1
    assert exc_info.value.result.killed_by_token is False


def test_very_large_output_is_handled_without_a_huge_command_line():
    """Checks that very large output is captured when the command line itself stays short."""
    payload_size = 100_000
    result = run(sys.executable, '-c', f'print("x" * {payload_size})', split=False, catch_output=True)

    assert result.stdout == 'x' * payload_size + '\n'


def test_very_large_stderr_output_is_handled_without_a_huge_command_line():
    """Checks that very large stderr output is captured when the command line itself stays short."""
    payload_size = 100_000
    result = run(
        sys.executable,
        '-c',
        f'import sys; sys.stderr.write("x" * {payload_size})',
        split=False,
        catch_output=True,
    )

    assert result.stderr == 'x' * payload_size


def test_command_with_nul_byte_is_rejected_consistently():
    """Checks that command with NUL byte is rejected consistently."""
    with pytest.raises((RunningCommandError, ValueError)):
        run('abc\0def')


def test_empty_path_object_is_rejected_consistently():
    """Checks that empty path object is rejected consistently."""
    with pytest.raises((RunningCommandError, PermissionError)):
        run(Path(''))  # noqa: PTH201


def test_current_directory_path_object_is_rejected_consistently():
    """Checks that current directory path object is rejected consistently."""
    with pytest.raises(RunningCommandError) as exc_info:
        run(Path('.'))  # noqa: PTH201

    assert isinstance(exc_info.value.__cause__, OSError)


def test_path_with_spaces_and_special_characters_executes_via_path_object(tmp_path: Path):
    """Checks that path with spaces and special characters executes via path object."""
    script = tmp_path / 'dir with spaces #and(parens)'
    script.mkdir()
    executable = script / 'echo.py'
    executable.write_text('print("ok")')
    result = run(Path(sys.executable), executable, split=False, catch_output=True)

    assert result.stdout == 'ok\n'


def test_run_uses_dedicated_stdout_thread():
    """Checks that run uses dedicated stdout thread."""
    with patch.object(_run_module, 'run_stdout_thread', wraps=_run_module.run_stdout_thread) as wrapped:
        result = run(sys.executable, '-c', 'print("ok")', split=False, catch_output=True)

    assert result.stdout == 'ok\n'
    wrapped.assert_called_once()


def test_kill_process_if_running_ignores_process_lookup_error():
    """Checks that kill process if running ignores process lookup error."""
    class MockProcess:
        def poll(self):
            return None

        def kill(self):
            raise ProcessLookupError('already exited')

    _run_module.kill_process_if_running(MockProcess())  # type: ignore[arg-type]


def test_path_object_that_looks_like_flag_is_treated_as_plain_argument():
    """Checks that path object that looks like flag is treated as plain argument."""
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
    """Checks that non string command arguments are rejected."""
    with pytest.raises(TypeError):
        run(command)  # type: ignore[arg-type]


def test_string_like_object_is_rejected():
    """Checks that string like object is rejected."""
    class StringLikeObject:
        def __str__(self) -> str:
            return 'python'

    with pytest.raises(TypeError):
        run(StringLikeObject())  # type: ignore[arg-type]


def test_split_false_does_not_split_single_string_command():
    """Checks that split=False does not split single string command."""
    with pytest.raises(RunningCommandError):
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
    """Checks that split=False preserves argument shape."""
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
    """Checks that split=False with empty executable is rejected."""
    with pytest.raises((RunningCommandError, ValueError)):
        run('', split=False)


def test_split_false_with_path_object_still_executes():
    """Checks that split=False with path object still executes."""
    result = run(Path(sys.executable), '-c', 'print("ok")', split=False, catch_output=True)

    assert result.stdout == 'ok\n'


@pytest.mark.skipif(sys.platform != 'win32', reason='Windows-only UNC path semantics')
def test_unc_path_survives_windows_processing():
    """Checks that UNC path survives windows processing."""
    result = run(r'\\server\share\python.exe -c pass', catch_exceptions=True)

    assert result.stderr is not None


@pytest.mark.skipif(sys.platform == 'win32', reason='Non-Windows-only behavior')
def test_double_backslash_true_changes_non_windows_argument_shape():
    """Checks that double backslash true changes non windows argument shape."""
    result = run(
        sys.executable,
        '-c "import sys; print(sys.argv[1:])"',
        r'hello\ world',
        double_backslash=True,
        catch_output=True,
    )

    assert result.stdout != "['hello world']\n"


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
    """Checks that posix file startup failures are normalized via RunningCommandError."""
    script = tmp_path / filename
    script.write_text(contents)
    script.chmod(mode)

    with pytest.raises(RunningCommandError) as exc_info:
        run(str(script))

    assert isinstance(exc_info.value.__cause__, expected_cause)


def test_directory_as_executable_is_normalized():
    """Checks that directory as executable is normalized."""
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
    """Checks that missing commands are normalized."""
    with assert_no_suby_thread_leaks(), pytest.raises(RunningCommandError) as exc_info:
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
def test_callback_exceptions_bubble_up(run_kwargs, command, error_message, assert_no_suby_thread_leaks):
    """Checks that callback exceptions bubble up."""
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
    """Checks that callback exceptions kill process and attach result."""
    start = time.perf_counter()
    with pytest.raises(RuntimeError, match=error_message) as exc_info:
        run(sys.executable, '-c', command, split=False, **run_kwargs)
    elapsed = time.perf_counter() - start

    assert elapsed < 2
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
    """Checks that callback exceptions after process exit keep success returncode."""
    def callback(_: str):
        time.sleep(0.1)
        raise RuntimeError(error_message)

    start = time.perf_counter()
    with pytest.raises(RuntimeError, match=error_message) as exc_info:
        run(sys.executable, '-c', command, split=False, **{callback_kwarg: callback})
    elapsed = time.perf_counter() - start

    assert elapsed < 2
    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert exc_info.value.result.stdout == expected_stdout  # type: ignore[attr-defined]
    assert exc_info.value.result.stderr == expected_stderr  # type: ignore[attr-defined]
    assert exc_info.value.result.returncode == 0  # type: ignore[attr-defined]
    assert exc_info.value.result.killed_by_token is False  # type: ignore[attr-defined]


def test_parallel_stdout_and_stderr_callback_failures_raise_one_of_them(assert_no_suby_thread_leaks):
    """Checks that parallel stdout and stderr callback failures raise one of them."""
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
def test_timeout_and_callback_error_raise_one_of_expected_exceptions(callback_kwarg, command, error_message):
    """Checks that timeout and callback error raise one of expected exceptions.

    The kill return code check is strict only on POSIX, because Windows does not use -9 for killed processes.
    """
    returncodes = []
    killed_flags = set()

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

        assert elapsed < 2
        result = cast(Any, exc_info.value).result

        assert isinstance(result, SubprocessResult)
        assert isinstance(result.stdout, str)
        assert isinstance(result.stderr, str)
        returncodes.append(result.returncode)
        killed_flags.add(result.killed_by_token)

    for returncode in returncodes:
        _assert_kill_returncode_matches_platform(returncode)
    assert killed_flags.issubset({False, True})


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
def test_timeout_and_callback_error_after_near_exit_raise_one_of_expected_exceptions(
    callback_kwarg,
    command,
    expected_stdout,
    expected_stderr,
    error_message,
):
    """Checks that timeout and callback error after near-exit raise one of expected exceptions.

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

        assert elapsed < 2
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
    """Checks that existing result attribute on callback exception is not overwritten."""
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


def test_result_getter_failure_does_not_mask_original_exception():
    """Checks that result getter failure does not mask original exception."""
    class ExplodingResultGetterError(RuntimeError):
        @property
        def result(self) -> SubprocessResult:
            raise RuntimeError('result getter exploded')

    def callback(_: str):
        raise ExplodingResultGetterError('stdout callback exploded')

    with pytest.raises(ExplodingResultGetterError, match='stdout callback exploded'):
        run(sys.executable, '-c', 'print("hello")', split=False, stdout_callback=callback)


def test_result_setter_failure_does_not_mask_original_exception():
    """Checks that result setter failure does not mask original exception."""
    class ExplodingResultSetterError(RuntimeError):
        @property
        def result(self):
            return None

        @result.setter
        def result(self, _value: SubprocessResult):
            raise RuntimeError('result setter exploded')

    def callback(_: str):
        raise ExplodingResultSetterError('stdout callback exploded')

    with pytest.raises(ExplodingResultSetterError, match='stdout callback exploded'):
        run(sys.executable, '-c', 'print("hello")', split=False, stdout_callback=callback)


def test_result_assignment_failure_does_not_mask_original_exception():
    """Checks that result assignment failure does not mask original exception."""
    class ExplodingResultAssignmentError(RuntimeError):
        def __setattr__(self, name: str, value: object):
            if name == 'result':
                raise RuntimeError('result assignment exploded')
            super().__setattr__(name, value)

    def callback(_: str):
        raise ExplodingResultAssignmentError('stdout callback exploded')

    with pytest.raises(ExplodingResultAssignmentError, match='stdout callback exploded'):
        run(sys.executable, '-c', 'print("hello")', split=False, stdout_callback=callback)


def test_attach_result_to_exception_handles_exception_without_dict():
    """Checks that attach result to exception handles exception without dict."""
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
    """Checks that attach result to exception handles object without dict."""
    result = SubprocessResult()
    result.stdout = 'hello'
    result.stderr = ''
    result.returncode = 0

    _run_module.attach_result_to_exception(object(), result)  # type: ignore[arg-type]


def test_raise_background_failure_ignores_wait_oserror_and_preserves_original_exception():
    """Checks that raise background failure ignores wait OSError and preserves original exception."""
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


def test_slow_stdout_callback_does_not_prevent_completion():
    """Checks that slow stdout callback does not prevent completion."""
    seen: List[str] = []

    def callback(text: str):
        time.sleep(0.05)
        seen.append(text)

    result = run(sys.executable, '-c', 'print("hello")', split=False, stdout_callback=callback)

    assert result.returncode == 0
    assert seen == ['hello\n']


def test_callback_that_prints_does_not_deadlock():
    """Checks that callback that prints does not deadlock."""
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
    """Checks that callbacks must be callable."""
    with pytest.raises(TypeError):
        run(sys.executable, '-c', command, split=False, **run_kwargs)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ('run_kwargs', 'command', 'expected_stdout', 'expected_stderr'),
    [
        (
            {'stdout_callback': lambda _: (_ for _ in ()).throw(RuntimeError('stdout callback should not be called'))},
            'print("hello")',
            'hello\n',
            '',
        ),
        (
            {'stderr_callback': lambda _: (_ for _ in ()).throw(RuntimeError('stderr callback should not be called'))},
            'import sys; sys.stderr.write("hello\\n")',
            '',
            'hello\n',
        ),
    ],
)
def test_catch_output_true_suppresses_failing_callbacks(
    run_kwargs,
    command,
    expected_stdout,
    expected_stderr,
    assert_no_suby_thread_leaks,
):
    """Checks that catch output=True suppresses failing callbacks."""
    with assert_no_suby_thread_leaks():
        result = run(
            sys.executable,
            '-c',
            command,
            split=False,
            catch_output=True,
            catch_exceptions=True,
            **run_kwargs,
        )

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
    """Checks that invalid timeout values are rejected."""
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
    """Checks that non cancelling custom token is handled."""
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


def test_condition_token_with_unsuppressed_exception_raises_on_bool_before_run():
    """Checks that ConditionToken with unsuppressed exception raises on bool before run."""
    def boom() -> bool:
        raise RuntimeError('token function exploded')

    token = ConditionToken(boom, suppress_exceptions=False)

    with pytest.raises(RuntimeError, match='token function exploded'):
        bool(token)


def test_condition_token_with_unsuppressed_exception_is_not_swallowed_by_run(assert_no_suby_thread_leaks):
    """Checks that ConditionToken with unsuppressed exception is not swallowed by run."""
    def boom() -> bool:
        raise RuntimeError('silent token exploded')

    token = ConditionToken(boom, suppress_exceptions=False)

    start = time.perf_counter()
    with assert_no_suby_thread_leaks(), pytest.raises(RuntimeError, match='silent token exploded') as exc_info:
        run(sys.executable, '-c', 'import time; time.sleep(5)', split=False, token=token)
    elapsed = time.perf_counter() - start

    assert elapsed < 2
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
    """Checks that silent process timeout and token error raise one of expected exceptions."""
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

    assert elapsed < 2
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
            'for i in range(1000):\n'
            ' print(i, flush=True)\n'
            ' time.sleep(0.01)',
            'stdout',
            'stderr',
        ),
        (
            'import sys, time\n'
            'for i in range(1000):\n'
            ' sys.stderr.write(f"{i}\\n")\n'
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
    assert_no_suby_thread_leaks,
):
    """Checks that token cancellation with active output preserves partial output."""
    start = time.perf_counter()
    token = ConditionToken(lambda: time.perf_counter() - start > 0.1)

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

    assert elapsed >= 0.1
    assert elapsed < 2
    assert result.returncode != 0
    assert result.killed_by_token is True
    assert isinstance(getattr(result, expected_non_empty_stream), str)
    assert getattr(result, expected_non_empty_stream) != ''
    assert '0\n' in getattr(result, expected_non_empty_stream)
    assert isinstance(getattr(result, expected_empty_stream), str)


def test_tiny_timeout_on_fast_process_is_still_well_formed():
    """Checks that tiny timeout on fast process is still well formed."""
    with pytest.raises(TimeoutCancellationError) as exc_info:
        run(sys.executable, '-c', 'import time; time.sleep(0.01)', split=False, timeout=0.000001, catch_output=True)

    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]


def test_stdout_without_newline_is_not_lost():
    """Checks that stdout without newline is not lost."""
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
    """Checks that large output is collected fully."""
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
    """Checks that stderr heavy process does not starve stdout."""
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
    """Checks that interleaved stdout and stderr are both collected."""
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


def test_non_utf8_output_is_rejected_or_normalized():
    """Checks that non-UTF-8 output is rejected consistently under explicit UTF-8 strict decoding."""
    with pytest.raises(UnicodeDecodeError):
        run(sys.executable, '-c', 'import os; os.write(1, b"\\xff\\xfe\\xfd")', split=False, catch_output=True)


def test_non_utf8_stderr_is_rejected_consistently():
    """Checks that non-UTF-8 stderr bytes are rejected consistently under explicit UTF-8 strict decoding."""
    with pytest.raises(UnicodeDecodeError):
        run(sys.executable, '-c', 'import os; os.write(2, b"\\xff\\xfe\\xfd")', split=False, catch_output=True)


def test_utf8_stdout_accepts_non_ascii_text():
    """Checks that explicit UTF-8 decoding preserves non-ASCII stdout text."""
    result = run(sys.executable, '-c', 'print("привет")', split=False, catch_output=True)

    assert result.stdout == 'привет\n'
    assert result.returncode == 0


def test_utf8_stderr_accepts_non_ascii_text():
    """Checks that explicit UTF-8 decoding preserves non-ASCII stderr text."""
    result = run(
        sys.executable,
        '-c',
        'import sys; sys.stderr.write("привет\\n")',
        split=False,
        catch_output=True,
    )

    assert result.stderr == 'привет\n'
    assert result.returncode == 0


def test_complex_kwargs_combination_is_well_formed():
    """Checks that complex kwargs combination is well formed."""
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
        timeout=5,
        token=DefaultToken(),
    )

    assert result.returncode == 0


def test_catch_output_true_suppresses_stdout_callback_even_in_complex_case():
    """Checks that catch output=True suppresses stdout callback even in complex case."""
    seen: List[str] = []
    run(
        sys.executable,
        '-c',
        'print("ok")',
        split=False,
        catch_output=True,
        stdout_callback=seen.append,
    )

    assert seen == []


def test_error_paths_return_consistent_subprocess_result_shapes():
    """Checks that error paths return consistent SubprocessResult shapes."""
    results: List[SubprocessResult] = []

    results.append(run('definitely_missing_command_for_suby_shape', catch_exceptions=True))
    results.append(run(sys.executable, '-c', 'import sys; sys.exit(1)', split=False, catch_exceptions=True, catch_output=True))
    results.append(run(sys.executable, '-c', 'import time; time.sleep(1)', split=False, timeout=0, catch_exceptions=True, catch_output=True))

    for result in results:

        assert isinstance(result.stdout, str)
        assert isinstance(result.stderr, str)
        assert isinstance(result.returncode, int)
        assert isinstance(result.killed_by_token, bool)


def test_logging_contract_across_outcomes_is_explicit():
    """Checks that logging contract across outcomes is explicit."""
    success_logger = MemoryLogger()
    error_logger = MemoryLogger()
    startup_logger = MemoryLogger()

    run(sys.executable, '-c', 'print("ok")', split=False, catch_output=True, logger=success_logger)
    run(sys.executable, '-c', 'import sys; sys.exit(1)', split=False, catch_exceptions=True, catch_output=True, logger=error_logger)
    run('definitely_missing_command_for_suby_logging', catch_exceptions=True, logger=startup_logger)

    assert len(success_logger.data.info) >= 1
    assert len(error_logger.data.error) + len(error_logger.data.exception) >= 1
    assert len(startup_logger.data.exception) >= 1


def test_parallel_runs_with_shared_callback_do_not_drop_events():
    """Checks that parallel runs with shared callback do not drop events."""
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
    """Checks that many short processes complete without state corruption."""
    for _ in range(100):
        result = run(sys.executable, '-c', 'print("ok")', split=False, catch_output=True)

        assert result.stdout == 'ok\n'
