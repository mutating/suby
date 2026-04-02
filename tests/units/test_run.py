import importlib
import json
import re
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


@pytest.mark.parametrize(
    'command',
    [
        (sys.executable, '-c "print(\'kek\')"'),
        ('python -c "print(\'kek\')"',),
    ],
)
def test_normal_way(command):
    result = run(*command)

    assert result.stdout == 'kek\n'
    assert result.stderr == ''
    assert result.returncode == 0


@pytest.mark.parametrize(
    'command',
    [
        (sys.executable, '-c "print(\'kek\')"'),
        ('python -c "print(\'kek\')"',),
    ],
)
def test_normal_way_with_simple_token(command):
    result = run(*command, token=SimpleToken())

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
def test_stderr_catching(command):
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
def test_catch_exception(command):
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
def test_timeout(command):
    sleep_time = 100000
    timeout = 0.001
    command = [subcommand.format(sleep_time=sleep_time) if isinstance(subcommand, str) else subcommand for subcommand in command]

    start_time = perf_counter()
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
def test_timeout_without_catching_exception(command):
    sleep_time = 100000
    timeout = 0.001
    command = [subcommand.format(sleep_time=sleep_time) if isinstance(subcommand, str) else subcommand for subcommand in command]

    with pytest.raises(TimeoutCancellationError):
        run(*command, timeout=timeout)

    start_time = perf_counter()
    with pytest.raises(TimeoutCancellationError) as exc_info:
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
def test_exception_in_subprocess_without_catching(command, error_text):
    with pytest.raises(RunningCommandError, match=re.escape(error_text)):
        run(*command)

    with pytest.raises(RunningCommandError) as exc_info:
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
def test_not_catching_output(command):
    stderr_buffer = StringIO()
    stdout_buffer = StringIO()

    with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
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
def test_catching_output(command):
    stderr_buffer = StringIO()
    stdout_buffer = StringIO()

    with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
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
    logger = MemoryLogger()

    run(*command, logger=logger, catch_output=True)

    assert len(logger.data.info) == 2
    assert len(logger.data.error) == 0
    assert len(logger.data) == 2

    assert logger.data.info[0].message == first_log_message
    assert logger.data.info[1].message == second_log_message


@pytest.mark.parametrize(
    ('command', 'first_log_message', 'second_log_message'),
    [
        ((sys.executable, f'-c "import time; time.sleep({500_000})"'), f'The beginning of the execution of the command "{sys.executable} -c "import time; time.sleep(500000)"".', f'The execution of the "{sys.executable} -c "import time; time.sleep(500000)"" command was canceled using a cancellation token.'),
        ((f'python -c "import time; time.sleep({500_000})"',), 'The beginning of the execution of the command "python -c "import time; time.sleep(500000)"".', 'The execution of the "python -c "import time; time.sleep(500000)"" command was canceled using a cancellation token.'),
    ],
)
def test_logging_with_expired_timeout(command, first_log_message, second_log_message):
    logger = MemoryLogger()

    run(*command, logger=logger, catch_exceptions=True, catch_output=True, timeout=0.0001)

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
        ((sys.executable, f'-c "import time; time.sleep({500_000})"'), f'The beginning of the execution of the command "{sys.executable} -c "import time; time.sleep(500000)"".', f'The execution of the "{sys.executable} -c "import time; time.sleep(500000)"" command was canceled using a cancellation token.'),
        ((f'python -c "import time; time.sleep({500_000})"',), 'The beginning of the execution of the command "python -c "import time; time.sleep(500000)"".', 'The execution of the "python -c "import time; time.sleep(500000)"" command was canceled using a cancellation token.'),
    ],
)
def test_logging_with_expired_timeout_without_catching_exceptions(command, first_log_message, second_log_message):
    logger = MemoryLogger()

    with pytest.raises(TimeoutCancellationError):
        run(*command, logger=logger, catch_output=True, timeout=0.0001)

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
def test_only_token(command):
    sleep_time = 100000
    timeout = 0.1
    command = [subcommand.format(sleep_time=sleep_time) if isinstance(subcommand, str) else subcommand for subcommand in command]

    start_time = perf_counter()
    token = ConditionToken(lambda: perf_counter() - start_time > timeout)

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
def test_only_token_without_catching(command):
    sleep_time = 100000
    timeout = 0.1
    command = [subcommand.format(sleep_time=sleep_time) if isinstance(subcommand, str) else subcommand for subcommand in command]

    start_time = perf_counter()
    token = ConditionToken(lambda: perf_counter() - start_time > timeout)

    with pytest.raises(ConditionCancellationError) as exc_info:
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
    'command',
    [
        (sys.executable, '-c "import time; time.sleep({sleep_time})"'),
        ('python -c "import time; time.sleep({sleep_time})"',),
    ],
)
def test_token_plus_timeout_but_timeout_is_more_without_catching(command):
    sleep_time = 100000
    timeout = 0.1
    command = [subcommand.format(sleep_time=sleep_time) if isinstance(subcommand, str) else subcommand for subcommand in command]

    start_time = perf_counter()
    token = ConditionToken(lambda: perf_counter() - start_time > timeout)

    with pytest.raises(ConditionCancellationError) as exc_info:
        run(*command, token=token, timeout=3)

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
    'command',
    [
        (sys.executable, '-c "import time; time.sleep({sleep_time})"'),
        ('python -c "import time; time.sleep({sleep_time})"',),
    ],
)
def test_token_plus_timeout_but_timeout_is_less_without_catching(command):
    sleep_time = 100000
    timeout = 0.1
    command = [subcommand.format(sleep_time=sleep_time) if isinstance(subcommand, str) else subcommand for subcommand in command]

    start_time = perf_counter()
    token = ConditionToken(lambda: perf_counter() - start_time > timeout)

    with pytest.raises(TimeoutCancellationError) as exc_info:
        run(*command, token=token, timeout=timeout/2)

    assert exc_info.value.token is not token
    result = exc_info.value.result

    end_time = perf_counter()

    assert result.returncode != 0
    assert result.stdout == ''
    assert result.stderr == ''
    assert result.killed_by_token == True

    assert end_time - start_time >= timeout/2
    assert end_time - start_time < sleep_time


@pytest.mark.parametrize(
    'command',
    [
        (sys.executable, '-c "print(\'kek\')"'),
        ('python -c "print(\'kek\')"',),
    ],
)
def test_replace_stdout_callback(command):
    accumulator = []

    stderr_buffer = StringIO()
    stdout_buffer = StringIO()

    with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
        result = run(*command, stdout_callback=lambda x: accumulator.append(x))

    assert accumulator == ['kek\n']

    assert result.returncode == 0
    assert result.stdout == 'kek\n'
    assert result.stderr == ''

    assert stderr_buffer.getvalue() == ''
    assert stdout_buffer.getvalue() == ''


@pytest.mark.parametrize(
    'command',
    [
        (sys.executable, '-c "import sys; sys.stderr.write(\'kek\')"'),
        ('python -c "import sys; sys.stderr.write(\'kek\')"',),
    ],
)
def test_replace_stderr_callback(command):
    accumulator = []

    stderr_buffer = StringIO()
    stdout_buffer = StringIO()

    with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
        result = run(*command, stderr_callback=lambda x: accumulator.append(x))

    assert accumulator == ['kek']

    assert result.returncode == 0
    assert result.stdout == ''
    assert result.stderr == 'kek'

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
    with pytest.raises(TypeError, match=match(exception_message)):
        run(*arguments)


@pytest.mark.parametrize(
    'command',
    [
        (Path(sys.executable), '-c "print(\'kek\')"'),
        (sys.executable, '-c "print(\'kek\')"'),
        ('python -c "print(\'kek\')"',),
    ],
)
def test_use_path_object_as_first_positional_argument(command):
    result = run(*command)

    assert result.stdout == 'kek\n'
    assert result.stderr == ''
    assert result.returncode == 0


@pytest.mark.parametrize(
    'command',
    [
        (Path(sys.executable), '-c', 'print(\'kek\')'),
        (sys.executable, '-c', 'print(\'kek\')'),
        ('python', '-c', 'print(\'kek\')'),
    ],
)
def test_multiple_args_without_split(command):
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
    with pytest.raises(WrongCommandError, match=match(exception_message)):
        run(*command)


def test_empty_command_raises_wrong_command_error():
    with pytest.raises(WrongCommandError, match=match('You must pass at least one positional argument with the command to run.')):
        run()


def test_single_string_is_split_on_all_platforms():
    # Under the old Windows behavior, a single string was NOT split by shlex —
    # it was passed as one token to the subprocess, which would fail.
    # This test verifies that shlex splitting works on all platforms.
    result = run('python -c pass')

    assert result.returncode == 0
    assert result.stdout == ''
    assert result.stderr == ''


def test_envs_for_subprocess_are_same_as_parent():
    subprocess_env = json.loads(run('python -c "import os, json; print(json.dumps(dict(os.environ)))"').stdout)

    # why: https://stackoverflow.com/questions/1780483/lines-and-columns-environmental-variables-lost-in-a-script
    subprocess_env.pop('LINES', None)
    subprocess_env.pop('COLUMNS', None)

    assert subprocess_env == environ


def test_executable_path_with_backslashes_passed_as_string():
    # On Windows, sys.executable is a path like C:\Python\python.exe.
    # shlex with posix=True treats \ as an escape character, silently eating backslashes.
    # This test verifies that backslashes in paths survive shlex splitting.
    result = run(sys.executable, '-c pass')

    assert result.returncode == 0


def test_executable_path_with_spaces_passed_as_unquoted_string_fails(tmp_path):
    # When a path containing spaces is embedded in a command string without quotes,
    # shlex splits on the space and the command fails.
    # To pass such a path correctly, it must be either quoted in the string
    # or passed as a separate Path object.
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
    result = run(f'{sys.executable} -c "print(\'kek\')"', catch_output=True)

    assert result.returncode == 0
    assert result.stdout == 'kek\n'


@pytest.mark.skipif(sys.platform != 'win32', reason='Windows-only test')
def test_double_backslash_can_be_disabled_on_windows():
    # With double_backslash=False, shlex eats the backslashes in the path, making the executable path invalid.
    # r'C:\fake\python.exe' → shlex in posix mode: \f→f, \p→p → 'C:fakepython.exe'
    with pytest.raises(RunningCommandError, match=match('Error when executing the command "C:fakepython.exe -c pass".')):
        run(r'C:\fake\python.exe -c pass', double_backslash=False)


@pytest.mark.skipif(sys.platform == 'win32', reason='non-Windows test')
def test_double_backslash_disabled_by_default_on_non_windows():
    # On non-Windows with default double_backslash=False, shlex treats \ as escape character.
    # "hello\ world" is parsed as a single argument "hello world" (backslash escapes the space).
    result = run(
        sys.executable,
        '-c "import sys; print(sys.argv[1])"',
        r'hello\ world',
        catch_output=True,
    )

    assert result.returncode == 0
    assert result.stdout.strip() == 'hello world'


@pytest.mark.skipif(sys.platform == 'win32', reason='non-Windows test')
def test_double_backslash_can_be_enabled_on_non_windows():
    # With double_backslash=True, backslashes are doubled before shlex, so they survive splitting.
    # "hello\ world" becomes "hello\\ world" before shlex, which parses it as two args: "hello\" and "world".
    result = run(
        sys.executable,
        '-c "import sys; print(sys.argv[1])"',
        r'hello\ world',
        double_backslash=True,
        catch_output=True,
    )

    assert result.returncode == 0
    assert result.stdout.strip() == 'hello\\'


def test_run_returns_subprocess_result():
    result = run('python -c pass')

    assert isinstance(result, SubprocessResult)


def test_missing_command_with_catch_exceptions_returns_filled_result():
    result = run('command_that_definitely_does_not_exist_12345', catch_exceptions=True)

    assert result.stdout == ''
    assert result.stderr != ''
    assert 'command_that_definitely_does_not_exist_12345' in result.stderr
    assert result.returncode == 1
    assert result.killed_by_token is False


def test_missing_command_without_catch_exceptions_attaches_filled_result():
    with pytest.raises(RunningCommandError) as exc_info:
        run('command_that_definitely_does_not_exist_12345')

    assert exc_info.value.result.stdout == ''
    assert exc_info.value.result.stderr != ''
    assert 'command_that_definitely_does_not_exist_12345' in exc_info.value.result.stderr
    assert exc_info.value.result.returncode == 1
    assert exc_info.value.result.killed_by_token is False


def test_missing_command_with_catch_exceptions_logs_exception():
    logger = MemoryLogger()

    result = run('command_that_definitely_does_not_exist_12345', catch_exceptions=True, logger=logger)

    assert result.returncode == 1
    assert len(logger.data.info) == 1
    assert len(logger.data.error) == 0
    assert len(logger.data.exception) == 1
    assert logger.data.info[0].message == 'The beginning of the execution of the command "command_that_definitely_does_not_exist_12345".'
    assert logger.data.exception[0].message == 'Error when executing the command "command_that_definitely_does_not_exist_12345".'


def test_missing_command_original_popen_raises_filenotfounderror():
    with pytest.raises(RunningCommandError) as exc_info:
        run('command_that_definitely_does_not_exist_12345')

    assert isinstance(exc_info.value.__cause__, FileNotFoundError)


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX-only exec format semantics')
def test_exec_format_error_original_popen_raises_plain_oserror(tmp_path):
    script = tmp_path / 'script-without-shebang'
    script.write_text('echo hello\n')
    script.chmod(0o755)

    with pytest.raises(RunningCommandError) as exc_info:
        run(str(script))

    assert type(exc_info.value.__cause__) is OSError
    assert 'Exec format error' in str(exc_info.value.__cause__)


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX-only permission semantics')
def test_permission_error_with_catch_exceptions_returns_filled_result(tmp_path):
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


def test_catch_output_suppresses_stdout_callback():
    accumulator = []

    run('python -c "print(\'kek\')"', catch_output=True, stdout_callback=lambda x: accumulator.append(x))

    assert accumulator == []


def test_catch_output_suppresses_stderr_callback():
    accumulator = []

    run('python -c "import sys; sys.stderr.write(\'kek\')"', catch_output=True, stderr_callback=lambda x: accumulator.append(x))

    assert accumulator == []


def test_multiple_strings_split_independently():
    # 'python -c' splits to ['python', '-c'], '"print(777)"' splits to ['print(777)']
    # each string is split independently and the results are concatenated
    result = run('python -c', '"print(777)"', catch_output=True)

    assert result.returncode == 0
    assert result.stdout == '777\n'


def test_argument_with_space_passed_with_split_false():
    # with split=False the string is passed as-is, spaces are not treated as delimiters
    result = run(
        sys.executable,
        '-c', 'import sys; print(sys.argv[1])',
        'hello world',
        split=False,
        catch_output=True,
    )

    assert result.returncode == 0
    assert result.stdout.strip() == 'hello world'


def test_running_command_error_is_importable_from_suby():

    assert hasattr(suby, 'RunningCommandError')


def test_wrong_command_error_is_importable_from_suby():

    assert hasattr(suby, 'WrongCommandError')


def test_timeout_cancellation_error_is_importable_from_suby():

    assert hasattr(suby, 'TimeoutCancellationError')


def test_already_cancelled_simple_token_kills_process():
    token = SimpleToken()
    token.cancel()

    result = run('python -c "import time; time.sleep(100)"', token=token, catch_exceptions=True)

    assert result.killed_by_token == True
    assert result.returncode != 0


def test_already_cancelled_simple_token_raises():
    token = SimpleToken()
    token.cancel()

    with pytest.raises(CancellationError):
        run('python -c "import time; time.sleep(100)"', token=token)


def test_immediately_satisfied_condition_token_kills_process():
    token = ConditionToken(lambda: True)

    result = run('python -c "import time; time.sleep(100)"', token=token, catch_exceptions=True)

    assert result.killed_by_token == True
    assert result.returncode != 0


def test_timeout_exception_message():
    with pytest.raises(TimeoutCancellationError, match=match('The timeout of 1 seconds has expired.')):
        run('python -c "import time; time.sleep(100)"', timeout=1)


def test_negative_timeout_error_message():
    with pytest.raises(ValueError, match=match('You cannot specify a timeout less than zero.')):
        run('python -c "import time; time.sleep(100)"', timeout=-1)


def test_large_output():
    lines = 1000

    result = run(f'python -c "for i in range({lines}): print(i)"', catch_output=True)

    assert result.returncode == 0
    assert result.stdout == ''.join(f'{i}\n' for i in range(lines))


def test_parallel_runs():
    results = [None] * 5

    def run_task(i: int):
        results[i] = run(f'python -c "print({i})"', catch_output=True)

    threads = [Thread(target=run_task, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    for i, result in enumerate(results):

        assert result.returncode == 0
        assert result.stdout == f'{i}\n'


def _python_print_argv_script() -> str:
    return 'import json, sys; print(json.dumps(sys.argv[1:]))'


def test_empty_string_command_rejected():
    with pytest.raises(WrongCommandError):
        run('')


def test_two_empty_string_commands_rejected():
    with pytest.raises(WrongCommandError):
        run('', '')


def test_whitespace_only_command_rejected():
    with pytest.raises(WrongCommandError):
        run('   ')


def test_single_double_quote_rejected():
    with pytest.raises(WrongCommandError):
        run('"')


def test_single_single_quote_rejected():
    with pytest.raises(WrongCommandError):
        run("'")


def test_broken_inline_quote_rejected():
    with pytest.raises(WrongCommandError):
        run('python -c "')


def test_very_long_command_string_is_handled():
    payload = 'x' * 100_000
    result = run(sys.executable, '-c', f'print("{payload}")', split=False, catch_output=True)

    assert result.stdout == payload + '\n'


def test_command_with_nul_byte_is_rejected_consistently():
    with pytest.raises((RunningCommandError, ValueError)):
        run('abc\0def')


def test_empty_path_object_is_rejected_consistently():
    with pytest.raises((RunningCommandError, PermissionError)):
        run(Path(''))  # noqa: PTH201


def test_current_directory_path_object_is_rejected_consistently():
    with pytest.raises(RunningCommandError) as exc_info:
        run(Path('.'))  # noqa: PTH201

    assert isinstance(exc_info.value.__cause__, OSError)


def test_path_with_spaces_and_special_characters_executes_via_path_object(tmp_path: Path):
    script = tmp_path / 'dir with spaces #and(parens)'
    script.mkdir()
    executable = script / 'echo.py'
    executable.write_text('print("ok")')
    result = run(Path(sys.executable), executable, split=False, catch_output=True)

    assert result.stdout == 'ok\n'


def test_run_uses_dedicated_stdout_thread():
    with patch.object(_run_module, 'run_stdout_thread', wraps=_run_module.run_stdout_thread) as wrapped:
        result = run(sys.executable, '-c', 'print("ok")', split=False, catch_output=True)

    assert result.stdout == 'ok\n'
    wrapped.assert_called_once()


def test_kill_process_if_running_ignores_process_lookup_error():
    class MockProcess:
        def poll(self):
            return None

        def kill(self):
            raise ProcessLookupError('already exited')

    _run_module.kill_process_if_running(MockProcess())  # type: ignore[arg-type]


def test_path_object_that_looks_like_flag_is_treated_as_plain_argument():
    result = run(
        Path(sys.executable),
        Path('-c'),
        Path(_python_print_argv_script()),
        split=False,
        catch_output=True,
    )

    assert result.returncode == 0


def test_bytes_argument_rejected():
    with pytest.raises(TypeError):
        run(b'python')  # type: ignore[arg-type]


def test_bytearray_argument_rejected():
    with pytest.raises(TypeError):
        run(bytearray(b'python'))  # type: ignore[arg-type]


def test_purepath_argument_rejected():
    with pytest.raises(TypeError):
        run(PurePath('python'))  # type: ignore[arg-type]


def test_string_like_object_rejected():
    class StringLikeObject:
        def __str__(self) -> str:
            return 'python'

    with pytest.raises(TypeError):
        run(StringLikeObject())  # type: ignore[arg-type]


def test_split_false_does_not_split_single_string_command():
    with pytest.raises(RunningCommandError):
        run('python -c "print(1)"', split=False)


def test_split_false_preserves_spaces_inside_argument():
    result = run(
        sys.executable,
        '-c',
        _python_print_argv_script(),
        'hello world',
        split=False,
        catch_output=True,
    )

    assert result.stdout == '["hello world"]\n'


def test_split_false_with_empty_executable_is_rejected():
    with pytest.raises((RunningCommandError, ValueError)):
        run('', split=False)


def test_split_false_with_path_object_still_executes():
    result = run(Path(sys.executable), '-c', 'print("ok")', split=False, catch_output=True)

    assert result.stdout == 'ok\n'


def test_double_backslash_has_no_effect_when_split_is_false():
    result = run(
        sys.executable,
        '-c',
        _python_print_argv_script(),
        r'hello\ world',
        split=False,
        double_backslash=True,
        catch_output=True,
    )

    assert result.stdout == '["hello\\\\ world"]\n'


def test_trailing_backslash_argument_is_preserved():
    result = run(
        sys.executable,
        '-c',
        _python_print_argv_script(),
        'endswith\\',
        split=False,
        catch_output=True,
    )

    assert result.stdout == '["endswith\\\\"]\n'


def test_path_with_spaces_and_backslashes_is_preserved_with_split_false():
    result = run(
        sys.executable,
        '-c',
        _python_print_argv_script(),
        r'folder with spaces\name',
        split=False,
        catch_output=True,
    )

    assert result.stdout == '["folder with spaces\\\\name"]\n'


@pytest.mark.skipif(sys.platform != 'win32', reason='Windows-only UNC path semantics')
def test_unc_path_survives_windows_processing():
    result = run(r'\\server\share\python.exe -c pass', catch_exceptions=True)

    assert result.stderr is not None


def test_backslash_before_quote_is_preserved_when_split_disabled():
    result = run(
        sys.executable,
        '-c',
        _python_print_argv_script(),
        r'value\"quoted',
        split=False,
        catch_output=True,
    )

    assert result.stdout == '["value\\\\\\"quoted"]\n'


@pytest.mark.skipif(sys.platform == 'win32', reason='Non-Windows-only behavior')
def test_double_backslash_true_changes_non_windows_argument_shape():
    result = run(
        sys.executable,
        '-c "import sys; print(sys.argv[1:])"',
        r'hello\ world',
        double_backslash=True,
        catch_output=True,
    )

    assert result.stdout != "['hello world']\n"


def test_mixed_argument_joining_shape_is_explicit():
    result = run(
        sys.executable,
        '-c',
        _python_print_argv_script(),
        r'hello\ world',
        'two words',
        split=False,
        catch_output=True,
    )

    assert result.stdout == '["hello\\\\ world", "two words"]\n'


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX-only permission semantics')
def test_non_executable_file_is_normalized_via_running_command_error(tmp_path: Path):
    script = tmp_path / 'script.sh'
    script.write_text('echo hello\n')
    script.chmod(0o644)
    with pytest.raises(RunningCommandError) as exc_info:
        run(str(script))

    assert isinstance(exc_info.value.__cause__, PermissionError)


def test_directory_as_executable_is_normalized():
    with pytest.raises(RunningCommandError):
        run(str(Path.cwd()))


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX-only exec format semantics')
def test_exec_format_error_is_normalized_via_running_command_error(tmp_path: Path):
    script = tmp_path / 'script-without-shebang'
    script.write_text('echo hello\n')
    script.chmod(0o755)
    with pytest.raises(RunningCommandError) as exc_info:
        run(str(script))

    assert type(exc_info.value.__cause__) is OSError


def test_missing_parent_path_is_normalized():
    missing = Path.cwd() / 'missing-parent-dir' / 'missing-command'
    with pytest.raises(RunningCommandError) as exc_info:
        run(str(missing))

    assert isinstance(exc_info.value.__cause__, FileNotFoundError)


def test_missing_path_command_is_normalized():
    with pytest.raises(RunningCommandError) as exc_info:
        run('definitely_missing_command_for_suby_tests')

    assert isinstance(exc_info.value.__cause__, FileNotFoundError)


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX-only shebang semantics')
def test_missing_shebang_interpreter_is_normalized(tmp_path: Path):
    script = tmp_path / 'script.py'
    script.write_text('#!/definitely/missing/interpreter\nprint("hello")\n')
    script.chmod(0o755)
    with pytest.raises(RunningCommandError) as exc_info:
        run(str(script))

    assert isinstance(exc_info.value.__cause__, OSError)


def test_stdout_callback_exception_bubbles_up():
    def callback(_: str):
        raise RuntimeError('stdout callback exploded')

    with pytest.raises(RuntimeError, match='stdout callback exploded'):
        run(sys.executable, '-c', 'print("hello")', split=False, stdout_callback=callback)


def test_stdout_callback_exception_kills_process_and_attaches_result():
    def callback(_: str):
        raise RuntimeError('stdout callback exploded')

    start = time.perf_counter()
    with pytest.raises(RuntimeError, match='stdout callback exploded') as exc_info:
        run(
            sys.executable,
            '-c',
            'import time; print("hello", flush=True); time.sleep(5)',
            split=False,
            stdout_callback=callback,
        )
    elapsed = time.perf_counter() - start

    assert elapsed < 2
    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert exc_info.value.result.stdout == 'hello\n'  # type: ignore[attr-defined]
    assert isinstance(exc_info.value.result.stderr, str)  # type: ignore[attr-defined]
    assert isinstance(exc_info.value.result.returncode, int)  # type: ignore[attr-defined]
    assert exc_info.value.result.returncode != 0  # type: ignore[attr-defined]
    assert exc_info.value.result.killed_by_token is False  # type: ignore[attr-defined]


def test_stdout_callback_exception_after_process_exit_keeps_success_returncode():
    def callback(_: str):
        time.sleep(0.1)
        raise RuntimeError('stdout callback exploded after exit')

    start = time.perf_counter()
    with pytest.raises(RuntimeError, match='stdout callback exploded after exit') as exc_info:
        run(
            sys.executable,
            '-c',
            'print("hello", flush=True)',
            split=False,
            stdout_callback=callback,
        )
    elapsed = time.perf_counter() - start

    assert elapsed < 2
    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert exc_info.value.result.stdout == 'hello\n'  # type: ignore[attr-defined]
    assert exc_info.value.result.stderr == ''  # type: ignore[attr-defined]
    assert exc_info.value.result.returncode == 0  # type: ignore[attr-defined]
    assert exc_info.value.result.killed_by_token is False  # type: ignore[attr-defined]


def test_stderr_callback_exception_bubbles_up():
    def callback(_: str):
        raise RuntimeError('stderr callback exploded')

    with pytest.raises(RuntimeError, match='stderr callback exploded'):
        run(sys.executable, '-c', 'import sys; sys.stderr.write("hello\\n")', split=False, stderr_callback=callback)


def test_stderr_callback_exception_kills_process_and_attaches_result():
    def callback(_: str):
        raise RuntimeError('stderr callback exploded')

    start = time.perf_counter()
    with pytest.raises(RuntimeError, match='stderr callback exploded') as exc_info:
        run(
            sys.executable,
            '-c',
            'import sys, time; sys.stderr.write("hello\\n"); sys.stderr.flush(); time.sleep(5)',
            split=False,
            stderr_callback=callback,
        )
    elapsed = time.perf_counter() - start

    assert elapsed < 2
    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert isinstance(exc_info.value.result.stdout, str)  # type: ignore[attr-defined]
    assert exc_info.value.result.stderr == 'hello\n'  # type: ignore[attr-defined]
    assert isinstance(exc_info.value.result.returncode, int)  # type: ignore[attr-defined]
    assert exc_info.value.result.returncode != 0  # type: ignore[attr-defined]
    assert exc_info.value.result.killed_by_token is False  # type: ignore[attr-defined]


def test_stderr_callback_exception_after_process_exit_keeps_success_returncode():
    def callback(_: str):
        time.sleep(0.1)
        raise RuntimeError('stderr callback exploded after exit')

    start = time.perf_counter()
    with pytest.raises(RuntimeError, match='stderr callback exploded after exit') as exc_info:
        run(
            sys.executable,
            '-c',
            'import sys; sys.stderr.write("hello\\n"); sys.stderr.flush()',
            split=False,
            stderr_callback=callback,
        )
    elapsed = time.perf_counter() - start

    assert elapsed < 2
    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert exc_info.value.result.stdout == ''  # type: ignore[attr-defined]
    assert exc_info.value.result.stderr == 'hello\n'  # type: ignore[attr-defined]
    assert exc_info.value.result.returncode == 0  # type: ignore[attr-defined]
    assert exc_info.value.result.killed_by_token is False  # type: ignore[attr-defined]


def test_parallel_stdout_and_stderr_callback_failures_raise_one_of_them():
    def stdout_callback(_: str):
        raise RuntimeError('stdout callback exploded')

    def stderr_callback(_: str):
        raise RuntimeError('stderr callback exploded')

    with pytest.raises(RuntimeError, match=r'(stdout|stderr) callback exploded') as exc_info:
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


def test_timeout_and_stdout_callback_error_raise_one_of_expected_exceptions():
    def stdout_callback(_: str):
        raise RuntimeError('stdout callback exploded')

    start = time.perf_counter()
    with pytest.raises((RuntimeError, TimeoutCancellationError)) as exc_info:
        run(
            sys.executable,
            '-c',
            'import time; print("hello", flush=True); time.sleep(5)',
            split=False,
            stdout_callback=stdout_callback,
            timeout=0.2,
        )
    elapsed = time.perf_counter() - start

    assert elapsed < 2
    result = cast(Any, exc_info.value).result

    assert isinstance(result, SubprocessResult)
    assert isinstance(result.stdout, str)
    assert isinstance(result.stderr, str)
    assert isinstance(result.returncode, int)


def test_timeout_and_stderr_callback_error_raise_one_of_expected_exceptions():
    returncodes = []
    killed_flags = set()

    for _ in range(5):
        def stderr_callback(_: str):
            raise RuntimeError('stderr callback exploded')

        start = time.perf_counter()
        with pytest.raises((RuntimeError, TimeoutCancellationError)) as exc_info:
            run(
                sys.executable,
                '-c',
                'import sys, time; sys.stderr.write("hello\\n"); sys.stderr.flush(); time.sleep(5)',
                split=False,
                stderr_callback=stderr_callback,
                timeout=0.2,
            )
        elapsed = time.perf_counter() - start

        assert elapsed < 2
        result = cast(Any, exc_info.value).result

        assert isinstance(result, SubprocessResult)
        assert isinstance(result.stdout, str)
        assert isinstance(result.stderr, str)
        returncodes.append(result.returncode)
        killed_flags.add(result.killed_by_token)

    assert returncodes == [-9, -9, -9, -9, -9]
    assert killed_flags.issubset({False, True})


def test_timeout_and_stdout_callback_error_after_near_exit_raise_one_of_expected_exceptions():
    returncodes = []
    killed_flags = []

    for _ in range(5):
        def stdout_callback(_: str):
            time.sleep(0.1)
            raise RuntimeError('stdout callback exploded after near-exit')

        start = time.perf_counter()
        with pytest.raises((RuntimeError, TimeoutCancellationError)) as exc_info:
            run(
                sys.executable,
                '-c',
                'import time; print("hello", flush=True); time.sleep(0.05)',
                split=False,
                stdout_callback=stdout_callback,
                timeout=0.02,
            )
        elapsed = time.perf_counter() - start

        assert elapsed < 2
        result = cast(Any, exc_info.value).result

        assert isinstance(result, SubprocessResult)
        assert result.stdout in ('', 'hello\n')
        assert isinstance(result.stderr, str)
        returncodes.append(result.returncode)
        killed_flags.append(result.killed_by_token)

    assert returncodes == [-9, -9, -9, -9, -9]
    assert killed_flags == [True, True, True, True, True]


def test_timeout_and_stderr_callback_error_after_near_exit_raise_one_of_expected_exceptions():
    returncodes = []
    killed_flags = []

    for _ in range(5):
        def stderr_callback(_: str):
            time.sleep(0.1)
            raise RuntimeError('stderr callback exploded after near-exit')

        start = time.perf_counter()
        with pytest.raises((RuntimeError, TimeoutCancellationError)) as exc_info:
            run(
                sys.executable,
                '-c',
                'import sys, time; sys.stderr.write("hello\\n"); sys.stderr.flush(); time.sleep(0.05)',
                split=False,
                stderr_callback=stderr_callback,
                timeout=0.02,
            )
        elapsed = time.perf_counter() - start

        assert elapsed < 2
        result = cast(Any, exc_info.value).result

        assert isinstance(result, SubprocessResult)
        assert isinstance(result.stdout, str)
        assert result.stderr in ('', 'hello\n')
        returncodes.append(result.returncode)
        killed_flags.append(result.killed_by_token)

    assert returncodes == [-9, -9, -9, -9, -9]
    assert killed_flags == [True, True, True, True, True]


def test_existing_result_attribute_on_callback_exception_is_not_overwritten():
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
    class ExplodingResultGetterError(RuntimeError):
        @property
        def result(self) -> SubprocessResult:
            raise RuntimeError('result getter exploded')

    def callback(_: str):
        raise ExplodingResultGetterError('stdout callback exploded')

    with pytest.raises(ExplodingResultGetterError, match='stdout callback exploded'):
        run(sys.executable, '-c', 'print("hello")', split=False, stdout_callback=callback)


def test_result_setter_failure_does_not_mask_original_exception():
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
    result = SubprocessResult()
    result.stdout = 'hello'
    result.stderr = ''
    result.returncode = 0

    _run_module.attach_result_to_exception(object(), result)  # type: ignore[arg-type]


def test_raise_background_failure_ignores_wait_oserror_and_preserves_original_exception():
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
    seen: List[str] = []

    def callback(text: str):
        time.sleep(0.05)
        seen.append(text)

    result = run(sys.executable, '-c', 'print("hello")', split=False, stdout_callback=callback)

    assert result.returncode == 0
    assert seen == ['hello\n']


def test_callback_that_prints_does_not_deadlock():
    def callback(text: str):
        print(text, end='')  # noqa: T201

    result = run(sys.executable, '-c', 'print("hello")', split=False, stdout_callback=callback, catch_output=False)

    assert result.returncode == 0


def test_shared_accumulator_callback_collects_output_from_parallel_runs():
    accumulator: List[str] = []

    def callback(text: str):
        accumulator.append(text)

    threads = [
        Thread(target=run, args=(sys.executable, '-c', f'print({i})'), kwargs={'split': False, 'stdout_callback': callback})
        for i in range(5)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(accumulator) == 5


def test_stdout_callback_must_be_callable():
    with pytest.raises(TypeError):
        run(sys.executable, '-c', 'print("hello")', split=False, stdout_callback=1)  # type: ignore[arg-type]


def test_stderr_callback_must_be_callable():
    with pytest.raises(TypeError):
        run(sys.executable, '-c', 'import sys; sys.stderr.write("hello")', split=False, stderr_callback=1)  # type: ignore[arg-type]


def test_catch_output_true_suppresses_failing_stdout_callback():
    def callback(_: str):
        raise RuntimeError('stdout callback should not be called')

    result = run(sys.executable, '-c', 'print("hello")', split=False, catch_output=True, stdout_callback=callback)

    assert result.stdout == 'hello\n'


def test_catch_output_true_suppresses_failing_stderr_callback():
    def callback(_: str):
        raise RuntimeError('stderr callback should not be called')

    result = run(
        sys.executable,
        '-c',
        'import sys; sys.stderr.write("hello\\n")',
        split=False,
        catch_output=True,
        catch_exceptions=True,
        stderr_callback=callback,
    )

    assert result.stderr == 'hello\n'


def test_zero_timeout_kills_immediately():
    with pytest.raises(TimeoutCancellationError):
        run(sys.executable, '-c', 'import time; time.sleep(1)', split=False, timeout=0)


def test_negative_timeout_rejected():
    with pytest.raises(ValueError, match=re.escape('You cannot specify a timeout less than zero.')):
        run(sys.executable, '-c', 'import time; time.sleep(1)', split=False, timeout=-1)


def test_nan_timeout_is_rejected_or_handled_consistently():
    with pytest.raises(ValueError, match=re.escape('You cannot specify NaN or infinite timeout values.')):
        run(sys.executable, '-c', 'import time; time.sleep(0.1)', split=False, timeout=float('nan'))


def test_infinite_timeout_is_supported_or_rejected_consistently():
    with pytest.raises(ValueError, match=re.escape('You cannot specify NaN or infinite timeout values.')):
        run(sys.executable, '-c', 'print("ok")', split=False, timeout=float('inf'), catch_output=True)


def test_string_timeout_is_rejected():
    with pytest.raises((TypeError, ValueError)):
        run(sys.executable, '-c', 'print("ok")', split=False, timeout='1')  # type: ignore[arg-type]


def test_already_cancelled_default_like_token_is_handled():
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
    result = run(sys.executable, '-c', 'print("ok")', split=False, token=token, catch_output=True)

    assert result.returncode == 0


def test_condition_token_with_unsuppressed_exception_raises_on_bool_before_run():
    def boom() -> bool:
        raise RuntimeError('token function exploded')

    token = ConditionToken(boom, suppress_exceptions=False)

    with pytest.raises(RuntimeError, match='token function exploded'):
        bool(token)


def test_condition_token_with_unsuppressed_exception_is_not_swallowed_by_run():
    def boom() -> bool:
        raise RuntimeError('token function exploded')

    token = ConditionToken(boom, suppress_exceptions=False)

    start = time.perf_counter()
    with pytest.raises(RuntimeError, match='token function exploded') as exc_info:
        run(sys.executable, '-c', 'import time; time.sleep(5)', split=False, token=token)
    elapsed = time.perf_counter() - start

    assert elapsed < 2
    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert isinstance(exc_info.value.result.stdout, str)  # type: ignore[attr-defined]
    assert isinstance(exc_info.value.result.stderr, str)  # type: ignore[attr-defined]
    assert isinstance(exc_info.value.result.returncode, int)  # type: ignore[attr-defined]


def test_timeout_and_token_error_raise_one_of_expected_exceptions():
    def boom() -> bool:
        raise RuntimeError('token function exploded')

    token = ConditionToken(boom, suppress_exceptions=False)

    start = time.perf_counter()
    with pytest.raises((RuntimeError, TimeoutCancellationError)) as exc_info:
        run(
            sys.executable,
            '-c',
            'import time; time.sleep(5)',
            split=False,
            token=token,
            timeout=0.2,
        )
    elapsed = time.perf_counter() - start

    assert elapsed < 2
    result = cast(Any, exc_info.value).result

    assert isinstance(result, SubprocessResult)
    assert isinstance(result.stdout, str)
    assert isinstance(result.stderr, str)
    assert isinstance(result.returncode, int)


def test_silent_process_timeout_and_immediate_token_error_raise_one_of_expected_exceptions():
    def boom() -> bool:
        raise RuntimeError('immediate token function exploded')

    token = ConditionToken(boom, suppress_exceptions=False)

    start = time.perf_counter()
    with pytest.raises((RuntimeError, TimeoutCancellationError)) as exc_info:
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


def test_silent_process_timeout_and_delayed_token_error_raise_one_of_expected_exceptions():
    calls = 0

    def boom_later() -> bool:
        nonlocal calls
        calls += 1
        if calls < 4:
            return False
        raise RuntimeError('delayed token function exploded')

    token = ConditionToken(boom_later, suppress_exceptions=False)

    start = time.perf_counter()
    with pytest.raises((RuntimeError, TimeoutCancellationError)) as exc_info:
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


def test_condition_token_exception_on_silent_process_surfaces_quickly():
    def boom() -> bool:
        raise RuntimeError('silent token exploded')

    token = ConditionToken(boom, suppress_exceptions=False)

    start = time.perf_counter()
    with pytest.raises(RuntimeError, match='silent token exploded') as exc_info:
        run(sys.executable, '-c', 'import time; time.sleep(5)', split=False, token=token)
    elapsed = time.perf_counter() - start

    assert elapsed < 2
    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert isinstance(exc_info.value.result.stdout, str)  # type: ignore[attr-defined]
    assert isinstance(exc_info.value.result.stderr, str)  # type: ignore[attr-defined]


def test_token_cancellation_with_active_stdout_preserves_partial_output():
    start = time.perf_counter()
    token = ConditionToken(lambda: time.perf_counter() - start > 0.1)

    result = run(
        sys.executable,
        '-c',
        'import time\n'
        'for i in range(1000):\n'
        ' print(i, flush=True)\n'
        ' time.sleep(0.01)',
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
    assert isinstance(result.stdout, str)
    assert result.stdout != ''
    assert '0\n' in result.stdout
    assert isinstance(result.stderr, str)


def test_token_cancellation_with_active_stderr_preserves_partial_output():
    start = time.perf_counter()
    token = ConditionToken(lambda: time.perf_counter() - start > 0.1)

    result = run(
        sys.executable,
        '-c',
        'import sys, time\n'
        'for i in range(1000):\n'
        ' sys.stderr.write(f"{i}\\n")\n'
        ' sys.stderr.flush()\n'
        ' time.sleep(0.01)',
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
    assert isinstance(result.stderr, str)
    assert result.stderr != ''
    assert '0\n' in result.stderr
    assert isinstance(result.stdout, str)


def test_token_and_timeout_race_is_consistent():
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
    result = run(sys.executable, '-c', 'print("ok")', split=False, token=token, timeout=0.5, catch_output=True)

    assert result.returncode == 0


def test_tiny_timeout_on_fast_process_is_still_well_formed():
    with pytest.raises(TimeoutCancellationError) as exc_info:
        run(sys.executable, '-c', 'import time; time.sleep(0.01)', split=False, timeout=0.000001, catch_output=True)

    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]


def test_stdout_without_newline_is_not_lost():
    result = run(sys.executable, '-c', 'import sys; sys.stdout.write("hello")', split=False, catch_output=True)

    assert result.stdout == 'hello'


def test_large_stdout_is_collected_fully():
    result = run(sys.executable, '-c', 'for i in range(5000): print(i)', split=False, catch_output=True)

    assert result.stdout is not None
    assert result.stdout.startswith('0\n')
    assert result.stdout.endswith('4999\n')


def test_large_stderr_is_collected_fully():
    result = run(
        sys.executable,
        '-c',
        'import sys\nfor i in range(5000): sys.stderr.write(f"{i}\\n")',
        split=False,
        catch_exceptions=True,
        catch_output=True,
    )

    assert result.stderr is not None
    assert result.stderr.startswith('0\n')
    assert result.stderr.endswith('4999\n')


def test_stderr_heavy_process_does_not_starve_stdout():
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
    with pytest.raises((UnicodeDecodeError, RunningCommandError)):
        run(sys.executable, '-c', 'import os; os.write(1, b"\\xff\\xfe\\xfd")', split=False, catch_output=True)


def test_last_line_without_newline_is_preserved():
    result = run(sys.executable, '-c', 'import sys; sys.stdout.write("tail")', split=False, catch_output=True)

    assert result.stdout == 'tail'


def test_complex_kwargs_combination_is_well_formed():
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
    success_logger = MemoryLogger()
    error_logger = MemoryLogger()
    startup_logger = MemoryLogger()

    run(sys.executable, '-c', 'print("ok")', split=False, catch_output=True, logger=success_logger)
    run(sys.executable, '-c', 'import sys; sys.exit(1)', split=False, catch_exceptions=True, catch_output=True, logger=error_logger)
    run('definitely_missing_command_for_suby_logging', catch_exceptions=True, logger=startup_logger)

    assert len(success_logger.data.info) >= 1
    assert len(error_logger.data.error) + len(error_logger.data.exception) >= 1
    assert len(startup_logger.data.exception) >= 1


def test_many_parallel_runs_do_not_corrupt_results():
    results: List[SubprocessResult] = [SubprocessResult() for _ in range(10)]

    def worker(index: int):
        results[index] = run(sys.executable, '-c', f'print({index})', split=False, catch_output=True)

    threads = [Thread(target=worker, args=(index,)) for index in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    for index, result in enumerate(results):

        assert result.stdout == f'{index}\n'


def test_parallel_runs_with_shared_callback_do_not_drop_events():
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
    for _ in range(100):
        result = run(sys.executable, '-c', 'print("ok")', split=False, catch_output=True)

        assert result.stdout == 'ok\n'
