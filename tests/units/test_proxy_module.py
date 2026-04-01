import json
import re
import sys
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from os import environ
from pathlib import Path
from threading import Thread
from time import perf_counter

import full_match
import pytest
from cantok import (
    CancellationError,
    ConditionCancellationError,
    ConditionToken,
    SimpleToken,
    TimeoutCancellationError,
)
from emptylog import MemoryLogger

import suby
from suby import RunningCommandError, WrongCommandError, run
from suby.subprocess_result import SubprocessResult


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
    with pytest.raises(TypeError, match=full_match(exception_message)):
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
    with pytest.raises(WrongCommandError, match=full_match(exception_message)):
        run(*command)


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
    with pytest.raises(RunningCommandError, match=full_match('Error when executing the command "C:fakepython.exe -c pass".')):
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
    with pytest.raises(TimeoutCancellationError, match=full_match('The timeout of 1 seconds has expired.')):
        run('python -c "import time; time.sleep(100)"', timeout=1)


def test_large_output():
    lines = 1000

    result = run(f'python -c "for i in range({lines}): print(i)"', catch_output=True)

    assert result.returncode == 0
    assert result.stdout == ''.join(f'{i}\n' for i in range(lines))


def test_parallel_runs():
    results = [None] * 5

    def run_task(i: int) -> None:
        results[i] = run(f'python -c "print({i})"', catch_output=True)

    threads = [Thread(target=run_task, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    for i, result in enumerate(results):
        assert result.returncode == 0
        assert result.stdout == f'{i}\n'
