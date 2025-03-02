import re
import sys
import json
from os import environ
from time import perf_counter
from io import StringIO
from contextlib import redirect_stdout, redirect_stderr
from pathlib import Path
import platform

import pytest
import full_match
from cantok import TimeoutCancellationError, ConditionCancellationError, ConditionToken, SimpleToken
from emptylog import MemoryLogger

import suby
from suby import RunningCommandError, WrongCommandError


@pytest.mark.parametrize(
    ['command'],
    [
        ((sys.executable, '-c "print(\'kek\')"'),),
        (('python -c "print(\'kek\')"',),),
    ]
)
def test_normal_way(command):
    result = suby(*command)

    assert result.stdout == 'kek\n'
    assert result.stderr == ''
    assert result.returncode == 0


@pytest.mark.parametrize(
    ['command'],
    [
        ((sys.executable, '-c "print(\'kek\')"'),),
        (('python -c "print(\'kek\')"',),),
    ]
)
def test_normal_way_with_simple_token(command):
    result = suby(*command, token=SimpleToken())

    assert result.stdout == 'kek\n'
    assert result.stderr == ''
    assert result.returncode == 0


@pytest.mark.parametrize(
    ['command'],
    [
        ((sys.executable, '-c "import sys; sys.stderr.write(\'kek\')"'),),
        (('python -c "import sys; sys.stderr.write(\'kek\')"',),),
    ]
)
def test_stderr_catching(command):
    result = suby(*command)

    assert result.stdout == ''
    assert result.stderr == 'kek'
    assert result.returncode == 0


@pytest.mark.parametrize(
    ['command'],
    [
        ((sys.executable, '-c "raise ValueError"'),),
        (('python -c "raise ValueError"',),),
    ]
)
def test_catch_exception(command):
    result = suby(*command, catch_exceptions=True)

    assert 'ValueError' in result.stderr
    assert result.returncode != 0


@pytest.mark.parametrize(
    ['command'],
    [
        ((sys.executable, '-c "import time; time.sleep({sleep_time})"'),),
        (('python -c "import time; time.sleep({sleep_time})"',),),
    ]
)
def test_timeout(command):
    sleep_time = 100000
    timeout = 0.001
    command = [subcommand.format(sleep_time=sleep_time) if isinstance(subcommand, str) else subcommand for subcommand in command]

    start_time = perf_counter()
    result = suby(*command, timeout=timeout, catch_exceptions=True)
    end_time = perf_counter()

    assert result.returncode != 0
    assert result.stdout == ''
    assert result.stderr == ''

    assert (end_time - start_time) < sleep_time
    assert (end_time - start_time) >= timeout


@pytest.mark.parametrize(
    ['command'],
    [
        ((sys.executable, '-c "import time; time.sleep({sleep_time})"'),),
        (('python -c "import time; time.sleep({sleep_time})"',),),
    ]
)
def test_timeout_without_catching_exception(command):
    sleep_time = 100000
    timeout = 0.001
    command = [subcommand.format(sleep_time=sleep_time) if isinstance(subcommand, str) else subcommand for subcommand in command]

    with pytest.raises(TimeoutCancellationError):
        suby(*command, timeout=timeout)

    start_time = perf_counter()
    try:
        suby(*command, timeout=timeout)
    except TimeoutCancellationError as e:
        assert e.result.stdout == ''
        assert e.result.stderr == ''
        assert e.result.returncode != 0
    end_time = perf_counter()

    assert (end_time - start_time) < sleep_time
    assert (end_time - start_time) >= timeout


@pytest.mark.parametrize(
    ['command', 'error_text'],
    [
        ((sys.executable, '-c "raise ValueError"'), f'Error when executing the command "{sys.executable} -c "raise ValueError"".'),
        (('python -c "raise ValueError"',), 'Error when executing the command "python -c "raise ValueError"".'),
    ]
)
def test_exception_in_subprocess_without_catching(command, error_text):
    with pytest.raises(RunningCommandError, match=re.escape(error_text)):
        suby(*command)

    try:
        suby(*command)
    except RunningCommandError as e:
        assert e.result.stdout == ''
        assert 'ValueError' in e.result.stderr
        assert e.result.returncode != 0


@pytest.mark.parametrize(
    ['command'],
    [
        ((sys.executable, '-c "print(\'kek1\', end=\'\'); import sys; sys.stderr.write(\'kek2\')"'),),
        (('python -c "print(\'kek1\', end=\'\'); import sys; sys.stderr.write(\'kek2\')"',),),
    ]
)
def test_not_catching_output(command):
    stderr_buffer = StringIO()
    stdout_buffer = StringIO()

    with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
        result = suby(*command, catch_output=False)

        stderr = stderr_buffer.getvalue()
        stdout = stdout_buffer.getvalue()

        assert result.returncode == 0
        assert stderr == 'kek2'
        assert stdout == 'kek1'


@pytest.mark.parametrize(
    ['command'],
    [
        ((sys.executable, '-c "print(\'kek1\', end=\'\'); import sys; sys.stderr.write(\'kek2\')"'),),
        (('python -c "print(\'kek1\', end=\'\'); import sys; sys.stderr.write(\'kek2\')"',),),
    ]
)
def test_catching_output(command):
    stderr_buffer = StringIO()
    stdout_buffer = StringIO()

    with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
        result = suby(*command, catch_output=True)

        assert result.returncode == 0
        assert stderr_buffer.getvalue() == ''
        assert stdout_buffer.getvalue() == ''


@pytest.mark.parametrize(
    ['command', 'first_log_message', 'second_log_message'],
    [
        ((sys.executable, '-c "print(\'kek\', end=\'\')"'), f'The beginning of the execution of the command "{sys.executable} -c "print(\'kek\', end=\'\')"".', f'The command "{sys.executable} -c "print(\'kek\', end=\'\')"" has been successfully executed.'),
        (('python -c "print(\'kek\', end=\'\')"',), 'The beginning of the execution of the command "python -c "print(\'kek\', end=\'\')"".', 'The command "python -c "print(\'kek\', end=\'\')"" has been successfully executed.'),
    ]
)
def test_logging_normal_way(command, first_log_message, second_log_message):
    logger = MemoryLogger()

    suby(*command, logger=logger, catch_output=True)

    assert len(logger.data.info) == 2
    assert len(logger.data.error) == 0
    assert len(logger.data) == 2

    assert logger.data.info[0].message == first_log_message
    assert logger.data.info[1].message == second_log_message


@pytest.mark.parametrize(
    ['command', 'first_log_message', 'second_log_message'],
    [
        ((sys.executable, f'-c "import time; time.sleep({500_000})"'), f'The beginning of the execution of the command "{sys.executable} -c "import time; time.sleep(500000)"".', f'The execution of the "{sys.executable} -c "import time; time.sleep(500000)"" command was canceled using a cancellation token.'),
        ((f'python -c "import time; time.sleep({500_000})"',), 'The beginning of the execution of the command "python -c "import time; time.sleep(500000)"".', 'The execution of the "python -c "import time; time.sleep(500000)"" command was canceled using a cancellation token.'),
    ]
)
def test_logging_with_expired_timeout(command, first_log_message, second_log_message):
    logger = MemoryLogger()

    suby(*command, logger=logger, catch_exceptions=True, catch_output=True, timeout=0.0001)

    assert len(logger.data.info) == 1
    assert len(logger.data.error) == 1
    assert len(logger.data) == 2

    assert logger.data.info[0].message == first_log_message
    assert logger.data.error[0].message == second_log_message


@pytest.mark.parametrize(
    ['command', 'first_log_message', 'second_log_message'],
    [
        ((sys.executable, '-c 1/0'), f'The beginning of the execution of the command "{sys.executable} -c 1/0".', f'Error when executing the command "{sys.executable} -c 1/0".'),
        (('python -c 1/0',), 'The beginning of the execution of the command "python -c 1/0".', 'Error when executing the command "python -c 1/0".'),
    ]
)
def test_logging_with_exception(command, first_log_message, second_log_message):
    logger = MemoryLogger()

    suby(*command, logger=logger, catch_exceptions=True, catch_output=True)

    assert len(logger.data.info) == 1
    assert len(logger.data.error) == 1
    assert len(logger.data) == 2

    assert logger.data.info[0].message == first_log_message
    assert logger.data.error[0].message == second_log_message


@pytest.mark.parametrize(
    ['command', 'first_log_message', 'second_log_message'],
    [
        ((sys.executable, f'-c "import time; time.sleep({500_000})"'), f'The beginning of the execution of the command "{sys.executable} -c "import time; time.sleep(500000)"".', f'The execution of the "{sys.executable} -c "import time; time.sleep(500000)"" command was canceled using a cancellation token.'),
        ((f'python -c "import time; time.sleep({500_000})"',), 'The beginning of the execution of the command "python -c "import time; time.sleep(500000)"".', 'The execution of the "python -c "import time; time.sleep(500000)"" command was canceled using a cancellation token.'),
    ]
)
def test_logging_with_expired_timeout_without_catching_exceptions(command, first_log_message, second_log_message):
    logger = MemoryLogger()

    with pytest.raises(TimeoutCancellationError):
        suby(*command, logger=logger, catch_output=True, timeout=0.0001)

    assert len(logger.data.info) == 1
    assert len(logger.data.error) == 1
    assert len(logger.data) == 2

    assert logger.data.info[0].message == first_log_message
    assert logger.data.error[0].message == second_log_message


@pytest.mark.parametrize(
    ['command', 'first_log_message', 'second_log_message'],
    [
        ((sys.executable, '-c 1/0'), f'The beginning of the execution of the command "{sys.executable} -c 1/0".', f'Error when executing the command "{sys.executable} -c 1/0".'),
        (('python -c 1/0',), 'The beginning of the execution of the command "python -c 1/0".', 'Error when executing the command "python -c 1/0".'),
    ]
)
def test_logging_with_exception_without_catching_exceptions(command, first_log_message, second_log_message):
    logger = MemoryLogger()

    with pytest.raises(RunningCommandError):
        suby(*command, logger=logger, catch_output=True)

    assert len(logger.data.info) == 1
    assert len(logger.data.error) == 1

    assert logger.data.info[0].message == first_log_message
    assert logger.data.error[0].message == second_log_message


@pytest.mark.parametrize(
    ['command'],
    [
        ((sys.executable, '-c "import time; time.sleep({sleep_time})"'),),
        (('python -c "import time; time.sleep({sleep_time})"',),),
    ]
)
def test_only_token(command):
    sleep_time = 100000
    timeout = 0.1
    command = [subcommand.format(sleep_time=sleep_time) if isinstance(subcommand, str) else subcommand for subcommand in command]

    start_time = perf_counter()
    token = ConditionToken(lambda: perf_counter() - start_time > timeout)

    result = suby(*command, catch_exceptions=True, token=token)

    end_time = perf_counter()

    assert result.returncode != 0
    assert result.stdout == ''
    assert result.stderr == ''
    assert result.killed_by_token == True

    assert end_time - start_time >= timeout
    assert end_time - start_time < sleep_time


@pytest.mark.parametrize(
    ['command'],
    [
        ((sys.executable, '-c "import time; time.sleep({sleep_time})"'),),
        (('python -c "import time; time.sleep({sleep_time})"',),),
    ]
)
def test_only_token_without_catching(command):
    sleep_time = 100000
    timeout = 0.1
    command = [subcommand.format(sleep_time=sleep_time) if isinstance(subcommand, str) else subcommand for subcommand in command]

    start_time = perf_counter()
    token = ConditionToken(lambda: perf_counter() - start_time > timeout)

    try:
        result = suby(*command, token=token)
    except ConditionCancellationError as e:
        assert e.token is token
        result = e.result

    end_time = perf_counter()

    assert result.returncode != 0
    assert result.stdout == ''
    assert result.stderr == ''
    assert result.killed_by_token == True

    assert end_time - start_time >= timeout
    assert end_time - start_time < sleep_time


@pytest.mark.parametrize(
    ['command'],
    [
        ((sys.executable, '-c "import time; time.sleep({sleep_time})"'),),
        (('python -c "import time; time.sleep({sleep_time})"',),),
    ]
)
def test_token_plus_timeout_but_timeout_is_more_without_catching(command):
    sleep_time = 100000
    timeout = 0.1
    command = [subcommand.format(sleep_time=sleep_time) if isinstance(subcommand, str) else subcommand for subcommand in command]

    start_time = perf_counter()
    token = ConditionToken(lambda: perf_counter() - start_time > timeout)

    try:
        result = suby(*command, token=token, timeout=3)
    except ConditionCancellationError as e:
        assert e.token is token
        result = e.result

    end_time = perf_counter()

    assert result.returncode != 0
    assert result.stdout == ''
    assert result.stderr == ''
    assert result.killed_by_token == True

    assert end_time - start_time >= timeout
    assert end_time - start_time < sleep_time


@pytest.mark.parametrize(
    ['command'],
    [
        ((sys.executable, '-c "import time; time.sleep({sleep_time})"'),),
        (('python -c "import time; time.sleep({sleep_time})"',),),
    ]
)
def test_token_plus_timeout_but_timeout_is_less_without_catching(command):
    sleep_time = 100000
    timeout = 0.1
    command = [subcommand.format(sleep_time=sleep_time) if isinstance(subcommand, str) else subcommand for subcommand in command]

    start_time = perf_counter()
    token = ConditionToken(lambda: perf_counter() - start_time > timeout)

    try:
        result = suby(*command, token=token, timeout=timeout/2)
    except TimeoutCancellationError as e:
        assert e.token is not token
        result = e.result

    end_time = perf_counter()

    assert result.returncode != 0
    assert result.stdout == ''
    assert result.stderr == ''
    assert result.killed_by_token == True

    assert end_time - start_time >= timeout/2
    assert end_time - start_time < sleep_time


@pytest.mark.parametrize(
    ['command'],
    [
        ((sys.executable, '-c "print(\'kek\')"'),),
        (('python -c "print(\'kek\')"',),),
    ]
)
def test_replace_stdout_callback(command):
    accumulator = []

    stderr_buffer = StringIO()
    stdout_buffer = StringIO()

    with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
        result = suby(*command, stdout_callback=lambda x: accumulator.append(x))

    assert accumulator == ['kek\n']

    assert result.returncode == 0
    assert result.stdout == 'kek\n'
    assert result.stderr == ''

    assert stderr_buffer.getvalue() == ''
    assert stdout_buffer.getvalue() == ''


@pytest.mark.parametrize(
    ['command'],
    [
        ((sys.executable, '-c "import sys; sys.stderr.write(\'kek\')"'),),
        (('python -c "import sys; sys.stderr.write(\'kek\')"',),),
    ]
)
def test_replace_stderr_callback(command):
    accumulator = []

    stderr_buffer = StringIO()
    stdout_buffer = StringIO()

    with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
        result = suby(*command, stderr_callback=lambda x: accumulator.append(x))

    assert accumulator == ['kek']

    assert result.returncode == 0
    assert result.stdout == ''
    assert result.stderr == 'kek'

    assert stderr_buffer.getvalue() == ''
    assert stdout_buffer.getvalue() == ''


@pytest.mark.parametrize(
    ['arguments', 'exception_message'],
    (
        ([None], 'Only strings and pathlib.Path objects can be positional arguments when calling the suby function. You passed "None" (NoneType).'),
        ([1], 'Only strings and pathlib.Path objects can be positional arguments when calling the suby function. You passed "1" (int).'),
        (['python', 1], 'Only strings and pathlib.Path objects can be positional arguments when calling the suby function. You passed "1" (int).'),
    ),
)
def test_pass_wrong_positional_argument(arguments, exception_message):
    with pytest.raises(TypeError, match=full_match(exception_message)):
        suby(*arguments)


@pytest.mark.parametrize(
    ['command'],
    [
        ((Path(sys.executable), '-c "print(\'kek\')"'),),
        ((sys.executable, '-c "print(\'kek\')"'),),
        (('python -c "print(\'kek\')"',),),
    ]
)
def test_use_path_object_as_first_positional_argument(command):
    result = suby(*command)

    assert result.stdout == 'kek\n'
    assert result.stderr == ''
    assert result.returncode == 0


def test_suby_as_attribute_of_suby():
    assert suby.suby is suby


def test_full_import_form():
    from suby import suby as local_suby

    assert suby is local_suby


@pytest.mark.parametrize(
    ['command'],
    [
        ((Path(sys.executable), '-c', 'print(\'kek\')'),),
        ((sys.executable, '-c', 'print(\'kek\')'),),
        (('python', '-c', 'print(\'kek\')'),),
    ]
)
def test_multiple_args_without_split(command):
    result = suby(*command, split=False)

    assert result.stdout == 'kek\n'
    assert result.stderr == ''
    assert result.returncode == 0


@pytest.mark.skipif(platform.system() == 'Windows', reason='There is no errors like this in mslex.')
@pytest.mark.parametrize(
    ['command', 'exception_message'],
    [
        ((Path(sys.executable), '-c "'), 'The expression "-c "" cannot be parsed.'),
        ((sys.executable, '-c "'), 'The expression "-c "" cannot be parsed.'),
        (('python -c "',), 'The expression "python -c "" cannot be parsed.'),
    ]
)
def test_wrong_command(command, exception_message):
    with pytest.raises(WrongCommandError, match=full_match(exception_message)):
        suby(*command)


def test_envs_for_subprocess_are_same_as_parent():
    subprocess_env = json.loads(suby('python -c "import os, json; print(json.dumps(dict(os.environ)))"').stdout)

    # why: https://stackoverflow.com/questions/1780483/lines-and-columns-environmental-variables-lost-in-a-script
    subprocess_env.pop('LINES', None)
    subprocess_env.pop('COLUMNS', None)

    assert subprocess_env == environ
