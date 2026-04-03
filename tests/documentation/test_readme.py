import re
import sys
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path

import pytest
from cantok import ConditionCancellationError, ConditionToken
from emptylog import MemoryLogger
from full_match import match

from suby import RunningCommandError, TimeoutCancellationError, run


def test_run_hello_world():
    """Checks that run hello world."""
    stderr_buffer = StringIO()
    stdout_buffer = StringIO()

    with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
        result = run('python -c "print(\'hello, world!\')"')

    assert stderr_buffer.getvalue() == ''
    assert stdout_buffer.getvalue() == 'hello, world!\n'

    assert result.stdout == 'hello, world!\n'
    assert result.stderr == ''
    assert result.returncode == 0
    assert not result.killed_by_token


def test_result_repr_format():
    """Checks that result repr format."""
    stderr_buffer = StringIO()
    stdout_buffer = StringIO()

    with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
        result = run('python -c "print(\'hello, world!\')"')

    assert re.fullmatch(
        r"SubprocessResult\(id='[0-9a-f]{32}', stdout='hello, world!\\n', stderr='', returncode=0, killed_by_token=False\)",
        repr(result),
    ) is not None


def test_path_object_as_argument():
    """Checks that path object as argument."""
    stderr_buffer = StringIO()
    stdout_buffer = StringIO()

    with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
        run(Path(sys.executable), '-c print(777)')

    assert stdout_buffer.getvalue() == '777\n'
    assert stderr_buffer.getvalue() == ''


def test_split_false():
    """Checks that split=False."""
    stderr_buffer = StringIO()
    stdout_buffer = StringIO()

    with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
        run('python', '-c', 'print(777)', split=False)

    assert stdout_buffer.getvalue() == '777\n'
    assert stderr_buffer.getvalue() == ''


@pytest.mark.skipif(sys.platform != 'win32', reason='Windows-only test')
def test_backslashes_preserved_by_default_on_windows():
    """Checks that backslashes preserved by default on windows."""
    result = run(f'{sys.executable} -c "print(777)"', catch_output=True)

    assert result.returncode == 0
    assert result.stdout == '777\n'


@pytest.mark.skipif(sys.platform != 'win32', reason='Windows-only test')
def test_double_backslash_can_be_disabled_on_windows():
    """Checks that double backslash can be disabled on windows."""
    with pytest.raises(RunningCommandError):
        run(f'{sys.executable} -c pass', double_backslash=False)


@pytest.mark.skipif(sys.platform == 'win32', reason='non-Windows test')
def test_double_backslash_can_be_enabled_on_non_windows():
    """Checks that double backslash can be enabled on non windows."""
    result = run(
        sys.executable,
        '-c "import sys; print(sys.argv[1])"',
        r'hello\ world',
        double_backslash=True,
        catch_output=True,
    )

    assert result.returncode == 0
    assert result.stdout.strip() == 'hello\\'


def test_stdout_callback_replaces_default_output():
    """Checks that stdout callback replaces default output."""
    collected = []

    def my_new_stdout(string: str):
        collected.append(string)

    stderr_buffer = StringIO()
    stdout_buffer = StringIO()

    with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
        result = run('python -c "print(\'hello, world!\')"', stdout_callback=my_new_stdout)

    assert collected == ['hello, world!\n']
    assert result.stdout == 'hello, world!\n'
    assert stdout_buffer.getvalue() == ''
    assert stderr_buffer.getvalue() == ''


def test_catch_output_suppresses_console_output():
    """Checks that catch output suppresses console output."""
    stderr_buffer = StringIO()
    stdout_buffer = StringIO()

    with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
        result = run('python -c "print(\'hello, world!\')"', catch_output=True)

    assert stdout_buffer.getvalue() == ''
    assert stderr_buffer.getvalue() == ''
    assert result.stdout == 'hello, world!\n'


def test_logging_on_success():
    """Checks that logging on success."""
    logger = MemoryLogger()

    run('python -c pass', logger=logger, catch_output=True)

    assert len(logger.data.info) == 2
    assert len(logger.data.error) == 0
    assert logger.data.info[0].message == 'The beginning of the execution of the command "python -c pass".'
    assert logger.data.info[1].message == 'The command "python -c pass" has been successfully executed.'


def test_logging_on_error():
    """Checks that logging on error."""
    logger = MemoryLogger()

    run('python -c "raise ValueError"', logger=logger, catch_exceptions=True, catch_output=True)

    assert len(logger.data.info) == 1
    assert len(logger.data.error) == 1
    assert logger.data.info[0].message == 'The beginning of the execution of the command "python -c "raise ValueError"".'
    assert logger.data.error[0].message == 'Error when executing the command "python -c "raise ValueError"".'


def test_running_command_error_message_is_printed():
    """Checks that RunningCommandError message is printed."""
    try:
        run('python -c 1/0', catch_output=True)
    except RunningCommandError as e:
        message = str(e)

    assert message == 'Error when executing the command "python -c 1/0".'


def test_catch_exceptions_with_timeout_returns_result():
    """Checks that catch exceptions with timeout returns result."""
    result = run('python -c "import time; time.sleep(10_000)"', timeout=0.001, catch_exceptions=True)

    assert result.stdout == ''
    assert result.stderr == ''
    assert result.returncode != 0
    assert result.killed_by_token == True


def test_timeout_cancellation_error_carries_result():
    """Checks that TimeoutCancellationError carries result."""
    with pytest.raises(TimeoutCancellationError, match=match('The timeout of 1 seconds has expired.')):
        run('python -c "import time; time.sleep(10_000)"', timeout=1)


def test_timeout_cancellation_error_result_fields():
    """Checks that TimeoutCancellationError result fields."""
    try:
        run('python -c "import time; time.sleep(10_000)"', timeout=0.001)
    except TimeoutCancellationError as e:
        result = e.result

    assert result.stdout == ''
    assert result.stderr == ''
    assert result.returncode != 0
    assert result.killed_by_token == True


def test_timeout_cancellation_error_result_repr():
    """Checks that TimeoutCancellationError result repr."""
    try:
        run('python -c "import time; time.sleep(10_000)"', timeout=0.001)
    except TimeoutCancellationError as e:
        result_repr = repr(e.result)

    assert re.fullmatch(
        r"SubprocessResult\(id='[0-9a-f]{32}', stdout='', stderr='', returncode=-?\d+, killed_by_token=True\)",
        result_repr,
    ) is not None


def test_condition_token_raises_when_cancelled():
    """Checks that ConditionToken raises when cancelled."""
    token = ConditionToken(lambda: True)

    with pytest.raises(ConditionCancellationError):
        run('python -c "import time; time.sleep(10_000)"', token=token)


def test_condition_token_with_catch_exceptions_returns_result():
    """Checks that ConditionToken with catch exceptions returns result."""
    token = ConditionToken(lambda: True)

    result = run('python -c "import time; time.sleep(10_000)"', token=token, catch_exceptions=True)

    assert result.stdout == ''
    assert result.stderr == ''
    assert result.returncode != 0
    assert result.killed_by_token == True


def test_condition_token_result_repr():
    """Checks that ConditionToken result repr."""
    token = ConditionToken(lambda: True)

    result = run('python -c "import time; time.sleep(10_000)"', token=token, catch_exceptions=True)

    assert re.fullmatch(
        r"SubprocessResult\(id='[0-9a-f]{32}', stdout='', stderr='', returncode=-?\d+, killed_by_token=True\)",
        repr(result),
    ) is not None


def test_timeout_raises_timeout_cancellation_error():
    """Checks that timeout raises TimeoutCancellationError."""
    with pytest.raises(TimeoutCancellationError, match=match('The timeout of 1 seconds has expired.')):
        run('python -c "import time; time.sleep(10_000)"', timeout=1)


def test_timeout_error_message():
    """Checks that timeout error message."""
    try:
        run('python -c "import time; time.sleep(10_000)"', timeout=0.001)
    except TimeoutCancellationError as e:
        message = str(e)

    assert 'seconds has expired' in message


def test_timeout_with_catch_exceptions_returns_result():
    """Checks that timeout with catch exceptions returns result."""
    result = run('python -c "import time; time.sleep(10_000)"', timeout=0.001, catch_exceptions=True)

    assert result.stdout == ''
    assert result.stderr == ''
    assert result.returncode != 0
    assert result.killed_by_token == True


def test_timeout_result_repr():
    """Checks that timeout result repr."""
    result = run('python -c "import time; time.sleep(10_000)"', timeout=0.001, catch_exceptions=True)

    assert re.fullmatch(
        r"SubprocessResult\(id='[0-9a-f]{32}', stdout='', stderr='', returncode=-?\d+, killed_by_token=True\)",
        repr(result),
    ) is not None
