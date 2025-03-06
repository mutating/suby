from io import StringIO
import platform
from contextlib import redirect_stdout, redirect_stderr

import suby
import pytest


@pytest.mark.skipif(platform.system() == 'Windows', reason='Windows and not windows have different rules of escaping characters.')
def test_run_hello_world_not_for_windows():
    stderr_buffer = StringIO()
    stdout_buffer = StringIO()

    with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
        result = suby('python -c "print(\'hello, world!\')"')

    assert stderr_buffer.getvalue() == ''
    assert stdout_buffer.getvalue() == 'hello, world!\n'

    assert result.stdout == 'hello, world!\n'
    assert result.stderr == ''
    assert result.returncode == 0
    assert not result.killed_by_token


@pytest.mark.skipif(platform.system() != 'Windows', reason='Windows and not windows have different rules of escaping characters.')
def test_run_hello_world_windows():

    stderr_buffer = StringIO()
    stdout_buffer = StringIO()

    with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
        result = suby('python', '-c', 'print("hello, world!")')

    assert stderr_buffer.getvalue() == ''
    assert stdout_buffer.getvalue() == 'hello, world!\n'

    assert result.stdout == 'hello, world!\n'
    assert result.stderr == ''
    assert result.returncode == 0
    assert not result.killed_by_token
