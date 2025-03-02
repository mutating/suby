import os
import sys
from io import StringIO
import platform
from contextlib import redirect_stdout, redirect_stderr

import suby
import pytest


@pytest.mark.skipif(platform.system() == 'Windows', reason='Windows and not windows have different rules of escaping characters.')
def test_run_hello_world_not_windows():
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
    from mslex import quote, split  # type: ignore[import-not-found]

    stderr_buffer = StringIO()
    stdout_buffer = StringIO()

    python_path = os.path.normcase(sys.executable)
    script_string = quote('"print(\"Hello, world!\n\")"')

    print('result', quote(f'{python_path} -c "print(\"Hello, world!\n\")"'))
    print('splitted result', split(quote(f'{python_path} -c "print(\"Hello, world!\n\")"')))
    print('splitted without normcasing result', split(quote(f'{sys.executable} -c "print(\"Hello, world!\n\")"')))
    print('splitted in a good way', split(quote(f'python -c {script_string}'), like_cmd=False, ucrt=True))

    with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
        #result = suby('python -c "print^(\'hello, world^!\'^)"', catch_exceptions=True)


        result = suby(quote(f'{python_path} -c "print(\"Hello, world!\n\")"'), catch_exceptions=True)
        print(result)

    print(result)

    print(stderr_buffer.getvalue())

    #assert stderr_buffer.getvalue() == ''
    assert stdout_buffer.getvalue() == 'hello, world!\n'

    assert result.stdout == 'hello, world!\n'
    assert result.stderr == ''
    assert result.returncode == 0
    assert not result.killed_by_token
