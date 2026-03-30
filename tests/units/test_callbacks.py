from contextlib import redirect_stderr, redirect_stdout
from io import StringIO

from suby.callbacks import stderr_with_flush, stdout_with_flush


def test_output_to_stdout():
    stderr_buffer = StringIO()
    stdout_buffer = StringIO()

    with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
        stdout_with_flush('kek')

    assert stderr_buffer.getvalue() == ''
    assert stdout_buffer.getvalue() == 'kek'


def test_output_to_stderr():
    stderr_buffer = StringIO()
    stdout_buffer = StringIO()

    with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
        stderr_with_flush('kek')

    assert stderr_buffer.getvalue() == 'kek'
    assert stdout_buffer.getvalue() == ''
