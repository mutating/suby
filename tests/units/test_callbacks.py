from contextlib import redirect_stderr, redirect_stdout
from io import StringIO

import pytest

from suby.callbacks import stderr_with_flush, stdout_with_flush


@pytest.mark.parametrize(
    ('callback', 'expected_stdout', 'expected_stderr'),
    [
        (stdout_with_flush, 'kek', ''),
        (stderr_with_flush, '', 'kek'),
    ],
)
def test_output_with_flush(callback, expected_stdout, expected_stderr):
    stderr_buffer = StringIO()
    stdout_buffer = StringIO()

    with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
        callback('kek')

    assert stderr_buffer.getvalue() == expected_stderr
    assert stdout_buffer.getvalue() == expected_stdout
