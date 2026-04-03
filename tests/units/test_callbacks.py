from contextlib import redirect_stderr, redirect_stdout
from io import StringIO

import pytest

from suby.callbacks import stderr_with_flush, stdout_with_flush


class FlushTrackingStringIO(StringIO):
    def __init__(self):
        super().__init__()
        self.flush_calls = 0

    def flush(self):
        self.flush_calls += 1
        super().flush()


@pytest.mark.parametrize(
    ('callback', 'expected_stdout', 'expected_stderr'),
    [
        (stdout_with_flush, 'kek', ''),
        (stderr_with_flush, '', 'kek'),
    ],
)
def test_output_with_flush(callback, expected_stdout, expected_stderr):
    """Default callbacks write the text to their target stream and flush it immediately."""
    stderr_buffer = FlushTrackingStringIO()
    stdout_buffer = FlushTrackingStringIO()

    with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
        callback('kek')

    assert stderr_buffer.getvalue() == expected_stderr
    assert stdout_buffer.getvalue() == expected_stdout
    assert stderr_buffer.flush_calls == (1 if expected_stderr else 0)
    assert stdout_buffer.flush_calls == (1 if expected_stdout else 0)
