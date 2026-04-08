import pytest

from suby.errors import RunningCommandError, WrongCommandError
from suby.subprocess_result import SubprocessResult


def test_init_exception_and_raise():
    """RunningCommandError preserves the message and the attached SubprocessResult when raised."""
    result = SubprocessResult()
    with pytest.raises(RunningCommandError) as exc_info:
        raise RunningCommandError('kek', result)

    assert str(exc_info.value) == 'kek'
    assert exc_info.value.result is result


def test_wrong_command_error_has_no_result():
    """WrongCommandError is raised before any process starts, so it has no attached SubprocessResult."""
    error = WrongCommandError('test message')

    assert not hasattr(error, 'result')
