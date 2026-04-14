import pytest

from suby.errors import (
    EnvironmentVariablesConflict,
    RunningCommandError,
    WrongCommandError,
    WrongDirectoryError,
)
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


def test_environment_variables_conflict_is_value_error_without_result():
    """EnvironmentVariablesConflict is a pre-start validation error and does not carry a SubprocessResult."""
    error = EnvironmentVariablesConflict('test message')

    assert isinstance(error, ValueError)
    assert str(error) == 'test message'
    assert not hasattr(error, 'result')


def test_wrong_directory_error_is_value_error_without_result():
    """WrongDirectoryError is a pre-start validation error and does not carry a SubprocessResult."""
    error = WrongDirectoryError('bad directory')

    assert isinstance(error, ValueError)
    assert str(error) == 'bad directory'
    assert not hasattr(error, 'result')
