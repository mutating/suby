from cantok import ConditionCancellationError as CantokConditionCancellationError
from cantok import TimeoutCancellationError as CantokTimeoutCancellationError

from suby.subprocess_result import SubprocessResult


class ConditionCancellationError(CantokConditionCancellationError):
    result: SubprocessResult  # pragma: no cover


class RunningCommandError(Exception):
    def __init__(self, message: str, subprocess_result: SubprocessResult) -> None:
        self.result = subprocess_result
        super().__init__(message)


class TimeoutCancellationError(CantokTimeoutCancellationError):
    result: SubprocessResult  # pragma: no cover


class WrongCommandError(Exception):
    ...
