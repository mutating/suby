from suby.subprocess_result import SubprocessResult


class RunningCommandError(Exception):
    def __init__(self, message: str, subprocess_result: SubprocessResult) -> None:
        self.result = subprocess_result
        super().__init__(message)


class WrongCommandError(Exception):
    ...
