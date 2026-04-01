import sys


def stderr_with_flush(string: str) -> None:
    sys.stderr.write(string)
    sys.stderr.flush()

def stdout_with_flush(string: str) -> None:
    print(string, end='')  # noqa: T201
    sys.stdout.flush()
