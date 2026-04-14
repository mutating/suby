from __future__ import annotations

import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, Thread

from cantok import ConditionToken, SimpleToken
from microbenchmark import Scenario, a

from suby import run

ITERATIONS = 100
PYTHON = Path(sys.executable)


def _cancel_token_after_subprocess_start(token: SimpleToken, marker_file: Path, stop_event: Event) -> None:
    while not stop_event.is_set():
        if marker_file.exists():
            if not stop_event.wait(0.01):
                token.cancel()
            return

        stop_event.wait(0.0001)


def run_with_delayed_simple_token_cancellation() -> None:
    token = SimpleToken()
    stop_event = Event()

    with TemporaryDirectory() as temporary_directory:
        marker_file = Path(temporary_directory) / 'subprocess-started'
        cancellation_thread = Thread(
            target=_cancel_token_after_subprocess_start,
            args=(token, marker_file, stop_event),
        )
        cancellation_thread.start()

        try:
            run(
                PYTHON,
                '-c',
                (
                    'import sys\n'
                    'import time\n'
                    'from pathlib import Path\n'
                    'Path(sys.argv[1]).touch()\n'
                    'time.sleep(1)'
                ),
                marker_file,
                split=False,
                token=token,
                catch_exceptions=True,
                catch_output=True,
            )
        finally:
            stop_event.set()
            cancellation_thread.join()


simple_success = Scenario(
    run,
    a(PYTHON, '-c', 'pass'),
    name='simple_success',
    doc='Runs a minimal successful Python subprocess.',
    number=ITERATIONS,
)

python_version_output = Scenario(
    run,
    a(PYTHON, '-VV', catch_output=True),
    name='python_version_output',
    doc='Runs the current Python executable as a pathlib.Path and prints its detailed version.',
    number=ITERATIONS,
)

string_executable = Scenario(
    run,
    a(sys.executable, '-c', 'pass'),
    name='string_executable',
    doc='Runs a minimal command where the executable is supplied as a string.',
    number=ITERATIONS,
)

path_argument = Scenario(
    run,
    a(PYTHON, '-c "import sys; print(sys.argv[1])"', Path(__file__), catch_output=True),
    name='path_argument',
    doc='Runs a command with a pathlib.Path supplied as one of the subprocess arguments.',
    number=ITERATIONS,
)

multi_line_stdout = Scenario(
    run,
    a(PYTHON, '-c "for i in range(10): print(i)"', catch_output=True),
    name='multi_line_stdout',
    doc='Runs a successful command that writes several short stdout lines.',
    number=ITERATIONS,
)

large_stdout = Scenario(
    run,
    a(PYTHON, '-c "print(\'x\' * 10000)"', catch_output=True),
    name='large_stdout',
    doc='Runs a successful command that writes one larger stdout payload.',
    number=ITERATIONS,
)

stderr_output = Scenario(
    run,
    a(PYTHON, '-c "import sys; sys.stderr.write(\'error line\\\\n\')"', catch_output=True),
    name='stderr_output',
    doc='Runs a successful command that writes to stderr.',
    number=ITERATIONS,
)

mixed_stdout_stderr = Scenario(
    run,
    a(PYTHON, '-c "import sys; print(\'out\'); sys.stderr.write(\'err\\\\n\')"', catch_output=True),
    name='mixed_stdout_stderr',
    doc='Runs a successful command that writes to both stdout and stderr.',
    number=ITERATIONS,
)

many_short_lines = Scenario(
    run,
    a(PYTHON, '-c "for i in range(1000): print(i)"', catch_output=True),
    name='many_short_lines',
    doc='Runs a command that emits many small stdout lines for stream-reading overhead.',
    number=ITERATIONS,
)

moderate_python_work = Scenario(
    run,
    a(PYTHON, '-c "sum(range(100000))"'),
    name='moderate_python_work',
    doc='Runs a subprocess that performs a small amount of CPU work before exiting.',
    number=ITERATIONS,
)

short_sleep = Scenario(
    run,
    a(PYTHON, '-c "import time; time.sleep(0.01)"'),
    name='short_sleep',
    doc='Runs a subprocess that stays alive briefly without producing output.',
    number=20,
)

simple_token_success = Scenario(
    run,
    a(PYTHON, '-c', 'pass', token=SimpleToken()),
    name='simple_token_success',
    doc='Runs a minimal subprocess while checking a non-cancelled SimpleToken.',
    number=ITERATIONS,
)

condition_token_success = Scenario(
    run,
    a(PYTHON, '-c', 'pass', token=ConditionToken(lambda: False)),
    name='condition_token_success',
    doc='Runs a minimal subprocess while polling a ConditionToken that remains active.',
    number=ITERATIONS,
)

cancelled_token_before_start = Scenario(
    run,
    a(
        PYTHON,
        '-c "import time; time.sleep(1)"',
        token=SimpleToken().cancel(),
        catch_exceptions=True,
        catch_output=True,
    ),
    name='cancelled_token_before_start',
    doc='Runs a subprocess with an already-cancelled token and catches the cancellation result.',
    number=20,
)

simple_token_cancel_after_start = Scenario(
    run_with_delayed_simple_token_cancellation,
    name='simple_token_cancel_after_start',
    doc='Starts a subprocess with an active SimpleToken and cancels it shortly after startup.',
    number=20,
)

all = (  # noqa: A001
    simple_success
    + python_version_output
    + string_executable
    + path_argument
    + multi_line_stdout
    + large_stdout
    + stderr_output
    + mixed_stdout_stderr
    + many_short_lines
    + moderate_python_work
    + short_sleep
    + simple_token_success
    + condition_token_success
    + cancelled_token_before_start
    + simple_token_cancel_after_start
)
