from __future__ import annotations

import sys
from functools import partial
from pathlib import Path

from microbenchmark import Scenario

from suby import run

ITERATIONS = 100
PYTHON = Path(sys.executable)
run_catching_output = partial(run, catch_output=True)


simple_success = Scenario(
    run,
    (PYTHON, '-c', 'pass'),
    name='simple_success',
    doc='Runs a minimal successful Python subprocess.',
    number=ITERATIONS,
)

python_version_output = Scenario(
    run_catching_output,
    (PYTHON, '-VV'),
    name='python_version_output',
    doc='Runs the current Python executable as a pathlib.Path and prints its detailed version.',
    number=ITERATIONS,
)

string_executable = Scenario(
    run,
    (sys.executable, '-c', 'pass'),
    name='string_executable',
    doc='Runs a minimal command where the executable is supplied as a string.',
    number=ITERATIONS,
)

path_argument = Scenario(
    run_catching_output,
    (PYTHON, '-c "import sys; print(sys.argv[1])"', Path(__file__)),
    name='path_argument',
    doc='Runs a command with a pathlib.Path supplied as one of the subprocess arguments.',
    number=ITERATIONS,
)

multi_line_stdout = Scenario(
    run_catching_output,
    (PYTHON, '-c "for i in range(10): print(i)"'),
    name='multi_line_stdout',
    doc='Runs a successful command that writes several short stdout lines.',
    number=ITERATIONS,
)

large_stdout = Scenario(
    run_catching_output,
    (PYTHON, '-c "print(\'x\' * 10000)"'),
    name='large_stdout',
    doc='Runs a successful command that writes one larger stdout payload.',
    number=ITERATIONS,
)

stderr_output = Scenario(
    run_catching_output,
    (PYTHON, '-c "import sys; sys.stderr.write(\'error line\\\\n\')"'),
    name='stderr_output',
    doc='Runs a successful command that writes to stderr.',
    number=ITERATIONS,
)

mixed_stdout_stderr = Scenario(
    run_catching_output,
    (PYTHON, '-c "import sys; print(\'out\'); sys.stderr.write(\'err\\\\n\')"'),
    name='mixed_stdout_stderr',
    doc='Runs a successful command that writes to both stdout and stderr.',
    number=ITERATIONS,
)

many_short_lines = Scenario(
    run_catching_output,
    (PYTHON, '-c "for i in range(1000): print(i)"'),
    name='many_short_lines',
    doc='Runs a command that emits many small stdout lines for stream-reading overhead.',
    number=ITERATIONS,
)

moderate_python_work = Scenario(
    run,
    (PYTHON, '-c "sum(range(100000))"'),
    name='moderate_python_work',
    doc='Runs a subprocess that performs a small amount of CPU work before exiting.',
    number=ITERATIONS,
)

short_sleep = Scenario(
    run,
    (PYTHON, '-c "import time; time.sleep(0.01)"'),
    name='short_sleep',
    doc='Runs a subprocess that stays alive briefly without producing output.',
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
)
