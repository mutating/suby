from pathlib import Path

from suby import run
from suby.run import convert_arguments, split_argument


def test_bench_split_argument_simple(benchmark):
    """Benchmark splitting a simple command string."""
    benchmark(split_argument, 'python -c pass', False)


def test_bench_split_argument_with_quotes(benchmark):
    """Benchmark splitting a command string with quoted arguments."""
    benchmark(split_argument, 'python -c "print(\'hello, world!\')"', False)


def test_bench_convert_arguments_single_string(benchmark):
    """Benchmark converting a single string argument."""
    benchmark(convert_arguments, ('python -c pass',), True, False)


def test_bench_convert_arguments_multiple_strings(benchmark):
    """Benchmark converting multiple string arguments."""
    benchmark(convert_arguments, ('python', '-c', 'print(777)'), True, False)


def test_bench_convert_arguments_with_path(benchmark):
    """Benchmark converting arguments that include a Path object."""
    benchmark(convert_arguments, (Path('/usr/bin/python'), '-c pass'), True, False)


def test_bench_convert_arguments_no_split(benchmark):
    """Benchmark converting arguments with split disabled."""
    benchmark(convert_arguments, ('python', '-c', 'print(777)'), False, False)


def test_bench_run_simple_command(benchmark):
    """Benchmark running a simple subprocess end-to-end."""
    result = benchmark(run, 'python -c pass', catch_output=True)
    assert result.returncode == 0
