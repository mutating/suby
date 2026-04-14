import pytest

from suby import benchmarks

SCENARIOS = [
    benchmarks.simple_success,
    benchmarks.python_version_output,
    benchmarks.string_executable,
    benchmarks.path_argument,
    benchmarks.multi_line_stdout,
    benchmarks.large_stdout,
    benchmarks.stderr_output,
    benchmarks.mixed_stdout_stderr,
    benchmarks.many_short_lines,
    benchmarks.moderate_python_work,
    benchmarks.short_sleep,
    benchmarks.simple_token_success,
    benchmarks.condition_token_success,
    benchmarks.cancelled_token_before_start,
    benchmarks.simple_token_cancel_after_start,
]


@pytest.mark.benchmark
@pytest.mark.parametrize('scenario', SCENARIOS, ids=[scenario.name for scenario in SCENARIOS])
def test_benchmark_scenario(benchmark, scenario):
    benchmark(scenario._call_once)
