from functools import partial
from pathlib import Path

from microbenchmark import Scenario, ScenarioGroup

from suby import benchmarks, run

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
]

OUTPUT_SCENARIOS = [
    benchmarks.python_version_output,
    benchmarks.path_argument,
    benchmarks.multi_line_stdout,
    benchmarks.large_stdout,
    benchmarks.stderr_output,
    benchmarks.mixed_stdout_stderr,
    benchmarks.many_short_lines,
]


def run_once(scenario: Scenario) -> None:
    Scenario(
        scenario.function,
        scenario._args,
        name=scenario.name,
        doc=scenario.doc,
        number=1,
    ).run()


def test_benchmarks_are_grouped_scenarios():
    assert all(isinstance(scenario, Scenario) for scenario in SCENARIOS)
    assert isinstance(benchmarks.all, ScenarioGroup)
    assert benchmarks.all._scenarios == SCENARIOS


def test_benchmark_names_match_variable_names():
    for scenario in SCENARIOS:
        assert getattr(benchmarks, scenario.name) is scenario


def test_benchmark_docs_are_present():
    for scenario in SCENARIOS:
        assert scenario.doc


def test_benchmark_iteration_counts():
    for scenario in SCENARIOS:
        if scenario is benchmarks.short_sleep:
            assert scenario.number == 20
        else:
            assert scenario.number == benchmarks.ITERATIONS


def test_all_benchmarks_run_once():
    for scenario in SCENARIOS:
        run_once(scenario)


def test_output_benchmarks_do_not_write_to_console(capsys):
    for scenario in OUTPUT_SCENARIOS:
        run_once(scenario)

    captured = capsys.readouterr()

    assert captured.out == ''
    assert captured.err == ''


def test_benchmarks_use_run_directly():
    for scenario in SCENARIOS:
        function = scenario.function

        if isinstance(function, partial):
            assert function.func is run
        else:
            assert function is run


def test_key_benchmark_arguments():
    assert benchmarks.simple_success._args == [benchmarks.PYTHON, '-c', 'pass']
    assert benchmarks.python_version_output._args == [benchmarks.PYTHON, '-VV']
    assert isinstance(benchmarks.string_executable._args[0], str)
    assert isinstance(benchmarks.path_argument._args[-1], Path)
    assert benchmarks.short_sleep._args == [
        benchmarks.PYTHON,
        '-c "import time; time.sleep(0.01)"',
    ]
