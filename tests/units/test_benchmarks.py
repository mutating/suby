import time
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
    benchmarks.simple_token_success,
    benchmarks.condition_token_success,
    benchmarks.cancelled_token_before_start,
    benchmarks.condition_token_cancel_after_start,
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
        scenario._arguments,
        name=scenario.name,
        doc=scenario.doc,
        number=1,
    ).run()


def test_benchmarks_are_grouped_scenarios():
    """Verify `SCENARIOS` contains `Scenario` objects and `benchmarks.all` is the ordered `ScenarioGroup` of exactly those scenarios."""
    assert all(isinstance(scenario, Scenario) for scenario in SCENARIOS)
    assert isinstance(benchmarks.all, ScenarioGroup)
    assert benchmarks.all._scenarios == SCENARIOS


def test_benchmark_names_match_variable_names():
    """Ensure each scenario's `name` matches the `benchmarks` attribute bound to that same object."""
    for scenario in SCENARIOS:
        assert getattr(benchmarks, scenario.name) is scenario


def test_benchmark_docs_are_present():
    """Every scenario in `SCENARIOS` has a non-empty `scenario.doc` value."""
    for scenario in SCENARIOS:
        assert scenario.doc


def test_benchmark_iteration_counts():
    """
    Pin `Scenario.number` for every benchmark scenario.

    Most scenarios use `benchmarks.ITERATIONS`, while `short_sleep`, `cancelled_token_before_start`, and `condition_token_cancel_after_start` stay capped at 20 to keep the suite practical.
    """
    for scenario in SCENARIOS:
        if scenario in (
            benchmarks.short_sleep,
            benchmarks.cancelled_token_before_start,
            benchmarks.condition_token_cancel_after_start,
        ):
            assert scenario.number == 20
        else:
            assert scenario.number == benchmarks.ITERATIONS


def test_all_benchmarks_run_once():
    """Smoke-test that every scenario in `SCENARIOS` is executable.

    Each scenario is cloned with `number=1` so the real run path is covered without turning this unit test into a performance benchmark.
    """
    for scenario in SCENARIOS:
        run_once(scenario)


def test_output_benchmarks_do_not_write_to_console(capsys):
    """
    Ensure scenarios from `OUTPUT_SCENARIOS` do not forward stdout or stderr to the console.

    The test runs one-iteration clones and asserts pytest captured no stdout or stderr.
    """
    for scenario in OUTPUT_SCENARIOS:
        run_once(scenario)

    captured = capsys.readouterr()

    assert captured.out == ''
    assert captured.err == ''


def test_benchmarks_use_run_directly():
    """Ensure normal benchmark `Scenario.function` values point to `suby.run` directly.

    Scenario-specific behavior should live in args/kwargs; only `condition_token_cancel_after_start` may use a helper to cancel after subprocess startup.
    """
    for scenario in SCENARIOS:
        function = scenario.function

        if function is benchmarks.run_with_delayed_condition_token_cancellation:
            assert scenario is benchmarks.condition_token_cancel_after_start
        else:
            assert function is run


def test_key_benchmark_arguments():
    """
    Lock down declared arguments for key benchmark scenarios.

    This verifies command forms, output capture, token kwargs, and the delayed-cancellation scenario's lack of static arguments.
    """
    assert benchmarks.simple_success._arguments.args == (benchmarks.PYTHON, '-c', 'pass')
    assert benchmarks.python_version_output._arguments.args == (benchmarks.PYTHON, '-VV')
    assert benchmarks.python_version_output._arguments.kwargs == {'catch_output': True}
    assert isinstance(benchmarks.string_executable._arguments.args[0], str)
    assert isinstance(benchmarks.path_argument._arguments.args[-1], Path)
    assert benchmarks.path_argument._arguments.kwargs == {'catch_output': True}
    assert benchmarks.multi_line_stdout._arguments.kwargs == {'catch_output': True}
    assert benchmarks.large_stdout._arguments.kwargs == {'catch_output': True}
    assert benchmarks.stderr_output._arguments.kwargs == {'catch_output': True}
    assert benchmarks.mixed_stdout_stderr._arguments.kwargs == {'catch_output': True}
    assert benchmarks.many_short_lines._arguments.kwargs == {'catch_output': True}
    assert benchmarks.short_sleep._arguments.args == (
        benchmarks.PYTHON,
        '-c "import time; time.sleep(0.01)"',
    )
    assert benchmarks.simple_token_success._arguments.args == (benchmarks.PYTHON, '-c', 'pass')
    assert set(benchmarks.simple_token_success._arguments.kwargs) == {'token'}
    assert benchmarks.condition_token_success._arguments.args == (benchmarks.PYTHON, '-c', 'pass')
    assert set(benchmarks.condition_token_success._arguments.kwargs) == {'token'}
    assert benchmarks.cancelled_token_before_start._arguments.args == (
        benchmarks.PYTHON,
        '-c "import time; time.sleep(1)"',
    )
    assert set(benchmarks.cancelled_token_before_start._arguments.kwargs) == {
        'token',
        'catch_exceptions',
        'catch_output',
    }
    assert benchmarks.cancelled_token_before_start._arguments.kwargs['catch_exceptions'] is True
    assert benchmarks.cancelled_token_before_start._arguments.kwargs['catch_output'] is True
    assert benchmarks.condition_token_cancel_after_start._arguments is None


def test_delayed_condition_token_cancellation_timer_starts_after_subprocess_marker(monkeypatch):
    """
    Verify delayed token cancellation is based on subprocess-start marker age.

    A fake `run` observes the token before marker creation, shortly after marker creation, and after the 10 ms cancellation delay.
    """
    observed_states = []

    def fake_run(*arguments, **kwargs):
        token = kwargs['token']
        marker_file = arguments[-1]

        time.sleep(0.02)
        observed_states.append(bool(token))

        marker_file.touch()
        marker_mtime_ns = marker_file.stat().st_mtime_ns
        times = iter(
            (
                marker_mtime_ns + 1_000_000,
                marker_mtime_ns + 20_000_000,
            ),
        )
        monkeypatch.setattr(benchmarks, 'time_ns', lambda: next(times))

        observed_states.append(bool(token))
        observed_states.append(bool(token))

    monkeypatch.setattr(benchmarks, 'run', fake_run)

    benchmarks.run_with_delayed_condition_token_cancellation()

    assert observed_states == [True, True, False]
