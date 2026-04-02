import importlib
import re
import sys
import time
from pathlib import Path, PurePath
from threading import Barrier, Event, Thread
from typing import Any, List, cast
from unittest.mock import patch

import pytest
from cantok import AbstractToken, ConditionToken, DefaultToken, TimeoutCancellationError
from emptylog import MemoryLogger

from suby import RunningCommandError, WrongCommandError, run
from suby.subprocess_result import SubprocessResult

_run_module = importlib.import_module('suby.run')


def _python_print_argv_script() -> str:
    return 'import json, sys; print(json.dumps(sys.argv[1:]))'


class StringLikeObject:
    def __str__(self) -> str:
        return 'python'


class ResultBearingError(RuntimeError):
    pass


class NeverCancelsToken(AbstractToken):
    def _superpower(self) -> bool:
        return True

    def _get_superpower_exception_message(self) -> str:
        return 'never cancels'

    def _text_representation_of_superpower(self) -> str:
        return 'never cancels'

    def __bool__(self) -> bool:
        return True

    def __iadd__(self, other: object) -> 'NeverCancelsToken':
        return self

    def check(self) -> None:
        return None


def test_empty_string_command_rejected() -> None:
    with pytest.raises(WrongCommandError):
        run('')


def test_two_empty_string_commands_rejected() -> None:
    with pytest.raises(WrongCommandError):
        run('', '')


def test_whitespace_only_command_rejected() -> None:
    with pytest.raises(WrongCommandError):
        run('   ')


def test_single_double_quote_rejected() -> None:
    with pytest.raises(WrongCommandError):
        run('"')


def test_single_single_quote_rejected() -> None:
    with pytest.raises(WrongCommandError):
        run("'")


def test_broken_inline_quote_rejected() -> None:
    with pytest.raises(WrongCommandError):
        run('python -c "')


def test_very_long_command_string_is_handled() -> None:
    payload = 'x' * 100_000
    result = run(sys.executable, '-c', f'print("{payload}")', split=False, catch_output=True)
    assert result.stdout == payload + '\n'


def test_command_with_nul_byte_is_rejected_consistently() -> None:
    with pytest.raises((RunningCommandError, ValueError)):
        run('abc\0def')


def test_empty_path_object_is_rejected_consistently() -> None:
    with pytest.raises((RunningCommandError, PermissionError)):
        run(Path(''))  # noqa: PTH201


def test_current_directory_path_object_is_rejected_consistently() -> None:
    with pytest.raises(RunningCommandError) as exc_info:
        run(Path('.'))  # noqa: PTH201
    assert isinstance(exc_info.value.__cause__, OSError)


def test_path_with_spaces_and_special_characters_executes_via_path_object(tmp_path: Path) -> None:
    script = tmp_path / 'dir with spaces #and(parens)'
    script.mkdir()
    executable = script / 'echo.py'
    executable.write_text('print("ok")')
    result = run(Path(sys.executable), executable, split=False, catch_output=True)
    assert result.stdout == 'ok\n'


def test_run_uses_dedicated_stdout_thread() -> None:
    with patch.object(_run_module, 'run_stdout_thread', wraps=_run_module.run_stdout_thread) as wrapped:
        result = run(sys.executable, '-c', 'print("ok")', split=False, catch_output=True)

    assert result.stdout == 'ok\n'
    wrapped.assert_called_once()


def test_kill_process_if_running_ignores_process_lookup_error() -> None:
    class MockProcess:
        def poll(self) -> None:
            return None

        def kill(self) -> None:
            raise ProcessLookupError('already exited')

    _run_module.kill_process_if_running(MockProcess())  # type: ignore[arg-type]


def test_path_object_that_looks_like_flag_is_treated_as_plain_argument() -> None:
    result = run(
        Path(sys.executable),
        Path('-c'),
        Path(_python_print_argv_script()),
        split=False,
        catch_output=True,
    )
    assert result.returncode == 0


def test_bytes_argument_rejected() -> None:
    with pytest.raises(TypeError):
        run(b'python')  # type: ignore[arg-type]


def test_bytearray_argument_rejected() -> None:
    with pytest.raises(TypeError):
        run(bytearray(b'python'))  # type: ignore[arg-type]


def test_purepath_argument_rejected() -> None:
    with pytest.raises(TypeError):
        run(PurePath('python'))  # type: ignore[arg-type]


def test_string_like_object_rejected() -> None:
    with pytest.raises(TypeError):
        run(StringLikeObject())  # type: ignore[arg-type]


def test_split_false_does_not_split_single_string_command() -> None:
    with pytest.raises(RunningCommandError):
        run('python -c "print(1)"', split=False)


def test_split_false_preserves_spaces_inside_argument() -> None:
    result = run(
        sys.executable,
        '-c',
        _python_print_argv_script(),
        'hello world',
        split=False,
        catch_output=True,
    )
    assert result.stdout == '["hello world"]\n'


def test_split_false_with_empty_executable_is_rejected() -> None:
    with pytest.raises((RunningCommandError, ValueError)):
        run('', split=False)


def test_split_false_with_path_object_still_executes() -> None:
    result = run(Path(sys.executable), '-c', 'print("ok")', split=False, catch_output=True)
    assert result.stdout == 'ok\n'


def test_double_backslash_has_no_effect_when_split_is_false() -> None:
    result = run(
        sys.executable,
        '-c',
        _python_print_argv_script(),
        r'hello\ world',
        split=False,
        double_backslash=True,
        catch_output=True,
    )
    assert result.stdout == '["hello\\\\ world"]\n'


def test_trailing_backslash_argument_is_preserved() -> None:
    result = run(
        sys.executable,
        '-c',
        _python_print_argv_script(),
        'endswith\\',
        split=False,
        catch_output=True,
    )
    assert result.stdout == '["endswith\\\\"]\n'


def test_path_with_spaces_and_backslashes_is_preserved_with_split_false() -> None:
    result = run(
        sys.executable,
        '-c',
        _python_print_argv_script(),
        r'folder with spaces\name',
        split=False,
        catch_output=True,
    )
    assert result.stdout == '["folder with spaces\\\\name"]\n'


@pytest.mark.skipif(sys.platform != 'win32', reason='Windows-only UNC path semantics')
def test_unc_path_survives_windows_processing() -> None:
    result = run(r'\\server\share\python.exe -c pass', catch_exceptions=True)
    assert result.stderr is not None


def test_backslash_before_quote_is_preserved_when_split_disabled() -> None:
    result = run(
        sys.executable,
        '-c',
        _python_print_argv_script(),
        r'value\"quoted',
        split=False,
        catch_output=True,
    )
    assert result.stdout == '["value\\\\\\"quoted"]\n'


@pytest.mark.skipif(sys.platform == 'win32', reason='Non-Windows-only behavior')
def test_double_backslash_true_changes_non_windows_argument_shape() -> None:
    result = run(
        sys.executable,
        '-c "import sys; print(sys.argv[1:])"',
        r'hello\ world',
        double_backslash=True,
        catch_output=True,
    )
    assert result.stdout != "['hello world']\n"


def test_mixed_argument_joining_shape_is_explicit() -> None:
    result = run(
        sys.executable,
        '-c',
        _python_print_argv_script(),
        r'hello\ world',
        'two words',
        split=False,
        catch_output=True,
    )
    assert result.stdout == '["hello\\\\ world", "two words"]\n'


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX-only permission semantics')
def test_non_executable_file_is_normalized_via_running_command_error(tmp_path: Path) -> None:
    script = tmp_path / 'script.sh'
    script.write_text('echo hello\n')
    script.chmod(0o644)
    with pytest.raises(RunningCommandError) as exc_info:
        run(str(script))
    assert isinstance(exc_info.value.__cause__, PermissionError)


def test_directory_as_executable_is_normalized() -> None:
    with pytest.raises(RunningCommandError):
        run(str(Path.cwd()))


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX-only exec format semantics')
def test_exec_format_error_is_normalized_via_running_command_error(tmp_path: Path) -> None:
    script = tmp_path / 'script-without-shebang'
    script.write_text('echo hello\n')
    script.chmod(0o755)
    with pytest.raises(RunningCommandError) as exc_info:
        run(str(script))
    assert type(exc_info.value.__cause__) is OSError


def test_missing_parent_path_is_normalized() -> None:
    missing = Path.cwd() / 'missing-parent-dir' / 'missing-command'
    with pytest.raises(RunningCommandError) as exc_info:
        run(str(missing))
    assert isinstance(exc_info.value.__cause__, FileNotFoundError)


def test_missing_path_command_is_normalized() -> None:
    with pytest.raises(RunningCommandError) as exc_info:
        run('definitely_missing_command_for_suby_tests')
    assert isinstance(exc_info.value.__cause__, FileNotFoundError)


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX-only shebang semantics')
def test_missing_shebang_interpreter_is_normalized(tmp_path: Path) -> None:
    script = tmp_path / 'script.py'
    script.write_text('#!/definitely/missing/interpreter\nprint("hello")\n')
    script.chmod(0o755)
    with pytest.raises(RunningCommandError) as exc_info:
        run(str(script))
    assert isinstance(exc_info.value.__cause__, OSError)


def test_stdout_callback_exception_bubbles_up() -> None:
    def callback(_: str) -> None:
        raise RuntimeError('stdout callback exploded')

    with pytest.raises(RuntimeError, match='stdout callback exploded'):
        run(sys.executable, '-c', 'print("hello")', split=False, stdout_callback=callback)


def test_stdout_callback_exception_kills_process_and_attaches_result() -> None:
    def callback(_: str) -> None:
        raise RuntimeError('stdout callback exploded')

    start = time.perf_counter()
    with pytest.raises(RuntimeError, match='stdout callback exploded') as exc_info:
        run(
            sys.executable,
            '-c',
            'import time; print("hello", flush=True); time.sleep(5)',
            split=False,
            stdout_callback=callback,
        )
    elapsed = time.perf_counter() - start

    assert elapsed < 2
    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert exc_info.value.result.stdout == 'hello\n'  # type: ignore[attr-defined]
    assert isinstance(exc_info.value.result.stderr, str)  # type: ignore[attr-defined]
    assert isinstance(exc_info.value.result.returncode, int)  # type: ignore[attr-defined]
    assert exc_info.value.result.returncode != 0  # type: ignore[attr-defined]
    assert exc_info.value.result.killed_by_token is False  # type: ignore[attr-defined]


def test_stdout_callback_exception_after_process_exit_keeps_success_returncode() -> None:
    def callback(_: str) -> None:
        time.sleep(0.1)
        raise RuntimeError('stdout callback exploded after exit')

    start = time.perf_counter()
    with pytest.raises(RuntimeError, match='stdout callback exploded after exit') as exc_info:
        run(
            sys.executable,
            '-c',
            'print("hello", flush=True)',
            split=False,
            stdout_callback=callback,
        )
    elapsed = time.perf_counter() - start

    assert elapsed < 2
    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert exc_info.value.result.stdout == 'hello\n'  # type: ignore[attr-defined]
    assert exc_info.value.result.stderr == ''  # type: ignore[attr-defined]
    assert exc_info.value.result.returncode == 0  # type: ignore[attr-defined]
    assert exc_info.value.result.killed_by_token is False  # type: ignore[attr-defined]


def test_stderr_callback_exception_bubbles_up() -> None:
    def callback(_: str) -> None:
        raise RuntimeError('stderr callback exploded')

    with pytest.raises(RuntimeError, match='stderr callback exploded'):
        run(sys.executable, '-c', 'import sys; sys.stderr.write("hello\\n")', split=False, stderr_callback=callback)


def test_stderr_callback_exception_kills_process_and_attaches_result() -> None:
    def callback(_: str) -> None:
        raise RuntimeError('stderr callback exploded')

    start = time.perf_counter()
    with pytest.raises(RuntimeError, match='stderr callback exploded') as exc_info:
        run(
            sys.executable,
            '-c',
            'import sys, time; sys.stderr.write("hello\\n"); sys.stderr.flush(); time.sleep(5)',
            split=False,
            stderr_callback=callback,
        )
    elapsed = time.perf_counter() - start

    assert elapsed < 2
    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert isinstance(exc_info.value.result.stdout, str)  # type: ignore[attr-defined]
    assert exc_info.value.result.stderr == 'hello\n'  # type: ignore[attr-defined]
    assert isinstance(exc_info.value.result.returncode, int)  # type: ignore[attr-defined]
    assert exc_info.value.result.returncode != 0  # type: ignore[attr-defined]
    assert exc_info.value.result.killed_by_token is False  # type: ignore[attr-defined]


def test_stderr_callback_exception_after_process_exit_keeps_success_returncode() -> None:
    def callback(_: str) -> None:
        time.sleep(0.1)
        raise RuntimeError('stderr callback exploded after exit')

    start = time.perf_counter()
    with pytest.raises(RuntimeError, match='stderr callback exploded after exit') as exc_info:
        run(
            sys.executable,
            '-c',
            'import sys; sys.stderr.write("hello\\n"); sys.stderr.flush()',
            split=False,
            stderr_callback=callback,
        )
    elapsed = time.perf_counter() - start

    assert elapsed < 2
    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert exc_info.value.result.stdout == ''  # type: ignore[attr-defined]
    assert exc_info.value.result.stderr == 'hello\n'  # type: ignore[attr-defined]
    assert exc_info.value.result.returncode == 0  # type: ignore[attr-defined]
    assert exc_info.value.result.killed_by_token is False  # type: ignore[attr-defined]


def test_coordinator_does_not_lose_failure_when_process_exit_and_failure_signals_race() -> None:
    process_exited = Event()
    synchronized_release = Barrier(2)

    def controlled_waiter(process: Any, state: Any) -> None:
        _run_module.wait_for_process_exit(process, None)
        process_exited.set()
        synchronized_release.wait(timeout=1)
        state.process_exit_event.set()
        state.wake_event.set()

    def stdout_callback(_: str) -> None:
        if not process_exited.wait(timeout=1):
            raise RuntimeError('coordinated race setup failed')
        synchronized_release.wait(timeout=1)
        raise RuntimeError('stdout callback exploded in coordinated race')

    with patch.object(_run_module, 'wait_for_process_exit_and_signal', new=controlled_waiter), \
         pytest.raises(RuntimeError, match='stdout callback exploded in coordinated race') as exc_info:
        run(
            sys.executable,
            '-c',
            'print("hello", flush=True)',
            split=False,
            stdout_callback=stdout_callback,
        )

    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert exc_info.value.result.stdout == 'hello\n'  # type: ignore[attr-defined]
    assert exc_info.value.result.stderr == ''  # type: ignore[attr-defined]
    assert exc_info.value.result.returncode == 0  # type: ignore[attr-defined]


def test_coordinator_does_not_lose_token_error_when_process_exit_and_failure_signals_race() -> None:
    process_exited = Event()
    synchronized_release = Barrier(2)

    def controlled_waiter(process: Any, state: Any) -> None:
        _run_module.wait_for_process_exit(process, None)
        process_exited.set()
        synchronized_release.wait(timeout=1)
        state.process_exit_event.set()
        state.wake_event.set()

    def boom_in_race() -> bool:
        if not process_exited.is_set():
            return False
        synchronized_release.wait(timeout=1)
        raise RuntimeError('token exploded in coordinated race')

    token = ConditionToken(boom_in_race, suppress_exceptions=False)

    with patch.object(_run_module, 'wait_for_process_exit_and_signal', new=controlled_waiter), \
         pytest.raises(RuntimeError, match='token exploded in coordinated race') as exc_info:
        run(
            sys.executable,
            '-c',
            'import time; time.sleep(0.02)',
            split=False,
            token=token,
        )

    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert exc_info.value.result.stdout == ''  # type: ignore[attr-defined]
    assert exc_info.value.result.stderr == ''  # type: ignore[attr-defined]
    assert isinstance(exc_info.value.result.returncode, int)  # type: ignore[attr-defined]


def test_timeout_thread_can_race_with_recorded_stdout_failure_before_main_thread_handles_it() -> None:
    original_raise_failure_if_needed = _run_module.raise_failure_if_needed
    failure_recorded = Event()
    delay_once = Event()

    def delayed_raise_failure_if_needed(process: Any, reader_threads: Any, state: Any) -> None:
        if state.failure_state.error is not None and not delay_once.is_set():
            failure_recorded.set()
            delay_once.set()
            time.sleep(0.05)
        return original_raise_failure_if_needed(process, reader_threads, state)

    def stdout_callback(_: str) -> None:
        raise RuntimeError('stdout callback exploded before timeout handling')

    with patch.object(_run_module, 'raise_failure_if_needed', new=delayed_raise_failure_if_needed):
        with pytest.raises(RuntimeError, match='stdout callback exploded before timeout handling') as exc_info:
            run(
                sys.executable,
                '-c',
                'import time; print("hello", flush=True); time.sleep(5)',
                split=False,
                stdout_callback=stdout_callback,
                timeout=0.01,
            )

    assert failure_recorded.is_set()
    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert exc_info.value.result.stdout == 'hello\n'  # type: ignore[attr-defined]
    assert isinstance(exc_info.value.result.stderr, str)  # type: ignore[attr-defined]
    assert isinstance(exc_info.value.result.returncode, int)  # type: ignore[attr-defined]


def test_timeout_thread_can_race_with_recorded_token_failure_before_main_thread_handles_it() -> None:
    original_raise_failure_if_needed = _run_module.raise_failure_if_needed
    failure_recorded = Event()
    delay_once = Event()

    def delayed_raise_failure_if_needed(process: Any, reader_threads: Any, state: Any) -> None:
        if state.failure_state.error is not None and not delay_once.is_set():
            failure_recorded.set()
            delay_once.set()
            time.sleep(0.05)
        return original_raise_failure_if_needed(process, reader_threads, state)

    def boom() -> bool:
        raise RuntimeError('token exploded before timeout handling')

    token = ConditionToken(boom, suppress_exceptions=False)

    with patch.object(_run_module, 'raise_failure_if_needed', new=delayed_raise_failure_if_needed):
        with pytest.raises(RuntimeError, match='token exploded before timeout handling') as exc_info:
            run(
                sys.executable,
                '-c',
                'import time; time.sleep(5)',
                split=False,
                token=token,
                timeout=0.01,
            )

    assert failure_recorded.is_set()
    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert exc_info.value.result.stdout == ''  # type: ignore[attr-defined]
    assert exc_info.value.result.stderr == ''  # type: ignore[attr-defined]
    assert isinstance(exc_info.value.result.returncode, int)  # type: ignore[attr-defined]


def test_parallel_stdout_and_stderr_callback_failures_raise_one_of_them() -> None:
    def stdout_callback(_: str) -> None:
        raise RuntimeError('stdout callback exploded')

    def stderr_callback(_: str) -> None:
        raise RuntimeError('stderr callback exploded')

    with pytest.raises(RuntimeError, match=r'(stdout|stderr) callback exploded') as exc_info:
        run(
            sys.executable,
            '-c',
            'import sys, time; print("out", flush=True); sys.stderr.write("err\\n"); sys.stderr.flush(); time.sleep(5)',
            split=False,
            stdout_callback=stdout_callback,
            stderr_callback=stderr_callback,
        )

    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert isinstance(exc_info.value.result.stdout, str)  # type: ignore[attr-defined]
    assert isinstance(exc_info.value.result.stderr, str)  # type: ignore[attr-defined]


def test_timeout_and_stdout_callback_error_raise_one_of_expected_exceptions() -> None:
    def stdout_callback(_: str) -> None:
        raise RuntimeError('stdout callback exploded')

    start = time.perf_counter()
    with pytest.raises((RuntimeError, TimeoutCancellationError)) as exc_info:
        run(
            sys.executable,
            '-c',
            'import time; print("hello", flush=True); time.sleep(5)',
            split=False,
            stdout_callback=stdout_callback,
            timeout=0.2,
        )
    elapsed = time.perf_counter() - start
    assert elapsed < 2
    result = cast(Any, exc_info.value).result
    assert isinstance(result, SubprocessResult)
    assert isinstance(result.stdout, str)
    assert isinstance(result.stderr, str)
    assert isinstance(result.returncode, int)


def test_timeout_and_stderr_callback_error_raise_one_of_expected_exceptions() -> None:
    def stderr_callback(_: str) -> None:
        raise RuntimeError('stderr callback exploded')

    start = time.perf_counter()
    with pytest.raises((RuntimeError, TimeoutCancellationError)) as exc_info:
        run(
            sys.executable,
            '-c',
            'import sys, time; sys.stderr.write("hello\\n"); sys.stderr.flush(); time.sleep(5)',
            split=False,
            stderr_callback=stderr_callback,
            timeout=0.2,
        )
    elapsed = time.perf_counter() - start
    assert elapsed < 2
    result = cast(Any, exc_info.value).result
    assert isinstance(result, SubprocessResult)
    assert isinstance(result.stdout, str)
    assert isinstance(result.stderr, str)
    assert isinstance(result.returncode, int)


def test_timeout_and_stdout_callback_error_after_near_exit_raise_one_of_expected_exceptions() -> None:
    def stdout_callback(_: str) -> None:
        time.sleep(0.1)
        raise RuntimeError('stdout callback exploded after near-exit')

    start = time.perf_counter()
    with pytest.raises((RuntimeError, TimeoutCancellationError)) as exc_info:
        run(
            sys.executable,
            '-c',
            'import time; print("hello", flush=True); time.sleep(0.05)',
            split=False,
            stdout_callback=stdout_callback,
            timeout=0.02,
        )
    elapsed = time.perf_counter() - start

    assert elapsed < 2
    result = cast(Any, exc_info.value).result
    assert isinstance(result, SubprocessResult)
    assert result.stdout == 'hello\n'
    assert isinstance(result.stderr, str)
    assert isinstance(result.returncode, int)


def test_timeout_and_stderr_callback_error_after_near_exit_raise_one_of_expected_exceptions() -> None:
    def stderr_callback(_: str) -> None:
        time.sleep(0.1)
        raise RuntimeError('stderr callback exploded after near-exit')

    start = time.perf_counter()
    with pytest.raises((RuntimeError, TimeoutCancellationError)) as exc_info:
        run(
            sys.executable,
            '-c',
            'import sys, time; sys.stderr.write("hello\\n"); sys.stderr.flush(); time.sleep(0.05)',
            split=False,
            stderr_callback=stderr_callback,
            timeout=0.02,
        )
    elapsed = time.perf_counter() - start

    assert elapsed < 2
    result = cast(Any, exc_info.value).result
    assert isinstance(result, SubprocessResult)
    assert isinstance(result.stdout, str)
    assert result.stderr == 'hello\n'
    assert isinstance(result.returncode, int)


def test_process_exit_and_stdout_last_line_callback_failure_raise_callback_error() -> None:
    seen: List[str] = []

    def stdout_callback(text: str) -> None:
        seen.append(text)
        if text == 'last\n':
            time.sleep(0.1)
            raise RuntimeError('stdout callback exploded on last line')

    start = time.perf_counter()
    with pytest.raises(RuntimeError, match='stdout callback exploded on last line') as exc_info:
        run(
            sys.executable,
            '-c',
            'print("first", flush=True); print("last", flush=True)',
            split=False,
            stdout_callback=stdout_callback,
        )
    elapsed = time.perf_counter() - start

    assert elapsed < 2
    assert seen == ['first\n', 'last\n']
    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert exc_info.value.result.stdout == 'first\nlast\n'  # type: ignore[attr-defined]
    assert exc_info.value.result.stderr == ''  # type: ignore[attr-defined]
    assert exc_info.value.result.returncode == 0  # type: ignore[attr-defined]


def test_process_exit_and_stderr_last_line_callback_failure_raise_callback_error() -> None:
    seen: List[str] = []

    def stderr_callback(text: str) -> None:
        seen.append(text)
        if text == 'last\n':
            time.sleep(0.1)
            raise RuntimeError('stderr callback exploded on last line')

    start = time.perf_counter()
    with pytest.raises(RuntimeError, match='stderr callback exploded on last line') as exc_info:
        run(
            sys.executable,
            '-c',
            'import sys; sys.stderr.write("first\\n"); sys.stderr.flush(); sys.stderr.write("last\\n"); sys.stderr.flush()',
            split=False,
            stderr_callback=stderr_callback,
        )
    elapsed = time.perf_counter() - start

    assert elapsed < 2
    assert seen == ['first\n', 'last\n']
    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert exc_info.value.result.stdout == ''  # type: ignore[attr-defined]
    assert exc_info.value.result.stderr == 'first\nlast\n'  # type: ignore[attr-defined]
    assert exc_info.value.result.returncode == 0  # type: ignore[attr-defined]


def test_process_exit_and_near_exit_token_error_raise_token_error() -> None:
    start = time.perf_counter()

    def boom_later() -> bool:
        if time.perf_counter() - start < 0.03:
            return False
        raise RuntimeError('token exploded near process exit')

    token = ConditionToken(boom_later, suppress_exceptions=False)

    with pytest.raises(RuntimeError, match='token exploded near process exit') as exc_info:
        run(
            sys.executable,
            '-c',
            'import time; time.sleep(0.05)',
            split=False,
            token=token,
        )

    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert exc_info.value.result.stdout == ''  # type: ignore[attr-defined]
    assert exc_info.value.result.stderr == ''  # type: ignore[attr-defined]
    assert isinstance(exc_info.value.result.returncode, int)  # type: ignore[attr-defined]


def test_near_exit_token_error_keeps_kill_returncode() -> None:
    returncodes = []

    for _ in range(5):
        start = time.perf_counter()

        def boom_later() -> bool:
            if time.perf_counter() - start < 0.03:
                return False
            raise RuntimeError('token exploded near process exit')

        token = ConditionToken(boom_later, suppress_exceptions=False)

        with pytest.raises(RuntimeError, match='token exploded near process exit') as exc_info:
            run(
                sys.executable,
                '-c',
                'import time; time.sleep(0.05)',
                split=False,
                token=token,
            )

        returncodes.append(exc_info.value.result.returncode)  # type: ignore[attr-defined]

    assert returncodes == [-9, -9, -9, -9, -9]


def test_timeout_and_stdout_callback_race_keeps_kill_returncode() -> None:
    returncodes = []

    for _ in range(5):
        def stdout_callback(_: str) -> None:
            raise RuntimeError('stdout callback exploded')

        with pytest.raises((RuntimeError, TimeoutCancellationError)) as exc_info:
            run(
                sys.executable,
                '-c',
                'import time; print("hello", flush=True); time.sleep(5)',
                split=False,
                stdout_callback=stdout_callback,
                timeout=0.2,
            )

        result = cast(Any, exc_info.value).result
        assert isinstance(result, SubprocessResult)
        returncodes.append(result.returncode)

    assert returncodes == [-9, -9, -9, -9, -9]


def test_existing_result_attribute_on_callback_exception_is_not_overwritten() -> None:
    preserved_result = SubprocessResult()
    preserved_result.stdout = 'preserved'
    preserved_result.stderr = 'preserved'
    preserved_result.returncode = 777
    error = ResultBearingError('stdout callback exploded')
    error.result = preserved_result  # type: ignore[attr-defined]

    def callback(_: str) -> None:
        raise error

    with pytest.raises(ResultBearingError) as exc_info:
        run(sys.executable, '-c', 'print("hello")', split=False, stdout_callback=callback)

    assert exc_info.value.result is preserved_result  # type: ignore[attr-defined]


def test_result_getter_failure_does_not_mask_original_exception() -> None:
    class ExplodingResultGetterError(RuntimeError):
        @property
        def result(self) -> SubprocessResult:
            raise RuntimeError('result getter exploded')

    def callback(_: str) -> None:
        raise ExplodingResultGetterError('stdout callback exploded')

    with pytest.raises(ExplodingResultGetterError, match='stdout callback exploded'):
        run(sys.executable, '-c', 'print("hello")', split=False, stdout_callback=callback)


def test_result_setter_failure_does_not_mask_original_exception() -> None:
    class ExplodingResultSetterError(RuntimeError):
        @property
        def result(self) -> None:
            return None

        @result.setter
        def result(self, _value: SubprocessResult) -> None:
            raise RuntimeError('result setter exploded')

    def callback(_: str) -> None:
        raise ExplodingResultSetterError('stdout callback exploded')

    with pytest.raises(ExplodingResultSetterError, match='stdout callback exploded'):
        run(sys.executable, '-c', 'print("hello")', split=False, stdout_callback=callback)


def test_result_assignment_failure_does_not_mask_original_exception() -> None:
    class ExplodingResultAssignmentError(RuntimeError):
        def __setattr__(self, name: str, value: object) -> None:
            if name == 'result':
                raise RuntimeError('result assignment exploded')
            super().__setattr__(name, value)

    def callback(_: str) -> None:
        raise ExplodingResultAssignmentError('stdout callback exploded')

    with pytest.raises(ExplodingResultAssignmentError, match='stdout callback exploded'):
        run(sys.executable, '-c', 'print("hello")', split=False, stdout_callback=callback)


def test_slow_stdout_callback_does_not_prevent_completion() -> None:
    seen: List[str] = []

    def callback(text: str) -> None:
        time.sleep(0.05)
        seen.append(text)

    result = run(sys.executable, '-c', 'print("hello")', split=False, stdout_callback=callback)
    assert result.returncode == 0
    assert seen == ['hello\n']


def test_callback_that_prints_does_not_deadlock() -> None:
    def callback(text: str) -> None:
        print(text, end='')  # noqa: T201

    result = run(sys.executable, '-c', 'print("hello")', split=False, stdout_callback=callback, catch_output=False)
    assert result.returncode == 0


def test_shared_accumulator_callback_collects_output_from_parallel_runs() -> None:
    accumulator: List[str] = []

    def callback(text: str) -> None:
        accumulator.append(text)

    threads = [
        Thread(target=run, args=(sys.executable, '-c', f'print({i})'), kwargs={'split': False, 'stdout_callback': callback})
        for i in range(5)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(accumulator) == 5


def test_stdout_callback_must_be_callable() -> None:
    with pytest.raises(TypeError):
        run(sys.executable, '-c', 'print("hello")', split=False, stdout_callback=1)  # type: ignore[arg-type]


def test_stderr_callback_must_be_callable() -> None:
    with pytest.raises(TypeError):
        run(sys.executable, '-c', 'import sys; sys.stderr.write("hello")', split=False, stderr_callback=1)  # type: ignore[arg-type]


def test_catch_output_true_suppresses_failing_stdout_callback() -> None:
    def callback(_: str) -> None:
        raise RuntimeError('stdout callback should not be called')

    result = run(sys.executable, '-c', 'print("hello")', split=False, catch_output=True, stdout_callback=callback)

    assert result.stdout == 'hello\n'


def test_catch_output_true_suppresses_failing_stderr_callback() -> None:
    def callback(_: str) -> None:
        raise RuntimeError('stderr callback should not be called')

    result = run(
        sys.executable,
        '-c',
        'import sys; sys.stderr.write("hello\\n")',
        split=False,
        catch_output=True,
        catch_exceptions=True,
        stderr_callback=callback,
    )

    assert result.stderr == 'hello\n'


def test_zero_timeout_kills_immediately() -> None:
    with pytest.raises(TimeoutCancellationError):
        run(sys.executable, '-c', 'import time; time.sleep(1)', split=False, timeout=0)


def test_negative_timeout_rejected() -> None:
    with pytest.raises(ValueError, match=re.escape('You cannot specify a timeout less than zero.')):
        run(sys.executable, '-c', 'import time; time.sleep(1)', split=False, timeout=-1)


def test_nan_timeout_is_rejected_or_handled_consistently() -> None:
    with pytest.raises(ValueError, match=re.escape('You cannot specify NaN or infinite timeout values.')):
        run(sys.executable, '-c', 'import time; time.sleep(0.1)', split=False, timeout=float('nan'))


def test_infinite_timeout_is_supported_or_rejected_consistently() -> None:
    with pytest.raises(ValueError, match=re.escape('You cannot specify NaN or infinite timeout values.')):
        run(sys.executable, '-c', 'print("ok")', split=False, timeout=float('inf'), catch_output=True)


def test_string_timeout_is_rejected() -> None:
    with pytest.raises((TypeError, ValueError)):
        run(sys.executable, '-c', 'print("ok")', split=False, timeout='1')  # type: ignore[arg-type]


def test_already_cancelled_default_like_token_is_handled() -> None:
    token = NeverCancelsToken()
    result = run(sys.executable, '-c', 'print("ok")', split=False, token=token, catch_output=True)
    assert result.returncode == 0


def test_condition_token_with_unsuppressed_exception_raises_on_bool_before_run() -> None:
    def boom() -> bool:
        raise RuntimeError('token function exploded')

    token = ConditionToken(boom, suppress_exceptions=False)

    with pytest.raises(RuntimeError, match='token function exploded'):
        bool(token)


def test_condition_token_with_unsuppressed_exception_is_not_swallowed_by_run() -> None:
    def boom() -> bool:
        raise RuntimeError('token function exploded')

    token = ConditionToken(boom, suppress_exceptions=False)

    start = time.perf_counter()
    with pytest.raises(RuntimeError, match='token function exploded') as exc_info:
        run(sys.executable, '-c', 'import time; time.sleep(5)', split=False, token=token)
    elapsed = time.perf_counter() - start

    assert elapsed < 2
    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert isinstance(exc_info.value.result.stdout, str)  # type: ignore[attr-defined]
    assert isinstance(exc_info.value.result.stderr, str)  # type: ignore[attr-defined]
    assert isinstance(exc_info.value.result.returncode, int)  # type: ignore[attr-defined]


def test_timeout_and_token_error_raise_one_of_expected_exceptions() -> None:
    def boom() -> bool:
        raise RuntimeError('token function exploded')

    token = ConditionToken(boom, suppress_exceptions=False)

    start = time.perf_counter()
    with pytest.raises((RuntimeError, TimeoutCancellationError)) as exc_info:
        run(
            sys.executable,
            '-c',
            'import time; time.sleep(5)',
            split=False,
            token=token,
            timeout=0.2,
        )
    elapsed = time.perf_counter() - start
    assert elapsed < 2
    result = cast(Any, exc_info.value).result
    assert isinstance(result, SubprocessResult)
    assert isinstance(result.stdout, str)
    assert isinstance(result.stderr, str)
    assert isinstance(result.returncode, int)


def test_silent_process_timeout_and_immediate_token_error_raise_one_of_expected_exceptions() -> None:
    def boom() -> bool:
        raise RuntimeError('immediate token function exploded')

    token = ConditionToken(boom, suppress_exceptions=False)

    start = time.perf_counter()
    with pytest.raises((RuntimeError, TimeoutCancellationError)) as exc_info:
        run(
            sys.executable,
            '-c',
            'import time; time.sleep(5)',
            split=False,
            token=token,
            timeout=0.05,
        )
    elapsed = time.perf_counter() - start

    assert elapsed < 2
    result = cast(Any, exc_info.value).result
    assert isinstance(result, SubprocessResult)
    assert result.stdout == ''
    assert result.stderr == ''
    assert isinstance(result.returncode, int)


def test_silent_process_timeout_and_delayed_token_error_raise_one_of_expected_exceptions() -> None:
    calls = 0

    def boom_later() -> bool:
        nonlocal calls
        calls += 1
        if calls < 4:
            return False
        raise RuntimeError('delayed token function exploded')

    token = ConditionToken(boom_later, suppress_exceptions=False)

    start = time.perf_counter()
    with pytest.raises((RuntimeError, TimeoutCancellationError)) as exc_info:
        run(
            sys.executable,
            '-c',
            'import time; time.sleep(5)',
            split=False,
            token=token,
            timeout=0.05,
        )
    elapsed = time.perf_counter() - start

    assert elapsed < 2
    result = cast(Any, exc_info.value).result
    assert isinstance(result, SubprocessResult)
    assert result.stdout == ''
    assert result.stderr == ''
    assert isinstance(result.returncode, int)


def test_condition_token_exception_on_silent_process_surfaces_quickly() -> None:
    def boom() -> bool:
        raise RuntimeError('silent token exploded')

    token = ConditionToken(boom, suppress_exceptions=False)

    start = time.perf_counter()
    with pytest.raises(RuntimeError, match='silent token exploded') as exc_info:
        run(sys.executable, '-c', 'import time; time.sleep(5)', split=False, token=token)
    elapsed = time.perf_counter() - start

    assert elapsed < 2
    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert isinstance(exc_info.value.result.stdout, str)  # type: ignore[attr-defined]
    assert isinstance(exc_info.value.result.stderr, str)  # type: ignore[attr-defined]


def test_token_cancellation_with_active_stdout_preserves_partial_output() -> None:
    start = time.perf_counter()
    token = ConditionToken(lambda: time.perf_counter() - start > 0.1)

    result = run(
        sys.executable,
        '-c',
        'import time\n'
        'for i in range(1000):\n'
        ' print(i, flush=True)\n'
        ' time.sleep(0.01)',
        split=False,
        token=token,
        catch_exceptions=True,
        catch_output=True,
    )

    elapsed = time.perf_counter() - start

    assert elapsed >= 0.1
    assert elapsed < 2
    assert result.returncode != 0
    assert result.killed_by_token is True
    assert isinstance(result.stdout, str)
    assert result.stdout != ''
    assert '0\n' in result.stdout
    assert isinstance(result.stderr, str)


def test_token_cancellation_with_active_stderr_preserves_partial_output() -> None:
    start = time.perf_counter()
    token = ConditionToken(lambda: time.perf_counter() - start > 0.1)

    result = run(
        sys.executable,
        '-c',
        'import sys, time\n'
        'for i in range(1000):\n'
        ' sys.stderr.write(f"{i}\\n")\n'
        ' sys.stderr.flush()\n'
        ' time.sleep(0.01)',
        split=False,
        token=token,
        catch_exceptions=True,
        catch_output=True,
    )

    elapsed = time.perf_counter() - start

    assert elapsed >= 0.1
    assert elapsed < 2
    assert result.returncode != 0
    assert result.killed_by_token is True
    assert isinstance(result.stderr, str)
    assert result.stderr != ''
    assert '0\n' in result.stderr
    assert isinstance(result.stdout, str)


def test_token_and_timeout_race_is_consistent() -> None:
    token = NeverCancelsToken()
    result = run(sys.executable, '-c', 'print("ok")', split=False, token=token, timeout=0.5, catch_output=True)
    assert result.returncode == 0


def test_tiny_timeout_on_fast_process_is_still_well_formed() -> None:
    with pytest.raises(TimeoutCancellationError) as exc_info:
        run(sys.executable, '-c', 'import time; time.sleep(0.01)', split=False, timeout=0.000001, catch_output=True)
    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]


def test_stdout_without_newline_is_not_lost() -> None:
    result = run(sys.executable, '-c', 'import sys; sys.stdout.write("hello")', split=False, catch_output=True)
    assert result.stdout == 'hello'


def test_large_stdout_is_collected_fully() -> None:
    result = run(sys.executable, '-c', 'for i in range(5000): print(i)', split=False, catch_output=True)
    assert result.stdout is not None
    assert result.stdout.startswith('0\n')
    assert result.stdout.endswith('4999\n')


def test_large_stderr_is_collected_fully() -> None:
    result = run(
        sys.executable,
        '-c',
        'import sys\nfor i in range(5000): sys.stderr.write(f"{i}\\n")',
        split=False,
        catch_exceptions=True,
        catch_output=True,
    )
    assert result.stderr is not None
    assert result.stderr.startswith('0\n')
    assert result.stderr.endswith('4999\n')


def test_stderr_heavy_process_does_not_starve_stdout() -> None:
    result = run(
        sys.executable,
        '-c',
        'import sys; print("out"); [sys.stderr.write("err\\n") for _ in range(1000)]',
        split=False,
        catch_output=True,
    )
    assert result.stdout is not None
    assert 'out\n' in result.stdout


def test_interleaved_stdout_and_stderr_are_both_collected() -> None:
    result = run(
        sys.executable,
        '-c',
        'import sys\nfor i in range(10):\n print(f"out-{i}")\n sys.stderr.write(f"err-{i}\\n")',
        split=False,
        catch_output=True,
    )
    assert result.stdout is not None
    assert result.stderr is not None
    assert 'out-0\n' in result.stdout
    assert 'err-0\n' in result.stderr


def test_non_utf8_output_is_rejected_or_normalized() -> None:
    with pytest.raises((UnicodeDecodeError, RunningCommandError)):
        run(sys.executable, '-c', 'import os; os.write(1, b"\\xff\\xfe\\xfd")', split=False, catch_output=True)


def test_last_line_without_newline_is_preserved() -> None:
    result = run(sys.executable, '-c', 'import sys; sys.stdout.write("tail")', split=False, catch_output=True)
    assert result.stdout == 'tail'


def test_complex_kwargs_combination_is_well_formed() -> None:
    logger = MemoryLogger()
    result = run(
        sys.executable,
        '-c',
        'print("ok")',
        split=False,
        catch_output=True,
        catch_exceptions=True,
        logger=logger,
        stdout_callback=lambda _: None,
        stderr_callback=lambda _: None,
        timeout=5,
        token=DefaultToken(),
    )
    assert result.returncode == 0


def test_catch_output_true_suppresses_stdout_callback_even_in_complex_case() -> None:
    seen: List[str] = []
    run(
        sys.executable,
        '-c',
        'print("ok")',
        split=False,
        catch_output=True,
        stdout_callback=seen.append,
    )
    assert seen == []


def test_error_paths_return_consistent_subprocess_result_shapes() -> None:
    results: List[SubprocessResult] = []

    results.append(run('definitely_missing_command_for_suby_shape', catch_exceptions=True))
    results.append(run(sys.executable, '-c', 'import sys; sys.exit(1)', split=False, catch_exceptions=True, catch_output=True))
    results.append(run(sys.executable, '-c', 'import time; time.sleep(1)', split=False, timeout=0, catch_exceptions=True, catch_output=True))

    for result in results:
        assert isinstance(result.stdout, str)
        assert isinstance(result.stderr, str)
        assert isinstance(result.returncode, int)
        assert isinstance(result.killed_by_token, bool)


def test_logging_contract_across_outcomes_is_explicit() -> None:
    success_logger = MemoryLogger()
    error_logger = MemoryLogger()
    startup_logger = MemoryLogger()

    run(sys.executable, '-c', 'print("ok")', split=False, catch_output=True, logger=success_logger)
    run(sys.executable, '-c', 'import sys; sys.exit(1)', split=False, catch_exceptions=True, catch_output=True, logger=error_logger)
    run('definitely_missing_command_for_suby_logging', catch_exceptions=True, logger=startup_logger)

    assert len(success_logger.data.info) >= 1
    assert len(error_logger.data.error) + len(error_logger.data.exception) >= 1
    assert len(startup_logger.data.exception) >= 1


def test_many_parallel_runs_do_not_corrupt_results() -> None:
    results: List[SubprocessResult] = [SubprocessResult() for _ in range(10)]

    def worker(index: int) -> None:
        results[index] = run(sys.executable, '-c', f'print({index})', split=False, catch_output=True)

    threads = [Thread(target=worker, args=(index,)) for index in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    for index, result in enumerate(results):
        assert result.stdout == f'{index}\n'


def test_parallel_runs_with_shared_callback_do_not_drop_events() -> None:
    seen: List[str] = []

    def worker(index: int) -> None:
        run(sys.executable, '-c', f'print({index})', split=False, stdout_callback=seen.append)

    threads = [Thread(target=worker, args=(index,)) for index in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(seen) == 10


def test_many_short_processes_complete_without_state_corruption() -> None:
    for _ in range(100):
        result = run(sys.executable, '-c', 'print("ok")', split=False, catch_output=True)
        assert result.stdout == 'ok\n'
