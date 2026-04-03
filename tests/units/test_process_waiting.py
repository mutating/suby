import importlib
import importlib.util
import os
import select
import signal
import subprocess
import sys
import time
from pathlib import Path
from threading import Barrier, Event, Lock, Thread
from time import monotonic
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest
from cantok import ConditionToken, SimpleToken, TimeoutCancellationError

from suby import process_waiting, run
from suby.process_waiting import (
    has_event_driven_wait,
    wait_for_process_exit,
)
from suby.subprocess_result import SubprocessResult

_run_module = importlib.import_module('suby.run')


_is_event_driven_platform = has_event_driven_wait()
_is_macos = sys.platform == 'darwin'
_is_linux = sys.platform == 'linux'
_has_pidfd = hasattr(os, 'pidfd_open')

_SLEEP_CMD = f'{sys.executable} -c "import time; time.sleep(1000)"'
_SHORT_SLEEP_CMD = f'{sys.executable} -c "import time; time.sleep(0.1)"'
_PRINT_CMD = f'{sys.executable} -c "print(\'hello\')"'
_PASS_CMD = f'{sys.executable} -c pass'


def _assert_kill_returncode_matches_platform(returncode: int) -> None:
    if sys.platform == 'win32':
        assert returncode != 0
    else:
        assert returncode == -9


def _load_linux_pidfd_process_waiting(pidfd_open: MagicMock):
    """Load a fresh process_waiting module as if it were imported on Linux with pidfd support."""
    module_path = Path(process_waiting.__file__)
    spec = importlib.util.spec_from_file_location('test_process_waiting_linux_pidfd', module_path)

    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)

    with patch.object(sys, 'platform', 'linux'), \
         patch.object(os, 'pidfd_open', pidfd_open, create=True):
        spec.loader.exec_module(module)

    return module

@pytest.mark.skipif(not _is_event_driven_platform, reason='No event-driven wait on this platform')
def test_event_driven_detects_already_exited_process():
    """The OS-notification waiter returns immediately for a child process that has already exited but was not waited yet."""
    process = subprocess.Popen([sys.executable, '-c', 'pass'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    process.stdout.close()  # type: ignore[union-attr]
    process.stderr.close()  # type: ignore[union-attr]
    time.sleep(0.5)

    start = monotonic()
    wait_for_process_exit(process, 10.0)
    elapsed = monotonic() - start

    process.wait()

    assert elapsed < 2.0


@pytest.mark.skipif(not _is_event_driven_platform, reason='No event-driven wait on this platform')
def test_event_driven_wakes_on_process_exit():
    """The OS-notification waiter returns promptly when the child process exits during the wait."""
    process = subprocess.Popen(
        [sys.executable, '-c', 'import time; time.sleep(0.1)'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    start = monotonic()
    wait_for_process_exit(process, 10.0)
    elapsed = monotonic() - start

    process.wait()

    assert elapsed < 2.0


def test_timeout_expiry_process_still_running():
    """When timeout expires and process is still running, the process remains alive."""
    process = subprocess.Popen(
        [sys.executable, '-c', 'import time; time.sleep(1000)'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        wait_for_process_exit(process, 0.1)

        assert process.poll() is None
    finally:
        process.kill()
        process.wait()


def test_wait_for_process_exit_without_timeout_waits_until_process_finishes():
    """A None timeout blocks until the process exits."""
    process = subprocess.Popen(
        [sys.executable, '-c', 'import time; time.sleep(0.1)'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    start = monotonic()
    wait_for_process_exit(process, None)
    elapsed = monotonic() - start

    process.wait()

    assert process.poll() is not None
    assert elapsed < 2.0


@pytest.mark.skipif(not _is_event_driven_platform, reason='No event-driven waiter to trigger OSError from')
def test_oserror_fallback_with_reaped_pid():
    """If the OS-notification waiter sees OSError for an already-waited PID, wait_for_process_exit falls back cleanly."""
    process = subprocess.Popen(
        [sys.executable, '-c', 'pass'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    process.wait()
    wait_for_process_exit(process, 0.1)


@pytest.mark.skipif(not _is_macos, reason='macOS only')
def test_has_event_driven_wait_true_on_macos():
    """On macOS, OS-notification waiting is available via kqueue."""
    assert has_event_driven_wait() is True


@pytest.mark.skipif(not (_is_linux and _has_pidfd), reason='Linux 3.9+ only')
def test_has_event_driven_wait_true_on_linux():
    """On Linux with pidfd_open, OS-notification waiting is available."""
    assert has_event_driven_wait() is True


@pytest.mark.skipif(_is_event_driven_platform, reason='Only for fallback platforms')
def test_has_event_driven_wait_false_on_fallback():
    """On platforms without pidfd or kqueue, the library falls back to plain process.wait() polling."""
    assert has_event_driven_wait() is False


@pytest.mark.skipif(not (_is_linux and _has_pidfd), reason='Linux with /proc only')
def test_fd_cleanup_no_leaks():
    """Repeated calls to wait_for_process_exit do not leak file descriptors."""
    fd_count_before = len(list(os.scandir(f'/proc/{os.getpid()}/fd')))

    for _ in range(100):
        process = subprocess.Popen(
            [sys.executable, '-c', 'pass'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        wait_for_process_exit(process, 1.0)
        process.wait()

    fd_count_after = len(list(os.scandir(f'/proc/{os.getpid()}/fd')))

    assert fd_count_after <= fd_count_before + 5


def test_concurrent_calls_thread_safety():
    """Multiple threads can call wait_for_process_exit for the same process without errors."""
    process = subprocess.Popen(
        [sys.executable, '-c', 'import time; time.sleep(1000)'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    errors = []

    def worker():
        try:
            wait_for_process_exit(process, 0.1)
        except OSError as e:
            errors.append(e)

    try:
        threads = [Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
    finally:
        process.kill()
        process.wait()


@pytest.mark.parametrize(
    'waiter_name',
    [
        pytest.param('_wait_kqueue', marks=pytest.mark.skipif(not _is_macos, reason='macOS only')),
        pytest.param('_wait_pidfd', marks=pytest.mark.skipif(not (_is_linux and _has_pidfd), reason='Linux 3.9+ only')),
    ],
)
def test_platform_waiter_directly_returns_without_killing_running_process(waiter_name):
    """Calling the low-level platform waiter directly with a short timeout returns without killing the process."""
    waiter = getattr(process_waiting, waiter_name)

    process = subprocess.Popen(
        [sys.executable, '-c', 'import time; time.sleep(1000)'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        waiter(process.pid, 0.01)

        assert process.poll() is None
    finally:
        process.kill()
        process.wait()


@pytest.mark.skipif(not _is_macos, reason='macOS only')
def test_macos_wait_for_process_exit_passes_none_to_event_driven_waiter():
    """On macOS, None timeout is forwarded to the event-driven waiter unchanged."""
    process = subprocess.Popen(
        [sys.executable, '-c', 'pass'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        with patch('suby.process_waiting._event_driven_waiter') as mock_waiter:
            wait_for_process_exit(process, None)
        mock_waiter.assert_called_once_with(process.pid, None)
    finally:
        process.wait()


@pytest.mark.skipif(not _is_macos, reason='macOS only')
def test_macos_wait_kqueue_builds_subscription_and_closes_queue():
    """The macOS kqueue waiter subscribes to the child-process exit event and closes the kqueue handle afterwards."""
    mock_kqueue = MagicMock()
    mock_event = object()

    with patch.object(process_waiting.select, 'kqueue', return_value=mock_kqueue), \
         patch.object(process_waiting.select, 'kevent', return_value=mock_event) as mock_kevent:
        process_waiting._wait_kqueue(12345, 0.5)

    mock_kevent.assert_called_once_with(
        12345,
        filter=process_waiting.select.KQ_FILTER_PROC,
        flags=process_waiting.select.KQ_EV_ADD | process_waiting.select.KQ_EV_ONESHOT,
        fflags=process_waiting.select.KQ_NOTE_EXIT,
    )
    mock_kqueue.control.assert_called_once_with([mock_event], 1, 0.5)
    mock_kqueue.close.assert_called_once()


@pytest.mark.skipif(not _is_macos, reason='macOS only')
def test_macos_wait_for_process_exit_falls_back_after_kqueue_oserror():
    """If the macOS waiter raises OSError, wait_for_process_exit falls back to process.wait()."""
    process = subprocess.Popen(
        [sys.executable, '-c', 'pass'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        with patch('suby.process_waiting._event_driven_waiter', side_effect=OSError('mocked')):
            wait_for_process_exit(process, None)

        assert process.poll() is not None
    finally:
        process.wait()


@pytest.mark.skipif(sys.platform == 'win32', reason='pidfd is Linux-only and select.poll is unavailable on Windows')
def test_simulated_linux_pidfd_wait_registers_polls_and_closes_fd():
    """The Linux pidfd waiter opens a process fd, waits for readability with poll(), converts timeout to ms, and closes it."""
    poller = MagicMock()
    poll_factory = MagicMock(return_value=poller)
    pidfd_open = MagicMock(return_value=123)
    close = MagicMock()

    module = _load_linux_pidfd_process_waiting(pidfd_open)

    with patch.object(module.select, 'poll', poll_factory), patch.object(module.os, 'close', close):
        module._wait_pidfd(456, 0.5)

    pidfd_open.assert_called_once_with(456)
    poll_factory.assert_called_once_with()
    poller.register.assert_called_once_with(123, select.POLLIN)
    poller.poll.assert_called_once_with(500.0)
    close.assert_called_once_with(123)


@pytest.mark.skipif(sys.platform == 'win32', reason='pidfd is Linux-only and select.poll is unavailable on Windows')
def test_simulated_linux_pidfd_wait_passes_none_timeout_and_closes_fd():
    """The Linux pidfd waiter passes None through to poll() and still closes the pidfd."""
    poller = MagicMock()
    poll_factory = MagicMock(return_value=poller)
    pidfd_open = MagicMock(return_value=123)
    close = MagicMock()

    module = _load_linux_pidfd_process_waiting(pidfd_open)

    with patch.object(module.select, 'poll', poll_factory), patch.object(module.os, 'close', close):
        module._wait_pidfd(456, None)

    poller.poll.assert_called_once_with(None)
    close.assert_called_once_with(123)


@pytest.mark.skipif(sys.platform == 'win32', reason='pidfd is Linux-only and select.poll is unavailable on Windows')
def test_simulated_linux_pidfd_wait_closes_fd_when_poll_raises():
    """The Linux pidfd waiter closes the pidfd even if poll() raises an OSError."""
    poller = MagicMock()
    poller.poll.side_effect = OSError('mocked poll failure')
    poll_factory = MagicMock(return_value=poller)
    pidfd_open = MagicMock(return_value=123)
    close = MagicMock()

    module = _load_linux_pidfd_process_waiting(pidfd_open)

    with patch.object(module.select, 'poll', poll_factory), \
         patch.object(module.os, 'close', close), \
         pytest.raises(OSError, match='mocked poll failure'):
        module._wait_pidfd(456, 0.5)

    close.assert_called_once_with(123)


@pytest.mark.skipif(not (_is_linux and _has_pidfd), reason='Linux 3.9+ only')
def test_wait_pidfd_direct_without_timeout_waits_until_process_finishes():
    """Direct pidfd waiting with None timeout blocks until the short-lived child process exits."""
    from suby.process_waiting import _wait_pidfd  # noqa: PLC0415

    process = subprocess.Popen(
        [sys.executable, '-c', 'import time; time.sleep(0.1)'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        start = monotonic()
        _wait_pidfd(process.pid, None)
        elapsed = monotonic() - start

        process.wait()

        assert process.poll() is not None
        assert elapsed < 2.0
    finally:
        if process.poll() is None:
            process.kill()
        process.wait()

def test_timeout_kills_long_running_process():
    """Timeout-only path kills the process and raises TimeoutCancellationError."""
    with pytest.raises(TimeoutCancellationError):
        run(_SLEEP_CMD, timeout=0.5)


def test_process_exits_before_timeout(assert_no_suby_thread_leaks):
    """When process exits before timeout, no exception is raised and output is captured."""
    with assert_no_suby_thread_leaks():
        result = run(_PRINT_CMD, timeout=10, catch_output=True)

    assert result.stdout == 'hello\n'
    assert result.returncode == 0
    assert result.killed_by_token is False


def test_very_short_timeout():
    """Very short timeout still kills the process and raises."""
    with pytest.raises(TimeoutCancellationError):
        run(_SLEEP_CMD, timeout=0.001)


def test_timeout_with_catch_exceptions():
    """With catch_exceptions=True, timeout doesn't raise but result reflects the kill."""
    result = run(_SLEEP_CMD, timeout=0.5, catch_exceptions=True)

    assert result.killed_by_token is True
    assert result.returncode != 0
    assert result.stdout == ''
    assert result.stderr == ''


def test_killed_process_returncode_matches_platform_contract():
    """A process killed by timeout reports SIGKILL as -9 on POSIX, while Windows only guarantees a non-zero exit code."""
    result = run(_SLEEP_CMD, timeout=0.01, catch_exceptions=True)

    _assert_kill_returncode_matches_platform(result.returncode)
    assert result.killed_by_token is True


@pytest.mark.skipif(sys.platform != 'win32', reason='Windows-only pidfd skip policy check')
def test_windows_has_no_event_driven_wait_capability():
    """On Windows, the OS-notification wait capability flags are all False."""
    assert _is_linux is False
    assert _has_pidfd is False
    assert has_event_driven_wait() is False


def test_run_timeout_thread_kills_running_process_and_marks_result():
    """The timeout helper thread kills a real process and marks the result."""
    process = subprocess.Popen(
        [sys.executable, '-c', 'import time; time.sleep(1000)'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    result = SubprocessResult()
    try:
        timeout_thread = _run_module.run_timeout_thread(process, 0.01, result)
        timeout_thread.join(timeout=2)

        assert timeout_thread.is_alive() is False

        process.wait(timeout=2)

        assert process.returncode != 0
        assert result.killed_by_token is True
    finally:
        if process.poll() is None:
            process.kill()
        process.wait()
        if process.stdout is not None:
            process.stdout.close()
        if process.stderr is not None:
            process.stderr.close()


def test_timeout_wait_does_not_kill_already_finished_process():
    """If the process exits before timeout, timeout_wait leaves it untouched."""
    process = subprocess.Popen(
        [sys.executable, '-c', 'pass'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    result = SubprocessResult()
    try:
        _run_module.timeout_wait(process, 10, result)

        process.wait(timeout=2)

        assert process.returncode == 0
        assert result.killed_by_token is False
    finally:
        if process.poll() is None:
            process.kill()
        process.wait()
        if process.stdout is not None:
            process.stdout.close()
        if process.stderr is not None:
            process.stderr.close()


def test_run_uses_timeout_thread_only_on_event_driven_platforms():
    """Timeout-only run() starts run_timeout_thread only when OS-notification waiting is available, not on fallback platforms."""
    with patch.object(_run_module, 'run_timeout_thread', wraps=_run_module.run_timeout_thread) as mock_timeout_thread:
        with pytest.raises(TimeoutCancellationError):
            run(_SLEEP_CMD, timeout=0.5)

        if _is_event_driven_platform:
            mock_timeout_thread.assert_called_once()
        else:
            mock_timeout_thread.assert_not_called()


def test_token_plus_timeout_does_not_use_timeout_thread():
    """When a custom token is passed, timeout thread is not used."""
    with patch.object(_run_module, 'run_timeout_thread', wraps=_run_module.run_timeout_thread) as mock_timeout_thread:
        with pytest.raises(TimeoutCancellationError):
            run(_SLEEP_CMD, timeout=0.5, token=SimpleToken())
        mock_timeout_thread.assert_not_called()


@pytest.mark.skipif(not _is_event_driven_platform, reason='Event-driven platforms only')
def test_event_driven_fast_process_exit_detection():
    """On event-driven platforms, process exit is detected near-instantly, not after full timeout."""
    start = monotonic()
    result = run(_SHORT_SLEEP_CMD, timeout=10, catch_output=True)
    elapsed = monotonic() - start

    assert result.returncode == 0
    assert elapsed < 2.0

@pytest.mark.parametrize(
    ('command', 'run_kwargs', 'expected_exception', 'expected_stdout'),
    [
        (_PRINT_CMD, {'token': SimpleToken(), 'catch_output': True}, None, 'hello\n'),
        (_SLEEP_CMD, {'timeout': 0.5, 'token': SimpleToken()}, TimeoutCancellationError, None),
        (_PRINT_CMD, {'catch_output': True}, None, 'hello\n'),
    ],
)
def test_run_uses_process_waiter_thread(command, run_kwargs, expected_exception, expected_stdout):
    """Each run() call starts one process-waiter thread and one reader thread per output stream."""
    with patch.object(_run_module, 'run_process_waiter_thread', wraps=_run_module.run_process_waiter_thread) as mock_waiter, \
         patch.object(_run_module, 'run_stdout_thread', wraps=_run_module.run_stdout_thread) as mock_stdout_thread, \
         patch.object(_run_module, 'run_stderr_thread', wraps=_run_module.run_stderr_thread) as mock_stderr_thread:
        if expected_exception is None:
            result = run(command, **run_kwargs)
        else:
            with pytest.raises(expected_exception):
                run(command, **run_kwargs)
            result = None

        mock_waiter.assert_called_once()
        mock_stdout_thread.assert_called_once()
        mock_stderr_thread.assert_called_once()

    if expected_stdout is not None:
        assert result is not None
        assert result.stdout == expected_stdout

@pytest.mark.skipif(sys.platform == 'win32', reason='No SIGTERM on Windows')
def test_process_killed_by_signal_during_wait():
    """OS-notification waiting notices when an external SIGTERM kills the child process."""
    process = subprocess.Popen(
        [sys.executable, '-c', 'import time; time.sleep(1000)'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    def kill_later():
        time.sleep(0.2)
        os.kill(process.pid, signal.SIGTERM)

    t = Thread(target=kill_later)
    t.start()

    start = monotonic()
    wait_for_process_exit(process, 10.0)
    elapsed = monotonic() - start
    t.join()

    process.wait()

    assert elapsed < 2.0


def test_rapid_sequential_timeout_calls():
    """Rapid sequential timeout-enabled calls should each complete successfully for a short-lived process."""
    for _ in range(10):
        result = run(_PASS_CMD, timeout=1, catch_output=True)

        assert result.returncode == 0


def test_rapid_sequential_timeout_calls_do_not_leak_threads(assert_no_suby_thread_leaks):
    """Rapid sequential timeout-enabled calls should finish without leaving suby worker threads alive."""
    with assert_no_suby_thread_leaks():
        for _ in range(10):
            result = run(_PASS_CMD, timeout=1, catch_output=True)

            assert result.returncode == 0


@pytest.mark.skipif(not (_is_linux and _has_pidfd), reason='Linux with /proc only')
def test_rapid_sequential_timeout_calls_do_not_leak_file_descriptors():
    """On Linux, rapid sequential timeout-enabled run() calls should not grow the process fd table."""
    fd_count_before = len(list(os.scandir(f'/proc/{os.getpid()}/fd')))

    for _ in range(10):
        result = run(_PASS_CMD, timeout=1, catch_output=True)

        assert result.returncode == 0

    fd_count_after = len(list(os.scandir(f'/proc/{os.getpid()}/fd')))

    assert fd_count_after <= fd_count_before + 5


def test_timeout_zero():
    """Timeout of 0 kills the process immediately."""
    with pytest.raises(TimeoutCancellationError):
        run(_SLEEP_CMD, timeout=0)


def test_race_process_exits_between_poll_and_kill():
    """When process exits between poll() and kill(), the OSError is caught gracefully."""
    from suby.run import timeout_wait  # noqa: PLC0415

    class MockProcess:
        pid = 99999

        def poll(self):
            return None

        def kill(self):
            raise ProcessLookupError('No such process')

    result = SubprocessResult()
    with patch.object(_run_module, 'wait_for_process_exit'):
        timeout_wait(MockProcess(), 1.0, result)  # type: ignore[arg-type]

    assert result.killed_by_token is False


def test_timeout_raises_timeout_cancellation_error_with_result():
    """TimeoutCancellationError carries the result object with correct fields."""
    with pytest.raises(TimeoutCancellationError) as exc_info:
        run(_SLEEP_CMD, timeout=0.5)

    assert hasattr(exc_info.value, 'result')
    assert exc_info.value.result.killed_by_token is True  # type: ignore[attr-defined]
    assert exc_info.value.result.returncode != 0  # type: ignore[attr-defined]


@pytest.mark.skipif(not _is_event_driven_platform, reason='Event-driven platforms only')
def test_oserror_fallback_returns_promptly_on_exit():
    """When event-driven waiter raises OSError, fallback to process.wait() detects early exit."""
    process = subprocess.Popen(
        [sys.executable, '-c', 'pass'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    time.sleep(0.5)

    with patch('suby.process_waiting._event_driven_waiter', side_effect=OSError('mocked')):
        start = monotonic()
        wait_for_process_exit(process, 10.0)
        elapsed = monotonic() - start

    process.wait()

    assert elapsed < 2.0


def test_oserror_fallback_with_timeout_expiry():
    """When event-driven waiter raises OSError and process is still running, fallback times out."""
    process = subprocess.Popen(
        [sys.executable, '-c', 'import time; time.sleep(1000)'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        with patch('suby.process_waiting._event_driven_waiter', side_effect=OSError('mocked')):
            wait_for_process_exit(process, 0.1)

        assert process.poll() is None
    finally:
        process.kill()
        process.wait()


def test_wait_for_process_exit_without_event_driven_waiter():
    """When _event_driven_waiter is None, falls back to process.wait() directly."""
    process = subprocess.Popen(
        [sys.executable, '-c', 'import time; time.sleep(1000)'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        with patch('suby.process_waiting._event_driven_waiter', None):
            wait_for_process_exit(process, 0.1)

        assert process.poll() is None
    finally:
        process.kill()
        process.wait()


def test_coordinator_does_not_lose_failure_when_process_exit_and_failure_signals_race(assert_no_suby_thread_leaks):
    """The main coordination loop still raises a recorded callback failure if process-exit and failure notifications race."""
    process_exited = Event()
    synchronized_release = Barrier(2)

    def controlled_waiter(process: Any, state: Any):
        _run_module.wait_for_process_exit(process, None)
        process_exited.set()
        synchronized_release.wait(timeout=1)
        state.process_exit_event.set()
        state.wake_event.set()

    def stdout_callback(_: str):
        if not process_exited.wait(timeout=1):
            raise RuntimeError('coordinated race setup failed')
        synchronized_release.wait(timeout=1)
        raise RuntimeError('stdout callback exploded in coordinated race')

    with assert_no_suby_thread_leaks(), \
         patch.object(_run_module, 'wait_for_process_exit_and_signal', new=controlled_waiter), \
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


def test_coordinator_does_not_lose_token_error_when_process_exit_and_failure_signals_race(assert_no_suby_thread_leaks):
    """The main coordination loop still raises a token-condition error if process-exit and failure notifications race."""
    process_exited = Event()
    synchronized_release = Barrier(2)

    def controlled_waiter(process: Any, state: Any):
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

    with assert_no_suby_thread_leaks(), \
         patch.object(_run_module, 'wait_for_process_exit_and_signal', new=controlled_waiter), \
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


def test_coordinator_raises_recorded_callback_failure_if_token_error_happens_second(assert_no_suby_thread_leaks):
    """If a callback failure is recorded first, the main coordination loop raises that error even if a token error follows."""
    from threading import current_thread, main_thread  # noqa: PLC0415

    callback_failure_saved = Event()
    token_check_started = Event()
    original_failure_set = _run_module._FailureState.set

    def instrumented_failure_set(self: Any, error: Exception):
        was_saved = original_failure_set(self, error)
        if was_saved and str(error) == 'stdout callback exploded first':
            callback_failure_saved.set()
        return was_saved

    def stdout_callback(_: str):
        if not token_check_started.wait(timeout=1):
            raise RuntimeError('coordinated callback setup failed')
        raise RuntimeError('stdout callback exploded first')

    def token_boom_after_callback_failure() -> bool:
        if current_thread() is not main_thread():
            return False

        token_check_started.set()
        if not callback_failure_saved.wait(timeout=1):
            raise RuntimeError('coordinated token setup failed')
        raise RuntimeError('token exploded second')

    token = ConditionToken(token_boom_after_callback_failure, suppress_exceptions=False)

    with assert_no_suby_thread_leaks(), \
         patch.object(_run_module._FailureState, 'set', new=instrumented_failure_set), \
         pytest.raises(RuntimeError, match='stdout callback exploded first') as exc_info:
        run(
            sys.executable,
            '-c',
            'import time; print("hello", flush=True); time.sleep(5)',
            split=False,
            stdout_callback=stdout_callback,
            token=token,
        )

    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert exc_info.value.result.stdout == 'hello\n'  # type: ignore[attr-defined]
    assert exc_info.value.result.stderr == ''  # type: ignore[attr-defined]
    assert exc_info.value.result.returncode != 0  # type: ignore[attr-defined]
    assert exc_info.value.result.killed_by_token is False  # type: ignore[attr-defined]


def test_stderr_callback_not_called_after_stdout_failure_is_recorded(assert_no_suby_thread_leaks):
    """Once a stdout failure is recorded, later stderr lines should not be delivered to stderr_callback."""
    failure_recorded = Event()
    late_stderr_callbacks = []
    original_failure_set = _run_module._FailureState.set

    def instrumented_failure_set(self: Any, error: Exception):
        was_saved = original_failure_set(self, error)
        if was_saved and str(error) == 'stdout callback exploded first':
            failure_recorded.set()
        return was_saved

    def stdout_callback(_: str):
        raise RuntimeError('stdout callback exploded first')

    def stderr_callback(text: str):
        if failure_recorded.is_set():
            late_stderr_callbacks.append(text)

    with assert_no_suby_thread_leaks(), \
         patch.object(_run_module._FailureState, 'set', new=instrumented_failure_set), \
         pytest.raises(RuntimeError, match='stdout callback exploded first'):
        run(
            sys.executable,
            '-c',
            (
                'import sys, time\n'
                'print("stdout-ready", flush=True)\n'
                'time.sleep(0.2)\n'
                'sys.stderr.write("late-stderr\\n")\n'
                'sys.stderr.flush()\n'
                'time.sleep(5)\n'
            ),
            split=False,
            stdout_callback=stdout_callback,
            stderr_callback=stderr_callback,
        )

    assert failure_recorded.is_set()
    assert late_stderr_callbacks == []


def test_result_excludes_stderr_after_stdout_callback_failure(assert_no_suby_thread_leaks):
    """After the stdout reader thread stores a callback failure, stderr written later by the process is not appended."""
    failure_recorded = Event()
    original_failure_set = _run_module._FailureState.set

    def instrumented_failure_set(self: Any, error: Exception):
        was_saved = original_failure_set(self, error)
        if was_saved and str(error) == 'stdout callback exploded first':
            failure_recorded.set()
        return was_saved

    def stdout_callback(_: str):
        raise RuntimeError('stdout callback exploded first')

    with assert_no_suby_thread_leaks(), \
         patch.object(_run_module._FailureState, 'set', new=instrumented_failure_set), \
         pytest.raises(RuntimeError, match='stdout callback exploded first') as exc_info:
        run(
            sys.executable,
            '-c',
            (
                'import sys, time\n'
                'print("stdout-ready", flush=True)\n'
                'time.sleep(0.2)\n'
                'sys.stderr.write("late-stderr\\n")\n'
                'sys.stderr.flush()\n'
                'time.sleep(5)\n'
            ),
            split=False,
            stdout_callback=stdout_callback,
            stderr_callback=lambda _: None,
        )

    assert failure_recorded.is_set()
    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert exc_info.value.result.stdout == 'stdout-ready\n'  # type: ignore[attr-defined]
    assert exc_info.value.result.stderr == ''  # type: ignore[attr-defined]


def test_recorded_stdout_failure_is_raised_promptly_by_coordinator(assert_no_suby_thread_leaks):
    """Once the stdout failure is stored, the main coordination loop raises it promptly instead of waiting for process exit."""
    failure_recorded = Event()
    failure_recorded_at = []
    original_failure_set = _run_module._FailureState.set

    def instrumented_failure_set(self: Any, error: Exception):
        was_saved = original_failure_set(self, error)
        if was_saved and str(error) == 'stdout callback exploded first':
            failure_recorded_at.append(time.perf_counter())
            failure_recorded.set()
        return was_saved

    def stdout_callback(_: str):
        raise RuntimeError('stdout callback exploded first')

    with assert_no_suby_thread_leaks(), \
         patch.object(_run_module._FailureState, 'set', new=instrumented_failure_set), \
         pytest.raises(RuntimeError, match='stdout callback exploded first') as exc_info:
        run(
            sys.executable,
            '-c',
            'import time; print("stdout-ready", flush=True); time.sleep(5)',
            split=False,
            stdout_callback=stdout_callback,
        )
    handled_after = time.perf_counter()

    assert failure_recorded.is_set()
    assert len(failure_recorded_at) == 1
    assert handled_after - failure_recorded_at[0] < 1

    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert exc_info.value.result.stdout == 'stdout-ready\n'  # type: ignore[attr-defined]


def test_failure_state_writes_are_locked_but_reads_are_not(assert_no_suby_thread_leaks):
    """The shared _FailureState.error field serializes writes with a lock, while reads intentionally remain lock-free."""
    locklib = pytest.importorskip('locklib')
    traced_locks = []

    def traced_failure_state_init(self: Any):
        self._error = None
        self._lock = locklib.LockTraceWrapper(Lock())
        traced_locks.append(self._lock)

    def traced_error_getter(self: Any):
        self._lock.notify('failure_state_error_read')
        return self._error

    def traced_error_setter(self: Any, error: Exception):
        self._lock.notify('failure_state_error_write')
        self._error = error

    def stdout_callback(_: str):
        raise RuntimeError('stdout callback exploded first')

    with assert_no_suby_thread_leaks(), \
         patch.object(_run_module._FailureState, '__init__', new=traced_failure_state_init), \
         patch.object(_run_module._FailureState, 'error', new=property(traced_error_getter, traced_error_setter), create=True), \
         pytest.raises(RuntimeError, match='stdout callback exploded first'):
        run(
            sys.executable,
            '-c',
            'import time; print("stdout-ready", flush=True); time.sleep(5)',
            split=False,
            stdout_callback=stdout_callback,
        )

    assert len(traced_locks) == 1
    assert traced_locks[0].was_event_locked('failure_state_error_write')
    assert traced_locks[0].was_event_locked('failure_state_error_read') is False


def test_timeout_thread_can_win_before_stdout_failure_is_recorded():
    """The timeout helper may finish cancellation before the stdout reader thread stores its callback exception.

    The kill return code assertion is platform-dependent: POSIX reports SIGKILL as -9, while Windows uses a
    different non-zero process exit code.
    """
    original_raise_failure_if_needed = _run_module.raise_failure_if_needed
    failure_recorded = Event()
    delay_once = Event()

    def delayed_raise_failure_if_needed(process: Any, reader_threads: Any, state: Any):
        if state.failure_state.error is not None and not delay_once.is_set():
            failure_recorded.set()
            delay_once.set()
            time.sleep(0.05)
        return original_raise_failure_if_needed(process, reader_threads, state)

    def stdout_callback(_: str):
        raise RuntimeError('stdout callback exploded before timeout handling')

    with patch.object(_run_module, 'raise_failure_if_needed', new=delayed_raise_failure_if_needed), \
         pytest.raises((RuntimeError, TimeoutCancellationError)) as exc_info:
        run(
            sys.executable,
            '-c',
            'import time; print("hello", flush=True); time.sleep(5)',
            split=False,
            stdout_callback=stdout_callback,
            timeout=0.01,
        )

    result = cast(Any, exc_info.value).result

    assert isinstance(result, SubprocessResult)

    if isinstance(exc_info.value, TimeoutCancellationError):

        assert failure_recorded.is_set() is False
        assert result.stdout == ''
        assert result.stderr == ''
        _assert_kill_returncode_matches_platform(result.returncode)
        assert result.killed_by_token is True
    else:

        assert isinstance(exc_info.value, RuntimeError)
        assert str(exc_info.value) == 'stdout callback exploded before timeout handling'
        assert failure_recorded.is_set() is True
        assert result.stdout == 'hello\n'
        assert isinstance(result.stderr, str)
        _assert_kill_returncode_matches_platform(result.returncode)
        assert result.killed_by_token in {False, True}


def test_timeout_thread_can_race_with_recorded_token_failure_before_main_thread_handles_it():
    """Timeout cancellation and a just-stored token-condition exception can overlap before run()'s main coordination loop handles it."""
    original_raise_failure_if_needed = _run_module.raise_failure_if_needed
    failure_recorded = Event()
    delay_once = Event()

    def delayed_raise_failure_if_needed(process: Any, reader_threads: Any, state: Any):
        if state.failure_state.error is not None and not delay_once.is_set():
            failure_recorded.set()
            delay_once.set()
            time.sleep(0.05)
        return original_raise_failure_if_needed(process, reader_threads, state)

    def boom() -> bool:
        raise RuntimeError('token exploded before timeout handling')

    token = ConditionToken(boom, suppress_exceptions=False)

    with patch.object(_run_module, 'raise_failure_if_needed', new=delayed_raise_failure_if_needed), \
         pytest.raises(RuntimeError, match='token exploded before timeout handling') as exc_info:
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


@pytest.mark.parametrize(
    ('callback_kwarg', 'command', 'expected_stdout', 'expected_stderr', 'error_message'),
    [
        (
            'stdout_callback',
            'print("first", flush=True); print("last", flush=True)',
            'first\nlast\n',
            '',
            'stdout callback exploded on last line',
        ),
        (
            'stderr_callback',
            'import sys; sys.stderr.write("first\\n"); sys.stderr.flush(); sys.stderr.write("last\\n"); sys.stderr.flush()',
            '',
            'first\nlast\n',
            'stderr callback exploded on last line',
        ),
    ],
)
def test_process_exit_and_last_line_callback_failure_raise_callback_error(
    callback_kwarg,
    command,
    expected_stdout,
    expected_stderr,
    error_message,
):
    """If process exit and the callback failure on the final output line happen together, run() raises the callback error."""
    seen: List[str] = []

    def callback(text: str):
        seen.append(text)
        if text == 'last\n':
            time.sleep(0.1)
            raise RuntimeError(error_message)

    start = time.perf_counter()
    with pytest.raises(RuntimeError, match=error_message) as exc_info:
        run(
            sys.executable,
            '-c',
            command,
            split=False,
            **{callback_kwarg: callback},
        )
    elapsed = time.perf_counter() - start

    assert elapsed < 2
    assert seen == ['first\n', 'last\n']
    assert isinstance(exc_info.value.result, SubprocessResult)  # type: ignore[attr-defined]
    assert exc_info.value.result.stdout == expected_stdout  # type: ignore[attr-defined]
    assert exc_info.value.result.stderr == expected_stderr  # type: ignore[attr-defined]
    assert exc_info.value.result.returncode == 0  # type: ignore[attr-defined]


def test_process_exit_and_near_exit_token_error_raise_token_error():
    """If the token condition fails right before process exit, run() raises the token-condition error."""
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


def test_near_exit_token_error_keeps_kill_result_shape():
    """A token failure right before process exit still returns the expected result fields and a killed-process return code.

    The exact kill return code is asserted only on POSIX, because Windows does not encode SIGKILL as -9.
    """
    returncodes = []
    killed_flags = []

    for _ in range(5):
        start = time.perf_counter()

        def boom_later(start_time: float = start) -> bool:
            if time.perf_counter() - start_time < 0.03:
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
        killed_flags.append(exc_info.value.result.killed_by_token)  # type: ignore[attr-defined]

    for returncode in returncodes:
        _assert_kill_returncode_matches_platform(returncode)
    assert killed_flags == [False, False, False, False, False]


def test_timeout_and_stdout_callback_race_result_shape_is_observable():
    """If timeout cancellation and stdout callback failure race, the attached result still has a valid returncode and kill flag.

    The return code assertion branches by OS because only POSIX exposes a signal-based -9 exit status here.
    """
    returncodes = []
    killed_flags = []

    for _ in range(5):
        def stdout_callback(_: str):
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
        killed_flags.append(result.killed_by_token)

    for returncode in returncodes:
        _assert_kill_returncode_matches_platform(returncode)
    assert len(killed_flags) == 5
    assert set(killed_flags).issubset({False, True})
