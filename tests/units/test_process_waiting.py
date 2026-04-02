import importlib
import importlib.util
import os
import select
import signal
import subprocess
import sys
import time
from pathlib import Path
from threading import Thread
from time import monotonic
from unittest.mock import MagicMock, patch

import pytest
from cantok import SimpleToken, TimeoutCancellationError

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
    """Event-driven wait returns instantly for an already-exited (zombie) process."""
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
    """Event-driven wait returns promptly when a process exits during the wait."""
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
    """When event-driven waiter gets OSError (reaped PID), falls back gracefully without raising."""
    process = subprocess.Popen(
        [sys.executable, '-c', 'pass'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    process.wait()
    wait_for_process_exit(process, 0.1)


@pytest.mark.skipif(not _is_macos, reason='macOS only')
def test_has_event_driven_wait_true_on_macos():
    """On macOS, event-driven waiting is available via kqueue."""
    assert has_event_driven_wait() is True


@pytest.mark.skipif(not (_is_linux and _has_pidfd), reason='Linux 3.9+ only')
def test_has_event_driven_wait_true_on_linux():
    """On Linux with pidfd_open, event-driven waiting is available."""
    assert has_event_driven_wait() is True


@pytest.mark.skipif(_is_event_driven_platform, reason='Only for fallback platforms')
def test_has_event_driven_wait_false_on_fallback():
    """On platforms without pidfd or kqueue, event-driven waiting is unavailable."""
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


@pytest.mark.skipif(not _is_macos, reason='macOS only')
def test_wait_kqueue_direct():
    """Direct call to _wait_kqueue with a running process returns without killing it."""
    from suby.process_waiting import _wait_kqueue  # noqa: PLC0415

    process = subprocess.Popen(
        [sys.executable, '-c', 'import time; time.sleep(1000)'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        _wait_kqueue(process.pid, 0.01)

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
    """The macOS kqueue waiter builds the expected exit subscription and closes the queue."""
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


@pytest.mark.skipif(not (_is_linux and _has_pidfd), reason='Linux 3.9+ only')
def test_wait_pidfd_direct():
    """Direct call to _wait_pidfd with a running process returns without killing it."""
    from suby.process_waiting import _wait_pidfd  # noqa: PLC0415

    process = subprocess.Popen(
        [sys.executable, '-c', 'import time; time.sleep(1000)'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        _wait_pidfd(process.pid, 0.01)

        assert process.poll() is None
    finally:
        process.kill()
        process.wait()


def test_simulated_linux_pidfd_wait_registers_polls_and_closes_fd():
    """The Linux pidfd waiter opens a pidfd, registers it with poll, converts timeout to ms, and closes it."""
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
    """Direct pidfd waiting with None timeout blocks until a short-lived process exits."""
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


def test_process_exits_before_timeout():
    """When process exits before timeout, no exception is raised and output is captured."""
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


def test_timeout_only_uses_timeout_thread():
    """Timeout-only path uses the dedicated timeout thread."""
    with patch.object(_run_module, 'run_timeout_thread', wraps=_run_module.run_timeout_thread) as mock_timeout_thread:
        with pytest.raises(TimeoutCancellationError):
            run(_SLEEP_CMD, timeout=0.5)
        mock_timeout_thread.assert_called_once()


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

def test_token_only_uses_process_waiter_thread():
    """When only a token is passed, process exit is still tracked by the waiter thread."""
    with patch.object(_run_module, 'run_process_waiter_thread', wraps=_run_module.run_process_waiter_thread) as mock_waiter:
        result = run(_PRINT_CMD, token=SimpleToken(), catch_output=True)
        mock_waiter.assert_called_once()

    assert result.stdout == 'hello\n'


def test_token_plus_timeout_uses_process_waiter_thread():
    """When both token and timeout are passed, process exit is tracked by the waiter thread."""
    with patch.object(_run_module, 'run_process_waiter_thread', wraps=_run_module.run_process_waiter_thread) as mock_waiter:
        with pytest.raises(TimeoutCancellationError):
            run(_SLEEP_CMD, timeout=0.5, token=SimpleToken())
        mock_waiter.assert_called_once()


def test_no_timeout_no_token_uses_process_waiter_thread():
    """When neither timeout nor token is passed, process exit is tracked by the waiter thread."""
    with patch.object(_run_module, 'run_process_waiter_thread', wraps=_run_module.run_process_waiter_thread) as mock_waiter, \
         patch.object(_run_module, 'run_stdout_thread', wraps=_run_module.run_stdout_thread) as mock_stdout_thread, \
         patch.object(_run_module, 'run_stderr_thread', wraps=_run_module.run_stderr_thread) as mock_stderr_thread:
        result = run(_PRINT_CMD, catch_output=True)
        mock_waiter.assert_called_once()
        mock_stdout_thread.assert_called_once()
        mock_stderr_thread.assert_called_once()

    assert result.stdout == 'hello\n'

@pytest.mark.skipif(sys.platform == 'win32', reason='No SIGTERM on Windows')
def test_process_killed_by_signal_during_wait():
    """Event-driven wait detects process killed by external signal."""
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
    """Rapid sequential calls with timeout don't leak resources."""
    for _ in range(10):
        result = run(_PASS_CMD, timeout=1, catch_output=True)

        assert result.returncode == 0


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
