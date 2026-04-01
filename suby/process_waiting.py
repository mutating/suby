import os
import select
import subprocess
import sys
from subprocess import Popen
from typing import Callable, Optional

_event_driven_waiter: Optional[Callable[[int, float], None]] = None

if sys.platform == 'linux' and hasattr(os, 'pidfd_open'):  # pragma: no cover
    def _wait_pidfd(pid: int, timeout_seconds: float) -> None:
        fd = os.pidfd_open(pid)  # type: ignore[attr-defined]
        try:
            poller = select.poll()
            poller.register(fd, select.POLLIN)
            poller.poll(timeout_seconds * 1000)
        finally:
            os.close(fd)

    _event_driven_waiter = _wait_pidfd

elif hasattr(select, 'kqueue'):  # pragma: no cover
    def _wait_kqueue(pid: int, timeout_seconds: float) -> None:
        kq = select.kqueue()
        try:
            ev = select.kevent(
                pid,
                filter=select.KQ_FILTER_PROC,
                flags=select.KQ_EV_ADD | select.KQ_EV_ONESHOT,
                fflags=select.KQ_NOTE_EXIT,
            )
            kq.control([ev], 1, timeout_seconds)
        finally:
            kq.close()

    _event_driven_waiter = _wait_kqueue


def has_event_driven_wait() -> bool:
    return _event_driven_waiter is not None


def wait_for_process_exit(process: 'Popen[str]', timeout_seconds: float) -> None:
    if _event_driven_waiter is not None:
        try:
            _event_driven_waiter(process.pid, timeout_seconds)
            return
        except OSError:
            pass
    try:
        process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        pass
