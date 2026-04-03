import os
import select
import subprocess
import sys
from subprocess import Popen
from typing import Callable, Optional, cast

_event_driven_waiter: Optional[Callable[[int, Optional[float]], None]] = None

if sys.platform == 'linux' and hasattr(os, 'pidfd_open'):
    pidfd_open = cast(Callable[[int], int], os.pidfd_open)

    def _wait_pidfd(pid: int, timeout_seconds: Optional[float]) -> None:
        fd = pidfd_open(pid)
        try:
            poller = select.poll()
            poller.register(fd, select.POLLIN)
            timeout_milliseconds = None if timeout_seconds is None else timeout_seconds * 1000
            poller.poll(timeout_milliseconds)
        finally:
            os.close(fd)

    _event_driven_waiter = _wait_pidfd

elif sys.platform == 'darwin':
    if not hasattr(select, 'kqueue'):  # pragma: no cover (Darwin)
        _event_driven_waiter = None
    else:
        def _wait_kqueue(pid: int, timeout_seconds: Optional[float]) -> None:  # pragma: no cover (!Darwin)
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


def wait_for_process_exit(process: 'Popen[str]', timeout_seconds: Optional[float]) -> None:
    if _event_driven_waiter is not None:
        try:
            _event_driven_waiter(process.pid, timeout_seconds)
            return
        except OSError:
            pass
    if timeout_seconds is None:
        process.wait()
        return
    try:
        process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        pass
