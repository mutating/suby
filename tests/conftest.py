import importlib
import os
from contextlib import contextmanager, nullcontext
from threading import Thread
from unittest.mock import patch

import pytest

_run_module = importlib.import_module('suby.run')


def pytest_configure(config):
    """Keep coverage enabled for xdist workers, but disable inheritance by child `run(... python -c ...)` subprocesses.

    If this hook is removed, every short-lived Python subprocess spawned by the test suite will inherit
    `COVERAGE_PROCESS_START`, import coverage from the `.pth` bootstrap, and the parallel coverage run will become
    significantly slower again.
    """
    if hasattr(config, 'workerinput'):
        os.environ.pop('COVERAGE_PROCESS_START', None)


@pytest.fixture
def assert_no_suby_thread_leaks():
    created_threads = []

    class TrackingThread(Thread):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            created_threads.append(self)

    @contextmanager
    def check_threads():
        with nullcontext():
            yield

    with patch.object(_run_module, 'Thread', TrackingThread):
        yield check_threads

    assert [thread for thread in created_threads if thread.is_alive()] == []

    return check_threads
