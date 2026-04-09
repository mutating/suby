import importlib
import os
from contextlib import contextmanager, nullcontext
from threading import Thread
from unittest.mock import patch

import pytest

_run_module = importlib.import_module('suby.run')



def pytest_ignore_collect(collection_path, config):
    """Skip typing-snippet tests in mutmut's copied test tree, because mutmut is a runtime mutation runner."""
    normalized_path = collection_path.as_posix()
    _ = config

    return (
        '/tests/typing/' in f'{normalized_path}/'
        and ('MUTANT_UNDER_TEST' in os.environ or '/mutants/tests/typing/' in f'{normalized_path}/')
    )


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
