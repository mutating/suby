import re
import sys

from suby import run
from suby.subprocess_result import SubprocessResult


def test_generated_id_has_expected_shape_and_is_unique():
    """Each SubprocessResult gets a unique 32-character lowercase hex identifier without dashes."""
    first_id = SubprocessResult().id
    second_id = SubprocessResult().id

    assert first_id != second_id
    assert isinstance(first_id, str)
    assert len(first_id) == 32
    assert '-' not in first_id


def test_same_command_run_results_have_distinct_ids():
    """Two results produced by the same command still have different ids, so callers can distinguish repeated runs."""
    first_result = run(sys.executable, '-c', 'pass', split=False)
    second_result = run(sys.executable, '-c', 'pass', split=False)

    assert first_result.id != second_result.id


def test_default_values():
    """A fresh SubprocessResult starts with empty process fields and killed_by_token=False."""
    assert SubprocessResult().stdout is None
    assert SubprocessResult().stderr is None
    assert SubprocessResult().returncode is None
    assert SubprocessResult().killed_by_token == False


def test_repr_format():
    """repr(SubprocessResult) includes the id and all public result fields in the expected format."""
    result = SubprocessResult()
    result.stdout = 'hello'
    result.stderr = ''
    result.returncode = 0

    assert re.fullmatch(
        r"SubprocessResult\(id='[0-9a-f]{32}', stdout='hello', stderr='', returncode=0, killed_by_token=False\)",
        repr(result),
    ) is not None
