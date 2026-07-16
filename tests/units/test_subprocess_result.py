import re
import sys
from dataclasses import asdict, fields
from inspect import signature

import pytest

from suby import SubprocessResult, run


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
    """A fresh result has no process data and is neither killed by a cancellation token nor successful."""
    result = SubprocessResult()

    assert result.stdout is None
    assert result.stderr is None
    assert result.returncode is None
    assert result.killed_by_token == False
    assert result.success is False


@pytest.mark.parametrize(
    ('returncode', 'expected_success'),
    [
        (None, False),
        (0, True),
        (1, False),
        (-1, False),
    ],
)
@pytest.mark.parametrize('killed_by_token', [False, True])
def test_success_reflects_returncode(returncode, expected_success, killed_by_token):
    """The success property depends only on whether returncode is exactly zero and leaves returncode and killed_by_token unchanged."""
    result = SubprocessResult(returncode=returncode, killed_by_token=killed_by_token)

    assert result.success is expected_success
    assert result.returncode == returncode
    assert type(result.returncode) is type(returncode)
    assert result.killed_by_token is killed_by_token


@pytest.mark.parametrize('killed_by_token', [False, True])
def test_success_is_recomputed_after_returncode_changes(killed_by_token):
    """For both killed_by_token values, success is recomputed after each returncode change, and reading it preserves the current returncode and killed_by_token."""
    result = SubprocessResult(killed_by_token=killed_by_token)

    assert result.success is False
    assert result.returncode is None
    assert result.killed_by_token is killed_by_token

    result.returncode = 0
    assert result.success is True
    assert result.returncode == 0
    assert type(result.returncode) is int
    assert result.killed_by_token is killed_by_token

    result.returncode = 1
    assert result.success is False
    assert result.returncode == 1
    assert type(result.returncode) is int
    assert result.killed_by_token is killed_by_token


@pytest.mark.parametrize(
    ('returncode', 'expected_success'),
    [
        (0, True),
        (1, False),
    ],
)
@pytest.mark.parametrize('killed_by_token', [False, True])
@pytest.mark.parametrize('assigned_success', [False, True])
def test_success_assignment_is_forbidden(returncode, expected_success, killed_by_token, assigned_success):
    """Assigning either boolean to success raises the built-in AttributeError with the exact read-only assignment message while preserving returncode and its type, killed_by_token, and result.success."""
    result = SubprocessResult(returncode=returncode, killed_by_token=killed_by_token)

    with pytest.raises(AttributeError) as exc_info:
        result.success = assigned_success

    assert exc_info.type is AttributeError
    assert str(exc_info.value) == 'The success property is read-only and cannot be assigned.'
    assert result.returncode == returncode
    assert type(result.returncode) is type(returncode)
    assert result.killed_by_token is killed_by_token
    assert result.success is expected_success


@pytest.mark.parametrize(
    ('returncode', 'expected_success'),
    [
        (0, True),
        (1, False),
    ],
)
@pytest.mark.parametrize('killed_by_token', [False, True])
def test_success_deletion_is_forbidden(returncode, expected_success, killed_by_token):
    """Deleting success raises the built-in AttributeError with the exact read-only deletion message while preserving returncode and its type, killed_by_token, and result.success."""
    result = SubprocessResult(returncode=returncode, killed_by_token=killed_by_token)

    with pytest.raises(AttributeError) as exc_info:
        del result.success

    assert exc_info.type is AttributeError
    assert str(exc_info.value) == 'The success property is read-only and cannot be deleted.'
    assert result.returncode == returncode
    assert type(result.returncode) is type(returncode)
    assert result.killed_by_token is killed_by_token
    assert result.success is expected_success


@pytest.mark.parametrize(
    ('returncode', 'expected_success'),
    [
        (0, True),
        (1, False),
    ],
)
def test_run_result_success_matches_process_exit_code(returncode, expected_success):
    """With catch_exceptions=True, the returned results retain exit codes zero and one and report success only for zero."""
    result = run(
        sys.executable,
        '-c',
        f'import sys; sys.exit({returncode})',
        split=False,
        catch_exceptions=True,
    )

    assert result.returncode == returncode
    assert result.success is expected_success


def test_success_is_not_a_dataclass_field():
    """The dataclass field names, asdict() output, and generated constructor parameter names remain unchanged and exclude success."""
    result = SubprocessResult(stdout='output', stderr='error', returncode=0)
    expected_field_values = {
        'id': result.id,
        'stdout': 'output',
        'stderr': 'error',
        'returncode': 0,
        'killed_by_token': False,
    }
    expected_field_names = tuple(expected_field_values)

    assert tuple(field.name for field in fields(result)) == expected_field_names
    assert asdict(result) == expected_field_values
    assert tuple(signature(SubprocessResult).parameters) == expected_field_names


def test_repr_format():
    """repr(SubprocessResult) includes every dataclass field but excludes the computed success property."""
    result = SubprocessResult()
    result.stdout = 'hello'
    result.stderr = ''
    result.returncode = 0

    assert re.fullmatch(
        r"SubprocessResult\(id='[0-9a-f]{32}', stdout='hello', stderr='', returncode=0, killed_by_token=False\)",
        repr(result),
    ) is not None
