import re

from suby.subprocess_result import SubprocessResult


def test_auto_id():
    assert SubprocessResult().id != SubprocessResult().id
    assert isinstance(SubprocessResult().id, str)
    assert len(SubprocessResult().id) > 10


def test_id_has_no_dashes():
    assert '-' not in SubprocessResult().id


def test_id_length_is_32():
    assert len(SubprocessResult().id) == 32


def test_default_values():
    assert SubprocessResult().stdout is None
    assert SubprocessResult().stderr is None
    assert SubprocessResult().returncode is None
    assert SubprocessResult().killed_by_token == False


def test_repr_format():
    result = SubprocessResult()
    result.stdout = 'hello'
    result.stderr = ''
    result.returncode = 0

    assert re.fullmatch(
        r"SubprocessResult\(id='[0-9a-f]{32}', stdout='hello', stderr='', returncode=0, killed_by_token=False\)",
        repr(result),
    ) is not None
