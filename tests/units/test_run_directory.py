import os
import re
import sys
from pathlib import Path, PurePath
from threading import Thread
from unittest.mock import patch

import pytest
from cantok import ConditionToken
from emptylog import MemoryLogger
from full_match import match

import suby
from suby import RunningCommandError, WrongDirectoryError, run
from suby.errors import WrongDirectoryError as ModuleWrongDirectoryError

_run_module = __import__('suby.run', fromlist=[''])


def _child_cwd(**run_kwargs) -> Path:
    result = run(
        sys.executable,
        '-c',
        'import os; print(os.getcwd())',
        split=False,
        catch_output=True,
        **run_kwargs,
    )

    assert result.stdout is not None
    assert result.returncode == 0
    return Path(result.stdout.rstrip('\n'))


def test_directory_error_is_exported_and_is_value_error():
    """WrongDirectoryError is public from both the package root and errors module."""
    assert suby.WrongDirectoryError is ModuleWrongDirectoryError
    assert WrongDirectoryError is ModuleWrongDirectoryError
    assert isinstance(WrongDirectoryError('bad directory'), ValueError)
    assert not hasattr(WrongDirectoryError('bad directory'), 'result')


def test_run_without_directory_inherits_parent_cwd_and_does_not_pass_cwd_to_popen():
    """Omitting directory keeps the old child cwd and Popen kwargs shape."""
    assert _child_cwd().resolve() == Path.cwd().resolve()

    with patch.object(_run_module, 'Popen', side_effect=FileNotFoundError('mocked missing command')) as mock_popen:
        run('python -c pass', catch_exceptions=True)

    assert 'cwd' not in mock_popen.call_args.kwargs


def test_run_with_directory_none_matches_omitted_directory():
    """directory=None is explicit no-op and does not pass cwd to Popen."""
    assert _child_cwd(directory=None).resolve() == Path.cwd().resolve()

    with patch.object(_run_module, 'Popen', side_effect=FileNotFoundError('mocked missing command')) as mock_popen:
        run('python -c pass', catch_exceptions=True, directory=None)

    assert 'cwd' not in mock_popen.call_args.kwargs


@pytest.mark.parametrize(
    'directory_factory',
    [
        pytest.param(lambda path: str(path), id='absolute-str'),
        pytest.param(lambda path: path, id='path-object'),
        pytest.param(lambda path: path.resolve(), id='resolved-path'),
    ],
)
def test_run_executes_in_absolute_directory_values(tmp_path, directory_factory):
    """directory accepts strings and Path objects without changing the parent cwd."""
    workdir = tmp_path / 'workdir'
    workdir.mkdir()
    parent_before = Path.cwd()
    directory = directory_factory(workdir)

    child_cwd = _child_cwd(directory=directory)

    assert child_cwd.resolve() == workdir.resolve()
    assert Path.cwd() == parent_before


def test_run_accepts_path_cwd_as_directory(tmp_path, monkeypatch):
    """A Path.cwd() value is accepted as an already-built Path object."""
    workdir = tmp_path / 'workdir'
    workdir.mkdir()
    monkeypatch.chdir(workdir)
    parent_before = Path.cwd()

    child_cwd = _child_cwd(directory=Path.cwd())

    assert child_cwd.resolve() == workdir.resolve()
    assert Path.cwd() == parent_before


@pytest.mark.parametrize(
    ('directory', 'expected_relative'),
    [
        ('.', '.'),
        ('./', '.'),
        ('..', '..'),
        ('./../.././target/', '../../target'),
        ('target/', 'target'),
    ],
)
def test_run_executes_in_relative_directory_values(tmp_path, monkeypatch, directory, expected_relative):
    """Relative directory values are interpreted from the parent cwd at call time."""
    root = tmp_path / 'root'
    call_site = root / 'a' / 'b'
    target = root / 'target'
    call_site.mkdir(parents=True)
    target.mkdir(parents=True)
    (call_site / 'target').mkdir()
    monkeypatch.chdir(call_site)

    child_cwd = _child_cwd(directory=directory)

    assert child_cwd.resolve() == (call_site / expected_relative).resolve()


@pytest.mark.parametrize('name', ['dir with spaces', ' ', '  ', 'директория'])
def test_directory_string_is_not_split_or_normalized(tmp_path, name):
    """Spaces and unicode in directory names are passed as one cwd value."""
    directory = tmp_path / name
    directory.mkdir()

    assert _child_cwd(directory=str(directory)).resolve() == directory.resolve()


def test_tilde_is_treated_as_literal_relative_path(tmp_path, monkeypatch):
    """directory does not perform shell-like tilde expansion."""
    monkeypatch.chdir(tmp_path)

    with pytest.raises(WrongDirectoryError, match='does not exist'):
        run(sys.executable, '-c', 'pass', split=False, directory='~/missing')


def test_symlink_to_directory_is_accepted(tmp_path):
    """A symlink whose target is a directory can be used as directory."""
    target = tmp_path / 'target'
    link = tmp_path / 'link'
    target.mkdir()
    try:
        link.symlink_to(target, target_is_directory=True)
    except (OSError, NotImplementedError) as error:
        pytest.skip(f'symlink creation is unavailable: {error}')

    assert _child_cwd(directory=link).resolve() == target.resolve()


def test_broken_symlink_is_reported_as_missing_directory(tmp_path):
    """A broken directory symlink is classified as a missing directory."""
    link = tmp_path / 'broken'
    try:
        link.symlink_to(tmp_path / 'missing', target_is_directory=True)
    except (OSError, NotImplementedError) as error:
        pytest.skip(f'symlink creation is unavailable: {error}')

    with pytest.raises(WrongDirectoryError, match='does not exist') as exc_info:
        run(sys.executable, '-c', 'pass', split=False, directory=link)

    assert isinstance(exc_info.value.__cause__, FileNotFoundError)


def test_symlink_to_file_is_reported_as_not_directory(tmp_path):
    """A symlink to a regular file is not an acceptable directory."""
    target = tmp_path / 'file.txt'
    link = tmp_path / 'file-link'
    target.write_text('content')
    try:
        link.symlink_to(target)
    except (OSError, NotImplementedError) as error:
        pytest.skip(f'symlink creation is unavailable: {error}')

    with pytest.raises(WrongDirectoryError, match='not a directory'):
        run(sys.executable, '-c', 'pass', split=False, directory=link)


def test_directory_composes_with_other_run_options(tmp_path):
    """directory only changes cwd; other run options keep their existing behavior."""
    directory = tmp_path / 'work'
    directory.mkdir()
    result = run(
        sys.executable,
        '-c',
        'import os, sys; print(os.environ["SUBY_DIRECTORY_ENV"]); sys.exit(3)',
        split=False,
        directory=directory,
        add_env={'SUBY_DIRECTORY_ENV': 'child'},
        catch_output=True,
        catch_exceptions=True,
    )

    assert result.stdout == 'child\n'
    assert result.returncode == 3


def test_directory_composes_with_split_false_and_token(tmp_path):
    """directory is independent from command parsing and token handling."""
    directory = tmp_path / 'work'
    directory.mkdir()
    token = ConditionToken(lambda: False)

    result = run(
        sys.executable,
        '-c',
        'import os; print(os.getcwd())',
        split=False,
        directory=directory,
        token=token,
        catch_output=True,
    )

    assert Path(result.stdout.strip()).resolve() == directory.resolve()


def test_directory_composes_with_stream_callbacks(tmp_path):
    """directory changes child cwd without bypassing stdout/stderr callbacks."""
    directory = tmp_path / 'work'
    directory.mkdir()
    stdout_lines = []
    stderr_lines = []

    result = run(
        sys.executable,
        '-c',
        'import os, sys; print(os.getcwd()); sys.stderr.write(os.getcwd() + "\\n")',
        split=False,
        directory=directory,
        stdout_callback=stdout_lines.append,
        stderr_callback=stderr_lines.append,
    )

    assert result.returncode == 0
    assert Path(stdout_lines[0].rstrip('\n')).resolve() == directory.resolve()
    assert Path(stderr_lines[0].rstrip('\n')).resolve() == directory.resolve()


def test_directory_with_catch_output_bypasses_stream_callbacks(tmp_path):
    """catch_output=True keeps bypassing callbacks when directory is set."""
    directory = tmp_path / 'work'
    directory.mkdir()
    stdout_lines = []
    stderr_lines = []

    result = run(
        sys.executable,
        '-c',
        'import sys; print("stdout from child"); sys.stderr.write("stderr from child\\n")',
        split=False,
        directory=directory,
        catch_output=True,
        stdout_callback=stdout_lines.append,
        stderr_callback=stderr_lines.append,
    )

    assert result.returncode == 0
    assert result.stdout == 'stdout from child\n'
    assert result.stderr == 'stderr from child\n'
    assert stdout_lines == []
    assert stderr_lines == []


def test_directory_composes_with_logger(tmp_path):
    """directory does not change the existing successful logging path."""
    directory = tmp_path / 'work'
    directory.mkdir()
    logger = MemoryLogger()

    run(sys.executable, '-c', 'pass', split=False, directory=directory, logger=logger)

    assert len(logger.data.info) == 2
    assert len(logger.data.error) == 0
    assert len(logger.data) == 2
    assert logger.data.info[0].message == f'The beginning of the execution of the command "{sys.executable} -c pass".'
    assert logger.data.info[1].message == f'The command "{sys.executable} -c pass" has been successfully executed.'


def test_parallel_runs_use_independent_directories(tmp_path):
    """Concurrent run() calls can pass different directories without changing the parent cwd."""
    parent_before = Path.cwd()
    directories = []
    results = [None, None, None]
    for index in range(3):
        directory = tmp_path / f'work-{index}'
        directory.mkdir()
        directories.append(directory)

    def run_task(index: int) -> None:
        results[index] = _child_cwd(directory=directories[index])

    threads = [Thread(target=run_task, args=(index,)) for index in range(len(directories))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert [result.resolve() for result in results] == [directory.resolve() for directory in directories]
    assert Path.cwd() == parent_before


def test_run_passes_cwd_to_popen_without_changing_existing_popen_options(tmp_path):
    """Adding directory preserves the existing text pipe, buffering, and env options."""
    child_env = dict(os.environ)
    child_env['SUBY_DIRECTORY_POPEN_ENV'] = '1'

    with patch.object(_run_module, 'Popen', side_effect=FileNotFoundError('mocked missing command')) as mock_popen:
        run(sys.executable, '-c', 'pass', split=False, catch_exceptions=True, env=child_env, directory=tmp_path)

    mock_popen.assert_called_once_with(
        [sys.executable, '-c', 'pass'],
        stdout=_run_module.PIPE,
        stderr=_run_module.PIPE,
        bufsize=1,
        text=True,
        encoding='utf-8',
        errors='strict',
        env=child_env,
        cwd=str(tmp_path),
    )


@pytest.mark.parametrize(
    ('directory_factory', 'message'),
    [
        (lambda tmp_path: tmp_path / 'missing', 'does not exist'),
        (lambda tmp_path: tmp_path / 'file.txt', 'not a directory'),
        (lambda tmp_path: tmp_path / 'file.txt' / 'child', 'intermediate component'),
    ],
    ids=['missing-leaf', 'file-leaf', 'intermediate-file'],
)
def test_invalid_directory_paths_raise_wrong_directory_error(tmp_path, directory_factory, message):
    """Missing paths and file paths fail before subprocess startup."""
    file_path = tmp_path / 'file.txt'
    file_path.write_text('content')

    with pytest.raises(WrongDirectoryError, match=message):
        run(sys.executable, '-c', 'pass', split=False, directory=directory_factory(tmp_path))


@pytest.mark.parametrize(
    ('directory', 'error_type', 'message'),
    [
        ('abc\0def', WrongDirectoryError, 'null byte'),
        ('', WrongDirectoryError, re.escape('use "."')),
        (b'.', TypeError, 'bytes'),
        (123, TypeError, 'int'),
        (PurePath('.'), TypeError, 'Pure'),  # noqa: PTH201
        (object(), TypeError, 'object'),
    ],
    ids=['null-byte', 'empty-string', 'bytes', 'int', 'purepath', 'object'],
)
def test_invalid_directory_values_raise_descriptive_errors(directory, error_type, message):
    """Invalid directory values fail with argument-validation errors."""
    with pytest.raises(error_type, match=message):
        run(sys.executable, '-c', 'pass', split=False, directory=directory)  # type: ignore[arg-type]


def test_relative_directory_reports_current_working_directory_failure():
    """A failing Path.cwd() is reported as a directory validation error for relative paths."""
    original_error = OSError('cwd disappeared')

    with patch.object(_run_module.Path, 'cwd', side_effect=original_error), \
         pytest.raises(WrongDirectoryError, match='current working directory') as exc_info:
        run(sys.executable, '-c', 'pass', split=False, directory='relative')

    assert exc_info.value.__cause__ is original_error


def test_absolute_directory_does_not_call_path_cwd(tmp_path):
    """Absolute directory validation does not depend on the parent cwd lookup."""
    with patch.object(_run_module.Path, 'cwd', side_effect=AssertionError('Path.cwd should not be called')):
        assert _child_cwd(directory=tmp_path).resolve() == tmp_path.resolve()


@pytest.mark.parametrize(
    ('stat_error', 'message'),
    [
        (PermissionError('mocked permission denied'), r'(?i)permission denied'),
        (OSError('mocked stat exploded'), r'(?i)could not access'),
    ],
)
def test_directory_stat_oserrors_are_wrapped(stat_error, message):
    """Filesystem errors from stat() become WrongDirectoryError with the original cause."""
    directory = Path('/definitely/absolute/directory')

    with patch.object(_run_module.Path, 'stat', side_effect=stat_error), \
         pytest.raises(WrongDirectoryError, match=message) as exc_info:
        run(sys.executable, '-c', 'pass', split=False, directory=directory)

    assert exc_info.value.__cause__ is stat_error


@pytest.mark.skipif(os.name == 'nt', reason='POSIX-only directory execute bit semantics')
def test_directory_without_execute_access_is_rejected(tmp_path):
    """On POSIX, a directory without search permission is rejected before Popen."""
    directory = tmp_path / 'locked'
    directory.mkdir()

    with patch.object(_run_module.os, 'access', return_value=False), \
         pytest.raises(WrongDirectoryError, match=r'(?i)permission denied'):
        run(sys.executable, '-c', 'pass', split=False, directory=directory)


@pytest.mark.parametrize('catch_output', [False, True])
def test_invalid_directory_with_catch_exceptions_still_raises_and_does_not_start_process(tmp_path, catch_output):
    """Output capture options do not affect pre-start directory validation."""
    missing = tmp_path / 'missing'
    logger = MemoryLogger()

    with patch.object(_run_module, 'Popen') as mock_popen, \
         pytest.raises(WrongDirectoryError, match='does not exist'):
        run(
            sys.executable,
            '-c',
            'pass',
            split=False,
            catch_exceptions=True,
            catch_output=catch_output,
            logger=logger,
            directory=missing,
        )

    mock_popen.assert_not_called()
    assert len(logger.data) == 0


@pytest.mark.parametrize('catch_exceptions', [False, True])
def test_valid_directory_preserves_missing_executable_startup_behavior(tmp_path, catch_exceptions):
    """A valid directory does not change existing missing-executable startup handling."""
    if catch_exceptions:
        result = run('definitely_missing_command_for_directory_tests', catch_exceptions=True, directory=tmp_path)
        assert result.stdout == ''
        assert result.stderr == ''
        assert result.returncode == 1
    else:
        with pytest.raises(
            RunningCommandError,
            match=match('The executable for the command "definitely_missing_command_for_directory_tests" was not found.'),
        ) as exc_info:
            run('definitely_missing_command_for_directory_tests', directory=tmp_path)
        assert isinstance(exc_info.value.__cause__, FileNotFoundError)
