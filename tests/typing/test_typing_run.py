from pathlib import Path, PurePath
from typing import Any, Optional, TypedDict

import pytest
from cantok import AbstractToken, ConditionToken, DefaultToken, SimpleToken
from emptylog import EmptyLogger, MemoryLogger

import suby
from suby import run
from suby.subprocess_result import SubprocessResult


@pytest.fixture(autouse=True)
def skip_runtime_typing_tests(request: pytest.FixtureRequest) -> None:
    """Only the [mypy] clone of each test should execute the snippet; the runtime clone is skipped."""
    if not request.node.name.startswith('[mypy]'):
        pytest.skip('This test body is a mypy snippet, not a runtime test.')


@pytest.mark.mypy_testing
def test_run_accepts_string_and_path_arguments() -> None:
    """Positional command arguments can be str, Path, or unpacked tuples of str/Path values."""
    run('python -c pass')
    run('python', '-c', 'pass')
    run(Path('python'), '-c', 'pass')
    run(Path('python'), Path('script.py'))

    string_args: tuple[str, ...] = ('python', '-c', 'pass')
    mixed_args: tuple[str | Path, ...] = (Path('python'), '-c', 'pass')
    run(*string_args)
    run(*mixed_args)


@pytest.mark.mypy_testing
def test_run_rejects_non_string_non_path_arguments() -> None:
    """mypy rejects positional command arguments whose static type is not str or Path."""
    run(b'python')  # E: [arg-type]
    run(123)  # E: [arg-type]
    run(object())  # E: [arg-type]
    run(None)  # E: [arg-type]
    run(PurePath('python'))  # E: [arg-type]

    object_args: tuple[object, ...] = ('python', '-c', 'pass')
    run(*object_args)  # E: [arg-type]

    bytes_args: tuple[bytes, ...] = (b'python',)
    run(*bytes_args)  # E: [arg-type]


@pytest.mark.mypy_testing
def test_run_rejects_list_or_tuple_passed_as_single_command_argument() -> None:
    """A list or tuple command must be unpacked with *, not passed as one positional argument."""
    command_list = ['python', '-c', 'pass']
    command_tuple = ('python', '-c', 'pass')

    run(command_list)  # E: [arg-type]
    run(command_tuple)  # E: [arg-type]


@pytest.mark.mypy_testing
def test_run_accepts_boolean_keyword_flags() -> None:
    """The boolean control flags accept bool literals and bool-typed variables."""
    flag: bool = True

    run('python -c pass', catch_output=True)
    run('python -c pass', catch_exceptions=False)
    run('python -c pass', split=flag)
    run('python -c pass', double_backslash=flag)
    run('python -c pass', catch_output=flag, catch_exceptions=flag, split=flag, double_backslash=flag)


@pytest.mark.mypy_testing
def test_run_rejects_non_boolean_keyword_flags_and_unknown_kwargs() -> None:
    """mypy rejects non-bool values for bool flags and keyword names that are not part of run()'s API."""
    run('python -c pass', catch_output=1)  # E: [arg-type]
    run('python -c pass', catch_exceptions='yes')  # E: [arg-type]
    run('python -c pass', split=None)  # E: [arg-type]
    run('python -c pass', double_backslash='false')  # E: [arg-type]
    run('python -c pass', catchOutput=True)  # E: [call-arg]
    run('python -c pass', cwd='.')  # E: [call-arg]
    run('python -c pass', env={})  # E: [call-arg]


@pytest.mark.mypy_testing
def test_run_rejects_keyword_flags_passed_positionally() -> None:
    """Passing a bool as a positional argument is a command-argument type error, not a shorthand for a flag."""
    run('python -c pass', True)  # E: [arg-type]


@pytest.mark.mypy_testing
def test_run_accepts_valid_logger_objects() -> None:
    """Built-in loggers and structural LoggerProtocol implementations are accepted."""
    class CustomLogger:
        def debug(self, message: str, *args: Any, **kwargs: Any) -> None:
            pass

        def info(self, message: str, *args: Any, **kwargs: Any) -> None:
            pass

        def warning(self, message: str, *args: Any, **kwargs: Any) -> None:
            pass

        def error(self, message: str, *args: Any, **kwargs: Any) -> None:
            pass

        def exception(self, message: str, *args: Any, **kwargs: Any) -> None:
            pass

        def critical(self, message: str, *args: Any, **kwargs: Any) -> None:
            pass

    run('python -c pass', logger=EmptyLogger())
    run('python -c pass', logger=MemoryLogger())
    run('python -c pass', logger=CustomLogger())


@pytest.mark.mypy_testing
def test_run_rejects_invalid_logger_objects() -> None:
    """mypy rejects logger objects that do not satisfy LoggerProtocol."""
    class MissingMethodsLogger:
        def info(self, message: str) -> None:
            pass

    run('python -c pass', logger=MissingMethodsLogger())  # E: [arg-type]
    run('python -c pass', logger=None)  # E: [arg-type]
    run('python -c pass', logger=object())  # E: [arg-type]


@pytest.mark.mypy_testing
def test_run_accepts_valid_stdout_and_stderr_callbacks() -> None:
    """Callbacks may accept a str line, use a wider object parameter, and return any value."""
    def callback(line: str) -> None:
        pass

    def callback_with_non_none_return(line: str) -> int:
        return len(line)

    def callback_with_wider_input(line: object) -> None:
        pass

    class CallableCallback:
        def __call__(self, line: str) -> object:
            return line

    run('python -c pass', stdout_callback=callback)
    run('python -c pass', stderr_callback=callback)
    run('python -c pass', stdout_callback=callback_with_non_none_return)
    run('python -c pass', stderr_callback=callback_with_non_none_return)
    run('python -c pass', stdout_callback=callback_with_wider_input)
    run('python -c pass', stderr_callback=callback_with_wider_input)
    run('python -c pass', stdout_callback=CallableCallback())
    run('python -c pass', stderr_callback=CallableCallback())
    run('python -c pass', stdout_callback=lambda _line: None, stderr_callback=lambda _line: None)


@pytest.mark.mypy_testing
def test_run_rejects_callbacks_with_invalid_signatures() -> None:
    """mypy rejects callbacks with the wrong arity, the wrong input type, or a non-callable value."""
    def callback_without_arguments() -> None:
        pass

    def callback_with_extra_argument(line: str, extra: int) -> None:
        pass

    def callback_with_bytes_input(line: bytes) -> None:
        pass

    run('python -c pass', stdout_callback=callback_without_arguments)  # E: [arg-type]
    run('python -c pass', stderr_callback=callback_without_arguments)  # E: [arg-type]
    run('python -c pass', stdout_callback=callback_with_extra_argument)  # E: [arg-type]
    run('python -c pass', stderr_callback=callback_with_extra_argument)  # E: [arg-type]
    run('python -c pass', stdout_callback=callback_with_bytes_input)  # E: [arg-type]
    run('python -c pass', stderr_callback=callback_with_bytes_input)  # E: [arg-type]
    run('python -c pass', stdout_callback=None)  # E: [arg-type]
    run('python -c pass', stderr_callback=123)  # E: [arg-type]


@pytest.mark.mypy_testing
def test_run_accepts_valid_timeout_values() -> None:
    """timeout accepts None, int, float, and variables typed as int | float | None."""
    maybe_timeout: int | float | None = 1

    run('python -c pass', timeout=None)
    run('python -c pass', timeout=1)
    run('python -c pass', timeout=0)
    run('python -c pass', timeout=0.5)
    run('python -c pass', timeout=maybe_timeout)


@pytest.mark.mypy_testing
def test_run_rejects_invalid_timeout_types() -> None:
    """mypy rejects timeout values whose static type is not int, float, or None."""
    run('python -c pass', timeout='1')  # E: [arg-type]
    run('python -c pass', timeout=b'1')  # E: [arg-type]
    run('python -c pass', timeout=object())  # E: [arg-type]


@pytest.mark.mypy_testing
def test_run_accepts_valid_tokens() -> None:
    """token accepts AbstractToken instances, concrete cantok tokens, unions of token subclasses, and custom subclasses."""
    token: AbstractToken = SimpleToken()
    union_token: SimpleToken | ConditionToken = SimpleToken()

    class CustomToken(AbstractToken):
        def _superpower(self) -> bool:
            return True

        def _get_superpower_exception_message(self) -> str:
            return 'never cancels'

        def _text_representation_of_superpower(self) -> str:
            return 'never cancels'

    run('python -c pass', token=DefaultToken())
    run('python -c pass', token=SimpleToken())
    run('python -c pass', token=ConditionToken(lambda: False))
    run('python -c pass', token=token)
    run('python -c pass', token=union_token)
    run('python -c pass', token=CustomToken())


@pytest.mark.mypy_testing
def test_run_rejects_invalid_token_objects() -> None:
    """mypy rejects None, arbitrary objects, token classes, and duck-typed objects that do not inherit AbstractToken."""
    class DuckToken:
        def __bool__(self) -> bool:
            return True

        def __iadd__(self, other: object) -> 'DuckToken':
            return self

        def check(self) -> None:
            pass

    run('python -c pass', token=None)  # E: [arg-type]
    run('python -c pass', token='token')  # E: [arg-type]
    run('python -c pass', token=object())  # E: [arg-type]
    run('python -c pass', token=SimpleToken)  # E: [arg-type]
    run('python -c pass', token=DuckToken())  # E: [arg-type]


@pytest.mark.mypy_testing
def test_run_result_and_result_fields_have_expected_types() -> None:
    """run() returns SubprocessResult, and its fields keep their Optional/boolean static types."""
    result = run('python -c pass')

    reveal_type(result)  # R: suby.subprocess_result.SubprocessResult
    reveal_type(result.stdout)  # R: Union[builtins.str, None]
    reveal_type(result.stderr)  # R: Union[builtins.str, None]
    reveal_type(result.returncode)  # R: Union[builtins.int, None]
    reveal_type(result.killed_by_token)  # R: builtins.bool

    _explicit_result: SubprocessResult = run('python -c pass')
    _optional_stdout: str | None = result.stdout
    _optional_stderr: str | None = result.stderr
    _optional_returncode: int | None = result.returncode
    _killed_by_token: bool = result.killed_by_token

    if result.stdout is not None:
        result.stdout.upper()
    if result.stderr is not None:
        result.stderr.upper()
    if result.returncode is not None:
        result.returncode + 1


@pytest.mark.mypy_testing
def test_run_result_optional_fields_require_none_checks() -> None:
    """stdout, stderr, and returncode are Optional fields, so mypy requires None checks before treating them as concrete values."""
    result = run('python -c pass')

    _stdout: str = result.stdout  # E: [assignment]
    _stderr: str = result.stderr  # E: [assignment]
    _returncode: int = result.returncode  # E: [assignment]
    result.stdout.upper()  # E: [union-attr]
    result.stderr.upper()  # E: [union-attr]
    _killed_by_token: str = result.killed_by_token  # E: [assignment]


@pytest.mark.mypy_testing
def test_run_can_be_imported_from_package_root_and_module() -> None:
    """Both suby.run(...) and from suby import run expose the same SubprocessResult return type."""
    package_result = suby.run('python -c pass')
    direct_result = run('python -c pass')

    reveal_type(package_result)  # R: suby.subprocess_result.SubprocessResult
    reveal_type(direct_result)  # R: suby.subprocess_result.SubprocessResult


@pytest.mark.mypy_testing
def test_run_accepts_optional_values_after_explicit_narrowing() -> None:
    """Optional command and token values become valid arguments after an explicit is not None check."""
    maybe_command: str | Path | None = 'python -c pass'
    maybe_token: Optional[AbstractToken] = SimpleToken()

    if maybe_command is not None:
        run(maybe_command)

    if maybe_token is not None:
        run('python -c pass', token=maybe_token)


@pytest.mark.mypy_testing
def test_run_rejects_optional_values_without_explicit_narrowing() -> None:
    """Optional command and token values are rejected until the None branch is ruled out."""
    maybe_command: str | None = 'python -c pass'
    maybe_token: Optional[AbstractToken] = SimpleToken()

    run(maybe_command)  # E: [arg-type]
    run('python -c pass', token=maybe_token)  # E: [arg-type]


@pytest.mark.mypy_testing
def test_run_accepts_valid_typed_kwargs_unpacking() -> None:
    """A TypedDict with run()'s keyword names and value types can be unpacked into **kwargs."""
    class RunKwargs(TypedDict, total=False):
        catch_output: bool
        catch_exceptions: bool
        split: bool
        double_backslash: bool
        timeout: int | float | None

    kwargs: RunKwargs = {
        'catch_output': True,
        'catch_exceptions': False,
        'split': True,
        'double_backslash': True,
        'timeout': 1,
    }

    run('python -c pass', **kwargs)


@pytest.mark.mypy_testing
def test_run_rejects_invalid_typed_kwargs_unpacking() -> None:
    """A TypedDict with a wrong value type or an unknown key is rejected when unpacked into run()."""
    class WrongTimeoutKwargs(TypedDict, total=False):
        timeout: str

    class UnknownKwargs(TypedDict, total=False):
        cwd: str

    wrong_timeout_kwargs: WrongTimeoutKwargs = {'timeout': '1'}
    unknown_kwargs: UnknownKwargs = {'cwd': '.'}

    run('python -c pass', **wrong_timeout_kwargs)  # E: [arg-type]
    run('python -c pass', **unknown_kwargs)  # E: [misc]


@pytest.mark.mypy_testing
def test_run_static_return_type_is_not_changed_by_catch_exceptions() -> None:
    """catch_exceptions changes runtime error handling, but the static return type remains SubprocessResult."""
    caught_result = run('python -c pass', catch_exceptions=True)
    raised_result = run('python -c pass', catch_exceptions=False)

    reveal_type(caught_result)  # R: suby.subprocess_result.SubprocessResult
    reveal_type(raised_result)  # R: suby.subprocess_result.SubprocessResult


@pytest.mark.mypy_testing
def test_run_known_typing_gaps_are_currently_accepted() -> None:
    """Current known gaps: mypy still accepts run() with no command args, timeout=True, and async callbacks."""
    async def async_callback(line: str) -> None:
        pass

    run()
    run('python -c pass', timeout=True)
    run('python -c pass', stdout_callback=async_callback)
