<details>
  <summary>ⓘ</summary>

[![Downloads](https://static.pepy.tech/badge/suby/month)](https://pepy.tech/project/suby)
[![Downloads](https://static.pepy.tech/badge/suby)](https://pepy.tech/project/suby)
[![Coverage Status](https://coveralls.io/repos/github/mutating/suby/badge.svg?branch=main)](https://coveralls.io/github/mutating/suby?branch=main)
[![Lines of code](https://sloc.xyz/github/mutating/suby/?category=code)](https://github.com/boyter/scc/)
[![Hits-of-Code](https://hitsofcode.com/github/mutating/suby?branch=main)](https://hitsofcode.com/github/mutating/suby/view?branch=main)
[![Test-Package](https://github.com/mutating/suby/actions/workflows/tests_and_coverage.yml/badge.svg)](https://github.com/mutating/suby/actions/workflows/tests_and_coverage.yml)
[![Python versions](https://img.shields.io/pypi/pyversions/suby.svg)](https://pypi.python.org/pypi/suby)
[![PyPI version](https://badge.fury.io/py/suby.svg)](https://badge.fury.io/py/suby)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/mutating/suby)
[![CodSpeed](https://img.shields.io/endpoint?url=https://codspeed.io/badge.json)](https://codspeed.io/mutating/suby?utm_source=badge)

</details>


![logo](https://raw.githubusercontent.com/mutating/suby/develop/docs/assets/logo_5.svg)


Suby is a small wrapper around the [subprocess](https://docs.python.org/3/library/subprocess.html) module. You can find many similar wrappers, but this particular one differs from the others in the following ways:

- Beautiful minimalistic call syntax.
- Ability to specify your callbacks to catch `stdout` and `stderr`.
- Support for [cancellation tokens](https://github.com/pomponchik/cantok).
- Ability to set timeouts for subprocesses.
- Efficient event-driven process waiting using `pidfd` (Linux) and `kqueue` (macOS).
- Logging of command execution.


## Table of contents

- [**Quick start**](#quick-start)
- [**Run subprocess and look at the result**](#run-subprocess-and-look-at-the-result)
- [**Command parsing**](#command-parsing)
- [**Output**](#output)
- [**Logging**](#logging)
- [**Exceptions**](#exceptions)
- [**Working with Cancellation Tokens**](#working-with-cancellation-tokens)
- [**Timeouts**](#timeouts)
- [**Environment variables**](#environment-variables)
- [**Changing directories**](#changing-directories)


## Quick start

Install it:

```bash
pip install suby
```

And use it:

```python
from suby import run

run('python -c "print(\'hello, world!\')"')
#> hello, world!
```

You can also quickly try out this and other packages without installing them, using [instld](https://github.com/pomponchik/instld).


## Run subprocess and look at the result

Import the `run` function like this:

```python
from suby import run
```

Let's try to call it:

```python
result = run('python -c "print(\'hello, world!\')"')
print(result)
#> SubprocessResult(id='e9f2d29acb4011ee8957320319d7541c', stdout='hello, world!\n', stderr='', returncode=0, killed_by_token=False)
```

It returns an object of the `SubprocessResult` class, which contains the following fields:

- **id**: a unique string that allows you to distinguish one result of calling the same command from another.
- **stdout**: a string containing the entire output of the command being run.
- **stderr**: a string containing the entire stderr output of the command being run. If the subprocess fails to start at all, this field remains empty because no process stderr existed yet.
- **returncode**: an integer indicating the return code of the subprocess. `0` means that the process was completed successfully; other values usually indicate an error.
- **killed_by_token**: a boolean flag indicating whether the subprocess was killed due to [token](https://cantok.readthedocs.io/en/latest/the_pattern/) cancellation.


## Command parsing

`suby` always builds an argument list for `subprocess`. By default, every string positional argument is split with [shlex](https://docs.python.org/3/library/shlex.html), and the resulting parts are concatenated.

The contract is:

- `str`: split with `shlex`
- `Path`: converted to `str` without splitting
- `split=False`: disable splitting for all string arguments

Examples:

```python
run('python -c "print(\'hello, world!\')"')
run('python', '-c "print(777)"')
```

`Path` arguments are passed through unchanged except for string conversion:

```python
import sys
from pathlib import Path

run(Path(sys.executable), '-c print(777)')
```

If you pass `split=False`, you must provide arguments in their final form:

```python
run('python', '-c', 'print(777)', split=False)
```

<details>
  <summary>Windows has its own quirks when it comes to backslashes.</summary>

### Backslashes on Windows

The [shlex](https://docs.python.org/3/library/shlex.html) module operates in POSIX mode, which means it treats the backslash (`\`) as an escape character. This is problematic on Windows, where backslashes are used as path separators — `shlex` would silently eat them.

To work around this, `suby` automatically doubles all backslashes in command strings before passing them to `shlex` on Windows. This is controlled by the `double_backslash` parameter, which defaults to `True` on Windows and `False` on other platforms:

```python
# On Windows, backslashes in paths are preserved by default:
run(r'C:\Python\python.exe -c pass')

# You can disable this behavior:
run(r'C:\Python\python.exe -c pass', double_backslash=False)

# Or enable it on non-Windows platforms:
run(r'path\to\executable -c pass', double_backslash=True)
```

Note that this only affects string arguments that go through `shlex` splitting. `Path` objects and arguments passed with `split=False` are not affected.

</details>


## Output

By default, the `stdout` and `stderr` of the subprocess are forwarded to the `stdout` and `stderr` of the current process. Reading from the subprocess is continuous, and output is flushed each time a full line is read. `suby` reads `stdout` and `stderr` in separate threads so that neither stream blocks the other.

You can override the output functions for `stdout` and `stderr`. To do this, you need to pass functions accepting a string as an argument via the `stdout_callback` and `stderr_callback` parameters, respectively. For example, you can color the output (the code example uses the [`termcolor`](https://github.com/termcolor/termcolor) library):

```python
from termcolor import colored

def my_new_stdout(string: str) -> None:
    print(colored(string, 'red'), end='')

run('python -c "print(\'hello, world!\')"', stdout_callback=my_new_stdout)
#> hello, world!
# You can't see it here, but if you run this code yourself, the output in the console will be red!
```

You can also completely disable the output by passing `True` as the `catch_output` parameter:

```python
run('python -c "print(\'hello, world!\')"', catch_output=True)
# There's nothing here.
```

If you specify `catch_output=True`, even if you have also defined custom callback functions, they will not be called. In addition, `suby` always returns [the result](#run-subprocess-and-look-at-the-result) of executing the command, containing the full output. The `catch_output` argument can suppress only the output, but it does not prevent the buffering of output.

<details>
  <summary>Notes about concurrent output</summary>

When the subprocess is canceled or interrupted because a callback raises an exception, the collected `stdout` and `stderr` may contain only the output that was read before termination. If both streams are active at the same time, some trailing output may or may not be captured depending on timing.

</details>


## Logging

By default, `suby` does not log command execution. However, you can pass a logger object to `run`, and in this case messages will be logged at the start and end of command execution:

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
    ]
)

run('python -c pass', logger=logging.getLogger('logger_name'))
#> 2024-02-22 02:15:08,155 [INFO] The beginning of the execution of the command "python -c pass".
#> 2024-02-22 02:15:08,190 [INFO] The command "python -c pass" has been successfully executed.
```

The message about the start of the command execution is always logged at the `INFO` [level](https://docs.python.org/3/library/logging.html#logging-levels). If the command is completed successfully, the completion message will also be at the `INFO` level. If the command fails, it will be at the `ERROR` level:

```python
run('python -c "raise ValueError"', logger=logging.getLogger('logger_name'), catch_exceptions=True, catch_output=True)
#> 2024-02-22 02:20:25,549 [INFO] The beginning of the execution of the command "python -c "raise ValueError"".
#> 2024-02-22 02:20:25,590 [ERROR] Error when executing the command "python -c "raise ValueError"".
```

If you don't need these details, simply omit the logger argument.

If you still prefer logging, you can use any object that implements the [logger protocol](https://github.com/pomponchik/emptylog?tab=readme-ov-file#universal-logger-protocol) from the [`emptylog`](https://github.com/pomponchik/emptylog) library, including ones from third-party libraries.

If the subprocess cannot be started at all, `suby` logs a startup-specific message via `logger.exception(...)`. For example, a missing executable is logged as `The executable for the command "definitely_missing_command" was not found.`. Permission and other operating-system startup failures have dedicated startup messages too.


## Exceptions

By default, `suby` raises exceptions in four cases:

1. If the command exits with a return code not equal to `0`. In this case, a `RunningCommandError` exception will be raised:

```python
from suby import run, RunningCommandError

try:
    run('python -c 1/0')
except RunningCommandError as e:
    print(e)
    #> Error when executing the command "python -c 1/0".
```

2. If the subprocess cannot be started, `suby` raises `RunningCommandError` with a startup-specific message and chains the original `OSError` as `__cause__`. In this case, the attached `result.stdout` and `result.stderr` stay empty because the process never started:

```python
from suby import run, RunningCommandError

try:
    run('definitely_missing_command')
except RunningCommandError as e:
    print(e)
    #> The executable for the command "definitely_missing_command" was not found.
    print(type(e.__cause__))
    #> <class 'FileNotFoundError'>
    print(e.result.stderr)
    #>
```

3. If you pass a [cancellation token](https://cantok.readthedocs.io/en/latest/the_pattern/) when calling the command, and the token is canceled, an exception will be raised [corresponding to the type](https://cantok.readthedocs.io/en/latest/what_are_tokens/exceptions/) of the canceled token. [This feature](#working-with-cancellation-tokens) is integrated with the [cantok](https://cantok.readthedocs.io/en/latest/) library, so we recommend that you familiarize yourself with it first.

4. If a [timeout](#timeouts) you set for the operation expires.

You can prevent `suby` from raising these exceptions. To do this, set the `catch_exceptions` parameter to `True`:

```python
result = run('python -c "import time; time.sleep(10_000)"', timeout=1, catch_exceptions=True)
print(result)
#> SubprocessResult(id='c9125b90d03111ee9660320319d7541c', stdout='', stderr='', returncode=-9, killed_by_token=True)
```

Keep in mind that the full result of the subprocess call can also be found through the `result` attribute of any exception raised by `suby`:

```python
from suby import run, TimeoutCancellationError

try:
    run('python -c "import time; time.sleep(10_000)"', timeout=1)
except TimeoutCancellationError as e:
    print(e.result)
    #> SubprocessResult(id='a80dc26cd03211eea347320319d7541c', stdout='', stderr='', returncode=-9, killed_by_token=True)
```

`catch_exceptions=True` applies to the four subprocess-related cases above. Invalid [directory](#changing-directories) values still raise their validation exceptions before a subprocess is started.

<details>
  <summary>Notes about callback and token errors</summary>

If a custom `stdout_callback`, `stderr_callback`, or cancellation token raises its own exception, `suby` re-raises that exception and attaches the current `SubprocessResult` to its `result` attribute. The captured output may be partial.

If multiple failures happen concurrently, for example both callbacks raise at nearly the same time, `suby` raises the first one it observes. Which one that is may depend on timing.

If a callback raises after the subprocess has already exited, the exception is still propagated, but the attached `result` may contain a successful `returncode`.

If a timeout and another failure race with each other, the timeout may still win if it expires before a callback failure has been recorded. In that case, `suby` raises `TimeoutCancellationError` and the callback exception may not be observed.

If a token exception has already been recorded before the timeout path wins the race, `suby` keeps propagating that token exception instead.

In timeout-versus-callback races, whether the callback comes from `stdout` or `stderr`, the attached `result.killed_by_token` flag may be either `True` or `False`, depending on whether the timeout path marked the result before the callback failure path was handled.

If a timeout and a callback error happen almost at the same time, the exception you catch and the attached `result` may describe different parts of that race. For example, `suby` may re-raise the callback exception, but the attached `result` may still show that the subprocess was stopped by the timeout. This depends on timing.

</details>


## Working with Cancellation Tokens

`suby` is fully compatible with the [cancellation token pattern](https://cantok.readthedocs.io/en/latest/the_pattern/) and supports any token objects from the [`cantok`](https://github.com/pomponchik/cantok) library.

The essence of the pattern is that you can pass an object to `suby` that signals whether the operation should continue. If not, `suby` kills the subprocess. This pattern is especially useful for long-running or unpredictably slow commands. When the result becomes unnecessary, there is no point in sitting and waiting for the command to complete.

In practice, you can pass your cancellation tokens to `suby`. By default, canceling a token causes an exception to be raised:

```python
from random import randint
from cantok import ConditionToken

token = ConditionToken(lambda: randint(1, 1000) == 7)  # This token will be canceled when a random unlikely event occurs.
run('python -c "import time; time.sleep(10_000)"', token=token)
#> cantok.errors.ConditionCancellationError: The cancellation condition was satisfied.
```

However, if you pass the `catch_exceptions=True` argument, the exception will not be raised (see [Exceptions](#exceptions)). Instead, you will get the [usual result](#run-subprocess-and-look-at-the-result) of calling `run` with the `killed_by_token=True` flag:

```python
token = ConditionToken(lambda: randint(1, 1000) == 7)
print(run('python -c "import time; time.sleep(10_000)"', token=token, catch_exceptions=True))
#> SubprocessResult(id='e92ccd54d24b11ee8376320319d7541c', stdout='', stderr='', returncode=-9, killed_by_token=True)
```

Under the hood, token state is checked while `stdout` and `stderr` are being read. When the token is canceled, the subprocess is killed.

## Timeouts

You can set a timeout for `suby`. It must be a number greater than or equal to zero, which specifies the maximum number of seconds the subprocess is allowed to run. If the timeout expires before the subprocess completes, an exception will be raised:

```python
run('python -c "import time; time.sleep(10_000)"', timeout=1)
#> cantok.errors.TimeoutCancellationError: The timeout of 1 seconds has expired.
```

A timeout of `0` is valid and means that the subprocess will be canceled immediately if it has not already exited.

Under the hood, `run` uses [`TimeoutToken`](https://cantok.readthedocs.io/en/latest/types_of_tokens/TimeoutToken/) from the [`cantok`](https://github.com/pomponchik/cantok) library to track the timeout.

`suby` re-exports this exception:

```python
from suby import run, TimeoutCancellationError

try:
    run('python -c "import time; time.sleep(10_000)"', timeout=1)
except TimeoutCancellationError as e:  # As you can see, TimeoutCancellationError is available in the suby module.
    print(e)
    #> The timeout of 1 seconds has expired.
```

Just as with [regular cancellation tokens](#working-with-cancellation-tokens), you can prevent exceptions from being raised using the `catch_exceptions=True` argument:

```python
print(run('python -c "import time; time.sleep(10_000)"', timeout=1, catch_exceptions=True))
#> SubprocessResult(id='ea88c518d25011eeb25e320319d7541c', stdout='', stderr='', returncode=-9, killed_by_token=True)
```


## Environment variables

By default, a subprocess receives the same environment variables as the current process:

```python
run('python -c "import os; print(os.environ.get(\'MY_VARIABLE\'))"')
```

Use `env` when the subprocess should receive exactly the variables you provide:

```python
run(
    'python -c "import os; print(os.environ.get(\'MY_VARIABLE\'))"',
    env={'MY_VARIABLE': 'hello'},
)
#> hello
```

When `env` is provided, variables from the current process are not added automatically. `env={}` means a truly empty environment, which may make some executables fail on platforms that require system variables.

Use `add_env` to start with the current process environment and add or override selected variables:

```python
run(
    'python -c "import os; print(os.environ.get(\'MY_VARIABLE\'))"',
    add_env={'MY_VARIABLE': 'hello'},
)
#> hello
```

If both `env` and `add_env` are provided, `add_env` is applied after `env`:

```python
run(
    'python -c "import os; print(os.environ.get(\'MY_VARIABLE\'))"',
    env={'MY_VARIABLE': 'from env'},
    add_env={'MY_VARIABLE': 'from add_env'},
)
#> from add_env
```

Use `delete_env` to remove variables from the environment passed to the subprocess:

```python
# If MY_VARIABLE exists in the current process environment, it will not be
# passed to this subprocess.
run(
    'python -c "import os; print(os.environ.get(\'MY_VARIABLE\'))"',
    delete_env=['MY_VARIABLE'],
)
#> None
```

`delete_env` is applied last. A variable cannot be explicitly set through `env` or `add_env` and deleted through `delete_env` in the same call:

```python
from suby import EnvironmentVariablesConflict

try:
    run(
        'python -c pass',
        env={'MY_VARIABLE': 'hello'},
        delete_env=['MY_VARIABLE'],
    )
except EnvironmentVariablesConflict as error:
    print(error)
    #> Environment variables cannot be both set via env/add_env and deleted via delete_env: MY_VARIABLE.
```

On `Windows`, environment variable names are handled case-insensitively. On other platforms, names are case-sensitive.


## Changing directories

By default, a subprocess starts in the same current working directory as the current process. Pass `directory` when the command should run somewhere else:

```python
from pathlib import Path

run(
    'python -c "import os; print(os.getcwd())"',
    directory=Path('project'),
)
```

The directory must already exist. Passing `directory` changes only the subprocess working directory; it does not change the current process directory:

```python
from pathlib import Path

before = Path.cwd()
run('python -c pass', directory='project')
assert Path.cwd() == before
```

The `directory` argument accepts a string or a `Path` object. Relative paths are resolved from the parent process's current working directory at the moment `run` is called, so values like `.`, `..`, and `./../.././project/` are valid when they point to an existing directory:

```python
run('python -c "import os; print(os.getcwd())"', directory='./project/')
```

`directory` is separate from command parsing. It is not split with `shlex`, and it is not affected by `split` or `double_backslash`. A directory path containing spaces is passed as one directory path:

```python
run('python -c pass', directory='project with spaces')
```

`directory` does not perform shell-style expansion. If you want a path under your home directory, expand it yourself:

```python
from pathlib import Path

run('python -c pass', directory=Path.home())
```

Invalid directories raise `WrongDirectoryError` before the subprocess is started:

```python
from suby import WrongDirectoryError

try:
    run('python -c pass', directory='missing-directory')
except WrongDirectoryError as error:
    print(error)
    #> The directory 'missing-directory' does not exist.
```

`catch_exceptions=True` does not hide invalid directory arguments, because those errors are raised before subprocess execution begins:

```python
from suby import WrongDirectoryError

try:
    run('python -c pass', directory='missing-directory', catch_exceptions=True)
except WrongDirectoryError as error:
    print(error)
    #> The directory 'missing-directory' does not exist.
```
