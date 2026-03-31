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

</details>


![logo](https://raw.githubusercontent.com/mutating/suby/develop/docs/assets/logo_5.svg)


Suby is a small wrapper around the [subprocess](https://docs.python.org/3/library/subprocess.html) module. You can find many similar wrappers, but this particular one differs from the others in the following ways:

- Beautiful minimalistic call syntax.
- Ability to specify your callbacks to catch `stdout` and `stderr`.
- Support for [cancellation tokens](https://github.com/pomponchik/cantok).
- Ability to set timeouts for subprocesses.
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


## Quick start

Install it:

```bash
pip install suby
```

And use it:

```python
from suby import run

run('python -c "print(\'hello, world!\')"')
# > hello, world!
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
# > SubprocessResult(id='e9f2d29acb4011ee8957320319d7541c', stdout='hello, world!\n', stderr='', returncode=0, killed_by_token=False)
```

It returns an object of the `SubprocessResult` class, which contains the following fields:

- **id**: a unique string that allows you to distinguish one result of calling the same command from another.
- **stdout**: a string containing the entire output of the command being run.
- **stderr**: a string containing the entire stderr output of the command being run.
- **returncode**: an integer indicating the return code of the subprocess. `0` means that the process was completed successfully; other values usually indicate an error.
- **killed_by_token**: a boolean flag indicating whether the subprocess was killed due to [token](https://cantok.readthedocs.io/en/latest/the_pattern/) cancellation.


## Command parsing

Each command you use to call `suby` is passed to a special [system call](https://en.wikipedia.org/wiki/System_call), which depends on the operating system. But regardless of the specific operating system, this system call accepts not a single line of input, but a list of substrings. This means that under the hood, `suby` splits the string you pass using [shlex](https://docs.python.org/3/library/shlex.html) on all platforms.

For example, the following line:

```bash
python -c "print('hello, world!')"
```

... should be written like this:

```python
run('python -c "print(\'hello, world!\')"')
```

You can pass not only strings to `run`, but also [`pathlib.Path`](https://docs.python.org/3/library/pathlib.html#pathlib.Path) objects:

```python
import sys
from pathlib import Path

run(Path(sys.executable), '-c print(777)')
# This will work too.
```

To disable automatic string splitting, pass `split=False`:

```python
run('python', '-c', 'print(777)', split=False)
```

In this case, you will have to split the command yourself.


## Output

By default, the `stdout` and `stderr` of the subprocess are forwarded to the `stdout` and `stderr` of the current process. Reading from the subprocess is continuous, and output is flushed each time a full line is read. For continuous reading from `stderr`, a separate thread is created so that `stdout` and `stderr` are read independently.

You can override the output functions for `stdout` and `stderr`. To do this, you need to pass functions accepting a string as an argument via the `stdout_callback` and `stderr_callback` parameters, respectively. For example, you can color the output (the code example uses the [`termcolor`](https://github.com/termcolor/termcolor) library):

```python
from termcolor import colored

def my_new_stdout(string: str) -> None:
    print(colored(string, 'red'), end='')

run('python -c "print(\'hello, world!\')"', stdout_callback=my_new_stdout)
# > hello, world!
# You can't see it here, but if you run this code yourself, the output in the console will be red!
```

You can also completely disable the output by passing `True` as the `catch_output` parameter:

```python
run('python -c "print(\'hello, world!\')"', catch_output=True)
# There's nothing here.
```

If you specify `catch_output=True`, even if you have also defined custom callback functions, they will not be called. In addition, `suby` always returns [the result](#run-subprocess-and-look-at-the-result) of executing the command, containing the full output. The `catch_output` argument can suppress only the output, but it does not prevent the buffering of output.


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
# > 2024-02-22 02:15:08,155 [INFO] The beginning of the execution of the command "python -c pass".
# > 2024-02-22 02:15:08,190 [INFO] The command "python -c pass" has been successfully executed.
```

The message about the start of the command execution is always logged at the `INFO` [level](https://docs.python.org/3/library/logging.html#logging-levels). If the command is completed successfully, the completion message will also be at the `INFO` level. If the command fails, it will be at the `ERROR` level:

```python
run('python -c "raise ValueError"', logger=logging.getLogger('logger_name'), catch_exceptions=True, catch_output=True)
# > 2024-02-22 02:20:25,549 [INFO] The beginning of the execution of the command "python -c "raise ValueError"".
# > 2024-02-22 02:20:25,590 [ERROR] Error when executing the command "python -c "raise ValueError"".
```

If you don't need these details, simply omit the logger argument.

If you still prefer logging, you can use any object that implements the [logger protocol](https://github.com/pomponchik/emptylog?tab=readme-ov-file#universal-logger-protocol) from the [`emptylog`](https://github.com/pomponchik/emptylog) library, including ones from third-party libraries.


## Exceptions

By default, `suby` raises exceptions in three cases:

1. If the command exits with a return code not equal to `0`. In this case, a `RunningCommandError` exception will be raised:

```python
from suby import run, RunningCommandError

try:
    run('python -c 1/0')
except RunningCommandError as e:
    print(e)
    # > Error when executing the command "python -c 1/0".
```

2. If you pass a [cancellation token](https://cantok.readthedocs.io/en/latest/the_pattern/) when calling the command, and the token is canceled, an exception will be raised [corresponding to the type](https://cantok.readthedocs.io/en/latest/what_are_tokens/exceptions/) of the canceled token. [This feature](#working-with-cancellation-tokens) is integrated with the [cantok](https://cantok.readthedocs.io/en/latest/) library, so we recommend that you familiarize yourself with it first.

3. If a [timeout](#timeouts) you set for the operation expires.

You can prevent `suby` from raising these exceptions. To do this, set the `catch_exceptions` parameter to `True`:

```python
result = run('python -c "import time; time.sleep(10_000)"', timeout=1, catch_exceptions=True)
print(result)
# > SubprocessResult(id='c9125b90d03111ee9660320319d7541c', stdout='', stderr='', returncode=-9, killed_by_token=True)
```

Keep in mind that the full result of the subprocess call can also be found through the `result` attribute of any exception raised by `suby`:

```python
from suby import run, TimeoutCancellationError

try:
    run('python -c "import time; time.sleep(10_000)"', timeout=1)
except TimeoutCancellationError as e:
    print(e.result)
    # > SubprocessResult(id='a80dc26cd03211eea347320319d7541c', stdout='', stderr='', returncode=-9, killed_by_token=True)
```


## Working with Cancellation Tokens

`suby` is fully compatible with the [cancellation token pattern](https://cantok.readthedocs.io/en/latest/the_pattern/) and supports any token objects from the [`cantok`](https://github.com/pomponchik/cantok) library.

The essence of the pattern is that you can pass an object to `suby` that signals whether the operation should continue. If not, `suby` kills the subprocess. This pattern is especially useful for long-running or unpredictably slow commands. When the result becomes unnecessary, there is no point in sitting and waiting for the command to complete.

In practice, you can pass your cancellation tokens to `suby`. By default, canceling a token causes an exception to be raised:

```python
from random import randint
from cantok import ConditionToken

token = ConditionToken(lambda: randint(1, 1000) == 7)  # This token will be canceled when a random unlikely event occurs.
run('python -c "import time; time.sleep(10_000)"', token=token)
# > cantok.errors.ConditionCancellationError: The cancellation condition was satisfied.
```

However, if you pass the `catch_exceptions=True` argument, the exception will not be raised (see [Exceptions](#exceptions)). Instead, you will get the [usual result](#run-subprocess-and-look-at-the-result) of calling `run` with the `killed_by_token=True` flag:

```python
token = ConditionToken(lambda: randint(1, 1000) == 7)
print(run('python -c "import time; time.sleep(10_000)"', token=token, catch_exceptions=True))
# > SubprocessResult(id='e92ccd54d24b11ee8376320319d7541c', stdout='', stderr='', returncode=-9, killed_by_token=True)
```

Under the hood, a separate thread is created to track the status of the token. When the token is canceled, the thread kills the subprocess.

## Timeouts

You can set a timeout for `suby`. It must be a number greater than zero, which specifies the maximum number of seconds the subprocess is allowed to run. If the timeout expires before the subprocess completes, an exception will be raised:

```python
run('python -c "import time; time.sleep(10_000)"', timeout=1)
# > cantok.errors.TimeoutCancellationError: The timeout of 1 seconds has expired.
```

Under the hood, `run` uses [`TimeoutToken`](https://cantok.readthedocs.io/en/latest/types_of_tokens/TimeoutToken/) from the [`cantok`](https://github.com/pomponchik/cantok) library to track the timeout.

`suby` re-exports this exception:

```python
from suby import run, TimeoutCancellationError

try:
    run('python -c "import time; time.sleep(10_000)"', timeout=1)
except TimeoutCancellationError as e:  # As you can see, TimeoutCancellationError is available in the suby module.
    print(e)
    # > The timeout of 1 seconds has expired.
```

Just as with [regular cancellation tokens](#working-with-cancellation-tokens), you can prevent exceptions from being raised using the `catch_exceptions=True` argument:

```python
print(run('python -c "import time; time.sleep(10_000)"', timeout=1, catch_exceptions=True))
# > SubprocessResult(id='ea88c518d25011eeb25e320319d7541c', stdout='', stderr='', returncode=-9, killed_by_token=True)
```
