import subprocess
import sys
from pathlib import Path

import instld
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.slow
def test_editable_install_is_visible_to_mypy_from_outside_checkout(tmp_path):
    """mypy must resolve suby after an editable install, without seeing the source checkout as cwd."""
    consumer = tmp_path / 'consumer'
    consumer.mkdir()
    probe = consumer / 'probe.py'
    probe.write_text('from suby import run\nreveal_type(run)\n')

    runner = consumer / 'run_mypy.py'
    runner.write_text(
        'import site\n'
        'import sys\n'
        '\n'
        'site.addsitedir(sys.argv[1])\n'
        'from mypy import api\n'
        '\n'
        'stdout, stderr, status = api.run([sys.argv[2]])\n'
        'sys.stdout.write(stdout)\n'
        'sys.stderr.write(stderr)\n'
        'raise SystemExit(status)\n',
    )

    with instld('mypy==1.14.1', editable=str(PROJECT_ROOT), catch_output=True, logger=None) as context:
        result = subprocess.run(
            [sys.executable, '-S', str(runner), context.where, str(probe)],
            check=False,
            cwd=consumer,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

    assert result.returncode == 0, result.stdout
    assert 'import-not-found' not in result.stdout
