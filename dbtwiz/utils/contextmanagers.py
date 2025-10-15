import io
import sys
from contextlib import contextmanager


@contextmanager
def suppress_output():
    """Temporarily suppress stdout and stderr."""
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    sys.stdout = io.StringIO()
    sys.stderr = io.StringIO()
    try:
        yield
    finally:
        sys.stdout = original_stdout
        sys.stderr = original_stderr
