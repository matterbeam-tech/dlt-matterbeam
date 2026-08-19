import os
import sys

import pytest

# BRIEF §4.2 bars this import from the *package* -- dlt_matterbeam never does this. It does
# not bar it from a test, and this is exactly the "harness that lives where the import is
# allowed" the design doc's §8.4 calls for (Phase 1 assertion 3). `../../../../backend` is
# read-only per BRIEF §0.
BACKEND_SHARED_SRC = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..", "backend", "domain", "shared", "src")
)
# `crf.py` has no dlt dependency, so this tier imports it straight from source rather than
# installing the whole package (which would pull in dlt, unnecessary for a bytes-level check).
OUR_SRC = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
if OUR_SRC not in sys.path:
    sys.path.insert(0, OUR_SRC)


def _matterbeam_shared_importable() -> bool:
    if not os.path.isdir(BACKEND_SHARED_SRC):
        return False
    if BACKEND_SHARED_SRC not in sys.path:
        sys.path.insert(0, BACKEND_SHARED_SRC)
    try:
        import matterbeam_shared.coldlog_writer.coldlog_writer  # noqa: F401
    except ImportError:
        return False
    return True


collect_ignore_glob: list[str] = []
if not _matterbeam_shared_importable():
    collect_ignore_glob.append("test_*.py")
