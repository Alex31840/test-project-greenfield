"""
Root-level pytest conftest.

This project's dependencies (aws-cdk-lib, constructs, boto3, ...) are
declared in requirements.txt and installed into a project-local
virtualenv (.venv) per the standard "default interpreter is read-only"
sandbox convention. If pytest is invoked with an interpreter that does
NOT already have these packages importable (e.g. a bare system/shared
interpreter that never ran `pip install -r requirements.txt`), fall
back to adding the project's own .venv site-packages directory onto
sys.path before test collection, so the test suite still runs without
requiring the caller to remember to activate the virtualenv first.

This is a no-op whenever the invoking interpreter already satisfies
the dependencies (e.g. the project's own .venv, or any other
environment that already has them installed).
"""
from __future__ import annotations

import glob
import os
import site
import sys

_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))


def _ensure_project_dependencies_importable() -> None:
    try:
        import aws_cdk  # noqa: F401
        return
    except ImportError:
        pass

    candidates = sorted(
        glob.glob(os.path.join(_PROJECT_ROOT, ".venv", "lib", "python3.*", "site-packages"))
    )
    for site_packages in candidates:
        if os.path.isdir(site_packages) and site_packages not in sys.path:
            site.addsitedir(site_packages)

    try:
        import aws_cdk  # noqa: F401
    except ImportError:
        # Leave the ImportError to surface naturally from the test
        # module itself -- this is a best-effort fallback, not a
        # substitute for a real environment with the dependencies
        # installed.
        pass


_ensure_project_dependencies_importable()
