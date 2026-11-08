"""Shared test setup: point the app at the worker sources before any app module reads its configuration."""

import os
from pathlib import Path

HERE = Path(__file__).resolve().parent
for candidate in (HERE.parent / "workers", HERE.parent.parent / "workers"):  # container layout, repository layout
    if candidate.is_dir():
        os.environ.setdefault("WORKERS_DIR", str(candidate))
        break
os.environ.setdefault("DATA_DIR", str(HERE / ".data"))
