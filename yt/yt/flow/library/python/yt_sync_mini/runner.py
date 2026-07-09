"""Opensource stand-in for the ``yt.yt_sync.runner`` module.

Compiled under that module name via ``PY_SRCS(NAMESPACE ...)`` only when
real yt_sync is absent from the build (see ya.make); flow tests and
examples keep their ``from yt.yt_sync.runner import ...`` lines unchanged.
"""

from yt.yt.flow.library.python.yt_sync_mini import StagesSpec  # noqa: F401
from yt.yt.flow.library.python.yt_sync_mini import run_yt_sync_easy_mode  # noqa: F401
