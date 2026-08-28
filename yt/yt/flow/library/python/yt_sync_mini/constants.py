"""Opensource stand-in for the ``yt.yt_sync.core.constants`` module.

Compiled under that module name via ``PY_SRCS(NAMESPACE ...)`` when the build carries the
OPENSOURCE flag (see ya.make). Such a build may still contain real yt_sync (autocheck compiles
the whole tree with the flag), and then this module shadows the real one — so it must mirror the
real module's public surface, not just the subset the flow tests use.
"""

from yt.yt.flow.library.python.pipeline_tables.schemas import (  # noqa: F401
    PIPELINE_FILES,
    PIPELINE_QUEUES,
    PIPELINE_TABLES,
)

from yt.yt.flow.library.python.yt_sync_mini import (  # noqa: F401
    CONSUMER_ATTRS,
    CONSUMER_SCHEMA,
    PRODUCER_ATTRS,
    PRODUCER_SCHEMA,
    QUEUE_META_COLUMNS,
)

KB: int = 1024
MB: int = KB * KB
GB: int = MB * KB
