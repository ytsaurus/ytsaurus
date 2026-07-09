"""
Public API:

* :func:`create_pipeline` / :func:`create_table` / :func:`register_consumer`
  — create the YT objects a Flow pipeline needs (consumers/producers are
  tables with the schemas/presets exported below).
* :class:`StagesSpec` / :func:`run_yt_sync_easy_mode` — mini replacement for
  yt_sync's easy mode (the subset used by Flow tests and examples) that
  proxies to the creation functions above.
"""

from .easy_mode import (
    StagesSpec,
    run_yt_sync_easy_mode,
)
from .yt_sync_mini import (
    CONSUMER_ATTRS,
    CONSUMER_SCHEMA,
    CURRENT_PIPELINE_FORMAT_VERSION,
    PIPELINE_FORMAT_VERSION_ATTRIBUTE,
    PRODUCER_ATTRS,
    PRODUCER_SCHEMA,
    QUEUE_META_COLUMNS,
    create_pipeline,
    create_table,
    register_consumer,
)

__all__ = [
    "CONSUMER_ATTRS",
    "CONSUMER_SCHEMA",
    "CURRENT_PIPELINE_FORMAT_VERSION",
    "PIPELINE_FORMAT_VERSION_ATTRIBUTE",
    "PRODUCER_ATTRS",
    "PRODUCER_SCHEMA",
    "QUEUE_META_COLUMNS",
    "StagesSpec",
    "create_pipeline",
    "create_table",
    "register_consumer",
    "run_yt_sync_easy_mode",
]
