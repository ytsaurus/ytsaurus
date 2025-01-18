from yt.yt_sync.core.constants import CONSUMER_ATTRS, CONSUMER_SCHEMA, PRODUCER_ATTRS, PRODUCER_SCHEMA

from .pipeline import PIPELINE_SORTED_TABLE_PRESET
from .pipeline import PIPELINE_ORDERED_TABLE_PRESET
from .pipeline import PIPELINE_FILE_PRESET
from .pipeline import PIPELINE_TABLES_PRESET
from .pipeline import PIPELINE_QUEUES_PRESET
from .pipeline import PIPELINE_FILES_PRESET

# Marker for documentation: [BEGIN builtin_presets]
BUILTIN_PRESETS = {
    "builtin:storage_preset": {},
    "builtin:table_preset": {
        "$merge_presets": ["builtin:storage_preset"],
        "clusters": {"_all_clusters": {"attributes": {"dynamic": True}}},
    },
    "builtin:consumer_preset": {
        "table": {
            "$merge_presets": ["builtin:table_preset"],
            "schema": CONSUMER_SCHEMA,
            "clusters": {"_all_clusters": {"attributes": CONSUMER_ATTRS}},
        },
    },
    "builtin:producer_preset": {
        "table": {
            "$merge_presets": ["builtin:table_preset"],
            "schema": PRODUCER_SCHEMA,
            "clusters": {"_all_clusters": {"attributes": PRODUCER_ATTRS}},
        },
    },
    "builtin:pipeline_sorted_table_preset": PIPELINE_SORTED_TABLE_PRESET,
    "builtin:pipeline_ordered_table_preset": PIPELINE_ORDERED_TABLE_PRESET,
    "builtin:pipeline_file_preset": PIPELINE_FILE_PRESET,
    "builtin:pipeline_preset": {
        "table_spec": PIPELINE_TABLES_PRESET,
        "queue_spec": PIPELINE_QUEUES_PRESET,
        "file_spec": PIPELINE_FILES_PRESET,
    },
}
# Marker for documentation: [END builtin_presets]
