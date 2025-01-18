from yt.yt_sync.core.constants import QUEUE_META_COLUMNS

QUEUES = {
    "main_queue": {
        "default": {
            "$merge_presets": ["builtin:table_preset"],
            "schema": [{"name": "data", "type": "string"}, *QUEUE_META_COLUMNS],
            "clusters": {"_all_clusters": {"attributes": {"tablet_count": 10}}},
        },
    },
}

CONSUMERS = {
    "consumer": {
        "default": {
            "$merge_presets": ["builtin:consumer_preset"],
            "in_stage_queues": {"main_queue": {"vital": True}},
        },
    },
}
