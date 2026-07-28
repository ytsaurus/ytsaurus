from yt.yt_sync.core.constants import QUEUE_META_COLUMNS

QUEUES = {
    "action_queue": {
        "default": {
            "$merge_presets": ["ordered_table_base_preset"],
            "schema": [
                {"name": "hit_id", "type": "string"},
                {"name": "hit_time", "type": "uint64"},
                {"name": "action_time", "type": "uint64"},
                {"name": "is_click", "type": "boolean"},
                *QUEUE_META_COLUMNS,
            ],
        },
    },
    "hit_queue": {
        "default": {
            "$merge_presets": ["ordered_table_base_preset"],
            "schema": [
                {"name": "hit_id", "type": "string"},
                {"name": "hit_time", "type": "uint64"},
                {"name": "hit_payload", "type": "string"},
                *QUEUE_META_COLUMNS,
            ],
        },
    },
    "output_queue": {
        "default": {
            "$merge_presets": ["builtin:table_preset"],
            "schema": [
                {"name": "hit_id", "type": "string"},
                {"name": "hit_time", "type": "uint64"},
                {"name": "is_click", "type": "boolean"},
                {"name": "show_time", "type": "uint64"},
                {"name": "click_time", "type": "uint64"},
                {"name": "hit_payload", "type": "string"},
                *QUEUE_META_COLUMNS,
            ],
        },
    },
}

CONSUMERS = {
    "consumer": {
        "default": {
            "$merge_presets": ["builtin:consumer_preset"],
            "in_stage_queues": {
                # queue_name: registration_attributes
                "action_queue": {"vital": True},
                "hit_queue": {"vital": True},
            },
        },
        # Or ordinary registrations for some stands.
        # Stand.STABLE: {
        #     "in_stage_queues": {"input": None},
        #     "queues": [{"cluster": ..., "path": ..., "vital": True}]
        # }
    }
}

PRODUCERS = {
    "producer": {"default": {"$merge_presets": ["builtin:producer_preset"]}},
}
