TABLES = {
    "kv_state": {
        "default": {
            "$merge_presets": ["sorted_table_base_preset"],
            "schema": [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ],
            "clusters": {
                "_all_data_clusters": {
                    "attributes": {
                        "optimize_for": "scan",
                        "chunk_format": "table_versioned_columnar",
                        "in_memory_mode": "uncompressed",
                        "enable_dynamic_store_read": True,
                    },
                },
            },
        },
    },
}
