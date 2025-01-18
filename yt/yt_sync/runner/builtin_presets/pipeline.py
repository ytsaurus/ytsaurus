PIPELINE_SORTED_TABLE_PRESET = {
    "$merge_presets": ["builtin:table_preset"],
    # Base attributes for all clusters.
    "clusters": {
        "_all_data_clusters": {
            "attributes": {
                "dynamic": True,
                "optimize_for": "scan",
                "chunk_format": "table_versioned_columnar",
                "erasure_codec": "isa_lrc_12_2_2",
                "hunk_erasure_codec": "isa_lrc_12_2_2",
                "mount_config": {
                    "min_data_ttl": 0,
                    "merge_rows_on_flush": True,
                    "merge_deletions_on_flush": True,
                },
                "hunk_chunk_reader": {"fragment_read_hedging_delay": 50},
            },
        },
    },
}

PIPELINE_ORDERED_TABLE_PRESET = {
    "$merge_presets": ["builtin:table_preset"],
    # Base attributes for all clusters.
    "clusters": {},
}

PIPELINE_FILE_PRESET = {
    "$merge_presets": ["builtin:storage_preset"],
    "clusters": {"_all_clusters": {"attributes": {"compression_codec": "lz4"}}},
}

PIPELINE_TABLES_PRESET = {
    "timer_messages": {
        "$merge_presets": ["builtin:pipeline_sorted_table_preset"],
        "clusters": {
            "_all_data_clusters": {
                "attributes": {
                    "in_memory_mode": "none",
                    "mount_config": {
                        "enable_lookup_hash_table": False,
                    },
                },
            },
        },
    },
    "input_messages": {
        "$merge_presets": ["builtin:pipeline_sorted_table_preset"],
        "clusters": {
            "_all_data_clusters": {
                "attributes": {
                    "in_memory_mode": "none",
                    "compression_codec": "zstd_6",
                    "chunk_writer": {
                        "key_filter": {"bits_per_key": 24},
                    },
                    "mount_config": {
                        "lookup_cache_rows_ratio": 0.03,
                        "enable_key_filter_for_lookup": True,
                        "enable_lookup_hash_table": False,
                        "min_data_versions": 0,
                        "min_data_ttl": 0,
                        "row_merger_type": "watermark",
                    },
                },
            },
        },
    },
    "output_messages": {
        "$merge_presets": ["builtin:pipeline_sorted_table_preset"],
        "clusters": {
            "_all_data_clusters": {
                "attributes": {
                    "in_memory_mode": "none",
                    "mount_config": {
                        "enable_lookup_hash_table": False,
                    },
                },
            },
        },
    },
    "partition_data": {
        "$merge_presets": ["builtin:pipeline_sorted_table_preset"],
        "clusters": {
            "_all_data_clusters": {
                "attributes": {
                    "in_memory_mode": "uncompressed",
                    "mount_config": {
                        "enable_lookup_hash_table": True,
                    },
                },
            },
        },
    },
}

PIPELINE_QUEUES_PRESET = {
    "controller_logs": {
        "$merge_presets": ["builtin:pipeline_ordered_table_preset"],
        "clusters": {
            "_all_data_clusters": {
                "attributes": {
                    "tablet_count": 1,
                    "mount_config": {
                        "min_data_versions": 0,
                        "min_data_ttl": 0,
                        "max_data_ttl": 86400000,
                    },
                },
            },
        },
    },
}

PIPELINE_FILES_PRESET = {
    "flow_view": {"$merge_presets": ["builtin:pipeline_file_preset"]},
    "spec": {"$merge_presets": ["builtin:pipeline_file_preset"]},
    "dynamic_spec": {"$merge_presets": ["builtin:pipeline_file_preset"]},
    "important_versions": {"$merge_presets": ["builtin:pipeline_file_preset"]},
}
