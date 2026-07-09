"""Flow pipeline preset definitions.

Cypress *attributes* of pipeline inner tables.

The shape of every dict follows yt_sync's spec language:

* ``$merge_presets``: list of preset names to merge.
* ``clusters``: per-cluster overlays.
"""

PIPELINE_SORTED_TABLE_PRESET = {
    "$merge_presets": ["builtin:table_preset"],
    # Base attributes for all clusters.
    "clusters": {
        "_all_data_clusters": {
            "attributes": {
                "dynamic": True,
                "optimize_for": "scan",
                "chunk_format": "table_versioned_columnar",
                "erasure_codec": "reed_solomon_3_3",
                "hunk_erasure_codec": "reed_solomon_3_3",
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
    "timers": {
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
                        "key_filter": {
                            "bits_per_key": 24,
                            "enable": True,
                        },
                    },
                    "mount_config": {
                        "auto_compaction_period": 3600000,
                        "lookup_cache_rows_ratio": 0.03,
                        "enable_key_filter_for_lookup": True,
                        "min_data_versions": 0,
                        "min_data_ttl": 0,
                        "row_merger_type": "watermark",
                    },
                },
            },
        },
    },
    "compact_input_messages": {
        "$merge_presets": ["builtin:pipeline_sorted_table_preset"],
        "clusters": {
            "_all_data_clusters": {
                "attributes": {
                    "in_memory_mode": "none",
                    "erasure_codec": "none",
                    "compression_codec": "none",
                    "chunk_writer": {
                        "key_filter": {
                            "bits_per_key": 24,
                            "enable": True,
                        },
                    },
                    "mount_config": {
                        "auto_compaction_period": 3600000,
                        "lookup_cache_rows_ratio": 0.03,
                        "enable_key_filter_for_lookup": True,
                        "min_data_versions": 0,
                        "min_data_ttl": 0,
                        "row_merger_type": "watermark",
                    },
                },
            },
        },
    },
    "compact_output_messages": {
        "$merge_presets": ["builtin:pipeline_sorted_table_preset"],
        "clusters": {
            "_all_data_clusters": {
                "attributes": {
                    "erasure_codec": "none",
                    "compression_codec": "none",
                },
            },
        },
    },
    "compact_partition_output_messages": {
        "$merge_presets": ["builtin:pipeline_sorted_table_preset"],
        "clusters": {
            "_all_data_clusters": {
                "attributes": {
                    "erasure_codec": "none",
                    "compression_codec": "none",
                },
            },
        },
    },
    "states": {
        "$merge_presets": ["builtin:pipeline_sorted_table_preset"],
        "clusters": {
            "_all_data_clusters": {
                "attributes": {},
            },
        },
    },
    "partition_states": {
        "$merge_presets": ["builtin:pipeline_sorted_table_preset"],
        "clusters": {
            "_all_data_clusters": {
                "attributes": {},
            },
        },
    },
    "key_visitor_states": {
        "$merge_presets": ["builtin:pipeline_sorted_table_preset"],
        "clusters": {
            "_all_data_clusters": {
                "attributes": {},
            },
        },
    },
    "flow_state": {
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
    "flow_state_obsolete": {
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
    "flow_control": {
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
    "partition_transactions": {
        "$merge_presets": ["builtin:pipeline_sorted_table_preset"],
        "clusters": {
            "_all_data_clusters": {
                "attributes": {
                    "in_memory_mode": "uncompressed",
                    "mount_config": {
                        "enable_lookup_hash_table": True,
                        # Tablets must be really small, so partitions are limited.
                        "min_partition_data_size": 100,
                        "desired_partition_data_size": 500,
                        "max_partition_data_size": 5000,
                        # Always take all stores in compaction.
                        "min_compaction_store_count": 2,
                        "min_partitioning_data_size": 1,
                    },
                    # Tablets must be really small to avoid overloading
                    # by high rps overwriting of lightweight states.
                    "chunk_writer": {
                        "block_size": 300,
                        "max_block_size": 500,
                        "desired_chunk_size": 300,
                    },
                    "tablet_balancer_config": {
                        "min_tablet_size": 300,
                        "desired_tablet_size": 1000,
                        "max_tablet_size": 3000,
                    },
                    # Erasure makes compaction slower.
                    "erasure_codec": "none",
                    # Compressing only increases size of this data.
                    "compression_codec": "none",
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

PIPELINE_FILES_PRESET = {}  # type: ignore[var-annotated]
