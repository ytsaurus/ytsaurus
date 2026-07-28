import os

from yt.wrapper import yson

from .common import Stage

TEST_YT_CLUSTER = os.environ.get("TEST_YT_CLUSTER", "primary")
TEST_YT_REMOTE_CLUSTER = yson.loads(os.environ.get("TEST_YT_REMOTE_CLUSTERS", '["remote_0"]').encode())[0]
TEST_YT_PATH = os.environ.get("TEST_YT_PATH", "//tmp/test")
TEST_YT_TABLET_CELL_BUNDLE = os.environ.get("TEST_YT_TABLET_CELL_BUNDLE", "default")
TEST_YT_PRIMARY_MEDIUM = os.environ.get("TEST_YT_PRIMARY_MEDIUM", "default")
TEST_INPUT_QUEUE_TABLET_COUNT = int(os.environ.get("TEST_INPUT_QUEUE_TABLET_COUNT", 1))


STAGES = {
    "default": {
        "presets": {
            "builtin:storage_preset": {"clusters": {"_all_clusters": {"attributes": {"primary_medium": "ssd_blobs"}}}},
            "builtin:table_preset": {
                "clusters": {
                    "_all_clusters": {
                        "attributes": {
                            "dynamic": True,
                            "enable_dynamic_store_read": True,
                            "tablet_cell_bundle": "waitclickjoin",
                        }
                    }
                }
            },
            "sorted_table_base_preset": {
                "$merge_presets": ["builtin:table_preset"],
                "clusters": {
                    "_all_data_clusters": {
                        "attributes": {
                            "optimize_for": "scan",
                            "chunk_format": "table_versioned_columnar",
                            "in_memory_mode": "uncompressed",
                        },
                    },
                },
            },
            "ordered_table_base_preset": {
                "$merge_presets": ["builtin:table_preset"],
            },
            "pipeline_prod_like_sorted_table_preset": {
                "clusters": {
                    "_all_data_clusters": {
                        "attributes": {
                            "tablet_balancer_config": {
                                "min_tablet_count": 100,
                                "desired_tablet_count": 200,
                                "group": "write",
                            },
                        },
                    },
                },
            },
        },
    },
    # Stage is provided as example, there is no real production of wait_click_join :)
    Stage.STABLE: {
        "folder": "//home/your_account/wait_click_join/stable",
        "presets": {
            "builtin:storage_preset": {
                "clusters": {"seneca-klg": {"main": True}},
            },
            "builtin:pipeline_sorted_table_preset": {"$merge_presets": ["pipeline_prod_like_sorted_table_preset"]},
        },
    },
    # Stage is provided as example.
    Stage.PRESTABLE: {
        "folder": "//home/your_account/wait_click_join/prestable",
        "presets": {
            "builtin:storage_preset": {
                "clusters": {"zzeeennoo": {"main": True}},
            },
            "builtin:pipeline_sorted_table_preset": {"$merge_presets": ["pipeline_prod_like_sorted_table_preset"]},
        },
    },
    # Stage is provided as example.
    Stage.DEV: {
        "folder": "//home/your_account/wait_click_join/dev",
        "presets": {
            "builtin:storage_preset": {
                "clusters": {"zzeennoo": {"main": True}},
            },
            "builtin:pipeline_sorted_table_preset": {"$merge_presets": ["pipeline_prod_like_sorted_table_preset"]},
        },
    },
    # Actual stage that is used in tests of example.
    Stage.TEST: {
        "folder": TEST_YT_PATH,
        "presets": {
            "builtin:storage_preset": {
                "clusters": {TEST_YT_CLUSTER: {"main": True, "attributes": {"primary_medium": TEST_YT_PRIMARY_MEDIUM}}},
            },
            # Make all tables replicated.
            "builtin:table_preset": {
                "clusters": {
                    TEST_YT_CLUSTER: {
                        "attributes": {
                            "tablet_cell_bundle": TEST_YT_TABLET_CELL_BUNDLE,
                            "replicated_table_options": {"min_sync_replica_count": 1},
                        },
                    },
                    TEST_YT_REMOTE_CLUSTER: {
                        "replicated_table_tracker_enabled": True,
                        "preferred_sync": True,
                        "attributes": {
                            "tablet_cell_bundle": TEST_YT_TABLET_CELL_BUNDLE,
                            "primary_medium": TEST_YT_PRIMARY_MEDIUM,
                        },
                    },
                },
            },
            "ordered_table_base_preset": {
                "clusters": {
                    "_all_clusters": {
                        "attributes": {"tablet_count": TEST_INPUT_QUEUE_TABLET_COUNT},
                    },
                },
            },
        },
    },
}
