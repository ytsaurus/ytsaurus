STAGES = {
    "default": {
        "presets": {
            "builtin:storage_preset": {
                "clusters": {"_all_clusters": {"attributes": {"primary_medium": "ssd_blobs"}}},
            },
            "builtin:table_preset": {
                "clusters": {"_all_clusters": {"attributes": {"tablet_cell_bundle": "your_bundle"}}},
            },
            "sorted_table_base_preset": {
                "$merge_presets": ["builtin:table_preset"],
            },
        }
    },
    "production": {
        "folder": "//home/your_project/testing",
        "presets": {
            "builtin:storage_preset": {
                "clusters": {"primary": {"main": True}},
            },
            "sorted_table_base_preset": {
                "clusters": {"replica_1": {}, "replica_2": {}},  # Two replicas for our big sorted tables.
            },
        },
    },
    "testing": {
        "folder": "//home/your_project/testing",
        "presets": {
            "builtin:storage_preset": {
                "clusters": {"primary": {"main": True}},
            },
        },
    },
}
