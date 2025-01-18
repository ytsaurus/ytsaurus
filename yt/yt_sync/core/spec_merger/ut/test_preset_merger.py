import pytest
from yt.yt_sync.core.spec_merger.preset_merger import PresetsMerger
from yt.yt_sync.core.spec_merger.preset_merger import StageMerger


class TestPresetMerger:
    def test_trivial(self):
        d = {"a": "b", "c": [2], "d": {"e": 1}}
        assert PresetsMerger("$merge_presets", {}).apply_presets(d) == d

    def test_simple_chain(self):
        merger = PresetsMerger(
            "$merge_presets",
            {
                "base_table_preset": {
                    "clusters": {
                        "_cluster": {
                            "attrs": {
                                "tablet_cell_bundle": "test",
                            },
                        },
                    },
                },
                "sorted_table_preset": {
                    "$merge_presets": ["base_table_preset"],
                    "clusters": {
                        "_data_cluster": {
                            "attrs": {
                                "in_memory_mode": "uncompressed",
                            },
                        },
                    },
                },
            },
        )

        tables = {
            "my_table": {
                "$merge_presets": ["sorted_table_preset"],
                "schema": [],
                "clusters": {
                    "_cluster": {},
                },
            },
        }

        assert merger.apply_presets(tables) == {
            "my_table": {
                "schema": [],
                "clusters": {
                    "_cluster": {
                        "attrs": {
                            "tablet_cell_bundle": "test",
                        },
                    },
                    "_data_cluster": {
                        "attrs": {
                            "in_memory_mode": "uncompressed",
                        },
                    },
                },
            },
        }

    def test_simple_multiple(self):
        merger = PresetsMerger(
            "$merge_presets",
            {
                "base_table_preset": {
                    "clusters": {
                        "_cluster": {
                            "attrs": {
                                "tablet_cell_bundle": "test",
                            },
                        },
                    },
                },
                "sorted_table_preset": {
                    "clusters": {
                        "_data_cluster": {
                            "attrs": {
                                "in_memory_mode": "uncompressed",
                            },
                        },
                    },
                },
            },
        )

        tables = {
            "my_table": {
                "$merge_presets": ["base_table_preset", "sorted_table_preset"],
                "schema": [],
                "clusters": {
                    "_cluster": {},
                },
            },
        }

        assert merger.apply_presets(tables) == {
            "my_table": {
                "schema": [],
                "clusters": {
                    "_cluster": {
                        "attrs": {
                            "tablet_cell_bundle": "test",
                        },
                    },
                    "_data_cluster": {
                        "attrs": {
                            "in_memory_mode": "uncompressed",
                        },
                    },
                },
            },
        }

    def test_last_preset_wins(self):
        merger = PresetsMerger(
            "$merge_presets",
            {
                "a": {"value": "A"},
                "b": {"value": "B"},
            },
        )

        objects = {
            "AB": {
                "$merge_presets": ["a", "b"],
            },
            "BA": {
                "$merge_presets": ["b", "a"],
            },
        }

        assert merger.apply_presets(objects) == {
            "AB": {"value": "B"},
            "BA": {"value": "A"},
        }

    # Just to avoid accidentally changing behavior.
    # Though it is better to avoid such cases as long as possible.
    def test_inner_merge_wins(self):
        merger = PresetsMerger(
            "$merge_presets",
            {
                "a": {"x": {"y": {"value": "A"}}},
                "b": {"y": {"value": "B"}},
            },
        )

        objects = {
            "o": {},
            "oA": {
                "$merge_presets": ["a"],
            },
            "oB": {
                "x": {
                    "$merge_presets": ["b"],
                },
            },
            "oAB": {
                "$merge_presets": ["a"],
                "x": {
                    "$merge_presets": ["b"],
                },
            },
        }

        assert merger.apply_presets(objects) == {
            "o": {},
            "oA": {"x": {"y": {"value": "A"}}},
            "oB": {"x": {"y": {"value": "B"}}},
            "oAB": {"x": {"y": {"value": "B"}}},
        }


class TestStageMerger:
    def test_merge(self):
        merger = StageMerger(["$merge_presets"])
        # Marker for documentation: [BEGIN merge_stage_tests]
        assert merger.merge({"a": 3}, {"a": 5}) == {"a": 5}
        assert merger.merge([1], [2]) == [2]
        assert merger.merge(
            {"$merge_presets": ["a"]},
            {"$merge_presets": ["b"]},
        ) == {"$merge_presets": ["a", "b"]}
        # Marker for documentation: [END merge_stage_tests]

    def test_merge_two(self):
        merger = StageMerger(["$merge_presets", "$merge_clusters"])
        assert merger.merge(
            {"$merge_presets": ["a"], "cluster": {"$merge_clusters": ["X"]}},
            {"$merge_presets": ["b"], "cluster": {"$merge_clusters": ["Y"]}},
        ) == {"$merge_presets": ["a", "b"], "cluster": {"$merge_clusters": ["X", "Y"]}}

    def test_get_section(self):
        merger = StageMerger(["$merge_presets"])
        stages = {
            "default": {"$merge_presets": ["a"]},
            "stable": {"$merge_presets": ["b"]},
            "prestable": {"$merge_presets": ["c"]},
        }
        merger.get_section(stages, "stable") == {"$merge_presets": ["a", "b"]}
        merger.get_section(stages, "prestable") == {"$merge_presets": ["a", "c"]}
        merger.get_section(stages, "dev") == {"$merge_presets": ["a"]}

    def test_unknown_special_keys(self):
        merger = StageMerger(["$merge_presets"], special_prefix="$")
        stages = {
            "default": {"$merge_presets": ["a"]},
            "stable": {"$merge_preset": ["b"]},  # Typo.
        }
        with pytest.raises(AssertionError) as ex:
            merger.get_section(stages, "stable")
        assert "unknown" in str(ex.value)
