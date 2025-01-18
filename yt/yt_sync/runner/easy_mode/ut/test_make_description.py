import yaml

from library.python import resource

from yt.yt_sync.runner.easy_mode import StagesSpec
from yt.yt_sync.runner.builtin_presets import BUILTIN_PRESETS
from yt.yt_sync.runner.easy_mode.make_description import ensure_builtin_presets
from yt.yt_sync.runner.easy_mode.make_description import make_runner_description
from yt.yt_sync.core.spec_merger.stages_merger import make_stage_specs


def test_ensure_builtin_presets():
    assert ensure_builtin_presets(StagesSpec()).builtin_presets == BUILTIN_PRESETS

    presets = {"gaga": {"bebe": 1}}
    assert ensure_builtin_presets(StagesSpec(builtin_presets=presets)).builtin_presets == presets


def test_make_runner_description():
    stages_spec = StagesSpec(
        stages={
            "default": {
                "presets": {
                    "builtin:storage_preset": {
                        "clusters": {"_all_clusters": {"attributes": {"primary_medium": "ssd_blobs"}}}
                    },
                    "builtin:table_preset": {
                        "clusters": {"_all_clusters": {"attributes": {"tablet_cell_bundle": "bundle"}}}
                    },
                    "pipeline_prod_like_sorted_table_preset": {
                        "clusters": {
                            "_all_data_clusters": {
                                "attributes": {
                                    "tablet_balancer_config": {"min_tablet_count": 100, "desired_tablet_count": 200},
                                },
                            },
                        },
                    },
                    "pipeline_prod_like_preset": {
                        "table_spec": {
                            "input_messages": {
                                "clusters": {
                                    "_all_data_clusters": {
                                        "attributes": {"mount_config": {"min_data_versions": 0, "max_data_ttl": "3h"}},
                                    }
                                }
                            }
                        }
                    },
                },
            },
            "stable": {
                "folder": "//stable",
                "presets": {
                    # Fix markov primary medium. All entities got cluster markov.
                    "builtin:storage_preset": {"clusters": {"markov": {"attributes": {"primary_medium": "default"}}}},
                    # All tables has markov as main.
                    "builtin:table_preset": {"clusters": {"markov": {"main": True}}},
                    # Override presets for pipeline.
                    "builtin:pipeline_sorted_table_preset": {
                        "$merge_presets": ["pipeline_prod_like_sorted_table_preset"]
                    },
                    "builtin:pipeline_preset": {"$merge_presets": ["pipeline_prod_like_preset"]},
                },
            },
            "dev": {
                "folder": "//dev",
                "presets": {
                    "builtin:storage_preset": {"clusters": {"zeno": {}}},
                },
            },
        },
        pipelines={
            "my_pipeline": {
                "default": {"$merge_presets": ["builtin:pipeline_preset"]},
                "stable": {
                    "monitoring_project": "<project>",
                    "monitoring_cluster": "<cluster>",
                },
                "dev": {
                    "monitoring_project": "<project>",
                    "monitoring_cluster": "<cluster-dev>",
                },
            },
        },
    )

    # Run builtin checks of runner and core specs.
    make_runner_description(stages_spec)

    stage_specs = make_stage_specs(ensure_builtin_presets(stages_spec))
    stable_spec = stage_specs["stable"]

    def check_stable_standalone_entity(clusters):
        assert clusters.get("seneca-sas") is None
        assert clusters["markov"]["attributes"]["primary_medium"] == "default"

    stable_my_pipeline = stable_spec.pipelines["my_pipeline"]
    check_stable_standalone_entity(stable_my_pipeline["table_spec"]["input_messages"]["clusters"])
    check_stable_standalone_entity(stable_my_pipeline["queue_spec"]["controller_logs"]["clusters"])
    check_stable_standalone_entity(stable_my_pipeline["file_spec"]["important_versions"]["clusters"])
    stable_my_pipeline_table_attrs = stable_my_pipeline["table_spec"]["input_messages"]["clusters"]["markov"][
        "attributes"
    ]
    # Rely on builtin presets.
    assert stable_my_pipeline_table_attrs.get("optimize_for") is not None
    assert stable_my_pipeline_table_attrs.get("compression_codec") is not None


def test_parse_stages_example():
    stages_spec_raw = yaml.load(resource.find("stages_example.yaml"), Loader=yaml.FullLoader)
    make_runner_description(StagesSpec(**stages_spec_raw))
