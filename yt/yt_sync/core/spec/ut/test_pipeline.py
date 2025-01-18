import json
import pytest

from yt.yt_sync.core.constants import PIPELINE_FILES
from yt.yt_sync.core.constants import PIPELINE_QUEUES
from yt.yt_sync.core.constants import PIPELINE_TABLES

from yt.yt_sync.core.spec import PipelineSpec


class TestPipelineSpecification:
    @staticmethod
    def get_good_spec():
        pipeline_path = "//tmp/pipeline"
        common_spec = {
            "clusters": {
                "primary": {
                    "attributes": {
                        "compression_codec": "lz4",
                        "tablet_cell_bundle": "test_bundle",
                        "primary_medium": "ssd_blobs",
                    },
                },
            },
        }
        spec = {
            "path": pipeline_path,
            "monitoring_project": "test-project",
            "monitoring_cluster": "test-cluster",
            "table_spec": {name: common_spec for name in PIPELINE_TABLES},
            "queue_spec": {name: common_spec for name in PIPELINE_QUEUES},
            "file_spec": {name: common_spec for name in PIPELINE_FILES},
        }
        return json.loads(json.dumps(spec))  # Deep copy without deduplication.

    def test_ok(self):
        PipelineSpec.parse(self.get_good_spec())  # No exception.

    def test_empty(self):
        with pytest.raises(AssertionError):
            PipelineSpec.parse({})

    @pytest.mark.parametrize("field", ["path", "monitoring_project", "monitoring_cluster"])
    def test_no_high_level_mandatory_field(self, field):
        spec = self.get_good_spec()
        spec[field] = None
        with pytest.raises(AssertionError):
            PipelineSpec.parse(spec)

    @pytest.mark.parametrize(
        "entity_type_and_attribute",
        [
            ("table", "primary_medium"),
            ("table", "tablet_cell_bundle"),
            ("queue", "primary_medium"),
            ("queue", "tablet_cell_bundle"),
            ("file", "primary_medium"),
            ("file", "compression_codec"),
        ],
    )
    def test_no_mandatory_attribute(self, entity_type_and_attribute):
        entity_type, attribute = entity_type_and_attribute
        spec = self.get_good_spec()
        first_entity = list(spec[f"{entity_type}_spec"].values())[0]
        first_cluster = list(first_entity["clusters"].values())[0]
        first_cluster["attributes"].pop(attribute)
        with pytest.raises(AssertionError):
            PipelineSpec.parse(spec)

    def test_ambiguous_main_cluster(self):
        table_name = list(PIPELINE_TABLES)[0]
        spec = self.get_good_spec()
        clusters = spec["table_spec"][table_name]["clusters"]
        clusters["secondary"] = clusters.pop("primary")
        print(spec)
        with pytest.raises(AssertionError):
            PipelineSpec.parse(spec)
