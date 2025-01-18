from typing import Any

import pytest

from yt.yt_sync.core.constants import CONSUMER_ATTRS
from yt.yt_sync.core.spec.details import NodeSpec
from yt.yt_sync.core.spec.node import Node


class TestNodeSpecification:
    def test_empty(self):
        with pytest.raises(AssertionError):
            NodeSpec.parse({})

    @pytest.mark.parametrize("value", [1, 1.0, "1", True, []])
    def test_parse_non_dict(self, value: Any):
        with pytest.raises(AssertionError):
            NodeSpec.parse(value)

    def test_no_type(self, table_path: str):
        with pytest.raises(AssertionError):
            NodeSpec.parse(
                {
                    "clusters": {
                        "primary": {"path": table_path, "attributes": CONSUMER_ATTRS},
                    },
                }
            )

    def test_no_clusters(self):
        with pytest.raises(AssertionError):
            NodeSpec.parse({"type": Node.Type.FOLDER})

    def test_empty_clusters(self):
        with pytest.raises(AssertionError):
            NodeSpec.parse({"type": Node.Type.FOLDER, "clusters": {}})

    def test_parse_unknown_type(self, table_path: str):
        with pytest.raises(ValueError):
            NodeSpec.parse(
                {
                    "type": "_unknown_",
                    "clusters": {
                        "primary": {"path": table_path, "attributes": CONSUMER_ATTRS},
                    },
                }
            )

    def test_parse_no_path(self):
        with pytest.raises(AssertionError):
            NodeSpec.parse(
                {
                    "type": Node.Type.FOLDER,
                    "clusters": {
                        "primary": {"attributes": CONSUMER_ATTRS},
                    },
                }
            )

    def test_parse_empty_path(self):
        with pytest.raises(AssertionError):
            NodeSpec.parse(
                {
                    "type": Node.Type.FOLDER,
                    "clusters": {
                        "primary": {"path": "", "attributes": CONSUMER_ATTRS},
                    },
                }
            )

    def test_parse_no_main(self, table_path: str):
        spec = NodeSpec.parse(
            {
                "type": Node.Type.FOLDER,
                "clusters": {
                    "primary": {"path": table_path, "attributes": CONSUMER_ATTRS},
                    "remote0": {"path": table_path, "attributes": CONSUMER_ATTRS},
                },
            }
        )
        assert spec.main_cluster is None
        assert spec.is_federation
        with pytest.raises(AssertionError):
            spec.main_cluster_spec

    def test_parse_many_main(self, table_path: str):
        with pytest.raises(AssertionError):
            NodeSpec.parse(
                {
                    "type": Node.Type.FOLDER,
                    "clusters": {
                        "primary": {"main": True, "path": table_path, "attributes": CONSUMER_ATTRS},
                        "remote0": {"main": True, "path": table_path, "attributes": CONSUMER_ATTRS},
                    },
                }
            )

    @pytest.mark.parametrize("main", [True, False, None])
    def test_parse_single_cluster(self, table_path: str, main: bool | None):
        for node_type in Node.Type.all():
            raw_spec = {
                "type": node_type,
                "clusters": {
                    "primary": {"path": table_path, "attributes": CONSUMER_ATTRS, "target_path": table_path},
                },
            }
            if main is not None:
                raw_spec["clusters"]["primary"]["main"] = main

            spec: NodeSpec = NodeSpec.parse(raw_spec)
            assert spec
            assert spec.type == node_type
            assert not spec.is_federation
            assert spec.clusters
            assert len(spec.clusters) == 1
            assert "primary" in spec.clusters

            if main is False:
                # explicit no main
                assert spec.main_cluster is None
                with pytest.raises(AssertionError):
                    spec.main_cluster_spec
            else:
                assert spec.main_cluster == "primary"
                main_cluster_spec = spec.main_cluster_spec
                assert main_cluster_spec.main

            cluster_spec = spec.clusters["primary"]
            assert cluster_spec.path == table_path
            assert cluster_spec.attributes == CONSUMER_ATTRS
            assert cluster_spec.target_path == table_path

    @pytest.mark.parametrize("main", [True, False, None])
    def test_parse_federation(self, table_path: str, main: bool | None):
        remote0path: str = f"{table_path}_0"
        raw_spec = {
            "type": Node.Type.FOLDER,
            "clusters": {
                "primary": {"path": table_path, "attributes": CONSUMER_ATTRS},
                "remote0": {"path": remote0path},
            },
        }
        if main is not None:
            raw_spec["clusters"]["primary"]["main"] = main
        spec: NodeSpec = NodeSpec.parse(raw_spec)
        assert spec
        assert spec.type == Node.Type.FOLDER
        assert spec.is_federation
        assert spec.clusters
        assert len(spec.clusters) == 2

        assert "primary" in spec.clusters
        primary_spec = spec.clusters["primary"]
        if main:
            assert spec.main_cluster == "primary"
            assert spec.main_cluster_spec == primary_spec
            assert primary_spec.main
        else:
            assert spec.main_cluster is None
            with pytest.raises(AssertionError):
                assert spec.main_cluster_spec
        assert primary_spec.path == table_path
        assert primary_spec.attributes == CONSUMER_ATTRS

        assert "remote0" in spec.clusters
        remote0spec = spec.clusters["remote0"]
        if main:
            assert remote0spec != spec.main_cluster_spec
        assert not remote0spec.main
        assert remote0spec.path == remote0path
        assert remote0spec.attributes == {}
