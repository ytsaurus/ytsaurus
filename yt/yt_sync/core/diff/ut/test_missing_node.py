from copy import deepcopy
from typing import Any

import pytest

from yt.yt_sync.core.diff import MissingNode
from yt.yt_sync.core.helpers import get_dummy_logger
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtNode

from .helpers import DiffDbTestBase


class TestMissingNode(DiffDbTestBase):
    @pytest.fixture
    def affected_cluster(self) -> str:
        return "remote0"

    def _test_added_node(
        self,
        desired_db: YtDatabase,
        affected_cluster: str,
        folder_path: str,
        added_attributes: dict[str, Any],
        desired_type: str = YtNode.Type.FOLDER,
        actual_type: str = YtNode.Type.FOLDER,
        should_fail: bool = False,
    ):
        desired_db.clusters[affected_cluster].nodes[folder_path] = YtNode.make(
            cluster_name=affected_cluster,
            path=folder_path,
            node_type=desired_type,
            exists=True,
            attributes=added_attributes,
        )
        actual_db = deepcopy(desired_db)
        actual_db.clusters[affected_cluster].nodes[folder_path].exists = False
        actual_db.clusters[affected_cluster].nodes[folder_path].attributes = {}
        actual_db.clusters[affected_cluster].nodes[folder_path].node_type = actual_type

        diff = MissingNode.make(
            desired_db.clusters[affected_cluster].nodes[folder_path],
            actual_db.clusters[affected_cluster].nodes[folder_path],
        )
        assert not diff.is_empty()

        assert diff.check_and_log(get_dummy_logger()) != should_fail

    def test_empty_diff(self, desired_db: YtDatabase, affected_cluster: str, folder_path: str):
        actual_db = deepcopy(desired_db)

        diff = MissingNode.make(
            desired_db.clusters[affected_cluster].nodes[folder_path],
            actual_db.clusters[affected_cluster].nodes[folder_path],
        )
        assert diff.is_empty()

    def test_attributes_change(self, desired_db: YtDatabase, affected_cluster: str, folder_path: str):
        actual_db = deepcopy(desired_db)
        actual_db.clusters[affected_cluster].nodes[folder_path].attributes["attribute"] = "prev"

        diff = MissingNode.make(
            desired_db.clusters[affected_cluster].nodes[folder_path],
            actual_db.clusters[affected_cluster].nodes[folder_path],
        )
        assert diff.is_empty()

    def test_added_folder(
        self,
        desired_db: YtDatabase,
        affected_cluster: str,
        folder_path: str,
    ):
        self._test_added_node(desired_db, affected_cluster, folder_path, {})

    def test_added_folder_with_attributes(
        self,
        desired_db: YtDatabase,
        affected_cluster: str,
        folder_path: str,
    ):
        self._test_added_node(desired_db, affected_cluster, folder_path, {"attr": "value"})

    def test_added_folder_different_type(
        self,
        desired_db: YtDatabase,
        affected_cluster: str,
        folder_path: str,
    ):
        self._test_added_node(
            desired_db,
            affected_cluster,
            folder_path,
            {},
            desired_type=YtNode.Type.FOLDER,
            actual_type=YtNode.Type.FILE,
        )

    def test_missing_any_node(
        self,
        desired_db: YtDatabase,
        affected_cluster: str,
        folder_path: str,
    ):
        self._test_added_node(
            desired_db,
            affected_cluster,
            folder_path,
            {},
            desired_type=YtNode.Type.ANY,
            should_fail=True,
        )
