from dataclasses import dataclass
from dataclasses import field as dataclass_field

import pytest

from yt.yt_sync.core import Settings
from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.diff import DbDiff
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtNode
from yt.yt_sync.core.spec import ClusterNode
from yt.yt_sync.core.spec import Node
from yt.yt_sync.core.state_builder import DesiredStateBuilder
from yt.yt_sync.scenario.helpers import NodeManager
from yt.yt_sync.scenario.helpers.node_manager import _Folder


@dataclass
class _NodeConfig:
    path: str
    attributes: Types.Attributes = dataclass_field(default_factory=dict)
    node_type: str = YtNode.Type.FOLDER
    is_implicit: bool = False

    def build(
        self,
        cluster_name: str = "primary",
        exists: bool = True,
        reset_attributes: bool = False,
        reset_implicit: bool = False,
    ) -> YtNode:
        return YtNode.make(
            cluster_name=cluster_name,
            path=self.path,
            node_type=self.node_type,
            exists=exists,
            attributes=self.attributes if not reset_attributes else {},
            is_implicit=self.is_implicit if not reset_implicit else False,
        )


def make_db(nodes: list[YtNode]) -> YtDatabase:
    db = YtDatabase()
    builder = DesiredStateBuilder(YtClientFactory(dry_run=False, token=None), db, False, True)

    for node in nodes:
        builder.add_node(
            Node(
                type=node.node_type,
                clusters={node.cluster_name: ClusterNode(path=node.path, attributes=node.yt_attributes)},
            )
        )
        db.clusters[node.cluster_name].nodes[node.path].exists = node.exists
        db.clusters[node.cluster_name].nodes[node.path].is_implicit = node.is_implicit
    builder.finalize()
    return db


class TestTreesGeneration:
    @pytest.fixture()
    @staticmethod
    def settings() -> Settings:
        return Settings(
            db_type=Settings.REPLICATED_DB,
            ensure_folders=True,
            ensure_collocation=True,
            collocation_name="test_collocation",
        )

    @staticmethod
    def empty_db(explicit: bool) -> tuple[list[YtNode], list[YtNode], list[_Folder]]:
        """
        DESIRED:
        primary:
        -- folder1
        -- -- folder
        -- folder2
        -- -- file

        ACTUAL:
        """
        desired_config = [
            _NodeConfig("//tmp/folder1"),
            _NodeConfig("//tmp/folder1/folder", attributes={"attribute": "value"}),
            _NodeConfig("//tmp/folder2"),
            _NodeConfig("//tmp/folder2/file", node_type=YtNode.Type.FILE),
        ]
        desired_nodes = {node_config.path: node_config.build() for node_config in desired_config}
        actual_nodes = [node_config.build(exists=False, reset_implicit=True) for node_config in desired_config] + [
            _NodeConfig("//tmp").build()
        ]
        if not explicit:
            del desired_nodes["//tmp/folder1"]
            del desired_nodes["//tmp/folder2"]

        return (
            list(desired_nodes.values()),
            actual_nodes,
            [
                _Folder(
                    path="//tmp/folder1",
                    children={
                        _Folder(
                            path="//tmp/folder1/folder",
                            attributes={"attribute": "value"},
                        )
                    },
                    furthest_child_provider="//tmp/folder1/folder",
                    furthest_child_intersections=1,
                ),
                _Folder("//tmp/folder2"),
            ],
        )

    @staticmethod
    def partial_db(explicit: bool) -> tuple[list[YtNode], list[YtNode], list[_Folder]]:
        """
        DESIRED:
        primary:
        -- folder1
        -- -- folder1
        -- -- folder2
        -- -- file
        -- folder2
        -- -- file

        ACTUAL:
        primary:
        -- folder1
        """
        desired_config = [
            _NodeConfig("//tmp/folder1"),
            _NodeConfig("//tmp/folder1/folder1"),
            _NodeConfig("//tmp/folder1/folder2", attributes={"attribute": "value"}),
            _NodeConfig("//tmp/folder1/file", node_type=YtNode.Type.FILE),
            _NodeConfig("//tmp/folder2"),
            _NodeConfig("//tmp/folder2/file", node_type=YtNode.Type.FILE),
        ]
        desired_nodes = {node_config.path: node_config.build() for node_config in desired_config}
        actual_nodes = [
            node_config.build(exists=node_config.path == "//tmp/folder1", reset_implicit=True)
            for node_config in desired_config
        ] + [_NodeConfig("//tmp").build()]
        if not explicit:
            del desired_nodes["//tmp/folder1"]
            del desired_nodes["//tmp/folder2"]

        return (
            list(desired_nodes.values()),
            actual_nodes,
            [
                _Folder("//tmp/folder1/folder1"),
                _Folder("//tmp/folder1/folder2", {"attribute": "value"}),
                _Folder("//tmp/folder2"),
            ],
        )

    @staticmethod
    def satisfied_db() -> tuple[list[YtNode], list[YtNode], list[_Folder]]:
        return (
            [
                _NodeConfig("//tmp/folder1").build(),
            ],
            [
                _NodeConfig("//tmp").build(),
                _NodeConfig("//tmp/folder1").build(),
                _NodeConfig("//tmp/folder1/folder").build(),
                _NodeConfig("//tmp/folder2").build(),
            ],
            list(),
        )

    @staticmethod
    def unmanaged_db() -> tuple[list[YtNode], list[YtNode], list[_Folder]]:
        return list(), [_NodeConfig("//tmp").build(), _NodeConfig("//tmp/folder1").build()], list()

    @staticmethod
    def deep_db() -> tuple[list[YtNode], list[YtNode], list[_Folder]]:
        """
        DESIRED:
        primary:
        -- folder1
        -- -- folder1
        -- -- -- folder
        -- -- folder2
        -- -- -- folder1
        -- -- -- -- folder
        -- -- -- -- file
        -- -- -- folder2
        -- -- file
        -- folder2
        -- -- folder
        -- -- file

        ACTUAL:
        primary:
        -- folder1
        -- -- folder1
        """
        desired_config = [
            _NodeConfig("//tmp/folder1"),
            _NodeConfig("//tmp/folder1/folder1", attributes={"attribute": "value"}),
            _NodeConfig("//tmp/folder1/folder1/folder"),
            _NodeConfig("//tmp/folder1/folder2"),
            _NodeConfig("//tmp/folder1/folder2/folder1"),
            _NodeConfig("//tmp/folder1/folder2/folder1/folder"),
            _NodeConfig("//tmp/folder1/folder2/folder1/file", node_type=YtNode.Type.FILE),
            _NodeConfig("//tmp/folder1/folder2/folder2"),
            _NodeConfig("//tmp/folder1/file", node_type=YtNode.Type.FILE),
            _NodeConfig("//tmp/folder2", attributes={"attribute": "value"}),
            _NodeConfig("//tmp/folder2/folder"),
            _NodeConfig("//tmp/folder2/file", node_type=YtNode.Type.FILE),
        ]
        desired_nodes = {node_config.path: node_config.build() for node_config in desired_config}
        actual_nodes = [
            node_config.build(
                exists="//tmp/folder1/folder1".startswith(node_config.path),
                reset_attributes=node_config.path == "//tmp/folder1/folder1",
                reset_implicit=True,
            )
            for node_config in desired_config
        ] + [_NodeConfig("//tmp").build()]
        excessive_nodes: set[str] = set()
        for node_path in desired_nodes:
            for other_node_path in desired_nodes:
                if (
                    node_path != other_node_path
                    and other_node_path != "//tmp/folder2"
                    and not desired_nodes[other_node_path].attributes
                    and node_path.startswith(other_node_path)
                ):
                    excessive_nodes.add(other_node_path)
        for node_path in excessive_nodes:
            del desired_nodes[node_path]

        return (
            list(desired_nodes.values()),
            actual_nodes,
            [
                _Folder("//tmp/folder1/folder1/folder", attributes={"attribute": "value"}),
                _Folder(
                    path="//tmp/folder1/folder2",
                    children={
                        _Folder(
                            path="//tmp/folder1/folder2/folder1",
                            children={_Folder("//tmp/folder1/folder2/folder1/folder")},
                            furthest_child_provider="//tmp/folder1/folder2/folder1/folder",
                            furthest_child_intersections=1,
                        ),
                        _Folder("//tmp/folder1/folder2/folder2"),
                    },
                    furthest_child_provider="//tmp/folder1/folder2/folder1",
                    furthest_child_intersections=2,
                ),
                _Folder(
                    path="//tmp/folder2",
                    attributes={"attribute": "value"},
                    children={_Folder("//tmp/folder2/folder", attributes={"attribute": "value"})},
                    furthest_child_provider="//tmp/folder2/folder",
                    furthest_child_intersections=1,
                ),
            ],
        )

    def partially_accessible_db(has_folder2: bool) -> tuple[list[YtNode], list[YtNode], list[_Folder] | None]:
        """
        DESIRED:
        primary:
        -- folder
        -- -- folder1
        -- -- -- folder
        -- -- -- -- folder1
        -- -- -- -- -- file
        -- -- -- -- folder2
        -- -- -- -- -- doc
        -- -- folder2
        -- -- -- file

        ACTUAL:
        primary:
        -- folder[DENIED]
        -- -- folder1[DENIED]
        -- -- -- folder
        -- -- folder2(?)
        """
        desired = [
            config
            for config in [
                _NodeConfig("//tmp/folder/folder1/folder/folder1/file", node_type=YtNode.Type.FILE),
                _NodeConfig("//tmp/folder/folder1/folder/folder2/doc", node_type=YtNode.Type.DOCUMENT),
                _NodeConfig("//tmp/folder/folder2/file", node_type=YtNode.Type.FILE),
            ]
        ]
        return (
            [config.build() for config in desired],
            [
                config.build()
                for config in [
                    _NodeConfig("//tmp"),
                    _NodeConfig("//tmp/folder", is_implicit=True),
                    _NodeConfig("//tmp/folder/folder1", is_implicit=True),
                    _NodeConfig("//tmp/folder/folder1/folder"),
                ]
            ]
            + [
                config.build(exists=False, reset_implicit=True)
                for config in desired
                + [
                    _NodeConfig("//tmp/folder/folder1/folder/folder1"),
                    _NodeConfig("//tmp/folder/folder1/folder/folder2"),
                ]
            ]
            + [_NodeConfig("//tmp/folder/folder2").build(exists=has_folder2, reset_implicit=True)],
            (
                [
                    _Folder("//tmp/folder/folder1/folder/folder1"),
                    _Folder("//tmp/folder/folder1/folder/folder2"),
                ]
                if has_folder2
                else None
            ),
        )

    @pytest.mark.parametrize(
        "desired_nodes, actual_nodes, expected_trees",
        [
            empty_db(explicit=True),
            empty_db(explicit=False),
            partial_db(explicit=True),
            partial_db(explicit=False),
            satisfied_db(),
            unmanaged_db(),
            deep_db(),
            partially_accessible_db(True),
            partially_accessible_db(False),
        ],
    )
    def test_trees_generation(
        self,
        settings: Settings,
        desired_nodes: list[YtNode],
        actual_nodes: list[YtNode],
        expected_trees: list[_Folder] | None,
    ):
        desired_db = make_db(desired_nodes)
        actual_db = make_db(actual_nodes)

        manager = NodeManager(YtClientFactory(dry_run=True, token=None), settings)
        if expected_trees is None:
            with pytest.raises(AssertionError):
                manager._generate_trees(DbDiff.generate(settings, desired_db, actual_db), actual_db.clusters["primary"])
        else:
            assert (
                manager._generate_trees(DbDiff.generate(settings, desired_db, actual_db), actual_db.clusters["primary"])
                == expected_trees
            )


class TestBatchGeneration:
    @staticmethod
    def no_trees() -> tuple[list[_Folder], list[set[_Folder]]]:
        return list(), list()

    @staticmethod
    def line() -> tuple[list[_Folder], list[set[_Folder]]]:
        return [
            _Folder(
                path="//tmp/folder",
                children={
                    _Folder(
                        path="//tmp/folder/folder",
                        children={_Folder("//tmp/folder/folder/folder")},
                        furthest_child_provider="//tmp/folder/folder/folder",
                        furthest_child_intersections=1,
                    )
                },
                furthest_child_provider="//tmp/folder/folder",
                furthest_child_intersections=2,
            )
        ], [
            {_Folder("//tmp/folder/folder/folder")},
        ]

    @staticmethod
    def tree() -> tuple[list[_Folder], list[set[_Folder]]]:
        return [
            _Folder(
                path="//tmp/folder",
                children={
                    _Folder(
                        path="//tmp/folder/folder1",
                        children={
                            _Folder(
                                path="//tmp/folder/folder1/folder1",
                                children={_Folder("//tmp/folder/folder1/folder1/folder")},
                                furthest_child_provider="//tmp/folder/folder1/folder1/folder",
                                furthest_child_intersections=1,
                            ),
                            _Folder("//tmp/folder/folder1/folder2"),
                        },
                        furthest_child_provider="//tmp/folder/folder1/folder1",
                        furthest_child_intersections=2,
                    ),
                    _Folder(
                        path="//tmp/folder/folder2",
                        children={
                            _Folder("//tmp/folder/folder2/folder1"),
                            _Folder("//tmp/folder/folder2/folder2"),
                        },
                        furthest_child_provider="//tmp/folder/folder2/folder1",
                        furthest_child_intersections=1,
                    ),
                },
                furthest_child_provider="//tmp/folder/folder1",
                furthest_child_intersections=3,
            )
        ], [
            {_Folder("//tmp/folder/folder1/folder1/folder")},
            {_Folder("//tmp/folder/folder1/folder2"), _Folder("//tmp/folder/folder2/folder1")},
            {_Folder("//tmp/folder/folder2/folder2")},
        ]

    @staticmethod
    def many_trees() -> tuple[list[_Folder], list[set[_Folder]]]:
        return [
            _Folder(
                path="//tmp/folder1",
                children={_Folder("//tmp/folder1/folder1"), _Folder("//tmp/folder1/folder2")},
                furthest_child_provider="//tmp/folder1/folder1",
                furthest_child_intersections=1,
            ),
            _Folder("//tmp/folder2"),
        ], [
            {_Folder("//tmp/folder1/folder1"), _Folder("//tmp/folder2")},
            {_Folder("//tmp/folder1/folder2")},
        ]

    @staticmethod
    def attributes_separation() -> tuple[list[_Folder], list[set[_Folder]]]:
        separation_node = _Folder(
            path="//tmp/folder1",
            attributes={"attribute": "value"},
            children={
                _Folder("//tmp/folder1/folder1"),
                _Folder("//tmp/folder1/folder2"),
            },
            furthest_child_provider="//tmp/folder1/folder1",
            furthest_child_intersections=1,
        )
        return [
            separation_node,
            _Folder("//tmp/folder2"),
        ], [
            {separation_node, _Folder("//tmp/folder2")},
            {
                _Folder("//tmp/folder1/folder1"),
                _Folder("//tmp/folder1/folder2"),
            },
        ]

    @pytest.mark.parametrize(
        "trees, batches",
        [no_trees(), line(), tree(), many_trees(), attributes_separation()],
    )
    def test_batch_generation(self, trees: list[_Folder], batches: list[set[_Folder]]):
        assert NodeManager._generate_creation_batches(trees) == batches
