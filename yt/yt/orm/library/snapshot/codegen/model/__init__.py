from .snapshot import Snapshot

from yt.yt.orm.library.snapshot.codegen.config import SnapshotConfig


def init_snapshot(config: SnapshotConfig) -> Snapshot:
    snapshot = Snapshot(config)
    # NOTICE(andrewln): The reason why we have to call aditional init stages
    # after constructing an object is that in general case, when we have not
    # one but a set of snapshots, we have to run each init stage after
    # previous one was executed for each snapshot in the set (see
    # init_snapshots below).
    # TODO(andrewln): It's awkward to have partially initialized object,
    # think of better API.
    snapshot._init_inheritance({})
    snapshot._init_references()
    return snapshot


def init_snapshots(configs: dict[str, SnapshotConfig]) -> dict[str, Snapshot]:
    snapshots = {name: Snapshot(config) for name, config in configs.items()}

    snapshot_names_ordered_by_inheritance = _topological_sort_by_inheritance(snapshots)
    for snapshot_name in snapshot_names_ordered_by_inheritance:
        snapshots[snapshot_name]._init_inheritance(snapshots)

    for snapshot in snapshots.values():
        snapshot._init_references()

    return snapshots


def _topological_sort_by_inheritance(snapshots: dict[str, Snapshot]) -> list[str]:
    visited = set()
    topological_order = []

    visited_chain = set()
    visited_chain_order = []

    def visit(name: str, snapshot: Snapshot):
        if name in visited:
            return

        visited_chain_order.append(name)

        if name in visited_chain:
            cycle = " -> ".join(visited_chain_order)
            raise ValueError(f'Error initializing snapshot models due to circular dependency: {cycle}.')

        visited_chain.add(name)

        for parent_name in snapshot.direct_parent_names:
            if parent_name not in snapshots:
                raise ValueError(
                    f'Error initializing snapshot models: snapshot "{snapshot.name}" '
                    f'inherits unknown snapshot "{parent_name}".'
                )
            visit(parent_name, snapshots[parent_name])

        visited.add(name)
        topological_order.append(name)

    def visit_chain(name: str, snapshot: Snapshot):
        visited_chain.clear()
        visited_chain_order.clear()
        visit(name, snapshot)

    for name, snapshot in snapshots.items():
        visit_chain(name, snapshot)

    assert set(topological_order) == set(snapshots.keys())
    return topological_order
