# flake8: noqa

from yt_dashboard_generator.dashboard import Rowset
from yt_dashboard_generator.sensor import MultiSensor
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr

from .common.sensors import Master, TabletBalancer

##################################################################

# Counters registered in TTableRegistry::InitializeProfilingCounters
# (yt/yt/server/tablet_balancer/table_registry.cpp).
ORDINARY_MOVES = ["ordinary_moves", "in_memory_moves"]
PARAMETERIZED_MOVES = ["parameterized_moves"]
REPLICA_MOVES = ["parameterized_replica_moves"]

ORDINARY_RESHARDS = ["tablet_merges", "tablet_splits", "non_trivial_reshards"]
PARAMETERIZED_RESHARDS = ["parameterized_reshard_merges", "parameterized_reshard_splits"]
REPLICA_RESHARDS = ["replica_merges", "replica_splits", "replica_non_trivial_reshards"]

# Counters of the in-master tablet balancer (yt/yt/server/master/cell_server/cell_bundle.cpp).
MASTER_MOVES = ["in_memory_moves", "ext_memory_moves"]
MASTER_RESHARDS = ["tablet_merges"]


def _counter_alias(counter):
    return (counter
        .replace("_reshard_", "_")
        .replace("parameterized_replica", "replica")
        .replace("ext_memory", "ordinary")
        .replace("in_memory", "in-memory")
        .replace("non_trivial", "non-trivial")
        .replace("_", " "))


def _build_master_tablet_balancer_sensor(counters):
    tb = Master("yt.tablet_server.tablet_balancer.{}.rate")
    return MultiSensor(*[
        MonitoringExpr(tb(counter).aggr("#H")).alias(_counter_alias(counter))
        for counter in counters
    ]).stack(False)


def _build_standalone_tablet_balancer_sensor(counters, fixed_table_path=False):
    tb = TabletBalancer("yt.tablet_balancer.tablet_balancer.{}.rate")
    sensors = []

    for counter in counters:
        sensor = tb(counter).all("container")
        alias = _counter_alias(counter)

        if counter.startswith(("parameterized_", "replica_")):
            sensor = sensor.all("group")
            if not fixed_table_path:
                sensor = sensor.aggr("table_path")
            sensors.append(MonitoringExpr(sensor).alias(alias + ", {{group}}"))
        elif fixed_table_path:
            sensors.append(MonitoringExpr(sensor.aggr("group")).alias(alias))
        else:
            sensors.append(MonitoringExpr(sensor.aggr("group").all("table_path"))
                .top_max(10)
                .alias(alias + ", {{table_path}}"))

    return MultiSensor(*sensors).stack(False)


# Use fixed_table_path=True when table_path is set by the dashboard.
def build_tablet_balancer_moves_sensor(
        counters=ORDINARY_MOVES + PARAMETERIZED_MOVES + REPLICA_MOVES,
        fixed_table_path=False,
        backend="monitoring"):
    if backend == "grafana":
        return _build_master_tablet_balancer_sensor(MASTER_MOVES)
    return _build_standalone_tablet_balancer_sensor(counters, fixed_table_path)


def build_tablet_balancer_reshards_sensor(
        counters=ORDINARY_RESHARDS + PARAMETERIZED_RESHARDS + REPLICA_RESHARDS,
        fixed_table_path=False,
        backend="monitoring"):
    if backend == "grafana":
        return _build_master_tablet_balancer_sensor(MASTER_RESHARDS)
    return _build_standalone_tablet_balancer_sensor(counters, fixed_table_path)


def build_tablet_balancer_rowset(backend="monitoring"):
    if backend == "grafana":
        return (Rowset()
            .row()
                .cell("Tablet balancer moves", build_tablet_balancer_moves_sensor(backend=backend))
                .cell("Tablet balancer reshards", build_tablet_balancer_reshards_sensor(backend=backend))
        ).owner

    rowset = Rowset()
    for title, moves, reshards in [
        ("Ordinary tablet balancer", ORDINARY_MOVES, ORDINARY_RESHARDS),
        ("Parameterized tablet balancer", PARAMETERIZED_MOVES, PARAMETERIZED_RESHARDS),
        ("Replica tablet balancer", REPLICA_MOVES, REPLICA_RESHARDS),
    ]:
        (rowset.row()
            .cell(f"{title} moves", _build_standalone_tablet_balancer_sensor(moves))
            .cell(f"{title} reshards", _build_standalone_tablet_balancer_sensor(reshards))
        )
    return rowset
