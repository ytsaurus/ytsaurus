"""Tests for what optimize_assignment writes into assign_df.

По строке назначения должно считаться изменение потребления ресурсов
(Base* × BaseCount против New* × NewCount) и строиться визуализация:
Usage* — потребление на инстанс, GroupID — что солвер считал вместе.

Run with:
    ya make -t yt/yt/tools/pod_size_actualization/optimization/tests
"""

import contextlib
import io

from yt.yt.tools.pod_size_actualization.optimization import data, simple
from yt.yt.tools.pod_size_actualization.optimization.scripts.shared import (
    BundleInstances,
    ContainerType,
)

NODE_SIZES = [(8.0, 40.0, 200.0), (28.0, 200.0, 600.0)]
PROXY_SIZES = [(6.0, 10.0, 200.0)]
COEFFICIENTS = {"a": 1.0, "b": 1.0, "c": 1.0}

MEDIUM = ContainerType(name="medium", cpu_limit=16.0, mem_limit=100.0, net_limit=400.0)


def assign(bundle_instances):
    with contextlib.redirect_stdout(io.StringIO()):
        groups = data.build_bundle_groups(bundle_instances)
        _, _, assign_df = simple.optimize_assignment(
            groups,
            NODE_SIZES,
            PROXY_SIZES,
            COEFFICIENTS,
        )
    return assign_df


def node(name="ads", cluster="seneca-sas", count=4, cpu=1.0, memory=5.0, network=10.0):
    return BundleInstances(
        name=name,
        instance_type="node",
        container_type=MEDIUM,
        count=count,
        cpu=cpu,
        memory=memory,
        network=network,
        cluster=cluster,
        node_type=MEDIUM.name,
    )


def test_base_columns_describe_the_current_instance():
    row = assign([node()]).iloc[0]
    assert (row["BaseContainerType"], row["BaseCount"]) == ("medium", 4)
    assert (row["BaseCPU"], row["BaseMemory"], row["BaseNetwork"]) == (16.0, 100.0, 400.0)


def test_new_columns_describe_the_assigned_size():
    row = assign([node()]).iloc[0]
    assigned = NODE_SIZES[row["AssignedContainerTypeID"]]
    assert (row["NewCPU"], row["NewMemory"], row["NewNetwork"]) == assigned


def test_resource_delta_is_computable_from_the_row():
    # Бандл с малым потреблением уезжает на меньший размер: ресурсов станет меньше.
    row = assign([node(count=4, cpu=1.0, memory=5.0, network=10.0)]).iloc[0]
    before = row["BaseCPU"] * row["BaseCount"]
    after = row["NewCPU"] * row["NewCount"]
    assert after < before


def test_usage_goes_into_the_row_as_is():
    # Потребление на инстанс, без margin: клип по текущему контейнеру уже сделан загрузкой.
    row = assign([node(cpu=3.5, memory=7.0, network=12.0)]).iloc[0]
    assert (row["UsageCPU"], row["UsageMemory"], row["UsageNetwork"]) == (3.5, 7.0, 12.0)


def test_bundles_solved_together_share_a_group_id():
    rows = assign([node(cluster="seneca-sas"), node(cluster="seneca-vla")])
    assert rows["GroupID"].nunique() == 1


def test_node_and_proxy_of_one_bundle_are_different_groups():
    proxy = BundleInstances(
        name="ads",
        instance_type="proxy",
        container_type=MEDIUM,
        count=4,
        cpu=1.0,
        memory=5.0,
        network=10.0,
        cluster="seneca-sas",
        node_type=MEDIUM.name,
    )
    rows = assign([node(), proxy])
    assert rows["GroupID"].nunique() == 2
