"""Tests for resource prices: the per-cluster-group config and how it is derived.

Цена ресурса — то, чем солвер готов пожертвовать ради другого ресурса. Считается
как дефицит: спрос против пула, нормированный на типовую единицу пула, чтобы
ядра, гигабайты и мегабайты в секунду сравнивались между собой.

Run with:
    ya make -t yt/yt/tools/pod_size_actualization/optimization/tests
"""

import pytest

from yt.yt.tools.pod_size_actualization.optimization import (
    compute_allocation_scarcity_coefficients,
)
from yt.yt.tools.pod_size_actualization.optimization.scripts import shared as cfg
from yt.yt.tools.pod_size_actualization.optimization.scripts.shared import (
    BundleInstances,
    ContainerType,
)

MEDIUM = ContainerType(name="medium", cpu_limit=16.0, mem_limit=100.0, net_limit=400.0)


def node(count=10, cpu=1.0, memory=5.0, network=10.0, container_type=MEDIUM, zones=1):
    return BundleInstances(
        name="ads",
        instance_type="node",
        container_type=container_type,
        count=count,
        cpu=cpu,
        memory=memory,
        network=network,
        cluster="hahn",
        node_type=container_type.name,
        zones=zones,
    )


@pytest.mark.parametrize("group", sorted(cfg.CLUSTER_GROUPS))
def test_every_cluster_group_is_priced(group):
    prices = cfg.RESOURCE_COEFFICIENTS[group]
    assert sorted(prices) == ["a", "b", "c"]
    assert min(prices.values()) > 0
    assert max(prices.values()) == 1.0


def test_the_most_used_resource_is_the_most_expensive():
    # Память выбрана почти целиком, CPU и сеть — едва.
    prices, _ = compute_allocation_scarcity_coefficients([node(cpu=1.0, memory=70.0, network=10.0)])
    assert prices["b"] == 1.0
    assert prices["a"] < 1.0 and prices["c"] < 1.0


def test_a_full_container_costs_in_proportion_to_utilization():
    # Единицы измерения из цены уходят: полный контейнер стоит по каждому ресурсу
    # ровно настолько, насколько этот ресурс выбран.
    prices, debug = compute_allocation_scarcity_coefficients([node(cpu=8.0, memory=50.0, network=200.0)])
    used = debug["utilization"]
    cpu_cost = prices["a"] * MEDIUM.cpu_limit_int()
    mem_cost = prices["b"] * MEDIUM.mem_limit_int()
    net_cost = prices["c"] * MEDIUM.net_limit_int()
    assert cpu_cost / mem_cost == pytest.approx(used["cpu"] / used["mem"])
    assert cpu_cost / net_cost == pytest.approx(used["cpu"] / used["net"])


def test_prices_do_not_depend_on_fleet_size():
    one = compute_allocation_scarcity_coefficients([node(count=1)])[0]
    many = compute_allocation_scarcity_coefficients([node(count=1000)])[0]
    assert one == many


def test_cross_dc_bundles_are_counted_in_all_zones():
    # Одна зона трёхзонного бандла весит столько же, сколько такой же однозонный:
    # в пул и в спрос идут все инстансы.
    small = node(count=1, cpu=8.0, memory=10.0, network=10.0)
    big = node(count=1, cpu=1.0, memory=90.0, network=10.0, zones=3)
    prices, debug = compute_allocation_scarcity_coefficients([small, big])
    assert debug["total_instances"] == 4
    assert (
        prices
        == compute_allocation_scarcity_coefficients([small] + [node(count=1, cpu=1.0, memory=90.0, network=10.0)] * 3)[
            0
        ]
    )
