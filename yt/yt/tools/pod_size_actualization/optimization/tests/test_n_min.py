"""Tests for compute_n_min_from_req — how many instances of a candidate
container size a bundle needs.

The model: a bundle currently runs `count` instances, each requiring `req`
(per-instance peak, margin already applied), so total demand is T = count * req.
Fitting T into n containers of size s needs n >= T / s. Going above the original
count costs a fixed overhead O per *extra* instance, i.e. n * s >= T + O * (n - count).

Run with:
    ya make -t yt/yt/tools/pod_size_actualization/optimization/tests
"""

import pytest

from yt.yt.tools.pod_size_actualization.optimization.scripts import shared as cfg
from yt.yt.tools.pod_size_actualization.optimization.scripts.shared import (
    compute_n_min_from_req,
)


def n_min(instance_type, count, cpu_req=0, mem_req=0, net_req=0, cpu_s=1, mem_s=1, net_s=1):
    """compute_n_min_from_req with unconstrained resources defaulted away."""
    return compute_n_min_from_req(
        instance_type,
        count,
        cpu_req,
        mem_req,
        net_req,
        cpu_s,
        mem_s,
        net_s,
    )


# ---------------------------------------------------------------------------
# Sizing the instance count
# ---------------------------------------------------------------------------


def test_instances_cover_total_demand():
    # T = 4 * 1000 = 4000, s = 1000 -> 4 containers.
    assert n_min("node", 4, cpu_req=1000, cpu_s=1000) == (4, True)


def test_a_bundle_can_be_consolidated_into_fewer_instances():
    # T = 4000 fits into 2 containers of 2000 — shrinking costs no overhead.
    assert n_min("node", 4, cpu_req=1000, cpu_s=2000) == (2, True)


def test_the_binding_resource_decides():
    # cpu alone would allow 1 container, memory needs 4 -> memory wins.
    assert n_min("node", 4, cpu_req=1000, mem_req=500, cpu_s=4000, mem_s=500) == (4, True)


def test_scale_out_charges_overhead_for_each_extra_instance():
    # T = 4000, s = 800 -> 5 containers would cover it, which is above count=4,
    # so overhead applies: O = max(2.0 cores, 5% of 1000) = 200.
    # n * 800 >= 4000 + 200 * (n - 4)  ->  n >= 3200 / 600  ->  n = 6.
    assert n_min("node", 4, cpu_req=1000, cpu_s=800) == (6, True)


def test_container_smaller_than_the_overhead_is_infeasible():
    # s = 200 equals the overhead, so extra instances add no usable capacity.
    assert n_min("node", 2, cpu_req=1000, cpu_s=200) == (cfg.MAX_EXTRA_RATIO * 2, False)


def test_more_than_double_the_original_count_is_infeasible():
    # 16 instances for a bundle of 2 is past MAX_EXTRA_RATIO — reported, but
    # marked infeasible rather than silently accepted.
    assert n_min("node", 2, cpu_req=1000, cpu_s=300) == (16, False)


def test_bundle_with_no_measured_usage_collapses_to_one_node():
    # A week of metrics across every instance came back at zero, so one node is
    # enough — the recommendation is reviewed by hand anyway.
    assert n_min("node", 20, cpu_s=1000, mem_s=1000, net_s=1000) == (1, True)


def test_empty_bundle_is_rejected():
    # Bundles are built only from non-empty ones, so a zero count is a bug in
    # the caller rather than something to silently paper over.
    with pytest.raises(ValueError, match="count=0"):
        n_min("node", 0, cpu_req=1000, cpu_s=1000)


# ---------------------------------------------------------------------------
# Availability floor: a redundant proxy bundle never drops to one proxy
# ---------------------------------------------------------------------------


def test_redundant_proxy_bundle_is_never_reduced_to_one():
    # T = 2000 fits into a single container of 2000, but dropping to one proxy
    # would mean downtime on its failure.
    assert n_min("proxy", 2, cpu_req=1000, cpu_s=2000) == (2, True)


def test_lightly_loaded_proxy_bundle_shrinks_to_two_not_one():
    assert n_min("proxy", 8, cpu_req=200, cpu_s=2000) == (2, True)


def test_proxy_bundle_with_no_measured_usage_still_keeps_two():
    assert n_min("proxy", 20, cpu_s=1000, mem_s=1000, net_s=1000) == (2, True)


def test_bundle_that_already_runs_a_single_proxy_stays_single():
    # The floor protects existing redundancy, it does not introduce it.
    assert n_min("proxy", 1, cpu_req=1000, cpu_s=2000) == (1, True)


def test_the_floor_never_lowers_a_larger_requirement():
    # T = 4000 with s = 600 needs 8 proxies — the floor must not cap that.
    assert n_min("proxy", 4, cpu_req=1000, mem_req=500, net_req=1000, cpu_s=600, mem_s=1000, net_s=2000) == (8, True)


def test_tablet_nodes_are_not_subject_to_the_proxy_floor():
    assert n_min("node", 2, cpu_req=1000, cpu_s=2000) == (1, True)
