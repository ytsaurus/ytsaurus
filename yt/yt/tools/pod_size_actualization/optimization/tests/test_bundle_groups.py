"""Tests for bundle grouping — what ends up in one group and how the resource
requirement of each bundle inside it is computed.

Two levels of requirement matter downstream:
  * BundleGroup.{cpu,mem,net}_req_int — the group's own requirement, always the
    max over everything in the group; used to size the container type.
  * BundleGroup.bundle_key_max_req[bi.bundle] — the per-bundle requirement that
    simple.optimize_assignment / precompute feed into compute_n_min_from_req to
    decide how many instances each bundle gets. Some groupings raise every
    member to a shared max here, others leave each member with its own value.

Run with:
    ya make -t yt/yt/tools/pod_size_actualization/optimization/tests
"""

import contextlib
import io

import pytest

from yt.yt.tools.pod_size_actualization.optimization import data
from yt.yt.tools.pod_size_actualization.optimization.scripts import shared as cfg
from yt.yt.tools.pod_size_actualization.optimization.scripts.shared import (
    BundleGroup,
    BundleInstances,
    ContainerType,
)

SENECAS = ("seneca-sas", "seneca-vla", "seneca-klg")


def bi(name, cluster, instance_type="node", node_type="medium", count=4, cpu=10.0, memory=50.0, network=100.0):
    """A BundleInstances with everything but the interesting fields defaulted.

    Container limits are set high enough that *_req_int never clamps to them.
    """
    return BundleInstances(
        name=name,
        instance_type=instance_type,
        container_type=ContainerType(
            name=node_type,
            cpu_limit=1e6,
            mem_limit=1e6,
            net_limit=1e6,
        ),
        count=count,
        cpu=cpu,
        memory=memory,
        network=network,
        cluster=cluster,
        node_type=node_type,
    )


def build(bundles):
    """build_bundle_groups() without its diagnostic output."""
    with contextlib.redirect_stdout(io.StringIO()):
        return data.build_bundle_groups(bundles)


def build_one(bundles):
    groups = build(bundles)
    assert len(groups) == 1, f"expected a single group, got {[g.label for g in groups]}"
    return groups[0]


def clusters_of(group):
    return sorted(group.bundles_by_cluster)


def cpu_reqs(group):
    """{bundle key -> per-bundle cpu requirement} — what the assigner uses."""
    return {key: req[0] for key, req in group.bundle_key_max_req.items()}


def cpu_req_of(cpu):
    """The requirement a bundle with this cpu usage produces on its own."""
    return bi("x", "seneca-sas", cpu=cpu).cpu_req_int()


# ---------------------------------------------------------------------------
# What ends up in one group
# ---------------------------------------------------------------------------


def test_same_bundle_across_seneca_clusters_forms_one_group():
    group = build_one([bi("foo", c) for c in SENECAS])

    assert clusters_of(group) == sorted(SENECAS)
    assert group.counts_by_cluster == {c: 4 for c in SENECAS}


def test_different_bundle_names_stay_separate():
    groups = build([bi("foo", "seneca-sas"), bi("bar", "seneca-sas")])

    assert len(groups) == 2


def test_node_and_proxy_of_one_bundle_are_separate_groups():
    groups = build(
        [
            bi("foo", "seneca-sas", instance_type="node"),
            bi("foo", "seneca-sas", instance_type="proxy"),
        ]
    )

    assert sorted(g.instance_type for g in groups) == ["node", "proxy"]


def test_env_suffix_variants_are_merged():
    group = build_one(
        [
            bi("foo", "seneca-sas"),
            bi("foo-prestable", "seneca-vla"),
            bi("foo_prod", "seneca-klg"),
        ]
    )

    assert sorted({b.name for b in group.all_bundles}) == [
        "foo",
        "foo-prestable",
        "foo_prod",
    ]


def test_testing_suffix_is_not_merged_with_production():
    groups = build([bi("foo", "seneca-sas"), bi("foo-testing", "seneca-sas")])

    assert len(groups) == 2


def test_node_type_divergence_splits_the_group():
    groups = build(
        [
            bi("foo", "seneca-sas", node_type="medium"),
            bi("foo", "seneca-vla", node_type="big"),
            bi("foo", "seneca-klg", node_type="big"),
        ]
    )

    assert sorted(clusters_of(g) for g in groups) == [
        ["seneca-klg", "seneca-vla"],
        ["seneca-sas"],
    ]


def test_count_divergence_up_to_two_keeps_the_group_consistent():
    group = build_one(
        [
            bi("foo", "seneca-sas", count=4),
            bi("foo", "seneca-vla", count=6),
        ]
    )

    assert clusters_of(group) == ["seneca-sas", "seneca-vla"]


def test_large_count_divergence_still_ends_up_in_one_group():
    # Split into per-count buckets first, then merged back together — the
    # difference shows up in the per-bundle requirements, not in the shape.
    group = build_one(
        [
            bi("foo", "seneca-sas", count=4),
            bi("foo", "seneca-vla", count=40),
        ]
    )

    assert clusters_of(group) == ["seneca-sas", "seneca-vla"]


# ---------------------------------------------------------------------------
# Per-bundle requirements: raised to a shared max, or left alone
# ---------------------------------------------------------------------------


def test_same_bundle_across_clusters_is_sized_by_the_heaviest_cluster():
    group = build_one(
        [
            bi("foo", "seneca-sas", cpu=30.0),
            bi("foo", "seneca-vla", cpu=10.0),
            bi("foo", "seneca-klg", cpu=10.0),
        ]
    )

    # All three clusters are sized as if they carried the sas load.
    assert cpu_reqs(group) == {
        "foo@node@seneca-sas": cpu_req_of(30.0),
        "foo@node@seneca-vla": cpu_req_of(30.0),
        "foo@node@seneca-klg": cpu_req_of(30.0),
    }


def test_env_merged_bundles_keep_their_own_requirements():
    group = build_one(
        [
            bi("foo", "seneca-sas", cpu=30.0),
            bi("foo-prestable", "seneca-sas", cpu=10.0),
        ]
    )

    # One container size for both, but prestable is not inflated to prod's load.
    assert cpu_reqs(group) == {
        "foo@node@seneca-sas": cpu_req_of(30.0),
        "foo-prestable@node@seneca-sas": cpu_req_of(10.0),
    }


def test_count_split_bundles_keep_their_own_requirements():
    group = build_one(
        [
            bi("foo", "seneca-sas", count=4, cpu=30.0),
            bi("foo", "seneca-vla", count=40, cpu=10.0),
        ]
    )

    # Counts differ too much to treat the clusters as one bundle, so each
    # cluster keeps the load it actually has.
    assert cpu_reqs(group) == {
        "foo@node@seneca-sas": cpu_req_of(30.0),
        "foo@node@seneca-vla": cpu_req_of(10.0),
    }


def test_below_minimum_merged_groups_keep_their_own_requirements(monkeypatch):
    monkeypatch.setattr(cfg, "MERGE_BELOW_MIN", True)
    tiny = dict(memory=1.0, network=10.0)
    group = build_one(
        [
            bi("tiny-a", "seneca-sas", cpu=1.0, **tiny),
            bi("tiny-b", "seneca-sas", cpu=2.0, **tiny),
        ]
    )

    assert cpu_reqs(group) == {
        "tiny-a@node@seneca-sas": cpu_req_of(1.0),
        "tiny-b@node@seneca-sas": cpu_req_of(2.0),
    }


def test_group_requirement_is_the_max_over_the_whole_group():
    group = build_one(
        [
            bi("foo", "seneca-sas", cpu=30.0, memory=50.0, network=100.0),
            bi("foo-prestable", "seneca-vla", cpu=10.0, memory=80.0, network=90.0),
        ]
    )

    # Even where per-bundle requirements stay apart, the group's own requirement
    # — which sizes the container type — is the max of each resource.
    assert group.cpu_req_int == bi("x", "seneca-sas", cpu=30.0).cpu_req_int()
    assert group.mem_req_int == bi("x", "seneca-sas", memory=80.0).mem_req_int()
    assert group.net_req_int == bi("x", "seneca-sas", network=100.0).net_req_int()


# ---------------------------------------------------------------------------
# The cluster-group boundary
# ---------------------------------------------------------------------------


def test_same_bundle_on_different_cluster_groups_is_not_merged():
    groups = build([bi("foo", "seneca-sas"), bi("foo", "hahn")])

    assert sorted(clusters_of(g) for g in groups) == [["hahn"], ["seneca-sas"]]


def test_unlisted_clusters_are_never_grouped_with_each_other():
    clusters = ["hahn", "arnold", "kolmogorov", "markov"]
    groups = build([bi("foo", c) for c in clusters])

    assert sorted(g.cluster_group for g in groups) == sorted(clusters)


def test_requirements_are_not_unified_across_cluster_groups():
    groups = build(
        [
            bi("foo", "seneca-sas", cpu=10.0),
            bi("foo", "hahn", cpu=100.0),
        ]
    )

    by_group = {g.cluster_group: g for g in groups}
    assert by_group["senecas"].cpu_req_int == cpu_req_of(10.0)
    assert by_group["hahn"].cpu_req_int == cpu_req_of(100.0)


def test_divergence_on_another_cluster_group_does_not_split_senecas():
    # Counts differ by 2 inside seneca (consistent), but hahn is 10x bigger.
    # Inconsistency is judged per cluster group, so hahn does not drag the
    # senecas through the inconsistent path and into a shared group with it.
    groups = build(
        [
            bi("foo", "seneca-sas", count=4),
            bi("foo", "seneca-vla", count=6),
            bi("foo", "seneca-klg", count=5),
            bi("foo", "hahn", count=40),
        ]
    )

    by_group = {g.cluster_group: g for g in groups}
    assert clusters_of(by_group["senecas"]) == sorted(SENECAS)
    assert clusters_of(by_group["hahn"]) == ["hahn"]


def test_merge_refuses_groups_from_different_cluster_groups():
    seneca_group, hahn_group = build([bi("foo", "seneca-sas"), bi("foo", "hahn")])

    with pytest.raises(ValueError, match="different cluster groups"):
        BundleGroup.merge([seneca_group, hahn_group])
