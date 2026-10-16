"""Tests for cross-dc clusters: instances split across availability zones.

Инстансы бандла поровну лежат в нескольких ДЦ, и активны не все ДЦ сразу. Размер
подбирается по одной зоне, а число инстансов наружу отдаётся умноженным на число
зон — так рекомендация остаётся кратной числу ДЦ.

Run with:
    ya make -t yt/yt/tools/pod_size_actualization/optimization/tests
"""

import contextlib
import io

import pandas as pd
import pytest

from yt.yt.tools.pod_size_actualization.optimization import data, simple

CLUSTER = "markov"
ZONES = 3
NODE_SIZES = [(4.0, 20.0, 160.0), (14.0, 100.0, 320.0)]
PROXY_SIZES = [(9.0, 20.0, 250.0)]
COEFFICIENTS = {"a": 1.0, "b": 1.0, "c": 1.0}

NODE_SPECS = pd.DataFrame(
    [
        {
            "cluster": CLUSTER,
            "container_type": "medium",
            "cpu_cores": 14.0,
            "memory_bytes": 100 * 2**30,
            "net_bytes": 320 * 2**20,
        }
    ]
)
RPC_SPECS = pd.DataFrame(
    [
        {
            "cluster": CLUSTER,
            "container_type": "medium",
            "cpu_cores": 9.0,
            "memory_bytes": 20 * 2**30,
            "net_bytes": 250 * 2**20,
        }
    ]
)


def bundle_csv(tmp_path, node_count, cpu_usage=1.0, zones=ZONES, cluster=CLUSTER):
    path = tmp_path / f"metrics_{cluster}.csv"
    pd.DataFrame(
        [
            {
                "cluster": cluster,
                "bundle": "ads",
                "method_name": "period_0",
                "availability_zones": zones,
                "node_type": "medium",
                "node_count": node_count,
                "node_cpu_total_p75": cpu_usage,
                "node_anon_memory_p75": 5.0,
                "node_net_tx_p75": 1.0,
                "node_net_rx_p75": 1.0,
                "rpc_type": "medium",
                "rpc_count": node_count,
                "proxy_cpu_total_p75": 1.0,
                "proxy_anon_memory_p75": 2.0,
                "proxy_net_tx_p75": 1.0,
                "proxy_net_rx_p75": 1.0,
            }
        ]
    ).to_csv(path, index=False)
    return str(path)


def load(tmp_path, node_count, cpu_usage=1.0):
    with contextlib.redirect_stdout(io.StringIO()):
        return data.load_bundle_data(
            [CLUSTER],
            ["period_0"],
            NODE_SPECS,
            RPC_SPECS,
            bundle_file_paths={"period_0": bundle_csv(tmp_path, node_count, cpu_usage)},
        )


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


def test_counts_are_split_across_zones(tmp_path):
    instances = load(tmp_path, node_count=12)
    assert {bi.count for bi in instances} == {4}
    assert {bi.total_count for bi in instances} == {12}


def test_base_count_stays_the_whole_bundle(tmp_path):
    row = assign(load(tmp_path, node_count=12)).iloc[0]
    assert row["BaseCount"] == 12


def test_recommended_count_is_a_multiple_of_the_zone_count(tmp_path):
    assign_df = assign(load(tmp_path, node_count=12, cpu_usage=0.1))
    assert (assign_df["NewCount"] % ZONES == 0).all()
    assert (assign_df["NewCount"] > 0).all()


def test_a_count_not_divisible_by_the_zone_count_is_an_error(tmp_path):
    # Такого в спек-логе быть не должно: по инстансу на ДЦ — минимальная конфигурация.
    with pytest.raises(ValueError, match="not divisible by 3"):
        load(tmp_path, node_count=4)


def test_a_single_zone_cluster_keeps_its_counts(tmp_path):
    # Тот же бандл на обычном кластере: делить нечего.
    path = bundle_csv(tmp_path, node_count=12, zones=1, cluster="hahn")
    specs = NODE_SPECS.assign(cluster="hahn")
    rpc_specs = RPC_SPECS.assign(cluster="hahn")
    with contextlib.redirect_stdout(io.StringIO()):
        instances = data.load_bundle_data(
            ["hahn"],
            ["period_0"],
            specs,
            rpc_specs,
            bundle_file_paths={"period_0": path},
        )
    assert {bi.count for bi in instances} == {12}
    assert {bi.total_count for bi in instances} == {12}
