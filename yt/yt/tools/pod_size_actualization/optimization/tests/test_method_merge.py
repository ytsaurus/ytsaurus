"""Tests for merging methods in load_bundle_data.

Потребление берётся как максимум по методам, конфигурация — из последнего метода.

Run with:
    ya make -t yt/yt/tools/pod_size_actualization/optimization/tests
"""

import contextlib
import io

import pandas as pd
import pytest

from yt.yt.tools.pod_size_actualization.optimization import data

CLUSTER = "seneca-sas"

NODE_SPECS = pd.DataFrame(
    [
        {
            "cluster": CLUSTER,
            "container_type": "medium",
            "cpu_cores": 100.0,
            "memory_bytes": 100 * 2**30,
            "net_bytes": 1000 * 2**20,
        },
        {
            "cluster": CLUSTER,
            "container_type": "large",
            "cpu_cores": 100.0,
            "memory_bytes": 100 * 2**30,
            "net_bytes": 1000 * 2**20,
        },
    ]
)

RPC_SPECS = pd.DataFrame(
    [
        {
            "cluster": CLUSTER,
            "container_type": "heavy",
            "cpu_cores": 100.0,
            "memory_bytes": 100 * 2**30,
            "net_bytes": 1000 * 2**20,
        }
    ]
)


def write_method(tmp_path, method, *, cpu_usage, count=4, node_type="medium"):
    """CSV одного метода: один бандл, только нодовая часть."""
    path = tmp_path / f"{method}.csv"
    pd.DataFrame(
        [
            {
                "cluster": CLUSTER,
                "bundle": "ads",
                "method_name": method,
                "node_type": node_type,
                "node_count": count,
                "node_cpu_total_p75": cpu_usage,
                "node_anon_memory_p75": 10.0,
                "node_net_tx_p75": 1.0,
                "node_net_rx_p75": 1.0,
                "rpc_type": None,
                "rpc_count": 0,
                "rpc_cpu_total_p75": None,
                "rpc_anon_memory_p75": None,
            }
        ]
    ).to_csv(path, index=False)
    return str(path)


def load(methods_and_paths, fail_on_method_mismatch=False):
    """load_bundle_data() без её диагностического вывода."""
    with contextlib.redirect_stdout(io.StringIO()):
        return data.load_bundle_data(
            [CLUSTER],
            list(methods_and_paths),
            NODE_SPECS,
            RPC_SPECS,
            bundle_file_paths=dict(methods_and_paths),
            fail_on_method_mismatch=fail_on_method_mismatch,
        )


# ---------------------------------------------------------------------------
# Максимум по методам
# ---------------------------------------------------------------------------


def test_consumption_is_the_max_across_methods(tmp_path):
    bundles = load(
        {
            "old": write_method(tmp_path, "old", cpu_usage=30.0),
            "new": write_method(tmp_path, "new", cpu_usage=10.0),
        }
    )
    assert [b.cpu for b in bundles] == [30.0]


# ---------------------------------------------------------------------------
# Эталон конфигурации — последний метод
# ---------------------------------------------------------------------------


def test_config_is_taken_from_the_last_method(tmp_path):
    bundles = load(
        {
            "old": write_method(tmp_path, "old", cpu_usage=30.0, count=2),
            "new": write_method(tmp_path, "new", cpu_usage=10.0, count=4),
        }
    )
    assert [b.count for b in bundles] == [4]


def test_methods_before_a_config_change_do_not_reach_the_max(tmp_path):
    bundles = load(
        {
            "old": write_method(tmp_path, "old", cpu_usage=30.0, count=2),
            "new": write_method(tmp_path, "new", cpu_usage=10.0, count=4),
        }
    )
    assert [b.cpu for b in bundles] == [10.0]


def test_a_changed_container_type_cuts_the_tail_the_same_way(tmp_path):
    bundles = load(
        {
            "old": write_method(tmp_path, "old", cpu_usage=30.0, node_type="large"),
            "new": write_method(tmp_path, "new", cpu_usage=10.0, node_type="medium"),
        }
    )
    assert [(b.node_type, b.cpu) for b in bundles] == [("medium", 10.0)]


def test_the_max_covers_every_method_matching_the_reference_config(tmp_path):
    # Не только последний: 25 приходит из middle, отброшен лишь oldest с count=2.
    bundles = load(
        {
            "oldest": write_method(tmp_path, "oldest", cpu_usage=99.0, count=2),
            "middle": write_method(tmp_path, "middle", cpu_usage=25.0, count=4),
            "newest": write_method(tmp_path, "newest", cpu_usage=10.0, count=4),
        }
    )
    assert [(b.count, b.cpu) for b in bundles] == [(4, 25.0)]


# ---------------------------------------------------------------------------
# Строгий режим
# ---------------------------------------------------------------------------


def test_strict_mode_rejects_a_config_change_between_methods(tmp_path):
    with pytest.raises(ValueError, match="differs across methods"):
        load(
            {
                "old": write_method(tmp_path, "old", cpu_usage=30.0, count=2),
                "new": write_method(tmp_path, "new", cpu_usage=10.0, count=4),
            },
            fail_on_method_mismatch=True,
        )


def test_strict_mode_passes_when_methods_agree(tmp_path):
    bundles = load(
        {
            "old": write_method(tmp_path, "old", cpu_usage=30.0, count=4),
            "new": write_method(tmp_path, "new", cpu_usage=10.0, count=4),
        },
        fail_on_method_mismatch=True,
    )
    assert [(b.count, b.cpu) for b in bundles] == [(4, 30.0)]
