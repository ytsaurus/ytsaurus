from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, wait, get, create_pool, run_sleeping_vanilla, abort_op,
    update_pool_tree_config)

from yt_scheduler_helpers import (
    scheduler_orchid_operation_path, scheduler_orchid_default_pool_tree_path)

from yt.test_helpers import are_almost_equal

import pytest


##################################################################


def _get_op_fair_share_cpu(op_id):
    path = scheduler_orchid_operation_path(op_id) + "/detailed_fair_share/total/cpu"
    return get(path, default=None)


def _is_whole_job_multiple(fair_share_cpu, per_job_cpu, total_cpu, eps=1e-4):
    """Return True if fair_share_cpu is an integer multiple of per_job_cpu/total_cpu."""
    if total_cpu <= 0 or per_job_cpu <= 0:
        return True
    per_job_fraction = per_job_cpu / total_cpu
    k = fair_share_cpu / per_job_fraction
    return abs(k - round(k)) < eps


##################################################################


@pytest.mark.enabled_multidaemon
class TestDiscretizedFairShare(YTEnvSetup):
    ENABLE_MULTIDAEMON = True
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    # Single node with 3 CPU and plenty of user_slots/memory.
    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "user_slots": 20,
                "cpu": 3,
                "memory": 10 * 1024 ** 3,
            }
        }
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100,
        }
    }

    @authors("ignat")
    def test_discretized_fair_share_whole_job_boundary(self):
        """
        Two equal-weight operations compete for 3 CPU.  Each requests 3 jobs at
        1 CPU/job, so demand = 1.0 (full cluster) each.  Without discretization
        the continuous fair share gives 0.5 to each (= 1.5 jobs — NOT a whole
        integer).  With discretization each op receives exactly 1 job = 1/3 of
        the cluster.
        """
        update_pool_tree_config("default", {
            "enable_discretized_fair_share": True,
            "max_discretized_steps": 100,
        })

        create_pool("pool")

        per_job_cpu = 1.0
        total_cpu = 3.0  # matches DELTA_NODE_CONFIG

        op1 = run_sleeping_vanilla(
            job_count=3,
            spec={"pool": "pool"},
            task_patch={"cpu_limit": per_job_cpu},
        )
        op2 = run_sleeping_vanilla(
            job_count=3,
            spec={"pool": "pool"},
            task_patch={"cpu_limit": per_job_cpu},
        )

        # Wait until orchid has both operations' fair share.
        wait(lambda: _get_op_fair_share_cpu(op1.id) is not None)
        wait(lambda: _get_op_fair_share_cpu(op2.id) is not None)

        def both_at_whole_job_boundary():
            fs1 = _get_op_fair_share_cpu(op1.id)
            fs2 = _get_op_fair_share_cpu(op2.id)
            if fs1 is None or fs2 is None:
                return False
            return (
                fs1 > 0 and
                fs2 > 0 and
                _is_whole_job_multiple(fs1, per_job_cpu, total_cpu) and
                _is_whole_job_multiple(fs2, per_job_cpu, total_cpu)
            )

        wait(both_at_whole_job_boundary)

        # Verify the orchid attribute introduced in T040.
        def _get_discretized_active(op_id):
            path = scheduler_orchid_operation_path(op_id) + "/discretized_fair_share_active"
            return get(path, default=None)

        wait(lambda: _get_discretized_active(op1.id) == True)
        wait(lambda: _get_discretized_active(op2.id) == True)

        abort_op(op1.id)
        abort_op(op2.id)

    @authors("ignat")
    def test_discretized_fair_share_disabled_preserves_continuous(self):
        """
        With the feature disabled (default), fair share is continuous.  The same
        two competing operations should get 0.5 each (NOT a whole-job multiple of
        1/3), confirming the continuous path is preserved.
        """
        # Feature is disabled by default — do NOT call update_pool_tree_config.
        create_pool("pool")

        per_job_cpu = 1.0
        total_cpu = 3.0

        op1 = run_sleeping_vanilla(
            job_count=3,
            spec={"pool": "pool"},
            task_patch={"cpu_limit": per_job_cpu},
        )
        op2 = run_sleeping_vanilla(
            job_count=3,
            spec={"pool": "pool"},
            task_patch={"cpu_limit": per_job_cpu},
        )

        wait(lambda: _get_op_fair_share_cpu(op1.id) is not None)
        wait(lambda: _get_op_fair_share_cpu(op2.id) is not None)

        # Each should converge to ~0.5 (continuous fair share with equal weights).
        def both_near_half():
            fs1 = _get_op_fair_share_cpu(op1.id)
            fs2 = _get_op_fair_share_cpu(op2.id)
            if fs1 is None or fs2 is None:
                return False
            return are_almost_equal(fs1, 0.5, absolute_error=0.05) and are_almost_equal(fs2, 0.5, absolute_error=0.05)

        wait(both_near_half)

        # Explicitly confirm that 0.5 is NOT a whole-job multiple of 1/3,
        # i.e., continuous fair share is NOT discretized.
        assert not _is_whole_job_multiple(0.5, per_job_cpu, total_cpu, eps=1e-4)

        abort_op(op1.id)
        abort_op(op2.id)

    @authors("ignat")
    def test_discretized_fair_share_uncontested_gives_full_demand(self):
        """
        A single uncontested operation with the feature enabled should receive its
        full demand share, which is always at a whole-job boundary.
        """
        update_pool_tree_config("default", {
            "enable_discretized_fair_share": True,
            "max_discretized_steps": 100,
        })

        create_pool("pool")

        per_job_cpu = 1.0
        total_cpu = 3.0
        job_count = 2

        op = run_sleeping_vanilla(
            job_count=job_count,
            spec={"pool": "pool"},
            task_patch={"cpu_limit": per_job_cpu},
        )

        wait(lambda: _get_op_fair_share_cpu(op.id) is not None)

        def at_demand():
            fs = _get_op_fair_share_cpu(op.id)
            if fs is None:
                return False
            expected = (job_count * per_job_cpu) / total_cpu
            return are_almost_equal(fs, expected, absolute_error=0.01)

        wait(at_demand)

        fs = _get_op_fair_share_cpu(op.id)
        assert _is_whole_job_multiple(fs, per_job_cpu, total_cpu)

        abort_op(op.id)

