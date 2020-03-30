import pytest

from yt_env_setup import YTEnvSetup, Restarter, SCHEDULERS_SERVICE, CONTROLLER_AGENTS_SERVICE, NODES_SERVICE, MASTERS_SERVICE

from yt_commands import *

from copy import deepcopy

import __builtin__

################################################################################

class TestMasterCellAddition(YTEnvSetup):
    NUM_SECONDARY_MASTER_CELLS = 3
    START_SECONDARY_MASTER_CELLS = False
    DELTA_MASTER_CONFIG = {
        "world_initializer": {
            "update_period": 1000,
        },
    }

    NUM_NODES = 3
    DEFER_NODE_START = True

    NUM_SCHEDULERS = 1
    DEFER_SCHEDULER_START = True

    DEFER_CONTROLLER_AGENT_START = True
    NUM_CONTROLLER_AGENTS = 1

    PATCHED_CONFIGS = []
    STASHED_CELL_CONFIGS = []
    CELL_IDS = __builtin__.set()

    @classmethod
    def setup_class(cls):
        super(TestMasterCellAddition, cls).setup_class()
        # NB: the last secondary cell is not started here.
        for cell_index in xrange(1, cls.NUM_SECONDARY_MASTER_CELLS):
            cls.Env.start_master_cell(cell_index)

        cls.Env.start_nodes()
        cls.Env.start_schedulers()
        cls.Env.start_controller_agents()

    @classmethod
    def _disable_last_cell_and_stash_config(cls, config):
        # Stash the last cell's config and remove it for the time being.
        cls.STASHED_CELL_CONFIGS.append(deepcopy(config["secondary_masters"][2]))
        del config["secondary_masters"][2]
        cls.PATCHED_CONFIGS.append(config)

        cls.CELL_IDS.add(config["primary_master"]["cell_id"])
        for secondary_master in config["secondary_masters"]:
            cls.CELL_IDS.add(secondary_master["cell_id"])

        assert len(cls.PATCHED_CONFIGS) == len(cls.STASHED_CELL_CONFIGS)

    @classmethod
    def _enable_last_cell(cls):
        assert len(cls.PATCHED_CONFIGS) == len(cls.STASHED_CELL_CONFIGS)

        with Restarter(cls.Env, [SCHEDULERS_SERVICE, CONTROLLER_AGENTS_SERVICE, NODES_SERVICE]):
            abort_all_transactions()

            for cell_id in cls.CELL_IDS:
                build_snapshot(cell_id=cell_id, set_read_only=True)

            assert get("//sys/topmost_transactions/@count") == 0

            with Restarter(cls.Env, MASTERS_SERVICE):
                for i in xrange(len(cls.PATCHED_CONFIGS)):
                    cls.PATCHED_CONFIGS[i]["secondary_masters"].append(cls.STASHED_CELL_CONFIGS[i])

                cls.Env.rewrite_master_configs()

            assert get("//sys/topmost_transactions/@count") == 0

            cls.Env.rewrite_node_configs()
            cls.Env.rewrite_scheduler_configs()
            cls.Env.rewrite_controller_agent_configs()

    @classmethod
    def modify_master_config(cls, config, index):
        cls._disable_last_cell_and_stash_config(config)

    @classmethod
    def modify_scheduler_config(cls, config):
        cls._disable_last_cell_and_stash_config(config["cluster_connection"])

    @classmethod
    def modify_controller_agent_config(cls, config):
        cls._disable_last_cell_and_stash_config(config["cluster_connection"])

    @classmethod
    def modify_node_config(cls, config):
        cls._disable_last_cell_and_stash_config(config["cluster_connection"])

    def _do_for_cell(self, cell_index, callback):
        return callback(get_driver(cell_index))

    def check_accounts(self):
        create_account("acc_sync_create")

        create_account("acc_async_remove")
        create("table", "//tmp/t", attributes={"account": "acc_async_remove", "external_cell_tag": 1})

        create_account("acc_sync_remove")
        remove_account("acc_sync_remove")

        create_account("acc_async_create", sync_creation=False)

        remove_account("acc_async_remove", sync_deletion=False)

        yield

        assert_true_for_secondary_cells(self.Env,
            lambda driver: not exists("//sys/accounts/acc_sync_remove", driver=driver))
        assert not exists("//sys/accounts/acc_sync_remove")

        assert_true_for_secondary_cells(self.Env,
            lambda driver: get("//sys/accounts/acc_sync_create/@life_stage", driver=driver) == "creation_committed")
        assert get("//sys/accounts/acc_sync_create/@life_stage") == "creation_committed"

        wait(lambda: get("//sys/accounts/acc_async_create/@life_stage") == "creation_committed")
        assert_true_for_secondary_cells(self.Env,
            lambda driver: get("//sys/accounts/acc_async_create/@life_stage", driver=driver) == "creation_committed")

        assert get("//sys/accounts/acc_async_remove/@life_stage") == "removal_started"
        wait(lambda: self._do_for_cell(1, lambda driver: get("//sys/accounts/acc_async_remove/@life_stage", driver=driver)) == "removal_started")
        wait(lambda: self._do_for_cell(2, lambda driver: get("//sys/accounts/acc_async_remove/@life_stage", driver=driver)) == "removal_pre_committed")
        wait(lambda: self._do_for_cell(3, lambda driver: get("//sys/accounts/acc_async_remove/@life_stage", driver=driver)) == "removal_pre_committed")

        remove("//tmp/t")

        wait(lambda: not exists("//sys/accounts/acc_async_remove"))
        assert_true_for_secondary_cells(self.Env,
            lambda driver: not exists("//sys/accounts/acc_async_remove", driver=driver))

    def check_sys_masters_node(self):
        def check(cell_ids):
            secondary_masters = get("//sys/secondary_masters")
            if sorted(secondary_masters.keys()) != sorted(cell_ids):
                return False

            for cell_id in cell_ids:
                assert len(secondary_masters[cell_id]) == 3

            return True

        assert check(['1', '2'])

        yield

        wait(lambda: check(['1', '2', '3']))

    @authors("shakurov")
    def test_add_new_cell(self):
        CHECKER_LIST = [
            self.check_accounts,
            self.check_sys_masters_node,
        ]

        checker_state_list = [iter(c()) for c in CHECKER_LIST]
        for s in checker_state_list:
            next(s)

        TestMasterCellAddition._enable_last_cell()

        for s in checker_state_list:
            with pytest.raises(StopIteration):
                next(s)
