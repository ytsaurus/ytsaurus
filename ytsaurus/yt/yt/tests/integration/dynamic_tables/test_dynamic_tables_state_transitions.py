from yt_dynamic_tables_base import DynamicTablesBase

from yt_env_setup import Restarter, NODES_SERVICE

from yt_commands import (
    authors, wait, get, mount_table, unmount_table, freeze_table, unfreeze_table,
    wait_for_tablet_state, sync_create_cells, create,
    sync_mount_table)

from yt.common import YtError
import yt.yson as yson

import random
from time import sleep

import pytest

##################################################################


class TestDynamicTableStateTransitions(DynamicTablesBase):
    NUM_TEST_PARTITIONS = 5

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "tablet_manager": {
            "leader_reassignment_timeout": 2000,
            "peer_revocation_timeout": 600000,
        }
    }

    DELTA_NODE_CONFIG = {
        "tablet_node": {
            "tablet_manager": {
                "sleep_before_post_to_master": 400,
            }
        }
    }

    def _get_expected_state(self, initial, first_command, second_command):
        M = "mounted"
        F = "frozen"
        E = "error"
        U = "unmounted"

        expected = {
            "mounted": {
                "mount": {
                    "mount": M,
                    "frozen_mount": E,
                    "unmount": U,
                    "freeze": F,
                    "unfreeze": M,
                },
                # frozen_mount
                "unmount": {
                    "mount": E,
                    "frozen_mount": E,
                    "unmount": U,
                    "freeze": E,
                    "unfreeze": E,
                },
                "freeze": {
                    "mount": E,
                    "frozen_mount": F,
                    "unmount": U,
                    "freeze": F,
                    "unfreeze": E,
                },
                "unfreeze": {
                    "mount": M,
                    "frozen_mount": E,
                    "unmount": U,
                    "freeze": F,
                    "unfreeze": M,
                },
            },
            "frozen": {
                # mount
                "frozen_mount": {
                    "mount": E,
                    "frozen_mount": F,
                    "unmount": U,
                    "freeze": F,
                    "unfreeze": M,
                },
                "unmount": {
                    "mount": E,
                    "frozen_mount": E,
                    "unmount": U,
                    "freeze": E,
                    "unfreeze": E,
                },
                "freeze": {
                    "mount": E,
                    "frozen_mount": F,
                    "unmount": U,
                    "freeze": F,
                    "unfreeze": M,
                },
                "unfreeze": {
                    "mount": M,
                    "frozen_mount": E,
                    "unmount": E,
                    "freeze": E,
                    "unfreeze": M,
                },
            },
            "unmounted": {
                "mount": {
                    "mount": M,
                    "frozen_mount": E,
                    "unmount": E,
                    "freeze": E,
                    "unfreeze": E,
                },
                "frozen_mount": {
                    "mount": E,
                    "frozen_mount": F,
                    "unmount": E,
                    "freeze": F,
                    "unfreeze": E,
                },
                "unmount": {
                    "mount": M,
                    "frozen_mount": F,
                    "unmount": U,
                    "freeze": E,
                    "unfreeze": E,
                },
                # freeze
                # unfreeze
            },
        }
        return expected[initial][first_command][second_command]

    def _create_cell(self):
        self._cell_id = sync_create_cells(1)[0]

    def _get_callback(self, command):
        callbacks = {
            "mount": lambda x: mount_table(x, cell_id=self._cell_id),
            "frozen_mount": lambda x: mount_table(x, cell_id=self._cell_id, freeze=True),
            "unmount": lambda x: unmount_table(x),
            "freeze": lambda x: freeze_table(x),
            "unfreeze": lambda x: unfreeze_table(x),
        }
        return callbacks[command]

    @pytest.mark.parametrize(
        ["initial", "command"],
        [
            ["mounted", "frozen_mount"],
            ["frozen", "mount"],
            ["unmounted", "freeze"],
            ["unmounted", "unfreeze"],
        ],
    )
    @authors("savrus")
    def test_initial_incompatible(self, initial, command):
        self._create_cell()
        self._create_sorted_table("//tmp/t")

        if initial == "mounted":
            sync_mount_table("//tmp/t")
        elif initial == "frozen":
            sync_mount_table("//tmp/t", freeze=True)

        with pytest.raises(YtError):
            self._get_callback(command)("//tmp/t")

    def _do_test_transition(self, initial, first_command, second_command):
        expected = self._get_expected_state(initial, first_command, second_command)
        if expected == "error":
            with Restarter(self.Env, NODES_SERVICE):
                self._get_callback(first_command)("//tmp/t")
                with pytest.raises(YtError):
                    self._get_callback(second_command)("//tmp/t")
        else:
            self._get_callback(first_command)("//tmp/t")
            wait(lambda: get("//tmp/t/@tablet_state") in ["mounted", "unmounted", "frozen"])
            self._get_callback(second_command)("//tmp/t")
            wait(lambda: get("//tmp/t/@tablet_state") == expected)
        wait(lambda: get("//tmp/t/@tablet_state") != "transient")

    @authors("savrus", "levysotsky")
    @pytest.mark.parametrize("second_command", ["mount", "frozen_mount", "unmount", "freeze", "unfreeze"])
    @pytest.mark.parametrize("first_command", ["mount", "unmount", "freeze", "unfreeze"])
    def test_state_transition_conflict_mounted(self, first_command, second_command):
        self._create_cell()
        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t", cell_id=self._cell_id)
        self._do_test_transition("mounted", first_command, second_command)

    @authors("savrus", "levysotsky")
    @pytest.mark.parametrize("second_command", ["mount", "frozen_mount", "unmount", "freeze", "unfreeze"])
    @pytest.mark.parametrize("first_command", ["frozen_mount", "unmount", "freeze", "unfreeze"])
    def test_state_transition_conflict_frozen(self, first_command, second_command):
        self._create_cell()
        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t", cell_id=self._cell_id, freeze=True)
        self._do_test_transition("frozen", first_command, second_command)

    @authors("savrus")
    @pytest.mark.parametrize("second_command", ["mount", "frozen_mount", "unmount", "freeze", "unfreeze"])
    @pytest.mark.parametrize("first_command", ["mount", "frozen_mount", "unmount"])
    def test_state_transition_conflict_unmounted(self, first_command, second_command):
        self._create_cell()
        self._create_sorted_table("//tmp/t")
        self._do_test_transition("unmounted", first_command, second_command)

    @authors("savrus")
    @pytest.mark.parametrize("inverse", [False, True])
    def test_freeze_expectations(self, inverse):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t", pivot_keys=[[], [1]])
        sync_mount_table("//tmp/t", first_tablet_index=0, last_tablet_index=0)

        callbacks = [
            lambda: freeze_table("//tmp/t", first_tablet_index=0, last_tablet_index=0),
            lambda: mount_table("//tmp/t", first_tablet_index=1, last_tablet_index=1, freeze=True),
        ]

        for callback in reversed(callbacks) if inverse else callbacks:
            callback()

        wait_for_tablet_state("//tmp/t", "frozen")
        wait(lambda: get("//tmp/t/@tablet_state") != "transient")
        assert get("//tmp/t/@expected_tablet_state") == "frozen"

    @authors("ifsmirnov")
    def test_stress_transitions(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")

        def _force_create_table():
            schema = yson.YsonList([
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ])
            attributes = {"schema": schema, "dynamic": True}
            create("table", "//tmp/t", attributes=attributes, force=True)

        callbacks = [
            lambda: mount_table("//tmp/t", freeze=False),
            lambda: mount_table("//tmp/t", freeze=True),
            lambda: unmount_table("//tmp/t", force=False),
            lambda: freeze_table("//tmp/t"),
            lambda: unfreeze_table("//tmp/t"),
            _force_create_table,

            # NB. Forced unmount can currently lead to alerts. See explanation at
            # master/tablet_manager.cpp::HydraOnTabletMounted.
            #  lambda: unmount_table("//tmp/t", force=True),
        ]

        random.seed(1234)

        for i in range(200):
            sleep(random.choice((0, .100, .200, .500)))
            try:
                random.choice(callbacks)()
            except YtError:
                pass


##################################################################


class TestDynamicTableStateTransitionsMulticell(TestDynamicTableStateTransitions):
    NUM_SECONDARY_MASTER_CELLS = 2


class TestDynamicTableStateTransitionsPortal(TestDynamicTableStateTransitionsMulticell):
    ENABLE_TMP_PORTAL = True
