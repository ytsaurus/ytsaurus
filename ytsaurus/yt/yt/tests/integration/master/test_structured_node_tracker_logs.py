from yt_env_setup import YTEnvSetup, Restarter, NODES_SERVICE

from yt_commands import (
    authors, ls, get, wait, set,
    get_active_primary_master_leader_address,
    get_active_primary_master_follower_address)

from yt_helpers import read_structured_log, write_log_barrier

import os

##################################################################


@authors("kvk1920")
class TestNodeTrackerLog(YTEnvSetup):
    LOG_WRITE_WAIT_TIME = 0.2

    @classmethod
    def modify_master_config(cls, config, tag, index):
        config["logging"]["flush_period"] = 50
        config["logging"]["rules"].append(
            {
                "min_level": "debug",
                "writers": ["node_tracker"],
                "include_categories": ["NodeTrackerServerStructured", "Barrier"],
                "message_format": "structured",
            }
        )
        config["logging"]["writers"]["node_tracker"] = {
            "type": "file",
            "file_name": os.path.join(cls.path_to_run, "logs/master-{}-{}.node_tracker.json.log".format(tag, index)),
            "accepted_message_format": "structured",
        }

    def _get_structured_log_path(self, peer_tag):
        return self.path_to_run + "/logs/master-10-{}.node_tracker.json.log".format(peer_tag)

    def _get_logs(self, peer_tag, from_barrier=None):
        return list(read_structured_log(
            self._get_structured_log_path(peer_tag),
            from_barrier=from_barrier
        ))

    def _get_primary_master_peer_tag(self, address):
        addresses = self.Env.configs["master"][0]["primary_master"]["addresses"]
        return addresses.index(address)

    def _get_grouped_log(self, peer_tag, from_barrier=None):
        log = self._get_logs(peer_tag, from_barrier)
        node_to_records = {}
        for event in log:
            node_to_records.setdefault(event["node_address"], [])
            node_to_records[event["node_address"]].append(event)
        return node_to_records

    def _sleep_until_all_nodes_are_online(self):
        def check_all_nodes_are_online():
            for node in ls("//sys/cluster_nodes"):
                for state_per_cell in get("//sys/cluster_nodes/{}/@multicell_states".format(node)).values():
                    if state_per_cell != "online":
                        return False
            return True
        wait(check_all_nodes_are_online)

    def test_simple(self):
        set("//sys/@config/node_tracker/enable_structured_log", True)
        # NB: Restart nodes in order to see node events after node tracker log is enabled.
        with Restarter(self.Env, NODES_SERVICE):
            pass

        follower_address = get_active_primary_master_follower_address(self)
        follower_peer_tag = self._get_primary_master_peer_tag(follower_address)

        def check_all_nodes_have_state(states, from_barrier=None):
            grouped_log = self._get_grouped_log(follower_peer_tag, from_barrier)
            for node in ls("//sys/cluster_nodes"):
                if node not in grouped_log:
                    return False
                is_good_state = False
                for event in grouped_log[node]:
                    is_good_state |= event["node_state"] in states
                if not is_good_state:
                    return False
            return True

        wait(lambda: check_all_nodes_have_state({"online"}))

        from_barrier = write_log_barrier(follower_address)

        with Restarter(self.Env, NODES_SERVICE):
            pass

        wait(lambda: check_all_nodes_have_state({"mixed", "offline"}, from_barrier))

    def test_no_logs_on_leader(self):
        set("//sys/@config/node_tracker/enable_structured_log", True)
        with Restarter(self.Env, NODES_SERVICE):
            pass

        self._sleep_until_all_nodes_are_online()

        leader_address = get_active_primary_master_leader_address(self)
        leader_peer_tag = self._get_primary_master_peer_tag(leader_address)

        assert not self._get_grouped_log(leader_peer_tag)

        with Restarter(self.Env, NODES_SERVICE):
            pass

        self._sleep_until_all_nodes_are_online()

        assert not self._get_grouped_log(leader_peer_tag)
