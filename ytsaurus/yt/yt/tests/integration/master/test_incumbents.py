from yt_env_setup import (YTEnvSetup, Restarter, MASTERS_SERVICE)

from yt.common import YtError

from yt_commands import (authors, wait, get, set, ls)

##################################################################


class TestIncumbents(YTEnvSetup):
    NUM_MASTERS = 5

    def _get_orchid(self, master):
        return get(f"//sys/primary_masters/{master}/orchid/incumbent_manager")

    def _get_leader_address(self):
        for master in ls("//sys/primary_masters"):
            address = f"//sys/primary_masters/{master}/orchid"
            if get(f"{address}/monitoring/hydra/state") == "leading":
                return master

    @authors("gritukan")
    def test_distribution(self):
        set("//sys/@config/incumbent_manager", {
            "scheduler": {
                "incumbents": {
                    "chunk_replicator": {
                        "use_followers": True,
                        "weight": 10**6,
                    }
                }
            },
            "peer_lease_duration": 1000,
            "peer_grace_period": 2000,
            "banned_peers": [],
        })

        # Recreate leases with new durations.
        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        def check_up():
            try:
                for master in ls("//sys/primary_masters"):
                    address = f"//sys/primary_masters/{master}/orchid"
                    get(f"{address}/monitoring/hydra/state")
            except YtError:
                return False
            return True
        wait(check_up)

        leader = self._get_leader_address()
        followers = ls("//sys/primary_masters")
        followers.remove(leader)

        def check_ok():
            shards = self._get_orchid(self._get_leader_address())["target_state"]["chunk_replicator"]["addresses"]
            shards_per_peer = {}
            alive_followers = ls("//sys/primary_masters")
            alive_followers.remove(leader)
            banned_peers = get("//sys/@config/incumbent_manager/banned_peers")
            for peer_to_fail in banned_peers:
                alive_followers.remove(peer_to_fail)

            for shard in shards:
                if shard not in alive_followers:
                    return False
                if shard in shards_per_peer:
                    shards_per_peer[shard] += 1
                else:
                    shards_per_peer[shard] = 1

            for peer, counter in shards_per_peer.items():
                if counter != 60 / len(alive_followers):
                    return False

            for peer in ls("//sys/primary_masters"):
                if peer in banned_peers:
                    continue
                if self._get_orchid(peer)["local_state"]["chunk_replicator"]["addresses"] != shards:
                    return False
            return True

        wait(check_ok)

        set("//sys/@config/incumbent_manager/banned_peers", followers[0:1])
        wait(check_ok)

        set("//sys/@config/incumbent_manager/banned_peers", followers[0:2])
        wait(check_ok)

        set("//sys/@config/incumbent_manager/banned_peers", followers[0:3])
        wait(check_ok)

        set("//sys/@config/incumbent_manager/banned_peers", followers[3:4])
        wait(check_ok)
