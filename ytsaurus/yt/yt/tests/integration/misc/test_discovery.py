from yt_env_setup import YTEnvSetup

from yt_commands import authors, wait, get, set, ls, exists, create_user

from yt.common import YtError

##################################################################


class TestDiscovery(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SECONDARY_MASTER_CELLS = 1

    @authors("aleksandra-zh")
    def test_master_discovery_orchid(self):
        master = ls("//sys/primary_masters")[0]
        assert exists("//sys/primary_masters/{0}/orchid/discovery_server".format(master))


class TestDiscoveryServers(YTEnvSetup):
    NUM_DISCOVERY_SERVERS = 5

    @authors("aleksandra-zh")
    def test_discovery_servers(self):
        set("//sys/@config/security_manager/enable_distributed_throttler", True)

        create_user("u")
        set("//sys/users/u/@request_limits/read_request_rate/default", 1000)
        for _ in range(10):
            ls("//sys", authenticated_user="u")

    @authors("aleksandra-zh")
    def test_discovery_servers_orchid(self):
        def primary_cell_tag_in_master_cells(ds):
            try:
                master_cells = ls("//sys/discovery_servers/{}/orchid/discovery_server/security/master_cells".format(ds))
            except YtError:
                return False

            return str(primary_cell_tag) in master_cells

        def enough_members(ds, primary_cell_tag):
            members = ls("//sys/discovery_servers/{}/orchid/discovery_server/security/master_cells/{}/@members"
                         .format(ds, primary_cell_tag))
            return len(members) == self.NUM_MASTERS

        set("//sys/@config/security_manager/enable_distributed_throttler", True)

        create_user("u")
        set("//sys/users/u/@request_limits/read_request_rate/default", 1000)
        ls("//sys", authenticated_user="u")

        primary_cell_tag = get("//sys/@primary_cell_tag")
        wait(lambda: exists("//sys/discovery_servers"))

        ds = ls(("//sys/discovery_servers"))[0]
        wait(lambda: primary_cell_tag_in_master_cells(ds))
        wait(lambda: enough_members(ds, primary_cell_tag))
