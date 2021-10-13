from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, set, remove, create_medium, create_data_center, remove_data_center,
    create_rack, remove_rack)


##################################################################


class TestNewReplicator(YTEnvSetup):
    DELTA_MASTER_CONFIG = {
        "use_new_replicator": True,
    }

    @authors("gritukan")
    def test_simple(self):
        pass

    @authors("gritukan")
    def test_dynamic_config_change(self):
        set("//sys/@config/chunk_manager/max_heavy_columns", 10)

    @authors("gritukan")
    def test_medium_update(self):
        create_medium("nvme")
        set("//sys/media/nvme/@name", "optane")
        set("//sys/media/optane/@config/max_replicas_per_rack", 3)

    @authors("gritukan")
    def test_data_centers_update(self):
        create_data_center("d1")
        set("//sys/data_centers/d1/@name", "d2")
        remove_data_center("d2")

    @authors("gritukan")
    def test_racks_update(self):
        create_rack("r")
        create_data_center("d1")
        set("//sys/racks/r/@data_center", "d1")
        remove("//sys/racks/r/@data_center")
        set("//sys/racks/r/@name", "w")
        remove_rack("w")


##################################################################


class TestNewReplicatorMulticell(TestNewReplicator):
    NUM_SECONDARY_MASTER_CELLS = 2
