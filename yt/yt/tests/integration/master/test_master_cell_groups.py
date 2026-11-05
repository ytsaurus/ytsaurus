from yt_env_setup import YTEnvSetup, Restarter, MASTERS_SERVICE

from yt_commands import (
    authors, build_snapshot, create_master_cell_group, exists, get, get_driver,
    ls, raises_yt_error, remove, set, wait,
)

from yt.common import YtError

import pytest

################################################################################


class TestMasterCellGroups(YTEnvSetup):
    ENABLE_MULTIDAEMON = False  # There are component restarts.
    NUM_MASTERS = 3
    NUM_NODES = 1
    NUM_SECONDARY_MASTER_CELLS = 2

    MASTER_CELL_DESCRIPTORS = {
        "11": {"roles": ["chunk_host"]},
        "12": {"roles": ["chunk_host"]},
    }

    @staticmethod
    def _path(name):
        return "//sys/master_cell_groups/{}".format(name)

    @staticmethod
    def _primary_cell_tag():
        return get("//sys/@cell_tag")

    def _check_true_for_secondaries(self, check):
        def _check():
            for index in range(self.Env.yt_config.secondary_cell_count):
                if not check(get_driver(index + 1)):
                    return False
            return True

        wait(_check, timeout=60, sleep_backoff=1.0)

    @authors("evanevannnn")
    def test_create_rename_and_remove(self):
        """Validate master cell group creation, renaming, and removal."""
        cell_tags = [self._primary_cell_tag()]
        create_master_cell_group("group", cell_tags)

        assert ls("//sys/master_cell_groups") == ["group"]
        assert get(self._path("group") + "/@cell_tags") == cell_tags

        set(self._path("group") + "/@name", "renamed")
        assert not exists(self._path("group"))
        assert get(self._path("renamed") + "/@cell_tags") == cell_tags

        remove(self._path("renamed"))
        assert not exists(self._path("renamed"))

    @authors("evanevannnn")
    def test_cell_tag_validation(self):
        """Validate that empty and unknown master cell tags are rejected."""
        with raises_yt_error("must contain at least one master cell"):
            create_master_cell_group("empty", [])

        with raises_yt_error("Unknown master cell tag"):
            create_master_cell_group("unknown", [1000])

    @authors("evanevannnn")
    def test_name_validation(self):
        """Validate master cell group name length and character restrictions."""
        cell_tags = [self._primary_cell_tag()]

        with raises_yt_error("Name is too long"):
            create_master_cell_group("a" * 101, cell_tags)

        for name in ["non-ascii-\u044f", "control-\x01", "with space", "with/slash"]:
            with raises_yt_error("Name must match regular expression"):
                create_master_cell_group(name, cell_tags)

        name = "Group_01-" + "a" * 91
        create_master_cell_group(name, cell_tags)
        assert exists(self._path(name))

    @authors("evanevannnn")
    @pytest.mark.parametrize("cell_tags, normalized_cell_tags", [
        ([11], [11]),
        ([11, 11], [11]),
        ([11, 11, 11], [11]),
        ([11, 12], [11, 12]),
        ([12, 11], [11, 12]),
        ([12, 11, 12, 11], [11, 12]),
    ])
    def test_duplicate_cell_tags(self, cell_tags, normalized_cell_tags):
        """Validate that tag repetitions and ordering cannot bypass group uniqueness checks."""
        create_master_cell_group("first", normalized_cell_tags)

        with raises_yt_error("same cell tags already exists"):
            create_master_cell_group("second", cell_tags)
        assert not exists(self._path("second"))

        other_cell_tags = [self._primary_cell_tag()]
        create_master_cell_group("second", other_cell_tags)
        with raises_yt_error("same cell tags already exists"):
            set(self._path("second") + "/@cell_tags", cell_tags)
        assert get(self._path("second") + "/@cell_tags") == other_cell_tags

        set(self._path("first") + "/@cell_tags", cell_tags)
        assert get(self._path("first") + "/@cell_tags") == normalized_cell_tags

    @authors("evanevannnn")
    def test_max_master_cell_group_count(self):
        """Validate that exceeding max_master_cell_group_count raises an error."""
        set("//sys/@config/multicell_manager/max_master_cell_group_count", 1)

        create_master_cell_group("first", [self._primary_cell_tag()])
        with raises_yt_error("count limit .* is reached"):
            create_master_cell_group("second", [self._primary_cell_tag()])

    @authors("evanevannnn")
    def test_snapshot(self):
        """Validate that master cell groups survive snapshot recovery."""
        cell_tags = [self._primary_cell_tag()]
        create_master_cell_group("group", cell_tags)

        build_snapshot(cell_id=get("//sys/@cell_id"))
        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        assert get(self._path("group") + "/@cell_tags") == cell_tags

    @authors("evanevannnn")
    def test_multicell_replication(self):
        """Validate master cell group replication to secondary masters."""
        primary_cell_tag = self._primary_cell_tag()
        cell_tags = [primary_cell_tag]
        create_master_cell_group("group", cell_tags)

        self._check_true_for_secondaries(
            lambda driver: get(self._path("group") + "/@cell_tags", driver=driver) == cell_tags)

        cell_tags = [11]
        set(self._path("group") + "/@cell_tags", cell_tags)
        set(self._path("group") + "/@name", "renamed")
        self._check_true_for_secondaries(
            lambda driver: (
                not exists(self._path("group"), driver=driver)
                and get(self._path("renamed") + "/@cell_tags", driver=driver) == cell_tags
            ))

        remove(self._path("renamed"))

        def check_removed(driver):
            try:
                return not exists(self._path("renamed"), driver=driver)
            except YtError:
                return False

        self._check_true_for_secondaries(check_removed)
