import pytest

from yt_env_setup import YTEnvSetup, Restarter, MASTERS_SERVICE
from yt_commands import *


@authors("renadeen")
class TestSchedulerPoolManipulations(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 0
    NUM_SCHEDULERS = 0

    def setup_method(self, method):
        super(TestSchedulerPoolManipulations, self).setup_method(method)
        if exists("//sys/pool_trees/default"):
            remove_pool_tree("default", wait_for_orchid=False)

    def test_create(self):
        assert get("//sys/pool_trees") == {}

        create_pool_tree("my_tree", wait_for_orchid=False)
        assert get("//sys/pool_trees") == {"my_tree": {}}

        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)
        assert get("//sys/pool_trees") == {"my_tree": {"nirvana": {}}}

        create_pool("prod", pool_tree="my_tree", parent_name="nirvana", wait_for_orchid=False)
        assert get("//sys/pool_trees") == {"my_tree": {"nirvana": {"prod": {}}}}

    def test_create_ignore_existing(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool_tree("my_tree", wait_for_orchid=False, ignore_existing=True)

        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", ignore_existing=True, wait_for_orchid=False)

    def test_remove(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)
        create_pool("prod", pool_tree="my_tree", parent_name="nirvana", wait_for_orchid=False)

        remove("//sys/pool_trees/my_tree/nirvana/prod")
        assert get("//sys/pool_trees") == {"my_tree": {"nirvana": {}}}

        remove("//sys/pool_trees/my_tree/nirvana")
        assert get("//sys/pool_trees") == {"my_tree": {}}

        remove_pool_tree("my_tree", wait_for_orchid=False)
        assert get("//sys/pool_trees") == {}

    def test_remove_with_force(self):
        with pytest.raises(YtError):
            remove_pool_tree("my_tree", wait_for_orchid=False)
        with pytest.raises(YtError):
            remove("//sys/pool_trees/my_tree/nirvana")

        remove_pool_tree("my_tree", wait_for_orchid=False, force=True)
        remove("//sys/pool_trees/my_tree/nirvana", force=True)

    def test_remove_subtree(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)
        create_pool("prod", pool_tree="my_tree", parent_name="nirvana", wait_for_orchid=False)

        remove("//sys/pool_trees/my_tree/nirvana")
        assert get("//sys/pool_trees") == {"my_tree": {}}

    def test_remove_non_empty_tree(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)
        create_pool("prod", pool_tree="my_tree", parent_name="nirvana", wait_for_orchid=False)

        remove_pool_tree("my_tree", wait_for_orchid=False)
        assert get("//sys/pool_trees") == {}

    def test_create_empty_names_validation(self):
        with pytest.raises(YtError):
            create_pool_tree("", wait_for_orchid=False)

        create_pool_tree("my_tree", wait_for_orchid=False)
        with pytest.raises(YtError):
            create_pool("", pool_tree="my_tree", wait_for_orchid=False)

        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)

        with pytest.raises(YtError):
            create_pool("", pool_tree="my_tree", parent_name="nirvana", wait_for_orchid=False)

    def test_duplicate_tree_names_forbidden(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        with pytest.raises(YtError):
            create_pool_tree("my_tree", wait_for_orchid=False)

    def test_duplicate_names_forbidden_in_same_tree(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)
        with pytest.raises(YtError):
            create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)

        create_pool("prod", pool_tree="my_tree", parent_name="nirvana", wait_for_orchid=False)
        with pytest.raises(YtError):
            create_pool(
                "prod",
                pool_tree="my_tree",
                parent_name="nirvana",
                wait_for_orchid=False,
            )
        with pytest.raises(YtError):
            create_pool("prod", pool_tree="my_tree", wait_for_orchid=False)

    def test_duplicate_names_allowed_in_different_trees(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)
        create_pool("prod", pool_tree="my_tree", parent_name="nirvana", wait_for_orchid=False)

        create_pool_tree("another_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="another_tree", wait_for_orchid=False)
        create_pool(
            "prod",
            pool_tree="another_tree",
            parent_name="nirvana",
            wait_for_orchid=False,
        )

    def test_same_pool_and_tree_names_allowed(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)

        create_pool_tree("nirvana", wait_for_orchid=False)
        create_pool_tree("prod", wait_for_orchid=False)

        create_pool("prod", pool_tree="my_tree", parent_name="nirvana", wait_for_orchid=False)

    def test_root_name_is_forbidden(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)

        with pytest.raises(YtError):
            create_pool(
                "<Root>",
                parent_name="nirvana",
                pool_tree="my_tree",
                wait_for_orchid=False,
            )

    def test_dollar_in_name_is_forbidden(self):
        create_pool_tree("my_tree", wait_for_orchid=False)

        with pytest.raises(YtError):
            create_pool("pool$name", pool_tree="my_tree", wait_for_orchid=False)

    def test_long_pool_name_is_forbidden(self):
        create_pool_tree("my_tree", wait_for_orchid=False)

        with pytest.raises(YtError):
            create_pool("abc" * 100, pool_tree="my_tree", wait_for_orchid=False)

    def test_create_checks_tree_and_parent_pool_existence(self):
        with pytest.raises(YtError):
            create_pool("nirvana", pool_tree="inexistent_tree", wait_for_orchid=False)

        create_pool_tree("my_tree", wait_for_orchid=False)
        with pytest.raises(YtError):
            create_pool(
                "prod",
                pool_tree="my_tree",
                parent_name="nirvana",
                wait_for_orchid=False,
            )

    def test_validate_tree_depth(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("pool0", pool_tree="my_tree", wait_for_orchid=False)
        for i in range(29):
            create_pool(
                "pool" + str(i + 1),
                pool_tree="my_tree",
                parent_name="pool" + str(i),
                wait_for_orchid=False,
            )
        with pytest.raises(YtError):
            create_pool(
                "pool31",
                pool_tree="my_tree",
                parent_name="pool30",
                wait_for_orchid=False,
            )

    def test_move_via_attribute_pool_to_subpool(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)
        create_pool("logfeller", pool_tree="my_tree", wait_for_orchid=False)
        create_pool(
            "logfeller_prod",
            pool_tree="my_tree",
            parent_name="logfeller",
            wait_for_orchid=False,
        )

        set("//sys/pool_trees/my_tree/logfeller/logfeller_prod/@parent_name", "nirvana")
        assert get("//sys/pool_trees") == {"my_tree": {"logfeller": {}, "nirvana": {"logfeller_prod": {}}}}

    def test_standard_move_pool_to_subpool(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)
        create_pool("logfeller", pool_tree="my_tree", wait_for_orchid=False)
        create_pool(
            "logfeller_prod",
            pool_tree="my_tree",
            parent_name="logfeller",
            wait_for_orchid=False,
        )

        move(
            "//sys/pool_trees/my_tree/logfeller/logfeller_prod",
            "//sys/pool_trees/my_tree/nirvana/logfeller_prod",
        )
        assert get("//sys/pool_trees") == {"my_tree": {"logfeller": {}, "nirvana": {"logfeller_prod": {}}}}

    def test_move_via_attribute_pool_to_root(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)
        create_pool(
            "logfeller",
            pool_tree="my_tree",
            parent_name="nirvana",
            wait_for_orchid=False,
        )

        set("//sys/pool_trees/my_tree/nirvana/logfeller/@parent_name", "<Root>")
        assert get("//sys/pool_trees") == {"my_tree": {"logfeller": {}, "nirvana": {}}}

    def test_standard_move_pool_to_root(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)
        create_pool(
            "logfeller",
            pool_tree="my_tree",
            parent_name="nirvana",
            wait_for_orchid=False,
        )

        move(
            "//sys/pool_trees/my_tree/nirvana/logfeller",
            "//sys/pool_trees/my_tree/logfeller",
        )
        assert get("//sys/pool_trees") == {"my_tree": {"logfeller": {}, "nirvana": {}}}

    def test_move_via_attribute_to_descendant_is_forbidden(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)
        create_pool("prod", pool_tree="my_tree", parent_name="nirvana", wait_for_orchid=False)

        with pytest.raises(YtError):
            set("//sys/pool_trees/my_tree/nirvana/@parent_name", "prod")

    def test_standard_move_to_descendant_is_forbidden2(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)
        create_pool("prod", pool_tree="my_tree", parent_name="nirvana", wait_for_orchid=False)

        with pytest.raises(YtError):
            move(
                "//sys/pool_trees/my_tree/nirvana",
                "//sys/pool_trees/my_tree/nirvana/prod/nirvana",
            )

    def test_move_via_attribute_of_pool_trees_is_forbidden(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool_tree("another_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="another_tree", wait_for_orchid=False)

        with pytest.raises(YtError):
            set("//sys/pool_trees/my_tree/@parent_name", "nirvana")

    def test_standard_move_of_pool_trees_is_forbidden(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool_tree("another_tree", wait_for_orchid=False)

        with pytest.raises(YtError):
            move("//sys/pool_trees/my_tree", "//sys/pool_trees/another_tree/nirvana")

    def test_move_via_attribute_to_another_pool_tree_is_forbidden(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)

        create_pool_tree("another_tree", wait_for_orchid=False)
        create_pool("logfeller", pool_tree="another_tree", wait_for_orchid=False)

        with pytest.raises(YtError):
            set("//sys/pool_trees/my_tree/nirvana/@parent_name", "logfeller")

    def test_standard_move_to_another_pool_tree(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)

        create_pool_tree("another_tree", wait_for_orchid=False)
        create_pool("logfeller", pool_tree="another_tree", wait_for_orchid=False)

        move(
            "//sys/pool_trees/my_tree/nirvana",
            "//sys/pool_trees/another_tree/logfeller/nirvana",
        )
        assert get("//sys/pool_trees") == {
            "my_tree": {},
            "another_tree": {"logfeller": {"nirvana": {}}},
        }

    def test_standard_move_to_another_tree_respects_duplicate_name_validation(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)
        create_pool_tree("another_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="another_tree", wait_for_orchid=False)
        create_pool("logfeller", pool_tree="another_tree", wait_for_orchid=False)

        with pytest.raises(YtError):
            move(
                "//sys/pool_trees/my_tree/nirvana",
                "//sys/pool_trees/another_tree/nirvana",
            )
        with pytest.raises(YtError):
            move(
                "//sys/pool_trees/my_tree/nirvana",
                "//sys/pool_trees/another_tree/logfeller/nirvana",
            )
        with pytest.raises(YtError):
            move(
                "//sys/pool_trees/my_tree/nirvana",
                "//sys/pool_trees/another_tree/nirvana/logfeller",
            )

        assert get("//sys/pool_trees") == {
            "my_tree": {"nirvana": {}},
            "another_tree": {"nirvana": {}, "logfeller": {}},
        }

    def test_move_via_attribute_respects_validation(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)
        create_pool("logfeller", pool_tree="my_tree", wait_for_orchid=False)

        set("//sys/pool_trees/my_tree/logfeller/@min_share_resources", {"cpu": 10})

        with pytest.raises(YtError):
            set("//sys/pool_trees/my_tree/logfeller/@parent_name", "nirvana")

        assert get("//sys/pool_trees") == {"my_tree": {"nirvana": {}, "logfeller": {}}}

    def test_standard_move_respects_validation(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)
        create_pool("logfeller", pool_tree="my_tree", wait_for_orchid=False)

        set("//sys/pool_trees/my_tree/logfeller/@min_share_resources", {"cpu": 10})

        with pytest.raises(YtError):
            move(
                "//sys/pool_trees/my_tree/logfeller",
                "//sys/pool_trees/my_tree/nirvana/logfeller",
            )

        assert get("//sys/pool_trees") == {"my_tree": {"nirvana": {}, "logfeller": {}}}

    def test_rename_pool(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)

        set("//sys/pool_trees/my_tree/nirvana/@name", "logfeller")
        assert get("//sys/pool_trees/my_tree") == {"logfeller": {}}
        assert get("//sys/pool_trees/my_tree/logfeller/@name") == "logfeller"

    def test_rename_via_attribute_respects_name_validation(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)
        create_pool("prod", pool_tree="my_tree", parent_name="nirvana", wait_for_orchid=False)
        create_pool("logfeller", pool_tree="my_tree", wait_for_orchid=False)

        with pytest.raises(YtError):
            set("//sys/pool_trees/my_tree/logfeller/@name", "logfeller$logfeller")
        with pytest.raises(YtError):
            set("//sys/pool_trees/my_tree/logfeller/@name", "prod")

    def test_move_with_rename(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)
        create_pool("logfeller", pool_tree="my_tree", wait_for_orchid=False)
        create_pool(
            "logfeller_prod",
            pool_tree="my_tree",
            parent_name="logfeller",
            wait_for_orchid=False,
        )

        move(
            "//sys/pool_trees/my_tree/logfeller/logfeller_prod",
            "//sys/pool_trees/my_tree/nirvana/nirvana_prod",
        )
        assert get("//sys/pool_trees") == {"my_tree": {"logfeller": {}, "nirvana": {"nirvana_prod": {}}}}

    def test_structure_loads_from_snapshot(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)
        create_pool("prod", pool_tree="my_tree", parent_name="nirvana", wait_for_orchid=False)

        build_snapshot(cell_id=None)
        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        assert get("//sys/pool_trees") == {"my_tree": {"nirvana": {"prod": {}}}}

    def test_max_depth_structure_loads_from_snapshot(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("pool0", pool_tree="my_tree", wait_for_orchid=False)
        i = 0
        while True:
            try:
                create_pool(
                    "pool" + str(i + 1),
                    pool_tree="my_tree",
                    parent_name="pool" + str(i),
                    wait_for_orchid=False,
                )
            except YtError:
                break

        build_snapshot(cell_id=None)
        with Restarter(self.Env, MASTERS_SERVICE):
            pass

    def test_validation_works_after_load_from_snapshot(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)
        create_pool("prod", pool_tree="my_tree", parent_name="nirvana", wait_for_orchid=False)

        build_snapshot(cell_id=None)
        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        with pytest.raises(YtError):
            create_pool_tree("my_tree", wait_for_orchid=False)
        with pytest.raises(YtError):
            create_pool("prod", pool_tree="my_tree", wait_for_orchid=False)

    def test_creation_works_after_load_from_snapshot(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)

        build_snapshot(cell_id=None)
        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        create_pool("prod", pool_tree="my_tree", parent_name="nirvana", wait_for_orchid=False)

        assert get("//sys/pool_trees") == {"my_tree": {"nirvana": {"prod": {}}}}

    def test_set_and_get_pool_attribute(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)

        set("//sys/pool_trees/my_tree/nirvana/@max_operation_count", 10)
        assert get("//sys/pool_trees/my_tree/nirvana/@max_operation_count") == 10

    def test_set_and_get_pooltree_config_attribute(self):
        create_pool_tree("my_tree", wait_for_orchid=False)

        set("//sys/pool_trees/my_tree/@config/default_parent_pool", "research")
        assert get("//sys/pool_trees/my_tree/@config/default_parent_pool") == "research"

    def test_set_and_get_unknown_pool_attribute(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)

        set("//sys/pool_trees/my_tree/nirvana/@some_unknown_attribute", 10)
        assert get("//sys/pool_trees/my_tree/nirvana/@some_unknown_attribute") == 10

    def test_set_and_get_unknown_pooltree_config_attribute(self):
        create_pool_tree("my_tree", wait_for_orchid=False)

        set("//sys/pool_trees/my_tree/@config/some_unknown_attribute", 10)
        assert get("//sys/pool_trees/my_tree/@config/some_unknown_attribute") == 10

    def test_get_pool_with_attributes_returns_only_specified_attributes(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)

        set("//sys/pool_trees/my_tree/nirvana/@max_operation_count", 2)
        result = get(
            "//sys/pool_trees/my_tree/nirvana",
            attributes=["max_operation_count", "max_running_operation_count"],
        )
        assert result.attributes["max_operation_count"] == 2
        assert "max_running_operation_count" not in result.attributes

    def test_get_pooltree_with_config_attribute(self):
        create_pool_tree("my_tree", wait_for_orchid=False)

        set("//sys/pool_trees/my_tree/@config/max_operation_count", 2)
        result = get("//sys/pool_trees/my_tree", attributes=["config"])
        assert result.attributes["config"]["max_operation_count"] == 2
        assert "max_running_operation_count" not in result.attributes["config"]

    def test_invalid_type_pool_attribute_is_refused_on_set(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)

        set("//sys/pool_trees/my_tree/nirvana/@max_operation_count", 10)
        with pytest.raises(YtError):
            set("//sys/pool_trees/my_tree/nirvana/@max_operation_count", True)
        assert get("//sys/pool_trees/my_tree/nirvana/@max_operation_count") == 10

    def test_invalid_type_pooltree_config_attribute_is_refused_on_set(self):
        create_pool_tree("my_tree", wait_for_orchid=False)

        set("//sys/pool_trees/my_tree/@config/default_parent_pool", "research")
        with pytest.raises(YtError):
            set("//sys/pool_trees/my_tree/@config/default_parent_pool", True)
        assert get("//sys/pool_trees/my_tree/@config/default_parent_pool") == "research"

    def test_pool_attribute_constraints_are_enforced_on_set(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)

        set("//sys/pool_trees/my_tree/nirvana/@max_operation_count", 10)
        with pytest.raises(YtError):
            set("//sys/pool_trees/my_tree/nirvana/@max_operation_count", -1)
        assert get("//sys/pool_trees/my_tree/nirvana/@max_operation_count") == 10

    def test_pooltree_config_attribute_constraints_are_enforced_on_set(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        set("//sys/pool_trees/my_tree/@config/max_ephemeral_pools_per_user", 10)
        with pytest.raises(YtError):
            set("//sys/pool_trees/my_tree/@config/max_ephemeral_pools_per_user", -1)
        assert get("//sys/pool_trees/my_tree/@config/max_ephemeral_pools_per_user") == 10

    def test_remove_builtin_pool_attribute(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)

        set("//sys/pool_trees/my_tree/nirvana/@max_operation_count", 10)
        remove("//sys/pool_trees/my_tree/nirvana/@max_operation_count")
        with pytest.raises(YtError):
            get("//sys/pool_trees/my_tree/nirvana/@max_operation_count")

    def test_remove_known_pooltree_attribute(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)

        set("//sys/pool_trees/my_tree/@config/default_parent_pool", "research")
        remove("//sys/pool_trees/my_tree/@config/default_parent_pool")
        with pytest.raises(YtError):
            get("//sys/pool_trees/my_tree/@config/default_parent_pool")

    def test_remove_composite_builtin_pool_attribute(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)

        set("//sys/pool_trees/my_tree/nirvana/@min_share_resources", {"cpu": 10})
        remove("//sys/pool_trees/my_tree/nirvana/@min_share_resources")
        with pytest.raises(YtError):
            get("//sys/pool_trees/my_tree/nirvana/@min_share_resources")

    def test_remove_nested_pool_attribute(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)

        set("//sys/pool_trees/my_tree/nirvana/@min_share_resources", {"cpu": 10})
        remove("//sys/pool_trees/my_tree/nirvana/@min_share_resources/cpu")
        assert get("//sys/pool_trees/my_tree/nirvana/@min_share_resources") == {}

    def test_if_remove_attribute_breaks_validation_value_is_preserved(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)
        create_pool("prod", pool_tree="my_tree", parent_name="nirvana", wait_for_orchid=False)

        set("//sys/pool_trees/my_tree/nirvana/@min_share_resources", {"cpu": 10})
        set("//sys/pool_trees/my_tree/nirvana/prod/@min_share_resources", {"cpu": 10})
        with pytest.raises(YtError):
            remove("//sys/pool_trees/my_tree/nirvana/@min_share_resources")
        assert get("//sys/pool_trees/my_tree/nirvana/@min_share_resources") == {"cpu": 10}

    def test_max_running_operation_count_validation(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)

        set("//sys/pool_trees/my_tree/nirvana/@max_operation_count", 10)
        with pytest.raises(YtError):
            set("//sys/pool_trees/my_tree/nirvana/@max_running_operation_count", 11)
        set("//sys/pool_trees/my_tree/nirvana/@max_running_operation_count", 9)
        with pytest.raises(YtError):
            set("//sys/pool_trees/my_tree/nirvana/@max_operation_count", 8)

    def test_subpools_of_fifo_pools_are_forbidden(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)
        create_pool("prod", pool_tree="my_tree", parent_name="nirvana", wait_for_orchid=False)

        with pytest.raises(YtError):
            set("//sys/pool_trees/my_tree/nirvana/@mode", "fifo")

        remove("//sys/pool_trees/my_tree/nirvana/prod")
        set("//sys/pool_trees/my_tree/nirvana/@mode", "fifo")
        with pytest.raises(YtError):
            create_pool(
                "prod",
                pool_tree="my_tree",
                parent_name="nirvana",
                wait_for_orchid=False,
            )

    def test_cant_give_child_strong_guarantee_without_parent_guarantee(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)
        create_pool("prod", pool_tree="my_tree", parent_name="nirvana", wait_for_orchid=False)

        with pytest.raises(YtError):
            set(
                "//sys/pool_trees/my_tree/nirvana/prod/@min_share_resources",
                {"cpu": 100.0},
            )

        set("//sys/pool_trees/my_tree/nirvana/@min_share_resources", {"cpu": 10.0})
        with pytest.raises(YtError):
            set(
                "//sys/pool_trees/my_tree/nirvana/prod/@min_share_resources",
                {"cpu": 100.0},
            )

        set("//sys/pool_trees/my_tree/nirvana/@min_share_resources/cpu", 100.0)
        set("//sys/pool_trees/my_tree/nirvana/prod/@min_share_resources", {"cpu": 100.0})

    def test_cant_give_child_burst_guarantee_without_parent_guarantee(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False, attributes={
            "integral_guarantees": {"guarantee_type": "none"}})
        create_pool("prod", pool_tree="my_tree", parent_name="nirvana", wait_for_orchid=False, attributes={
            "integral_guarantees": {"guarantee_type": "burst"}})

        def set_burst_guarantee(pool_path, cpu):
            path = "//sys/pool_trees/my_tree/" + pool_path + "/@integral_guarantees"
            set(path, {"burst_guarantee_resources": {"cpu": cpu}})

        with pytest.raises(YtError):
            set_burst_guarantee("nirvana/prod", 100.0)

        set_burst_guarantee("nirvana", 10.0)
        with pytest.raises(YtError):
            set_burst_guarantee("nirvana/prod", 100.0)

        set_burst_guarantee("nirvana", 100.0)
        set_burst_guarantee("nirvana/prod", 100.0)

    def test_cant_give_child_resource_flow_without_parent_flow(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False, attributes={
            "integral_guarantees": {"guarantee_type": "none"}})
        create_pool("prod", pool_tree="my_tree", parent_name="nirvana", wait_for_orchid=False, attributes={
            "integral_guarantees": {"guarantee_type": "relaxed"}})

        def set_resource_flow(pool_path, cpu):
            path = "//sys/pool_trees/my_tree/" + pool_path + "/@integral_guarantees"
            set(path, {"resource_flow": {"cpu": cpu}})

        with pytest.raises(YtError):
            set_resource_flow("nirvana/prod", 100.0)

        set_resource_flow("nirvana", 10.0)
        with pytest.raises(YtError):
            set_resource_flow("nirvana/prod", 100.0)

        set_resource_flow("nirvana", 100.0)
        set_resource_flow("nirvana/prod", 100.0)

    def test_cant_give_resource_flow_to_child_of_integral_pool(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)
        create_pool("prod", pool_tree="my_tree", parent_name="nirvana", wait_for_orchid=False)

        # burst
        set("//sys/pool_trees/my_tree/nirvana/@integral_guarantees", {
            "guarantee_type": "burst",
            "resource_flow": {"cpu": 50},
            "burst_guarantee_resources": {"cpu": 100},
        })
        with pytest.raises(YtError):
            set("//sys/pool_trees/my_tree/nirvana/prod/@integral_guarantees", {"resource_flow": {"cpu": 10}})

        # relaxed
        set("//sys/pool_trees/my_tree/nirvana/@integral_guarantees", {
            "guarantee_type": "relaxed",
            "resource_flow": {"cpu": 50},
        })
        with pytest.raises(YtError):
            set("//sys/pool_trees/my_tree/nirvana/prod/@integral_guarantees", {"resource_flow": {"cpu": 10}})

    def test_cant_make_parent_burst_or_relaxed_if_child_has_integral_resources(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False, attributes={
            "integral_guarantees": {
                "guarantee_type": "none",
                "resource_flow": {"cpu": 50},
                "burst_guarantee_resources": {"cpu": 50},
            }
        })
        create_pool("prod", pool_tree="my_tree", parent_name="nirvana", wait_for_orchid=False)

        # child has resource flow
        set("//sys/pool_trees/my_tree/nirvana/prod/@integral_guarantees", {
            "resource_flow": {"cpu": 50},
        })
        with pytest.raises(YtError):
            set("//sys/pool_trees/my_tree/nirvana/@integral_guarantees", {"guarantee_type": "burst"})
        with pytest.raises(YtError):
            set("//sys/pool_trees/my_tree/nirvana/@integral_guarantees", {"guarantee_type": "relaxed"})

        # child has burst guarantee resources
        set("//sys/pool_trees/my_tree/nirvana/prod/@integral_guarantees", {
            "burst_guarantee_resources": {"cpu": 50},
        })
        with pytest.raises(YtError):
            set("//sys/pool_trees/my_tree/nirvana/@integral_guarantees", {"guarantee_type": "burst"})
        with pytest.raises(YtError):
            set("//sys/pool_trees/my_tree/nirvana/@integral_guarantees", {"guarantee_type": "relaxed"})

    @pytest.mark.skipif(True, reason="Not important yet(renadeen)")
    def test_update_nested_double_with_int(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)
        create_pool("prod", pool_tree="my_tree", parent_name="nirvana", wait_for_orchid=False)

        set("//sys/pool_trees/my_tree/nirvana/@min_share_resources", {"cpu": 10})
        set("//sys/pool_trees/my_tree/nirvana/@min_share_resources/cpu", 100)

    def test_set_and_get_composite_attribute(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)

        with pytest.raises(YtError):
            get("//sys/pool_trees/my_tree/nirvana/@min_share_resources")

        set("//sys/pool_trees/my_tree/nirvana/@min_share_resources", {"cpu": 100})
        assert get("//sys/pool_trees/my_tree/nirvana/@min_share_resources") == {"cpu": 100}

        set("//sys/pool_trees/my_tree/nirvana/@min_share_resources", {})
        assert get("//sys/pool_trees/my_tree/nirvana/@min_share_resources") == {}

    def test_set_and_get_nested_attribute(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)

        with pytest.raises(YtError):
            set(
                "//sys/pool_trees/my_tree/nirvana/@ephemeral_subpool_config/max_operation_count",
                10,
            )
        set(
            "//sys/pool_trees/my_tree/nirvana/@ephemeral_subpool_config",
            {"max_operation_count": 10},
        )
        assert get("//sys/pool_trees/my_tree/nirvana/@ephemeral_subpool_config/max_operation_count") == 10

        set(
            "//sys/pool_trees/my_tree/nirvana/@ephemeral_subpool_config/max_operation_count",
            5,
        )
        assert get("//sys/pool_trees/my_tree/nirvana/@ephemeral_subpool_config/max_operation_count") == 5

    def test_exist_pool_attribute(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)

        assert not exists("//sys/pool_trees/my_tree/nirvana/@max_operation_count")
        set("//sys/pool_trees/my_tree/nirvana/@max_operation_count", 10)
        assert exists("//sys/pool_trees/my_tree/nirvana/@max_operation_count")

        assert not exists("//sys/pool_trees/my_tree/nirvana/@custom_attr")
        set("//sys/pool_trees/my_tree/nirvana/@custom_attr", 10)
        assert exists("//sys/pool_trees/my_tree/nirvana/@custom_attr")

    def test_exist_pool_tree_config_attribute(self):
        create_pool_tree("my_tree", wait_for_orchid=False)

        assert not exists("//sys/pool_trees/my_tree/@config/nodes_filter")
        set("//sys/pool_trees/my_tree/@config/nodes_filter", "filter")
        assert exists("//sys/pool_trees/my_tree/@config/nodes_filter")

        assert not exists("//sys/pool_trees/my_tree/@config/custom_attr")
        set("//sys/pool_trees/my_tree/@config/custom_attr", 10)
        assert exists("//sys/pool_trees/my_tree/@config/custom_attr")

    def test_set_using_different_attribute_aliases_is_forbidden(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)

        attribute_key = "//sys/pool_trees/my_tree/nirvana/@aggressive_starvation_enabled"
        attribute_alias = "//sys/pool_trees/my_tree/nirvana/@enable_aggressive_starvation"

        set(attribute_key, True)
        with pytest.raises(YtError):
            set(attribute_alias, True)

        remove(attribute_key)
        set(attribute_alias, True)
        with pytest.raises(YtError):
            set(attribute_key, True)

    def test_set_inexistent_path_fails_with_correct_error(self):
        create_pool_tree("my_tree", wait_for_orchid=False)

        with raises_yt_error(ResolveErrorCode):
            set("//sys/pool_trees/my_tree/nirvana/@mode", "fifo")

    def test_pool_tree_and_pool_common_attributes(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)

        set("//sys/pool_trees/my_tree/@config/fair_share_preemption_timeout", 1)
        assert get("//sys/pool_trees/my_tree/@config/fair_share_preemption_timeout") == 1

        set("//sys/pool_trees/my_tree/nirvana/@fair_share_preemption_timeout", 2)
        assert get("//sys/pool_trees/my_tree/nirvana/@fair_share_preemption_timeout") == 2

    def test_access_to_pool_tree_config_attribute_on_pool_tree_object_is_forbidden(self):
        create_pool_tree("my_tree", wait_for_orchid=False)

        with pytest.raises(YtError):
            set("//sys/pool_trees/my_tree/@default_parent_pool", "research")
        with pytest.raises(YtError):
            get("//sys/pool_trees/my_tree/@default_parent_pool")
        with pytest.raises(YtError):
            remove("//sys/pool_trees/my_tree/@default_parent_pool")

    def test_access_to_pool_attribute_on_pooltree_is_forbidden(self):
        create_pool_tree("my_tree", wait_for_orchid=False)

        with pytest.raises(YtError):
            set("//sys/pool_trees/my_tree/@weight", 1)
        with pytest.raises(YtError):
            get("//sys/pool_trees/my_tree/@weight")

    def test_get_root_returns_descendant_attributes(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)

        set("//sys/pool_trees/my_tree/@some_attr", "attr_value")
        set("//sys/pool_trees/my_tree/nirvana/@max_operation_count", 10)

        root_get = get("//sys/pool_trees", attributes=["max_operation_count", "some_attr"])

        assert "some_attr" in root_get["my_tree"].attributes
        assert root_get["my_tree"].attributes["some_attr"] == "attr_value"

        assert "max_operation_count" in root_get["my_tree"]["nirvana"].attributes
        assert root_get["my_tree"]["nirvana"].attributes["max_operation_count"] == 10

    # def test_weird_bug_in_create_with_attributes(self):
    #     # Test weird situation, when on create attribute descriptors are cached at master
    #     # but pool and pool tree has different attribute descriptors
    #     # and it can cause pool tree to use pool's descriptors
    #     create_pool_tree("my_tree", wait_for_orchid=False)
    #     create_pool("nirvana", pool_tree="my_tree", attributes={"mode": "fifo"}, wait_for_orchid=False)
    #     # Exception was thrown here
    #     create_pool_tree("other_tree", wait_for_orchid=False, config={"mode": "trash_value"})

    def test_create_doesnt_set_unwanted_attributes(self):
        create_pool_tree("my_tree", wait_for_orchid=False, allow_patching=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)
        assert not exists("//sys/pool_trees/my_tree/nirvana/@pool_tree")

    def test_create_pool_with_attributes(self):
        create_pool_tree("my_tree", wait_for_orchid=False, allow_patching=False)
        create_pool(
            "nirvana",
            pool_tree="my_tree",
            attributes={"mode": "fifo"},
            wait_for_orchid=False,
        )
        assert get("//sys/pool_trees/my_tree/nirvana/@mode") == "fifo"

    def test_create_pool_tree_with_config_attributes(self):
        create_pool_tree("my_tree", wait_for_orchid=False, config={"default_parent_pool": "research"}, allow_patching=False)
        assert get("//sys/pool_trees/my_tree/@config/default_parent_pool") == "research"

    def test_fail_on_create_pool_with_attributes(self):
        create_pool_tree("my_tree", wait_for_orchid=False, allow_patching=False)

        with pytest.raises(YtError):
            create_pool(
                "nirvana",
                pool_tree="my_tree",
                attributes={"mode": "trash"},
                wait_for_orchid=False,
            )
        assert not exists("//sys/pool_trees/my_tree/nirvana")

    def test_fail_on_create_pool_tree_with_attributes(self):
        with pytest.raises(YtError):
            create_pool_tree("my_tree", wait_for_orchid=False, config={"max_operation_count": "trash"})
        assert not exists("//sys/pool_trees/my_tree")

    def test_get_set_remove_empty_pooltree_config(self):
        create_pool_tree("my_tree", wait_for_orchid=False, allow_patching=False)
        assert exists("//sys/pool_trees/my_tree/@config")
        assert get("//sys/pool_trees/my_tree/@config") == {}

        remove("//sys/pool_trees/my_tree/@config")
        assert exists("//sys/pool_trees/my_tree/@config")
        assert get("//sys/pool_trees/my_tree/@config") == {}

    def test_set_pooltree_config_overwriting(self):
        create_pool_tree("my_tree", wait_for_orchid=False, allow_patching=False)

        set("//sys/pool_trees/my_tree/@config", {"nodes_filter": "filter"})
        assert get("//sys/pool_trees/my_tree/@config") == {"nodes_filter": "filter"}

        set("//sys/pool_trees/my_tree/@config", {"inexistent": "value"})
        assert get("//sys/pool_trees/my_tree/@config") == {"inexistent": "value"}

        set("//sys/pool_trees/my_tree/@config", {"max_operation_count": 1})
        assert get("//sys/pool_trees/my_tree/@config") == {"max_operation_count": 1}

    def test_remove_pooltree_config(self):
        create_pool_tree("my_tree", wait_for_orchid=False, config={
            "nodes_filter": "filter",
            "inexistent": "value"
        })
        remove("//sys/pool_trees/my_tree/@config")

        assert get("//sys/pool_trees/my_tree/@config") == {}

    def test_attributes_load_from_snapshot(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        set("//sys/pool_trees/my_tree/@config/default_parent_pool", "research")
        set("//sys/pool_trees/my_tree/@config/unknown_config_attribute", 1)
        set("//sys/pool_trees/my_tree/@unknown_object_attribute", 2)

        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)
        set("//sys/pool_trees/my_tree/nirvana/@max_operation_count", 10)
        set("//sys/pool_trees/my_tree/nirvana/@some_unknown_attribute", "xxx")

        build_snapshot(cell_id=None)
        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        assert get("//sys/pool_trees/my_tree/@config/default_parent_pool") == "research"
        assert get("//sys/pool_trees/my_tree/@config/unknown_config_attribute") == 1
        assert get("//sys/pool_trees/my_tree/@unknown_object_attribute") == 2
        assert get("//sys/pool_trees/my_tree/nirvana/@max_operation_count") == 10
        assert get("//sys/pool_trees/my_tree/nirvana/@some_unknown_attribute") == "xxx"


@authors("renadeen")
class TestSchedulerPoolAcls(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 0
    NUM_SCHEDULERS = 0

    def setup_method(self, method):
        super(TestSchedulerPoolAcls, self).setup_method(method)
        if exists("//sys/pool_trees/default"):
            remove_pool_tree("default", wait_for_orchid=False)

    def test_nested_pool_use_permission(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)
        create_pool("prod", pool_tree="my_tree", parent_name="nirvana", wait_for_orchid=False)
        create_user("u")

        assert check_permission("u", "use", "//sys/pool_trees/my_tree/nirvana/prod")["action"] == "allow"

        set("//sys/pool_trees/my_tree/nirvana/@inherit_acl", False)
        assert check_permission("u", "use", "//sys/pool_trees/my_tree/nirvana/prod")["action"] == "deny"

        set("//sys/pool_trees/my_tree/nirvana/@acl", [make_ace("allow", "u", "use")])
        assert check_permission("u", "use", "//sys/pool_trees/my_tree/nirvana/prod")["action"] == "allow"

        set("//sys/pool_trees/my_tree/nirvana/@acl", [make_ace("deny", "u", "use")])
        assert check_permission("u", "use", "//sys/pool_trees/my_tree/nirvana/prod")["action"] == "deny"

    def test_modify_children_allows_to_create_child(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)

        create_user("u")
        with pytest.raises(YtError):
            create_pool(
                "prod",
                pool_tree="my_tree",
                parent_name="nirvana",
                authenticated_user="u",
                wait_for_orchid=False,
            )

        set(
            "//sys/pool_trees/my_tree/nirvana/@acl",
            [make_ace("allow", "u", "modify_children")],
        )
        create_pool(
            "prod",
            pool_tree="my_tree",
            parent_name="nirvana",
            authenticated_user="u",
            wait_for_orchid=False,
        )

    def test_modify_children_allows_to_remove_child(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)
        create_pool("prod", pool_tree="my_tree", parent_name="nirvana", wait_for_orchid=False)

        create_user("u")
        with pytest.raises(YtError):
            remove("//sys/pool_trees/my_tree/nirvana/prod", authenticated_user="u")

        set(
            "//sys/pool_trees/my_tree/nirvana/@acl",
            [make_ace("allow", "u", "modify_children")],
        )
        set(
            "//sys/pool_trees/my_tree/nirvana/prod/@acl",
            [make_ace("allow", "u", "remove")],
        )
        remove("//sys/pool_trees/my_tree/nirvana/prod", authenticated_user="u")

    def test_write_allows_to_set_user_managed_attribute(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)

        create_user("u")
        with pytest.raises(YtError):
            set(
                "//sys/pool_trees/my_tree/nirvana/@max_operation_count",
                10,
                authenticated_user="u",
            )

        set("//sys/pool_trees/my_tree/nirvana/@acl", [make_ace("allow", "u", "write")])
        set(
            "//sys/pool_trees/my_tree/nirvana/@max_operation_count",
            10,
            authenticated_user="u",
        )

    def test_administer_allows_to_set_non_user_managed_attribute(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)

        create_user("u")
        set("//sys/pool_trees/my_tree/nirvana/@acl", [make_ace("allow", "u", "write")])
        with pytest.raises(YtError):
            set(
                "//sys/pool_trees/my_tree/nirvana/@enable_aggressive_starvation",
                True,
                authenticated_user="u",
            )

        set(
            "//sys/pool_trees/my_tree/nirvana/@acl",
            [make_ace("allow", "u", ["administer", "write"])],
        )
        set(
            "//sys/pool_trees/my_tree/nirvana/@enable_aggressive_starvation",
            True,
            authenticated_user="u",
        )

    def test_administer_allows_to_set_non_system_attribute(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)

        create_user("u")
        set("//sys/pool_trees/my_tree/nirvana/@acl", [make_ace("allow", "u", "write")])
        with pytest.raises(YtError):
            set(
                "//sys/pool_trees/my_tree/nirvana/@non_system_attribute",
                True,
                authenticated_user="u",
            )

        set(
            "//sys/pool_trees/my_tree/nirvana/@acl",
            [make_ace("allow", "u", ["administer", "write"])],
        )
        set(
            "//sys/pool_trees/my_tree/nirvana/@non_system_attribute",
            True,
            authenticated_user="u",
        )

    def test_administer_allows_to_remove_non_system_attribute(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_pool("nirvana", pool_tree="my_tree", wait_for_orchid=False)
        set("//sys/pool_trees/my_tree/nirvana/@non_system_attribute", True)

        create_user("u")
        set("//sys/pool_trees/my_tree/nirvana/@acl", [make_ace("allow", "u", "write")])
        with pytest.raises(YtError):
            remove(
                "//sys/pool_trees/my_tree/nirvana/@non_sytem_attribute",
                authenticated_user="u",
            )

        set(
            "//sys/pool_trees/my_tree/nirvana/@acl",
            [make_ace("allow", "u", ["administer", "write"])],
        )
        remove(
            "//sys/pool_trees/my_tree/nirvana/@non_system_attribute",
            authenticated_user="u",
        )

    def test_write_on_root_allows_to_create_remove_pool_trees(self):
        create_pool_tree("my_tree", wait_for_orchid=False)
        create_user("u")
        with pytest.raises(YtError):
            create_pool_tree("new_tree", wait_for_orchid=False, authenticated_user="u")
        with pytest.raises(YtError):
            remove_pool_tree("my_tree", wait_for_orchid=False, authenticated_user="u")

        set("//sys/pool_trees/@acl", [make_ace("allow", "u", ["write", "remove"])])

        create_pool_tree("new_tree", wait_for_orchid=False, authenticated_user="u")
        remove_pool_tree("my_tree", wait_for_orchid=False, authenticated_user="u")
