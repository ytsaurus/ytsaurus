from yt.yt.orm.example.python.local.local import ExampleInstance

from yt.orm.recipe.recipe import Recipe

from yt.wrapper.common import update

from library.python.testing.recipe import declare_recipe


DEFAULT_EXAMPLE_MASTER_CONFIG = {
    "yt_connector": {"user": "root"},
    "transaction_manager": {
        "full_scan_allowed_by_default": False,
        "allow_index_primary_key_in_continuation_token": True,
        "scalar_attribute_loads_null_as_typed": True,
        "any_to_one_attribute_value_getter_returns_null": False,
        "fetch_root_optimization_level": "squash_value_getters",
        "build_key_expression": True,
    },
    "object_manager": {
        "index_mode_per_name": {
            "books_by_year": "enabled",
            "editors_by_achievements": "building",
        },
        "removed_objects_sweep_period": 2000,
        "removed_objects_grace_timeout": 4000,
        "removed_object_table_reader": {
            "read_by_tablets": True,
            "batch_size": 7,
        },
        "default_parents_table_mode": "dont_write_to_common_table",
    },
    "watch_manager": {
        "query_selector_enabled_per_type": {
            "author": True,
            "executor": True,
        },
        "distribution_policy": {
            "per_type": {
                "executor": {
                    "type": "hash",
                },
            },
        },
    },
}


class ExampleRecipe(Recipe):
    def __init__(self):
        super(ExampleRecipe, self).__init__(name="EXAMPLE")

    def get_args_parser(self):
        parser = super(ExampleRecipe, self).get_args_parser()
        parser.add_argument(
            "--use-native-connection",
            help="Use RPC proxy connection",
            action="store_true",
        )
        return parser

    def make_orm_instance(
        self,
        destination,
        master_config=None,
        local_yt_options=None,
        port_locks_path=None,
        master_count=1,
    ):
        return ExampleInstance(
            destination,
            options=dict(
                example_master_config=update(DEFAULT_EXAMPLE_MASTER_CONFIG, master_config),
                enable_ssl=True,
                local_yt_options=local_yt_options,
                port_locks_path=port_locks_path,
                use_native_connection=self.args.use_native_connection,
                master_count=master_count,
            ),
        )


if __name__ == "__main__":
    recipe = ExampleRecipe()
    declare_recipe(recipe.start, recipe.stop)
