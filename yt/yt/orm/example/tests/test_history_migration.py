from .conftest import (
    ExampleTestEnvironment,
    HistoryMigrationState,
    NEW_HISTORY_INDEX,
    NEW_HISTORY_TABLE,
    OLD_HISTORY_INDEX,
    OLD_HISTORY_TABLE,
)

from yt.orm.tests.helpers import HistoryMigratorCli

import yt.wrapper as yt

import typing as tp
import pytest


class ExampleHistoryMigratorCli(HistoryMigratorCli):
    SOURCE_TIME_MODE = "logical"
    CHANGE_EVENT_TYPE_SIGN = False


def migrate_history(orm_env, dry_run=False):
    ExampleHistoryMigratorCli.run(orm_env, dry_run, OLD_HISTORY_TABLE, NEW_HISTORY_TABLE)
    ExampleHistoryMigratorCli.run(orm_env, dry_run, OLD_HISTORY_INDEX, NEW_HISTORY_INDEX)


class TestHistoryMigration:
    def test_parametrized_environment(self, example_env: ExampleTestEnvironment, for_both_history_tables):
        yt_client = example_env.yt_client
        db_path = example_env.db_manager.get_db_path()
        history_events = for_both_history_tables[0]

        def total_history_length():
            query_string = "* from [{}]".format(yt.ypath_join(db_path, history_events))
            return len(list(yt_client.select_rows(query_string)))

        initial_length = total_history_length()
        example_env.create_publisher()
        assert initial_length + 1 == total_history_length()

    def test_direct_switch(self, example_env: ExampleTestEnvironment):
        example_env.clean_history()
        yt_client = example_env.yt_client
        db_path = example_env.db_manager.get_db_path()

        publisher = example_env.create_publisher()

        def validate_write_to_certain_table(table_name: str, migration_state: HistoryMigrationState):
            example_env.set_history_migration_state(migration_state)

            query_string = "* from [{}]".format(yt.ypath_join(db_path, table_name))
            initial_row_count = len(list(yt_client.select_rows(query_string)))
            example_env.create_book(publisher)
            final_row_count = len(list(yt_client.select_rows(query_string)))
            assert final_row_count == initial_row_count + 1

        validate_write_to_certain_table(OLD_HISTORY_TABLE, HistoryMigrationState.INITIAL)
        validate_write_to_certain_table(NEW_HISTORY_TABLE, HistoryMigrationState.TARGET)
        validate_write_to_certain_table(OLD_HISTORY_INDEX, HistoryMigrationState.INITIAL)
        validate_write_to_certain_table(NEW_HISTORY_INDEX, HistoryMigrationState.TARGET)

    @pytest.mark.parametrize(
        ("object_type", "selector", "use_index", "create_object", "update_object"),
        [
            pytest.param(
                "publisher",
                "/spec/name",
                False,
                lambda env: env.create_publisher(),
                lambda env, id, param: env.update_publisher_name(id, param),
                id="publisher_history_events",
            ),
            pytest.param(
                "book",
                "/spec/digital_data/available_formats",
                False,
                lambda env: env.create_book(),
                lambda env, id, _: env.append_book_available_format(id),
                id="book_history_events",
            ),
            pytest.param(
                "book",
                "/spec/digital_data/available_formats",
                True,
                lambda env: env.create_book(),
                lambda env, id, _: env.append_book_available_format(id),
                id="book_history_index",
            ),
        ],
    )
    @pytest.mark.parametrize("dry_run", [True, False], ids=["dry-run", "migrated"])
    def test_full_cycle(
        self,
        example_env: ExampleTestEnvironment,
        object_type: str,
        selector: str,
        use_index: bool,
        create_object: tp.Callable[[ExampleTestEnvironment], tp.Any],
        update_object: tp.Callable[[ExampleTestEnvironment, tp.Any, str], None],
        dry_run: bool,
    ):
        def select_object_history(object_id: tp.Any, continuation_token: str = None):
            options = dict()
            if continuation_token is not None:
                options["continuation_token"] = continuation_token
            if use_index:
                options["distinct"] = True
                options["index_mode"] = "enabled"
            result = example_env.client.select_object_history(object_type, str(object_id), [selector], options)
            return (result["events"], result["continuation_token"])

        example_env.set_history_migration_state(HistoryMigrationState.INITIAL)
        object_id = create_object(example_env)
        events, old_token = select_object_history(object_id)
        assert 1 == len(events)

        example_env.set_history_migration_state(HistoryMigrationState.WRITE_BOTH)
        update_object(example_env, object_id, "Second Name")
        events, old_token = select_object_history(object_id, old_token)
        assert 1 == len(events)

        # Optionally run background vanilla migration.
        migrate_history(example_env, dry_run)

        example_env.set_history_migration_state(HistoryMigrationState.AFTER_BACKGROUND_MIGRATION)
        events, old_token = select_object_history(object_id, old_token)
        assert 0 == len(events)

        example_env.set_history_migration_state(HistoryMigrationState.READ_NEW)
        events, old_token = select_object_history(object_id, old_token)
        assert 0 == len(events)

        # In dry run mode single event is not transferred
        optionally_migrated_events = 0 if dry_run else 1
        events, new_token = select_object_history(object_id)
        assert 1 + optionally_migrated_events == len(events)

        update_object(example_env, object_id, "Forth Name")
        events, old_token = select_object_history(object_id, old_token)
        assert 1 == len(events)
        events, new_token = select_object_history(object_id, new_token)
        assert 1 == len(events)

        example_env.set_history_migration_state(HistoryMigrationState.REVOKE_OLD_TOKENS)
        with pytest.raises(yt.YtResponseError):
            select_object_history(object_id, old_token)
        events, new_token = select_object_history(object_id, new_token)
        assert 0 == len(events)

        example_env.set_history_migration_state(HistoryMigrationState.TARGET)
        update_object(example_env, object_id, "Fifth Name")
        events, new_token = select_object_history(object_id, new_token)
        assert 1 == len(events)

    def test_master_and_vanilla_concurrent_writes(self, example_env: ExampleTestEnvironment):
        book = example_env.create_book()

        def update_book():
            example_env.append_book_available_format(book)

        example_env.set_history_migration_state(HistoryMigrationState.INITIAL)
        for _ in range(10):
            update_book()

        example_env.set_history_migration_state(HistoryMigrationState.WRITE_BOTH)
        for _ in range(3):
            migrate_history(example_env)
            update_book()

        old_events = example_env.client.select_object_history(
            "book", str(book), ["/spec/digital_data/available_formats"]
        )["events"]
        example_env.set_history_migration_state(HistoryMigrationState.TARGET)
        new_events = example_env.client.select_object_history(
            "book", str(book), ["/spec/digital_data/available_formats"]
        )["events"]
        assert old_events == new_events
