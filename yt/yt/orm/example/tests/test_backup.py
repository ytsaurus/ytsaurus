from yt.orm.tests.helpers import sync_master_yt_connection_caches

from yt.yt.orm.example.python.admin.data_model_traits import (
    ExampleDataModelTraits,
    EXAMPLE_DATA_MODEL_TRAITS,
)

from yt.orm.admin.db_operations import (
    ConsistentTimestampGenerator,
    DbManager,
    backup_by_copy,
    backup_orm,
    clone_db,
    freeze_orm,
    unfreeze_orm,
)

from yt.wrapper.ypath import ypath_join

import pytest


class TestTimestampGenerator:
    def test_generator_call(self, example_env):
        db_manager = example_env.db_manager
        yt_client = db_manager.yt_client
        tables = db_manager.get_tables_list(skip_watch_logs=True)
        generator = ConsistentTimestampGenerator.create_simple(tables, db_manager=db_manager)
        ts = generator.generate()
        freeze_orm(yt_client, db_manager.orm_path, EXAMPLE_DATA_MODEL_TRAITS)
        generator.check_generated_timestamp_obsoletion(ts)

        unfreeze_orm(yt_client, db_manager.orm_path, EXAMPLE_DATA_MODEL_TRAITS)

    @pytest.mark.parametrize(
        "enable_dynamic_store_read,merge_rows_on_flush",
        [
            (True, True),
            (True, False),
            (False, True),
            (False, False),
        ],
    )
    def test_dynamic_store_read_check(
        self, example_env, enable_dynamic_store_read, merge_rows_on_flush
    ):
        db_manager = example_env.db_manager
        yt_client = db_manager.yt_client
        tables = db_manager.get_tables_list(skip_watch_logs=True)
        generator = ConsistentTimestampGenerator.create_simple(
            [tables[0]],
            db_manager=db_manager,
        )

        db_manager.unmount_tables(tables)

        path = db_manager.get_table_path(tables[0])

        yt_client.set(path + "/@enable_dynamic_store_read", enable_dynamic_store_read)
        yt_client.set(path + "/@merge_rows_on_flush", merge_rows_on_flush)

        db_manager.mount_unmounted_tables()

        ts = yt_client.generate_timestamp()
        bounds = generator.get_retained_and_unflushed_timestamp_bounds()
        if enable_dynamic_store_read and not merge_rows_on_flush:
            assert ts < bounds[1]
        else:
            assert bounds[1] < ts

    def test_generator_call_replicated_cluster(self, example_env_with_replicas):
        replica_db_manager = example_env_with_replicas.replicas_db_managers[0]
        yt_client = replica_db_manager.yt_client
        tables = example_env_with_replicas.db_manager.get_tables_list(skip_watch_logs=True)
        generator = ConsistentTimestampGenerator.create_replicated(
            tables,
            meta_db_manager=example_env_with_replicas.db_manager,
            replica_db_manager=replica_db_manager,
        )
        ts = generator.generate()
        freeze_orm(yt_client, replica_db_manager.orm_path, EXAMPLE_DATA_MODEL_TRAITS)
        generator.check_generated_timestamp_obsoletion(ts)

        unfreeze_orm(yt_client, replica_db_manager.orm_path, EXAMPLE_DATA_MODEL_TRAITS)


class TestBackup:
    def test_local_copy(self, example_env):
        db_manager = example_env.db_manager
        yt_client = db_manager.yt_client

        example_env.create_book()

        path_to_copy = "//home/example_copy"
        backup_by_copy(
            source_yt_client=yt_client,
            source_orm_path=db_manager.get_orm_path(),
            target_yt_client=yt_client,
            target_orm_path=path_to_copy,
            remove_if_exists=True,
            data_model_traits=EXAMPLE_DATA_MODEL_TRAITS,
        )
        tables = yt_client.list(ypath_join(path_to_copy, "db"), attributes=["upstream_replica_id"])
        for table in tables:
            assert (
                table.attributes["upstream_replica_id"] == "0-0-0-0"
            ), "Table {} has non-zero \"upstream_replica_id\" attribute ({})".format(table, table.attributes["upstream_replica_id"])

        original_rows = [row for row in yt_client.select_rows("* from [//home/example/db/books]")]
        copy_of_books = ypath_join(path_to_copy, "db/books")
        # Tables are in unmount state after copying so it is necessary to mount them.
        yt_client.mount_table(copy_of_books, sync=True)
        copied_rows = [
            row for row in yt_client.select_rows(f"* from [{copy_of_books}]")
        ]
        assert original_rows == copied_rows and len(copied_rows) == 1
        yt_client.remove(path_to_copy, recursive=True)

    @pytest.mark.parametrize("backup_function", [backup_by_copy, clone_db])
    @pytest.mark.parametrize("preferred_medium", ["default", "ssd_blobs"])
    def test_backup_while_list_options(self, example_env, backup_function, preferred_medium):
        db_manager = example_env.db_manager
        yt_client = db_manager.yt_client
        path_to_copy = "//home/example_backup"

        backup_function(
            source_yt_client=yt_client,
            source_orm_path=db_manager.get_orm_path(),
            target_yt_client=yt_client,
            target_orm_path=path_to_copy,
            remove_if_exists=True,
            data_model_traits=EXAMPLE_DATA_MODEL_TRAITS,
            preferred_medium=preferred_medium,
        )

        tables = yt_client.list(ypath_join(path_to_copy, "db"), attributes=["primary_medium"])
        assert set(str(table) for table in tables) == set(
            EXAMPLE_DATA_MODEL_TRAITS.get_tables_for_backup()
        )
        assert all((table.attributes["primary_medium"] == preferred_medium for table in tables))

        yt_client.remove(path_to_copy, recursive=True)

    @pytest.mark.parametrize("backup_function", [backup_by_copy, clone_db])
    @pytest.mark.parametrize(
        "include_tables, exclude_tables",
        [(None, ["authors", "books"]), (["authors", "books"], None)],
    )
    def test_backup_include_and_exclude_tables(
        self, example_env, backup_function, include_tables, exclude_tables
    ):
        class DataModelTraitsWithoutTablesForBackup(ExampleDataModelTraits):
            def get_tables_for_backup(self):
                return None

        db_manager = example_env.db_manager
        yt_client = db_manager.yt_client
        path_to_copy = "//home/example_backup"

        backup_function(
            source_yt_client=yt_client,
            source_orm_path=db_manager.get_orm_path(),
            target_yt_client=yt_client,
            target_orm_path=path_to_copy,
            remove_if_exists=True,
            data_model_traits=DataModelTraitsWithoutTablesForBackup(),
            exclude_tables=exclude_tables,
            include_tables=include_tables,
        )

        if include_tables:
            assert set(yt_client.list("{}/db".format(path_to_copy))) == set(include_tables)
        else:
            assert set(yt_client.list("{}/db".format(path_to_copy))) == set(
                db_manager.get_tables_list(skip_watch_logs=True)
            ).difference(set(exclude_tables))
        yt_client.remove(path_to_copy, recursive=True)

    def test_native_backup(self, example_env):
        db_manager = example_env.db_manager
        yt_client = db_manager.yt_client
        tables = db_manager.get_tables_list()
        path_to_copy = "//home/example_copy"
        backup_manager = DbManager(yt_client, path_to_copy, db_manager.get_data_model_traits())

        yt_client.set("//sys/@config/tablet_manager/enable_backups", True)
        db_manager.unmount_tables(tables)
        db_manager.batch_apply(
            lambda path, client: client.set(path + "/@enable_dynamic_store_read", True), tables
        )
        db_manager.mount_unmounted_tables()

        sync_master_yt_connection_caches(example_env, "publisher", all_watch_logs=True)
        sync_master_yt_connection_caches(example_env, "book", all_watch_logs=True)

        book = example_env.create_book()

        backup_orm(
            yt_client,
            db_manager.get_orm_path(),
            db_manager.get_data_model_traits(),
            [path_to_copy],
            use_native_backup=True,
            convert_to_dynamic=True,
        )
        assert set(backup_manager.get_tables_list()) == set(tables)
        backup_manager.mount_all_tables()
        books = list(yt_client.select_rows("* from [{}]".format(path_to_copy + "/db/books")))
        assert len(books) == 1
        assert "{};{}".format(books[0]["meta.id"], books[0]["meta.id2"]) == book

        yt_client.remove(path_to_copy, recursive=True)
