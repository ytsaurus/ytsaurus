from .conftest import Cli
from .test_incomplete_base import IncompleteBaseTest

from yt.orm.library.common import YtResponseError
import yt.yson as yson

import yatest.common

import pytest


class SlicerCli(Cli):
    def __init__(self):
        super(SlicerCli, self).__init__(
            yatest.common.binary_path("yt/yt/orm/tools/migration/slicer/slicer")
        )


class TouchIndexCli(Cli):
    def __init__(self):
        super(TouchIndexCli, self).__init__(
            yatest.common.binary_path("yt/yt/orm/example/tools/touch_index/touch_index")
        )


class TestIndexBuilding:
    NAME_INDEX_TABLE = "//home/example/db/editors_by_name"
    ACHIEVEMENTS_INDEX_TABLE = "//home/example/db/editors_by_achievements"
    POST_INDEX_TABLE = "//home/example/db/editors_by_post"
    PHONE_NUMBER_INDEX_TABLE = "//home/example/db/editors_by_phone_number"
    GENRE_BY_NAME_TABLE = "//home/example/db/genre_by_name"

    @pytest.fixture
    def clear_index_tables(self, example_env):
        def _inner():
            for index_table_name in (
                self.NAME_INDEX_TABLE,
                self.ACHIEVEMENTS_INDEX_TABLE,
                self.POST_INDEX_TABLE,
                self.PHONE_NUMBER_INDEX_TABLE,
            ):
                example_env.clear_dummy_content_table(index_table_name)

        return _inner

    @pytest.fixture(autouse=True)
    def clear_index_tables_before_test(self, clear_index_tables):
        return clear_index_tables()

    @pytest.fixture
    def create_editor(self, example_env):
        def _inner(name):
            return example_env.client.create_object(
                "editor",
                attributes={
                    "spec": {
                        "name": name,
                        "achievements": ["The fastest in 2020", "The best editor of fantasy"],
                        "post": "Senior editor",
                        "phone_number": "+7 (912) 680-20-75",
                    },
                },
                enable_structured_response=True,
                request_meta_response=True,
            )["meta"]["id"]

        return _inner

    @pytest.fixture
    def editor_id(self, create_editor):
        return create_editor(name="Stephan Nait")

    @pytest.fixture
    def assert_index_table(self, example_env):
        def _inner(editor_id, name=None, achievements=[], post=None, phone_number=None):
            assert [
                {
                    "name": i,
                    "editor_id": editor_id,
                    "dummy": None,
                }
                for i in [name]
                if i is not None
            ] == sorted(
                example_env.get_table_rows(self.NAME_INDEX_TABLE),
                key=lambda row: row["name"],
            )
            assert [
                {
                    "achievements": achievement,
                    "editor_id": editor_id,
                    "dummy": None,
                }
                for achievement in sorted(achievements)
            ] == sorted(
                example_env.get_table_rows(self.ACHIEVEMENTS_INDEX_TABLE),
                key=lambda row: row["achievements"],
            )
            assert [
                {
                    "post": i,
                    "editor_id": editor_id,
                    "dummy": None,
                }
                for i in [post]
                if i is not None
            ] == sorted(
                example_env.get_table_rows(self.POST_INDEX_TABLE),
                key=lambda row: row["post"],
            )
            assert [
                {
                    "phone_number": i,
                    "editor_id": editor_id,
                    "dummy": None,
                }
                for i in [phone_number]
                if i is not None
            ] == sorted(
                example_env.get_table_rows(self.PHONE_NUMBER_INDEX_TABLE),
                key=lambda row: row["phone_number"],
            )

        return _inner

    def test_create(self, clear_index_tables, assert_index_table, create_editor):
        clear_index_tables()
        editor_id = create_editor(name="Eldar Fatachov")
        assert_index_table(
            editor_id=editor_id,
            name="Eldar Fatachov",
            achievements=["The fastest in 2020", "The best editor of fantasy"],
            post="Senior editor",
            phone_number="+7 (912) 680-20-75",
        )

    def test_remove(
        self,
        clear_index_tables,
        editor_id,
        example_env,
        assert_index_table,
    ):
        clear_index_tables()
        example_env.client.remove_object("editor", editor_id)
        assert_index_table(editor_id)

    @pytest.mark.parametrize(
        "post",
        [
            pytest.param("Senior editor", id="zero diff"),
            pytest.param("Superior editor", id="different value"),
        ],
    )
    def test_update_scalar_etc_column(
        self,
        clear_index_tables,
        editor_id,
        example_env,
        assert_index_table,
        post,
    ):
        clear_index_tables()
        example_env.client.update_object(
            "editor",
            editor_id,
            set_updates=[{"path": "/spec/post", "value": post}],
        )
        assert_index_table(editor_id=editor_id, post=post)

    @pytest.mark.parametrize(
        "phone_number",
        [
            pytest.param("+7 (912) 680-20-75", id="zero diff"),
            pytest.param("5213", id="different value"),
        ],
    )
    def test_update_scalar_separate_column(
        self,
        clear_index_tables,
        editor_id,
        example_env,
        assert_index_table,
        phone_number,
    ):
        clear_index_tables()
        example_env.client.update_object(
            "editor",
            editor_id,
            set_updates=[{"path": "/spec/phone_number", "value": phone_number}],
        )
        assert_index_table(editor_id=editor_id, phone_number=phone_number)

    @pytest.mark.parametrize(
        "achievements",
        [
            pytest.param(
                ["The fastest in 2020", "The best editor of fantasy"],
                id="zero diff",
            ),
            pytest.param(
                ["The fastest in 2020", "The best editor of fantasy", "World editor reward"],
                id="add item",
            ),
            pytest.param(
                ["The fastest in 2020"],
                id="remove item",
            ),
            pytest.param(
                ["World editor reward"],
                id="change all items",
            ),
        ],
    )
    def test_update_repeated(
        self,
        clear_index_tables,
        editor_id,
        example_env,
        assert_index_table,
        achievements,
    ):
        clear_index_tables()
        example_env.client.update_object(
            "editor",
            editor_id,
            set_updates=[{"path": "/spec/achievements", "value": achievements}],
        )
        assert_index_table(editor_id=editor_id, achievements=achievements)

    @pytest.mark.parametrize(
        "index_name,actual",
        [
            pytest.param(
                "editors_by_name",
                dict(name="Stephan Nait"),
                id="scalar_by_name",
            ),
            pytest.param(
                "editors_by_achievements",
                dict(achievements=["The best editor of fantasy", "The fastest in 2020"]),
                id="repeated",
            ),
            pytest.param(
                "editors_by_post",
                dict(post="Senior editor"),
                id="scalar_by_post",
            ),
            pytest.param(
                "editors_by_phone_number",
                dict(phone_number="+7 (912) 680-20-75"),
                id="scalar_by_phone_number",
            ),
        ],
    )
    def test_touch_index(
        self,
        clear_index_tables,
        editor_id,
        example_env,
        assert_index_table,
        index_name,
        actual,
    ):
        clear_index_tables()
        example_env.client.update_object(
            "editor",
            editor_id,
            set_updates=[{"path": "/control/touch_index", "value": {"index_names": [index_name]}}],
        )
        assert_index_table(editor_id=editor_id, **actual)

    def test_touch_indices(
        self,
        clear_index_tables,
        editor_id,
        example_env,
        assert_index_table,
    ):
        indices = ["editors_by_name", "editors_by_achievements", "editors_by_post", "editors_by_phone_number"]
        clear_index_tables()
        example_env.client.update_object(
            "editor",
            editor_id,
            set_updates=[
                {
                    "path": "/control/touch_index",
                    "value": {"index_names": [index_name for index_name in indices]},
                }
            ],
        )
        expected = {
            "phone_number": "+7 (912) 680-20-75",
            "name": "Stephan Nait",
            "post": "Senior editor",
            "achievements": ["The best editor of fantasy", "The fastest in 2020"],
        }
        assert_index_table(editor_id=editor_id, **expected)

    def test_touch_unknown_index(self, editor_id, example_env):
        with pytest.raises(YtResponseError, match="No such index"):
            example_env.client.update_object(
                "editor",
                editor_id,
                set_updates=[
                    {"path": "/control/touch_index", "value": {"index_names": ["unknown_index"]}}
                ],
            )

    def test_dynconfig(self, editor_id, clear_index_tables, example_env):
        def assert_achievements_index_table(achievements):
            assert [
                {
                    "achievements": achievement,
                    "editor_id": editor_id,
                    "dummy": None,
                }
                for achievement in sorted(achievements)
            ] == sorted(
                example_env.get_table_rows(self.ACHIEVEMENTS_INDEX_TABLE),
                key=lambda row: row["achievements"],
            )

        def update_editor_achievemens(achievements):
            example_env.client.update_object(
                "editor",
                editor_id,
                set_updates=[{"path": "/spec/achievements", "value": achievements}],
            )

        update_editor_achievemens(["First achievement"])
        assert_achievements_index_table(["First achievement"])
        clear_index_tables()

        config = {
            "object_manager": {"index_mode_per_name": {"editors_by_achievements": "disabled"}}
        }
        with example_env.set_cypress_config_patch_in_context(config):
            update_editor_achievemens(["Second achievement"])
            assert_achievements_index_table([])

    def test_touch_unique_index(
        self,
        example_env,
    ):
        genre_id = example_env.client.create_object(
            "genre",
            attributes={"spec": {"name": "fantasy"}},
            enable_structured_response=True,
            request_meta_response=True,
        )["meta"]["id"]
        example_env.clear_table(self.GENRE_BY_NAME_TABLE, lambda key: key == "name")

        example_env.client.update_object(
            "genre",
            genre_id,
            set_updates=[
                {"path": "/control/touch_index", "value": {"index_names": ["genre_by_name"]}}
            ],
        )
        assert [{"name": "fantasy", "genre_id": genre_id}] == example_env.get_table_rows(
            self.GENRE_BY_NAME_TABLE
        )

    class TestMigration:
        def test_restore_index(
            self,
            create_editor,
            clear_index_tables,
            example_env,
        ):
            yt_client = example_env.yt_client

            # 1. Prepare test data.
            editor_ids = [create_editor(name="Editor #{}".format(idx)) for idx in range(7)]
            clear_index_tables()

            # 2. Backup.
            source_table = "//tmp/sources"
            yt_client.write_table(
                source_table,
                [
                    row
                    for row in yt_client.select_rows("* from [//home/example/db/editors]")
                    if row["meta.id"] in editor_ids
                ],
            )
            yt_client.run_sort(source_table, sort_by=["meta.id"])
            assert yt_client.row_count(source_table) == 7

            # 1.1. Remove some objects from source list.
            example_env.client.remove_object("editor", editor_ids[-1])

            # 3. Slice source table.
            slices_table = "//tmp/slices"
            proxy_url = yt_client.config["proxy"]["url"]
            job_count = 2
            SlicerCli().check_call([
                "--source-table",
                source_table,
                "--destination-table",
                slices_table,
                "--slice-count",
                str(job_count),
                "--proxy",
                proxy_url,
                "--force-create-destination",
            ])

            # 4. Run touch index operation.
            worker_config_path = yatest.common.work_path("worker_config.json")
            with open(worker_config_path, "wb") as file:
                file.write(
                    yson.dumps(
                        {
                            "cluster": proxy_url,
                            "job_count": job_count,
                        }
                    )
                )
            operation_config_path = yatest.common.work_path("operation_config.json")
            with open(operation_config_path, "wb") as file:
                file.write(
                    yson.dumps(
                        {
                            "cluster": proxy_url,
                            "orm_token": "",
                            "source_table": source_table,
                            "slices_table": slices_table,
                            "index_name": "editors_by_name",
                            "object_key_columns": ["meta.id"],
                            "orm_connection_config": {
                                "secure": False,
                                "discovery_address": example_env.example_instance.orm_client_grpc_address,
                            },
                        }
                    )
                )
            touch_index = TouchIndexCli()
            touch_index.set_env_patch({"YT_TOKEN": "<non_empty_string>"})
            touch_index.check_call([
                "--worker-config-path",
                worker_config_path,
                "--operation-config-path",
                operation_config_path,
            ])

            # 5. Verify index table.
            assert [
                {
                    "name": "Editor #{}".format(idx),
                    "editor_id": editor_id,
                    "dummy": None,
                }
                for idx, editor_id in zip(range(6), editor_ids)
            ] == sorted(
                example_env.get_table_rows("//home/example/db/editors_by_name"),
                key=lambda row: (row["name"], row["editor_id"]),
            )


class TestIndexIncomplete(IncompleteBaseTest):
    EXAMPLE_MASTER_CONFIG = {
        "transaction_manager": {
            "scalar_attribute_loads_null_as_typed": True,
        },
    }

    @pytest.fixture
    def object_type(self):
        return "book"

    @pytest.fixture
    def old_value(self):
        return 2003

    @pytest.fixture
    def new_value(self):
        return 2004

    @pytest.fixture
    def publisher_id(self, example_env):
        return example_env.client.create_object(
            "publisher",
            enable_structured_response=True,
            request_meta_response=True,
        )["meta"]["id"]

    @pytest.fixture
    def object_id(self, example_env, publisher_id, old_value):
        return example_env.client.create_object(
            "book",
            {
                "meta": {
                    "isbn": "978-1449355739",
                    "parent_key": str(publisher_id),
                },
                "spec": {
                    "year": old_value,
                },
            },
            enable_structured_response=True,
            request_meta_response=True,
        )["meta"]["key"]

    @pytest.fixture
    def primary_key(self, publisher_id, object_id):
        parts = str(object_id).split(';')
        if len(parts) == 2:
            return {"meta.publisher_id": publisher_id, "meta.id": int(parts[0]), "meta.id2": int(parts[1])}
        return {"meta.publisher_id": publisher_id, "meta.id": object_id}

    @pytest.fixture
    def incomplete_path_items(self):
        return ["spec", "year"]
