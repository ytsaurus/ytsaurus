import pytest

from abc import ABCMeta, abstractmethod
from datetime import timedelta

from yt.yson.yson_types import YsonEntity

from .conftest import format_object_key


class IncompleteBaseTest(object):
    __metaclass__ = ABCMeta

    @pytest.fixture
    @abstractmethod
    def object_type(self):
        raise NotImplementedError

    @pytest.fixture
    @abstractmethod
    def object_id(self):
        raise NotImplementedError

    @pytest.fixture
    @abstractmethod
    def incomplete_path_items(self):
        raise NotImplementedError

    @pytest.fixture
    @abstractmethod
    def old_value(self):
        raise NotImplementedError

    @pytest.fixture
    @abstractmethod
    def new_value(self):
        raise NotImplementedError

    @pytest.fixture
    def client(self, example_env):
        return example_env.client

    @pytest.fixture
    def yt_client(self, example_env):
        return example_env.yt_client

    ################################################################################

    @pytest.fixture
    def table_name(self, object_type):
        return "//home/example/db/{}s".format(object_type)

    @pytest.fixture
    def incomplete_path(self, incomplete_path_items):
        return "/".join([""] + incomplete_path_items)

    @pytest.fixture
    def incomplete_column(self, incomplete_path_items):
        return ".".join(incomplete_path_items)

    @pytest.fixture
    def primary_key(self, object_id):
        parts = str(object_id).split(';')
        if len(parts) == 2:
            return {"meta.id": int(parts[0]), "meta.id2": int(parts[1])}
        return {"meta.id": object_id}

    @pytest.fixture
    def lookup_row(self, yt_client, table_name, primary_key):
        def _inner():
            rows = list(yt_client.lookup_rows(table_name, [primary_key]))
            assert len(rows) == 1
            return rows[0]

        return _inner

    @pytest.fixture
    def incomplete_id(
        self,
        client,
        lookup_row,
        yt_client,
        table_name,
        object_type,
        object_id,
        old_value,
        incomplete_path,
        incomplete_column,
    ):
        assert (
            client.get_object(
                object_type, format_object_key(object_id), selectors=[incomplete_path]
            )[0]
            == old_value
        )

        row = lookup_row()
        assert row[incomplete_column] == old_value

        del row[incomplete_column]
        if "hash" in row:
            del row["hash"]
        yt_client.insert_rows(table_name, [row])

        return object_id

    @pytest.fixture
    def select_query(self, incomplete_id):
        parts = str(incomplete_id).split(';')
        if len(parts) == 2:
            return "[/meta/id]={} AND [/meta/id2]={}".format(parts[0], parts[1])
        return "[/meta/id]={}".format(incomplete_id)

    def test_incomplete_stored(self, incomplete_id, lookup_row, incomplete_column):
        row = lookup_row()
        assert row[incomplete_column] is None

    def test_incomplete_read(
        self, incomplete_id, client, object_type, incomplete_path
    ):
        assert (
            client.get_object(
                object_type, format_object_key(incomplete_id), selectors=[incomplete_path]
            )[0]
            == YsonEntity()
        )

    def test_incomplete_update(
        self, incomplete_id, client, object_type, incomplete_path, new_value
    ):
        client.update_object(
            object_type,
            format_object_key(incomplete_id),
            set_updates=[{"path": incomplete_path, "value": new_value}],
        )
        actual = client.get_object(
            object_type, format_object_key(incomplete_id), selectors=[incomplete_path]
        )
        assert actual[0] == new_value

    def test_incomplete_delete(self, incomplete_id, client, object_type):
        client.remove_object(
            object_type,
            format_object_key(incomplete_id),
        )

    def test_incomplete_select(
        self, incomplete_id, client, object_type, select_query, incomplete_path
    ):
        actual = client.select_objects(
            object_type,
            filter=select_query,
            selectors=[incomplete_path],
        )
        assert actual[0] == [YsonEntity()]

    def test_incomplete_history_create(
        self, incomplete_id, client, object_type, incomplete_path, old_value
    ):
        actual = client.select_object_history(
            object_type,
            format_object_key(incomplete_id),
            [incomplete_path],
        )
        assert 1 == len(actual["events"])
        assert old_value == actual["events"][0]["results"][0]["value"]

    def test_incomplete_history_update(
        self, incomplete_id, client, object_type, incomplete_path, new_value
    ):
        client.update_object(
            object_type,
            format_object_key(incomplete_id),
            set_updates=[{"path": incomplete_path, "value": new_value}],
        )
        actual = client.select_object_history(
            object_type,
            format_object_key(incomplete_id),
            [incomplete_path],
        )
        assert 2 == len(actual["events"])
        assert new_value == actual["events"][1]["results"][0]["value"]

    def test_incomplete_watch_update(
        self, incomplete_id, client, object_type, incomplete_path, new_value
    ):
        start_timestamp = client.generate_timestamp()
        client.update_object(
            object_type,
            format_object_key(incomplete_id),
            set_updates=[{"path": incomplete_path, "value": new_value}],
        )
        actual = client.watch_objects(
            object_type,
            start_timestamp=start_timestamp,
            timestamp=client.generate_timestamp(),
            time_limit=timedelta(seconds=5),
            enable_structured_response=True,
            request_meta_response=True,
        )
        events = actual["events"]
        assert len(events) == 1
        assert str(events[0]["meta"]["key"]) == str(incomplete_id)
