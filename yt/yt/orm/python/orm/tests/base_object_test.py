from yt.orm.library.common import DuplicateObjectIdError, NoSuchObjectError, NotWatchableError
from yt.yson.yson_types import YsonEntity, YsonMap

from abc import ABCMeta, abstractmethod
from datetime import timedelta
import pytest


class BaseObjectTest(object):
    __metaclass__ = ABCMeta

    @pytest.fixture
    @abstractmethod
    def object_type(self):
        raise NotImplementedError

    @abstractmethod
    def object_watchable(self):
        return False

    @abstractmethod
    def history_enabled_selectors(self):
        # Make sure these are exercised in set_updates.
        return []

    @pytest.fixture
    def object_type_name(self, object_type):
        assert object_type
        return object_type.capitalize().replace("_", " ")

    @pytest.fixture
    def object_spec(self):
        return {}

    @pytest.fixture
    def object_meta(self):
        return {}

    def object_key(self, orm_client, object_type, object_meta, object_spec):
        return orm_client.create_object(
            object_type,
            {"meta": object_meta, "spec": object_spec},
            request_meta_response=True,
        )

    @pytest.fixture(name="object_key")
    def object_key_fixture(self, orm_env, object_type, object_meta, object_spec):
        return self.object_key(orm_env.orm_client, object_type, object_meta, object_spec)

    @pytest.fixture
    def set_updates(self):
        return []

    def test_create(self, object_key):
        assert object_key

    def test_upsert(self, orm_env, object_type, object_key, object_meta, object_spec, set_updates):
        object_meta.update({"key": object_key})
        with pytest.raises(DuplicateObjectIdError):
            orm_env.orm_client.create_object(
                object_type,
                {"meta": object_meta, "spec": object_spec},
                request_meta_response=True,
            )
        key = orm_env.orm_client.create_object(
            object_type,
            {"meta": object_meta, "spec": object_spec},
            request_meta_response=True,
            update_if_existing={"set_updates": set_updates},
        )
        assert object_key == key

    def drop_opaque_values(self, value):
        if not isinstance(value, YsonMap):
            return
        keys = [key for key in value if value[key] == YsonEntity()]
        for key in keys:
            del value[key]
        for key, child in value.items():
            self.drop_opaque_values(child)

    def test_read(self, orm_env, object_type, object_key, object_meta, object_spec):
        meta, spec = orm_env.orm_client.get_object(object_type, object_key, ["/meta", "/spec"])
        self.drop_opaque_values(spec)
        for key in object_meta:
            assert object_meta[key] == meta[key]
        assert object_type == meta["type"]
        assert object_key == meta["key"]
        if "api_revision" in spec:
            del spec["api_revision"]
        assert object_spec == spec

    def test_update(self, orm_env, object_type, object_key, set_updates):
        assert orm_env.orm_client.update_object(object_type, object_key, set_updates)
        for set_update in set_updates:
            (actual,) = orm_env.orm_client.get_object(object_type, object_key, [set_update["path"]])
            assert set_update["value"] == actual

    def test_remove(self, orm_env, object_type, object_key):
        assert orm_env.orm_client.remove_object(object_type, object_key, allow_removal_with_non_empty_reference=True)

        with pytest.raises(NoSuchObjectError):
            orm_env.orm_client.get_object(object_type, object_key, ["/meta"])

        with pytest.raises(NoSuchObjectError):
            orm_env.orm_client.remove_object(object_type, object_key)

    def test_removes(self, orm_env, object_type, object_key):
        assert orm_env.orm_client.remove_objects(
            [(object_type, object_key)], allow_removal_with_non_empty_reference=True
        )

        with pytest.raises(NoSuchObjectError):
            orm_env.orm_client.get_object(object_type, object_key, ["/meta"])

        with pytest.raises(NoSuchObjectError):
            orm_env.orm_client.remove_objects([(object_type, object_key)])

    @abstractmethod
    def test_watch(self, orm_env, object_type, object_key, set_updates):
        start_timestamp = orm_env.orm_client.generate_timestamp()
        if not self.object_watchable():
            with pytest.raises(NotWatchableError):
                orm_env.orm_client.watch_objects(object_type, start_timestamp=start_timestamp)
        else:
            self.test_update(
                orm_env=orm_env,
                object_type=object_type,
                object_key=object_key,
                set_updates=set_updates,
            )

            end_timestamp = orm_env.orm_client.generate_timestamp()

            result = orm_env.orm_client.watch_objects(
                object_type,
                start_timestamp=start_timestamp,
                timestamp=end_timestamp,
                time_limit=timedelta(seconds=5),
                enable_structured_response=True,
                request_meta_response=True,
            )
            events = result["events"]
            assert len(events) == 1
            assert events[0]["meta"]["key"] == object_key

    def test_history(self, orm_env, object_type, object_key, set_updates):
        selectors = self.history_enabled_selectors()
        self.test_update(
            orm_env=orm_env,
            object_type=object_type,
            object_key=object_key,
            set_updates=set_updates,
        )

        result = orm_env.orm_client.select_object_history(
            object_type,
            object_key,
            selectors,
        )
        events = result["events"]
        assert len(events) >= len(selectors)

    def test_remove_non_existent(self, orm_env, object_type, object_key):
        assert orm_env.orm_client.remove_object(object_type, object_key, allow_removal_with_non_empty_reference=True)
        assert orm_env.orm_client.remove_object(object_type, object_key, ignore_nonexistent=True)

    def test_removes_non_existent(self, orm_env, object_type, object_key):
        assert orm_env.orm_client.remove_object(object_type, object_key, allow_removal_with_non_empty_reference=True)
        response = orm_env.orm_client.remove_objects([(object_type, object_key)], ignore_nonexistent=True)
        assert response

    def test_removes_ignore_non_existent(self, orm_env, object_type, object_key, object_meta, object_spec):
        object_key2 = self.object_key(orm_env.orm_client, object_type, object_meta, object_spec)
        assert orm_env.orm_client.remove_object(object_type, object_key2, allow_removal_with_non_empty_reference=True)
        response = orm_env.orm_client.remove_objects(
            [(object_type, object_key), (object_type, object_key2)],
            ignore_nonexistent=True,
            allow_removal_with_non_empty_reference=True,
        )
        assert response

    def test_remove_non_existent_in_transacton(self, orm_env, object_type, object_key):
        transaction_id = orm_env.orm_client.start_transaction()
        orm_env.orm_client.remove_object(
            object_type,
            object_key,
            transaction_id=transaction_id,
            allow_removal_with_non_empty_reference=True,
        )
        orm_env.orm_client.remove_object(
            object_type,
            object_key,
            transaction_id=transaction_id,
            ignore_nonexistent=True,
        )
        orm_env.orm_client.commit_transaction(transaction_id)
        with pytest.raises(NoSuchObjectError):
            orm_env.orm_client.get_object(object_type, object_key, ["/meta"])
