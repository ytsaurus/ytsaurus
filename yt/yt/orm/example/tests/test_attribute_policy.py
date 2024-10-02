from yt.orm.library.common import InvalidObjectIdError

from yt.wrapper.errors import YtTabletTransactionLockConflict, YtError

from yt.common import YtResponseError, wait

import pytest


def _is_monotonic(sequence):
    if not sequence:
        return True

    last = sequence[0]
    for next in sequence[1:]:
        if last >= next:
            return False
        last = next
    return True


def _is_contiguous(sequence, start=None):
    if not sequence:
        return True

    last = sequence[0]
    if start and start != last:
        return False
    for next in sequence[1:]:
        if last != (next - 1):
            return False
        last = next
    return True


def _is_timestamp_buffer_filled(client):
    try:
        client.create_object("buffered_timestamp_id", request_meta_response=True)
    except YtError:
        return False
    return True


class TestManualAttributePolicy:
    def test_id_is_required(self, example_env):
        with pytest.raises(InvalidObjectIdError):
            example_env.client.create_object("manual_id", request_meta_response=True)

    def test_validation(self, example_env):
        with pytest.raises(InvalidObjectIdError):
            example_env.client.create_object(
                "manual_id",
                {
                    "meta": {
                        "str_id": "abcd",
                        "i64_id": -100,
                        "ui64_id": 100,
                    },
                },
                request_meta_response=True,
            )

        with pytest.raises(InvalidObjectIdError):
            example_env.client.create_object(
                "manual_id",
                {
                    "meta": {
                        "str_id": "abcdeabcdea",
                        "i64_id": -100,
                        "ui64_id": 100,
                    },
                },
                request_meta_response=True,
            )

        with pytest.raises(InvalidObjectIdError):
            example_env.client.create_object(
                "manual_id",
                {
                    "meta": {
                        "str_id": "abcdef",
                        "i64_id": -100,
                        "ui64_id": 100,
                    },
                },
                request_meta_response=True,
            )

        with pytest.raises(InvalidObjectIdError):
            example_env.client.create_object(
                "manual_id",
                {
                    "meta": {
                        "str_id": "abcde",
                        "i64_id": -100,
                        "ui64_id": 100,
                    },
                    "spec": {"str_value": "bar"},
                },
                request_meta_response=True,
            )

            with pytest.raises(InvalidObjectIdError):
                example_env.client.create_object(
                    "manual_id",
                    {
                        "meta": {
                            "str_id": "abcde",
                            "i64_id": -100,
                            "ui64_id": 100,
                        },
                        "spec": {"i32_value": -100},
                    },
                    request_meta_response=True,
                )
        with pytest.raises(InvalidObjectIdError):
            example_env.client.create_object(
                "manual_id",
                {
                    "meta": {
                        "str_id": "abcde",
                        "i64_id": -100,
                        "ui64_id": 100,
                    },
                    "spec": {"ui32_value": 100},
                },
                request_meta_response=True,
            )

    def test_create(self, example_env):
        resp = example_env.client.create_object(
            "manual_id",
            {
                "meta": {
                    "str_id": "abcde",
                    "i64_id": -100,
                    "ui64_id": 100,
                },
                "spec": {
                    "str_value": "foo",
                    "i32_value": -42,
                    "ui32_value": 42,
                },
            },
            enable_structured_response=True,
            request_meta_response=True,
        )

        assert resp["meta"]["str_id"] == "abcde"
        assert resp["meta"]["i64_id"] == -100
        assert resp["meta"]["ui64_id"] == 100
        spec = example_env.client.get_object("manual_id", resp["meta"], selectors=["/spec"])[0]
        assert spec["str_value"] == "foo"
        assert spec["i32_value"] == -42
        assert spec["ui32_value"] == 42


class TestRandomAttributePolicy:
    def test_create(self, example_env):
        OBJECT_COUNT = 100
        str_ids = []
        i64_ids = []
        ui64_ids = []
        str_values = []

        subresponses = example_env.client.create_objects(
            (("random_id", dict()) for _ in range(OBJECT_COUNT)),
            request_meta_response=True,
            enable_structured_response=True,
        )["subresponses"]
        for subresponse in subresponses:
            str_ids.append(subresponse["meta"]["str_id"])
            i64_ids.append(subresponse["meta"]["i64_id"])
            ui64_ids.append(subresponse["meta"]["ui64_id"])

        specs = example_env.client.get_objects(
            "random_id",
            [subresponse["meta"] for subresponse in subresponses],
            selectors=["/spec"],
        )
        for spec in specs:
            str_values.append(spec[0]["str_value"])
        assert len(set(str_ids)) == len(str_ids)
        assert len(set(i64_ids)) == len(i64_ids)
        assert len(set(ui64_ids)) == len(ui64_ids)
        assert len(set(str_values)) == len(str_values)

    def test_manual(self, example_env):
        resp = example_env.client.create_object(
            "random_id",
            {
                "meta": {
                    "str_id": "a",
                    "i64_id": -100,
                    "ui64_id": 100,
                },
                "spec": {"str_value": "foo"},
            },
            enable_structured_response=True,
            request_meta_response=True,
        )

        assert resp["meta"]["str_id"] == "a"
        assert resp["meta"]["i64_id"] == -100
        assert resp["meta"]["ui64_id"] == 100
        spec = example_env.client.get_object("random_id", resp["meta"], selectors=["/spec"])[0]
        assert spec["str_value"] == "foo"


class TestTimestampAttributePolicy:
    def test_create(self, example_env):
        OBJECT_COUNT = 100
        i64_ids = []
        ui64_ids = []
        ui64_values = []

        subresponses = example_env.client.create_objects(
            (("timestamp_id", dict()) for _ in range(OBJECT_COUNT)),
            request_meta_response=True,
            enable_structured_response=True,
        )["subresponses"]
        for subresponse in subresponses:
            i64_ids.append(subresponse["meta"]["i64_id"])
            ui64_ids.append(subresponse["meta"]["ui64_id"])

        specs = example_env.client.get_objects(
            "timestamp_id",
            [subresponse["meta"] for subresponse in subresponses],
            selectors=["/spec"],
        )
        for spec in specs:
            ui64_values.append(spec[0]["ui64_value"])

        assert _is_monotonic(i64_ids)
        assert _is_monotonic(ui64_ids)
        assert _is_monotonic(ui64_values)

    def test_manual(self, example_env):
        resp = example_env.client.create_object(
            "timestamp_id",
            {
                "meta": {
                    "i64_id": -100,
                    "ui64_id": 100,
                },
                "spec": {"ui64_value": 42},
            },
            enable_structured_response=True,
            request_meta_response=True,
        )

        assert resp["meta"]["i64_id"] == -100
        assert resp["meta"]["ui64_id"] == 100
        spec = example_env.client.get_object("timestamp_id", resp["meta"], selectors=["/spec"])[0]
        assert spec["ui64_value"] == 42


class TestBufferedTimestampAttributePolicy:
    def test_create(self, example_env):
        example_config = {
            "transaction_manager": {
                "timestamp_buffer_enabled": True,
                "timestamp_buffer_update_period": 500,
            }
        }
        with example_env.set_cypress_config_patch_in_context(example_config):
            wait(lambda: _is_timestamp_buffer_filled(example_env.client))
            OBJECT_COUNT = 100
            i64_ids = []
            ui64_ids = []
            i64_values = []

            subresponses = example_env.client.create_objects(
                (("buffered_timestamp_id", dict()) for _ in range(OBJECT_COUNT)),
                request_meta_response=True,
                enable_structured_response=True,
            )["subresponses"]
            for subresponse in subresponses:
                i64_ids.append(subresponse["meta"]["i64_id"])
                ui64_ids.append(subresponse["meta"]["ui64_id"])

            specs = example_env.client.get_objects(
                "buffered_timestamp_id",
                [subresponse["meta"] for subresponse in subresponses],
                selectors=["/spec"],
            )
            for spec in specs:
                i64_values.append(spec[0]["i64_value"])

            assert _is_monotonic(i64_ids)
            assert _is_monotonic(ui64_ids)
            assert _is_monotonic(i64_values)

    def test_manual(self, example_env):
        resp = example_env.client.create_object(
            "buffered_timestamp_id",
            {
                "meta": {
                    "i64_id": -100,
                    "ui64_id": 100,
                },
                "spec": {"i64_value": -42},
            },
            enable_structured_response=True,
            request_meta_response=True,
        )

        assert resp["meta"]["i64_id"] == -100
        assert resp["meta"]["ui64_id"] == 100
        spec = example_env.client.get_object(
            "buffered_timestamp_id", resp["meta"], selectors=["/spec"]
        )[0]
        assert spec["i64_value"] == -42


class TestIndexedIncrementAttributePolicy:
    def test_create_key(self, example_env):
        OBJECT_COUNT = 100
        i64_ids = []
        metas = []
        for _ in range(OBJECT_COUNT):
            resp = example_env.client.create_object(
                "indexed_increment_id",
                enable_structured_response=True,
                request_meta_response=True,
            )
            i64_ids.append(resp["meta"]["i64_id"])
            metas.append(resp["meta"])

        assert _is_contiguous(i64_ids, -42)

        # The range is only 100 values.
        with pytest.raises(YtResponseError):
            example_env.client.create_object(
                "indexed_increment_id",
                enable_structured_response=True,
                request_meta_response=True,
            )

        example_env.client.remove_objects([("indexed_increment_id", meta) for meta in metas])

        example_env.client.create_object(
            "indexed_increment_id",
            enable_structured_response=True,
            request_meta_response=True,
        )

    # def test_create_value(self, example_env):
    #     OBJECT_COUNT = 100
    #     metas = []
    #     ui32_values = []
    #     for _ in range(OBJECT_COUNT):
    #         resp = example_env.client.create_object(
    #             "indexed_increment_value", enable_structured_response=True, request_meta_response=True
    #         )
    #         metas.append(resp["meta"])
    #         spec = example_env.client.get_object(
    #             "indexed_increment_value", resp["meta"], selectors=["/spec"]
    #         )[0]
    #         ui32_values.append(spec["ui32_value"])

    #     assert _is_contiguous(ui32_values, 43)

    #     # The range is only 100 values.
    #     with pytest.raises(YtResponseError):
    #         example_env.client.create_object(
    #             "indexed_increment_value", enable_structured_response=True, request_meta_response=True
    #         )

    #     example_env.client.remove_objects([("indexed_increment_value", meta) for meta in metas])

    #     example_env.client.create_object(
    #         "indexed_increment_value", enable_structured_response=True, request_meta_response=True
    #     )

    def test_concurrent_key(self, example_env):
        tx1 = example_env.client.start_transaction()
        tx2 = example_env.client.start_transaction()
        example_env.client.create_object(
            "indexed_increment_id",
            enable_structured_response=True,
            request_meta_response=True,
            transaction_id=tx1,
        )
        example_env.client.create_object(
            "indexed_increment_id",
            enable_structured_response=True,
            request_meta_response=True,
            transaction_id=tx2,
        )
        example_env.client.commit_transaction(tx1)
        with pytest.raises(YtTabletTransactionLockConflict):
            example_env.client.commit_transaction(tx2)

    # def test_concurrent_value(self, example_env):
    #     tx1 = example_env.client.start_transaction()
    #     tx2 = example_env.client.start_transaction()
    #     example_env.client.create_object(
    #         "indexed_increment_value",
    #         enable_structured_response=True,
    #         request_meta_response=True,
    #         transaction_id=tx1,
    #     )
    #     example_env.client.create_object(
    #         "indexed_increment_value",
    #         enable_structured_response=True,
    #         request_meta_response=True,
    #         transaction_id=tx2,
    #     )
    #     example_env.client.commit_transaction(tx1)
    #     example_env.client.commit_transaction(tx2)  # Should fail with unique index.


class TestMultipleAttributePolicy:
    def test_id_is_required(self, example_env):
        with pytest.raises(InvalidObjectIdError):
            example_env.client.create_object("multipolicy_id")

    def test_create(self, example_env):
        example_config = {
            "transaction_manager": {
                "timestamp_buffer_enabled": True,
                "timestamp_buffer_size": 10,
                "timestamp_buffer_low_watermark": 5,
            }
        }
        with example_env.set_cypress_config_patch_in_context(example_config):
            wait(lambda: _is_timestamp_buffer_filled(example_env.client))
            OBJECT_COUNT = 100
            str_ids = []
            i64_ids = []
            ui64_ids = []
            other_ui64_ids = []
            str_values = []
            i64_values = []
            ui64_values = []
            other_ui64_values = []

            subresponses = example_env.client.create_objects(
                (("multipolicy_id", dict(meta=dict(str_id="a"))) for _ in range(OBJECT_COUNT)),
                request_meta_response=True,
                enable_structured_response=True,
            )["subresponses"]
            for subresponse in subresponses:
                str_ids.append(subresponse["meta"]["str_id"])
                i64_ids.append(subresponse["meta"]["i64_id"])
                ui64_ids.append(subresponse["meta"]["ui64_id"])
                other_ui64_ids.append(subresponse["meta"]["another_ui64_id"])

            specs = example_env.client.get_objects(
                "multipolicy_id",
                [subresponse["meta"] for subresponse in subresponses],
                selectors=["/spec"],
            )
            for spec in specs:
                str_values.append(spec[0]["str_value"])
                i64_values.append(spec[0]["i64_value"])
                ui64_values.append(spec[0]["ui64_value"])
                other_ui64_values.append(spec[0]["another_ui64_value"])

            assert len(set(str_ids)) == 1
            assert len(set(i64_ids)) == len(i64_ids)
            assert _is_monotonic(ui64_ids)
            assert _is_monotonic(other_ui64_ids)
            assert len(set(str_values)) == 1
            assert len(set(i64_values)) == len(i64_values)
            assert _is_monotonic(ui64_values)
            assert _is_monotonic(other_ui64_values)

    def test_manual(self, example_env):
        resp = example_env.client.create_object(
            "multipolicy_id",
            {
                "meta": {
                    "str_id": "a",
                    "i64_id": -100,
                    "ui64_id": 100,
                    "another_ui64_id": 200,
                },
                "spec": {
                    "str_value": "foo",
                    "i64_value": -42,
                    "ui64_value": 42,
                    "another_ui64_value": 43,
                },
            },
            enable_structured_response=True,
            request_meta_response=True,
        )

        assert resp["meta"]["str_id"] == "a"
        assert resp["meta"]["i64_id"] == -100
        assert resp["meta"]["ui64_id"] == 100
        assert resp["meta"]["another_ui64_id"] == 200
        spec = example_env.client.get_object("multipolicy_id", resp["meta"], selectors=["/spec"])[0]
        assert spec["str_value"] == "foo"
        assert spec["i64_value"] == -42
        assert spec["ui64_value"] == 42
        assert spec["another_ui64_value"] == 43
