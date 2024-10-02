from yt.wrapper.errors import YtResponseError

import pytest


class TestValidation:
    def test_proto_repeated(self, example_env):
        publisher = example_env.client.create_object("publisher")
        with pytest.raises(YtResponseError):
            example_env.client.create_object(
                "book",
                dict(
                    meta=dict(isbn="978-1449355730", parent_key=publisher),
                    spec=dict(chapter_descriptions=[dict(unexpected_name=42)]),
                )
            )

    def test_proto_map(self, example_env):
        expected_some_map = {"3": "c"}
        expected_some_map_to_message = {"4": {"nested_field": "hello maps!"}}
        expected_some_map_to_message_column = {"4": {"nested_field": "hello maps in column!", "unknown_field": 10}}
        expected_nexus_spec = {
            "some_map": expected_some_map,
            "some_map_to_message": expected_some_map_to_message,
            "some_map_to_message_column": expected_some_map_to_message_column,
        }
        with pytest.raises(YtResponseError):
            example_env.client.create_object("nexus", {"spec": expected_nexus_spec})

    @pytest.mark.parametrize(
        "check, expect_throw", [("disable", False), ("log_on_fail", False), ("throw_on_fail", True)]
    )
    def test_utf8_check(self, example_env, check, expect_throw):
        non_utf8_string = b"\xc3\x28\xdd\x321"

        def create_with_bad_key_column():
            example_env.create_object("genre", meta={"id": non_utf8_string}, spec={"name": "fantasy"})

        def create_with_bad_string_column():
            example_env.create_object("editor", spec={"phone_number": non_utf8_string})

        def bad_string_through_store_in_plugin():
            example_env.create_object("editor", spec={"phone_number": "change_me_through_store"})

        def bad_string_through_mutable_load_in_plugin():
            example_env.create_object("editor", spec={"phone_number": "change_me_through_mutable_load"})

        def create_with_bad_repeated_column():
            example_env.create_object("editor", spec={"achievements": ["medal_of_honor", non_utf8_string]})

        def create_with_bad_columnar_map_key():
            example_env.create_object(
                "nested_columns",
                spec={"column_map": {"key": {"foo": "value"}, non_utf8_string: {"foo": "value"}}},
            )

        def create_with_bad_etc():
            example_env.create_object("executor", spec={"first_name": non_utf8_string})

        test_cases = (
            create_with_bad_key_column,
            create_with_bad_string_column,
            bad_string_through_store_in_plugin,
            bad_string_through_mutable_load_in_plugin,
            create_with_bad_repeated_column,
            create_with_bad_columnar_map_key,
            create_with_bad_etc,
        )

        config = {
            "object_manager": {"utf8_check": check},
            "protobuf_interop": {"utf8_check": check}
        }
        with example_env.set_cypress_config_patch_in_context(config):
            if expect_throw:
                for create_function in test_cases:
                    with pytest.raises(YtResponseError, match="Non UTF-8 value in string field"):
                        create_function()
            else:
                for create_function in test_cases:
                    create_function()
