from yt.wrapper.errors import YtResponseError

import yt.yson as yson

import pytest


class TestAttributesValidation:
    def test_cannot_add_attribute_to_labels_root(self, example_env):
        client = example_env.client
        editor = example_env.create_editor()
        with pytest.raises(YtResponseError):
            client.update_object(
                "editor",
                str(editor),
                set_updates=[
                    {
                        "path": "/labels/@attr",
                        "value": "attribute_value"
                    },
                ],
            )

    def test_cannot_add_attribute_inside_labels_by_path(self, example_env):
        client = example_env.client
        editor = example_env.create_editor(labels={"rating": "excellent"})
        with pytest.raises(YtResponseError):
            client.update_object(
                "editor",
                str(editor),
                set_updates=[
                    {
                        "path": "/labels/rating/@attribute",
                        "value": {"key1": "value"}
                    },
                ],
            )

    def test_cannot_add_attribute_inside_labels_by_value(self, example_env):
        client = example_env.client
        editor = example_env.create_editor()

        value_with_attributes = yson.YsonEntity()
        value_with_attributes.attributes["attribute"] = "value"

        with pytest.raises(YtResponseError):
            client.update_object(
                "editor",
                str(editor),
                set_updates=[
                    {
                        "path": "/labels/rating",
                        "value": value_with_attributes,
                    },
                ],
            )

    def test_attributes_are_not_written_in_history(self, example_env):
        client = example_env.client
        book = example_env.create_book()

        def update_forbid_yson_attributes_usage(value):
            example_env.dynamic_config.update_config({
                "transaction_manager": {
                    "forbid_yson_attributes_usage": value,
                },
            })

        # Phase 1: Some update added attribute, while it was allowed.
        update_forbid_yson_attributes_usage(False)

        value_with_attributes = yson.YsonEntity()
        value_with_attributes.attributes["attribute"] = "value"
        client.update_object(
            "book",
            str(book),
            set_updates=[
                {
                    "path": "/labels/rating",
                    "value": value_with_attributes,
                },
            ],
        )

        # Phase 2: After enabling validation, even allowed update should trigger
        # history writing failure, because previously set YSON-attributes will appear
        # in history.
        update_forbid_yson_attributes_usage(True)

        with pytest.raises(YtResponseError):
            client.update_object(
                "book",
                str(book),
                set_updates=[
                    {
                        "path": "/spec/font",
                        "value": "New Nice Font"
                    },
                ],
            )
