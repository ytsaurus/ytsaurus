from .conftest import ExampleTestEnvironment

import typing as tp
import pytest


class TestSelectTimestamp:
    @pytest.mark.parametrize(
        ("object_type", "create_object", "filter", "selector", "index", "update_value", "zero_timestamp_expected"),
        [
            pytest.param(
                "book",
                lambda env: env.create_book(enable_structured_response=True),
                None,
                "/spec/name",
                None,
                "New Book Name",
                False,
                id="book_column",
            ),
            pytest.param(
                "book",
                lambda env: env.create_book(enable_structured_response=True),
                "[/spec/font]=''",
                "/spec/font",
                "books_by_font",
                None,
                False,
                id="book_index_key",
            ),
            pytest.param(
                "book",
                lambda env: env.create_book(enable_structured_response=True),
                "[/spec/font]=''",
                "/spec/name",
                "books_by_font",
                "New Book Name",
                False,
                id="book_index_column",
            ),
            pytest.param(
                "publisher",
                lambda env: env.create_publisher(enable_structured_response=True),
                None,
                "/spec/name",
                None,
                "New Publisher Name",
                False,
                id="publisher_column",
            ),
            pytest.param(
                "publisher",
                lambda env: env.create_publisher(enable_structured_response=True),
                None,
                "/meta/id",
                None,
                None,
                True,
                id="publisher_key",
            ),
            pytest.param(
                "publisher",
                lambda env: env.create_publisher(enable_structured_response=True),
                None,
                "/meta/ultimate_question_of_life",
                None,
                None,
                False,
                id="publisher_evaluated",
            ),
            pytest.param(
                "editor",
                lambda env: env.client.create_object("editor", enable_structured_response=True),
                None,
                "/spec/name",
                None,
                "New Editor Name",
                False,
                id="editor_etc",
            ),
        ],
    )
    def test(
        self,
        example_env: ExampleTestEnvironment,
        object_type: str,
        create_object: tp.Callable[[ExampleTestEnvironment], tp.Any],
        filter: tp.Optional[str],
        selector: str,
        index: tp.Optional[str],
        update_value: tp.Any,
        zero_timestamp_expected: bool,
    ):
        client = example_env.client

        response = create_object(example_env)
        creation_timestamp = response["commit_timestamp"]
        object_key = response["meta"]["key"]

        def extract_timestamps(result) -> tp.List[int]:
            return [value["timestamp"] for value in result]

        def check_correct_timestamp(expected_timestamp):
            result = client.select_objects(
                object_type,
                filter=filter,
                selectors=[selector],
                options={"fetch_timestamps": True},
                common_options={"fetch_performance_statistics": True},
                index=index,
                limit=1,
                enable_structured_response=True,
            )

            assert 1 == len(result["results"])
            if zero_timestamp_expected:
                assert [0] == extract_timestamps(result["results"][0])
            else:
                assert [expected_timestamp] == extract_timestamps(result["results"][0])

            assert 1 == result["performance_statistics"]["read_phase_count"]

        check_correct_timestamp(creation_timestamp)
        if update_value is not None:
            commit_timestamp = client.update_object(object_type, str(object_key), set_updates=[
                {"path": selector, "value": update_value}
            ])["commit_timestamp"]
            check_correct_timestamp(commit_timestamp)
