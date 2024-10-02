from yt.orm.library.common import InvalidRequestArgumentsError

from yt.yson.yson_types import YsonEntity

from contextlib import nullcontext as does_not_raise

import pytest


class TestApiValidations:
    def test_composite_update(self, example_env):
        id = example_env.create_cat()
        with pytest.raises(
            InvalidRequestArgumentsError,
            match="Composite attribute.*cannot be updated with aggregate mode",
        ):
            example_env.client.update_object(
                "cat",
                id,
                set_updates=[{"path": "/spec", "value": {}, "aggregate_mode": "override"}],
            )

    def test_etc_update(self, example_env):
        id = example_env.create_cat(spec={"mood_in_previous_days": ["playful", "sleepy"]})
        with pytest.raises(
            InvalidRequestArgumentsError,
            match="Etc attribute.*cannot be updated with aggregate mode",
        ):
            example_env.client.update_object(
                "cat",
                id,
                set_updates=[
                    {
                        "path": "/spec/mood_in_previous_days",
                        "value": [],
                        "aggregate_mode": "override",
                    }
                ],
            )

    def test_scalar_attribute_update(self, example_env):
        id = example_env.create_book()
        with pytest.raises(
            InvalidRequestArgumentsError,
            match="ScalarAttribute cannot be updated with aggregate mode",
        ):
            example_env.client.update_object(
                "book",
                id,
                set_updates=[{"path": "/spec/year", "value": 1997, "aggregate_mode": "override"}],
            )

    @pytest.mark.parametrize(
        "set_updates, expected_raise",
        [
            pytest.param(
                [
                    {
                        "path": "/spec/statistics_for_days_of_year",
                        "value": {},
                        "aggregate_mode": "override",
                    }
                ],
                does_not_raise(),
            ),
            pytest.param(
                [
                    {
                        "path": "/spec/statistics_for_days_of_year/1",
                        "value": {},
                        "aggregate_mode": "override",
                    }
                ],
                pytest.raises(
                    InvalidRequestArgumentsError,
                    match="Attribute.*cannot be updated with "
                    "aggregate mode and nonempty suffix path.*",
                ),
            ),
        ],
    )
    def test_update_by_path(self, example_env, set_updates, expected_raise):
        id = example_env.create_cat(
            spec=dict(statistics_for_days_of_year={"1": {"number_of_meals": 3}})
        )
        with expected_raise:
            example_env.client.update_object(
                "cat",
                id,
                set_updates=set_updates,
            )


# TODO(kmokrov): Use "dict_sum" aggregate function instead of "first" after removing 23.2
class TestAggregatedColumns:
    FRIEND_CATS_COUNT_PATH = "/spec/friend_cats_count"
    STATISTICS_FOR_DAY_OF_YEARS_PATH = "/spec/statistics_for_days_of_year"

    def test_aggregate_and_override(self, example_env):
        id = example_env.create_cat()
        assert example_env.client.get_object(
            "cat", str(id), [self.FRIEND_CATS_COUNT_PATH, self.STATISTICS_FOR_DAY_OF_YEARS_PATH]
        ) == [0, {}]

        example_env.client.update_object(
            "cat",
            str(id),
            set_updates=[
                {
                    "path": self.FRIEND_CATS_COUNT_PATH,
                    "value": 1,
                },
                {
                    "path": self.STATISTICS_FOR_DAY_OF_YEARS_PATH,
                    "value": {
                        "1": {"hours_of_purring": 6, "number_of_meals": 3},
                        "2": {"hours_of_purring": 2, "number_of_meals": 2},
                    },
                },
            ],
        )
        assert example_env.client.get_object(
            "cat", str(id), [self.FRIEND_CATS_COUNT_PATH, self.STATISTICS_FOR_DAY_OF_YEARS_PATH]
        ) == [1, {}]

        tx = example_env.client.start_transaction()
        example_env.client.update_object(
            "cat",
            str(id),
            set_updates=[
                {
                    "path": self.FRIEND_CATS_COUNT_PATH,
                    "value": 2,
                },
                {
                    "path": self.STATISTICS_FOR_DAY_OF_YEARS_PATH,
                    "value": {"1": {"hours_of_purring": 2}, "2": {"number_of_meals": 3}},
                },
            ],
        )
        example_env.client.update_object(
            "cat",
            str(id),
            set_updates=[
                {
                    "path": self.FRIEND_CATS_COUNT_PATH,
                    "value": 3,
                },
                {
                    "path": self.STATISTICS_FOR_DAY_OF_YEARS_PATH,
                    "value": {"1": {"hours_of_purring": -1}},
                },
            ],
            transaction_id=tx,
        )
        example_env.client.commit_transaction(tx)
        assert example_env.client.get_object(
            "cat", str(id), [self.FRIEND_CATS_COUNT_PATH, self.STATISTICS_FOR_DAY_OF_YEARS_PATH]
        ) == [6, {}]

        tx = example_env.client.start_transaction()
        example_env.client.update_object(
            "cat",
            str(id),
            set_updates=[
                {
                    "path": self.FRIEND_CATS_COUNT_PATH,
                    "value": 4,
                    "aggregate_mode": "aggregate",
                },
                {
                    "path": self.STATISTICS_FOR_DAY_OF_YEARS_PATH,
                    "value": {"1": {"number_of_meals": -1}},
                    "aggregate_mode": "aggregate",
                },
            ],
            transaction_id=tx,
        )
        example_env.client.update_object(
            "cat",
            str(id),
            set_updates=[
                {
                    "path": self.FRIEND_CATS_COUNT_PATH,
                    "value": 5,
                    "aggregate_mode": "override",
                },
                {
                    "path": self.STATISTICS_FOR_DAY_OF_YEARS_PATH,
                    "value": {"1": {}, "2": {"hours_of_purring": 12}},
                    "aggregate_mode": "override",
                },
            ],
            transaction_id=tx,
        )
        example_env.client.update_object(
            "cat",
            str(id),
            set_updates=[
                {
                    "path": self.FRIEND_CATS_COUNT_PATH,
                    "value": 6,
                    "aggregate_mode": "aggregate",
                },
                {
                    "path": self.STATISTICS_FOR_DAY_OF_YEARS_PATH,
                    "value": {
                        "1": {"hours_of_purring": 2, "number_of_meals": 3},
                        "2": {"hours_of_purring": 2, "number_of_meals": 5},
                    },
                    "aggregate_mode": "aggregate",
                },
            ],
            transaction_id=tx,
        )
        example_env.client.commit_transaction(tx)
        assert example_env.client.get_object(
            "cat", str(id), [self.FRIEND_CATS_COUNT_PATH, self.STATISTICS_FOR_DAY_OF_YEARS_PATH]
        ) == [11, {"1": {}, "2": {"hours_of_purring": 12}}]

    def test_remove(self, example_env):
        id = example_env.create_cat(
            spec={
                "friend_cats_count": 123,
                "statistics_for_days_of_year": {
                    "1": {"hours_of_purring": 6, "number_of_meals": 3},
                    "2": {"hours_of_purring": 2, "number_of_meals": 2},
                },
            }
        )
        assert example_env.client.get_object(
            "cat", str(id), [self.FRIEND_CATS_COUNT_PATH, self.STATISTICS_FOR_DAY_OF_YEARS_PATH]
        ) == [
            123,
            {
                "1": {"hours_of_purring": 6, "number_of_meals": 3},
                "2": {"hours_of_purring": 2, "number_of_meals": 2},
            },
        ]

        example_env.client.update_object(
            "cat",
            str(id),
            remove_updates=[
                {
                    "path": self.FRIEND_CATS_COUNT_PATH,
                },
                {
                    "path": self.STATISTICS_FOR_DAY_OF_YEARS_PATH,
                },
            ],
        )
        assert example_env.client.get_object(
            "cat", str(id), [self.FRIEND_CATS_COUNT_PATH, self.STATISTICS_FOR_DAY_OF_YEARS_PATH]
        ) == [0, {}]

        tx = example_env.client.start_transaction()
        example_env.client.update_object(
            "cat",
            str(id),
            set_updates=[
                {
                    "path": self.FRIEND_CATS_COUNT_PATH,
                    "value": 2,
                },
                {
                    "path": self.STATISTICS_FOR_DAY_OF_YEARS_PATH,
                    "value": {"1": {"hours_of_purring": 6}},
                },
            ],
            transaction_id=tx,
        )
        example_env.client.update_object(
            "cat",
            str(id),
            remove_updates=[
                {
                    "path": self.FRIEND_CATS_COUNT_PATH,
                },
                {
                    "path": self.STATISTICS_FOR_DAY_OF_YEARS_PATH,
                },
            ],
            transaction_id=tx,
        )
        example_env.client.commit_transaction(tx)
        assert example_env.client.get_object(
            "cat", str(id), [self.FRIEND_CATS_COUNT_PATH, self.STATISTICS_FOR_DAY_OF_YEARS_PATH]
        ) == [0, {}]

        tx = example_env.client.start_transaction()
        example_env.client.update_object(
            "cat",
            str(id),
            set_updates=[
                {
                    "path": self.FRIEND_CATS_COUNT_PATH,
                    "value": 4,
                    "aggregate_mode": "aggregate",
                },
                {
                    "path": self.STATISTICS_FOR_DAY_OF_YEARS_PATH,
                    "value": {"1": {"hours_of_purring": 6}},
                    "aggregate_mode": "aggregate",
                },
            ],
            transaction_id=tx,
        )
        example_env.client.update_object(
            "cat",
            str(id),
            remove_updates=[
                {
                    "path": self.FRIEND_CATS_COUNT_PATH,
                },
                {
                    "path": self.STATISTICS_FOR_DAY_OF_YEARS_PATH,
                },
            ],
            transaction_id=tx,
        )
        example_env.client.update_object(
            "cat",
            str(id),
            set_updates=[
                {
                    "path": self.FRIEND_CATS_COUNT_PATH,
                    "value": 6,
                    "aggregate_mode": "override",
                },
                {
                    "path": self.STATISTICS_FOR_DAY_OF_YEARS_PATH,
                    "value": {"1": {"hours_of_purring": 6}},
                    "aggregate_mode": "override",
                },
            ],
            transaction_id=tx,
        )
        example_env.client.commit_transaction(tx)
        assert example_env.client.get_object(
            "cat", str(id), [self.FRIEND_CATS_COUNT_PATH, self.STATISTICS_FOR_DAY_OF_YEARS_PATH]
        ) == [6, {"1": {"hours_of_purring": 6}}]

    def test_select(self, example_env):
        FRIENDS_CATS_COUNT = 123
        STATISTICS_FOR_DAYS_OF_YEAR = {
            "1": {"hours_of_purring": 6, "number_of_meals": 3},
            "2": {"hours_of_purring": 2, "number_of_meals": 2},
        }
        id = example_env.create_cat(
            spec=dict(
                friend_cats_count=FRIENDS_CATS_COUNT,
                statistics_for_days_of_year=STATISTICS_FOR_DAYS_OF_YEAR,
            )
        )
        assert example_env.client.get_object(
            "cat", str(id), [self.FRIEND_CATS_COUNT_PATH, self.STATISTICS_FOR_DAY_OF_YEARS_PATH]
        ) == [FRIENDS_CATS_COUNT, STATISTICS_FOR_DAYS_OF_YEAR]

        cat = example_env.client.select_objects(
            "cat",
            filter=f"[/meta/id] = {id}",
            selectors=["/spec"],
        )[0]
        assert (
            cat[0]["friend_cats_count"] == YsonEntity()
            and cat[0]["statistics_for_days_of_year"] == YsonEntity()
        )

        cat = example_env.client.select_objects(
            "cat",
            filter=f"[/meta/id] = {id}",
            selectors=[self.FRIEND_CATS_COUNT_PATH, self.STATISTICS_FOR_DAY_OF_YEARS_PATH],
        )[0]
        assert cat[0] == FRIENDS_CATS_COUNT and cat[1] == STATISTICS_FOR_DAYS_OF_YEAR

    def test_concurrent_update(self, example_env):
        id = example_env.create_cat(
            spec=dict(
                friend_cats_count=123,
                statistics_for_days_of_year={"1": {"hours_of_purring": 6, "number_of_meals": 3}},
            )
        )
        tx1 = example_env.client.start_transaction()
        tx2 = example_env.client.start_transaction()

        example_env.client.update_object(
            "cat",
            str(id),
            set_updates=[
                {
                    "path": self.FRIEND_CATS_COUNT_PATH,
                    "value": 4,
                },
                {
                    "path": self.STATISTICS_FOR_DAY_OF_YEARS_PATH,
                    "value": {"1": {"hours_of_purring": 6}},
                },
            ],
            transaction_id=tx1,
        )

        example_env.client.update_object(
            "cat",
            str(id),
            set_updates=[
                {
                    "path": self.FRIEND_CATS_COUNT_PATH,
                    "value": 5,
                },
                {
                    "path": self.STATISTICS_FOR_DAY_OF_YEARS_PATH,
                    "value": {"1": {"hours_of_purring": 6}, "2": {"number_of_meals": 6}},
                },
            ],
            transaction_id=tx2,
        )

        assert example_env.client.get_object(
            "cat", str(id), [self.FRIEND_CATS_COUNT_PATH, self.STATISTICS_FOR_DAY_OF_YEARS_PATH]
        ) == [123, {"1": {"hours_of_purring": 6, "number_of_meals": 3}}]

        example_env.client.commit_transaction(tx1)
        assert example_env.client.get_object(
            "cat", str(id), [self.FRIEND_CATS_COUNT_PATH, self.STATISTICS_FOR_DAY_OF_YEARS_PATH]
        ) == [127, {"1": {"hours_of_purring": 6, "number_of_meals": 3}}]

        example_env.client.commit_transaction(tx2)
        assert example_env.client.get_object(
            "cat", str(id), [self.FRIEND_CATS_COUNT_PATH, self.STATISTICS_FOR_DAY_OF_YEARS_PATH]
        ) == [132, {"1": {"hours_of_purring": 6, "number_of_meals": 3}}]

    # TODO(kmokrov): Move to test_performance.py after removing 23.2
    def test_performance(self, example_env):
        create_response = example_env.client.create_objects(
            (
                (
                    "cat",
                    {
                        "spec": {
                            "friend_cats_count": 123,
                            "statistics_for_days_of_year": {
                                "2": {"hours_of_purring": 2, "number_of_meals": 2}
                            },
                        },
                    },
                ),
            ),
            request_meta_response=True,
            enable_structured_response=True,
            common_options=dict(fetch_performance_statistics=True),
        )
        assert create_response["performance_statistics"]["read_phase_count"] == 2

        for set_update in (
            {"path": "/spec/health_condition", "value": "healthy"},
            {"path": "/spec/friend_cats_count", "value": 123},
        ):
            update_response = example_env.client.update_objects(
                (
                    {
                        "object_type": "cat",
                        "meta": {"id": subresponce["meta"]["id"]},
                        "set_updates": [
                            set_update,
                        ],
                    }
                    for subresponce in create_response["subresponses"]
                ),
                common_options=dict(fetch_performance_statistics=True),
            )
            assert update_response["performance_statistics"]["read_phase_count"] == 3
