from yt.orm.client.client import BatchingOptions


class TestYsonExtract:
    def test_employer_job_title(self, example_env):
        example_env.client.create_object("employer", {"meta": {"id": "employer1"}, "status": {"job_title": "baker"}})

        example_env.client.select_objects(
            "employer", selectors=["/status/job_title"], filter="[/status/job_title] IN ('baker')"
        )

    def test_executor_rank(self, example_env):
        example_env.client.create_object("executor", {"meta": {"id": 1}, "spec": {"rank": 2}})

        example_env.client.select_objects("executor", selectors=["/spec/rank"], filter="[/spec/rank] IN (2)")

    def test_executor_rank_order_by(self, example_env):
        example_env.client.create_object("executor", {"meta": {"id": 1}, "spec": {}})

        example_env.client.create_object("executor", {"meta": {"id": 2}, "spec": {}})

        res = example_env.client.select_objects(
            "executor",
            selectors=["/spec/rank"],
            order_by=[{"expression": "/spec/rank"}],
            limit=2,
            batching_options=BatchingOptions(1),
        )

        assert len(res) == 2

    def test_hitchhiker(self, example_env):
        example_env.client.create_object(
            "hitchhiker",
            {
                "meta": {"id": 1},
                "spec": {
                    "name": "Alex",
                    "miles_traveled": 200,
                    "galaxies_visited": 10,
                    "x_cord": -5,
                    "y_cord": -5,
                    "z_cord": -5,
                    "w_cord": -5,
                    "card_balance": -20,
                    "cash_balance": -30,
                    "time_cord": 13,
                    "likes_acquired": 55,
                    "wears_glasses": True,
                },
            },
        )

        example_env.client.create_object(
            "hitchhiker",
            {
                "meta": {"id": 2},
                "spec": {
                    "name": "Ivan",
                    "miles_traveled": 4,
                    "galaxies_visited": 10,
                    "x_cord": -5,
                    "y_cord": -5,
                    "z_cord": -5,
                    "w_cord": -5,
                    "card_balance": -20,
                    "cash_balance": -30,
                    "time_cord": 13,
                    "likes_acquired": 55,
                    "wears_glasses": True,
                },
            },
        )

        result = example_env.client.select_objects(
            "hitchhiker", selectors=["/spec/name"], filter="[/spec/name] IN ('Alex', 'Ivan')"
        )
        assert len(result) == 2

        result = example_env.client.select_objects("hitchhiker", selectors=["/meta/id"], filter="[/spec/name] = 'Alex'")
        assert len(result) == 1

        result = example_env.client.select_objects(
            "hitchhiker", selectors=["/meta/id"], filter="[/spec/miles_traveled] = 200"
        )
        assert len(result) == 1

        result = example_env.client.select_objects(
            "hitchhiker", selectors=["/meta/id"], filter="[/spec/galaxies_visited] = 0"
        )
        assert len(result) == 0

        result = example_env.client.select_objects("hitchhiker", selectors=["/meta/id"], filter="[/spec/x_cord] = -5")
        assert len(result) == 2

        result = example_env.client.select_objects("hitchhiker", selectors=["/meta/id"], filter="[/spec/y_cord] = -5")
        assert len(result) == 2

        result = example_env.client.select_objects(
            "hitchhiker", selectors=["/meta/id"], filter="[/spec/z_cord] IN (-5, 1)"
        )
        assert len(result) == 2

        result = example_env.client.select_objects(
            "hitchhiker", selectors=["/meta/id"], filter="[/spec/z_cord] IN (0, 1)"
        )
        assert len(result) == 0

        result = example_env.client.select_objects(
            "hitchhiker", selectors=["/meta/id"], filter="[/spec/card_balance] = -20"
        )
        assert len(result) == 2

        result = example_env.client.select_objects(
            "hitchhiker", selectors=["/meta/id"], filter="[/spec/cash_balance] = -30"
        )
        assert len(result) == 2

        result = example_env.client.select_objects(
            "hitchhiker", selectors=["/meta/id"], filter="[/spec/time_cord] IN (13, 55)"
        )
        assert len(result) == 2

        result = example_env.client.select_objects(
            "hitchhiker", selectors=["/meta/id"], filter="[/spec/likes_acquired] IN (13, 55)"
        )
        assert len(result) == 2

        result = example_env.client.select_objects(
            "hitchhiker", selectors=["/meta/id"], filter="[/spec/wears_glasses] = false"
        )
        assert len(result) == 0

    def test_nirvana_order_by(self, example_env):
        example_env.client.create_object(
            "nirvana_dm_process_instance", {"meta": {"id": "1"}, "spec": {"block_count": 30, "message": "aa"}}
        )

        example_env.client.create_object(
            "nirvana_dm_process_instance", {"meta": {"id": "2"}, "spec": {"block_count": 20, "message": "bb"}}
        )

        result = example_env.client.select_objects(
            "nirvana_dm_process_instance", selectors=["/meta/id"], order_by=[{"expression": "/spec/block_count"}]
        )

        assert result == [["2"], ["1"]]

        result = example_env.client.select_objects(
            "nirvana_dm_process_instance", selectors=["/meta/id"], order_by=[{"expression": "/spec/message"}]
        )

        assert result == [["1"], ["2"]]

    def test_book_repeated(self, example_env):
        book = example_env.create_book()
        example_env.client.update_object(
            "book",
            str(book),
            set_updates=[
                {"path": "/spec/chapter_descriptions", "value": [{"name": "Chapter 1"}]},
                {"path": "/spec/genres", "value": ["fantasy"]},
            ],
        )

        result = example_env.client.select_objects(
            "book", selectors=["/meta/id"], filter="[/spec/chapter_descriptions/0/name] IN ('Chapter 1')"
        )
        assert len(result) == 1

        result = example_env.client.select_objects(
            "book", selectors=["/meta/id"], filter="[/spec/genres/0] IN ('fantasy')"
        )
        assert len(result) == 1

    def test_nexus_map(self, example_env):
        example_env.client.create_object(
            "nexus",
            {
                "spec": {
                    "some_map_to_message_column": {"42": {"nested_field": "lucky_number", "nested_integer_field": 42}}
                }
            },
        )

        result = example_env.client.select_objects(
            "nexus",
            selectors=["/meta/id"],
            filter="[/spec/some_map_to_message_column/42/nested_field] IN ('lucky_number')",
        )
        assert len(result) == 1

        result = example_env.client.select_objects(
            "nexus", selectors=["/meta/id"], filter="[/spec/some_map_to_message_column/42/nested_integer_field] IN (42)"
        )
        assert len(result) == 1

    def test_cat_enums(self, example_env):
        example_env.client.create_object(
            "cat",
            {
                "spec": {
                    "name": "Leopold",
                    "best_friend_eye_color": "hazel",
                }
            },
        )

        result = example_env.client.select_objects(
            "cat", selectors=["/spec/name"], filter="[/spec/best_friend_eye_color] IN ('hazel')"
        )
        assert len(result) == 1
