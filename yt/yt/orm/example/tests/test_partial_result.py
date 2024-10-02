class TestPartialResult:
    EXAMPLE_MASTER_CONFIG = {
        "transaction_manager": {
            "partial_result_batch_fraction": 0.2,
            "partial_result_select_timeout_slack": 2300,
        },
    }

    def test(self, example_env):
        example_env.client.create_objects(
            (
                ("cat", dict())
                for i in range(20)
            ),
        )

        options = dict(enable_partial_result=True)

        response = example_env.client.select_objects(
            "cat",
            selectors=["/meta/key", "/spec/sleep_time/1000"],
            options=options,
            enable_structured_response=True,
        )

        results = response["results"]
        assert len(results) == 8

    def test_filter(self, example_env):
        cat_names = ["Whiskers", "Luna", "Oliver", "Bella", "Simba", "Chloe", "Max", "Shadow", "Milo", "Tigger"]
        example_env.client.create_objects(
            (
                ("cat", dict(spec=dict(name=cat_names[i % 10])))
                for i in range(20)
            ),
        )

        options = dict(enable_partial_result=True)

        select_result = example_env.client.select_objects(
            "cat",
            filter="string([/spec/name]) in ('Max', 'Chloe', 'Milo') or boolean([/spec/sleep_time/1000])",
            selectors=["/meta/key"],
            options=options,
            order_by=[{"expression": "[/spec/name]"}],
            enable_structured_response=True,
        )
        results = select_result["results"]

        assert 0 < len(results) < 6

    def test_continuation_token(self, example_env):
        cat_keys = []
        for i in range(20):
            cat_keys.append(
                example_env.client.create_object("cat")
            )

        continuation_token = None
        selected_cat_keys = []
        limit = 10
        options = dict(enable_partial_result=True)

        while True:
            if continuation_token is not None:
                options["continuation_token"] = continuation_token
            response = example_env.client.select_objects(
                "cat",
                filter="not boolean([/spec/sleep_time/400])",
                selectors=["/meta/key"],
                options=options,
                limit=limit,
                enable_structured_response=True,
            )
            results = response["results"]
            continuation_token = response["continuation_token"]
            for result in results:
                selected_cat_keys.append(result[0]["value"])
            if len(results) < limit:
                break

        assert list(sorted(cat_keys)) == list(sorted(selected_cat_keys))
