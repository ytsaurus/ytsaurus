class TestMethodCall:
    def test(self, example_env):
        cat = example_env.client.create_object("cat")

        result = example_env.client.update_object(
            "cat",
            cat,
            method_calls=[
                {"path": "/control/catch", "value": {"mouse": ""}},
                {"path": "/control/catch", "value": {"mouse": "Mickey"}},
            ],
        )["method_results"]["value_payloads"]

        assert result[0]["yson"]["mood"] == "angry"
        assert result[1]["yson"]["mood"] == "nice"

    def test_update_objects(self, example_env):
        cats = example_env.client.create_objects(
            (
                ("cat", dict())
                for i in range(20)
            ),
            enable_structured_response=True,
        )

        method_calls = [
            {"path": "/control/catch", "value": {"mouse": "Mickey"}},
            {"path": "/control/catch", "value": {}}
        ]

        update_response = example_env.client.update_objects(
            (
                {
                    "object_type": "cat",
                    "meta": {"id": subresponce["meta"]["id"]},
                    "method_calls": [
                        method_calls[i % 2],
                    ],
                }
                for i, subresponce in enumerate(cats["subresponses"])
            ),
            common_options=dict(fetch_performance_statistics=True),
        )
        assert sorted(
            list([update_response["subresponses"][i]["method_results"]["value_payloads"][0]["yson"]["mood"] for i in range(20)])
        ) == list(["angry" for i in range(10)] + ["nice" for i in range(10)])

    def test_void(self, example_env):
        cat = example_env.client.create_object("cat")

        result = example_env.client.update_object(
            "cat",
            cat,
            method_calls=[{"path": "/control/pet", "value": {}}],
        )["method_results"]["value_payloads"][0]['yson']
        assert result == {}

    def test_upsert(self, example_env):
        cat = example_env.client.create_object(
            "cat",
            enable_structured_response=True,)

        response = example_env.client.create_object(
            "cat",
            {"meta": {"id": cat["meta"]["id"]}},
            update_if_existing={"method_calls": [{"path": "/control/catch", "value": {"mouse": "Minnie"}}]},
            enable_structured_response=True,
        )

        assert response["method_results"]["value_payloads"][0]["yson"]["mood"] == "nice"

    def test_control(self, example_env):
        cat = example_env.client.create_object("cat")

        result = example_env.client.update_object(
            "cat",
            cat,
            method_calls=[{"path": "/control/touch", "value": {}}],
        )["method_results"]["value_payloads"][0]['yson']
        assert result == {}
