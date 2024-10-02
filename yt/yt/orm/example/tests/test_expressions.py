class TestLikeExpression:
    def test_employer(self, example_env):
        example_env.client.create_object(
            "employer", {"meta": {"id": "employer1"}}
        )
        example_env.client.create_object(
            "employer", {"meta": {"id": "employer2"}}
        )

        assert [["employer1"]] == example_env.client.select_objects(
            "employer",
            selectors=["/meta/id"],
            filter="[/meta/id] like '%yer1%'",
        )


class TestCaseExpression:
    def test_employer(self, example_env):
        example_env.client.create_object(
            "employer", {"meta": {"id": "employer1"}}
        )

        assert [["employer1"]] == example_env.client.select_objects(
            "employer",
            selectors=["/meta/id"],
            filter="case [/meta/id] when 'employer1' then '1' when 'employer2' then '2' end = '1'",
        )
