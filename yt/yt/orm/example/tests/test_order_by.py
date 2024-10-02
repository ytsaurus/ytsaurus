from .conftest import create_user_client, sync_access_control, ExampleTestEnvironment

from yt.orm.library.common import AuthorizationError, InvalidRequestArgumentsError

import pytest


class TestOrderBy:
    @pytest.fixture(params=[False, True], ids=["with_continuation", "without_continuation"])
    def _do_test_continuation_token(self, request):
        return request.param

    @pytest.fixture
    def _select(self, example_env, _do_test_continuation_token):
        def _inner(object, limit, order_by):
            def unwrap_result(result):
                return [[value['value'] for value in values] for values in result]

            results = []
            current_limit = 1 if _do_test_continuation_token else limit
            continuation_token = None
            while len(results) < limit:
                result = example_env.client.select_objects(
                    object,
                    selectors=["/meta/id"],
                    limit=current_limit,
                    order_by=order_by,
                    enable_structured_response=True,
                    options={"continuation_token": continuation_token},
                )

                continuation_token = result["continuation_token"]
                unwrapped_result = unwrap_result(result["results"])
                results.extend(unwrapped_result)
                if len(unwrapped_result) < current_limit:
                    break
            return results

        return _inner

    def test_employer_job_order_by(self, example_env, _select):
        example_env.client.create_object(
            "employer", {"meta": {"id": "employer1"}, "status": {"job_title": "baker", "salary": 100}}
        )
        example_env.client.create_object(
            "employer", {"meta": {"id": "employer2"}, "status": {"job_title": "plumber", "salary": 200}}
        )

        order_by = [{"expression": "[/status/job_title] = 'plumber'"}]
        result = _select("employer", limit=2, order_by=order_by)
        assert result == [["employer1"], ["employer2"]]

        order_by = [{"expression": "[/status/job_title] = 'baker'"}]
        result = _select("employer", limit=2, order_by=order_by)
        assert result == [["employer2"], ["employer1"]]

        order_by = [{"expression": "[/status/salary] * 2'"}]
        result = _select("employer", limit=2, order_by=order_by)
        assert result == [["employer1"], ["employer2"]]

    def test_hitchhiker_order_by(self, example_env, _select):
        example_env.client.create_object(
            "hitchhiker", {"meta": {"id": 1}, "spec": {"name": "Alex", "miles_traveled": 0, "galaxies_visited": 100}}
        )
        example_env.client.create_object(
            "hitchhiker", {"meta": {"id": 2}, "spec": {"name": "Peter", "miles_traveled": 10, "galaxies_visited": 15}}
        )
        example_env.client.create_object(
            "hitchhiker", {"meta": {"id": 3}, "spec": {"name": "Ivan", "miles_traveled": 15, "galaxies_visited": 10}}
        )

        order_by = [{"expression": "[/spec/miles_traveled] * 2 + [/spec/galaxies_visited]'"}]
        result = _select("hitchhiker", limit=3, order_by=order_by)
        assert result == [[2], [3], [1]]

        order_by = [
            {"expression": "[/spec/miles_traveled] + [/spec/galaxies_visited]'"},
            {"expression": "[/spec/name]"},
        ]
        result = _select("hitchhiker", limit=3, order_by=order_by)
        assert result == [[3], [2], [1]]

        order_by = [{"expression": "Length([/spec/name])"}, {"expression": "IF([/spec/name] = 'Ivan', 10, 20)"}]
        result = _select("hitchhiker", limit=3, order_by=order_by)
        assert result == [[3], [1], [2]]

    def test_access_control(self, test_environment, example_env):
        def update_schema_permissions_for(object_type, permissions):
            example_env.client.update_object(
                "schema",
                object_type,
                set_updates=[{"path": "/meta/acl", "value": permissions}],
            )

        example_env.client.create_object("publisher", {"meta": {"id": 1}, "spec": {"name": "Some Publisher Co"}})
        update_schema_permissions_for("publisher", [])

        with create_user_client(test_environment) as client:
            update_schema_permissions_for(
                "publisher",
                [
                    {
                        "action": "allow",
                        "permissions": ["read"],
                        "subjects": [client._config["user"]],
                        "attributes": ["/spec/name"],
                    }
                ],
            )
            sync_access_control()

            client.select_objects("publisher", selectors=["/spec/name"])
            with pytest.raises(AuthorizationError):
                order_by = [{"expression": "[/meta/id]"}]
                client.select_objects("publisher", selectors=["/spec/name"], order_by=order_by)

    def test_any_forbidden(self, example_env: ExampleTestEnvironment):
        example_env.create_book()

        with pytest.raises(InvalidRequestArgumentsError):
            order_by = [{"expression": "try_get_any([/spec/name], \"\")"}]
            example_env.client.select_objects("book", order_by=order_by)
