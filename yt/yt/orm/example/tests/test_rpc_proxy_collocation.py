from yt.orm.library.common import YtResponseError

import yt.wrapper as yt

import uuid
import pytest


@pytest.fixture(scope="function")
def rpc_proxy_address(example_env):
    masters = example_env.client.get_masters()["master_infos"]
    assert 1 == len(masters)
    address = masters[0]["rpc_proxy_address"]
    assert address
    return address


@pytest.fixture(scope="function")
def rpc_proxy_client(rpc_proxy_address):
    # Configure proxy addresses manually, because
    # RPC proxy collocation does not support discovery via cluster_url.
    yield yt.YtClient(
        rpc_proxy_address,
        config=dict(
            backend="rpc",
            driver_config=dict(
                connection_type="rpc",
                proxy_addresses=[rpc_proxy_address],
            ),
        ),
    )


@pytest.fixture(scope="function")
def state_table(rpc_proxy_client):
    path = "//tmp/state_{}".format(uuid.uuid4().hex[:8])
    rpc_proxy_client.create(
        "table",
        path,
        attributes={
            "dynamic": True,
            "schema": [
                {"name": "key", "type": "string", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ],
        },
    )
    rpc_proxy_client.mount_table(path, sync=True)
    yield path


class TestRpcProxyCollocation:
    def test_independent_transactions(self, example_env, rpc_proxy_client, state_table):
        # Smoke test: independent transactions do not crash each other.

        rpc_proxy_client.insert_rows(
            state_table,
            [{"key": "a", "value": "123"}],
        )

        transaction_id = example_env.client.start_transaction()
        example_env.client.create_object(
            "user",
            attributes=dict(meta=dict(id="u1")),
            transaction_id=transaction_id,
        )
        assert [] == example_env.client.select_objects(
            "user",
            filter='[/meta/id] = "u1"',
            selectors=["/meta/id"],
        )
        example_env.client.commit_transaction(transaction_id)
        assert [["u1"]] == example_env.client.select_objects(
            "user",
            filter='[/meta/id] = "u1"',
            selectors=["/meta/id"],
        )

        rpc_proxy_client.insert_rows(
            state_table,
            [{"key": "b", "value": "456"}],
        )

    def test_common_transaction(self, example_env, rpc_proxy_client, state_table):
        def select_rpc_proxy():
            return list(
                rpc_proxy_client.select_rows(
                    "key, value from [{}]".format(state_table),
                )
            )

        def select_orm():
            return example_env.client.select_objects(
                "user",
                filter='[/meta/id] = "u2"',
                selectors=["/meta/id"],
            )

        with rpc_proxy_client.Transaction(type="tablet") as underlying_transaction:
            underlying_transaction_id = underlying_transaction.transaction_id
            rpc_proxy_client.insert_rows(
                state_table,
                [{"key": "a", "value": "123"}],
            )

            transaction_id = example_env.client.start_transaction(
                underlying_transaction_id=underlying_transaction_id
            )
            example_env.client.create_object(
                "user",
                attributes=dict(meta=dict(id="u2")),
                transaction_id=transaction_id,
            )

            assert [] == select_rpc_proxy()
            assert [] == select_orm()

            example_env.client.commit_transaction(transaction_id)

            assert [] == select_rpc_proxy()
            assert [] == select_orm()

        assert [{"key": "a", "value": "123"}] == select_rpc_proxy()
        assert [["u2"]] == select_orm()

    def test_start_timestamp(self, example_env, rpc_proxy_client):
        with rpc_proxy_client.Transaction(type="tablet") as underlying_transaction:
            underlying_transaction_id = underlying_transaction.transaction_id

            start_timestamp = example_env.client.start_transaction(
                underlying_transaction_id=underlying_transaction_id,
                enable_structured_response=True,
            )["start_timestamp"]
            assert start_timestamp > 2

            with pytest.raises(YtResponseError):
                example_env.client.start_transaction(
                    start_timestamp=start_timestamp - 1,
                    underlying_transaction_id=underlying_transaction_id,
                )
            with pytest.raises(YtResponseError):
                example_env.client.start_transaction(
                    start_timestamp=start_timestamp + 1,
                    underlying_transaction_id=underlying_transaction_id,
                )

            example_env.client.start_transaction(
                start_timestamp=start_timestamp,
                underlying_transaction_id=underlying_transaction_id,
            )

    def test_underlying_transaction_address(
        self, example_env, rpc_proxy_client, rpc_proxy_address
    ):
        # Address is specified, but id is missing.
        with pytest.raises(YtResponseError):
            example_env.client.start_transaction(underlying_transaction_address=rpc_proxy_address)

        with rpc_proxy_client.Transaction(type="tablet") as underlying_transaction:
            underlying_transaction_id = underlying_transaction.transaction_id

            # Address must be empty for collocated transaction.
            with pytest.raises(YtResponseError):
                example_env.client.start_transaction(
                    underlying_transaction_id=underlying_transaction_id,
                    underlying_transaction_address=rpc_proxy_address,
                )

            # Sanity check.
            example_env.client.start_transaction(
                underlying_transaction_id=underlying_transaction_id,
            )
