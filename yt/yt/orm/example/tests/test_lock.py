from yt.wrapper.errors import YtTabletTransactionLockConflict

import pytest


class TestConcurrentRemoveAndUpdate:
    @pytest.fixture
    def publisher_id(self, example_env):
        return example_env.client.create_object("publisher", request_meta_response=True)

    @pytest.fixture
    # Remove is update meta.removal_time field with lock=api
    def remove_transaction_id(self, example_env, publisher_id):
        transaction_id = example_env.client.start_transaction()
        example_env.client.remove_object("publisher", publisher_id, transaction_id=transaction_id)
        return transaction_id

    @pytest.fixture
    # Update spec.year_of_fondation with lock=api
    def update_year_of_fondation_transaction_id(self, example_env, publisher_id):
        transaction_id = example_env.client.start_transaction()
        example_env.client.update_object(
            "publisher",
            publisher_id,
            [{"path": "/spec/year_of_fondation", "value": 1990}],
            transaction_id=transaction_id,
        )
        return transaction_id

    @pytest.fixture
    # Update spec.name with lock=web
    def update_name_transaction_id(self, example_env, publisher_id):
        transaction_id = example_env.client.start_transaction()
        example_env.client.update_object(
            "publisher",
            publisher_id,
            [{"path": "/spec/name", "value": "O'Relly"}],
            transaction_id=transaction_id,
        )
        return transaction_id

    @pytest.fixture
    # Update status.licensed with lock=worker from parent message
    def update_licensed_transaction_id(self, example_env, publisher_id):
        transaction_id = example_env.client.start_transaction()
        example_env.client.update_object(
            "publisher",
            publisher_id,
            [{"path": "/status/licensed", "value": True}],
            transaction_id=transaction_id,
        )
        return transaction_id

    def test_remove_and_update(
        self,
        remove_transaction_id,
        update_name_transaction_id,
        update_year_of_fondation_transaction_id,
        update_licensed_transaction_id,
        example_env,
    ):
        example_env.client.commit_transaction(remove_transaction_id)
        example_env.client.commit_transaction(update_name_transaction_id)
        example_env.client.commit_transaction(update_licensed_transaction_id)
        with pytest.raises(YtTabletTransactionLockConflict):
            example_env.client.commit_transaction(update_year_of_fondation_transaction_id)

    def test_update_and_remove(
        self,
        remove_transaction_id,
        update_name_transaction_id,
        update_year_of_fondation_transaction_id,
        update_licensed_transaction_id,
        example_env,
    ):
        example_env.client.commit_transaction(update_year_of_fondation_transaction_id)
        example_env.client.commit_transaction(update_name_transaction_id)
        example_env.client.commit_transaction(update_licensed_transaction_id)
        with pytest.raises(YtTabletTransactionLockConflict):
            example_env.client.commit_transaction(remove_transaction_id)
