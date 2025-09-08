from .conftest import authors
from .helpers import TEST_DIR

import yt.wrapper as yt

import pytest


@pytest.mark.usefixtures("yt_env")
class TestPrerequisite(object):
    @authors("ostyakov")
    def test_prerequisite_transaction(self):
        transaction_id = None
        with yt.Transaction() as tx:
            transaction_id = tx.transaction_id
            client = yt.create_client_with_command_params(prerequisite_transaction_ids=[transaction_id])
            client.mkdir(TEST_DIR + "/prerequisite", recursive=True)
            assert client.exists(TEST_DIR + "/prerequisite")

        with pytest.raises(yt.YtError):
            client = yt.create_client_with_command_params(prerequisite_transaction_ids=[transaction_id])
            client.mkdir(TEST_DIR + "/prerequisite/test", recursive=True)

        with yt.Transaction() as tx:
            with yt.Transaction() as another_tx:
                transaction_id = another_tx.transaction_id
                client = yt.create_client_with_command_params(prerequisite_transaction_ids=[tx.transaction_id,
                                                                                            another_tx.transaction_id])
                client.mkdir(TEST_DIR + "/prerequisite/test_many", recursive=True)
                assert client.exists(TEST_DIR + "/prerequisite/test_many")

            with pytest.raises(yt.YtError):
                client = yt.create_client_with_command_params(prerequisite_transaction_ids=[tx.transaction_id,
                                                                                            another_tx.transaction_id])
                client.mkdir(TEST_DIR + "/prerequisite/test", recursive=True)

        client = yt.create_client_with_command_params()
        with yt.Transaction() as tx:
            with client.Transaction(prerequisite_transaction_ids=[tx.transaction_id]):
                client.mkdir(TEST_DIR + "/prerequisite/test2", recursive=True)
                assert client.exists(TEST_DIR + "/prerequisite/test2")

            assert yt.exists(TEST_DIR + "/prerequisite/test2")

        with pytest.raises(yt.YtError):
            with client.Transaction(prerequisite_transaction_ids=[tx.transaction_id]):
                client.mkdir(TEST_DIR + "/prerequisite/test3", recursive=True)

        assert not yt.exists(TEST_DIR + "/prerequisite/test3")

        with pytest.raises(RuntimeError) as e:
            with client.Transaction(transaction_id=tx.transaction_id, prerequisite_transaction_ids=[tx.transaction_id]):
                pass

        assert "prerequisite_transaction_ids=['{}'] must be None or empty when transaction_id is not None".format(tx.transaction_id) in str(e.value)


@pytest.mark.usefixtures("yt_env")
class TestTransactionsWithCommandParams(object):
    @authors("ignat")
    def test_read_write(self):
        client = yt.create_client_with_command_params()
        client.write_table(TEST_DIR + "/test_table", [{"a": 10}])
        assert list(client.read_table(TEST_DIR + "/test_table")) == [{"a": 10}]
