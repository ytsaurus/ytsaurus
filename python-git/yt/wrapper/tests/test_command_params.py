from .helpers import TEST_DIR

import yt.wrapper as yt

import pytest

@pytest.mark.usefixtures("yt_env")
class TestPrerequisite(object):
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

    def test_prerequisite_revision(self):
        yt.mkdir(TEST_DIR + "/prerequisite", recursive=True)
        revision = yt.create_revision_parameter(TEST_DIR + "/prerequisite")
        client = yt.create_client_with_command_params(prerequisite_revisions=[revision])
        client.mkdir(TEST_DIR + "/prerequisite_test")
        assert client.exists(TEST_DIR + "/prerequisite_test")

        with pytest.raises(yt.YtError):
            revision = yt.create_revision_parameter(TEST_DIR + "/prerequisite")
            client = yt.create_client_with_command_params(prerequisite_revisions=[revision])
            client.remove(TEST_DIR + "/prerequisite")
            client.mkdir(TEST_DIR + "/prerequisite")

        assert not yt.exists(TEST_DIR + "/prerequisite")
        yt.mkdir(TEST_DIR + "/prerequisite")

        revisions = [yt.create_revision_parameter(TEST_DIR + "/prerequisite"),
                     yt.create_revision_parameter(TEST_DIR + "/prerequisite_test")]
        client = yt.create_client_with_command_params(prerequisite_revisions=revisions)
        client.mkdir(TEST_DIR + "/prerequisite_test/test")
        assert client.exists(TEST_DIR + "/prerequisite_test/test")

        with pytest.raises(yt.YtError):
            revisions = [yt.create_revision_parameter(TEST_DIR + "/prerequisite"),
                         yt.create_revision_parameter(TEST_DIR + "/prerequisite_test")]
            client = yt.create_client_with_command_params(prerequisite_revisions=revisions)
            client.remove(TEST_DIR + "/prerequisite_test/test")
            client.mkdir(TEST_DIR + "/prerequisite/test")

        assert not yt.exists(TEST_DIR + "/prerequisite/test")
        assert not yt.exists(TEST_DIR + "/prerequisite_test/test")
