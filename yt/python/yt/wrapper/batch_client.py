from . import batch_api
from .client_helpers import initialize_client, create_class_method
from .client_state import ClientState


class BatchClient(ClientState):
    """Implements YT batch client.
    An object of this class is a copy of object of YtClient class
    without methods which are forbidden in batch executions (like write_table, run_map, etc.).

    An object of class BatchClient also has commit_batch method which commits tasks of the current batch.
    """

    def __init__(self, batch_executor, client_state=None, proxy=None, token=None, config=None):
        super(BatchClient, self).__init__(client_state)
        initialize_client(self, proxy, token, config)
        self._batch_executor = batch_executor
        self._client_type = "batch"

    def commit_batch(self):
        self._batch_executor.commit_batch()


for name in batch_api._batch_commands:
    setattr(BatchClient, name, create_class_method(getattr(batch_api, name)))
