from .batch_helpers import batch_apply
from .config import get_config, get_total_request_timeout
from .common import group_blobs_by_size, MB
from .cypress_commands import mkdir, concatenate, find_free_subpath, remove
from .default_config import DEFAULT_WRITE_CHUNK_SIZE, DEFAULT_WRITE_PARALLEL_MAX_THREAD_COUNT
from .ypath import YPath, YPathSupportingAppend
from .transaction import Transaction
from .transaction_commands import _make_transactional_request
from .thread_pool import ThreadPool
from .heavy_commands import WriteRequestRetrier

import copy
import threading

class ParallelWriter(object):
    def __init__(self, path, params, create_object, unordered, transaction_timeout,
                 thread_count, write_action, remote_temp_directory, tx_id, client):
        self._unordered = unordered
        self._remote_temp_directory = remote_temp_directory
        self._write_action = write_action
        self._transaction_timeout = transaction_timeout
        self._path = path
        self._create_object = create_object
        self._tx_id = tx_id
        self._client = client

        self._path_attributes = copy.deepcopy(self._path.attributes)
        self._path_attributes["append"] = False

        self._thread_data = {}
        self._pool = ThreadPool(thread_count, self._init_thread, (get_config(client), params))

    def _init_thread(self, client_config, params):
        # TODO: Fix in YT-6615
        from .client import YtClient
        ident = threading.current_thread().ident

        client = YtClient(config=client_config)

        write_action = lambda chunk, params: self._write_action(chunk, params, client)
        retrier = WriteRequestRetrier(transaction_timeout=self._transaction_timeout,
                                      write_action=write_action,
                                      client=client)

        self._thread_data[ident] = {"client": client,
                                    "params": copy.deepcopy(params),
                                    "retrier": retrier}

    def _create_temp_object(self, client):
        if not get_config(client)["write_parallel"]["use_tmp_dir_for_intermediate_data"]:
            path = find_free_subpath(str(self._path) + ".", client=client)
        else:
            temp_directory = self._remote_temp_directory
            mkdir(temp_directory, recursive=True, client=client)
            if not temp_directory.endswith("/"):
                temp_directory = temp_directory + "/"
            path = find_free_subpath(temp_directory, client=client)

        self._create_object(path, client)
        return path

    def _write_chunk(self, chunk):
        ident = threading.current_thread().ident

        retrier = self._thread_data[ident]["retrier"]
        params = self._thread_data[ident]["params"]
        client = self._thread_data[ident]["client"]

        with client.Transaction(transaction_id=self._tx_id):
            params["path"] = YPath(self._create_temp_object(client), attributes=self._path_attributes)
            retrier.run_write_action(chunk, params)

        return str(params["path"])

    def _write_iterator(self, chunks):
        if self._unordered:
            return self._pool.imap_unordered(self._write_chunk, chunks)
        return self._pool.imap(self._write_chunk, chunks)

    def write(self, blobs):
        try:
            output_objects = []
            for table in self._write_iterator(blobs):
                output_objects.append(table)
                if len(output_objects) >= get_config(self._client)["write_parallel"]["concatenate_size"]:
                    concatenate(output_objects, self._path, client=self._client)
                    batch_apply(remove, output_objects, client=self._client)
                    self._path.attributes["append"] = True
                    output_objects = []
            if output_objects:
                concatenate(output_objects, self._path, client=self._client)
                batch_apply(remove, output_objects, client=self._client)
        finally:
            self._pool.close()


def _get_chunk_size_and_thread_count(size_hint, config):
    chunk_size = config["write_retries"]["chunk_size"]
    thread_count = config["write_parallel"]["max_thread_count"]
    if size_hint is None:
        chunk_size = chunk_size if chunk_size is not None else DEFAULT_WRITE_CHUNK_SIZE
        thread_count = thread_count if thread_count is not None else DEFAULT_WRITE_PARALLEL_MAX_THREAD_COUNT
    elif chunk_size is None and thread_count is None:
        chunk_size = 128 * MB
        thread_count = min(15, size_hint // chunk_size + 1)
    elif chunk_size is None:
        # NB: make sure that 64MB <= chunk_size <= 512MB
        chunk_size = min(max((64 * MB, size_hint // thread_count)), 512 * MB)
    else:
        thread_count = min(15, size_hint // chunk_size + 1)
    return chunk_size, thread_count

def make_parallel_write_request(command_name, stream, path, params, unordered,
                                create_object, remote_temp_directory, size_hint=None, client=None):
    path = YPathSupportingAppend(path, client=client)
    transaction_timeout = get_total_request_timeout(client)
    if get_config(client)["yamr_mode"]["create_tables_outside_of_transaction"]:
        create_object(path, client)

    title = "Python wrapper: {0} {1}".format(command_name, path)
    with Transaction(timeout=transaction_timeout,
                     attributes={"title": title},
                     client=client,
                     transaction_id=get_config(client)["write_retries"]["transaction_id"]) as tx:
        chunk_size, thread_count = _get_chunk_size_and_thread_count(size_hint, get_config(client))

        write_action = lambda chunk, params, client: _make_transactional_request(
            command_name,
            params,
            data=iter(chunk),
            is_data_compressed=False,
            use_heavy_proxy=True,
            client=client)

        writer = ParallelWriter(
            tx_id=tx.transaction_id,
            path=path,
            create_object=create_object,
            transaction_timeout=transaction_timeout,
            thread_count=thread_count,
            params=params,
            unordered=unordered,
            write_action=write_action,
            remote_temp_directory=remote_temp_directory,
            client=client)

        writer.write(group_blobs_by_size(stream, chunk_size))
