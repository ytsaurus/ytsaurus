from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Protocol
    import yt.wrapper as yt

    # NOTE: This is an interface for all progress monitoring objects
    # It's not documented anywhere, but used in type annotation
    class _ProgressMonitor(Protocol):
        def start(self): ...
        def finish(self, status="ok"): ...
        def update(self, size): ...


from .batch_helpers import batch_apply
from .config import get_config
from .common import MB, typing  # noqa
from .cypress_commands import get, mkdir, concatenate, find_free_subpath, remove, exists
from .default_config import DEFAULT_WRITE_CHUNK_SIZE
from .driver import make_request
from .ypath import _STORAGE_ATTRIBUTES, _WRITABLE_STORAGE_ATTRIBUTES, YPath, YPathSupportingAppend, ypath_dirname, ypath_join
from .progress_bar import SimpleProgressBar, FakeProgressReporter
from .stream import RawStream, ItemStream
from .transaction import Transaction
from .thread_pool import ThreadPool
from .heavy_commands import WriteRequestRetrier

from yt.common import YT_NULL_TRANSACTION_ID

import copy
import random
import threading


class _ParallelProgressReporter(object):
    def __init__(self, monitor):
        self._monitor = monitor
        self._lock = threading.Lock()

    def __enter__(self):
        self._monitor.start()

    def __exit__(self, type, value, traceback):
        self._monitor.finish()

    def __del__(self):
        self._monitor.finish()

    def wrap_stream(self, stream):
        for substream in stream:
            yield _SubstreamWrapper(substream, self._update)

    def _update(self, size):
        with self._lock:
            self._monitor.update(size)


class _SubstreamWrapper(object):
    def __init__(self, substream, update):
        self._substream = substream
        self._update = update

    def __iter__(self):
        for chunk in self._substream:
            self._update(len(chunk))
            yield chunk


class ParallelWriter(object):
    def __init__(self, path, params, create_object, unordered, transaction_timeout,
                 thread_count, write_action, remote_temp_directory, tx_id,
                 use_tmp_dir_for_intermediate_data, concatenate_size, storage_attributes,
                 client):
        self._unordered = unordered
        self._remote_temp_directory = remote_temp_directory
        self._write_action = write_action
        self._transaction_timeout = transaction_timeout
        self._path = path
        self._create_object = create_object
        self._tx_id = tx_id
        self._concatenate_size = concatenate_size
        self._use_tmp_dir_for_intermediate_data = use_tmp_dir_for_intermediate_data
        self._client = client

        self._storage_attributes = storage_attributes

        self._thread_data = {}
        self._pool = ThreadPool(thread_count, self._init_thread, (get_config(client), params))

    def _init_thread(self, client_config, params):
        # TODO: Fix in YT-6615
        from .client import YtClient
        ident = threading.current_thread().ident

        client = YtClient(config=client_config)

        write_action = lambda chunk, params: self._write_action(chunk, params, client)  # noqa
        retrier = WriteRequestRetrier(transaction_timeout=self._transaction_timeout,
                                      write_action=write_action,
                                      client=client)

        self._thread_data[ident] = {"client": client,
                                    "params": copy.deepcopy(params),
                                    "retrier": retrier}

    def _create_temp_object(self, client):
        if not self._use_tmp_dir_for_intermediate_data:
            path = find_free_subpath(str(self._path) + ".", client=client)
        else:
            temp_directory = self._remote_temp_directory
            bucket_count = get_config(client)["remote_temp_tables_bucket_count"]
            bucket = "{:02x}".format(random.randrange(bucket_count))
            temp_directory = ypath_join(temp_directory, bucket)
            with client.Transaction(transaction_id=YT_NULL_TRANSACTION_ID):
                mkdir(temp_directory, recursive=True, client=client)
            if not temp_directory.endswith("/"):
                temp_directory = temp_directory + "/"
            path = find_free_subpath(temp_directory, client=client)

        path = YPath(path=path, client=client)
        self._create_object(path, client, attributes=self._storage_attributes)
        return path

    def _write_chunk(self, chunk):
        ident = threading.current_thread().ident

        retrier = self._thread_data[ident]["retrier"]
        params = self._thread_data[ident]["params"]
        client = self._thread_data[ident]["client"]

        with client.Transaction(transaction_id=self._tx_id):
            params["path"] = YPath(self._create_temp_object(client))
            retrier.run_write_action(chunk, params)

        return str(params["path"])

    def _write_iterator(self, chunks):
        if self._unordered:
            return self._pool.imap_unordered(self._write_chunk, chunks)
        return self._pool.imap(self._write_chunk, chunks)

    def write(self, chunks):
        try:
            output_objects = []
            path = self._path
            for table in self._write_iterator(chunks):
                output_objects.append(table)
                if len(output_objects) >= self._concatenate_size:
                    concatenate(output_objects, path, client=self._client)
                    batch_apply(remove, output_objects, client=self._client)
                    if not path.append:
                        path = path.clone_with_append_set()

                    output_objects = []
            if output_objects:
                concatenate(output_objects, path, client=self._client)
                batch_apply(remove, output_objects, client=self._client)
        finally:
            self._pool.close()


def _get_chunk_size_and_thread_count(size_hint, config):
    chunk_size = config["write_retries"]["chunk_size"]
    thread_count = config["write_parallel"]["max_thread_count"]
    memory_limit = config["write_parallel"]["memory_limit"]
    if size_hint is not None:
        memory_limit = min((memory_limit, size_hint))
    if chunk_size is None and thread_count is None:
        chunk_size = DEFAULT_WRITE_CHUNK_SIZE
        # NB: make sure that 1 <= thread_count <= 15
        thread_count = min(max(memory_limit // chunk_size, 1), 15)
    elif chunk_size is None:
        # NB: make sure that 64MB <= chunk_size <= 512MB
        chunk_size = min(max(64 * MB, memory_limit // thread_count), 512 * MB)
    elif thread_count is None:
        # NB: make sure that 1 <= thread_count <= 15
        thread_count = min(max(memory_limit // chunk_size, 1), 15)
    return chunk_size, thread_count


def make_parallel_write_request(
    command_name: str,
    stream: RawStream | ItemStream,
    path: str | YPath,
    params: dict,
    unordered: bool,
    create_object: callable[[YPath, yt.YtClient], None],
    remote_temp_directory: str,
    size_hint: int | None = None,
    filename_hint: str | None = None,
    progress_monitor: _ProgressMonitor | None = None,
    client: yt.YtClient | None = None
) -> None:

    assert isinstance(stream, (RawStream, ItemStream))

    if stream.isatty():
        enable_progress_bar = False
    else:
        enable_progress_bar = get_config(client)["write_progress_bar"]["enable"] and not stream.isatty()

    if progress_monitor is None:
        progress_monitor = SimpleProgressBar("upload", size_hint, filename_hint, enable_progress_bar)
    if get_config(client)["write_progress_bar"]["enable"] is not False:
        progress_reporter = _ParallelProgressReporter(progress_monitor)
    else:
        progress_reporter = FakeProgressReporter()

    path = YPathSupportingAppend(path, client=client)

    transaction_timeout = max(
        get_config(client)["proxy"]["heavy_request_timeout"],
        get_config(client)["transaction_timeout"])
    created = False

    storage_attributes = {
        key: value
        for key, value in path.attributes.items()
        if key in _WRITABLE_STORAGE_ATTRIBUTES
    }

    if get_config(client)["yamr_mode"]["create_tables_outside_of_transaction"]:
        create_object(path, client, storage_attributes)
        created = True

    write_parallel_config = get_config(client)["write_parallel"]
    title = "Python wrapper: {0} {1}".format(command_name, path)
    with Transaction(timeout=transaction_timeout,
                     attributes={"title": title},
                     client=client,
                     transaction_id=get_config(client)["write_retries"]["transaction_id"]) as tx:
        if not created:
            create_object(path, client, storage_attributes)

        # NOTE: Path should exist at this point, as it was created just before we called _get_storage_attributes()
        storage_attributes = get(path + "/@", attributes=tuple(_STORAGE_ATTRIBUTES), client=client)

        chunk_size, thread_count = _get_chunk_size_and_thread_count(size_hint, get_config(client))
        stream = stream.into_chunks(chunk_size)

        # NB: split stream into 2 MB pieces so that we can safely send them separately
        # without a risk of triggering timeout.
        stream = stream.split_chunks(2 * MB)

        write_action = lambda chunk, params, client: make_request(  # noqa
            command_name,
            params,
            data=iter(chunk),
            is_data_compressed=False,
            use_heavy_proxy=True,
            client=client)

        use_tmp_dir_for_intermediate_data = write_parallel_config["use_tmp_dir_for_intermediate_data"]
        target_medium = storage_attributes.get("primary_medium")
        if target_medium not in ("default", None) and use_tmp_dir_for_intermediate_data:
            # Resolve account for remote_tmp_directory
            parent_directory = remote_temp_directory
            while not exists(parent_directory, client=client):
                parent_directory = ypath_dirname(parent_directory)
            account = get(parent_directory + '/@account', client=client)
            # If there is no access to target_medium in tmp directory account - let's disable it
            if not exists(f'//sys/accounts/{account}/@resource_limits/disk_space_per_medium/{target_medium}', client=client):
                use_tmp_dir_for_intermediate_data = False

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
            use_tmp_dir_for_intermediate_data=use_tmp_dir_for_intermediate_data,
            concatenate_size=write_parallel_config["concatenate_size"],
            storage_attributes=storage_attributes,
            client=client)

        with progress_reporter:
            writer.write(progress_reporter.wrap_stream(stream))
