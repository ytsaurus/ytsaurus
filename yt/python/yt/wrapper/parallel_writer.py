from .batch_helpers import batch_apply
from .config import get_config
from .common import MB, typing  # noqa
from .cypress_commands import mkdir, concatenate, find_free_subpath, remove
from .default_config import DEFAULT_WRITE_CHUNK_SIZE
from .driver import make_request
from .ypath import YPath, YPathSupportingAppend, ypath_join
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

        write_action = lambda chunk, params: self._write_action(chunk, params, client)  # noqa
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
            bucket_count = get_config(client)["remote_temp_tables_bucket_count"]
            bucket = "{:02x}".format(random.randrange(bucket_count))
            temp_directory = ypath_join(temp_directory, bucket)
            with client.Transaction(transaction_id=YT_NULL_TRANSACTION_ID):
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

    def write(self, chunks):
        try:
            output_objects = []
            for table in self._write_iterator(chunks):
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


def make_parallel_write_request(command_name, stream, path, params, unordered,
                                create_object, remote_temp_directory, size_hint=None,
                                filename_hint=None, progress_monitor=None, client=None):
    # type: (str, RawStream | ItemStream, str | YPath, dict, bool, typing.Callable[[YPath, yt.YtClient], None], str, int | None, str | None, _ProgressReporter | None, yt.YtClient | None) -> None

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
    if get_config(client)["yamr_mode"]["create_tables_outside_of_transaction"]:
        create_object(path, client)

    title = "Python wrapper: {0} {1}".format(command_name, path)
    with Transaction(timeout=transaction_timeout,
                     attributes={"title": title},
                     client=client,
                     transaction_id=get_config(client)["write_retries"]["transaction_id"]) as tx:
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

        with progress_reporter:
            writer.write(progress_reporter.wrap_stream(stream))
