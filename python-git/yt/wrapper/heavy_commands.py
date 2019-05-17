from .config import get_option, get_config, get_total_request_timeout, get_command_param
from .common import (CustomTqdm, group_blobs_by_size, split_lines_by_max_size,
                     stream_or_empty_bytes, YtError, MB)
from .default_config import DEFAULT_WRITE_CHUNK_SIZE
from .retries import Retrier, IteratorRetrier, default_chaos_monkey
from .errors import YtMasterCommunicationError, YtChunkUnavailable
from .ypath import YPathSupportingAppend
from .transaction import Transaction
from .transaction_commands import _make_transactional_request
from .http_helpers import get_retriable_errors
from .response_stream import ResponseStreamWithReadRow
from .lock_commands import lock
from .format import YtFormatReadError

import yt.logger as logger

import time
import os

def _split_stream_into_pieces(stream):
    # NB: we first split every line in a stream into pieces <= 1 MB,
    # then merge consequent lines into chunks >= 1 MB.
    # This way, the resulting chunks' size is between 1 MB and 2 MB.
    stream_split = split_lines_by_max_size(stream, MB)
    stream_grouped = group_blobs_by_size(stream_split, MB)
    for group in stream_grouped:
        yield b"".join(group)

class _FakeProgressReporter(object):
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        pass

    def __exit__(self, type, value, traceback):
        pass

    def wrap_stream(self, stream):
        return stream

class _ProgressReporter(object):
    def __init__(self, monitor):
        self._monitor = monitor

    def __enter__(self):
        self._monitor.start()

    def __exit__(self, type, value, traceback):
        self._monitor.finish()

    def wrap_stream(self, stream):
        for chunk in stream:
            self._monitor.update(len(chunk))
            yield chunk

class _SimpleProgressBar(object):
    def __init__(self, size_hint=None, filename_hint=None, enable=None):
        self.size_hint = size_hint
        self.filename_hint = filename_hint
        self.enable = enable
        self._tqdm = None

    def _set_status(self, status):
        if self.filename_hint:
            self._tqdm.set_description("[{}]: {}".format(status.upper(), os.path.basename(self.filename_hint)))
        else:
            self._tqdm.set_description("[{}]".format(status.upper()))

    def start(self):
        if self.enable is None:
            disable = None
        else:
            disable = not self.enable
        self._tqdm = CustomTqdm(disable=disable, total=self.size_hint, leave=False)
        self._set_status("upload")
        return self

    def finish(self, status="ok"):
        self._set_status(status)
        self._tqdm.close()
        self._tqdm = None

    def update(self, size):
        self._tqdm.update(size)

def process_read_exception(exception):
    logger.warning("Read request failed with error: %s", str(exception))

class FakeTransaction(object):
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    def __bool__(self):
        return False

    def abort(self):
        pass

    def commit(self):
        pass

    def is_pinger_alive(self):
        return True

    __nonzero__ = __bool__

class WriteRequestRetrier(Retrier):
    def __init__(self, transaction_timeout, write_action, client=None):
        retry_config = get_config(client)["write_retries"]
        request_timeout = get_config(client)["proxy"]["heavy_request_timeout"]
        chaos_monkey_enable = get_option("_ENABLE_HEAVY_REQUEST_CHAOS_MONKEY", client)
        super(WriteRequestRetrier, self).__init__(retry_config=retry_config,
                                                  timeout=request_timeout,
                                                  exceptions=get_retriable_errors() + (YtMasterCommunicationError,),
                                                  chaos_monkey=default_chaos_monkey(chaos_monkey_enable))
        self.write_action = write_action
        self.transaction_timeout = transaction_timeout
        self.client = client

        self.chunk = None
        self.params = None

    def action(self):
        assert self.chunk is not None

        with Transaction(timeout=self.transaction_timeout, client=self.client):
            self.write_action(self.chunk, self.params)

    def except_action(self, error, attempt):
        logger.warning("%s: %s", type(error), str(error))

    def run_write_action(self, chunk, params):
        self.chunk = chunk
        self.params = params
        self.run()

        self.chunk = None
        self.params = None

def make_write_request(command_name, stream, path, params, create_object, use_retries,
                       is_stream_compressed=False, size_hint=None, filename_hint=None,
                       progress_monitor=None, client=None):
    # NB: if stream is empty, we still want to make an empty write, e.g. for `write_table(name, [])`.
    # So, if stream is empty, we explicitly make it b"".
    stream = stream_or_empty_bytes(stream)

    # NB: split stream into 1-2 MB pieces so that we can safely send them separately
    # without a risk of triggering timeout.
    chunk_size = get_config(client)["write_retries"]["chunk_size"]
    if chunk_size is None:
        chunk_size = DEFAULT_WRITE_CHUNK_SIZE
    if chunk_size > 2 * MB:
        stream = _split_stream_into_pieces(stream)

    path = YPathSupportingAppend(path, client=client)
    transaction_timeout = get_total_request_timeout(client)

    created = False
    if get_config(client)["yamr_mode"]["create_tables_outside_of_transaction"]:
        create_object(path, client)
        created = True

    title = "Python wrapper: {0} {1}".format(command_name, path)

    with Transaction(timeout=transaction_timeout,
                     attributes={"title": title},
                     client=client,
                     transaction_id=get_config(client)["write_retries"]["transaction_id"]):
        if not created:
            create_object(path, client)
        params["path"] = path

        enable_progress_bar = get_config(client)["write_progress_bar"]["enable"]
        if progress_monitor is None:
            progress_monitor = _SimpleProgressBar(size_hint, filename_hint, enable_progress_bar)
        if get_config(client)["write_progress_bar"]["enable"] is not False:
            progress_reporter = _ProgressReporter(progress_monitor)
        else:
            progress_reporter = _FakeProgressReporter()

        with progress_reporter:
            if use_retries:
                write_action = lambda chunk, params: _make_transactional_request(
                    command_name,
                    params,
                    data=progress_reporter.wrap_stream(chunk),
                    is_data_compressed=is_stream_compressed,
                    use_heavy_proxy=True,
                    client=client)

                runner = WriteRequestRetrier(transaction_timeout=transaction_timeout,
                                             write_action=write_action,
                                             client=client)
                for chunk in group_blobs_by_size(stream, chunk_size):
                    assert isinstance(chunk, list)
                    logger.debug(
                        "Processing {0} chunk (length: {1}, transaction: {2})"
                        .format(command_name, len(chunk), get_command_param("transaction_id", client)))

                    runner.run_write_action(chunk, params)
                    params["path"].append = True
                    # NOTE: If previous chunk was successfully written then
                    # no need in additional attributes here, it is already
                    # set by first request.
                    for attr in ["schema", "optimize_for", "compression_codec", "erasure_codec"]:
                        if attr in params["path"].attributes:
                            del params["path"].attributes[attr]
            else:
                _make_transactional_request(
                    command_name,
                    params,
                    data=progress_reporter.wrap_stream(stream),
                    is_data_compressed=is_stream_compressed,
                    use_heavy_proxy=True,
                    client=client)

def _get_read_response(command_name, params, transaction_id, client=None):
    make_request = lambda: _make_transactional_request(
        command_name,
        params,
        return_content=False,
        use_heavy_proxy=True,
        allow_retries=False,
        client=client)

    response = None
    if transaction_id:
        with Transaction(transaction_id=transaction_id, client=client):
            response = make_request()
    else:
        response = make_request()
    return response

class ReadIterator(IteratorRetrier):
    def __init__(self, command_name, transaction, process_response_action, retriable_state_class, client=None):
        chaos_monkey_enabled = get_option("_ENABLE_READ_TABLE_CHAOS_MONKEY", client)
        retriable_errors = tuple(list(get_retriable_errors()) + [YtChunkUnavailable, YtFormatReadError])
        retry_config = get_config(client)["read_retries"]
        timeout = get_config(client)["proxy"]["heavy_request_timeout"]

        super(ReadIterator, self).__init__(retry_config, timeout, retriable_errors,
                                           default_chaos_monkey(chaos_monkey_enabled))
        self.client = client
        self.command_name = command_name
        self.transaction = transaction
        self.process_response_action = process_response_action
        self.retriable_state = retriable_state_class()
        self.response = None
        self.start_response = None
        self.last_response = None
        self.iterator = None
        self.change_proxy_period = get_config(client)["read_retries"]["change_proxy_period"]

    def _iterator(self):
        if self.start_response is None:
            self.start_response = self.get_response()
            self.process_response_action(self.start_response)
        while True:
            start_read_time = time.time()
            for elem in self.retriable_state.iterate(self.get_response()):
                if not self.transaction.is_pinger_alive():
                    raise YtError("Transaction pinger failed, read interrupted")
                yield elem

                if self.change_proxy_period:
                    if time.time() - start_read_time > self.change_proxy_period / 1000.0:
                        self.response = None
                        break
            else:
                break

    def get_response(self):
        if self.response is None:
            params = self.retriable_state.prepare_params_for_retry()
            transaction_id = self.transaction.transaction_id if self.transaction else None
            self.response = _get_read_response(self.command_name, params, transaction_id, self.client)

        self.last_response = self.response
        return self.response

    def next(self):
        return self.__next__()

    def __next__(self):
        if self.iterator is None:
            self.iterator = self.run()
        return next(self.iterator)

    def __iter__(self):
        return self

    def close(self):
        if self.last_response is not None:
            self.last_response.close()
        self.transaction.abort()
        self.iterator.close()

    def except_action(self, exception, attempt):
        self.response = None
        process_read_exception(exception)

def make_read_request(command_name, path, params, process_response_action, retriable_state_class, client):
    if not get_config(client)["read_retries"]["enable"]:
        response = _make_transactional_request(
            command_name,
            params,
            return_content=False,
            use_heavy_proxy=True,
            client=client)
        process_response_action(response)
        return response
    else:
        title = "Python wrapper: {0} {1}".format(command_name, path)

        if get_config(client)["read_retries"]["create_transaction_and_take_snapshot_lock"]:
            title = "Python wrapper: read {0}".format(str(YPathSupportingAppend(path, client=client)))
            tx = Transaction(attributes={"title": title}, interrupt_on_failed=False, client=client)
        else:
            tx = FakeTransaction()

        try:
            if tx:
                with Transaction(transaction_id=tx.transaction_id, attributes={"title": title}, client=client):
                    lock(path, mode="snapshot", client=client)
            iterator = ReadIterator(command_name, tx, process_response_action, retriable_state_class, client)
            return ResponseStreamWithReadRow(
                get_response=lambda: iterator.last_response,
                iter_content=iterator,
                close=lambda: iterator.close(),
                process_error=lambda response: iterator.last_response._process_error(
                    iterator.last_response._get_response()),
                get_response_parameters=lambda: iterator.start_response.response_parameters)
        except:
            tx.abort()
            raise
