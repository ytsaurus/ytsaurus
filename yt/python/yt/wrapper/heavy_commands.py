from .config import get_option, get_config, get_command_param
from .common import YtError, MB, typing  # noqa
from .cypress_commands import get
from .default_config import DEFAULT_WRITE_CHUNK_SIZE
from .driver import make_request
from .retries import Retrier, IteratorRetrier, default_chaos_monkey
from .errors import YtMasterCommunicationError, YtChunkUnavailable, YtAllTargetNodesFailed
from .ypath import YPathSupportingAppend, YPath
from .progress_bar import SimpleProgressBar, FakeProgressReporter
from .stream import RawStream, ItemStream
from .transaction import Transaction, add_transaction_to_abort
from .http_helpers import get_retriable_errors
from .response_stream import ResponseStreamWithReadRow
from .lock_commands import lock
from .format import YtFormatReadError

import yt.logger as logger

from yt.common import join_exceptions, YT_NULL_TRANSACTION_ID as null_transaction_id

try:
    from yt.packages.six.moves import xrange
except ImportError:
    from six.moves import xrange

import sys
import time


class _ProgressReporter(object):
    def __init__(self, monitor):
        self._monitor = monitor

    def __enter__(self):
        self._monitor.start()

    def __exit__(self, type, value, traceback):
        self._monitor.finish()

    def __del__(self):
        self._monitor.finish()


class _IteratorProgressReporter(_ProgressReporter):
    def wrap_stream(self, stream):
        for chunk in stream:
            self._monitor.update(len(chunk))
            yield chunk


class _FileProgressReporter(_ProgressReporter):
    def wrap_file(self, target):
        self._file = target
        return self

    def read(self, length=None):
        if length is None:
            length = 2 ** 63
        result = []
        step = 2 * MB
        for start in xrange(0, length, step):
            result.append(self._file.read(min((step, length - start))))
            if not result[-1]:
                self._monitor.finish()
                break
            self._monitor.update(len(result[-1]))
        return b"".join(result)


class _FakeFileProgressReporter:
    def wrap_file(self, target):
        return target

    def wrap_stream(self, stream):
        return stream

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass


def process_read_exception(exception):
    logger.warning("Read request failed with error: %s", str(exception))


class FakeTransaction(object):
    def __init__(self):
        self.transaction_id = null_transaction_id

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
        super(WriteRequestRetrier, self).__init__(
            retry_config=retry_config,
            timeout=request_timeout,
            exceptions=get_retriable_errors() + (YtMasterCommunicationError, YtAllTargetNodesFailed,),
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
        logger.warning("Write failed with error %s", repr(error))

    def run_write_action(self, chunk, params):
        self.chunk = chunk
        self.params = params
        self.run()

        self.chunk = None
        self.params = None


def make_write_request(command_name, stream, path, params, create_object, use_retries,
                       is_stream_compressed=False, size_hint=None, filename_hint=None,
                       progress_monitor=None, client=None):
    # type: (str, RawStream | ItemStream, str | YPath, dict, typing.Callable[[YPath, YtClient], None], bool, bool | None, int | None, str | None, _ProgressReporter | None, YtClient | None) -> None
    assert isinstance(stream, (RawStream, ItemStream))

    path = YPathSupportingAppend(path, client=client)
    transaction_timeout = max(
        get_config(client)["proxy"]["heavy_request_timeout"],
        get_config(client)["transaction_timeout"])

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

        if stream.isatty():
            enable_progress_bar = False
        else:
            enable_progress_bar = get_config(client)["write_progress_bar"]["enable"] and not stream.isatty()

        if progress_monitor is None:
            progress_monitor = SimpleProgressBar("upload", size_hint, filename_hint, enable_progress_bar)
        if get_config(client)["write_progress_bar"]["enable"] is not False:
            progress_reporter = _IteratorProgressReporter(progress_monitor)
        else:
            progress_reporter = FakeProgressReporter()

        with progress_reporter:
            if use_retries:
                chunk_size = get_config(client)["write_retries"]["chunk_size"]
                if chunk_size is None:
                    chunk_size = DEFAULT_WRITE_CHUNK_SIZE

                write_action = lambda chunk, params: make_request(  # noqa
                    command_name,
                    params,
                    data=progress_reporter.wrap_stream(chunk),
                    is_data_compressed=is_stream_compressed,
                    use_heavy_proxy=True,
                    client=client)

                runner = WriteRequestRetrier(transaction_timeout=transaction_timeout,
                                             write_action=write_action,
                                             client=client)

                # NB: split stream into 2 MB pieces so that we can safely send them separately
                # without a risk of triggering timeout.
                for chunk in stream.into_chunks(chunk_size).split_chunks(2 * MB):
                    assert isinstance(chunk, list)
                    logger.debug(
                        "Processing {0} chunk (length: {1}, transaction: {2})"
                        .format(command_name, sum(len(part) for part in chunk), get_command_param("transaction_id", client)))

                    runner.run_write_action(chunk, params)

                    params["path"].append = True
                    # NOTE: If previous chunk was successfully written then
                    # no need in additional attributes here, it is already
                    # set by first request.
                    for attr in ["schema", "optimize_for", "compression_codec", "erasure_codec"]:
                        if attr in params["path"].attributes:
                            del params["path"].attributes[attr]
            else:
                make_request(
                    command_name,
                    params,
                    data=progress_reporter.wrap_stream(stream.into_chunks(2 * MB)),
                    is_data_compressed=is_stream_compressed,
                    use_heavy_proxy=True,
                    client=client)


def _get_read_response(command_name, params, transaction_id, client=None):
    make_read_request = lambda: make_request(  # noqa
        command_name,
        params,
        return_content=False,
        use_heavy_proxy=True,
        allow_retries=False,
        client=client)

    response = None
    if transaction_id:
        with Transaction(transaction_id=transaction_id, client=client):
            response = make_read_request()
    else:
        response = make_read_request()
    return response


def _abort_transaction_on_close(transaction, from_delete):
    if from_delete:
        add_transaction_to_abort(transaction)
    else:
        transaction.abort()


class ReadIterator(IteratorRetrier):
    def __init__(self, command_name, transaction, process_response_action, retriable_state, client=None):
        chaos_monkey_enabled = get_option("_ENABLE_READ_TABLE_CHAOS_MONKEY", client)
        retriable_errors = join_exceptions(get_retriable_errors(), YtChunkUnavailable, YtFormatReadError)
        retry_config = get_config(client)["read_retries"]
        timeout = get_config(client)["proxy"]["heavy_request_timeout"]

        super(ReadIterator, self).__init__(retry_config, timeout, retriable_errors,
                                           default_chaos_monkey(chaos_monkey_enabled))
        self.client = client
        self.command_name = command_name
        self.transaction = transaction
        self.process_response_action = process_response_action
        self.retriable_state = retriable_state
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

    def close(self, from_delete=False):
        if self.last_response is not None:
            self.last_response.close()

        _abort_transaction_on_close(self.transaction, from_delete=from_delete)
        self.transaction = None

        self.iterator.close()

    def except_action(self, exception, attempt):
        super(ReadIterator, self).except_action(exception, attempt)
        self.response = None
        process_read_exception(exception)


def _try_get_size(path, client, request_size):
    if request_size:
        try:
            return int(get(str(path) + "/@uncompressed_data_size", client=client))
        except (ValueError, YtError):
            pass
    return None


def _get_read_progress_reporter(size_hint, filename_hint, client, filelike=False):
    if sys.stderr.isatty():
        enable_progress_bar = get_config(client)["read_progress_bar"]["enable"]
    else:
        enable_progress_bar = False

    if enable_progress_bar:
        bar = SimpleProgressBar("download", size_hint=size_hint,
                                filename_hint=filename_hint, enable=enable_progress_bar)
        return _FileProgressReporter(bar) if filelike else _IteratorProgressReporter(bar)
    else:
        return _FakeFileProgressReporter()


def make_read_request(command_name, path, params, process_response_action, retriable_state_class, client,
                      filename_hint=None, request_size=False):
    assert isinstance(path, YPath)

    if get_config(client)["read_retries"]["create_transaction_and_take_snapshot_lock"]:
        title = "Python wrapper: read {0}".format(str(path))
        tx = Transaction(attributes={"title": title}, interrupt_on_failed=False, client=client)
    else:
        tx = FakeTransaction()

    try:
        if tx:
            with Transaction(transaction_id=tx.transaction_id, attributes={"title": title}, client=client):
                lock_result = lock(path, mode="snapshot", client=client)
                if get_config(client)["read_retries"]["use_locked_node_id"]:
                    if get_config(client)["api_version"] == "v4":
                        node_id = lock_result["node_id"]
                    else:
                        node_id = get(path + "/@id", client=client)
                    params["path"] = YPath("#" + node_id, attributes=path.attributes)
                size_hint = _try_get_size(path, client, request_size)
        else:
            size_hint = _try_get_size(path, client, request_size)

        if not get_config(client)["read_retries"]["enable"] or retriable_state_class is None:
            response = _get_read_response(
                command_name,
                params,
                transaction_id=tx.transaction_id,
                client=client)

            response.add_close_action(lambda from_delete: _abort_transaction_on_close(tx, from_delete))

            process_response_action(response)

            reporter = _get_read_progress_reporter(size_hint, filename_hint, client, filelike=True)
            # NB: __exit__() is done in __del__()
            reporter.__enter__()
            return reporter.wrap_file(response)
        else:
            retriable_state = retriable_state_class(params, client, process_response_action)
            iterator = ReadIterator(command_name, tx, process_response_action, retriable_state, client)
            reporter = _get_read_progress_reporter(size_hint, filename_hint, client, filelike=False)

            # NB: __exit__() is done in __del__()
            reporter.__enter__()
            return ResponseStreamWithReadRow(
                get_response=lambda: iterator.last_response,
                iter_content=reporter.wrap_stream(iterator),
                close=lambda from_delete: iterator.close(from_delete),
                process_error=lambda response: iterator.last_response._process_error(
                    iterator.last_response._get_response()),
                get_response_parameters=lambda: iterator.start_response.response_parameters)
    except:  # noqa
        tx.abort()
        raise
