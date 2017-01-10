from .config import get_option, get_config, get_total_request_timeout, get_request_retry_count
from .common import get_backoff, chunk_iter_blobs, YtError
from .errors import YtResponseError, YtRetriableError
from .ypath import YPathSupportingAppend
from .transaction import Transaction
from .transaction_commands import _make_transactional_request
from .http_helpers import get_retriable_errors
from .response_stream import ResponseStream
from .lock_commands import lock
from .format import YtFormatReadError

import yt.logger as logger

from yt.packages.six import Iterator as IteratorBase
from yt.packages.six.moves import xrange

import time
import random

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

def make_write_request(command_name, stream, path, params, create_object, use_retries,
                       is_stream_compressed=False, client=None):
    path = YPathSupportingAppend(path, client=client)
    request_timeout = get_total_request_timeout(client)

    created = False
    if get_config(client)["yamr_mode"]["create_tables_outside_of_transaction"]:
        create_object(path)
        created = True

    title = "Python wrapper: {0} {1}".format(command_name, path)
    with Transaction(timeout=request_timeout,
                     attributes={"title": title},
                     client=client,
                     transaction_id=get_config(client)["write_retries"]["transaction_id"]):
        if not created:
            create_object(path)
        if use_retries:
            chunk_size = get_config(client)["write_retries"]["chunk_size"]

            started = False
            for chunk in chunk_iter_blobs(stream, chunk_size):
                assert isinstance(chunk, list)

                if started:
                    path.append = True
                    # NOTE: If previous chunk was successfully written then
                    # no need in schema attribute here, it is already set and
                    # new data will be validated according to it.
                    if "schema" in path.attributes:
                        del path.attributes["schema"]

                started = True

                logger.debug("Processing {0} chunk (length: {1}, transaction: {2})"
                    .format(command_name, len(chunk), get_option("TRANSACTION", client)))

                for attempt in xrange(get_request_retry_count(client)):
                    try:
                        if get_option("_ENABLE_HEAVY_REQUEST_CHAOS_MONKEY", client) and random.randint(1, 5) == 1:
                            raise YtRetriableError()
                        with Transaction(timeout=request_timeout, client=client):
                            params["path"] = path

                            _make_transactional_request(
                                command_name,
                                params,
                                data=iter(chunk),
                                is_data_compressed=is_stream_compressed,
                                use_heavy_proxy=True,
                                client=client)
                        break
                    except get_retriable_errors() as err:
                        if attempt + 1 == get_request_retry_count(client):
                            raise
                        logger.warning("%s: %s", type(err), str(err))
                        backoff = get_backoff(
                            request_start_time=None,
                            request_timeout=get_config(client)["proxy"]["heavy_request_retry_timeout"],
                            is_request_heavy=True,
                            attempt=attempt,
                            backoff_config=get_config(client)["retry_backoff"])
                        if backoff:
                            logger.warning("Sleep for %.2lf seconds before next retry", backoff)
                            time.sleep(backoff)
                        logger.warning("New retry (%d) ...", attempt + 2)
        else:
            params["path"] = path
            _make_transactional_request(
                command_name,
                params,
                data=stream,
                is_data_compressed=is_stream_compressed,
                use_heavy_proxy=True,
                client=client)

def make_read_request(command_name, path, params, process_response_action, retriable_state_class, client):
    retriable_errors = tuple(list(get_retriable_errors()) + [YtResponseError, YtFormatReadError])

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
        retry_count = get_config(client)["read_retries"]["retry_count"]
        title = "Python wrapper: {0} {1}".format(command_name, path)

        if get_config(client)["read_retries"]["create_transaction_and_take_snapshot_lock"]:
            title = "Python wrapper: read {0}".format(str(YPathSupportingAppend(path, client=client)))
            tx = Transaction(attributes={"title": title}, interrupt_on_failed=False, client=client)
        else:
            tx = FakeTransaction()

        def iter_with_retries(iter):
            chaos_monkey_enabled = get_option("_ENABLE_READ_TABLE_CHAOS_MONKEY", client)
            try:
                attempt = 0
                while True:
                    try:
                        for elem in iter():
                            if not tx.is_pinger_alive():
                                raise YtError("Transaction pinger failed, read interrupted")
                            yield elem
                            # NB: We should possible raise error only after row yielded.
                            if chaos_monkey_enabled and random.randint(1, 5) == 1:
                                raise YtRetriableError()
                            # Reset attempt counter since we successfully read row.
                            attempt = 0
                        break
                    except retriable_errors as err:
                        attempt += 1
                        if isinstance(err, YtResponseError) and not err.is_chunk_unavailable():
                            raise
                        if attempt == retry_count:
                            raise
                        logger.warning(str(err))
                        backoff = get_backoff(
                            request_start_time=None,
                            request_timeout=get_config(client)["proxy"]["heavy_request_retry_timeout"],
                            is_request_heavy=True,
                            attempt=attempt,
                            backoff_config=get_config(client)["retry_backoff"])
                        logger.warning("Sleep for %.2lf seconds before next retry", backoff)
                        time.sleep(backoff)
                        logger.warning("New retry (%d) ...", attempt + 1)
            except GeneratorExit:
                pass
            finally:
                tx.abort()

        class Iterator(IteratorBase):
            def __init__(self):
                self.retriable_state = retriable_state_class()
                self.response = None
                self.iterator = iter_with_retries(self.execute_read)

                self.start_response = self.get_response()
                process_response_action(self.start_response)

            def get_response(self):
                if self.response is None:
                    params = self.retriable_state.prepare_params_for_retry()
                    make_request = lambda: _make_transactional_request(
                        command_name,
                        params,
                        return_content=False,
                        use_heavy_proxy=True,
                        allow_retries=True,
                        client=client)

                    if tx:
                        with Transaction(transaction_id=tx.transaction_id, client=client):
                            self.response = make_request()
                    else:
                        self.response = make_request()

                self.last_response = self.response
                return self.response

            def execute_read(self):
                try:
                    for elem in self.retriable_state.iterate(self.get_response()):
                        yield elem
                finally:
                    self.response = None

            def __next__(self):
                return next(self.iterator)

            def __iter__(self):
                return self

            def close(self):
                if self.last_response is not None:
                    self.last_response.close()
                tx.abort()
                self.iterator.close()

        try:
            if tx:
                with Transaction(transaction_id=tx.transaction_id, attributes={"title": title}, client=client):
                    lock(path, mode="snapshot", client=client)
            iterator = Iterator()
            return ResponseStream(
                get_response=lambda: iterator.last_response,
                iter_content=iterator,
                close=lambda: iterator.close(),
                process_error=lambda response: iterator.last_response._process_error(iterator.last_response._get_response()),
                get_response_parameters=lambda: iterator.start_response.response_parameters)
        except:
            tx.abort()
            raise
