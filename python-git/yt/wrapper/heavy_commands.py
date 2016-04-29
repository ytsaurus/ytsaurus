"""heavy command"""

import yt.logger as logger
import config
from config import get_option, get_config, get_total_request_timeout, get_single_request_timeout, get_request_retry_count
from common import get_backoff, chunk_iter_blobs, YtError
from errors import YtResponseError, YtRetriableError
from table import to_table, to_name
from transaction import Transaction
from transaction_commands import _make_transactional_request
from http import get_retriable_errors
from response_stream import ResponseStream
from lock import lock

import time
import random
import exceptions
from datetime import datetime

class FakeTransaction(object):
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    def __nonzero__(self):
        return False

    def abort(self):
        pass

    def commit(self):
        pass

    def is_pinger_alive(self):
        return True

def make_write_request(command_name, stream, path, params, create_object, use_retries, client=None):
    """
    param stream: list or iterator over string blobs.
    """
    path = to_table(path, client=client)
    request_timeout = get_total_request_timeout(client)

    title = "Python wrapper: {0} {1}".format(command_name, path.name)
    with Transaction(timeout=request_timeout,
                     attributes={"title": title},
                     client=client):
        create_object(path.name)
        if use_retries:
            chunk_size = get_config(client)["write_retries"]["chunk_size"]

            started = False
            for chunk in chunk_iter_blobs(stream, chunk_size):
                assert isinstance(chunk, list)

                if started:
                    path.append = True
                started = True

                logger.debug("Processing {0} chunk (length: {1}, transaction: {2})"
                    .format(command_name, len(chunk), get_option("TRANSACTION", client)))

                for attempt in xrange(get_request_retry_count(client)):
                    current_time = datetime.now()
                    try:
                        if get_option("_ENABLE_HEAVY_REQUEST_CHAOS_MONKEY", client) and random.randint(1, 5) == 1:
                            raise YtRetriableError()
                        with Transaction(timeout=request_timeout, client=client):
                            params["path"] = path.to_yson_type()

                            _make_transactional_request(
                                command_name,
                                params,
                                data=iter(chunk),
                                use_heavy_proxy=True,
                                retry_unavailable_proxy=False,
                                client=client)
                        break
                    except get_retriable_errors() as err:
                        if attempt + 1 == get_request_retry_count(client):
                            raise
                        logger.warning("%s: %s", type(err), str(err))
                        backoff = get_backoff(get_single_request_timeout(client), current_time)
                        if backoff:
                            logger.warning("Sleep for %.2lf seconds before next retry", backoff)
                            time.sleep(backoff)
                        logger.warning("New retry (%d) ...", attempt + 2)
        else:
            params["path"] = path.to_yson_type()
            _make_transactional_request(
                command_name,
                params,
                data=stream,
                use_heavy_proxy=True,
                client=client)

def make_read_request(command_name, path, params, process_response_action, retriable_state_class, client):
    retriable_errors = tuple(list(get_retriable_errors()) + [YtResponseError])

    def execute_with_retries(func):
        for attempt in xrange(config.get_request_retry_count(client)):
            try:
                return func()
            except get_retriable_errors() as err:
                if attempt + 1 == config.get_request_retry_count(client):
                    raise
                logger.warning(str(err))
                logger.warning("New retry (%d) ...", attempt + 2)


    if not get_config(client)["read_retries"]["enable"]:
        response = _make_transactional_request(
            command_name,
            params,
            return_content=False,
            use_heavy_proxy=True,
            allow_retries=True,
            client=client)
        process_response_action(response)
        return response
    else:
        retry_count = get_config(client)["read_retries"]["retry_count"]

        if get_config(client)["read_retries"]["create_transaction_and_take_snapshot_lock"]:
            title = "Python wrapper: read {0}".format(to_name(path, client=client))
            tx = Transaction(attributes={"title": title}, interrupt_on_failed=False, client=client)
        else:
            tx = FakeTransaction()

        def iter_with_retries(iter):
            try:
                for attempt in xrange(retry_count):
                    try:
                        for elem in iter():
                            if not tx.is_pinger_alive():
                                raise YtError("Transaction pinger failed, read interrupted")
                            yield elem
                            # NB: We should possible raise error only after row yielded.
                            if get_option("_ENABLE_READ_TABLE_CHAOS_MONKEY", client) and random.randint(1, 5) == 1:
                                raise YtRetriableError()
                        break
                    except retriable_errors as err:
                        if isinstance(err, YtResponseError) and not err.is_chunk_unavailable():
                            raise
                        if attempt + 1 == retry_count:
                            raise
                        logger.warning(str(err))
                        logger.warning("New retry (%d) ...", attempt + 2)
            except exceptions.GeneratorExit:
                pass
            finally:
                tx.abort()

        class Iterator(object):
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

            def next(self):
                return self.iterator.next()

            def __iter__(self):
                return self

            def close(self):
                if self.last_response is not None:
                    self.last_response.close()
                tx.abort()

        try:
            if tx:
                with Transaction(transaction_id=tx.transaction_id, client=client):
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
