"""heavy command"""

import yt.logger as logger
import config
from config import get_option, get_config, get_total_request_timeout, get_single_request_timeout, get_request_retry_count
from common import get_backoff
from table import to_table, to_name
from transaction import Transaction, EmptyTransaction, Abort
from transaction_commands import _make_transactional_request
from http import RETRIABLE_ERRORS, HTTPError
from response_stream import ResponseStream
from lock import lock

import sys
import time
import random
import exceptions
from datetime import datetime

def make_write_request(command_name, stream, path, params, create_object, use_retries, client=None):
    path = to_table(path, client=client)
    request_timeout = get_total_request_timeout(client)

    title = "Python wrapper: {0} {1}".format(command_name, path.name)
    with Transaction(timeout=request_timeout,
                     attributes={"title": title},
                     client=client):
        create_object(path.name)
        if use_retries:
            started = False
            for chunk in stream:
                if started:
                    path.append = True
                started = True

                logger.debug("Processing {0} chunk (length: {1}, transaction: {2})"
                    .format(command_name, len(chunk), get_option("TRANSACTION", client)))

                for attempt in xrange(get_request_retry_count(client)):
                    current_time = datetime.now()
                    try:
                        if get_option("_ENABLE_HEAVY_REQUEST_CHAOS_MONKEY", client) and random.randint(1, 5) == 1:
                            raise HTTPError()
                        with Transaction(timeout=request_timeout, client=client):
                            params["path"] = path.to_yson_type()
                            if isinstance(chunk, list):
                                data = iter(chunk)
                            else:
                                data = chunk

                            _make_transactional_request(
                                command_name,
                                params,
                                data=data,
                                use_heavy_proxy=True,
                                retry_unavailable_proxy=False,
                                client=client)
                        break
                    except RETRIABLE_ERRORS as err:
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
    def execute_with_retries(func):
        for attempt in xrange(config.get_request_retry_count(client)):
            try:
                return func()
            except RETRIABLE_ERRORS as err:
                if attempt + 1 == config.get_request_retry_count(client):
                    raise
                logger.warning(str(err))
                logger.warning("New retry (%d) ...", attempt + 2)


    if not get_config(client)["read_retries"]["enable"]:
        def simple_read():
            response = _make_transactional_request(
                command_name,
                params,
                return_content=False,
                use_heavy_proxy=True,
                client=client)
            process_response_action(response)
            return response
        return execute_with_retries(simple_read)
    else:
        retry_count = get_config(client)["read_retries"]["retry_count"]

        if get_config(client)["read_retries"]["create_transaction_and_take_snapshot_lock"]:
            title = "Python wrapper: read {0}".format(to_name(path, client=client))
            tx = Transaction(attributes={"title": title}, client=client)
            tx.__enter__()
        else:
            tx = EmptyTransaction()

        def iter_with_retries(iter):
            try:
                for attempt in xrange(retry_count):
                    try:
                        for elem in iter():
                            if get_option("_ENABLE_READ_TABLE_CHAOS_MONKEY", client) and random.randint(1, 5) == 1:
                                raise HTTPError()
                            yield elem
                        break
                    except RETRIABLE_ERRORS as err:
                        if attempt + 1 == retry_count:
                            raise
                        logger.warning(str(err))
                        logger.warning("New retry (%d) ...", attempt + 2)
            except exceptions.GeneratorExit:
                tx.__exit__(None, None, None)
            except:
                tx.__exit__(*sys.exc_info())
                raise
            else:
                tx.__exit__(None, None, None)

        class Iterator(object):
            def __init__(self):
                self.retriable_state = retriable_state_class()
                self.response = None
                self.iterator = iter_with_retries(self.execute_read)

            def execute_read(self):
                params = self.retriable_state.prepare_params_for_retry()
                self.response = _make_transactional_request(
                    command_name,
                    params,
                    return_content=False,
                    use_heavy_proxy=True,
                    client=client)
                for elem in self.retriable_state.iterate(self.response):
                    yield elem

            def next(self):
                return self.iterator.next()

            def __iter__(self):
                return self

            def close(self):
                if self.response is not None:
                    self.response.close()
                tx.__exit__(Abort, None, None)

        try:
            if tx:
                lock(path, mode="snapshot", client=client)
            iterator = Iterator()
            return ResponseStream(
                lambda: iterator.response,
                iterator,
                close=iterator.close,
                process_error=lambda request: iterator.response._process_error(request),
                get_response_parameters=lambda: None)
        except:
            tx.__exit__(*sys.exc_info())
            raise
