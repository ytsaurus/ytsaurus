from .config import get_option, get_config, get_total_request_timeout, get_command_param
from .common import (group_blobs_by_size, YtError, update, remove_nones_from_dict,
                     get_value)
from .retries import Retrier, IteratorRetrier
from .errors import YtResponseError, YtMasterCommunicationError
from .ypath import YPathSupportingAppend
from .transaction import Transaction
from .transaction_commands import _make_transactional_request
from .http_helpers import get_retriable_errors
from .response_stream import ResponseStreamWithReadRow
from .lock_commands import lock
from .format import YtFormatReadError

import yt.logger as logger

import time
from copy import deepcopy

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
        retry_config = {
            "backoff": get_config(client)["retry_backoff"],
            "count": get_config(client)["proxy"]["request_retry_count"],
        }
        retry_config = update(deepcopy(get_config(client)["write_retries"]), remove_nones_from_dict(retry_config))
        request_timeout = get_value(get_config(client)["proxy"]["heavy_request_retry_timeout"],
                                    get_config(client)["proxy"]["heavy_request_timeout"])
        chaos_monkey_enable = get_option("_ENABLE_HEAVY_REQUEST_CHAOS_MONKEY", client)
        super(WriteRequestRetrier, self).__init__(retry_config=retry_config,
                                                  timeout=request_timeout,
                                                  exceptions=get_retriable_errors() + (YtMasterCommunicationError,),
                                                  chaos_monkey_enable=chaos_monkey_enable)
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
                       is_stream_compressed=False, client=None):
    path = YPathSupportingAppend(path, client=client)
    transaction_timeout = get_total_request_timeout(client)

    created = False
    if get_config(client)["yamr_mode"]["create_tables_outside_of_transaction"]:
        create_object(path)
        created = True

    title = "Python wrapper: {0} {1}".format(command_name, path)

    with Transaction(timeout=transaction_timeout,
                     attributes={"title": title},
                     client=client,
                     transaction_id=get_config(client)["write_retries"]["transaction_id"]):
        if not created:
            create_object(path)
        params["path"] = path
        if use_retries:
            chunk_size = get_config(client)["write_retries"]["chunk_size"]

            write_action = lambda chunk, params: _make_transactional_request(
                command_name,
                params,
                data=iter(chunk),
                is_data_compressed=is_stream_compressed,
                use_heavy_proxy=True,
                client=client)

            runner = WriteRequestRetrier(transaction_timeout=transaction_timeout,
                                         write_action=write_action,
                                         client=client)
            for chunk in group_blobs_by_size(stream, chunk_size):
                assert isinstance(chunk, list)
                logger.debug("Processing {0} chunk (length: {1}, transaction: {2})"
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
                data=stream,
                is_data_compressed=is_stream_compressed,
                use_heavy_proxy=True,
                client=client)


class ReadIterator(IteratorRetrier):
    def __init__(self, command_name, transaction, process_response_action, retriable_state_class, client=None):
        chaos_monkey_enabled = get_option("_ENABLE_READ_TABLE_CHAOS_MONKEY", client)
        retriable_errors = tuple(list(get_retriable_errors()) + [YtResponseError, YtFormatReadError])
        retry_config = {
            "count": get_config(client)["read_retries"]["retry_count"],
            "backoff": get_config(client)["retry_backoff"],
        }
        retry_config = update(deepcopy(get_config(client)["read_retries"]), remove_nones_from_dict(retry_config))
        timeout = get_value(get_config(client)["proxy"]["heavy_request_retry_timeout"],
                            get_config(client)["proxy"]["heavy_request_timeout"])

        super(ReadIterator, self).__init__(self.read_iterator, retry_config, timeout, retriable_errors,
                                           chaos_monkey_enabled)
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

    def read_iterator(self):
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
            make_request = lambda: _make_transactional_request(
                self.command_name,
                params,
                return_content=False,
                use_heavy_proxy=True,
                allow_retries=False,
                client=self.client)

            if self.transaction:
                with Transaction(transaction_id=self.transaction.transaction_id, client=self.client):
                    self.response = make_request()
            else:
                self.response = make_request()

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
        if isinstance(exception, YtResponseError) and not exception.is_chunk_unavailable():
            raise
        else:
            logger.warning("Read request failed with error: %s", str(exception))

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
                process_error=lambda response: iterator.last_response._process_error(iterator.last_response._get_response()),
                get_response_parameters=lambda: iterator.start_response.response_parameters)
        except:
            tx.abort()
            raise
