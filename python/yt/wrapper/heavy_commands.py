"""heavy command"""

import yt.logger as logger
from config import get_option, get_total_request_timeout, get_single_request_timeout, get_request_retry_count
from common import get_backoff
from table import to_table
from errors import YtIncorrectResponse
from transaction import Transaction
from transaction_commands import _make_transactional_request
from http import RETRIABLE_ERRORS

import time
import random
from datetime import datetime

def make_heavy_request(command_name, stream, path, params, create_object, use_retries, client=None):
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

                if isinstance(chunk, list):
                    chunk = iter(chunk)

                for attempt in xrange(get_request_retry_count(client)):
                    current_time = datetime.now()
                    try:
                        if get_option("_ENABLE_HEAVY_REQUEST_CHAOS_MONKEY", client) and random.randint(1, 5) == 1:
                            raise YtIncorrectResponse()
                        with Transaction(timeout=request_timeout, client=client):
                            params["path"] = path.to_yson_type()
                            _make_transactional_request(
                                command_name,
                                params,
                                data=chunk,
                                use_heavy_proxy=True,
                                retry_unavailable_proxy=False,
                                client=client)
                        break
                    except RETRIABLE_ERRORS as err:
                        if attempt + 1 == get_request_retry_count(client):
                            raise
                        backoff = get_backoff(get_single_request_timeout(client), current_time)
                        if backoff:
                            logger.warning("%s. Sleep for %.2lf seconds...", str(err), backoff)
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
