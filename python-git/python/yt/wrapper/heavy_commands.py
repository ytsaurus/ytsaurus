"""heavy command"""

import config
import yt.logger as logger
from common import get_backoff
from errors import format_error
from table import to_table
from transaction import PingableTransaction
from transaction_commands import _make_transactional_request
from http import RETRIABLE_ERRORS

import time
from datetime import datetime

def make_heavy_request(command_name, stream, path, params, create_object, use_retries, client=None):
    path = to_table(path, client=client)

    title = "Python wrapper: {0} {1}".format(command_name, path.name)
    with PingableTransaction(timeout=config.http.get_timeout(),
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
                    .format(command_name, len(chunk), config.TRANSACTION))

                for attempt in xrange(config.http.REQUEST_RETRY_COUNT):
                    current_time = datetime.now()
                    try:
                        with PingableTransaction(timeout=config.http.get_timeout(), client=client):
                            params["path"] = path.get_json()
                            _make_transactional_request(
                                command_name,
                                params,
                                data=chunk,
                                use_heavy_proxy=True,
                                retry_unavailable_proxy=False,
                                client=client)
                        break
                    except RETRIABLE_ERRORS as err:
                        if attempt + 1 == config.http.REQUEST_RETRY_COUNT:
                            raise
                        backoff = get_backoff(config.http.REQUEST_RETRY_TIMEOUT, current_time)
                        if backoff:
                            logger.warning("%s. Sleep for %.2lf seconds...", format_error(err), backoff)
                            time.sleep(backoff)
                        logger.warning("New retry (%d) ...", attempt + 2)
        else:
            params["path"] = path.get_json()
            _make_transactional_request(
                command_name,
                params,
                data=stream,
                use_heavy_proxy=True,
                client=client)
