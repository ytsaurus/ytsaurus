import config
import yt.logger as logger
from common import YtError
from table import to_table
from transaction import PingableTransaction
from transaction_commands import _make_transactional_request
from driver import get_host_for_heavy_operation
from http import NETWORK_ERRORS

def make_heavy_request(command_name, stream, path, params, create_object, use_retries):
    path = to_table(path)

    title = "Python wrapper: {0} {1}".format(command_name, path.name)
    with PingableTransaction(timeout=config.HEAVY_COMMAND_TRANSACTION_TIMEOUT,
                             attributes={"title": title}):
        create_object(path.name)
        if use_retries:
            started = False
            for chunk in stream:
                if started:
                    path.append = True
                started = True

                logger.debug("Processing {0} chunk (length: {1}, transaction: {2})"
                    .format(command_name, len(chunk), config.TRANSACTION))
                
                for i in xrange(config.HEAVY_COMMAND_RETRIES_COUNT):
                    try: 
                        with PingableTransaction(timeout=config.HEAVY_COMMAND_TRANSACTION_TIMEOUT):
                            params["path"] = path.get_json()
                            _make_transactional_request(
                                command_name,
                                params,
                                data=chunk,
                                proxy=get_host_for_heavy_operation(),
                                retry_unavailable_proxy=False)
                        break
                    except tuple(list(NETWORK_ERRORS) + [YtError]) as err:
                        logger.warning("Retry %d failed with message %s", i + 1, str(err))
                        if i + 1 == config.HEAVY_COMMAND_RETRIES_COUNT:
                            raise
        else:
            params["path"] = path.get_json()
            _make_transactional_request(
                command_name,
                params,
                data=stream,
                proxy=get_host_for_heavy_operation())
