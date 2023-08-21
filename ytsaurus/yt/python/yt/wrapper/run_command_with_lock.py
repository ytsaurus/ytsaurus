from .errors import YtResponseError
from .cypress_commands import create, set
from .transaction import Transaction
from .lock_commands import lock

from yt.common import YT_NULL_TRANSACTION_ID, get_fqdn

import yt.logger as logger

import signal
import subprocess
import time


class SuccessfullyFinished(BaseException):
    pass


def run_command_with_lock(
        path, command, popen_kwargs=None,
        lock_conflict_callback=None, ping_failed_callback=None,
        set_address=True, address_path=None,
        create_lock_options=None, poll_period=None,
        forward_signals=None, client=None):
    """
    Run given command under lock.
    """

    if popen_kwargs is None:
        popen_kwargs = {}
    if poll_period is None:
        poll_period = 1.0

    create("map_node", path, ignore_existing=True, client=client, **create_lock_options)

    try:
        with Transaction(attributes={"title": "run_command_with_lock transaction"}, client=client) as tx:
            create("map_node", path, ignore_existing=True, client=client)
            try:
                lock(path, client=client)
            except YtResponseError as error:
                if error.is_concurrent_transaction_lock_conflict():
                    logger.info("Failed to take lock at %s", path)
                    lock_conflict_callback()
                raise

            if set_address:
                if address_path is not None:
                    with Transaction(transaction_id=YT_NULL_TRANSACTION_ID, client=client):
                        set(address_path, get_fqdn(), client=client)
                else:
                    set(path + "/@address", get_fqdn(), client=client)

            logger.info("Running command %s", command)
            if "env" not in popen_kwargs:
                popen_kwargs["env"] = {}
            popen_kwargs["env"]["YT_LOCK_TRANSACTION_ID"] = tx.transaction_id

            proc = subprocess.Popen(command, **popen_kwargs)

            if forward_signals is not None:
                for signal_num in forward_signals:
                    signal.signal(signal_num, lambda signum, frame: proc.send_signal(signum))

            while True:
                if not tx._ping_thread.is_alive():
                    logger.info("Transaction is not alive")
                    if ping_failed_callback is not None:
                        ping_failed_callback(proc)
                    break

                if proc.poll() is not None:
                    break

                time.sleep(poll_period)

            raise SuccessfullyFinished()
    except SuccessfullyFinished:
        pass

    return proc.returncode
