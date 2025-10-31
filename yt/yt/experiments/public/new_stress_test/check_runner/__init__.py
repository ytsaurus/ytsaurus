import _thread
import datetime
import logging
import multiprocessing
import signal
import threading
import time

import yt.common as yt_common
import yt.wrapper as yt

from yt.wrapper.http_helpers import get_token
from lib.reporter import create_test_run_reporter

##################################################################

class ProcessTerminatedError(BaseException):
    """
    Used instead of ProcessTerminatedError because the latter is sometimes handled
    in the internals.
    """
    pass

##################################################################

FORMAT = "%(asctime)s\t%(levelname)s\t%(message)s"
logging.basicConfig(format=FORMAT)
logging.getLogger().setLevel(logging.INFO)

class TimeoutHandler():
    """
    Utility for interrupting the main thread after a timeout.
    """

    def __init__(self, timeout):
        self.timeout = timeout
        self.finished_event = threading.Event()
        self.timeout_thread = None

    def arm(self):
        def timeout_handler():
            start = time.time()
            while time.time() - start < self.timeout:
                if self.finished_event.is_set():
                    return
                time.sleep(0.1)
            logging.info("Timed out, interrupting main")
            _thread.interrupt_main()

        self.timeout_thread = threading.Thread(target=timeout_handler)
        self.timeout_thread.start()

    def disarm(self):
        self.finished_event.set()
        if self.timeout_thread is not None:
            logging.info("Joining timeout handler thread")
            self.timeout_thread.join()
            logging.info("Timeout handler thread joined")


def run_and_track_success(
    callback,
    base_path,
    timeout,
    transaction_title,
    failure_expiration_timeout=None,
    success_expiration_timeout=None,
    test_run_reporter_config=None,
):
    """
    Prepares environment for the Odin check, runs the callback and reports its status.

    Execution steps:
        - create a subdirectory under |base_path|
        - start a transaction with |transaction_title|
        - take a shared lock for the subdirectory
        - run |callback| with a subdirectory path as an argument
        - if the callback finishes successfully:
          - set "@success" attribute at the subdirectory to True
          - set "@expiration_time" attribute to now + |expiration_timeout| seconds
          - quit
        - if the callback throws an exception or does not terminate within |timeout|,
          terminate the program and set "@success" attribute to False.
        - if the callback throws an exception, the description of the exception
          is set to the "@error_message" attribute. Custom error message can be
          provided with the "error_message" exception member.
    """

    test_run_reporter = create_test_run_reporter(test_run_reporter_config)

    dir_name = str(int(datetime.datetime.now().timestamp()))
    path = base_path + "/" + dir_name

    timeout_handler = TimeoutHandler(timeout)

    success_attribute_set = False

    def _set_expiration_timeout(timeout):
        if timeout is None:
            return

        logging.info(f"Set expiration timeout to {timeout} seconds")
        expiration_time = datetime.datetime.utcnow() + datetime.timedelta(seconds=timeout)
        client.set(path + "/@expiration_time", yt_common.datetime_to_string(expiration_time))

    def _report_success():
        nonlocal success_attribute_set
        if success_attribute_set:
            return

        try:
            client.set(path + "/@success", True)
            _set_expiration_timeout(success_expiration_timeout)
            test_run_reporter.report_success()
        except Exception as e:
            # Parent exception has been already logged by the caller, do not log it twice.
            e.__context__ = None
            logging.exception("Caught exception while reporting success")

        success_attribute_set = True

    def _report_failure(error_message):
        nonlocal success_attribute_set
        if success_attribute_set:
            return

        try:
            client.set(path + "/@success", False)
            client.set(path + "/@error_message", error_message)
            _set_expiration_timeout(failure_expiration_timeout)
            test_run_reporter.report_failure(error_message)
        except Exception as e:
            # Parent exception has been already logged by the caller, do not log it twice.
            e.__context__ = None
            logging.exception("Caught exception while reporting failure")

        success_attribute_set = True

    main_pid = multiprocessing.current_process().pid
    def _sigterm_handler(*args):
        if multiprocessing.current_process().pid != main_pid:
            return
        with client.Transaction(transaction_id="0-0-0-0"):
            logging.info("Caught SIGTERM, interrupting")
            _report_failure("Interrupted")
            timeout_handler.disarm()
            raise ProcessTerminatedError

    signal.signal(signal.SIGTERM, _sigterm_handler)

    client = yt.YtClient(proxy=yt.http_helpers.get_proxy_url(), token=get_token())
    client.config["ping_failed_mode"] = "interrupt_main"


    try:
        timeout_handler.arm()

        logging.info(f"Initializing instance {path}")

        client.create("map_node", path)

        # Add a little slack so that timeout handler will be executed earlier and give cleaner error message.
        deadline = datetime.datetime.utcnow() + datetime.timedelta(seconds=timeout + 120)
        tx = client.Transaction(
            deadline=deadline,
            attributes={
                "title": transaction_title,
            })

        logging.info(f"Running in directory {path} under transaction {tx.transaction_id}")

        with client.Transaction(transaction_id=tx.transaction_id):
            client.lock(path, mode="shared")

            try:
                logging.info("Running callback")
                callback(path)
                logging.info("Finished OK")

                _report_success()

            except ProcessTerminatedError:
                raise
            except Exception as e:
                logging.exception("Callback failed")

                if hasattr(e, "error_message"):
                    error_message = e.error_message
                else:
                    error_message = str(e)

                _report_failure(error_message)

        logging.info("Committing transaction")
        tx.commit()

    except KeyboardInterrupt:
        if "tx" not in locals() or tx.is_pinger_alive():
            _report_failure(f"Timed out after {timeout} seconds")
        else:
            logging.error(f"Transaction {tx.transaction_id} was aborted or has expired")
            _report_failure(f"Transaction {tx.transaction_id} was aborted or has expired")
    except ProcessTerminatedError:
        pass
    except Exception:
        logging.exception("Main function terminated")
    finally:
        timeout_handler.disarm()

        children = multiprocessing.active_children()
        if children:
            logging.info("Terminating children")
            for process in children:
                process.kill()

        logging.info(f"Instance {path} finished")
