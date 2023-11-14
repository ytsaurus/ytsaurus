from .argument_manager import CheckArgumentManager

from yt_odin.logserver import OdinCheckSocketHandler, UNAVAILABLE_STATE, FAILED_STATE

import yt.logger as yt_logger
import yt.wrapper as yt
import yt.packages.requests as requests

from yt.wrapper.common import get_binary_std_stream

from six.moves import cPickle as pickle

import os
import socket
import sys
import time
from functools import wraps
import logging
import traceback
import logging.handlers


class TaskSocketHandler(logging.handlers.SocketHandler):
    def __init__(self, *args, **kwargs):
        self._task_id = kwargs.pop("task_id")
        super(TaskSocketHandler, self).__init__(*args, **kwargs)

    def emit(self, record):
        record.msg = "{}\t{}".format(self._task_id, record.msg)
        super(TaskSocketHandler, self).emit(record)


def catch_all_errors(func, task_logger):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (yt.YtError, requests.RequestException) as error:
            # TODO(ignat): do we still need this hack?
            # Python2.7 has a bug in unpickling exception with non-trivial constructor.
            # To overcome it we convert exception to string here.
            # https://bugs.python.org/issue1692335
            task_logger.exception(str(error))
            return UNAVAILABLE_STATE
        except:  # noqa
            errors = traceback.format_exception(*sys.exc_info())
            task_logger.error("Check failed with unknown error " + "\n".join(errors))
            return FAILED_STATE
    return wrapper


def add_logs(func):
    @wraps(func)
    def wrapper(task_logger, *args, **kwargs):
        task_logger.info("Check started (hostname: %s)", socket.gethostname())
        yt._cleanup_http_session()
        return catch_all_errors(func, task_logger)(*args, **kwargs)
    return wrapper


def configure_loggers(
    task_id,
    check_log_server_socket_path,
    text_log_server_port,
    frontend_log_level,
    backend_log_level,
):
    handlers = []
    if check_log_server_socket_path is not None:
        handlers.append(OdinCheckSocketHandler(check_log_server_socket_path, task_id))
    else:
        # We are in standalone mode, just print to stderr.
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setFormatter(logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s'))
        handlers.append(stderr_handler)
    handlers[-1].setLevel(frontend_log_level)

    if text_log_server_port is not None:  # This is None in tests and in case of standalone binary.
        text_handler = TaskSocketHandler(
            "localhost",
            text_log_server_port,
            task_id=task_id,
        )
        text_handler.setLevel(backend_log_level)
        handlers.append(text_handler)

    def setup_logger(logger):
        logger.handlers = handlers
        logger.setLevel(logging.DEBUG)
        logger.propagate = False

    # YT logger
    setup_logger(yt_logger.LOGGER)

    # YP logger
    try:
        import yp.logger as yp_logger
        setup_logger(yp_logger.logger)
    except ImportError:
        pass

    # Solomon logger
    try:
        from solomon.solomon import LOGGER as solomon_logger
        setup_logger(solomon_logger)
    except ImportError:
        pass

    tasks_logger = logging.getLogger("Odin tasks")
    setup_logger(tasks_logger)

    return tasks_logger


def run_check(check_function, task_logger, argument_manager):
    try:
        check_arguments = argument_manager.make_arguments(check_function)
    except (TypeError, ValueError):
        task_logger.exception('Failed to make arguments for check')
        return dict(
            state=FAILED_STATE,
            description=None,
            duration=None
        )

    start_timestamp = time.time()
    check_result = add_logs(check_function)(task_logger, **check_arguments)
    duration = int(1000 * (time.time() - start_timestamp))

    if isinstance(check_result, tuple):
        state, description = check_result
    else:
        state = check_result
        description = None
    return dict(
        state=state,
        description=description,
        duration=duration
    )


def main(check_function):
    kwargs = pickle.load(get_binary_std_stream(sys.stdin))
    frontend_log_level = logging.getLevelName(os.environ.get("YT_ODIN_FRONTEND_LOG_LEVEL", "INFO").upper())
    backend_log_level = logging.getLevelName(os.environ.get("YT_ODIN_BACKEND_LOG_LEVEL", "DEBUG").upper())
    task_logger = configure_loggers(kwargs["task_id"], kwargs["check_log_server_socket_path"],
                                    kwargs["text_log_server_port"], frontend_log_level, backend_log_level)
    argument_manager = CheckArgumentManager(task_logger, kwargs)
    check_result = run_check(check_function, task_logger, argument_manager)
    pickle.dump(check_result, get_binary_std_stream(sys.stdout), protocol=2)
