import logging
import threading
import sys
import random
import string


logging_attributes = threading.local()


def generate_trace():
    return "".join(random.choice(string.ascii_uppercase) for i in range(8))


class TraceContext:
    def __enter__(self):
        self.trace = generate_trace()
        logging_attributes.trace = self.trace
        return self.trace

    def __exit__(self, *args, **kwargs):
        del logging_attributes.trace


class ThreadContextFilter(logging.Filter):
    def filter(self, record):
        record.thread_name = threading.current_thread().name
        for key, value in logging_attributes.__dict__.items():
            setattr(record, key, value)
        if not hasattr(record, "trace"):
            record.trace = ""

        return True


_file_handler = None


def update_file_handler(filename):
    global _file_handler

    if _file_handler is not None:
        logger.removeHandler(_file_handler)

    file_handler = logging.FileHandler(filename, "a")
    file_handler.setLevel(logging.DEBUG)
    format = '[%(thread_name)s] %(asctime)s %(levelname)-8s %(message)s    %(trace)s'
    file_handler.setFormatter(logging.Formatter(format))
    logger.addHandler(file_handler)

    _file_handler = file_handler


def get_logger():
    if "logger" in globals():
        return globals()["logger"]

    global logger

    logger = logging.getLogger("my")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    logging.basicConfig(format='[%(thread_name)s] %(asctime)s %(levelname)-8s %(message)s    %(trace)s')
    format = '[%(thread_name)s] %(asctime)s %(levelname)-8s %(message)s    %(trace)s'
    logger.addFilter(ThreadContextFilter())

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(format))
    logger.addHandler(console_handler)

    update_file_handler("app.log")

    logging.getLogger("Yt").setLevel(logging.ERROR)

    return logger


logger = get_logger()
