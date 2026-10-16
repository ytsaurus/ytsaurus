import logging
import os

formatter = logging.Formatter("%(asctime)s\t%(levelname).1s\t%(name)-22s\t%(message)s")
handler = logging.StreamHandler(None)
handler.setFormatter(formatter)
logging.root.addHandler(handler)
logging.root.setLevel(logging.INFO)

start_time = os.environ.get("JOB_START_TIME")
if start_time is not None:
    logging.info("Job started at %s", start_time)


def clear_logger():
    logging.getLogger().removeHandler(handler)
