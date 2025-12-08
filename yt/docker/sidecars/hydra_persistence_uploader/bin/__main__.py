import os
import logging

from apscheduler.schedulers import blocking
from apscheduler.executors import pool
from apscheduler.jobstores import memory
from apscheduler.triggers.interval import IntervalTrigger


import hydra_persistence_uploader


handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)-8s %(name)-30s %(message)s"))

log = logging.getLogger(__name__)
log.addHandler(handler)
log.setLevel(logging.DEBUG)


def configure_logging():
    periodic_log = logging.getLogger("periodic")
    periodic_log.addHandler(handler)
    periodic_log.setLevel(logging.DEBUG)

    apscheduler_log = logging.getLogger("apscheduler")
    apscheduler_log.addHandler(handler)
    apscheduler_log.setLevel(logging.DEBUG)

    urllib3_log = logging.getLogger("urllib3")
    urllib3_log.addHandler(handler)
    urllib3_log.setLevel(logging.DEBUG)


def get_scheduler():
    scheduler = blocking.BlockingScheduler()
    scheduler.add_jobstore(memory.MemoryJobStore())
    return scheduler


def add_hydra_persistence_uploader_job(scheduler):
    yt_proxy = os.environ.get("YT_PROXY", None)
    if yt_proxy is None:
        raise ValueError("YT_PROXY is not set")

    ytserver_config = os.environ.get("YT_SERVER_CONFIG", "/config/ytserver-master.yson")
    yt_token_file = os.environ.get("YT_TOKEN_FILE", "/root/.yt/token")

    log.debug(
        "add_hydra_persistence_uploader_job with: yt_proxy '%s', ytserver_config '%s', yt_token_file %s",
        yt_proxy,
        ytserver_config,
        yt_token_file,
    )

    setup = hydra_persistence_uploader.get_setup(ytserver_config)

    if not os.path.exists(setup.master_binary_path):
        setup.master_binary_path = "/shared-binaries/ytserver-master"

    yt_client = hydra_persistence_uploader.get_yt_client(yt_proxy, yt_token_file)

    config = hydra_persistence_uploader.MasterHydraPersistenceUploaderConfig(upload_changelogs=False)

    hydra_persistence_uploader_job = hydra_persistence_uploader.MasterHydraPersistenceUploaderJob(setup, yt_client, config)

    scheduler.add_job(
        hydra_persistence_uploader_job,
        IntervalTrigger(seconds=60),
        max_instances=1,
        id="{}".format(hydra_persistence_uploader_job.__class__),
        misfire_grace_time=60,
    )


if __name__ == "__main__":
    configure_logging()

    scheduler = get_scheduler()

    add_hydra_persistence_uploader_job(scheduler)

    scheduler.add_executor(pool.ThreadPoolExecutor(len(scheduler.get_jobs())))
    scheduler.start()
