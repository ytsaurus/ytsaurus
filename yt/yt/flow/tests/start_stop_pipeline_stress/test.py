import logging
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
import requests

from yt.yt.flow.tests.computation_cycles_and_buffers.lib.test_base import TestBase

##################################################################

GET_WORKER_ORCHID_COMMAND = "get-worker-orchid"

#################################################################


def get_worker_jobs(orchid_address):
    try:
        return requests.get(orchid_address).json()
    except requests.exceptions.RequestException:
        logging.exception("Can't perform request to %s", orchid_address)
        return {}
    except requests.exceptions.JSONDecodeError:
        logging.exception("Can't parse response from %s", orchid_address)


def get_workers_jobs(workers, yt_client, args):
    with ThreadPoolExecutor(max_workers=100) as executor:
        logging.info("Getting worker orchids directly")
        worker_orchid_addresses = set()
        for worker in workers.values():
            monitoring_address = worker["monitoring_address"]
            worker_orchid_addresses.add(f"http://{monitoring_address}/orchid/job_tracker/jobs")
        return list(executor.map(get_worker_jobs, worker_orchid_addresses))


def get_data(yt_client, pipeline_path):
    flow_view = yt_client.get_flow_view(pipeline_path, cache=False)
    workers = flow_view["state"]["workers"]
    return get_workers_jobs(workers, yt_client, {"pipeline_path": pipeline_path})


class Test(TestBase):

    @pytest.mark.authors(["engeldanila"])
    @pytest.mark.parametrize(
        ("workers_count", "controllers_count", "problems"),
        [pytest.param(1, 1, False, id="1c_1w_stable"), pytest.param(1, 4, False, id="1c_4w_stable")],
    )
    def test_basic(self, workers_count, controllers_count, problems):
        self.prepare_environment()
        pipeline_config_path = self.prepare_pipeline_config(finite=False)
        federation = self.start_flow_process_federation(
            pipeline_binary_args={
                "--config": pipeline_config_path,
            },
            workers_count=workers_count,
            controllers_count=controllers_count,
            problems=problems,
        )

        with federation:
            self.wait_pipeline_state("working", timeout=180)
            self.client.stop_pipeline(self.pipeline_path)
            self.wait_pipeline_state(["stopped", "completed"], timeout=180)

            spec = self.client.get_pipeline_spec(self.pipeline_path)

            # If test timeouts then iterate fewer times
            logging.info("Beginned multiple start/stop pipeline iterations")
            for _ in range(5):
                self.client.start_pipeline(self.pipeline_path)
                deadline = time.monotonic() + 10
                all_workers_have_jobs = True
                while time.monotonic() < deadline:
                    data = get_data(self.client, self.pipeline_path)
                    all_workers_have_jobs = True
                    for worker in data:
                        if len(worker) == 0:
                            all_workers_have_jobs = False
                            break
                    if all_workers_have_jobs:
                        break
                if not all_workers_have_jobs:
                    pytest.fail("Jobs don't start after pipeline start")

                self.client.stop_pipeline(self.pipeline_path)
                deadline = time.monotonic() + 180
                all_jobs_are_dead = False
                while time.monotonic() < deadline:
                    all_jobs_are_dead = True
                    data = get_data(self.client, self.pipeline_path)
                    for worker in data:
                        if len(worker) != 0:
                            all_jobs_are_dead = False
                            break
                    if all_jobs_are_dead:
                        break
                if not all_jobs_are_dead:
                    pytest.fail("Jobs don't die after pipeline stop")
            logging.info("Multiple start/stop pipeline iterations finished")

            # The loop above only waits for worker jobs to disappear, but the controller may still
            # be finishing the drain (the "draining" state), and set_pipeline_spec is rejected until
            # the pipeline is fully stopped.
            self.wait_pipeline_state(["stopped", "completed"], timeout=180)

            spec["spec"]["computations"]["reader"]["source_streams"]["queue"]["parameters"]["finite"] = True
            self.client.set_pipeline_spec(self.pipeline_path, spec["spec"], expected_version=spec["version"])
            self.client.start_pipeline(self.pipeline_path)

            self.wait_pipeline_state("completed", timeout=180)
            logging.info("pipeline completed")
