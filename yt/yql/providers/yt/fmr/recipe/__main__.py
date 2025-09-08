import argparse
import logging
import os
import yatest.common
import sys
import time

from library.python.testing.recipe import declare_recipe, set_env
from library.python.testing.recipe.ports import get_port_range, release_port_range
from library.recipes.common import start_daemon

logging.basicConfig(
    stream=sys.stderr,
    level=logging.DEBUG,
    force=True,
)

logger = logging.getLogger(__name__)

COORDINATOR_PID_FILE = "recipe.coordinator.pid"
WORKER_PID_FILE = "recipe.worker.pid"
TABLE_DATA_SERVICE_PID_FILE = "recipe.table_data_service.pid"

TABLE_DATA_SERVICE_DISCOVERY_FILE = "table_data_service_discovery.txt"


TABLE_DATA_SERVICE_BINARY_PATH = "yt/yql/providers/yt/fmr/table_data_service/service/run_table_data_service_server"
COORDINATOR_BINARY_PATH = "yt/yql/providers/yt/fmr/coordinator/service/run_coordinator_server"
WORKER_BINARY_PATH = "yt/yql/providers/yt/fmr/worker/service/run_worker"
FMRJOB_BINARY_PATH = "yt/yql/providers/yt/fmr/job/fmrjob/fmrjob"


class FmrProcess:
    """Base class for all binaries needed to run for fmr gateway

    Args:
        name: type of process (coordinator, worker or table data service)
        binary_path: binary to execute in separate process
        args: args to binary (except for port)
        pid_filename: where to write pid of launched process
        id: id of process (in case of multiple worker / table data service nodes)
    """
    def __init__(self: str, name: str, binary_path: str, args: list[str], pid_filename: str, id: int = 0):
        self.name = name
        self.binary_path = binary_path
        self.args = args
        self.env = os.environ.copy()
        self.pid_filename = pid_filename
        self.id = id

    def run(self, port):
        logger.info(f"Running fmr {self.name} with id {self.id}")

        start_daemon(
            command=[self.binary_path] + self.args + ["--port", str(port)],
            environment=self.env,
            is_alive_check=lambda: is_alive(port),
            pid_file_name=yatest.common.work_path(self.pid_filename),
            daemon_name=self.name + "_" + str(self.id)
        )

        with open(yatest.common.work_path(self.pid_filename)) as file:
            pid = file.read()
        logging.info(f"{self.name} with id {self.id} is running on port {port} with pid {pid}")


def is_alive(port):
    # TODO  - заменить на вызов ручки /ping в сервисах
    time.sleep(1)
    return True


def parse_input_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('--worker_nodes_num', default=1, type=int)
    parser.add_argument('--table_data_service_nodes_num', default=1, type=int)
    parser.add_argument('--gateway_type', default="native", type=str)
    parser.add_argument('-v', '--verbosity', default=8, type=int)
    res = vars(parser.parse_args(args))
    return res


def get_pid_filename(pid_filename, index):
    return pid_filename + "_" + str(index)


def create_table_data_service_discovery_file(table_data_service_node_ports):
    with open(TABLE_DATA_SERVICE_DISCOVERY_FILE, 'w') as file:
        for port in table_data_service_node_ports:
            file.write(f"localhost:{port}\n")


def terminate_process(pid_filename: str, process_name: str, id: int = 1):
    with open(pid_filename, 'r') as file:
        pid = file.read()
    logger.info(f"Terminating {process_name} with id {id} on pid {pid}")
    release_port_range(pid_filename=pid_filename)


def start(args):
    arguments = parse_input_args(args)
    table_data_service_nodes_num, worker_nodes_num = arguments['table_data_service_nodes_num'], arguments['worker_nodes_num']
    logger.info("Gotten request to start %s table data service nodes, and %s worker nodes", table_data_service_nodes_num, worker_nodes_num)
    logging_level = arguments['verbosity']
    gateway_type = arguments['gateway_type']

    if (gateway_type != "native" and gateway_type != "file"):
        error_message = f"Gotten incorrect gateway type \"{gateway_type}\" in recipe parameters"
        raise RuntimeError(error_message)

    # starting table data service nodes
    table_data_service_node_ports = []
    for i in range(table_data_service_nodes_num):
        port = get_port_range(pid_filename=get_pid_filename(TABLE_DATA_SERVICE_PID_FILE, i))
        table_data_service = FmrProcess(
            name="table_data_service",
            binary_path=yatest.common.binary_path(TABLE_DATA_SERVICE_BINARY_PATH),
            args=["--verbosity", str(logging_level), "--worker-id", str(i), "--workers-num", str(table_data_service_nodes_num)],
            pid_filename=get_pid_filename(TABLE_DATA_SERVICE_PID_FILE, i),
            id=i
        )
        table_data_service.run(port)
        table_data_service_node_ports.append(port)

    # creating table_data_service_discovery_file
    create_table_data_service_discovery_file(table_data_service_node_ports)

    # starting coordinator
    coordinator_port = get_port_range(pid_filename=COORDINATOR_PID_FILE)
    coordinator = FmrProcess(
        name="coordinator",
        binary_path=yatest.common.binary_path(COORDINATOR_BINARY_PATH),
        args=[
            "--verbosity", str(logging_level),
            "--workers-num", str(worker_nodes_num),
            "--table-data-service-discovery-file-path", TABLE_DATA_SERVICE_DISCOVERY_FILE,
            "--gateway-type", gateway_type
        ],
        pid_filename=COORDINATOR_PID_FILE
    )
    coordinator.run(coordinator_port)
    coordinator_url = "http://localhost:" + str(coordinator_port)
    set_env("FMR_COORDINATOR_URL", coordinator_url)

    # starting workers
    for i in range(worker_nodes_num):
        worker_port = get_port_range(pid_filename=get_pid_filename(WORKER_PID_FILE, i))
        worker = FmrProcess(
            name="worker",
            binary_path=yatest.common.binary_path(WORKER_BINARY_PATH),
            args=[
                "--verbosity", str(logging_level),
                "--coordinator-url", coordinator_url,
                "--table-data-service-discovery-file-path", TABLE_DATA_SERVICE_DISCOVERY_FILE,
                "--worker-id", str(i),
                "--fmrjob-binary-path", FMRJOB_BINARY_PATH,
                "--gateway-type", gateway_type
            ],
            pid_filename=get_pid_filename(WORKER_PID_FILE, i),
            id=i
        )
        worker.run(worker_port)


def stop(args):
    arguments = parse_input_args(args)
    table_data_service_nodes, worker_nodes = arguments['table_data_service_nodes_num'], arguments['worker_nodes_num']

    for i in range(worker_nodes):
        terminate_process(get_pid_filename(WORKER_PID_FILE, i), "worker", i)

    terminate_process(COORDINATOR_PID_FILE, "coordinator")

    for i in range(table_data_service_nodes):
        terminate_process(get_pid_filename(TABLE_DATA_SERVICE_PID_FILE, i), "table_data_service", i)


if __name__ == "__main__":
    declare_recipe(start, stop)
