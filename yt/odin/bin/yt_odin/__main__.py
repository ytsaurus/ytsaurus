#!/usr/bin/env python3

from yt_odin.odinserver.common import BoundProcess, get_part
from yt_odin.odinserver.odin import Odin
from yt_odin.storage.db import get_cluster_client_factory_from_db_config

from yt.common import update
import yt.logger as logger
import yt.wrapper as yt

from six.moves import cPickle as pickle, socketserver

from multiprocessing import Queue
from copy import deepcopy
import os
import sys
import argparse
import json
import time
import logging
import random
import signal
import struct
import socket


# See https://docs.python.org/2/howto/logging-cookbook.html#logging-to-a-single-file-from-multiple-processes
class LogRecordStreamHandler(socketserver.StreamRequestHandler):
    def handle(self):
        while True:
            chunk = self.connection.recv(4)
            if len(chunk) < 4:
                break
            slen = struct.unpack(">L", chunk)[0]
            chunk = self.connection.recv(slen)
            while len(chunk) < slen:
                delta = self.connection.recv(slen - len(chunk))
                if not delta:
                    logger.error("Root logserver got incomplete message (bytes received: %s, "
                                 "expected: %s)", len(chunk), slen)
                    return
                chunk += delta
            record = logging.makeLogRecord(pickle.loads(chunk))
            logger.LOGGER.handle(record)


def acquire_yt_lock(proxy, token, locks_dir, timeout, queue, shard_count):
    yt_client = yt.YtClient(proxy, token)
    yt_client.create("map_node", locks_dir, recursive=True, ignore_existing=True)
    title = f"Odin instance {socket.getfqdn()} transaction"
    with yt_client.Transaction(attributes={"title": title}) as tx:
        logger.info("Acquiring lock under tx %s", tx.transaction_id)
        shard_index = 0
        while True:
            try:
                yt_client.lock(locks_dir, mode="shared", child_key=str(shard_index))
                break
            except yt.YtError as err:
                if err.is_concurrent_transaction_lock_conflict():
                    logger.info("Failed to take lock for key %s", shard_index)
                    time.sleep(random.uniform(0.5 * timeout, 1.5 * timeout))
                    shard_index = (shard_index + 1) % shard_count
                    continue
                logger.exception(str(err))
                raise

        logger.info("Lock for shard %s is acquired", shard_index)

        queue.put(shard_index)

        # Sleep forever.
        while True:
            time.sleep(timeout)


def run_logging_server(port):
    socket_receiver = socketserver.ThreadingTCPServer(("localhost", port), LogRecordStreamHandler)
    socket_receiver.serve_forever()


def run_odin(storage_kwargs, odin_kwargs):
    odin = Odin(get_cluster_client_factory_from_db_config(storage_kwargs), **odin_kwargs)
    odin.run()


class LoggerConfigurator(object):
    def __init__(self, logging_config):
        self._level = logging.__dict__[logging_config.get("level", "INFO")]
        if "filename" in logging_config:
            self._handler = logging.handlers.WatchedFileHandler(logging_config["filename"])
        else:
            self._handler = logging.StreamHandler()
        self._handler.setLevel(logging.DEBUG)

    def configure(self, logger):
        logger.propagate = False
        logger.setLevel(self._level)
        logger.handlers = [self._handler]
        logger.handlers[0].setFormatter(logging.Formatter("%(asctime)-15s %(process)s %(levelname)s\t%(message)s"))


def shutdown(lock_process, odin_processes):
    logger.info("Killing child odin processes")
    try:
        os.kill(lock_process.pid, signal.SIGINT)
    except OSError:
        pass

    for process in odin_processes:
        try:
            os.kill(process.pid, signal.SIGTERM)
        except OSError:
            pass
    time.sleep(0.1)
    for process in odin_processes:
        try:
            os.kill(process.pid, signal.SIGKILL)
        except OSError:
            pass


def main(args):
    config = json.load(open(args.config))

    def get_config_parameter(cluster, subconfig, parameter=None):
        cluster_config = config["clusters"][cluster]
        template_config = config["template"]

        def get(cfg):
            if parameter is not None:
                return cfg[subconfig][parameter]
            else:
                return cfg[subconfig]

        assert any(item is not None for item in (subconfig, parameter))
        try:
            return get(cluster_config)
        except KeyError:
            return get(template_config)

    # Configure logging
    logger_configurator = LoggerConfigurator(config["logging"])
    for logger_id in ("Yt", "Odin", "Odin tasks"):
        logger_configurator.configure(logging.getLogger(logger_id))
    logging_process = BoundProcess(target=run_logging_server, args=(config["logging"]["port"],),
                                   name="LogMain")
    logging_process.daemon = True
    logging_process.start()

    logger.LOGGER.handlers = [logging.handlers.SocketHandler('localhost', config["logging"]["port"])]

    # Take lock
    service_config = config["service_config"]
    shard_count = service_config["shard_count"]
    assert shard_count > 0

    lock_kwargs = dict((x, service_config[x]) for x in ["proxy", "token", "timeout", "shard_count"])
    lock_queue = Queue()
    lock_kwargs["queue"] = lock_queue
    lock_kwargs["locks_dir"] = yt.ypath_join(service_config["path"], "locks")
    lock_process = BoundProcess(target=acquire_yt_lock, kwargs=lock_kwargs,
                                name="Lock", pdeathsig=signal.SIGINT)
    lock_process.daemon = True
    lock_process.start()
    shard_index = lock_queue.get()
    assert 0 <= shard_index < shard_count

    clusters = get_part(config["clusters"].keys(), shard_index, shard_count)
    logger.info("Monitoring [%s] clusters", ', '.join(clusters))

    juggler_config = config.get("juggler_config", {})
    secrets = config.get("secrets", {})

    processes = []
    try:
        for cluster in clusters:
            cluster_config = update(deepcopy(config["template"]), config["clusters"][cluster])

            db_kwargs = cluster_config["db_config"]

            socket_path = cluster_config["log_server_config"]["socket_path_pattern"].format(cluster_name=cluster)
            if os.path.exists(socket_path):
                os.unlink(socket_path)

            log_server_storage_writer_config = cluster_config["log_server_config"]["storage_writer"]
            odin_kwargs = dict(
                cluster_name=cluster,
                proxy=cluster_config["yt_config"]["proxy"],
                token=cluster_config["yt_config"]["token"],
                log_server_max_write_batch_size=log_server_storage_writer_config["max_write_batch_size"],
                log_server_socket_path=socket_path,
                checks_path=config["checks"]["path"],
                yt_request_retry_timeout=cluster_config["yt_config"]["request_retry_timeout"],
                yt_request_retry_count=cluster_config["yt_config"]["request_retry_count"],
                yt_heavy_request_retry_timeout=cluster_config["yt_config"]["heavy_request_retry_timeout"],
                default_check_timeout=config["checks"]["check_timeout"],
                check_log_messages_max_size=config["checks"]["check_log_messages_max_size"],
                juggler_client_host=juggler_config.get("host"),
                juggler_client_port=juggler_config.get("port"),
                juggler_client_scheme=juggler_config.get("scheme"),
                juggler_host=cluster_config.get("juggler_host"),
                juggler_responsibles=juggler_config.get("responsibles"),
                yt_enable_proxy_discovery=cluster_config["yt_config"].get("enable_proxy_discovery", True),
                yt_driver_address_resolver_config=cluster_config["yt_config"].get("driver_address_resolver_config", {}),
                secrets=secrets)

            process = BoundProcess(target=run_odin, args=(db_kwargs, odin_kwargs),
                                   name="Odin[{}]".format(cluster))
            process.start()
            processes.append(process)

        with open(service_config["pids_file"], "w") as f:
            for process in processes:
                f.write(str(process.pid) + "\n")

        # Sleep infinitely for correct handling errors in child processes
        while True:
            time.sleep(1)
    except:  # noqa
        logger.exception("Odin terminated")
        shutdown(lock_process, processes)


if __name__ == "__main__":
    try:
        import prctl
        prctl.set_name("Main")
    except ImportError:
        pass

    parser = argparse.ArgumentParser(description="Odin -- service for observing of clusters health")
    parser.add_argument("--config", required=True, help="path to config file (file in json format)")
    args = parser.parse_args()
    sys.exit(main(args))
