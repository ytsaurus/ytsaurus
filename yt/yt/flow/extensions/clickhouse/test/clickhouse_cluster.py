import os
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path

import yatest.common
from clickhouse_driver import Client
from clickhouse_driver.errors import Error
from yatest.common.network import PortManager

from yt.common import wait


class ClickHouseCluster:
    def __init__(self, native_port):
        self.native_port = native_port
        self.hosts = {"a1": "127.0.0.3", "a2": "127.0.0.4", "b1": "127.0.0.5"}
        self.clients = {}
        self.processes = {}
        self.logs = {}
        self.ports = PortManager()

    def start(self):
        template = yatest.common.source_path("library/recipes/clickhouse/recipe/config/config_with_zookeeper.xml")
        users = yatest.common.source_path("library/recipes/clickhouse/recipe/config/users.xml")
        try:
            for name, host in self.hosts.items():
                directory = Path(yatest.common.output_path("physical_clickhouse_" + name))
                directory.mkdir(parents=True, exist_ok=True)
                tree = ET.parse(template)
                root = tree.getroot()
                for tag in ("listen_host", "interserver_http_host"):
                    for node in root.findall(tag):
                        root.remove(node)
                    ET.SubElement(root, tag).text = host
                values = {
                    "tcp_port": str(self.native_port),
                    "http_port": str(self.ports.get_port()),
                    "interserver_http_port": str(self.ports.get_port()),
                    "path": str(directory / "data") + "/",
                    "tmp_path": str(directory / "tmp") + "/",
                    "user_files_path": str(directory / "user_files") + "/",
                    "format_schema_path": str(directory / "format_schema") + "/",
                    "users_config": users,
                    "logger/log": str(directory / "server.log"),
                    "macros/shard": name[0],
                    "macros/replica": name,
                }
                for tag, value in values.items():
                    node = root.find(tag)
                    assert node is not None, tag
                    node.attrib.clear()
                    node.text = value
                for subdirectory in ("data", "tmp", "user_files", "format_schema"):
                    (directory / subdirectory).mkdir(exist_ok=True)
                config = directory / "config.xml"
                tree.write(config)
                log = open(directory / "process.log", "wb")
                self.logs[name] = log
                self.processes[name] = subprocess.Popen(
                    [os.environ["RECIPE_CLICKHOUSE_BIN"], "server", "--config", str(config)],
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    env=os.environ.copy(),
                )
                self.clients[name] = Client(host=host, port=self.native_port)

                def ready():
                    assert self.processes[name].poll() is None, name
                    try:
                        return self.clients[name].execute("SELECT 1") == [(1,)]
                    except Error:
                        return False

                wait(ready, timeout=60)
        except BaseException:
            self.close()
            raise

    def stop(self, name):
        process = self.processes[name]
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)

    def close(self):
        try:
            for name in self.processes:
                self.stop(name)
            for client in self.clients.values():
                client.disconnect()
            for log in self.logs.values():
                log.close()
        finally:
            self.ports.release()
