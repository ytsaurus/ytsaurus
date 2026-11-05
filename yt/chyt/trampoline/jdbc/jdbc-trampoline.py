import argparse
import json
import os
import re
from pathlib import Path
import tempfile

import yt.yson as yson

CONFIG_FILE = "jdbc-trampoline.yson"


def resolve_secrets(value, environ):
    if isinstance(value, dict):
        return {key: resolve_secrets(item, environ) for key, item in value.items()}
    if isinstance(value, list):
        return [resolve_secrets(item, environ) for item in value]
    if isinstance(value, str) and re.fullmatch(r"\$YT_SECURE_VAULT_[A-Za-z0-9_]+", value):
        key = value[1:]
        if key not in environ:
            raise ValueError("Missing JDBC secret: " + key)
        return environ[key]
    return value


def prepare_command(config, environ, port):
    bridge_jar = Path(config["bridge_jar"]).resolve()
    if not bridge_jar.is_file():
        raise ValueError("JDBC bridge JAR does not exist: " + str(bridge_jar))

    java_bin = config.get("java_bin")
    if not isinstance(java_bin, str) or not java_bin:
        raise ValueError("java_bin must be a non-empty string")

    drivers_dir = Path("drivers").resolve()
    drivers_dir.mkdir(exist_ok=True)
    for filename in config.get("drivers", []):
        source = Path(filename).resolve()
        if not source.is_file():
            raise ValueError("JDBC driver JAR does not exist: " + str(source))
        destination = drivers_dir / Path(filename).name
        if destination.is_symlink():
            destination.unlink()
        destination.symlink_to(source)

    datasources = {}
    for filename in config.get("datasource_files", []):
        path = Path(filename)
        with path.open() as stream:
            source = json.load(stream)
        if not isinstance(source, dict):
            raise ValueError("Datasource file must contain a JSON object: " + str(path))
        for name, properties in source.items():
            if name == "$schema":
                continue
            if name in datasources:
                raise ValueError("Duplicate JDBC datasource: " + name)
            datasources[name] = resolve_secrets(properties, environ)

    # Never modify sandbox artifacts: they may be links into the shared chunk cache.
    config_dir = Path(tempfile.mkdtemp(prefix="jdbc-config-", dir=Path.cwd()))
    with (config_dir / "server.json").open("x") as stream:
        json.dump({"serverPort": port}, stream)
    datasource_dir = config_dir / "datasources"
    datasource_dir.mkdir(mode=0o700)
    destination = datasource_dir / "datasources.json"
    with open(destination, "x", opener=lambda path, flags: os.open(path, flags, 0o600)) as stream:
        json.dump(datasources, stream)

    return [
        java_bin,
        "-Xms128m", "-Xmx1g",
        "-Djdbc-bridge.config.dir=" + str(config_dir),
        "-Djdbc-bridge.driver.dir=" + str(drivers_dir),
        "-jar",
        str(bridge_jar),
    ]


def main():
    parser = argparse.ArgumentParser(description="Prepare JDBC datasources and execute the JDBC bridge")
    parser.add_argument("--port", type=int, required=True, help="YT-allocated JDBC bridge port")
    args = parser.parse_args()
    with open(CONFIG_FILE, "rb") as stream:
        config = yson.load(stream)
    command = prepare_command(config, os.environ, args.port)
    os.execvp(command[0], command)
