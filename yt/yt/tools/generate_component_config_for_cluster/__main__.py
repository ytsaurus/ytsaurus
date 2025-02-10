import click
import yt.wrapper as yt
import yt.yson as yson

from enum import IntEnum, unique
from importlib import resources
from typing import Any

import functools
import sys


ROOT_RESOURCE = resources.files(__package__)


@unique
class LogLevel(IntEnum):
    TRACE = 0
    DEBUG = 1
    INFO = 2
    WARNING = 3
    ERROR = 4


@click.group()
def cli():
    """Fetch @cluster_connection from a cluster, and apply it to the default config."""


def fetch_cluster_connection(proxy: str) -> yson.YsonType:
    yc = yt.YtClient(proxy=proxy)
    cluster_connection = yc.get("//sys/@cluster_connection")
    return cluster_connection


def create_logging_config(component_name: str, log_level: LogLevel) -> dict[str, Any]:
    rules = []
    writers = dict()
    for current_level in LogLevel:
        if current_level < log_level:
            continue

        rules.append(
            {
                "min_level": current_level.name.lower(),
                "writers": [
                    current_level.name,
                ],
            }
        )

        text_level = (
            "" if current_level == LogLevel.INFO else f".{current_level.name.lower()}"
        )
        filename = f"./{component_name}{text_level}.log"
        writers[current_level.name] = {
            "type": "file",
            "file_name": filename,
        }

    return {
        "rules": rules,
        "writers": writers,
    }


def create_structured_logging_config(
    component_name: str, structured_category_to_writer: dict[str, str]
) -> dict[str, Any]:
    rules = []
    writers = dict()
    for category, writer in structured_category_to_writer.items():
        filename = f"./{component_name}.yson.{writer}.log"
        rules.append(
            {
                "min_level": "info",
                "writers": [writer],
                "family": "structured",
                "include_categories": [category],
            },
        )
        writers[writer] = {
            "format": "yson",
            "type": "file",
            "file_name": filename,
        }

    return {
        "rules": rules,
        "writers": writers,
    }


# Recursive update. Lists are concatenated.
def deep_update(mapping, *updating_mappings):
    def is_dict(obj):
        return isinstance(obj, dict) or isinstance(obj, yson.YsonMap)

    def is_list(obj):
        return isinstance(obj, list) or isinstance(obj, yson.YsonList)

    updated_mapping = mapping.copy()
    for updating_mapping in updating_mappings:
        for k, v in updating_mapping.items():
            if k in updated_mapping:
                if is_dict(updated_mapping[k]) and is_dict(v):
                    updated_mapping[k] = deep_update(updated_mapping[k], v)
                elif is_list(updated_mapping[k]) and is_list(v):
                    updated_mapping[k] += v
                else:
                    updated_mapping[k] = v
            else:
                updated_mapping[k] = v
    return updated_mapping


def component_config(component_name: str):
    def decorator(func):
        @cli.command(component_name)
        @click.option(
            "--proxy",
            type=str,
            required=True,
            envvar="YT_PROXY",
            help="YT proxy to fetch cluster connection from. Also fetched from env var YT_PROXY.",
        )
        @click.option(
            "--log-level",
            type=click.Choice([i.name for i in LogLevel], case_sensitive=False),
            default="DEBUG",
            envvar="YT_LOG_LEVEL",
            help="Server log verbosity. Also fetched from YT_LOG_LEVEL.",
            show_default=True,
        )
        @click.option(
            "--patch-config",
            type=click.File("rb"),
            required=False,
            multiple=True,
            help="Custom config file(s) to apply on top of the default.",
        )
        @functools.wraps(func)
        def wrapper(
            proxy: str, log_level: str, patch_config: tuple[click.File], *args, **kwargs
        ):
            with (
                ROOT_RESOURCE / "templates" / "components" / f"{component_name}.yson"
            ).open("rb") as f:
                config = yson.load(f)

            cluster_connection = fetch_cluster_connection(proxy)
            config = deep_update(config, {"cluster_connection": cluster_connection})

            enum_level = LogLevel[log_level.upper()]
            config = deep_update(
                config, {"logging": create_logging_config(component_name, enum_level)}
            )

            rendered_config = func(*args, **kwargs, config=config)
            rendered_config = deep_update(
                rendered_config, *[yson.load(patch) for patch in patch_config]
            )

            sys.stdout.buffer.write(yson.dumps(rendered_config, yson_format="pretty"))

        return wrapper

    return decorator


@component_config("tablet-node")
def tablet_node(config: yson.YsonType) -> yson.YsonType:
    return config


@component_config("controller-agent")
@click.option("--tag", type=str, multiple=True, help="Controller agent tags.")
def controller_agent(config: yson.YsonType, tag: tuple[str]) -> yson.YsonType:
    if tag:
        config = deep_update(config, {"controller_agent": {"tags": list(tag)}})
    config = deep_update(
        config,
        {
            "logging": create_structured_logging_config(
                "controller-agent",
                {
                    "ChunkPool": "chunk_pool",
                },
            ),
        },
    )
    return config


@component_config("http-proxy")
def http_proxy(config: yson.YsonType) -> yson.YsonType:
    return config


@component_config("rpc-proxy")
def rpc_proxy(config: yson.YsonType) -> yson.YsonType:
    config = deep_update(
        config,
        {
            "logging": create_structured_logging_config(
                "rpc-proxy",
                {
                    "RpcProxyStructuredMain": "main",
                    "RpcProxyStructuredError": "error",
                },
            ),
        },
    )
    return config


if __name__ == "__main__":
    cli()
