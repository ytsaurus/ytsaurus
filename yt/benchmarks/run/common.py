import click
import functools
import json

from importlib import resources
from importlib.resources.abc import Traversable
from pathlib import Path
from datetime import datetime
from typing import Any
from enum import StrEnum

################################################################################

ROOT_RESOURCE = resources.files(__package__)


class QuerySource(StrEnum):
    RESOURCES = "resources"
    FILESYSTEM = "filesystem"


def root_path(query_source: QuerySource) -> Path | Traversable:
    return ROOT_RESOURCE if query_source == QuerySource.RESOURCES else Path()


def make_query(
    index: int,
    optimized: bool,
    query_prefix: str,
    optimized_prefix: str,
    query_source: QuerySource,
    pragma_add: list[str],
    pragma_file: str | None,
    pragma_preset: list[str],
) -> str:
    if pragma_file:
        with open(pragma_file, "r") as file:
            pragmas = file.read()
    else:
        pragmas = ""
        for preset in pragma_preset:
            pragmas += (ROOT_RESOURCE / "pragmas" / f"{preset}.sql").read_text() + "\n"

    for pragma in pragma_add:
        pragmas += "pragma " + pragma + ";\n"

    query_body = None
    query_name = f"{index:02d}.sql"
    path = root_path(query_source)
    query_path = None
    optimized_path = None
    try:
        query_path = path / query_prefix / query_name
        optimized_path = path / optimized_prefix / query_name
    except FileNotFoundError:
        pass
    if query_path is not None and query_path.is_file():
        if optimized and optimized_path is not None and optimized_path.is_file():
            query_body = optimized_path.read_text()
        else:
            query_body = query_path.read_text()

    if query_body is None:
        raise RuntimeError(f"Query with index {index} is not present in benchmark")

    return pragmas + "\n" + query_body


def list_all_queries(query_prefix: str, query_source: QuerySource) -> list[int]:
    path = root_path(query_source)
    return [int(query.name.removesuffix(".sql"))
            for query in (path / query_prefix).iterdir()
            if query.name.endswith(".sql")]


def list_all_pragma_presets() -> list[str]:
    return [preset.name.removesuffix(".sql")
            for preset in (ROOT_RESOURCE / "pragmas").iterdir()
            if preset.name.endswith(".sql")]


class DurationType(click.ParamType):
    name = "duration"

    # Returns the number of seconds in a duration `value`.
    def convert(self, value: str | int, param, ctx) -> int:
        if isinstance(value, int):
            return value

        try:
            value = value.lower()
            if value.endswith("h"):
                return int(value[:-1]) * 60 * 60
            if value.endswith("m"):
                return int(value[:-1]) * 60
            if value.endswith("s"):
                return int(value[:-1])
            return int(value)
        except Exception:
            self.fail(f"{value!r} is not a valid duration", param, ctx)


class IntList(click.ParamType):
    name = "int_list"

    def convert(self, value: str | list[int], param, ctx) -> list[int]:
        if isinstance(value, list):
            return value

        components = value.split(",")
        res: list[int] = []
        for component in components:
            try:
                res.append(int(component))
            except ValueError:
                try:
                    start, end = component.split("-", maxsplit=1)
                    start = int(start)
                    end = int(end)
                    for i in range(start, end + 1):
                        res.append(i)
                except Exception:
                    self.fail(f"{component!r} is not a valid int range", param, ctx)
        return res


class ArtifactLogger:
    path: Path | None = None
    launch_path: Path | None = None
    now: datetime | None = None
    query_path: Path | None = None

    def __init__(self, path: str | None):
        if not path:
            return
        self.path = Path(path)
        self.path.mkdir(exist_ok=True)

    def __enter__(self):
        if not self.path:
            return self
        self.now = datetime.now()
        self.launch_path = self.path / f"launch-{self.now.isoformat()}"
        self.launch_path.mkdir()
        return self

    def __exit__(self, *args):
        self.query_path = None
        self.launch_path = None
        self.now = None

    def dump_launch(self, arguments: dict[str, Any]):
        if not self.launch_path:
            return

        removed_keys: list[str] = []
        for key in arguments.keys():
            if "token" in key.lower():
                removed_keys.append(key)
        for key in removed_keys:
            arguments.pop(key)

        info = {
            "timestamp": (self.now.timestamp() if self.now else -1),
            "arguments": arguments,
        }
        with (self.launch_path / "launch.json").open("w") as f:
            json.dump(info, f)

    def start_query(self, query: int):
        if not self.launch_path:
            return
        self.query_path = self.launch_path / "queries" / str(query)
        self.query_path.mkdir(parents=True)

    def dump_query(self, query: str):
        if not self.query_path:
            return
        with (self.query_path / "query.yql").open("w") as f:
            f.write(query)

    def dump_id(self, query_id: str):
        if not self.query_path:
            return
        with (self.query_path / "id.txt").open("w") as f:
            f.write(query_id)

    def dump_info(self, query_info: dict[str, Any]):
        if not self.query_path:
            return
        with (self.query_path / "info.json").open("w") as f:
            json.dump(query_info, f)

    def dump_error(self, error: dict[str, Any]):
        if not self.query_path:
            return
        with (self.query_path / "error.json").open("w") as f:
            json.dump(error, f)


################################################################################


def run_options(func):
    @click.option(
        "-q",
        "--queries",
        type=IntList(),
        default=list_all_queries("queries", QuerySource.RESOURCES),
        help="Comma-separated query index (or index range) list (by default all .sql files are loaded)",
    )
    @click.option(
        "--optimized/--no-optimized",
        default=False,
        show_default=True,
        help="Use optimized queries when they are available.",
    )
    @click.option(
        "--query-source",
        type=click.Choice(list(QuerySource)),
        default="resources",
        show_default=True,
        help="Where to load the queries from: resfs or filesystem."
    )
    @click.option(
        "--query-path",
        type=str,
        default="queries",
        show_default=True,
        help="Path to load the queries from."
    )
    @click.option(
        "--optimized-path",
        type=str,
        default="queries_optimized",
        show_default=True,
        help="Path to load the optimized queries from."
    )
    @click.option(
        "--proxy",
        envvar="YT_PROXY",
        required=True,
        help="YT cluster where the queries should be executed.",
    )
    @click.option(
        "-p",
        "--pragma-add",
        multiple=True,
        default=None,
        help="Adds a single SQL pragma statement directly. This option can be repeated to include multiple pragmas.",
    )
    @click.option(
        "--pragma-file",
        help="Loads pragma settings from a specified file.",
        type=click.Path(exists=True, dir_okay=False, readable=True),
    )
    @click.option(
        "--pragma-preset",
        multiple=True,
        default=["default"],
        show_default=True,
        type=click.Choice(list_all_pragma_presets(), case_sensitive=False),
        help="Add a predefined set of pragma configurations from built-in preset.",
    )
    @click.option(
        "--poller-interval",
        default="500ms",
        show_default=True,
    )
    @click.option(
        "--timeout", type=DurationType(), default="30m", show_default=True, help="Query execution time limit."
    )
    @click.option(
        "--artifact-path",
        type=click.Path(file_okay=False, writable=True),
        help="Path to save artifacts into.",
    )
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


@click.group()
def cli():
    """Run TPC-DS benchmark queries."""
