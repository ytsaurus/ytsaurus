import click
import functools
import json
import logging

from importlib import resources
from importlib.resources.abc import Traversable
from pathlib import Path
from datetime import datetime
from typing import Any
from enum import StrEnum

from yt.yson import YsonType, yson_to_json

################################################################################

ROOT_RESOURCE = resources.files(__package__)


class QuerySource(StrEnum):
    RESOURCES = "resources"
    FILESYSTEM = "filesystem"


def root_path(query_source: QuerySource) -> Path | Traversable:
    return ROOT_RESOURCE if query_source == QuerySource.RESOURCES else Path()


class RunnableQuery:
    index: int
    hand_optimized: bool
    pragmas: str

    def __init__(self, index: int, hand_optimized: bool, pragmas: str):
        self.index = index
        self.hand_optimized = hand_optimized
        self.pragmas = pragmas

    def get_title(self, prefix: str = ""):
        result = prefix + f"TPC-DS {self.index:02d}"
        if self.hand_optimized:
            result = result + " (hand-optimized)"
        return result


def combine_pragma_settings(
    default_pragmas_text: str,
    pragma_add: list[str],
    pragma_file: str | None,
    pragma_preset: list[str]
) -> str:
    result = default_pragmas_text
    if pragma_file:
        with open(pragma_file, "r") as file:
            result = file.read()

    if pragma_preset:
        result = ""
        for preset in pragma_preset:
            result += (ROOT_RESOURCE / "pragmas" / f"{preset}.sql").read_text() + "\n"

    for pragma in pragma_add:
        result += "pragma " + pragma + ";\n"

    return result


def get_runnable_queries(
    queries: list[int] | None,
    use_hand_optimized: bool,
    query_path: str,
    optimized_path: str,
    query_source: QuerySource,
    pragma_add: list[str],
    pragma_file: str | None,
    pragma_preset: list[str]
) -> list[RunnableQuery]:

    original_queries = list_all_queries(query_path, query_source)
    original_pragmas = combine_pragma_settings(
        (ROOT_RESOURCE / "pragmas" / "default.sql").read_text() + "\n",
        pragma_add,
        pragma_file,
        pragma_preset
    )
    original_set = set(original_queries)

    if not queries:
        queries = original_queries
    else:
        unknown_queries = set(queries).difference(original_queries)
        if unknown_queries:
            logging.warning(f"Unknown queries {unknown_queries}")

    if not use_hand_optimized:
        return [RunnableQuery(num, False, original_pragmas) for num in queries if num in original_set]

    # Override the original queries with hand-optimized when present.

    optimized_queries = list_all_queries(optimized_path, query_source)
    optimized_pragmas = combine_pragma_settings(
        (ROOT_RESOURCE / "pragmas" / "optimized.sql").read_text() + "\n",
        pragma_add,
        pragma_file,
        pragma_preset
    )
    optimized_set = set(optimized_queries)

    runset = []
    for num in queries:
        if num not in original_set:
            continue

        optimized = num in optimized_set
        pragma_set = optimized_pragmas if optimized else original_pragmas
        runset.append(RunnableQuery(num, optimized, pragma_set))
    return runset


def make_query(
    runnable: RunnableQuery,
    query_path: str,
    optimized_path: str,
    query_source: QuerySource,
) -> str:
    query_body = None
    query_name = f"{runnable.index:02d}.sql"
    path_root = root_path(query_source)

    if runnable.hand_optimized:
        path = path_root / optimized_path / query_name
    else:
        path = path_root / query_path / query_name

    query_body = path.read_text()
    return runnable.pragmas + "\n" + query_body


def list_all_queries(query_prefix: str, query_source: QuerySource) -> list[int]:
    path = root_path(query_source)
    return [int(query.name.removesuffix(".sql"))
            for query in (path / query_prefix).iterdir()
            if query.name.endswith(".sql")]


def list_all_pragma_presets() -> list[str]:
    return [preset.name.removesuffix(".sql")
            for preset in (ROOT_RESOURCE / "pragmas").iterdir()
            if preset.name.endswith(".sql")]


def nested_get(dic: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        if key not in dic.keys():
            return None
        dic = dic.get(key, None)
    return dic


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


class FullQueryInfo:
    qt_query_info: YsonType | None = None
    yt_operation_info: dict[str, YsonType] = dict()


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
            json.dump(info, f, indent=2)

    def start_query(self, query: RunnableQuery):
        if not self.launch_path:
            return
        query_type = "hand_optimized" if query.hand_optimized else "original"
        self.query_path = self.launch_path / "queries" / query_type / str(query.index)
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

    def dump_info(self, query_info: FullQueryInfo):
        if not self.query_path:
            return
        with (self.query_path / "info.json").open("w") as f:
            json.dump(yson_to_json(query_info.qt_query_info), f, indent=2)
        if query_info.yt_operation_info:
            (self.query_path / "yt").mkdir()
        for id, info in query_info.yt_operation_info.items():
            with (self.query_path / "yt" / f"{id}.json").open("w") as f:
                json.dump(yson_to_json(info), f, indent=2)

    def dump_error(self, error: dict[str, Any]):
        if not self.query_path:
            return
        with (self.query_path / "error.json").open("w") as f:
            json.dump(error, f, indent=2)


################################################################################


def run_options(func):
    @click.option(
        "-q",
        "--queries",
        type=IntList(),
        help="Comma-separated query index (or index range) list (by default all .sql files are loaded)",
    )
    @click.option(
        "--use-hand-optimized/--no-hand-optimized",
        default=True,
        show_default=True,
        help="Override original queries with hand-optimized versions when those are available.",
    )
    @click.option(
        "--query-source",
        type=click.Choice(list(QuerySource)),
        default="resources",
        show_default=True,
        help="Where to load the queries from: resfs or filesystem.",
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
        help="Path to load the optimized queries from.",
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
        default=None,
        type=click.Choice(list_all_pragma_presets(), case_sensitive=False),
        help="Add a predefined set of pragma configurations from built-in preset.",
    )
    @click.option(
        "--poller-interval",
        default="500ms",
        show_default=True,
    )
    @click.option(
        "--timeout", type=DurationType(), default="30m", show_default=True, help="Single query execution time limit."
    )
    @click.option(
        "--launch-timeout", type=DurationType(), default="22h", show_default=True, help="Full launch time limit."
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
