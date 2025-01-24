import click
import sys
import time
import traceback
from pathlib import Path

from yt.common import YtResponseError
from yt.wrapper import YtClient
from yt.yson import YsonType

from . import common


def get_query_with_retries(client: YtClient, query_id: str) -> YsonType:
    result = None
    num_retries = 0
    MAX_RETRIES = 30
    while result is None and num_retries < MAX_RETRIES:
        num_retries += 1
        try:
            result = client.get_query(query_id)
        except YtResponseError as e:
            if e.contains_text("is not found"):
                time.sleep(60)
            else:
                raise
    if result is None:
        raise RuntimeError(f"Query {query_id} failed after {MAX_RETRIES} retries")
    return result


def fetch_full_query_info(client: YtClient, query_id: str, with_operation_info: bool) -> common.FullQueryInfo:
    result = common.FullQueryInfo()
    result.qt_query_info = get_query_with_retries(client, query_id)
    if not with_operation_info:
        return result
    yql_progress = common.nested_get(result.qt_query_info, ["progress", "yql_progress"])
    if yql_progress is None:
        return result
    for (id, status) in yql_progress.items():
        if not status.get("remoteId"):
            continue
        operation_id = status["remoteId"].rsplit("/", 1)[1]
        if not operation_id:
            continue
        operation_info = client.get_operation(operation_id)
        result.yt_operation_info[id] = operation_info
    return result


@common.cli.command()
@click.option("--stage", default="production", show_default=True, help="Stage of YQL agent.")
@click.option(
    "--token",
    envvar="YT_TOKEN",
    help="YT token. Fetched from file ~/.yt/token or from env var YT_TOKEN by default. "
    "See https://ytsaurus.tech/docs/user-guide/storage/auth for more information on how to get your token.",
)
@click.option(
    "--with-operation-artifacts/--no-operation-artifacts",
    is_flag=True,
    default=False,
    show_default=True,
    help="Store inner operations info in the artifacts. Can increase the total artifact size by a lot."
)
@click.option("--title-prefix", default="[QT] ", show_default=True, help="Title prefix of YQL query on query tracker")
@common.run_options
def qt(
    queries: list[int] | None,
    use_hand_optimized: bool,
    query_path: str,
    optimized_path: str,
    query_source: common.QuerySource,
    proxy: str,
    pragma_add: list[str],
    pragma_file: str | None,
    pragma_preset: list[str],
    poller_interval: str,
    stage: str,
    token: str | None,
    timeout: int,  # seconds
    launch_timeout: int,  # seconds
    artifact_path: str | None,
    with_operation_artifacts: bool,
    title_prefix: str,
) -> None:
    """Run TPC-DS benchmark queries using the Query Tracker."""

    arguments = locals()

    if token is None:
        try:
            with open(Path.home() / ".yt" / "token", "r") as f:
                token = f.read().strip()
        except FileNotFoundError:
            pass

    if token is None:
        raise RuntimeError("YT token is not specified")

    runnable_queries = common.get_runnable_queries(queries, use_hand_optimized, query_path, optimized_path, query_source, pragma_add, pragma_file, pragma_preset)

    with common.ArtifactLogger(artifact_path) as logger:
        logger.dump_launch(arguments)

        client = YtClient(proxy=proxy, token=token)
        launch_start = time.time()
        for runnable in runnable_queries:
            if time.time() - launch_start >= launch_timeout:
                raise TimeoutError(f"Exceeded launch timeout of {launch_timeout} seconds, stopping at query {runnable.index}")
            query_id = None
            query_title = ""
            try:
                logger.start_query(runnable)
                query = common.make_query(runnable, query_path, optimized_path, query_source)
                logger.dump_query(query)

                settings = {
                    "stage": stage,
                    "poller_interval": poller_interval,
                }

                query_title = runnable.get_title(title_prefix)
                query_id = client.start_query(
                    engine="yql",
                    query=query,
                    settings=settings,
                    annotations={"title": query_title}
                )
                logger.dump_id(query_id)

                query_link = f"https://beta.yt.yandex-team.ru/{proxy}/queries/{query_id}"

                print(query_id, flush=True)
                print(f"Query {query_title} link: {query_link}", file=sys.stderr)

                start_time = time.time()

                state = get_query_with_retries(client, query_id)["state"]
                while state == "pending" or state == "running":
                    if time.time() - start_time >= timeout:
                        client.abort_query(query_id)
                        state = "aborted"
                    else:
                        time.sleep(5)
                        state = get_query_with_retries(client, query_id)["state"]
                print(f"Query {query_title} finished with state: {state}", file=sys.stderr)
                query_info = fetch_full_query_info(client, query_id, with_operation_artifacts)
                logger.dump_info(query_info)
            except Exception as err:
                print(f"Error while running query {query_title}: {err}", file=sys.stderr)
                print(traceback.format_exc(), file=sys.stderr)
                logger.dump_error({
                    "error": str(err),
                    "traceback": traceback.format_exc(),
                })
            except KeyboardInterrupt:
                if query_id is not None:
                    try:
                        client.abort_query(query_id)
                    except YtResponseError:
                        pass
                raise
