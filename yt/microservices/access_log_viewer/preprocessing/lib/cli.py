import click
from yt.microservices.access_log_viewer.preprocessing.lib.operations import bulk, import_single, clear

DEFAULT_OUTPUT = "//sys/admin/yt-microservices/access_log_viewer"
DEFAULT_TMP = "//sys/admin/yt-microservices/tmp/access-master-log/postprocessing"
DEFAULT_POOL = "yt-microservices"
DEFAULT_NODE_ID_DICT_PATH = "//sys/admin/yt-microservices/node_id_dict"
DEFAULT_MAX_PARALLEL_OPS = 50
DEFAULT_MAX_TABLES_TO_PROCESS = 1
DEFAULT_STORE_FOR = None


@click.group()
def main():
    pass


@main.command("bulk")
@click.option(
    "-c",
    "--cluster",
    required=True,
)
@click.option(
    "-i",
    "--input",
    required=True,
)
@click.option(
    "-o",
    "--output",
    default=DEFAULT_OUTPUT,
)
@click.option(
    "-t",
    "--tmp",
    default=DEFAULT_TMP,
)
@click.option(
    "-n",
    "--max-parallel-ops",
    default=DEFAULT_MAX_PARALLEL_OPS,
)
@click.option(
    "-p",
    "--period",
    type=click.Choice(["30min", "1d"]),
    default=None,
)
@click.option("--pool", default=DEFAULT_POOL)
@click.option(
    "--max-tables",
    type=int,
    default=DEFAULT_MAX_TABLES_TO_PROCESS,
)
@click.option(
    "--store-for",
    type=int,
    default=DEFAULT_STORE_FOR,
)
@click.option(
    "--network-project",
)
@click.option(
    "--node-id-dict-path",
    default=DEFAULT_NODE_ID_DICT_PATH,
)
@click.option(
    "--ignored-user",
    type=str,
    multiple=True,
)
def import_access_log_tables(
    cluster, input, tmp, output, max_parallel_ops, period, pool, max_tables, store_for, network_project, node_id_dict_path, ignored_users
):
    bulk(cluster, input, tmp, output, max_parallel_ops, period, pool, max_tables, store_for, network_project, node_id_dict_path, ignored_users)


@main.command("import")
@click.option(
    "--path",
)
@click.option(
    "-c",
    "--cluster",
    required=True,
)
@click.option(
    "-i",
    "--input",
    required=True,
)
@click.option(
    "-o",
    "--output",
    default=DEFAULT_OUTPUT,
)
@click.option(
    "-t",
    "--tmp",
    default=DEFAULT_TMP,
)
@click.option(
    "-n",
    "--max-parallel-ops",
    default=DEFAULT_MAX_PARALLEL_OPS,
)
@click.option(
    "--pool",
    default=DEFAULT_POOL,
)
@click.option(
    "--network-project",
)
@click.option(
    "--node-id-dict-path",
    default=DEFAULT_NODE_ID_DICT_PATH,
)
@click.option(
    "--ignored-users",
    type=str,
    multiple=True,
)
def import_single_table(cluster, pool, input, tmp, output, max_parallel_ops, path, network_project, node_id_dict_path, ignored_users):
    import_single(cluster, pool, input, tmp, output, max_parallel_ops, path, network_project, node_id_dict_path, ignored_users)


@main.command("clear")
@click.option(
    "-c",
    "--cluster",
    required=True,
)
@click.option(
    "-i",
    "--input",
    required=True,
)
@click.option(
    "-o",
    "--output",
    default=DEFAULT_OUTPUT,
)
@click.option(
    "--store-for",
    default=DEFAULT_STORE_FOR,
    type=int,
)
def clear_excess(cluster, input, output, store_for):
    clear(cluster, input, output, store_for)


if __name__ == '__main__':
    main()
