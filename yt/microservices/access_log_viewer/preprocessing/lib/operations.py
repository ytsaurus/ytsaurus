import dataclasses
import datetime
import dateutil
import itertools
import logging
import os
import random
import typing

import yt.admin.core.operation_pipeline as aops
from yt.microservices.id_to_path_mapping.client import NodeIdResolver, NodeIdPatcher, get_node_id

import yt.wrapper as yt

logger = logging.getLogger("access-log-processor")

DEDUPLICATION_WINDOW = 15
MAP_MEMORY_LIMIT = 160 * 2**20
REDUCE_CPU_LIMIT = 0.45
REDUCE_MEMORY_LIMIT = 2 * 2**30
MAPREDUCE_JOBS_LIMIT = 500
DATA_WEIGHT_PER_MAP_JOB = 256 * 2**20
DATA_WEIGHT_PER_SORT_JOB = 2 * 2**30
DATA_WEIGHT_PER_REDUCE_JOB = 256 * 2**20
N_GROUPS = 2**10


def row_group(row):
    return hash(row.cluster + (get_node_id(row.path) or row.path)) % N_GROUPS


@yt.yt_dataclass
class RawAccessLogRecord:
    iso_eventtime: typing.Optional[str] = None

    cluster: typing.Optional[str] = None

    path: typing.Optional[str] = None
    original_path: typing.Optional[str] = None

    type: typing.Optional[str] = None
    method: typing.Optional[str] = None
    user: typing.Optional[str] = None

    destination_path: typing.Optional[str] = None
    original_destination_path: typing.Optional[str] = None

    transaction_id: typing.Optional[str] = None
    transaction_info: typing.Optional[yt.schema.YsonBytes] = None


@yt.yt_dataclass
class IntermediateRecord():
    cluster: str = None
    path: str = None
    instant: str = None
    timestamp: int = None

    original_path: typing.Optional[str] = None

    type: typing.Optional[str] = None
    method: typing.Optional[str] = None
    user: typing.Optional[str] = None

    destination_path: typing.Optional[str] = None
    original_destination_path: typing.Optional[str] = None

    transaction_id: typing.Optional[str] = None
    transaction_info: typing.Optional[yt.schema.YsonBytes] = None

    reduce_group: int = 0
    random: float = 0.0

    @classmethod
    def from_raw_record(cls, raw: RawAccessLogRecord):
        if raw.path == raw.original_path:
            raw.original_path = None
        if raw.destination_path == raw.original_destination_path:
            raw.original_destination_path = None
        fields = dataclasses.asdict(raw)
        fields["instant"] = fields["iso_eventtime"]
        fields["timestamp"] = int(dateutil.parser.parse(fields["iso_eventtime"]).timestamp())
        fields.pop("iso_eventtime")
        return cls(
            **fields
        )

    def is_similar(self, other):
        for key in IntermediateRecord.__slots__:
            if key in {"instant", "timestamp", "reduce_group", "random"}:
                continue
            if getattr(self, key) != getattr(other, key):
                return False
        return True

    def expand(self):
        self.reduce_group = row_group(self)
        self.random = random.random()


@yt.yt_dataclass
class OutputRecord:
    path: typing.Optional[str] = None
    instant: typing.Optional[str] = None
    timestamp: int = None

    target_path: typing.Optional[str] = None
    original_path: typing.Optional[str] = None

    destination_path: typing.Optional[str] = None
    original_destination_path: typing.Optional[str] = None

    source_path: typing.Optional[str] = None
    original_source_path: typing.Optional[str] = None

    type: typing.Optional[str] = None
    method: typing.Optional[str] = None
    user: typing.Optional[str] = None

    transaction_info: typing.Optional[yt.schema.YsonBytes] = None
    transaction_id: typing.Optional[str] = None

    @classmethod
    def from_patched(cls, patched: IntermediateRecord):
        patched_dict = dataclasses.asdict(patched)
        patched_dict.pop("reduce_group")
        patched_dict.pop("random")
        patched_dict.pop("cluster")

        return list(
            itertools.chain(
                *map(cls.duplicate_record_around_copy, cls.duplicate_record_around_symlink(cls(**patched_dict)))
            )
        )

    @classmethod
    def duplicate_record_around_symlink(cls, record):
        if record.original_path is not None:
            return (
                record,
                cls(
                    **{
                        **dataclasses.asdict(record),
                        **dict(
                            original_path=None,
                            path=record.original_path,
                            target_path=record.path,
                        ),
                    }
                ),
            )
        return (record,)

    @classmethod
    def duplicate_record_around_copy(cls, record):
        if record.destination_path is not None:
            return (
                record,
                cls(
                    **{
                        **dataclasses.asdict(record),
                        **dict(
                            target_path=None,
                            path=record.destination_path,
                            original_path=record.original_destination_path,
                            destination_path=None,
                            original_destination_path=None,
                            source_path=record.path,
                            original_source_path=record.original_path,
                        ),
                    }
                ),
            )
        return (record,)


class AccessLogDeduplicator(yt.TypedJob):
    def __init__(self, users_to_ignore):
        self.last = None
        self.users_to_ignore = set(users_to_ignore)

    def get_intermediate_stream_count(self):
        return 1

    def prepare_operation(self, context, preparer):
        preparer.input(
            0,
            type=RawAccessLogRecord,
        )
        preparer.output(
            0,
            type=IntermediateRecord,
        )

    def __call__(self, row):
        if row.cluster is None or row.cluster == "":
            return
        if row.path is None or row.path == "":
            return
        if row.method is None or row.method == "Revise":
            return
        if row.user in self.users_to_ignore:
            return
        current = IntermediateRecord.from_raw_record(row)
        if self.last is not None and self.last.is_similar(current):
            if current.timestamp < self.last.timestamp:
                raise Exception("Unordered input stream")
            if current.timestamp - self.last.timestamp < DEDUPLICATION_WINDOW:
                return
        else:
            current.expand()
            self.last = current
            yield current


@yt.reduce_aggregator
class AccessLogReducer(yt.TypedJob):
    def __init__(self, cluster, id_path_resolve_table, cluster_to_idx, logger=logger.getChild("reducer")):
        self._logger = logger
        self.cluster = cluster
        self.id_path_resolve_table = id_path_resolve_table
        self.cluster_to_idx = cluster_to_idx
        self.fields = {field.name for field in dataclasses.fields(OutputRecord)}

    def start(self):
        self.row_count = 0
        self.resolved_from_cache = 0
        self.filtered_as_duplicates = 0

        config = yt.default_config.get_config_from_env()
        config["backend"] = "rpc"
        config["dynamic_table_retries"] = {"count": 3}

        self.client = yt.YtClient(proxy=self.cluster,
                                  token=os.environ.get("YT_SECURE_VAULT_yt_token"),
                                  config=config)
        self.resolver = NodeIdResolver(self.client, self.id_path_resolve_table, logger=self._logger)
        self.patcher = NodeIdPatcher(self.resolver, (
            "path",
            "original_path",
            "destination_path",
            "original_destination_path",
        ), preserve_order=False, logger=self._logger)

    def finish(self):
        yt.write_statistics({"self.row_count": self.row_count, "self.resolved_from_cache": self.resolved_from_cache, "self.filtered_as_duplicates": self.filtered_as_duplicates})

    def prepare_operation(self, context, preparer):
        assert context.get_input_count() == 1

        preparer.input(0, type=IntermediateRecord)
        for idx in range(context.get_output_count()):
            preparer.output(idx, type=OutputRecord)

    def duplicate_row(self, row):
        table_index = self.cluster_to_idx.get(row.cluster)
        if table_index is None:
            return
        yield from (yt.schema.OutputRow(output, table_index=table_index) for output in OutputRecord.from_patched(row))

    def __call__(self, rows_groups):
        try:
            self.start()
            for rows in rows_groups:
                for row in rows:
                    self.row_count += 1
                    yield from itertools.chain(*(self.duplicate_row(patched) for patched in self.patcher.batched_patch(row)))

            yield from itertools.chain(*(self.duplicate_row(patched) for patched in self.patcher.batched_flush()))
        except Exception as e:
            self._logger.exception("Exception goes brrr")
            raise e

        self.finish()


def remove_tables(cluster, token, result_root, excess):
    batch_client = yt.YtClient(cluster, token=token, config=yt.default_config.get_config_from_env()).create_batch_client()
    for table in excess:
        batch_client.remove(f"{result_root}/{'/'.join(table)}")
    batch_client.commit_batch()


def import_table(cluster, network_project, pool, token, log_root, tmp_root, out_root, log_duration, log_name, node_id_dict_path, users_to_ignore):
    if token is None:
        raise Exception("YT_TOKEN is empty")
    client = yt.YtClient(cluster, token=token, config=yt.default_config.get_config_from_env())
    log_dir = log_root
    tmp_dir = tmp_root
    out_dir = out_root

    log_name = f"{log_duration}/{log_name}"
    log_path = f"{log_dir}/{log_name}"

    all_clusters = client.list("//sys/clusters")

    batch_client = client.create_batch_client()
    for root in [tmp_dir, out_dir]:
        for out_cluster in all_clusters:
            batch_client.create(
                "map_node", f"{root}/{out_cluster}/{log_duration}", recursive=True, ignore_existing=True
            )
    batch_client.commit_batch()

    exist_checks = [batch_client.exists(f"{out_dir}/{out_cluster}/{log_name}") for out_cluster in all_clusters]
    batch_client.commit_batch()

    if all([check.get_result() for check in exist_checks]):
        return aops.Function(
            lambda: logger.info("Skipping import of table %s, it already exists", log_path),
            title=f"Skipping import of table {log_path}, it already exists",
        )

    table_schema = {field["name"] for field in client.get("{}/@schema".format(log_path))}
    columns = list(set(RawAccessLogRecord.__dataclass_fields__).intersection(table_schema))
    resulting_schema = yt.schema.TableSchema.from_row_type(OutputRecord)
    op_spec = {"environment": {}}
    pass_env_variables = ["YT_PROXY", "YT_PROXY_URL_ALIASING_CONFIG", "YT_CONFIG_PATCHES"]
    for pass_env_variable in pass_env_variables:
        if os.environ.get(pass_env_variable) is not None:
            op_spec["environment"][pass_env_variable] = os.environ.get(pass_env_variable)

    if network_project is not None:
        if client.exists(f"//sys/network_projects/{network_project}"):
            op_spec["network_project"] = network_project
        else:
            logger.warning("Network project %v does not exist. It will not be added to the spec.", network_project)
    deduplicated_table_path = f"{tmp_dir}/{log_name}.deduplicated"
    deduplicated_sorted_table_path = f"{deduplicated_table_path}.sorted"

    def get_deduplicate_op():
        return aops.AsyncOp(
            client,
            yt.MapSpecBuilder()
            .title(f"Preprocess access-log rows for {log_name}")  # noqa
            .pool(pool)  # noqa
            .ordered(True)
            .data_size_per_job(DATA_WEIGHT_PER_MAP_JOB)
            .use_columnar_statistics(True)
            .input_table_paths([
                yt.TablePath(log_path, columns=columns),
            ])
            .begin_mapper()
                .spec(op_spec)  # noqa: E131
                .command(AccessLogDeduplicator(users_to_ignore=users_to_ignore))
                .memory_limit(MAP_MEMORY_LIMIT)
            .end_mapper()
            .max_failed_job_count(20)
            .output_table_paths(
                deduplicated_table_path
            )
            .secure_vault({"yt_token": token}),
        )

    def get_deduplicated_sort_op():
        return aops.AsyncOp(
            client,
            yt.SortSpecBuilder()
            .pool(pool)
            .title(f"Sort deduplicated access-log for {cluster}/{log_name}")
            .input_table_paths(deduplicated_table_path)
            .use_columnar_statistics(True)
            .sort_by(["reduce_group", "random"])
            .output_table_path(
                yt.TablePath(
                    deduplicated_sorted_table_path, optimize_for="scan"
                )
            )
        )

    def get_reduce_op():
        return aops.AsyncOp(
            client,
            yt.ReduceSpecBuilder()
            .pool(pool)  # noqa
            .title(f"Preprocess access-log rows for {log_name}")  # noqa
            .resource_limits({"user_slots": MAPREDUCE_JOBS_LIMIT})
            .data_size_per_job(DATA_WEIGHT_PER_REDUCE_JOB)
            .use_columnar_statistics(True)
            .input_table_paths(deduplicated_sorted_table_path)
            .reduce_by(["reduce_group", "random"])
            .begin_reducer()
                .spec(op_spec)  # noqa: E131
                .command(
                    AccessLogReducer(
                        cluster,
                        node_id_dict_path,
                        {out_cluster: idx for idx, out_cluster in enumerate(all_clusters)}
                    )
                )
                .cpu_limit(REDUCE_CPU_LIMIT)  # noqa: E131
                .memory_limit(REDUCE_MEMORY_LIMIT)   # noqa: E131
            .end_reducer()
            .max_failed_job_count(20)
            .output_table_paths([
                f"{tmp_dir}/{cluster}/{log_name}" for cluster in all_clusters
            ])
            .secure_vault({"yt_token": token}),
        )

    def get_sortreduce_op():
        return aops.AsyncOp(
            client,
            yt.MapReduceSpecBuilder()
            .title(f"Preprocess access-log rows for {log_name}")
            .pool(pool)  # noqa
            .input_table_paths(deduplicated_table_path)
            .resource_limits({"user_slots": MAPREDUCE_JOBS_LIMIT})
            .data_size_per_sort_job(DATA_WEIGHT_PER_SORT_JOB)
            .data_size_per_reduce_job(DATA_WEIGHT_PER_REDUCE_JOB)
            .use_columnar_statistics(True)
            .reduce_by(["reduce_group", "random"])
            .begin_reducer()
                .spec(op_spec)  # noqa: E131
                .command(
                    AccessLogReducer(
                        cluster,
                        node_id_dict_path,
                        {out_cluster: idx for idx, out_cluster in enumerate(all_clusters)}
                    )
                )
                .cpu_limit(REDUCE_CPU_LIMIT)  # noqa: E131
                .memory_limit(REDUCE_MEMORY_LIMIT)  # noqa: E131
            .end_reducer()
            .max_failed_job_count(20)
            .output_table_paths([
                f"{tmp_dir}/{cluster}/{log_name}" for cluster in all_clusters
            ])
            .secure_vault({"yt_token": token}),
        )

    def get_mapreduce_op():
        return aops.AsyncOp(
            client,
            yt.MapReduceSpecBuilder()
            .title(f"Preprocess access-log rows for {log_name}")  # noqa
            .pool(pool)  # noqa
            .input_table_paths([
                yt.TablePath(log_path, columns=columns),
            ])
            .map_selectivity_factor(0.1)
            .ordered(True)
            .resource_limits({"user_slots": MAPREDUCE_JOBS_LIMIT})
            .use_columnar_statistics(True)
            .data_size_per_map_job(DATA_WEIGHT_PER_MAP_JOB)
            .data_size_per_sort_job(DATA_WEIGHT_PER_SORT_JOB)
            .partition_data_size(DATA_WEIGHT_PER_SORT_JOB)
            .data_size_per_reduce_job(DATA_WEIGHT_PER_REDUCE_JOB)
            .begin_mapper()
                .spec(op_spec)  # noqa: E131
                .command(AccessLogDeduplicator(users_to_ignore=users_to_ignore))
                .memory_limit(MAP_MEMORY_LIMIT)
            .end_mapper()
            .reduce_by(["reduce_group", "random"])
            .begin_reducer()
                .spec(op_spec)
                .command(
                    AccessLogReducer(
                        cluster,
                        node_id_dict_path,
                        {out_cluster: idx for idx, out_cluster in enumerate(all_clusters)}
                    )
                )
                .cpu_limit(REDUCE_CPU_LIMIT)  # noqa: E131
                .memory_limit(REDUCE_MEMORY_LIMIT)  # noqa: E131
            .end_reducer()
            .max_failed_job_count(20)
            .output_table_paths([
                f"{tmp_dir}/{cluster}/{log_name}" for cluster in all_clusters
            ])
            .secure_vault({"yt_token": token}),
        )

    def get_sort_result_op():
        return aops.Parallel(
            *(
                aops.AsyncOp(
                    client,
                    yt.SortSpecBuilder()
                    .pool(pool)
                    .title(f"Sort access-log rows for {cluster}/{log_name}")
                    .input_table_paths(f"{tmp_dir}/{cluster}/{log_name}")
                    .sort_by(["path", "instant"])
                    .output_table_path(
                        yt.TablePath(
                            f"{out_dir}/{cluster}/{log_name}", schema=resulting_schema, optimize_for="scan"
                        )
                    )
                    .begin_merge_job_io()
                    .table_writer({"desired_chunk_weight": 1024 * 1024 * 1024})
                    .end_merge_job_io(),
                )
                for cluster in all_clusters
            ),
            title="Sorting access-logs for all clusters",
        )

    def get_v1_ops():
        return (get_deduplicate_op(), get_deduplicated_sort_op(), get_reduce_op(), get_sort_result_op(),)

    def get_v2_ops():
        return (get_deduplicate_op(), get_sortreduce_op(), get_sort_result_op(),)

    def get_v3_ops():
        return (get_mapreduce_op(), get_sort_result_op(),)

    return aops.Workspace(
        yt_client=client,
        paths=[f"{out_dir}/{cluster}/{log_name}" for cluster in all_clusters],
        remove=False,
        gen=aops.Workspace(
            yt_client=client,
            paths=[f"{tmp_dir}/{cluster}/{log_name}" for cluster in all_clusters] + [deduplicated_table_path, deduplicated_sorted_table_path,],
            remove=True,
            gen=aops.Chain(
                *get_v2_ops(),
                title=f"Import chain for access-log: {log_name}",
            ),
            skip_on_lock_error=True,
            title=f"Lock on temporary table: {tmp_root}/{log_name}",
        ),
        skip_on_lock_error=True,
        title=f"Lock on output table: {out_root}/{log_name}",
    )


def find_diff(cluster, token, log_root, res_root, period, store_from):
    """
    :param cluster: YT cluster
    :param token: YT token
    :param log_root: map_node with logs
    :param res_root: map_node in which results must be stored
    :param period: None, 1d or 30min to select which part of logs to process
    :param store_from: None, or string in same form as 1d log names
    :return: missing tables, excess tables
    """

    def path_filter(path, period, store_from=None):
        if period is None:
            return True
        if store_from is not None:
            table_date = path.rsplit("/", 1)[-1]
            if "T" not in table_date:
                if table_date < store_from:
                    return False
        return path.rsplit("/", 2)[-2] == period

    def list_log_tables(root):
        return (
            set(
                tuple(path.rsplit("/", 2)[1:])
                for path in yt_client.search(
                    root, node_type=["table"], path_filter=lambda path: path_filter(path, period, store_from)
                )
            )
            if yt_client.exists(root)
            else set()
        )

    def list_res_tables(root, clusters):
        tables_per_cluster = {cluster: set() for cluster in clusters}
        logs = [
            tuple(path.rsplit("/", 3)[-3:])
            for path in yt_client.search(
                root, node_type=["table"], path_filter=lambda path: path_filter(path, period)
            )
        ] if yt_client.exists(root) else set()
        for cluster, log_len, log_time in logs:
            if cluster in tables_per_cluster:
                tables_per_cluster[cluster].add((log_len, log_time))
        return tables_per_cluster

    yt_client = yt.YtClient(cluster, token=token, config=yt.default_config.get_config_from_env())
    clusters = yt_client.list("//sys/clusters")

    log_tables = list_log_tables(log_root)
    res_tables = list_res_tables(res_root, clusters)

    excess = []
    missing = set()
    for cluster_from_table, tables in res_tables.items():
        excess.extend((cluster_from_table, *table) for table in tables.difference(log_tables))
    #     missing.update(log_tables.difference(tables))
    missing.update(log_tables.difference(res_tables[cluster]))
    return sorted(missing), sorted(excess)


def bulk(cluster, log_root, tmp_root, result_root, n_threads, period, pool, max_tables, store_for, network_project, node_id_dict_path, users_to_ignore, token_env_variable):
    store_from = None
    if store_for is not None:
        store_from = (datetime.datetime.now() + dateutil.relativedelta(months=-6)).strftime("%Y-%m-%d")
    logger.addHandler(logging.StreamHandler())
    logger.level = logging.DEBUG
    token = os.environ.get(token_env_variable)

    new, excess = find_diff(cluster, token, log_root, result_root, period, store_from)
    logger.info("New version")
    logger.info("Processing access-logs src: %s -> tmp: %s -> dst: %s.", log_root, tmp_root, result_root)
    logger.info("Missing in dst:\n%s", "\n".join(f"    {str(item)}" for item in new))
    logger.info("Excess in dst:\n%s", "\n".join(f"    {str(item)}" for item in excess))
    new = new[:max_tables]
    logger.info("Process:\n%s", "\n".join(f"    {str(item)}" for item in new))
    remove_tables(cluster, token, result_root, excess)
    aops.PipelineWatcher(
        aops.Parallel(
            *(
                import_table(cluster, network_project, pool, token, log_root, tmp_root, result_root, *path, node_id_dict_path, users_to_ignore)
                for path in new
            ),
            title="Import new access-logs.",
        ),
        logger,
        max_concurrent_operations=n_threads,
    ).wait(abort_concurrent_on_fail=False)


def import_single(cluster, pool, log_root, tmp_root, result_root, n_threads, path, network_project, node_id_dict_path, users_to_ignore, token_env_variable):
    logger.addHandler(logging.StreamHandler())
    logger.level = logging.DEBUG
    token = os.environ.get(token_env_variable)
    aops.PipelineWatcher(
        import_table(
            cluster, network_project, pool, token, log_root, tmp_root, result_root, *(path.rsplit("/", 2)[-2:]), node_id_dict_path, users_to_ignore
        ),
        logger,
        max_concurrent_operations=n_threads,
    ).wait(abort_concurrent_on_fail=False)


def clear(cluster, log_root, result_root, store_from, token_env_variable):
    token = os.environ.get(token_env_variable)
    _, excess = find_diff(cluster, token, log_root, result_root, None, store_from)
    remove_tables(cluster, token, result_root, excess)
