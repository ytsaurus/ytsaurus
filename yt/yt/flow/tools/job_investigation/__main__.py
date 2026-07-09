import argparse
import logging
import math
import datetime
import os
import pandas as pd
import sys
import yt.wrapper as yt

from yt.yt.flow.library.python.client.flow_view import get_flow_view

from copy import deepcopy
from dataclasses import dataclass

from pandas.api.types import is_numeric_dtype, is_float_dtype

from yt.ypath.rich import RichYPath

# Print logs to stdout, because we often want to save logs and result table in one file.
stdout_logger = logging.getLogger("job_investigation")


def setup_logging(args):
    stdout_logger.handlers.clear()
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(message)s"))
    stdout_logger.addHandler(handler)

    if args.verbose:
        stdout_logger.setLevel(logging.DEBUG)
        logging.setLevel(logging.DEBUG)
    else:
        stdout_logger.setLevel(logging.INFO)


def parse_args():
    argparser = argparse.ArgumentParser()
    argparser.add_argument(
        "--input",
        type=str,
        required=True,
        help="`cluster://path` address of pipeline",
    )
    argparser.add_argument("--computation", type=str, required=False, default="*")
    argparser.add_argument("--sort-by", type=str, required=False, default="messages_per_second")
    argparser.add_argument("--limit", type=int, required=False, default=1000000)
    argparser.add_argument(
        "--group-limit",
        type=int,
        required=False,
        default=1e9,
        help="How many jobs or job-streams to take from one computation or computation-stream",
    )
    argparser.add_argument("--group-by-host", action="store_true", help="Group by host")
    argparser.add_argument("--no-heavy-hitters", action="store_true", help="Exclude heavy hitters")
    argparser.add_argument(
        "--check-is-full",
        action="store_true",
        help="If gathered info is incomplete, this fact is logged and exit status is not 0",
    )
    argparser.add_argument("--watch-iterations", type=int, required=False, default=1)
    argparser.add_argument(
        "--timeout",
        type=float,
        default=60,
        help="Per-request timeout in seconds for YT calls (get_flow_view etc.). "
        "The default RPC timeout is too low for heavy pipelines.",
    )
    argparser.add_argument("--verbose", action="store_true", help="Print more logs")
    args = argparser.parse_args()

    assert not (
        args.group_by_host and args.watch_iterations != 1
    ), "--watch-iterations can not be used with --group-by-host"

    args.pipeline_path, path_attributes = RichYPath().parse(args.input)
    args.cluster_name = path_attributes["cluster"]

    return args


@dataclass
class Data:
    jobs: pd.DataFrame
    jobs_source_streams: pd.DataFrame
    jobs_input_streams: pd.DataFrame
    jobs_output_streams: pd.DataFrame


@dataclass
class BufferMetrics:
    max_fill_rate: float = 0.0
    used_bytes: int = 0
    limit_bytes: int = 0


def make_unique_stream_ids(spec):
    spec = deepcopy(spec)
    computation_specs = spec["computations"]

    for computation_id, computation_spec in computation_specs.items():
        external_streams = set(computation_spec["input_stream_ids"] + computation_spec["output_stream_ids"])

        def get_full_id(stream_id):
            if stream_id in external_streams:
                return stream_id
            return f"{computation_id}.{stream_id}"

        streams_dependency = {}
        for stream_id, dependencies in computation_spec["streams_dependency"].items():
            streams_dependency[get_full_id(stream_id)] = [
                get_full_id(dependency_stream_id) for dependency_stream_id in dependencies
            ]
        computation_spec["streams_dependency"] = streams_dependency

        computation_spec["timer_streams"] = {
            get_full_id(stream_id): stream_spec for stream_id, stream_spec in computation_spec["timer_streams"].items()
        }

        computation_spec["source_streams"] = {
            get_full_id(stream_id): stream_spec for stream_id, stream_spec in computation_spec["source_streams"].items()
        }

    return spec


def timestamp_to_iso8601(timestamp):
    return datetime.datetime.fromtimestamp(timestamp, datetime.UTC).isoformat() + "Z"


def get_data(args):
    yt_client_config = yt.default_config.get_config_from_env()
    yt_client_config["backend"] = "rpc"
    yt_client = yt.YtClient(proxy=args.cluster_name, config=yt_client_config)
    # Raise the per-request timeout; the default RPC timeout is too low for heavy pipelines.
    yt.config.set_command_param("timeout", int(args.timeout * 1000), yt_client)
    flow_view = get_flow_view(yt_client, args.pipeline_path)

    is_full = True

    raw_computation_specs = flow_view["current_spec"]["value"]["computations"]
    computation_specs = make_unique_stream_ids(flow_view["current_spec"]["value"])["computations"]
    computations = list(computation_specs.keys())

    # For each computation, build a mapping: local_source_stream_id -> global_source_stream_id.
    # from_partition_traverse_data uses local ids; computation_specs uses global (prefixed) ids.
    computation_local_to_global_source = {}
    for computation_id, raw_spec in raw_computation_specs.items():
        external_streams = set(raw_spec["input_stream_ids"] + raw_spec["output_stream_ids"])
        local_to_global = {}
        for local_stream_id in raw_spec.get("source_streams", {}).keys():
            if local_stream_id in external_streams:
                global_stream_id = local_stream_id
            else:
                global_stream_id = f"{computation_id}.{local_stream_id}"
            local_to_global[local_stream_id] = global_stream_id
        computation_local_to_global_source[computation_id] = local_to_global
    assert (
        args.computation == "*" or args.computation in computations
    ), f"Computation {args.computation} not found in {computations}"

    stream_order = {}
    stream_order_in_progress = set()

    def generate_description(partition):
        if "lower_key" in partition and "upper_key" in partition:
            return f"KeyRange: {partition['lower_key']}-{partition['upper_key']}"
        if "source_key" in partition:
            return f"SourceKey: {partition['source_key']}"
        else:
            return "Empty"

    def stream_order_dfs(stream_id):
        if stream_id in stream_order:
            return
        assert stream_id not in stream_order_in_progress
        stream_order_in_progress.add(stream_id)

        for computation_id, computation_spec in computation_specs.items():
            for next_stream_id in computation_spec["output_stream_ids"] + list(
                computation_spec["timer_streams"].keys()
            ):
                if stream_id in computation_specs[computation_id]["streams_dependency"][next_stream_id]:
                    stream_order_dfs(next_stream_id)

        stream_order_in_progress.remove(stream_id)
        stream_order[stream_id] = len(stream_order)

    for computation_id, computation_spec in computation_specs.items():
        for stream_id in (
            computation_spec["input_stream_ids"]
            + computation_spec["output_stream_ids"]
            + list(computation_spec["timer_streams"].keys())
        ):
            stream_order_dfs(stream_id)

    partition_job_statuses = flow_view["feedback"]["partition_job_statuses"]
    layout = flow_view["state"]["execution_spec"]["layout"]
    partitions = layout["partitions"]
    jobs = layout["jobs"]

    jobs_data = {}
    source_stream_data = {}
    input_stream_data = {}
    output_stream_data = {}
    jobs_count = 0
    for partition in partitions.values():
        if partition["state"] != "executing" or "current_job_id" not in partition:
            continue
        jobs_count += 1

        job_id = partition["current_job_id"]

        partition_job_status = partition_job_statuses.get(partition["partition_id"])
        if partition_job_status is None:
            continue
        if job_id != partition_job_status.get("current_job_id"):
            continue
        status = partition_job_status.get("current_job_status")
        if status is None:
            continue

        worker_address = jobs[job_id]["worker_address"]
        performance_metrics = status["performance_metrics"]
        worker = worker_address.rsplit(":", 1)[0]  # Cut port.

        message_lag = 0
        not_drained_streams = 0

        for stream_id, stream in (
            status.get("from_partition_traverse_data", {}).get("node", {}).get("streams", {}).items()
        ):
            stream_message_lag = stream["inflight_metrics"]["count"]
            stream_not_drained = int(stream["state"] != "drained")

            message_lag += stream_message_lag
            not_drained_streams += stream_not_drained

            stream_data = {
                "state": stream["state"],
                "system_watermark": timestamp_to_iso8601(stream["system_watermark"]),
                "event_watermark": timestamp_to_iso8601(stream["event_watermark"]),
            }

            computation_id = partition["computation_id"]
            computation_spec = computation_specs[computation_id]
            local_to_global_source = computation_local_to_global_source[computation_id]
            if stream_id in local_to_global_source:
                global_stream_id = local_to_global_source[stream_id]
                source_stream_data.setdefault((job_id, global_stream_id), {}).update(
                    {
                        **stream_data,
                        "lag_count": stream["inflight_metrics"]["count"],
                        "lag_bytes": stream["inflight_metrics"].get("byte_size", pd.NA),
                    }
                )
            elif stream_id in computation_spec["input_stream_ids"]:
                input_stream_data.setdefault((job_id, stream_id), {}).update(stream_data)
            elif stream_id in computation_spec["output_stream_ids"]:
                output_stream_data.setdefault((job_id, stream_id), {}).update(stream_data)

        global_input_metrics = status["input_metrics"]["global"]
        input_metrics = {
            "messages_per_second": global_input_metrics.get("messages_per_second", pd.NA),
        }
        if not args.no_heavy_hitters:
            input_metrics["heavy_hitters"] = str(global_input_metrics.get("heavy_hitters", {}))

        for stream, stream_metrics in status["input_metrics"]["streams"].items():
            input_job_stream_data = input_stream_data.setdefault((job_id, stream), {})
            input_job_stream_data["messages_per_second"] = stream_metrics.get("messages_per_second", pd.NA)
            if not args.no_heavy_hitters:
                input_metrics["heavy_hitters"] = str(stream_metrics.get("heavy_hitters", {}))

        # Fill buffer metrics from flow_view input_limits / output_limits.
        # input_limits and output_limits are maps: buffer_type -> stream_id -> {limit, used, pending}.
        def fill_buffer_metrics_from_limits(limits: dict, stream_data_dict: dict) -> BufferMetrics:
            result = BufferMetrics()
            for stream_id, limit_status in limits.items():
                used = limit_status.get("used", 0)
                limit = limit_status.get("limit", 0)
                fill_rate = used / max(limit, 1)
                result.max_fill_rate = max(result.max_fill_rate, fill_rate)
                result.used_bytes += used
                result.limit_bytes += limit
                stream_data_dict.setdefault((job_id, stream_id), {}).update(
                    {
                        "buffer_fill_rate": fill_rate,
                        "buffer_used_bytes": used,
                        "buffer_limit_bytes": limit,
                    }
                )
            return result

        input_buffer_metrics = fill_buffer_metrics_from_limits(
            status.get("input_limits", {}).get("input_buffer", {}), input_stream_data
        )
        output_buffer_metrics = fill_buffer_metrics_from_limits(
            status.get("output_limits", {}).get("output_buffer", {}), output_stream_data
        )

        # Fill epoch part times from flow_view, normalized so that sum == 1 per job.
        raw_epoch_part_times = status.get("epoch_part_times", {})
        total_epoch_time = sum(raw_epoch_part_times.values()) or 1
        epoch_part_metrics = {
            f"ept.{part_name}": part_wall_time / total_epoch_time
            for part_name, part_wall_time in raw_epoch_part_times.items()
        }

        stdout_logger.debug("Find job %s in partitions", job_id)
        jobs_data[job_id] = {
            "job_id": job_id,
            "worker_address": worker,
            "partition_id": partition["partition_id"],
            "computation_id": partition["computation_id"],
            "partition_description": generate_description(partition),
            "epoch": status["epoch"],
            "not_drained_streams": not_drained_streams,
            "message_lag": message_lag,
            **input_metrics,
            **performance_metrics,
            "input_buffer_max_fill_rate": input_buffer_metrics.max_fill_rate,
            "input_buffer_used_bytes": input_buffer_metrics.used_bytes,
            "input_buffer_limit_bytes": input_buffer_metrics.limit_bytes,
            "output_buffer_max_fill_rate": output_buffer_metrics.max_fill_rate,
            "output_buffer_used_bytes": output_buffer_metrics.used_bytes,
            "output_buffer_limit_bytes": output_buffer_metrics.limit_bytes,
            **epoch_part_metrics,
        }

    is_full = is_full and (len(jobs_data) == jobs_count)

    def nan_to_zero(value):
        return value if not pd.isna(value) else 0

    def make_jobs_stream_df(stream_data):
        rows = []
        for (job_id, stream_id), data in stream_data.items():
            job = jobs_data[job_id]
            rows.append(
                {
                    "computation_id": job["computation_id"],
                    "stream_id": stream_id,
                    "job_id": job_id,
                    "worker_address": job["worker_address"],
                    **data,
                }
            )

        rows.sort(
            key=lambda x: (
                stream_order.get(x["stream_id"], 0),
                x["computation_id"],
                -nan_to_zero(x.get("buffer_fill_rate", 0)),
            )
        )
        return pd.DataFrame.from_records(rows)

    assert len(jobs_data) > 0, "No jobs found - can not provide any info"
    return (
        Data(
            jobs=pd.DataFrame.from_records(list(jobs_data.values()), index="job_id"),
            jobs_source_streams=make_jobs_stream_df(source_stream_data),
            jobs_input_streams=make_jobs_stream_df(input_stream_data),
            jobs_output_streams=make_jobs_stream_df(output_stream_data),
        ),
        is_full,
    )


# Make output more compact and readable.
def smart_round(f):
    if pd.isna(f):
        return f
    if f == 0:
        return 0
    order = math.floor(math.log10(abs(f)))
    factor = 10 ** (order - 2)
    if factor > 1:
        return round(f)
    return round(f / factor) * factor


def main():
    args = parse_args()
    setup_logging(args)

    stdout_logger.info("Run job investigation tool with arguments: %s", args)

    # Obtain data.
    is_full = True
    datas = []
    for i in range(args.watch_iterations):
        data, local_is_full = get_data(args)
        datas.append(data)
        is_full = is_full and local_is_full
    jobs_dfs = [data.jobs for data in datas]
    assert len(jobs_dfs) > 0, "Invariant is broken"

    assert args.sort_by in jobs_dfs[0].columns, f"Column '{args.sort_by}' not found in {list(jobs_dfs[0].columns)}"

    if len(jobs_dfs) > 1:
        string_columns = ["worker_address", "partition_id", "computation_id"]
        jobs_df_grouped = pd.concat(jobs_dfs).groupby(["job_id"] + string_columns)
        numeric_aggregation_functions = ["min", "mean", "max"]
        aggregation_functions = {}
        for column in jobs_dfs[0].columns:
            if is_numeric_dtype(jobs_dfs[0][column]):
                aggregation_functions[column] = numeric_aggregation_functions
        jobs_df = jobs_df_grouped.agg(aggregation_functions)
        for column in reversed(string_columns):
            jobs_df = jobs_df.reset_index(level=column)
        sort_by_columns = [(args.sort_by, f) for f in numeric_aggregation_functions]
    else:
        jobs_df = jobs_dfs[0]
        sort_by_columns = [args.sort_by]

    jobs_source_streams_df = datas[0].jobs_source_streams
    jobs_input_streams_df = datas[0].jobs_input_streams
    jobs_output_streams_df = datas[0].jobs_output_streams

    # Filter.
    if args.computation != "*":
        old_count = len(jobs_df)
        jobs_df = jobs_df[jobs_df["computation_id"] == args.computation]
        if len(jobs_source_streams_df) != 0:
            jobs_source_streams_df = jobs_source_streams_df[
                jobs_source_streams_df["computation_id"] == args.computation
            ]
        jobs_input_streams_df = jobs_input_streams_df[jobs_input_streams_df["computation_id"] == args.computation]
        if len(jobs_output_streams_df) != 0:
            jobs_output_streams_df = jobs_output_streams_df[
                jobs_output_streams_df["computation_id"] == args.computation
            ]
        stdout_logger.info(
            "Removed %d jobs with computation filter. Jobs left: %d", old_count - len(jobs_df), len(jobs_df)
        )

    old_count = len(jobs_df)
    jobs_df = jobs_df.dropna(subset=sort_by_columns)
    stdout_logger.info(
        "Removed %d jobs because of nans in sort_by column. Jobs left: %d", old_count - len(jobs_df), len(jobs_df)
    )

    # Group by host if needed.
    if args.group_by_host:
        jobs_df = jobs_df.drop(["partition_id", "computation_id"], axis=1)
        jobs_df = jobs_df.groupby("worker_address").sum()

    # Output.
    jobs_df = jobs_df[sort_by_columns + [column for column in jobs_df.columns if column not in sort_by_columns]]
    jobs_df = jobs_df.sort_values(sort_by_columns[-1], ascending=False)  # Only column or MAX.

    def apply_group_limit(df, group_key):
        counts = {}
        mask = []
        for i, row in df.iterrows():
            key = tuple(row[group_key])
            count = counts.get(key, 0) + 1
            mask.append(count <= args.group_limit)
            counts[key] = count
        return df[mask]

    if not args.group_by_host:
        jobs_df = apply_group_limit(jobs_df, ["computation_id"])
    jobs_source_streams_df = apply_group_limit(jobs_source_streams_df, ["computation_id", "stream_id"])
    jobs_input_streams_df = apply_group_limit(jobs_input_streams_df, ["computation_id", "stream_id"])
    jobs_output_streams_df = apply_group_limit(jobs_output_streams_df, ["computation_id", "stream_id"])

    for column in list(jobs_df.columns):
        if is_float_dtype(jobs_df[column]):
            jobs_df[column] = jobs_df[column].apply(smart_round)

    max_width = 10000
    if sys.stdout.isatty():
        max_width, _ = os.get_terminal_size(1)

    print("=============== JOBS ===============")
    print(jobs_df.to_string(max_rows=args.limit, line_width=max_width))
    print("=============== JOBS SOURCE STREAMS ===============")
    print(jobs_source_streams_df.to_string(index=False, max_rows=args.limit, line_width=max_width))
    print("=============== JOBS INPUT STREAMS ===============")
    print(jobs_input_streams_df.to_string(index=False, max_rows=args.limit, line_width=max_width))
    print("=============== JOBS OUTPUT STREAMS ===============")
    print(jobs_output_streams_df.to_string(index=False, max_rows=args.limit, line_width=max_width))

    if args.check_is_full and not is_full:
        logging.error("Graph is not full")
        return 1
    return 0


if __name__ == "__main__":
    exit(main())
