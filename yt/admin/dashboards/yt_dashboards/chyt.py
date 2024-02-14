# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent

from .common.sensors import *

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.specific_tags.tags import TemplateTag

from yt_dashboard_generator.sensor import MultiSensor, Text

from yt_dashboard_generator.backends.monitoring import MonitoringLabelDashboardParameter, MonitoringExpr


def build_chyt_health_rowsets():

    # Health checker

    health_checker = (Chyt("yt.clickhouse.yt.health_checker.success")
        .all("query_index")
        .legend_format("health checker {{query_index}}"))

    clique_instance_count = (MonitoringExpr(Chyt("yt.clickhouse.yt.clique_instance_count"))
        .value("cookie", "!Aggr")
        .series_max()
        .alias("clique instance count"))

    discovery_health = ((Chyt("yt.clickhouse.yt.discovery.participant_count") / clique_instance_count)
        .drop_nan()
        .alias("discovery health"))

    # Instance restart count

    instance_restart_count = (Chyt("yt.server.restarted")
        .value("window", "30min")
        .legend_format("restart count"))

    # Description

    DESCRIPTION = """
**Health Checker**: number of instances that report that they are healthy. Value less than `instance count` indicates that some instances are unavailable.
**Discovery Health**: number of instances seen by each instance normalized by `instance count`. Value less than `instance count` indicates connectivity/discovery problems.
Frequent **instance restarts** may indicate insufficient resources in the compute pool or instance OOM/SIGSEGV problems.
"""

    return [
        (Rowset()
            .value("operation_alias", TemplateTag("clique_alias"))
            .value("cookie", TemplateTag("instance_cookie"))
            .row()
                .cell(
                    "Health Checker",
                    MultiSensor(
                        health_checker,
                        clique_instance_count,
                        discovery_health,
                    ).min(0),
                    display_legend=True,
                )
                .cell(
                    "Instance Restart Count (30 min window)",
                    instance_restart_count.min(0),
                    display_legend=True,
                )
        ).owner,
        (Rowset().row(height=4).cell("", Text(DESCRIPTION))).owner,
    ]


def build_chyt_queries_rowsets():
    # Monitoring grid size in seconds
    grid_size = 5

    # Running queries

    running_initial_queries = (Chyt("yt.clickhouse.yt.query_registry.running_initial_query_count")
        .legend_format("running initial queries"))

    running_secondary_queries = (Chyt("yt.clickhouse.yt.query_registry.running_secondary_query_count")
        .legend_format("running secondary queries"))

    running_queries_sensors = MultiSensor(
        running_initial_queries,
        running_secondary_queries,
    ).aggr("user")

    # Requests Per Minute

    scale = MonitoringExpr(60 / grid_size)

    started_initial_queries = ((Chyt("yt.clickhouse.yt.query_registry.historical_initial_query_count.rate") * scale)
        .legend_format("started initial queries"))

    finished_initial_queries = ((Chyt("yt.clickhouse.yt.query_registry.historical_finished_initial_query_count.rate") * scale)
        .legend_format("finished initial queries"))

    requests_per_minute_sensors = MultiSensor(
        started_initial_queries,
        finished_initial_queries,
    ).aggr("user")

    # Query Fail Rate (%)

    query_rate = (Chyt("yt.clickhouse.native.global_profile_events.query.rate"))
    failed_query_rate = (Chyt("yt.clickhouse.native.global_profile_events.failed_query.rate"))
    query_memory_limit_exceeded_rate = (Chyt("yt.clickhouse.native.global_profile_events.query_memory_limit_exceeded.rate"))

    failed_query_percent = ((MonitoringExpr(failed_query_rate) / query_rate * 100)
        .alias("failed"))
    query_memory_limit_exceeded_percent = ((MonitoringExpr(query_memory_limit_exceeded_rate) / query_rate * 100)
        .alias("memory limit exceeded"))

    failed_query_rate_moving_5m = MonitoringExpr(failed_query_rate).moving_sum("5m")
    query_rate_moving_5m = MonitoringExpr(query_rate).moving_sum("5m")

    failed_query_percent_moving_5min = ((failed_query_rate_moving_5m / query_rate_moving_5m * 100)
        .alias("failed (5 min window)"))

    # Data Cache Read Per Minute

    scale = MonitoringExpr(60 / grid_size)

    compressed_data_hit_rate = ((Chyt("yt.connection.block_cache.compressed_data.hit_weight.rate") * scale)
        .alias("compressed hit"))

    compressed_data_missed_rate = ((Chyt("yt.connection.block_cache.compressed_data.missed_weight.rate") * scale)
        .alias("compressed missed"))

    uncompressed_data_hit_rate = ((Chyt("yt.connection.block_cache.uncompressed_data.hit_weight.rate") * scale)
        .alias("uncompressed hit"))

    uncompressed_data_missed_rate = ((Chyt("yt.connection.block_cache.uncompressed_data.missed_weight.rate") * scale)
        .alias("uncompressed missed"))

    data_cache_read_sensors = MultiSensor(
        compressed_data_hit_rate,
        compressed_data_missed_rate,
        uncompressed_data_hit_rate,
        uncompressed_data_missed_rate,
    ).aggr("hit_type", "connection_name")

    # Description

    DESCRIPTION = """
**Initial Query** is a query started by a real user.
**Secondary Query** is an internal query started by CHYT to process an initial query distributedly.
Typically one initial query produces `instance count` secondary queries.

**Query Fail Rate** is calculated based on both initial and secondary queries. Use with caution.
"""

    return [
        (Rowset()
            .value("operation_alias", TemplateTag("clique_alias"))
            .value("cookie", TemplateTag("instance_cookie"))
            .row()
                .cell(
                    "Running Queries",
                    running_queries_sensors.min(0),
                    display_legend=True,
                )
                .cell(
                    "Requests Per Minute",
                    requests_per_minute_sensors.min(0),
                    display_legend=True,
                )
            .row()
                .cell(
                    "Query Fail Rate (%)",
                    MultiSensor(
                        failed_query_percent,
                        query_memory_limit_exceeded_percent,
                        failed_query_percent_moving_5min,
                    ).min(0),
                    display_legend=True,
                )
                .cell(
                    "Data Cache Read Per Minute",
                    data_cache_read_sensors.min(0),
                    display_legend=True,
                )
        ).owner,
        (Rowset().row(height=4).cell("", Text(DESCRIPTION))).owner,
    ]


def build_chyt_resources_rowsets():
    # CPU

    allocated_cpu = (Chyt("yt.clickhouse.yt.cpu_limit")
        .legend_format("allocated"))

    # YT CPU metrics are exported as CPU cores x100
    cpu_scale = MonitoringExpr(1 / 100)

    total_cpu_usage = ((Chyt("yt.resource_tracker.user_cpu") * cpu_scale)
        .aggr("thread")
        .legend_format("usage"))

    # RAM

    memory_usage_rss = (Chyt("yt.resource_tracker.memory_usage.rss")
        .legend_format("memory usage (rss)"))

    clickhouse_memory_limit = (Chyt("yt.clickhouse.native.memory_limit")
        .legend_format("clickhouse memory limit"))

    oom_memory_limit = (Chyt("yt.clickhouse.yt.memory_limit.oom")
        .legend_format("oom memory limit"))

    watchdog_memory_limit = (Chyt("yt.clickhouse.yt.memory_limit.watchdog")
        .legend_format("watchdog memory limit"))

    # Detailed CPU

    worker_cpu = ((Chyt("yt.resource_tracker.user_cpu") * cpu_scale)
        .value("thread", "Worker")
        .legend_format("worker"))

    compression_cpu = ((Chyt("yt.resource_tracker.user_cpu") * cpu_scale)
        .value("thread", "FSCompression")
        .legend_format("compression"))

    chunk_reader_cpu = ((Chyt("yt.resource_tracker.user_cpu") * cpu_scale)
        .value("thread", "ChunkReader")
        .legend_format("chunk reader"))

    control_cpu = ((Chyt("yt.resource_tracker.user_cpu") * cpu_scale)
        .value("thread", "Control")
        .legend_format("control"))

    clickhouse_cpu = ((Chyt("yt.clickhouse.native.user_profile_events.user_time_microseconds.rate") / MonitoringExpr(1000000))
        .aggr("user")
        .legend_format("clickhouse"))

    # Detailed RAM

    compressed_block_cache = (Chyt("yt.connection.block_cache.compressed_data.weight")
        .aggr("segment", "connection_name")
        .legend_format("compressed block cache"))

    uncompressed_block_cache = (Chyt("yt.connection.block_cache.uncompressed_data.weight")
        .aggr("segment", "connection_name")
        .legend_format("uncompressed block cache"))

    chunk_meta_cache = (Chyt("yt.connection.chunk_meta_cache.weight")
        .aggr("segment", "connection_name")
        .legend_format("chunk meta cache"))

    fiber_stacks = (Chyt("yt.fiber.stack.bytes_alive")
        .legend_format("fiber stacks"))

    chunk_reader_buffers = (Chyt("yt.chunk_reader.memory.usage")
        .aggr("user")
        .legend_format("chunk reader buffers"))

    # Description

    DESCRIPTION = """
**Allocated CPU** - number of CPU cores allocated by YT for clique instances. It is the usage from YT's point of view.
**CPU usage** - a real CPU usage by the clique. It is expected that **usage** is much less than **allocated** most of the time. In rare cases **usage** may exceed **allocated** CPU if there are idle CPUs on the YT node.

**Detailed CPU** shows a CPU usage categorization. It tipically sums up to total **CPU usage**.
**Detailed RAM** shows a RAM usage categorization. It tipically does **not** sum up to total **memory usage (rss)**, because:
- Memory used by ClickHouse is not tracked separately.
- Each category includes all logically allocated memory (i.e., virtual memory), but the total memory usage only includes phisically allocated pages.

When examining resource usage, it's more practical to look at each instance's chart separately.

When instance memory usage reaches:
- `clickhouse memory limit`, queries that try to allocate more memory will receive `Memory limit exceeded (Total)` error.
- `watchdog memory limit`, the instance will try to gracefully shut down all queries and restart.
- `oom limit`, the instance will be terminated by YT immediately. All queries running on this instance will fail.

"""

    return [
        (Rowset()
            .value("operation_alias", TemplateTag("clique_alias"))
            .value("cookie", TemplateTag("instance_cookie"))
            .row()
                .cell(
                    "CPU",
                    MultiSensor(
                        allocated_cpu,
                        total_cpu_usage,
                    ).min(0),
                    display_legend=True,
                )
                .cell(
                    "RAM",
                    MultiSensor(
                        memory_usage_rss,
                        oom_memory_limit,
                        watchdog_memory_limit,
                        clickhouse_memory_limit,
                    ).min(0),
                    display_legend=True,
                )
            .row()
                .cell(
                    "Detailed CPU",
                    MultiSensor(
                        worker_cpu,
                        compression_cpu,
                        chunk_reader_cpu,
                        control_cpu,
                        clickhouse_cpu,
                    ).min(0).stack(True),
                    display_legend=True,
                )
                .cell(
                    "Detailed RAM",
                    MultiSensor(
                        fiber_stacks,
                        compressed_block_cache,
                        uncompressed_block_cache,
                        chunk_meta_cache,
                        chunk_reader_buffers,
                    ).min(0).stack(True),
                    display_legend=True,
                )
        ).owner,
        (Rowset().row(height=13).cell("", Text(DESCRIPTION))).owner,
    ]


def build_chyt_monitoring_rowsets():
    res = []
    res.extend(build_chyt_health_rowsets())
    res.extend(build_chyt_queries_rowsets())
    res.extend(build_chyt_resources_rowsets())
    return res


def build_chyt_monitoring():
    d = Dashboard()

    for r in build_chyt_monitoring_rowsets():
        d.add(r)

    d.add_parameter(
        "cluster",
        "YT cluster",
        MonitoringLabelDashboardParameter("yt", "cluster", "hahn"))

    d.add_parameter(
        "clique_alias",
        "Clique alias",
        MonitoringLabelDashboardParameter("yt", "operation_alias", "ch_public"))

    d.add_parameter(
        "instance_cookie",
        "Instance cookie",
        MonitoringLabelDashboardParameter("yt", "cookie", "Aggr"))

    return d
