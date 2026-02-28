# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent
# [E131] continuation line unaligned for hanging indent

from ..common.sensors import FlowController, FlowWorker

from .common import create_dashboard

from yt_dashboard_generator.dashboard import Rowset
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr, PlainMonitoringExpr
from yt_dashboard_generator.backends.monitoring import MonitoringLabelDashboardParameter
from yt_dashboard_generator.sensor import MultiSensor, EmptyCell

from textwrap import dedent

class ComputationCellGenerator:
    def __init__(self, has_computation_id_tag=False):
        self._has_computation_id_tag = has_computation_id_tag

    def add_epoch_parts_time_cell(self, row):
        return row.cell(
            "Epoch parts time" + (" (Computation: {{computation_id}})" if self._has_computation_id_tag else ""),
        MonitoringExpr(
                FlowWorker("yt.flow.worker.computation.epoch_parts_time.rate")
                    .value("computation_id", "{{computation_id}}" if self._has_computation_id_tag else "-")
                    .value("part", "!-"))
                    .query_transformation("{query} * 0.001")
                .aggr("host")
                .stack(True),
            description=dedent("""\
                It is what jobs do at every moment.

                **Init** - loading timers and output messages after start of job (do you understand why previous jobs were finished?).
                **Input.Empty/Input.Fetch** - just waiting for input messages.
                **Input.InjectionDelay** - has input messages but waits for watermark to be advanced because of injection delay logic.
                **CheckDelayedMessages** - checking delayed messages (little CPU-bound work).
                **Distribute.Start** - scheduling distributing (little CPU-bound work).
                **Distribute.OutputBufferOverflow** - waiting because of output buffer overflow.
                **Distribute.OutputStoreOverflow** - waiting because of output store overflow.
                **Process** - executing user logic (and may be state loading logic if this is a stateful computation).
                **Sync** - writing to opened transaction (CPU-bound work).
                **Finish** - finishing job (little CPU-bound work).
                **DeduplicateInput** - deduplicating input messages (IO-bound work).
                **StartTransaction** - opening transaction (IO-bound work).
                **Commit** - committing transaction (IO-bound work).
            """),
            colors={
                "Unknown": "#999999",

                "Init": "#b70000",

                # Waiting new data.
                "Input.Empty": "#00e500",
                "Input.Fetch": "#00ff00",
                "Input.InjectionDelay": "#b7e500",
                "CheckDelayedMessages": "#8eb200",

                # Backpressure.
                "Distribute.Start": "#b266b2",
                "Distribute.OutputBufferOverflow": "#730073",
                "Distribute.OutputStoreOverflow": "#993299",

                # CPU-bound work + user work.
                "Process": "#ffa500",
                "Sync": "#ffb732",
                "Finish": "#ffc966",

                # System IO-bound work.
                "DeduplicateInput": "#236bb2",
                "StartTransaction": "#d6eaff",
                "Commit": "#84c1ff",
            })

    def add_epoch_duration_max_time_cell(self, row):
        return row.cell(
            "Epoch duration max time",
            MonitoringExpr(FlowWorker("yt.flow.worker.computation.epoch_time.max"))
                .all("host")
                .value("computation_id", "{{computation_id}}" if self._has_computation_id_tag else "!-")
                .alias(("{{computation_id}} - " if not self._has_computation_id_tag else "") + "{{host}}")
                .top(50)
                .unit("UNIT_SECONDS")
                .stack(False))

    def add_epoch_count_total_cell(self, row):
        return row.cell(
            "Epoch count total",
            MonitoringExpr(FlowWorker("yt.flow.worker.computation.epoch.rate"))
                .aggr("host")
                .value("computation_id", "{{computation_id}}" if self._has_computation_id_tag else "!-")
                .unit("UNIT_COUNT")
                .stack(False))

    def build_resources_rowset(self):
        stream_alias = "{{computation_id}} / {{stream_id}}" if not self._has_computation_id_tag else "{{stream_id}}"
        return (Rowset()
            .stack(True)
            .value("computation_id", "{{computation_id}}" if self._has_computation_id_tag else "!-")
            .aggr("host")
            .row()
                .cell(
                    "Computation cpu time (approximate)",
                    MonitoringExpr(FlowWorker("yt.flow.worker.computation.cpu_time.rate"))
                        .alias("{{computation_id}}")
                        .unit("UNIT_NONE"),
                    description="May be overestimated under high CPU load.\nUses RDTSC deltas, which can increase due to context switches")
                .cell(
                    "Computation memory usage (approximate)",
                    MonitoringExpr(FlowWorker("yt.flow.worker.computation.memory_usage"))
                        .alias("{{computation_id}}")
                        .unit("UNIT_BYTES_SI"),
                    description="May be imprecise due to sampling nature of measuring")
                .cell(
                    "Input buffers size",
                    MonitoringExpr(FlowWorker("yt.flow.worker.buffer_state.computations.input.size"))
                        .all("stream_id")
                        .alias(stream_alias)
                        .unit("UNIT_BYTES_SI"))
                .cell(
                    "Output buffers size",
                    MonitoringExpr(FlowWorker("yt.flow.worker.buffer_state.computations.output.size"))
                        .all("stream_id")
                        .alias(stream_alias)
                        .unit("UNIT_BYTES_SI"))
        )

    def build_message_rate_rowset(self):
        stream_alias = "{{computation_id}} - {{stream_id}}" if not self._has_computation_id_tag else "{{stream_id}}"
        return (Rowset()
            .stack(True)
            .aggr("host")
            .value("computation_id", "{{computation_id}}" if self._has_computation_id_tag else "!-")
            .all("stream_id")
            .row()
                .cell(
                    "Processed messages rate",
                    MultiSensor(
                        MonitoringExpr(FlowWorker("yt.flow.worker.computation.input_streams.persisted_count.rate"))
                            .alias(f"input - {stream_alias}"),
                        MonitoringExpr(FlowWorker("yt.flow.worker.computation.source_streams.persisted_count.rate"))
                            .alias(f"source - {stream_alias}"),
                        MonitoringExpr(FlowWorker("yt.flow.worker.computation.source.source_streams.persisted_count.rate"))
                            .query_transformation("{query} / 4")
                            .alias(f"bugfix source - {stream_alias}"),
                        MonitoringExpr(FlowWorker("yt.flow.worker.computation.timer_streams.unregistered_count.rate"))
                            .alias(f"timer - {stream_alias}")
                    )
                        .unit("UNIT_COUNTS_PER_SECOND"),
                    description="Input/source/timer messages that are processed and can be forgotten")
                .cell(
                    "Generated messages rate",
                    MultiSensor(
                        MonitoringExpr(FlowWorker("yt.flow.worker.computation.output_streams.registered_count.rate"))
                            .alias(f"output - {stream_alias}"),
                        MonitoringExpr(FlowWorker("yt.flow.worker.computation.timer_streams.registered_count.rate"))
                            .alias(f"timer - {stream_alias}")
                    )
                        .unit("UNIT_COUNTS_PER_SECOND"),
                    description="Output/timer messages that was generated in pipeline")
                .cell(
                    "Processed messages bytes rate",
                    MultiSensor(
                        MonitoringExpr(FlowWorker("yt.flow.worker.computation.input_streams.persisted_bytes.rate"))
                            .alias(f"input - {stream_alias}"),
                        MonitoringExpr(FlowWorker("yt.flow.worker.computation.source_streams.persisted_bytes.rate"))
                            .alias(f"source - {stream_alias}"),
                        MonitoringExpr(FlowWorker("yt.flow.worker.computation.source.source_streams.persisted_bytes.rate"))
                            .query_transformation("{query} / 4")
                            .alias(f"bugfix source - {stream_alias}"),
                        MonitoringExpr(FlowWorker("yt.flow.worker.computation.timer_streams.unregistered_bytes.rate"))
                            .alias(f"timer - {stream_alias}")
                    )
                        .unit("UNIT_BYTES_SI_PER_SECOND"))
                .cell(
                    "Generated messages bytes rate",
                    MultiSensor(
                        MonitoringExpr(FlowWorker("yt.flow.worker.computation.output_streams.registered_bytes.rate"))
                            .alias(f"output - {stream_alias}"),
                        MonitoringExpr(FlowWorker("yt.flow.worker.computation.timer_streams.registered_bytes.rate"))
                            .alias(f"timer - {stream_alias}")
                    )
                        .unit("UNIT_BYTES_SI_PER_SECOND"))
        )

    def build_partition_aggregates_rowset(self):
        stream_alias = "{{{{computation_id}}}} - {{{{stream_id}}}}" if not self._has_computation_id_tag else "{{{{stream_id}}}}"

        def transformation(alias):
            return "let non_empty_computation_id = (\"{{{{computation_id}}}}\" == '-' ? '*' : \"{{{{computation_id}}}}\"); alias({query}" + f", \"{alias}\")"

        def add_cpu_cell(row, title_prefix, metric_suffix):
            description = (
                "Each partition is processed in a single thread mode. "
                "Consumption of ≥ 0.5 of a processor core per partition is a bad indicator. "
                "(Job also does some IO-bound work, so saturation is often reached below 1.0.)"
            )
            return row.cell(
                f"{title_prefix} partition cpu usage",
                MultiSensor(
                    MonitoringExpr(FlowController(f"yt.flow.controller.computations.partition_cpu_usage.{metric_suffix}"))
                        .query_transformation(transformation("{{{{computation_id}}}}")),
                    PlainMonitoringExpr("constant_line(1)")
                        .alias("One core - hard limit"),
                    PlainMonitoringExpr("constant_line(0.5)")
                        .alias("Warning level"))
                    .min(0),
                colors={
                    "One core - hard limit": "#f4cccc",
                    "Warning level": "#ffedcc",
                },
                description=description)

        def add_messages_per_second_cell(row, title_prefix, metric_suffix):
            description = (
                "Partition messages rate ≤ 1000 messages/s is usually good enough. "
                "Higher values are sometimes acceptable."
            )
            return row.cell(
                f"{title_prefix} partition messages per second",
                MonitoringExpr(FlowController(f"yt.flow.controller.computations.partition_*messages_per_second.{metric_suffix}"))
                    .all("stream_id")
                    .query_transformation(transformation(stream_alias))
                    .unit("UNIT_COUNTS_PER_SECOND"),
                description=description)

        def add_bytes_per_second_cell(row, title_prefix, metric_suffix):
            description = (
                "Partition bytes rate ≤ 2 MB/s is usually good enough. "
                "Higher values are sometimes acceptable."
            )
            return row.cell(
                f"{title_prefix} partition bytes per second",
                MonitoringExpr(FlowController(f"yt.flow.controller.computations.partition_*bytes_per_second.{metric_suffix}"))
                    .all("stream_id")
                    .query_transformation(transformation(stream_alias))
                    .unit("UNIT_BYTES_SI_PER_SECOND"),
                description=description)

        def add_watermark_difference_cell(row, kind):
            description = (
                "Watermark is evaluated in all jobs of computation. "
                "This metric is difference between maximum and minimum among them. "
                "For source computation it is a measure of unevenness of reading this source "
                "(one partition is read ahead of another)"
            )
            return row.cell(
                f"Partition {kind} watermark difference (max - min)",
                MonitoringExpr(FlowController(f"yt.flow.controller.computations.partition_{kind}_watermark_min_max_difference"))
                    .all("stream_id")
                    .query_transformation(transformation(stream_alias))
                    .unit("UNIT_SECONDS"),
                description=description)

        return (Rowset()
            .stack(False)
            .value("computation_id", "{{non_empty_computation_id}}" if self._has_computation_id_tag else "*")
            .row()
                .apply_func(lambda row: add_cpu_cell(row, "Max", "max"))
                .apply_func(lambda row: add_cpu_cell(row, "Average", "avg"))
                .apply_func(lambda row: add_watermark_difference_cell(row, "system"))
                .apply_func(lambda row: add_watermark_difference_cell(row, "event"))
            .row()
                .apply_func(lambda row: add_messages_per_second_cell(row, "Max", "max"))
                .apply_func(lambda row: add_messages_per_second_cell(row, "Average", "avg"))
                .apply_func(lambda row: add_bytes_per_second_cell(row, "Max", "max"))
                .apply_func(lambda row: add_bytes_per_second_cell(row, "Average", "avg"))
        )

    def build_partition_store_operations_rowset(self):
        host_alias = "{{computation_id}} - {{host}}" if not self._has_computation_id_tag else "{{host}}"
        return (Rowset()
            .stack(False)
            .value("computation_id", "{{computation_id}}" if self._has_computation_id_tag else "!-")
            .row()
                .cell(
                    "Partition store commit rate",
                    MonitoringExpr(FlowWorker("yt.flow.worker.computation.partition_store.commit_total.rate"))
                        .aggr("host")
                        .unit("UNIT_REQUESTS_PER_SECOND"))
                .cell(
                    "Partition store failed commit rate",
                    MonitoringExpr(FlowWorker("yt.flow.worker.computation.partition_store.commit_failed.rate"))
                        .aggr("host")
                        .unit("UNIT_REQUESTS_PER_SECOND"))
                .cell(
                    "Partition store commit time",
                    MonitoringExpr(FlowWorker("yt.flow.worker.computation.partition_store.commit_time.max"))
                        .all("host")
                        .alias(host_alias)
                        .top(50)
                        .unit("UNIT_SECONDS"))
                .cell(
                    "Input messages lookup time",
                    MonitoringExpr(FlowWorker("yt.flow.worker.computation.partition_store.input_messages.lookup_time.max"))
                        .all("host")
                        .alias(host_alias)
                        .top(50)
                        .unit("UNIT_SECONDS"))
        )

    def build_processed_message_rate_rowset(self):
        return (Rowset()
            .stack(False)
            .value("computation_id", "{{computation_id}}" if self._has_computation_id_tag else "!-")
            .row()
                .cell(
                    "Processed messages rate",
                    MonitoringExpr(FlowWorker("yt.flow.worker.computation.partition_store.input_messages.checked.rate|yt.flow.worker.computation.partition_store.input_messages.filtered.rate"))
                        .aggr("host")
                        .unit("UNIT_COUNTS_PER_SECOND")
                )
                .cell("", EmptyCell())
                .cell("", EmptyCell())
                .cell("", EmptyCell())
        )

GENERATOR = ComputationCellGenerator(has_computation_id_tag=True)


def build_epoch_timings():
    return (Rowset()
        .row()
            .apply_func(GENERATOR.add_epoch_parts_time_cell)
            .apply_func(GENERATOR.add_epoch_duration_max_time_cell)
            .apply_func(GENERATOR.add_epoch_count_total_cell)
            .cell("", EmptyCell())
    )


def build_flow_computation():
    def fill(d):
        d.add_parameter("computation_id", "Computation (only for some graphs)", MonitoringLabelDashboardParameter("", "computation_id", "-"))

        d.add(build_epoch_timings())
        d.add(GENERATOR.build_message_rate_rowset())
        d.add(GENERATOR.build_resources_rowset())
        d.add(GENERATOR.build_partition_aggregates_rowset())
        d.add(GENERATOR.build_partition_store_operations_rowset())
        d.add(GENERATOR.build_processed_message_rate_rowset())

    return create_dashboard("computation", fill)
