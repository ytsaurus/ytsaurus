""" Note: the code of this script was vibe-coded using Claude Sonnet 4"""
# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent

from .common.sensors import UserJobSensors

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.sensor import Sensor, MultiSensor, Text, Title
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.backends.monitoring import MonitoringLabelDashboardParameter, MonitoringExpr

##################################################################


FRACTION_LABEL = "Sum of device fractions"
BYTES_LABEL = "Bytes"


def _add_series_sum(subquery):
    return f'series_sum("job_descriptor", {subquery})'


def _build_user_job_sensor(sensor_name, legend=None, query_transformation=None, unit=None):
    sensor = UserJobSensors(sensor_name)\
        .query_transformation(_add_series_sum(query_transformation if query_transformation is not None else "{query}"))
    if legend is not None:
        sensor = sensor.legend_format(legend + " ({{job_descriptor}})")
    if unit is not None:
        sensor = sensor.unit(unit)
    return sensor


def _build_cpu_and_memory_metrics(d):
    d.add(Rowset().row(height=2).cell("", Title("CPU and Memory", size="TITLE_SIZE_L")))
    d.add(Rowset()
        .nan_as_zero()
        .row()
            .stack(True)
            .min(0)
            .cell("CPU", MultiSensor(
                    _build_user_job_sensor(
                        "yt.user_job.cpu.user.rate",
                        query_transformation="{query} / 1000.0",
                        legend="User CPU"),
                    _build_user_job_sensor(
                        "yt.user_job.cpu.system.rate",
                        query_transformation="{query} / 1000.0",
                        legend="System CPU"),
                ),
                yaxis_label="Cores",
                display_legend=False,
            )
            .cell("Memory", MultiSensor(
                    _build_user_job_sensor(
                        "yt.user_job.tmpfs_size",
                        legend="Tmpfs size",
                        unit="UNIT_BYTES_SI"),
                    _build_user_job_sensor(
                        "yt.user_job.current_memory.rss",
                        legend="RSS",
                        unit="UNIT_BYTES_SI"),
                    _build_user_job_sensor(
                        "yt.user_job.current_memory.mapped_file",
                        legend="Mapped",
                        unit="UNIT_BYTES_SI"),
                ),
                yaxis_label=BYTES_LABEL,
                display_legend=False,
            )
    )
    d.add(Rowset()
        .nan_as_zero()
        .row()
            .stack(True)
            .min(0)
            .cell("CPU Burst and Throttled", MultiSensor(
                    _build_user_job_sensor(
                        "yt.user_job.cpu.burst.rate",
                        query_transformation="{query} / 1000.0",
                        legend="Burst"),
                    _build_user_job_sensor(
                        "yt.user_job.cpu.cfs_throttled.rate",
                        query_transformation="{query} / 1000.0",
                        legend="CFS Throttled"),
                ),
                yaxis_label="Cores",
                display_legend=False)
            .cell("CPU Wait",
                _build_user_job_sensor(
                    "yt.user_job.cpu.wait.rate",
                    query_transformation="{query} / 1000.0",
                    legend="CPU wait",
                ),
                yaxis_label="Cores",
                display_legend=False)
    )


def _build_disk_metrics(d):
    d.add(Rowset().row(height=2).cell("", Title("Storage", size="TITLE_SIZE_L")))
    d.add(Rowset()
        .nan_as_zero()
        .row()
            .stack(False)
            .min(0)
            .cell("Disk space usage", MultiSensor(
                    _build_user_job_sensor(
                        "yt.user_job.disk.usage",
                        legend="Usage",
                        unit="UNIT_BYTES_SI"),
                    _build_user_job_sensor(
                        "yt.user_job.disk.limit",
                        legend="Limit",
                        unit="UNIT_BYTES_SI"),
                ),
                yaxis_label=BYTES_LABEL,
                display_legend=False)
            .cell("Disk IO",
                _build_user_job_sensor(
                    "yt.user_job.block_io.io_total.rate",
                    legend="IO",
                    unit="UNIT_IO_OPERATIONS_PER_SECOND"),
                yaxis_label="IO operations/sec",
                display_legend=False)
    )


def _build_network_metrics(d):
    d.add(Rowset().row(height=2).cell("", Title("Network", size="TITLE_SIZE_L")))
    d.add(Rowset()
        .nan_as_zero()
        .unit("UNIT_BYTES_SI_PER_SECOND")
        .row()
            .stack(False)
            .min(0)
            .cell("Network TX", MultiSensor(
                    _build_user_job_sensor(
                        "yt.user_job.network.tx_bytes.rate",
                        legend="Network output"),
                    _build_user_job_sensor(
                        "yt.user_job.network.custom_tx_bytes.rate",
                        legend="Network output for subcontainer"),
                ),
                yaxis_label="Bytes/sec",
                display_legend=False)
            .cell("Network RX", MultiSensor(
                    _build_user_job_sensor(
                        "yt.user_job.network.rx_bytes.rate",
                        legend="Network input"),
                    _build_user_job_sensor(
                        "yt.user_job.network.custom_rx_bytes.rate",
                        legend="Network input for subcontainer"),
                ),
                yaxis_label="Bytes/sec",
                display_legend=False)
    )


def _build_gpu_metrics(d):
    d.add(Rowset().row(height=2).cell("", Title("GPU Common", size="TITLE_SIZE_L")))
    d.add(Rowset()
        .value("gpu_slot", "-")
        .nan_as_zero()
        .row()
            .stack(False)
            .min(0)
            .cell("GPU Utilization", _build_user_job_sensor("yt.user_job.gpu.utilization_gpu", legend="Utilization"),
                  yaxis_label=FRACTION_LABEL, display_legend=False)
            .cell("GPU Memory",
                  _build_user_job_sensor("yt.user_job.gpu.memory", legend="Memory", unit="UNIT_BYTES_SI"),
                  yaxis_label=BYTES_LABEL, display_legend=False)
    )
    d.add(Rowset()
        .value("gpu_slot", "-")
        .nan_as_zero()
        .row()
            .stack(False)
            .min(0)
            .cell("GPU SM utilization", _build_user_job_sensor("yt.user_job.gpu.sm_utilization", legend="Utilization"),
                  yaxis_label=FRACTION_LABEL, display_legend=False)
            .cell("GPU SM occupancy", _build_user_job_sensor("yt.user_job.gpu.sm_occupancy", legend="SM occupancy"),
                  yaxis_label=FRACTION_LABEL, display_legend=False)
    )
    d.add(Rowset()
        .value("gpu_slot", "-")
        .nan_as_zero()
        .row()
            .stack(False)
            .min(0)
            .cell("GPU Power", _build_user_job_sensor("yt.user_job.gpu.power", legend="Power"),
                  yaxis_label="Watts", display_legend=False)
            # TODO(renadeen): migrate to slowdown_type.
            .cell("GPU Slowdown", UserJobSensors("yt.user_job.gpu.*slowdown"),
                  yaxis_label="Indicator", display_legend=False)
    )


def _build_interconnect_metrics(d):
    d.add(Rowset().row(height=2).cell("", Title("GPU Interconnect", size="TITLE_SIZE_L")))
    d.add(Rowset()
        .value("gpu_slot", "-")
        .nan_as_zero()
        .row()
            .stack(False)
            .min(0)
            .unit("UNIT_BYTES_SI_PER_SECOND")
            .cell("PCIe TX", _build_user_job_sensor("yt.user_job.gpu.pcie.tx_bytes.rate", legend="PCIe input"),
                  yaxis_label="Bytes/sec", display_legend=False)
            .cell("PCIe RX", _build_user_job_sensor("yt.user_job.gpu.pcie.rx_bytes.rate", legend="PCIe output"),
                  yaxis_label="Bytes/sec", display_legend=False)
    )
    d.add(Rowset()
        .value("gpu_slot", "-")
        .nan_as_zero()
        .row()
            .stack(False)
            .min(0)
            .unit("UNIT_BYTES_SI_PER_SECOND")
            .cell("NVLink TX", _build_user_job_sensor("yt.user_job.gpu.nvlink.tx_bytes.rate", legend="NVLink input"),
                  yaxis_label="Bytes/sec", display_legend=False)
            .cell("NVLink RX", _build_user_job_sensor("yt.user_job.gpu.nvlink.rx_bytes.rate", legend="NVLink output"),
                  yaxis_label="Bytes/sec", display_legend=False)
    )
    d.add(Rowset()
        .value("rdma_device", "-")
        .nan_as_zero()
        .row()
            .stack(False)
            .min(0)
            .unit("UNIT_BYTES_SI_PER_SECOND")
            .cell("RDMA TX", _build_user_job_sensor("yt.user_job.gpu.rdma.tx_bytes.rate", legend="RDMA input"),
                  yaxis_label="Bytes/sec", display_legend=False)
            .cell("RDMA RX", _build_user_job_sensor("yt.user_job.gpu.rdma.rx_bytes.rate", legend="RDMA output"),
                  yaxis_label="Bytes/sec", display_legend=False)
    )


def _build_advanced_gpu_metrics(d):
    d.add(Rowset().row(height=2).cell("", Title("GPU Activity", size="TITLE_SIZE_L")))
    d.add(Rowset()
        .value("gpu_slot", "-")
        .nan_as_zero()
        .row()
            .stack(False)
            .min(0)
            .cell("Tensor Activity", _build_user_job_sensor("yt.user_job.gpu.tensor_activity", legend="Tensor activity"),
                  yaxis_label=FRACTION_LABEL, display_legend=False)
            .cell("DRAM Activity", _build_user_job_sensor("yt.user_job.gpu.dram_activity", legend="DRAM activity"),
                  yaxis_label=FRACTION_LABEL, display_legend=False)
    )


def build_jobs_monitor():
    d = Dashboard()
    _build_cpu_and_memory_metrics(d)
    _build_disk_metrics(d)
    _build_network_metrics(d)
    _build_gpu_metrics(d)
    _build_interconnect_metrics(d)
    _build_advanced_gpu_metrics(d)

    d.value("job_descriptor", TemplateTag("job_descriptor"))

    d.add_parameter(
        "cluster",
        "Cluster",
        MonitoringLabelDashboardParameter(
            "yt",
            "cluster",
            "*",
        ))
    d.add_parameter(
        "job_descriptor",
        "Job descriptor",
        MonitoringLabelDashboardParameter(
            "yt",
            "job_descriptor",
            "*",
        ))

    d.set_monitoring_serializer_options(dict(default_row_height=9))

    return d
