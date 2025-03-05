# flake8: noqa
from yt_dashboard_generator.dashboard import Rowset
from yt_dashboard_generator.sensor import MultiSensor
from yt_dashboard_generator.backends.monitoring import MonitoringTag
from yt_dashboard_generator.backends.monitoring.sensors import MonitoringExpr
from yt_dashboard_generator.specific_tags.tags import DuplicateTag

from ..common.sensors import *

##################################################################


def build_tablet_network():
    reader_stats = NodeTablet(
        "yt.tablet_node.{}.chunk_reader_statistics.{}.rate")

    # TODO: show per-user lines with host=Aggr and per-host with host=!Aggr.
    return (Rowset()
        .aggr("table_tag", "table_path")
        .all("#UB")
        .top()
        .stack(True)
        .row()
            .cell("Table lookup bytes received", reader_stats("lookup", "data_bytes_transmitted"))
            .cell("Table select bytes received", reader_stats("select", "data_bytes_transmitted"))
        ).owner


def _with_label_for_aggr_host(sensor, label):
    duplicate_host = DuplicateTag(MonitoringTag("host"))
    return MultiSensor(
        sensor.all(duplicate_host).aggr(label).sensor_stack(False),
        sensor.aggr(duplicate_host).all(label).sensor_stack(True))


def build_user_network():
    return (Rowset()
        .top()
        .aggr("network", "encrypted")
        .row()
            .cell(
                "Network received bytes (bus)",
                _with_label_for_aggr_host(TabNodeInternal("yt.bus.in_bytes.rate"), "band"))
            .cell(
                "Network transmitted bytes (bus)",
                _with_label_for_aggr_host(TabNodeInternal("yt.bus.in_bytes.rate"), "band"))
        .row()
            .cell("Pending out bytes", _with_label_for_aggr_host(TabNodeInternal("yt.bus.pending_out_bytes"), "band"))
            .cell("TCP retransmits rate", _with_label_for_aggr_host(TabNodeInternal("yt.bus.retransmits.rate"), "band"))
        ).owner


def build_throttling():
    def limit(sensor):
        return MonitoringExpr(sensor).series_max().alias("Limit")

    return (Rowset()
        .top()
        .row()
            .cell("Receive network throttler bytes rate", MultiSensor(
                *_with_label_for_aggr_host(TabNode("yt.cluster_node.in_throttler.value.rate"), "bucket").sensors,
                limit(TabNode("yt.cluster_node.in_throttler.total_limit"))))
            .cell("Transmit network throttler bytes rate", MultiSensor(
                *_with_label_for_aggr_host(TabNode("yt.cluster_node.out_throttler.value.rate"), "bucket").sensors,
                limit(TabNode("yt.cluster_node.out_throttler.total_limit"))))
        .row()
            .all("bucket")
            .cell("Receive network throttled", TabNode("yt.cluster_node.in_throttler.throttled"))
            .cell("Transmit network throttled", TabNode("yt.cluster_node.out_throttler.throttled"))
        ).owner


def build_rpc_request_rate():
    request_rate = (TabNodeRpc("yt.rpc.server.{}.rate")
        .aggr("#U")
        .all("yt_service", "method")
        .stack(False)
        .top())
    return (Rowset()
        .aggr("queue")
        .row()
            .cell("RPC request rate", request_rate("request_count"))
            .cell("RPC rejected due to queue overflow", request_rate("request_queue_size_errors"))
        .row()
            .cell("RPC failed request rate", request_rate("failed_request_count"))
            .cell("RPC timed out request rate", request_rate("timed_out_request_count"))
        ).owner


def build_user_bus_cpu():
    return (Rowset()
        .stack(False)
        .top()
        .row()
            .cell("Bus thread pool CPU utilization", TabNodeCpu("yt.resource_tracker.utilization").value("thread", "Bus*"))
        ).owner


def build_lookup_select_ack_time():
    s = (TabNodeRpc("yt.rpc.server.request_time.remote_wait.max")
        .value("yt_service", "QueryService")
        .aggr("user")
        .stack(False)
        .top())
    return (Rowset()
        .row()
            .cell("Table lookup (QueryService::Multiread) RPC acknowledge time", s.value("method", "Multiread"))
            .cell("Table select (QueryService::Execute) RPC acknowledge time", s.value("method", "Execute"))
        ).owner


def build_rpc_message_size_stats_per_host(
    sensor_class, client_or_server, name_prefix, name_suffix=""
):
    s = (sensor_class("yt.rpc.{}.{{}}.rate".format(client_or_server))
         .aggr("method", "yt_service", "user")
         .stack(False)
         .top())
    if name_suffix:
        name_suffix = " " + name_suffix
    return (Rowset()
        .aggr("queue")
        .row()
            .cell("{} request message body size{}".format(name_prefix, name_suffix), s("request_message_body_bytes"))
            .cell("{} request message attachment size{}".format(name_prefix, name_suffix), s("request_message_attachment_bytes"))
        .row()
            .cell("{} response message body size{}".format(name_prefix, name_suffix), s("response_message_body_bytes"))
            .cell("{} response message attachment size{}".format(name_prefix, name_suffix), s("response_message_attachment_bytes"))
        ).owner
