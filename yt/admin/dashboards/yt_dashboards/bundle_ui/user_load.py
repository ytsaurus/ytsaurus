# flake8: noqa
from yt_dashboard_generator.dashboard import Rowset
from yt_dashboard_generator.backends.monitoring import MonitoringTag

from ..common.sensors import *

##################################################################


def build_user_load():
    top_rate = NodeTablet("yt.tablet_node.{}.{}.rate")

    return (Rowset()
        .aggr(MonitoringTag("host"), "table_tag", "table_path")
        .all("#UB")
        .top()

        .row()
            .cell("Table write data weight rate", top_rate("write", "data_weight"))
            .cell("Table write row count rate", top_rate("write", "row_count"))
        .row()
            .cell("Table commit data weight rate", top_rate("commit", "data_weight"))
            .cell("Table commit row count rate", top_rate("commit", "row_count"))
        .row()
            .cell("Table lookup request count", top_rate("multiread", "request_count"))
            .cell("Table select request count", top_rate("execute", "request_count"))
        .row()
            .cell("Table lookup data weight rate", top_rate("lookup", "data_weight"))
            .cell("Table select data weight rate", top_rate("select", "data_weight"))
        .row()
            .cell("Table lookup unmerged data weight rate", top_rate("lookup", "unmerged_data_weight"))
            .cell("Table select unmerged data weight rate", top_rate("select", "unmerged_data_weight"))
        .row()
            .cell("Table lookup row count rate", top_rate("lookup", "row_count"))
            .cell("Table select row count rate", top_rate("select", "row_count"))
        .row()
            .cell("Table lookup unmerged row count rate", top_rate("lookup", "unmerged_row_count"))
            .cell("Table select unmerged row count rate", top_rate("select", "unmerged_row_count"))
        .row()
            .cell("Fetch table rows data weight rate", top_rate("fetch_table_rows", "data_weight").unit("UNIT_BYTES_SI"))
            .cell("Fetch table rows row count rate", top_rate("fetch_table_rows", "row_count"))
        ).owner


def build_max_lookup_select_execute_time_per_host():
    return (Rowset()
        .aggr(MonitoringTag("host"), "table_tag", "table_path")
        .all("#B")
        .top()
        .stack(False)
        .row()
            .cell("Table lookup max duration", NodeTablet("yt.tablet_node.multiread.request_duration.max"))
            .cell("Table select max duration", NodeTablet("yt.tablet_node.execute.request_duration.max"))
        ).owner
