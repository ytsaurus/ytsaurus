# flake8: noqa
from yt_dashboard_generator.dashboard import Rowset
from yt_dashboard_generator.sensor import MultiSensor

from ..common.sensors import *

##################################################################


def build_bundle_key_filter_rowset():
    filter_cache = TabNode("yt.data_node.block_cache.xor_filter.{}")

    return (Rowset()
        .min(0)
        .row()
            .cell("Lookup key filter block cache usage", filter_cache("weight"))
            .cell("Lookup cumulative cpu time", NodeTablet("yt.tablet_node.lookup.cpu_time.rate"))
        .row()
            .cell("Key filter cache hit count rate", MultiSensor(
                filter_cache("hit_count.rate"),
                filter_cache("miss_count.rate"),
            ))
            .cell("Key filter cache hit weight rate", MultiSensor(
                filter_cache("hit_weight.rate"),
                filter_cache("miss_weight.rate"),
            ))
    ).owner
