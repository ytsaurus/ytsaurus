#pragma once

#include <yt/yt/core/misc/ema_counter.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

#define ITERATE_TABLET_PERFORMANCE_COUNTERS(XX) \
    XX(dynamic_row_read,                        DynamicRowRead) \
    XX(dynamic_row_read_data_weight,            DynamicRowReadDataWeight) \
    XX(dynamic_row_lookup,                      DynamicRowLookup) \
    XX(dynamic_row_lookup_data_weight,          DynamicRowLookupDataWeight) \
    XX(dynamic_row_write,                       DynamicRowWrite) \
    XX(dynamic_row_write_data_weight,           DynamicRowWriteDataWeight) \
    XX(dynamic_row_delete,                      DynamicRowDelete) \
    XX(static_chunk_row_read,                   StaticChunkRowRead) \
    XX(static_chunk_row_read_data_weight,       StaticChunkRowReadDataWeight) \
    XX(static_chunk_row_lookup,                 StaticChunkRowLookup) \
    XX(static_chunk_row_lookup_data_weight,     StaticChunkRowLookupDataWeight) \
    XX(compaction_data_weight,                  CompactionDataWeight) \
    XX(partitioning_data_weight,                PartitioningDataWeight) \
    XX(lookup_error,                            LookupError) \
    XX(write_error,                             WriteError)

#define ITERATE_NODE_TABLET_PERFORMANCE_COUNTERS(XX) \
    XX(lookup_cpu_time,                         LookupCpuTime)

struct TTabletPerformanceCounters
{
    static const TEmaCounter::TWindowDurations TabletPerformanceWindowDurations;
    #define XX(name, Name) TEmaCounter Name = TEmaCounter(TabletPerformanceWindowDurations);
    ITERATE_TABLET_PERFORMANCE_COUNTERS(XX)
    ITERATE_NODE_TABLET_PERFORMANCE_COUNTERS(XX)
    #undef XX
};

void Serialize(const TTabletPerformanceCounters& counters, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
