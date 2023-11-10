#pragma once

#include "public.h"

#include <yt/yt/core/misc/ema_counter.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TPerformanceCountersEma
{
    std::atomic<i64> Counter;
    TEmaCounter Ema{TEmaCounter::TWindowDurations{
        TDuration::Minutes(10),
        TDuration::Hours(1),
    }};

    void UpdateEma();
};

static_assert(sizeof(TEmaCounter) >= 64 - 8, "Consider adding alignment in TPerformanceCountersEma to avoid false sharing.");

////////////////////////////////////////////////////////////////////////////////

struct TChunkReaderPerformanceCounters
    : public TRefCounted
{
    TPerformanceCountersEma StaticChunkRowRead;
    TPerformanceCountersEma StaticChunkRowReadDataWeight;
    TPerformanceCountersEma StaticChunkRowLookup;
    TPerformanceCountersEma StaticChunkRowLookupDataWeight;
};

DEFINE_REFCOUNTED_TYPE(TChunkReaderPerformanceCounters)

////////////////////////////////////////////////////////////////////////////////

struct TTabletPerformanceCounters
    : public TChunkReaderPerformanceCounters
{
    TPerformanceCountersEma DynamicRowRead;
    TPerformanceCountersEma DynamicRowReadDataWeight;
    TPerformanceCountersEma DynamicRowLookup;
    TPerformanceCountersEma DynamicRowLookupDataWeight;
    TPerformanceCountersEma DynamicRowWrite;
    TPerformanceCountersEma DynamicRowWriteDataWeight;
    TPerformanceCountersEma DynamicRowDelete;
    TPerformanceCountersEma CompactionDataWeight;
    TPerformanceCountersEma PartitioningDataWeight;
    TPerformanceCountersEma LookupError;
    TPerformanceCountersEma WriteError;
};

DEFINE_REFCOUNTED_TYPE(TTabletPerformanceCounters)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EDataSource,
    (DynamicStore)
    (ChunkStore)
);

DEFINE_ENUM(ERequestType,
    (Lookup)
    (Read)
);

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateVersionedPerformanceCountingReader(
    IVersionedReaderPtr reader,
    TTabletPerformanceCountersPtr performanceCounters,
    EDataSource source,
    ERequestType type);

ISchemafulUnversionedReaderPtr CreateSchemafulPerformanceCountingReader(
    ISchemafulUnversionedReaderPtr reader,
    TTabletPerformanceCountersPtr performanceCounters,
    EDataSource source,
    ERequestType type);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

