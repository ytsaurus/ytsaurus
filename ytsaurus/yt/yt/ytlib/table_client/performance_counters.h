#pragma once

#include "public.h"
#include <yt/yt/ytlib/table_client/versioned_chunk_reader.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TChunkReaderPerformanceCounters
    : public TRefCounted
{
    std::atomic<i64> StaticChunkRowReadCount = 0;
    std::atomic<i64> StaticChunkRowReadDataWeight = 0;
    std::atomic<i64> StaticChunkRowLookupCount = 0;
    std::atomic<i64> StaticChunkRowLookupDataWeight = 0;
};

DEFINE_REFCOUNTED_TYPE(TChunkReaderPerformanceCounters)

////////////////////////////////////////////////////////////////////////////////

struct TTabletPerformanceCounters
    : public TChunkReaderPerformanceCounters
{
    std::atomic<i64> DynamicRowReadCount = 0;
    std::atomic<i64> DynamicRowReadDataWeight = 0;
    std::atomic<i64> DynamicRowLookupCount = 0;
    std::atomic<i64> DynamicRowLookupDataWeight = 0;
    std::atomic<i64> DynamicRowWriteCount = 0;
    std::atomic<i64> DynamicRowWriteDataWeight = 0;
    std::atomic<i64> DynamicRowDeleteCount = 0;
    std::atomic<i64> CompactionDataWeight = 0;
    std::atomic<i64> PartitioningDataWeight = 0;
    std::atomic<i64> LookupErrorCount = 0;
    std::atomic<i64> WriteErrorCount = 0;
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

