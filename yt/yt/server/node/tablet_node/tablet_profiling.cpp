#include "private.h"
#include "tablet.h"
#include "tablet_profiling.h"

#include <yt/server/lib/misc/profiling_helpers.h>

#include <yt/client/chunk_client/data_statistics.h>

#include <yt/client/table_client/versioned_reader.h>

#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/ytlib/chunk_client/chunk_writer_base.h>
#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/ytlib/table_client/versioned_chunk_writer.h>
#include <yt/ytlib/table_client/config.h>

#include <yt/core/profiling/profile_manager.h>
#include <yt/core/profiling/profiler.h>

#include <yt/core/misc/tls_cache.h>
#include <yt/core/misc/farm_hash.h>

namespace NYT::NTabletNode {

using namespace NProfiling;
using namespace NChunkClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

struct TChunkWriteCounters
{
    explicit TChunkWriteCounters(const TTagIdList& tagIds)
        : DiskSpace("/chunk_writer/disk_space", tagIds)
        , DataWeight("/chunk_writer/data_weight", tagIds)
        , CompressionCpuTime("/chunk_writer/compression_cpu_time", tagIds)
    { }

    TMonotonicCounter DiskSpace;
    TMonotonicCounter DataWeight;
    TMonotonicCounter CompressionCpuTime;
};

using TChunkWriteProfilerTrait = TTagListProfilerTrait<TChunkWriteCounters>;

void ProfileChunkWriter(
    const TTabletSnapshotPtr& tabletSnapshot,
    const TDataStatistics& dataStatistics,
    const TCodecStatistics& codecStatistics,
    TTagId methodTag)
{
    auto diskSpace = CalculateDiskSpaceUsage(
        tabletSnapshot->WriterOptions->ReplicationFactor,
        dataStatistics.regular_disk_space(),
        dataStatistics.erasure_disk_space());
    auto compressionCpuTime = codecStatistics.GetTotalDuration();
    auto tags = tabletSnapshot->DiskProfilerTags;
    tags.push_back(methodTag);
    auto& counters = GetLocallyGloballyCachedValue<TChunkWriteProfilerTrait>(tags);
    TabletNodeProfiler.Increment(counters.DiskSpace, diskSpace);
    TabletNodeProfiler.Increment(counters.DataWeight, dataStatistics.data_weight());
    TabletNodeProfiler.Increment(counters.CompressionCpuTime, DurationToValue(compressionCpuTime));
}

////////////////////////////////////////////////////////////////////////////////

struct TChunkReadCounters
{
    explicit TChunkReadCounters(const TTagIdList& tagIds)
        : CompressedDataSize("/chunk_reader/compressed_data_size", tagIds)
        , UnmergedDataWeight("/chunk_reader/unmerged_data_weight", tagIds)
        , DecompressionCpuTime("/chunk_reader/decompression_cpu_time", tagIds)
        , ChunkReaderStatisticsCounters("/chunk_reader_statistics", tagIds)
    { }

    TMonotonicCounter CompressedDataSize;
    TMonotonicCounter UnmergedDataWeight;
    TMonotonicCounter DecompressionCpuTime;
    TChunkReaderStatisticsCounters ChunkReaderStatisticsCounters;
};

using TChunkReadProfilerTrait = TTagListProfilerTrait<TChunkReadCounters>;

void ProfileChunkReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    const TDataStatistics& dataStatistics,
    const TCodecStatistics& codecStatistics,
    const TChunkReaderStatisticsPtr& chunkReaderStatistics,
    TTagId methodTag)
{
    auto compressionCpuTime = codecStatistics.GetTotalDuration();
    auto tags = tabletSnapshot->DiskProfilerTags;
    tags.push_back(methodTag);
    auto& counters = GetLocallyGloballyCachedValue<TChunkReadProfilerTrait>(tags);
    TabletNodeProfiler.Increment(counters.CompressedDataSize, dataStatistics.compressed_data_size());
    TabletNodeProfiler.Increment(counters.UnmergedDataWeight, dataStatistics.data_weight());
    TabletNodeProfiler.Increment(counters.DecompressionCpuTime, DurationToValue(compressionCpuTime));
    counters.ChunkReaderStatisticsCounters.Increment(TabletNodeProfiler, chunkReaderStatistics);
}

////////////////////////////////////////////////////////////////////////////////

struct TDynamicMemoryUsageCounters
{
    explicit TDynamicMemoryUsageCounters(const TTagIdList& tagIds)
        : DynamicMemoryUsage("/dynamic_memory_usage", tagIds)
    { }

    TSimpleGauge DynamicMemoryUsage;
};

using TDynamicMemoryProfilerTrait = TTagListProfilerTrait<TDynamicMemoryUsageCounters>;

void ProfileDynamicMemoryUsage(
    const NProfiling::TTagIdList& tags,
    ETabletDynamicMemoryType memoryType,
    i64 memoryUsage)
{
    static const NProfiling::TEnumMemberTagCache<ETabletDynamicMemoryType> MemoryTypeTagCache("memory_type");
    
    auto allTags = tags;
    allTags.push_back(MemoryTypeTagCache.GetTag(memoryType));
    
    auto& counters = GetLocallyGloballyCachedValue<TDynamicMemoryProfilerTrait>(allTags);
    TabletNodeProfiler.Update(counters.DynamicMemoryUsage, memoryUsage);
}

void TWriterProfiler::Profile(const TTabletSnapshotPtr& tabletSnapshot, TTagId tag)
{
    ProfileChunkWriter(tabletSnapshot, DataStatistics_, CodecStatistics_, tag);
}

void TWriterProfiler::Update(const NTableClient::IVersionedMultiChunkWriterPtr& writer)
{
    if (writer) {
        DataStatistics_ += writer->GetDataStatistics();
        CodecStatistics_ += writer->GetCompressionStatistics();
    }
}

void TWriterProfiler::Update(const IChunkWriterBasePtr& writer)
{
    if (writer) {
        DataStatistics_ += writer->GetDataStatistics();
        CodecStatistics_ += writer->GetCompressionStatistics();
    }
}

void TReaderProfiler::Profile(
    const TTabletSnapshotPtr& tabletSnapshot,
    TTagId tag)
{
    ProfileChunkReader(tabletSnapshot, DataStatistics_, CodecStatistics_, ChunkReaderStatistics_, tag);
}

void TReaderProfiler::Update(
    const NTableClient::IVersionedReaderPtr& reader,
    const NChunkClient::TChunkReaderStatisticsPtr& chunkReaderStatistics)
{
    if (reader) {
        DataStatistics_ += reader->GetDataStatistics();
        CodecStatistics_ += reader->GetDecompressionStatistics();
    }
    ChunkReaderStatistics_ = chunkReaderStatistics;
}

void TReaderProfiler::SetCompressedDataSize(i64 compressedDataSize)
{
    DataStatistics_.set_compressed_data_size(compressedDataSize);
}

void TReaderProfiler::SetCodecStatistics(const TCodecStatistics& codecStatistics)
{
    CodecStatistics_ = codecStatistics;
}

void TReaderProfiler::SetChunkReaderStatistics(const TChunkReaderStatisticsPtr& chunkReaderStatistics)
{
    ChunkReaderStatistics_ = chunkReaderStatistics;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
