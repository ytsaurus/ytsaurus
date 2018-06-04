#include "private.h"
#include "tablet.h"
#include "tablet_profiling.h"

#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/ytlib/chunk_client/data_statistics.h>
#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/core/profiling/profile_manager.h>
#include <yt/core/profiling/profiler.h>

#include <yt/core/misc/tls_cache.h>
#include <yt/core/misc/farm_hash.h>

namespace NYT {
namespace NTabletNode {

using namespace NProfiling;
using namespace NChunkClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

struct TUserTagTrait
{
    using TKey = TString;
    using TValue = TTagId;

    static const TString& ToKey(const TString& user)
    {
        return user;
    }

    static TTagId ToValue(const TString& user)
    {
        return TProfileManager::Get()->RegisterTag("user", user);
    }
};

TTagIdList AddUserTag(const TString& user, TTagIdList tags)
{
    tags.push_back(GetLocallyCachedValue<TUserTagTrait>(user));
    return tags;
}

////////////////////////////////////////////////////////////////////////////////

TSimpleProfilerTraitBase::TKey TSimpleProfilerTraitBase::ToKey(const TTagIdList& list)
{
    // list.back() is user tag.
    return list.back();
}

////////////////////////////////////////////////////////////////////////////////

TListProfilerTraitBase::TKey TListProfilerTraitBase::ToKey(const TTagIdList& list)
{
    return list;
}

////////////////////////////////////////////////////////////////////////////////

struct TChunkWriteCounters
{
    explicit TChunkWriteCounters(const TTagIdList& list)
        : DiskBytesWritten("/disk_bytes_written", list)
        , DiskDataWeightWritten("/disk_data_weight_written", list)
        , CompressionCpuTime("/chunk_writer/compression_cpu_time", list)
    { }

    TMonotonicCounter DiskBytesWritten;
    TMonotonicCounter DiskDataWeightWritten;
    TMonotonicCounter CompressionCpuTime;
};

using TChunkWriteProfilerTrait = TListProfilerTrait<TChunkWriteCounters>;

void ProfileChunkWriter(
    TTabletSnapshotPtr tabletSnapshot,
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
    TabletNodeProfiler.Increment(counters.DiskBytesWritten, diskSpace);
    TabletNodeProfiler.Increment(counters.DiskDataWeightWritten, dataStatistics.data_weight());
    TabletNodeProfiler.Increment(counters.CompressionCpuTime, DurationToValue(compressionCpuTime));
}

struct TChunkReadCounters
{
    explicit TChunkReadCounters(const TTagIdList& list)
        : CompressedDataSize("/chunk_reader/compressed_data_size", list)
        , UnmergedDataWeight("/chunk_reader/unmerged_data_weight", list)
        , CompressionCpuTime("/chunk_reader/compression_cpu_time", list)
        , ChunkReaderStatisticsCounters("/chunk_reader_statistics", list)
    { }

    TMonotonicCounter CompressedDataSize;
    TMonotonicCounter UnmergedDataWeight;
    TMonotonicCounter CompressionCpuTime;
    TChunkReaderStatisticsCounters ChunkReaderStatisticsCounters;
};

using TChunkReadProfilerTrait = TListProfilerTrait<TChunkReadCounters>;

void ProfileChunkReader(
    TTabletSnapshotPtr tabletSnapshot,
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
    TabletNodeProfiler.Increment(counters.CompressionCpuTime, DurationToValue(compressionCpuTime));
    counters.ChunkReaderStatisticsCounters.Increment(TabletNodeProfiler, chunkReaderStatistics);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
