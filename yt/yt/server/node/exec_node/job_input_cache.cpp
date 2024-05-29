#include "job_input_cache.h"

#include "bootstrap.h"
#include "private.h"

#include <yt/yt/server/node/data_node/bootstrap.h>
#include <yt/yt/server/node/data_node/chunk_store.h>

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/ytlib/exec_node_tracker_client/exec_node_tracker_service_proxy.h>

#include <yt/yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/client/chunk_client/helpers.h>

#include <yt/yt/core/concurrency/retrying_periodic_executor.h>

#include <yt/yt/core/misc/sync_cache.h>

#include <yt/yt/core/rpc/helpers.h>

#include <yt/yt/core/utilex/random.h>

namespace NYT::NExecNode {

using namespace NChunkClient;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NExecNodeTrackerClient;
using namespace NExecNodeTrackerClient::NProto;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NProto;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

TJobInputCache::TJobInputCache(
    NExecNode::IBootstrap* bootstrap,
    NProfiling::TProfiler profiler)
    : Bootstrap_(bootstrap)
    , BlockCache_(CreateClientBlockCache(
        New<TBlockCacheConfig>(),
        EBlockType::CompressedData,
        Bootstrap_->GetNodeMemoryUsageTracker()->WithCategory(EMemoryCategory::JobInputBlockCache),
        profiler.WithPrefix("/block_cache")))
    , MetaCache_(CreateClientChunkMetaCache(
        New<TClientChunkMetaCacheConfig>(),
        Bootstrap_->GetNodeMemoryUsageTracker()->WithCategory(EMemoryCategory::JobInputChunkMetaCache),
        profiler.WithPrefix("/meta_cache")))
    , ChunkReaderHost_(New<TChunkReaderHost>(
        Bootstrap_->GetClient(),
        Bootstrap_->GetLocalDescriptor(),
        BlockCache_,
        MetaCache_,
        /*nodeStatusDirectory*/ nullptr,
        Bootstrap_->GetExecNodeBootstrap()->GetThrottler(EExecNodeThrottlerKind::JobIn),
        Bootstrap_->GetReadRpsOutThrottler(),
        /*mediumThrottler*/ GetUnlimitedThrottler(),
        /*trafficMeter*/ nullptr))
    , Profiler_(std::move(profiler))
{
    Profiler_.AddFuncGauge("/registered_job_count", MakeStrong(this), [this] {
        auto guard = ReaderGuard(Lock_);
        return JobToChunks_.size();
    });

    Profiler_.AddFuncGauge("/chunks", MakeStrong(this), [this] {
        auto guard = ReaderGuard(Lock_);
        return ChunkToJobs_.size();
    });

    Reconfigure(New<TJobInputCacheDynamicConfig>());
}

bool TJobInputCache::IsCachedChunk(TChunkId chunkId) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(Lock_);
    return ChunkToJobs_.contains(chunkId);
}

bool TJobInputCache::IsEnabled()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto config = Config_.Acquire();
    return config && config->Enabled;
}

void TJobInputCache::Reconfigure(const TJobInputCacheDynamicConfigPtr& config)
{
    VERIFY_THREAD_AFFINITY_ANY();

    BlockCache_->Reconfigure(config->BlockCache);
    MetaCache_->Reconfigure(config->MetaCache);

    Config_.Store(config);
}

THashSet<TChunkId> TJobInputCache::FilterHotChunks(const std::vector<TChunkId>& chunkSpecs)
{
    auto guard = ReaderGuard(Lock_);

    THashSet<TChunkId> hotChunks;

    auto threshold = Config_.Acquire()->JobCountThreshold;

    if (!threshold) {
        return {};
    }

    for (const auto& chunkId : chunkSpecs) {
        auto chunkIt = ChunkToJobs_.find(chunkId);
        if (chunkIt && std::ssize(chunkIt->second) >= threshold.value()) {
            EmplaceOrCrash(hotChunks, chunkId);
        }
    }

    return hotChunks;
}

void TJobInputCache::RegisterJobChunks(
    TJobId jobId,
    THashMap<TChunkId, TRefCountedChunkSpecPtr> chunkSpecs)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = WriterGuard(Lock_);

    // Insert job.
    auto& jobChunks = EmplaceOrCrash(JobToChunks_, jobId, THashSet<TChunkId>())->second;

    for (auto& [chunkId, chunkSpec] : chunkSpecs) {
        // Create empty entry if chunk id not found.
        auto& chunkJobs = ChunkToJobs_[chunkId];

        // Add chunk for job.
        EmplaceOrCrash(jobChunks, chunkId);

        // Add job for chunk.
        EmplaceOrCrash(chunkJobs, jobId);

        // Insert chunk spec.
        ChunkToSpec_.emplace(std::move(chunkId), std::move(chunkSpec));
    }
}

void TJobInputCache::UnregisterJobChunks(TJobId jobId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = WriterGuard(Lock_);

    // Get job chunks.
    auto jobIt = JobToChunks_.find(jobId);

    if (jobIt.IsEnd()) {
        return;
    }

    const auto& chunkIds = jobIt->second;

    for (const auto& chunkId : chunkIds) {
        // Remove job from chunk.
        auto chunkIt = GetIteratorOrCrash(ChunkToJobs_, chunkId);
        auto& jobIds = chunkIt->second;

        EraseOrCrash(jobIds, jobId);

        // If chunk jobs is empty, than clean chunk specs and readers.
        if (jobIds.empty()) {
            ChunkToJobs_.erase(chunkIt);
            ChunksToReader_.erase(chunkId);
            EraseOrCrash(ChunkToSpec_, chunkId);
        }
    }

    // Remove job from jobs.
    JobToChunks_.erase(jobIt);
}

TFuture<TRefCountedChunkMetaPtr> TJobInputCache::GetChunkMeta(
    TChunkId chunkId,
    TClientChunkReadOptions options,
    std::optional<int> partitionTag,
    const std::optional<std::vector<int>>& extensionTags)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto annotation = Format(
        "Proxying read via exec node (ChunkId: %v)",
        chunkId);
    options.WorkloadDescriptor.Annotations.push_back(std::move(annotation));

    return GetOrCreateReaderForChunk(chunkId)->GetMeta(options, partitionTag, extensionTags)
        .ToUncancelable();
}

TFuture<std::vector<TBlock>> TJobInputCache::ReadBlocks(
    TChunkId chunkId,
    int firstBlockIndex,
    int blockCount,
    IChunkReader::TReadBlocksOptions options)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto annotation = Format(
        "Proxying read via exec node (BlockIds: %v:%v-%v)",
        chunkId,
        firstBlockIndex,
        firstBlockIndex + blockCount - 1);
    options.ClientOptions.WorkloadDescriptor.Annotations.push_back(std::move(annotation));

    return GetOrCreateReaderForChunk(chunkId)->ReadBlocks(options, firstBlockIndex, blockCount)
        .ToUncancelable();
}

TFuture<std::vector<TBlock>> TJobInputCache::ReadBlocks(
    TChunkId chunkId,
    const std::vector<int>& blockIndices,
    IChunkReader::TReadBlocksOptions options)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto annotation = Format(
        "Proxying read via exec node (BlockIds: %v:%v)",
        chunkId,
        MakeShrunkFormattableView(blockIndices, TDefaultFormatter(), 3));
    options.ClientOptions.WorkloadDescriptor.Annotations.push_back(std::move(annotation));

    return GetOrCreateReaderForChunk(chunkId)->ReadBlocks(options, blockIndices)
        .ToUncancelable();
}

TRefCountedChunkSpecPtr TJobInputCache::DoGetChunkSpec(TChunkId chunkId) const
{
    auto specIt = ChunkToSpec_.find(chunkId);
    return specIt.IsEnd() ? nullptr : specIt->second;
}

IChunkReaderPtr TJobInputCache::GetOrCreateReaderForChunk(TChunkId chunkId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = WriterGuard(Lock_);

    auto readerIt = ChunksToReader_.find(chunkId);

    if (readerIt.IsEnd()) {
        auto reader = CreateReaderForChunk(chunkId);
        ChunksToReader_.insert({chunkId, reader});
        return reader;
    } else {
        return readerIt->second;
    }
}

IChunkReaderPtr TJobInputCache::CreateReaderForChunk(TChunkId chunkId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto spec = DoGetChunkSpec(chunkId);

    if (!spec) {
        THROW_ERROR_EXCEPTION("Chunk spec not found")
            << TErrorAttribute("chunk_id", chunkId);
    }

    auto erasureReaderConfig = New<TErasureReaderConfig>();
    erasureReaderConfig->EnableAutoRepair = true;

    return CreateRemoteReader(
        *spec,
        std::move(erasureReaderConfig),
        New<TRemoteReaderOptions>(),
        ChunkReaderHost_);
}

////////////////////////////////////////////////////////////////////////////////

TJobInputCachePtr CreateJobInputCache(NExecNode::IBootstrap* bootstrap)
{
    return New<TJobInputCache>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
