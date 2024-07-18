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

class TJobInputCache
    : public IJobInputCache
{
public:
    TJobInputCache(
        NExecNode::IBootstrap* bootstrap,
        NProfiling::TProfiler profiler = JobInputCacheProfiler);

    TFuture<std::vector<NChunkClient::TBlock>> ReadBlocks(
        NChunkClient::TChunkId chunkId,
        const std::vector<int>& blockIndices,
        NChunkClient::IChunkReader::TReadBlocksOptions options) override;

    TFuture<std::vector<NChunkClient::TBlock>> ReadBlocks(
        NChunkClient::TChunkId chunkId,
        int firstBlockCount,
        int blockCount,
        NChunkClient::IChunkReader::TReadBlocksOptions options) override;

    TFuture<NChunkClient::TRefCountedChunkMetaPtr> GetChunkMeta(
        NChunkClient::TChunkId chunkId,
        NChunkClient::TClientChunkReadOptions options,
        std::optional<int> partitionTag,
        const std::optional<std::vector<int>>& extensionTags) override;

    THashSet<NChunkClient::TChunkId> FilterHotChunkIds(const std::vector<NChunkClient::TChunkId>& chunkIds) override;

    void RegisterJobChunks(
        TJobId jobId,
        THashMap<NChunkClient::TChunkId, TRefCountedChunkSpecPtr> chunkSpecs) override;

    void UnregisterJobChunks(TJobId jobId) override;

    bool IsChunkCached(NChunkClient::TChunkId chunkId) override;

    bool IsEnabled() override;

    void Reconfigure(const NExecNode::TJobInputCacheDynamicConfigPtr& config) override;

    bool IsBlockCacheMemoryLimitExceeded() override;

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock_);

    NExecNode::IBootstrap* const Bootstrap_;

    const IMemoryUsageTrackerPtr BlockMemoryTracker_;
    const IMemoryUsageTrackerPtr MetaMemoryTracker_;

    const NChunkClient::IClientBlockCachePtr BlockCache_;
    const NChunkClient::IClientChunkMetaCachePtr MetaCache_;

    const NChunkClient::TChunkReaderHostPtr ChunkReaderHost_;

    const NProfiling::TProfiler Profiler_;

    TAtomicIntrusivePtr<NExecNode::TJobInputCacheDynamicConfig> Config_;

    THashMap<TJobId, THashSet<NChunkClient::TChunkId>> JobIdToChunkIds_;
    THashMap<NChunkClient::TChunkId, THashSet<TJobId>> ChunkIdToJobIds_;

    THashMap<NChunkClient::TChunkId, TRefCountedChunkSpecPtr> ChunkIdToSpec_;
    THashMap<NChunkClient::TChunkId, NChunkClient::IChunkReaderPtr> ChunkIdToReader_;

    NChunkClient::IChunkReaderPtr GetOrCreateReaderForChunk(NChunkClient::TChunkId chunkId);
    NChunkClient::IChunkReaderPtr CreateReaderForChunk(NChunkClient::TChunkId chunkId);

    TRefCountedChunkSpecPtr DoGetChunkSpec(NChunkClient::TChunkId chunkId) const;

    TFuture<std::vector<NChunkClient::TBlock>> DoGetBlockSet(
        NChunkClient::TChunkId chunkId,
        const std::vector<int>& blockIndices,
        NChunkClient::IChunkReader::TReadBlocksOptions options);

    TFuture<NChunkClient::TRefCountedChunkMetaPtr> DoGetChunkMeta(
        NChunkClient::TChunkId chunkId,
        NChunkClient::TClientChunkReadOptions options);
};

////////////////////////////////////////////////////////////////////////////////

TJobInputCache::TJobInputCache(
    NExecNode::IBootstrap* bootstrap,
    NProfiling::TProfiler profiler)
    : Bootstrap_(bootstrap)
    , BlockMemoryTracker_(Bootstrap_->GetNodeMemoryUsageTracker()->WithCategory(EMemoryCategory::JobInputBlockCache))
    , MetaMemoryTracker_(Bootstrap_->GetNodeMemoryUsageTracker()->WithCategory(EMemoryCategory::JobInputChunkMetaCache))
    , BlockCache_(CreateClientBlockCache(
        New<TBlockCacheConfig>(),
        EBlockType::CompressedData,
        BlockMemoryTracker_,
        profiler.WithPrefix("/block_cache")))
    , MetaCache_(CreateClientChunkMetaCache(
        New<TClientChunkMetaCacheConfig>(),
        MetaMemoryTracker_,
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
        return JobIdToChunkIds_.size();
    });

    Profiler_.AddFuncGauge("/chunks", MakeStrong(this), [this] {
        auto guard = ReaderGuard(Lock_);
        return ChunkIdToJobIds_.size();
    });

    Reconfigure(New<TJobInputCacheDynamicConfig>());
}

bool TJobInputCache::IsChunkCached(TChunkId chunkId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(Lock_);
    return ChunkIdToJobIds_.contains(chunkId);
}

bool TJobInputCache::IsBlockCacheMemoryLimitExceeded()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return BlockMemoryTracker_->IsExceeded();
}

bool TJobInputCache::IsEnabled()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Config_.Acquire()->Enabled;
}

void TJobInputCache::Reconfigure(const TJobInputCacheDynamicConfigPtr& config)
{
    VERIFY_THREAD_AFFINITY_ANY();

    BlockCache_->Reconfigure(config->BlockCache);
    MetaCache_->Reconfigure(config->MetaCache);

    BlockMemoryTracker_->SetLimit(BlockMemoryTracker_->GetLimit() + config->TotalInFlightBlockSize);

    Config_.Store(config);
}

THashSet<TChunkId> TJobInputCache::FilterHotChunkIds(const std::vector<TChunkId>& chunkIds)
{
    auto guard = ReaderGuard(Lock_);

    THashSet<TChunkId> hotChunkIds;

    if (IsBlockCacheMemoryLimitExceeded()) {
        return {};
    }

    auto threshold = Config_.Acquire()->JobCountThreshold;

    if (!threshold) {
        return {};
    }

    for (const auto& chunkId : chunkIds) {
        auto chunkIt = ChunkIdToJobIds_.find(chunkId);
        if (!chunkIt.IsEnd() && std::ssize(chunkIt->second) >= *threshold) {
            EmplaceOrCrash(hotChunkIds, chunkId);
        }
    }

    return hotChunkIds;
}

void TJobInputCache::RegisterJobChunks(
    TJobId jobId,
    THashMap<TChunkId, TRefCountedChunkSpecPtr> chunkSpecs)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = WriterGuard(Lock_);

    // Insert job.
    auto& jobChunks = EmplaceOrCrash(JobIdToChunkIds_, jobId, THashSet<TChunkId>())->second;

    for (auto& [chunkId, chunkSpec] : chunkSpecs) {
        // Create empty entry if chunk id not found.
        auto& chunkJobs = ChunkIdToJobIds_[chunkId];

        // Add chunk for job.
        EmplaceOrCrash(jobChunks, chunkId);

        // Add job for chunk.
        EmplaceOrCrash(chunkJobs, jobId);

        // Insert chunk spec.
        ChunkIdToSpec_.emplace(std::move(chunkId), std::move(chunkSpec));
    }
}

void TJobInputCache::UnregisterJobChunks(TJobId jobId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = WriterGuard(Lock_);

    // Get job chunks.
    auto jobIt = JobIdToChunkIds_.find(jobId);

    if (jobIt.IsEnd()) {
        return;
    }

    const auto& chunkIds = jobIt->second;

    for (const auto& chunkId : chunkIds) {
        // Remove job from chunk.
        auto chunkIt = GetIteratorOrCrash(ChunkIdToJobIds_, chunkId);
        auto& jobIds = chunkIt->second;

        EraseOrCrash(jobIds, jobId);

        // If chunk jobs is empty, than clean chunk specs and readers.
        if (jobIds.empty()) {
            ChunkIdToJobIds_.erase(chunkIt);
            ChunkIdToReader_.erase(chunkId);
            EraseOrCrash(ChunkIdToSpec_, chunkId);
        }
    }

    // Remove job from jobs.
    JobIdToChunkIds_.erase(jobIt);
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
    options.MemoryUsageTracker = MetaMemoryTracker_;

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
    options.ClientOptions.MemoryUsageTracker = BlockMemoryTracker_;

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
    options.ClientOptions.MemoryUsageTracker = BlockMemoryTracker_;

    return GetOrCreateReaderForChunk(chunkId)->ReadBlocks(options, blockIndices)
        .ToUncancelable();
}

TRefCountedChunkSpecPtr TJobInputCache::DoGetChunkSpec(TChunkId chunkId) const
{
    auto specIt = ChunkIdToSpec_.find(chunkId);
    return specIt.IsEnd() ? nullptr : specIt->second;
}

IChunkReaderPtr TJobInputCache::GetOrCreateReaderForChunk(TChunkId chunkId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = WriterGuard(Lock_);

    auto readerIt = ChunkIdToReader_.find(chunkId);

    if (readerIt.IsEnd()) {
        auto reader = CreateReaderForChunk(chunkId);
        ChunkIdToReader_.insert({chunkId, reader});
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
    erasureReaderConfig->UseChunkProber = true;
    erasureReaderConfig->UseReadBlocksBatcher = true;

    return CreateRemoteReader(
        *spec,
        std::move(erasureReaderConfig),
        New<TRemoteReaderOptions>(),
        ChunkReaderHost_);
}

////////////////////////////////////////////////////////////////////////////////

IJobInputCachePtr CreateJobInputCache(NExecNode::IBootstrap* bootstrap)
{
    return New<TJobInputCache>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
