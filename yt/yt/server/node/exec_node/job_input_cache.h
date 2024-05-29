#pragma once

#include "public.h"
#include "bootstrap.h"

#include <yt/yt/server/node/data_node/chunk.h>

#include <yt/yt/server/node/exec_node/job_controller.h>

#include <yt/yt/server/lib/exec_node/proxying_data_node_service_helpers.h>

#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/block_id.h>

#include <yt/yt/client/job_tracker_client/public.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/async_slru_cache.h>
#include <yt/yt/core/misc/property.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class TJobInputCache
    : public virtual TRefCounted
{
public:
    TJobInputCache(
        NExecNode::IBootstrap* bootstrap,
        NProfiling::TProfiler profiler = JobInputCacheProfiler);

    TFuture<std::vector<NChunkClient::TBlock>> ReadBlocks(
        NChunkClient::TChunkId chunkId,
        const std::vector<int>& blockIndices,
        NChunkClient::IChunkReader::TReadBlocksOptions options);

    TFuture<std::vector<NChunkClient::TBlock>> ReadBlocks(
        NChunkClient::TChunkId chunkId,
        int firstBlockCount,
        int blockCount,
        NChunkClient::IChunkReader::TReadBlocksOptions options);

    TFuture<NChunkClient::TRefCountedChunkMetaPtr> GetChunkMeta(
        NChunkClient::TChunkId chunkId,
        NChunkClient::TClientChunkReadOptions options,
        std::optional<int> partitionTag,
        const std::optional<std::vector<int>>& extensionTags);

    THashSet<NChunkClient::TChunkId> FilterHotChunks(const std::vector<NChunkClient::TChunkId>& chunkSpecs);

    void RegisterJobChunks(
        TJobId jobId,
        THashMap<NChunkClient::TChunkId, TRefCountedChunkSpecPtr> chunkSpecs);

    void UnregisterJobChunks(TJobId jobId);

    bool IsCachedChunk(NChunkClient::TChunkId chunkId) const;

    bool IsEnabled();

    void Reconfigure(const NExecNode::TJobInputCacheDynamicConfigPtr& config);

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock_);

    NExecNode::IBootstrap* const Bootstrap_;

    const NChunkClient::IClientBlockCachePtr BlockCache_;
    const NChunkClient::IClientChunkMetaCachePtr MetaCache_;

    const NChunkClient::TChunkReaderHostPtr ChunkReaderHost_;

    const NProfiling::TProfiler Profiler_;

    NProfiling::TCounter InputRequests_;
    NProfiling::TCounter OutputRequests_;

    NProfiling::TCounter RequestedBytes_;
    NProfiling::TCounter ProxyingBytes_;

    TAtomicIntrusivePtr<NExecNode::TJobInputCacheDynamicConfig> Config_;

    THashMap<TJobId, THashSet<NChunkClient::TChunkId>> JobToChunks_;
    THashMap<NChunkClient::TChunkId, THashSet<TJobId>> ChunkToJobs_;

    THashMap<NChunkClient::TChunkId, TRefCountedChunkSpecPtr> ChunkToSpec_;
    THashMap<NChunkClient::TChunkId, NChunkClient::IChunkReaderPtr> ChunksToReader_;

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

DEFINE_REFCOUNTED_TYPE(TJobInputCache)

////////////////////////////////////////////////////////////////////////////////

TJobInputCachePtr CreateJobInputCache(NExecNode::IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
