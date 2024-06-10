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

struct IJobInputCache
    : public virtual TRefCounted
{
public:
    virtual TFuture<std::vector<NChunkClient::TBlock>> ReadBlocks(
        NChunkClient::TChunkId chunkId,
        const std::vector<int>& blockIndices,
        NChunkClient::IChunkReader::TReadBlocksOptions options) = 0;

    virtual TFuture<std::vector<NChunkClient::TBlock>> ReadBlocks(
        NChunkClient::TChunkId chunkId,
        int firstBlockCount,
        int blockCount,
        NChunkClient::IChunkReader::TReadBlocksOptions options) = 0;

    virtual TFuture<NChunkClient::TRefCountedChunkMetaPtr> GetChunkMeta(
        NChunkClient::TChunkId chunkId,
        NChunkClient::TClientChunkReadOptions options,
        std::optional<int> partitionTag,
        const std::optional<std::vector<int>>& extensionTags) = 0;

    virtual THashSet<NChunkClient::TChunkId> FilterHotChunkIds(const std::vector<NChunkClient::TChunkId>& chunkIds) = 0;

    virtual void RegisterJobChunks(
        TJobId jobId,
        THashMap<NChunkClient::TChunkId, TRefCountedChunkSpecPtr> chunkSpecs) = 0;

    virtual void UnregisterJobChunks(TJobId jobId) = 0;

    virtual bool IsChunkCached(NChunkClient::TChunkId chunkId) = 0;

    virtual bool IsEnabled() = 0;

    virtual void Reconfigure(const NExecNode::TJobInputCacheDynamicConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobInputCache)

////////////////////////////////////////////////////////////////////////////////

IJobInputCachePtr CreateJobInputCache(NExecNode::IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
