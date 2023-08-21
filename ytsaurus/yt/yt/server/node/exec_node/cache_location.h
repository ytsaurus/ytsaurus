#pragma once

#include "public.h"
#include "private.h"

#include <yt/yt/server/node/data_node/blob_chunk.h>
#include <yt/yt/server/node/data_node/location.h>
#include <yt/yt/server/node/data_node/public.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

//! A blob chunk owned by TChunkCache.
class TCachedBlobChunk
    : public NDataNode::TBlobChunkBase
    , public TAsyncCacheValueBase<NDataNode::TArtifactKey, TCachedBlobChunk>
{
public:
    TCachedBlobChunk(
        NDataNode::TChunkContextPtr context,
        NDataNode::TChunkLocationPtr location,
        const NDataNode::TChunkDescriptor& descriptor,
        NChunkClient::TRefCountedChunkMetaPtr meta,
        const NDataNode::TArtifactKey& key,
        TClosure destroyedHandler);

    ~TCachedBlobChunk();

private:
    const TClosure DestroyedHandler_;
};

DEFINE_REFCOUNTED_TYPE(TCachedBlobChunk)

////////////////////////////////////////////////////////////////////////////////

class TCacheLocation
    : public NDataNode::TChunkLocation
{
public:
    TCacheLocation(
        TString id,
        NDataNode::TCacheLocationConfigPtr config,
        NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
        NDataNode::TChunkContextPtr chunkContext,
        NDataNode::IChunkStoreHostPtr chunkStoreHost,
        TChunkCachePtr chunkCache);

    const NConcurrency::IThroughputThrottlerPtr& GetInThrottler() const;

    bool ScheduleDisable(const TError& reason) override;

private:
    const NConcurrency::IThroughputThrottlerPtr InThrottler_;
    const TChunkCachePtr ChunkCache_;

    TFuture<void> RemoveChunks();

    std::optional<NDataNode::TChunkDescriptor> Repair(NDataNode::TChunkId chunkId, const TString& metaSuffix);
    std::optional<NDataNode::TChunkDescriptor> RepairChunk(NDataNode::TChunkId chunkId) override;

    std::vector<TString> GetChunkPartNames(NDataNode::TChunkId chunkId) const override;

};

DEFINE_REFCOUNTED_TYPE(TCacheLocation)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
