#pragma once

#include "public.h"
#include "artifact.h"
#include "private.h"

#include <yt/yt/server/node/data_node/blob_chunk.h>
#include <yt/yt/server/node/data_node/public.h>

#include <yt/yt/server/lib/node/chunk_location.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class TArtifact
    : public TAsyncCacheValueBase<TArtifactKey, TArtifact>
{
public:
    TArtifact(
        TCacheLocationPtr location,
        const NNode::TChunkDescriptor& descriptor,
        const TArtifactKey& key,
        NChunkClient::TRefCountedChunkMetaPtr meta,
        TClosure destroyedHandler);

    ~TArtifact();

    const std::string& GetFileName() const;
    const TCacheLocationPtr& GetLocation() const;

    i64 GetDiskSpace() const;

    // TODO(pogorelov): Make better name than chunk id.
    NChunkClient::TChunkId GetId() const;

    const NChunkClient::TRefCountedChunkMetaPtr& GetMeta() const;

private:
    const NChunkClient::TChunkId Id_;

    const TClosure DestroyedHandler_;

    const std::string FileName_;

    const TCacheLocationPtr Location_;

    NChunkClient::TRefCountedChunkMetaPtr Meta_;

    i64 DiskSpace_;
};

DEFINE_REFCOUNTED_TYPE(TArtifact)

////////////////////////////////////////////////////////////////////////////////

class TCacheLocation
    : public NNode::TChunkLocationBase
{
public:
    TCacheLocation(
        TString id,
        NDataNode::TCacheLocationConfigPtr config,
        const NClusterNode::IBootstrap* bootstrap,
        TArtifactCachePtr artifactCache);

    const NDataNode::TCacheLocationConfigPtr& GetStaticConfig() const;

    //! Updates the runtime configuration.
    void Reconfigure(NDataNode::TCacheLocationConfigPtr config);

    NConcurrency::IThroughputThrottlerPtr GetInThrottler() const;

    bool ScheduleDisable(const TError& reason) override;

    const std::string& GetMediumName() const;

private:
    const NDataNode::TCacheLocationConfigPtr StaticConfig_;
    const NConcurrency::IReconfigurableThroughputThrottlerPtr InThrottler_;
    const TArtifactCachePtr ArtifactCache_;

    const std::string MediumName_;

    const NClusterNode::IBootstrap* const Bootstrap_;

    TFuture<void> RemoveChunks();

    std::optional<NNode::TChunkDescriptor> Repair(NDataNode::TChunkId chunkId, const TString& metaSuffix);
    std::optional<NNode::TChunkDescriptor> RepairChunk(NDataNode::TChunkId chunkId) override;

    std::vector<TString> GetChunkPartNames(NDataNode::TChunkId chunkId) const override;

    NNode::TBriefChunkLocationConfig GetBriefConfig() const;
};

DEFINE_REFCOUNTED_TYPE(TCacheLocation)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
