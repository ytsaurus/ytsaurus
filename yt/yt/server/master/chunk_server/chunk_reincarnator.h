#pragma once

#include "job.h"
#include "job_controller.h"

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TReincarnationJob
    : public TJob
{
public:
    DEFINE_BYREF_RO_PROPERTY(TChunkId, OldChunkId);
    DEFINE_BYREF_RO_PROPERTY(TChunkId, NewChunkId);

public:
    TReincarnationJob(
        TJobId jobId,
        TChunk* oldChunk,
        TJobEpoch jobEpoch,
        NChunkClient::TChunkIdWithIndexes newChunkId,
        NNodeTrackerServer::TNode* node,
        TNodePtrWithReplicaIndexList sourceReplicas,
        TNodePtrWithReplicaAndMediumIndexList targetReplicas,
        int mediumIndex);

    bool FillJobSpec(
        NCellMaster::TBootstrap* bootstrap,
        NJobTrackerClient::NProto::TJobSpec* jobSpec) const override;

private:
    TNodePtrWithReplicaIndexList SourceReplicas_;
    TNodePtrWithReplicaAndMediumIndexList TargetReplicas_;
    NErasure::ECodec ErasureCodec_;
    NCompression::ECodec CompressionCodec_;
    bool EnableSkynetSharing_;
    int MediumIndex_;

    static NNodeTrackerClient::NProto::TNodeResources GetJobResourceUsage();
};

DECLARE_REFCOUNTED_TYPE(TReincarnationJob);
DEFINE_REFCOUNTED_TYPE(TReincarnationJob);

////////////////////////////////////////////////////////////////////////////////

class IChunkReincarnator
    : public virtual ITypedJobController<TReincarnationJob>
{
public:
    virtual void Initialize() = 0;

    virtual void OnChunkDestroyed(TChunk* chunk) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkReincarnator)

////////////////////////////////////////////////////////////////////////////////

IChunkReincarnatorPtr CreateChunkReincarnator(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
