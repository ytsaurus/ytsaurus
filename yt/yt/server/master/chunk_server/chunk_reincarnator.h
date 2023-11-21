#pragma once

#include "job.h"
#include "job_controller.h"

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TChunkReincarnationOptions;

} // namespace NProto

struct TChunkReincarnationOptions
{
    bool IgnoreCreationTime = false;
    bool IgnoreAccountSettings = false;
};

void FromProto(
    TChunkReincarnationOptions* options,
    const NProto::TChunkReincarnationOptions& protoOptions);

void ToProto(
    NProto::TChunkReincarnationOptions* protoOptions,
    const TChunkReincarnationOptions& options);

void Deserialize(TChunkReincarnationOptions& options, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

struct TReincarnationJob
    : public TJob
{
public:
    DEFINE_BYREF_RO_PROPERTY(TChunkId, OldChunkId);
    DEFINE_BYREF_RO_PROPERTY(TChunkId, NewChunkId);
    DEFINE_BYVAL_RO_PROPERTY(TChunkReincarnationOptions, ReincarnationOptions);

public:
    TReincarnationJob(
        TJobId jobId,
        TChunk* oldChunk,
        TJobEpoch jobEpoch,
        NChunkClient::TChunkIdWithIndexes newChunkId,
        NNodeTrackerServer::TNode* node,
        TNodePtrWithReplicaAndMediumIndexList sourceReplicas,
        TNodePtrWithReplicaAndMediumIndexList targetReplicas,
        int mediumIndex,
        TChunkReincarnationOptions reincarnationOptions);

    bool FillJobSpec(
        NCellMaster::TBootstrap* bootstrap,
        NJobTrackerClient::NProto::TJobSpec* jobSpec) const override;

private:
    const TNodePtrWithReplicaAndMediumIndexList SourceReplicas_;
    const TNodePtrWithReplicaAndMediumIndexList TargetReplicas_;
    const int MediumIndex_;
    NErasure::ECodec ErasureCodec_;
    NCompression::ECodec CompressionCodec_;
    bool EnableSkynetSharing_;

    static NNodeTrackerClient::NProto::TNodeResources GetJobResourceUsage();
};

DECLARE_REFCOUNTED_TYPE(TReincarnationJob)
DEFINE_REFCOUNTED_TYPE(TReincarnationJob)

////////////////////////////////////////////////////////////////////////////////

class IChunkReincarnator
    : public virtual ITypedJobController<TReincarnationJob>
{
public:
    virtual void Initialize() = 0;

    virtual void OnChunkDestroyed(TChunk* chunk) = 0;

    virtual void ScheduleReincarnation(
        TChunkTree* chunk,
        TChunkReincarnationOptions options) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkReincarnator)

////////////////////////////////////////////////////////////////////////////////

IChunkReincarnatorPtr CreateChunkReincarnator(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
