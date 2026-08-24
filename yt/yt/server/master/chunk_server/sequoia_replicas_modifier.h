#pragma once

#include "public.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/public.h>
#include <yt/yt/ytlib/sequoia_client/public.h>

#include <yt/yt/ytlib/data_node_tracker_client/proto/data_node_tracker_service.pb.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TSequoiaReplicaModificationProfile
{
    TEnumIndexedArray<ESequoiaReplicaModificationPhase, NProfiling::TSummary> PhaseTime;

    NProfiling::TCounter StartedCount;
    NProfiling::TCounter StartedReplicaCount;

    NProfiling::TGauge SemaphoreWaiting;
    int SemaphoreWaitingCount = 0;
    NProfiling::TGauge SemaphoreWaitingReplicas;
    int SemaphoreWaitingReplicaCount = 0;

    NProfiling::TCounter FinishedSuccessfullyCount;
    NProfiling::TCounter FinishedSuccessfullyReplicaCount;
    NProfiling::TCounter FinishedWithErrorCount;
    NProfiling::TCounter FinishedWithErrorReplicaCount;
};

////////////////////////////////////////////////////////////////////////////////

struct ISequoiaReplicasModifier
    : public TRefCounted
{
    virtual void AddRequest(
        std::unique_ptr<NDataNodeTrackerClient::NProto::TReqModifyReplicas> request) = 0;

    virtual void AddRequest(
        std::unique_ptr<NDataNodeTrackerClient::NProto::TReqReplaceLocationReplicas> request) = 0;

    virtual TFuture<void> ModifyReplicas() = 0;
};

DEFINE_REFCOUNTED_TYPE(ISequoiaReplicasModifier)

////////////////////////////////////////////////////////////////////////////////

ISequoiaReplicasModifierPtr CreateSequoiaReplicasModifier(
    TSequoiaReplicaModificationProfile& modificationProfile,
    NSequoiaClient::ESequoiaTransactionType,
    NCellMaster::TBootstrap* bootstrap,
    const TDynamicChunkManagerConfigPtr& config);

} // namespace NYT::NChunkServer
