#pragma once

// This header is the first intentionally.
#include <yp/server/lib/misc/public.h>

#include <yp/server/master/public.h>

#include <yp/server/objects/public.h>

#include <yp/server/lib/cluster/public.h>

#include <array>

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TScheduleQueue;
class TAllocationPlan;

DECLARE_REFCOUNTED_STRUCT(IGlobalResourceAllocator)
DECLARE_REFCOUNTED_CLASS(TPodDisruptionBudgetController)

DECLARE_REFCOUNTED_CLASS(TScheduleQueue)
DECLARE_REFCOUNTED_CLASS(TResourceManager)
DECLARE_REFCOUNTED_CLASS(TScheduler)

DECLARE_REFCOUNTED_CLASS(TEveryNodeSelectionStrategyConfig)
DECLARE_REFCOUNTED_CLASS(TPodNodeScoreConfig)
DECLARE_REFCOUNTED_CLASS(TNodeScoreFeatureConfig)
DECLARE_REFCOUNTED_CLASS(TNodeScoreConfig)
DECLARE_REFCOUNTED_CLASS(TGlobalResourceAllocatorConfig)
DECLARE_REFCOUNTED_CLASS(TPodDisruptionBudgetControllerConfig)
DECLARE_REFCOUNTED_CLASS(TSchedulePodsStageConfig)
DECLARE_REFCOUNTED_CLASS(TSchedulerConfig)

using NObjects::EResourceKind;
using NObjects::TObjectId;

DECLARE_REFCOUNTED_STRUCT(IPodNodeScore)
DECLARE_REFCOUNTED_STRUCT(INodeScore)

DEFINE_ENUM(EPodNodeScoreType,
    (NodeRandomHash)
    (FreeCpuMemoryShareVariance)
    (FreeCpuMemoryShareSquaredMinDelta)
);

using TPodNodeScoreValue = double;
using TNodeScoreValue = int;

DEFINE_ENUM(ESchedulerLoopStage,
    (UpdateNodeSegmentsStatus)
    (UpdateAccountsStatus)
    (RunPodDisruptionBudgetController)
    (RunPodEvictionByHfsmController)
    (RevokePodsWithAcknowledgedEviction)
    (RemoveOrphanedAllocations)
    (AcknowledgeNodeMaintenance)
    (SchedulePods)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
