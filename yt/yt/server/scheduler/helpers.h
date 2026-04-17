#pragma once

#include "public.h"

#include <yt/yt/server/scheduler/strategy/public.h>

#include <yt/yt/server/scheduler/common/helpers.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/core/yson/public.h>
#include <yt/yt/core/yson/forwarding_consumer.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/misc/codicil.h>

#include <yt/yt/core/logging/fluent_log.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

void BuildMinimalOperationAttributes(TOperationPtr operation, NYTree::TFluentMap fluent);
void BuildFullOperationAttributes(TOperationPtr operation, bool includeOperationId, bool includeHeavyAttributes, NYTree::TFluentMap fluent);
void BuildMutableOperationAttributes(TOperationPtr operation, NYTree::TFluentMap fluent);

////////////////////////////////////////////////////////////////////////////////

std::string MakeOperationCodicil(TOperationId operationId);
TCodicilGuard MakeOperationCodicilGuard(TOperationId operationId);

////////////////////////////////////////////////////////////////////////////////

struct TListOperationsResult
{
    std::vector<std::pair<TOperationId, EOperationState>> OperationsToRevive;
    std::vector<TOperationId> OperationsToArchive;
};

TListOperationsResult ListOperations(
    TCallback<NObjectClient::TObjectServiceProxy::TReqExecuteBatchPtr()> createBatchRequest);

////////////////////////////////////////////////////////////////////////////////

void BuildSupportedFeatures(NYTree::TFluentMap fluent);

////////////////////////////////////////////////////////////////////////////////

std::string GuessGpuType(const std::string& treeId);

std::vector<std::pair<TInstant, TInstant>> SplitTimeIntervalByHours(TInstant startTime, TInstant finishTime);

////////////////////////////////////////////////////////////////////////////////

THashSet<int> GetDiskQuotaMedia(const TDiskQuota& diskQuota);

////////////////////////////////////////////////////////////////////////////////

struct TAllocationDescription
{
    bool Running = false;
    NNodeTrackerClient::TNodeId NodeId;

    std::optional<std::string> NodeAddress;

    struct TAllocationProperties
    {
        TOperationId OperationId;
        TInstant StartTime;
        EAllocationState State;
        std::string TreeId;
        bool Preempted;
        std::string PreemptionReason;
        TDuration PreemptionTimeout;
        std::optional<TInstant> PreemptibleProgressStartTime;
    };

    std::optional<TAllocationProperties> Properties;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
