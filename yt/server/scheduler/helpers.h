#pragma once

#include "private.h"

#include <yt/ytlib/hive/cluster_directory.h>

#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/core/yson/public.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

void BuildMinimalOperationAttributes(TOperationPtr operation, NYTree::TFluentMap fluent);
void BuildFullOperationAttributes(TOperationPtr operation, NYTree::TFluentMap fluent);
void BuildMutableOperationAttributes(TOperationPtr operation, NYTree::TFluentMap fluent);
void BuildExecNodeAttributes(TExecNodePtr node, NYTree::TFluentMap fluent);

////////////////////////////////////////////////////////////////////////////////

EAbortReason GetAbortReason(const TError& resultError);

////////////////////////////////////////////////////////////////////////////////

TString MakeOperationCodicilString(const TOperationId& operationId);
TCodicilGuard MakeOperationCodicilGuard(const TOperationId& operationId);

////////////////////////////////////////////////////////////////////////////////

TJobStatus JobStatusFromError(const TError& error);
TJobId GenerateJobId(NObjectClient::TCellTag tag, NNodeTrackerClient::TNodeId nodeId);
NNodeTrackerClient::TNodeId NodeIdFromJobId(const TJobId& jobId);

////////////////////////////////////////////////////////////////////////////////

struct TListOperationsResult
{
    std::vector<std::pair<TOperationId, EOperationState>> OperationsToRevive;
    std::vector<TOperationId> OperationsToArchive;
    std::vector<TOperationId> OperationsToRemove;
    std::vector<TOperationId> OperationsToSync;
};

TListOperationsResult ListOperations(
    TCallback<NObjectClient::TObjectServiceProxy::TReqExecuteBatchPtr()> createBatchRequest);

////////////////////////////////////////////////////////////////////////////////

void ValidateOperationAccess(
    const TString& user,
    const TOperationId& operationId,
    EAccessType accessType,
    const NYTree::INodePtr& acl,
    const NApi::NNative::IClientPtr& client,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

