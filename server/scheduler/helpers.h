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
TJobStatus JobStatusFromError(const TError& error);

////////////////////////////////////////////////////////////////////////////////

TString MakeOperationCodicilString(TOperationId operationId);
TCodicilGuard MakeOperationCodicilGuard(TOperationId operationId);

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

} // namespace NYT::NScheduler

