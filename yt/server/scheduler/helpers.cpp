#include "stdafx.h"
#include "helpers.h"
#include "operation.h"
#include "job.h"
#include "exec_node.h"
#include "operation_controller.h"

#include <ytlib/object_client/helpers.h>

#include <ytlib/node_tracker_client/helpers.h>

#include <ytlib/security_client/public.h>

#include <ytlib/cell_directory/cell_directory.h>

#include <core/concurrency/fiber.h>

#include <core/ytree/fluent.h>


namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NConcurrency;
using namespace NSecurityClient;

////////////////////////////////////////////////////////////////////

void BuildInitializingOperationAttributes(TOperationPtr operation, NYson::IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("operation_type").Value(operation->GetType())
        .Item("start_time").Value(operation->GetStartTime())
        .Item("spec").Value(operation->GetSpec())
        .Item("authenticated_user").Value(operation->GetAuthenticatedUser())
        .Item("mutation_id").Value(operation->GetMutationId())
        .Do(BIND(&BuildRunningOperationAttributes, operation));
}

void BuildRunningOperationAttributes(TOperationPtr operation, NYson::IYsonConsumer* consumer)
{
    auto userTransaction = operation->GetUserTransaction();
    auto syncTransaction = operation->GetSyncSchedulerTransaction();
    auto asyncTransaction = operation->GetAsyncSchedulerTransaction();
    auto inputTransaction = operation->GetInputTransaction();
    auto outputTransaction = operation->GetOutputTransaction();
    BuildYsonMapFluently(consumer)
        .Item("state").Value(operation->GetState())
        .Item("suspended").Value(operation->GetSuspended())
        .Item("user_transaction_id").Value(userTransaction ? userTransaction->GetId() : NullTransactionId)
        .Item("sync_scheduler_transaction_id").Value(syncTransaction ? syncTransaction->GetId() : NullTransactionId)
        .Item("async_scheduler_transaction_id").Value(asyncTransaction ? asyncTransaction->GetId() : NullTransactionId)
        .Item("input_transaction_id").Value(inputTransaction ? inputTransaction->GetId() : NullTransactionId)
        .Item("output_transaction_id").Value(outputTransaction ? outputTransaction->GetId() : NullTransactionId);
}

void BuildJobAttributes(TJobPtr job, NYson::IYsonConsumer* consumer)
{
    auto state = job->GetState();
    BuildYsonMapFluently(consumer)
        .Item("job_type").Value(FormatEnum(job->GetType()))
        .Item("state").Value(FormatEnum(state))
        .Item("address").Value(job->GetNode()->GetAddress())
        .Item("start_time").Value(job->GetStartTime())
        .Item("account").Value(TmpAccountName)
        .DoIf(job->GetFinishTime(), [=] (TFluentMap fluent) {
            fluent.Item("finish_time").Value(job->GetFinishTime().Get());
        })
        .DoIf(state == EJobState::Failed, [=] (TFluentMap fluent) {
            auto error = FromProto(job->Result().error());
            fluent.Item("error").Value(error);
        });
}

void BuildExecNodeAttributes(TExecNodePtr node, NYson::IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("resource_usage").Value(node->ResourceUsage())
        .Item("resource_limits").Value(node->ResourceLimits());
}

////////////////////////////////////////////////////////////////////////////////

i64 Clamp(i64 value, i64 minValue, i64 maxValue)
{
    value = std::min(value, maxValue);
    value = std::max(value, minValue);
    return value;
}

Stroka TrimCommandForBriefSpec(const Stroka& command)
{
    const int MaxBriefSpecCommandLength = 256;
    return
        command.length() <= MaxBriefSpecCommandLength
        ? command
        : command.substr(0, MaxBriefSpecCommandLength) + "...";
}

////////////////////////////////////////////////////////////////////

TMultiCellBatchResponse::TMultiCellBatchResponse(
    const std::vector<TObjectServiceProxy::TRspExecuteBatchPtr>& batchResponses,
    const std::vector<std::pair<int, int>>& index,
    const std::multimap<Stroka, int>& keyToIndexes)
    : BatchResponses_(batchResponses)
    , ResponseIndex_(index)
    , KeyToIndexes_(keyToIndexes)
{ }

int TMultiCellBatchResponse::GetSize() const
{
    return ResponseIndex_.size();
}

TError TMultiCellBatchResponse::GetCumulativeError() const
{
    TError cumulativeError("Error communicating with master");
    for (const auto& batchRsp : BatchResponses_) {
        for (const auto& rsp : batchRsp->GetResponses()) {
            auto error = rsp->GetError();
            if (!error.IsOK()) {
                cumulativeError.InnerErrors().push_back(error);
            }
        }
    }
    return cumulativeError.InnerErrors().empty() ? TError() : cumulativeError;
}

TYPathResponsePtr TMultiCellBatchResponse::GetResponse(int index) const
{
    return GetResponse<TYPathResponse>(index);
}

TYPathResponsePtr TMultiCellBatchResponse::FindResponse(const Stroka& key) const
{
    return FindResponse<TYPathResponse>(key);
}

TYPathResponsePtr TMultiCellBatchResponse::GetResponse(const Stroka& key) const
{
    return GetResponse<TYPathResponse>(key);
}

std::vector<TYPathResponsePtr> TMultiCellBatchResponse::GetResponses(const Stroka& key) const
{
    return GetResponses<TYPathResponse>(key);
}

TNullable<std::vector<TYPathResponsePtr>> TMultiCellBatchResponse::FindResponses(const Stroka& key) const
{
    return FindResponses<TYPathResponse>(key);
}

bool TMultiCellBatchResponse::IsOK() const
{
    return this->operator TError().IsOK();
}

TMultiCellBatchResponse::operator TError() const
{
    TError resultError("Error communicating with master");
    for (const auto& batchRsp : BatchResponses_) {
        auto error = TError(*batchRsp);
        if (!error.IsOK()) {
            resultError.InnerErrors().push_back(error);
        }
    }
    return resultError.InnerErrors().empty() ? TError() : resultError;
}

////////////////////////////////////////////////////////////////////

TMultiCellBatchRequest::TMultiCellBatchRequest(
    NCellDirectory::TCellDirectoryPtr cellDirectory,
    bool throwIfCellIsMissing)
    : CellDirectory_(cellDirectory)
    , ThrowIfCellIsMissing_(throwIfCellIsMissing)
{ }

bool TMultiCellBatchRequest::AddRequest(TYPathRequestPtr req, const Stroka& key, TCellId cellId)
{
    if (!Init(cellId)) {
        if (ThrowIfCellIsMissing_) {
            THROW_ERROR_EXCEPTION("Cannot find cluster with cell id %s", ~ToString(cellId));
        }
        return false;
    }
    int index = BatchRequests_[cellId]->GetSize();
    RequestIndex_.push_back(std::make_pair(cellId, BatchRequests_[cellId]->GetSize()));
    KeyToIndexes_.insert(std::make_pair(key, index));
    BatchRequests_[cellId]->AddRequest(req, key);
    return true;
}

bool TMultiCellBatchRequest::AddRequestForTransaction(TYPathRequestPtr req, const Stroka& key, const TTransactionId& id)
{
    return AddRequest(req, key, GetCellId(id, EObjectType::Transaction));
}

TMultiCellBatchResponse TMultiCellBatchRequest::Execute(IInvokerPtr invoker)
{
    std::map<TCellId, int> cellIdToResponseNumber;

    std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> futures;
    int counter = 0;
    for (auto& pair : BatchRequests_) {
        futures.push_back(pair.second->Invoke());
        cellIdToResponseNumber[pair.first] = counter++;
    }

    std::vector< std::pair<int, int> > responseIndex;
    for (const auto& elem : RequestIndex_) {
        responseIndex.push_back(std::make_pair(cellIdToResponseNumber[elem.first], elem.second));
    }

    std::vector<TObjectServiceProxy::TRspExecuteBatchPtr> responses;
    for (const auto& future : futures) {
        responses.push_back(WaitFor(future, invoker));
    }

    return TMultiCellBatchResponse(responses, responseIndex, KeyToIndexes_);
}

bool TMultiCellBatchRequest::Init(TCellId cellId)
{
    if (BatchRequests_.find(cellId) == BatchRequests_.end()) {
        auto channel = CellDirectory_->GetChannel(cellId);
        if (!channel) {
            return false;
        }
        auto objectServiceProxy = TObjectServiceProxy(channel);
        BatchRequests_[cellId] = objectServiceProxy.ExecuteBatch();
    }
    return true;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

