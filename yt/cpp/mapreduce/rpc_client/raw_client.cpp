#include "raw_client.h"

#include "rpc_parameters_serialization.h"

#include <yt/cpp/mapreduce/common/helpers.h>

#include <library/cpp/yson/node/node_io.h>

namespace NYT::NDetail {

using namespace NYT::NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TRpcRawClient::TRpcRawClient(
    NApi::IClientPtr client,
    const TClientContext& context)
    : Client_(std::move(client))
    , Context_(context)
{ }

TNode TRpcRawClient::Get(
    const TTransactionId& transactionId,
    const TYPath& path,
    const TGetOptions& options)
{
    auto newPath = AddPathPrefix(path, Context_.Config->Prefix);
    auto future = Client_->GetNode(newPath, SerializeOptionsForGet(transactionId, options));
    auto result = WaitFor(future).ValueOrThrow();
    return NodeFromYsonString(result.AsStringBuf());
}

void TRpcRawClient::Set(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TNode& value,
    const TSetOptions& options)
{
    auto newOptions = SerializeOptionsForSet(mutationId, transactionId, options);
    auto newPath = AddPathPrefix(path, Context_.Config->Prefix);
    auto ysonValue = NYson::TYsonString(NodeToYsonString(value, NYson::EYsonFormat::Binary));
    auto future = Client_->SetNode(newPath, ysonValue, SerializeOptionsForSet(mutationId, transactionId, options));
    WaitFor(future).ThrowOnError();
}

bool TRpcRawClient::Exists(
    const TTransactionId& transactionId,
    const TYPath& path,
    const TExistsOptions& options)
{
    auto newPath = AddPathPrefix(path, Context_.Config->Prefix);
    auto future = Client_->NodeExists(newPath, SerializeOptionsForExists(transactionId, options));
    return WaitFor(future).ValueOrThrow();
}

void TRpcRawClient::MultisetAttributes(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TNode::TMapType& value,
    const TMultisetAttributesOptions& options)
{
    auto newPath = AddPathPrefix(path, Context_.Config->Prefix);
    auto attributes = NYTree::ConvertToAttributes(
        NYson::TYsonString(NodeToYsonString(value, NYson::EYsonFormat::Binary)));
    auto future = Client_->MultisetAttributesNode(newPath, attributes->ToMap(), SerializeOptionsForMultisetAttributes(mutationId, transactionId, options));
    WaitFor(future).ThrowOnError();
}

TNodeId TRpcRawClient::Create(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TYPath& path,
    const ENodeType& type,
    const TCreateOptions& options)
{
    auto newPath = AddPathPrefix(path, Context_.Config->Prefix);
    auto future = Client_->CreateNode(newPath, ToApiObjectType(type), SerializeOptionsForCreate(mutationId, transactionId, options));
    auto result = WaitFor(future).ValueOrThrow();
    return UtilGuidFromYtGuid(result);
}

TNodeId TRpcRawClient::CopyWithoutRetries(
    const TTransactionId& transactionId,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TCopyOptions& options)
{
    TMutationId mutationId;
    auto newSourcePath = AddPathPrefix(sourcePath, Context_.Config->Prefix);
    auto newDestinationPath = AddPathPrefix(destinationPath, Context_.Config->Prefix);
    auto future = Client_->CopyNode(newSourcePath, newDestinationPath, SerializeOptionsForCopy(mutationId, transactionId, options));
    auto result = WaitFor(future).ValueOrThrow();
    return UtilGuidFromYtGuid(result);
}

TNodeId TRpcRawClient::CopyInsideMasterCell(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TCopyOptions& options)
{
    auto newSourcePath = AddPathPrefix(sourcePath, Context_.Config->Prefix);
    auto newDestinationPath = AddPathPrefix(destinationPath, Context_.Config->Prefix);

    // Make cross cell copying disable.
    auto newOptions = SerializeOptionsForCopy(mutationId, transactionId, options);
    newOptions.EnableCrossCellCopying = false;

    auto future = Client_->CopyNode(newSourcePath, newDestinationPath, newOptions);
    auto result = WaitFor(future).ValueOrThrow();
    return UtilGuidFromYtGuid(result);
}

TNodeId TRpcRawClient::MoveWithoutRetries(
    const TTransactionId& transactionId,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TMoveOptions& options)
{
    TMutationId mutationId;
    auto newSourcePath = AddPathPrefix(sourcePath, Context_.Config->Prefix);
    auto newDestinationPath = AddPathPrefix(destinationPath, Context_.Config->Prefix);
    auto future = Client_->MoveNode(newSourcePath, newDestinationPath, SerializeOptionsForMove(mutationId, transactionId, options));
    auto result = WaitFor(future).ValueOrThrow();
    return UtilGuidFromYtGuid(result);
}

TNodeId TRpcRawClient::MoveInsideMasterCell(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TMoveOptions& options)
{
    auto newSourcePath = AddPathPrefix(sourcePath, Context_.Config->Prefix);
    auto newDestinationPath = AddPathPrefix(destinationPath, Context_.Config->Prefix);

    // Make cross cell copying disable.
    auto newOptions = SerializeOptionsForMove(mutationId, transactionId, options);
    newOptions.EnableCrossCellCopying = false;

    auto future = Client_->MoveNode(newSourcePath, newDestinationPath, newOptions);
    auto result = WaitFor(future).ValueOrThrow();
    return UtilGuidFromYtGuid(result);
}

void TRpcRawClient::Remove(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TRemoveOptions& options)
{
    auto newPath = AddPathPrefix(path, Context_.Config->Prefix);
    auto future = Client_->RemoveNode(newPath, SerializeOptionsForRemove(mutationId, transactionId, options));
    WaitFor(future).ThrowOnError();
}

TNode::TListType TRpcRawClient::List(
    const TTransactionId& transactionId,
    const TYPath& path,
    const TListOptions& options)
{
    auto newPath = AddPathPrefix(path, Context_.Config->Prefix);
    auto future = Client_->ListNode(newPath, SerializeOptionsForList(transactionId, options));
    auto result = WaitFor(future).ValueOrThrow();
    return NodeFromYsonString(result.AsStringBuf()).AsList();
}

TNodeId TRpcRawClient::Link(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TYPath& targetPath,
    const TYPath& linkPath,
    const TLinkOptions& options)
{
    auto newTargetPath = AddPathPrefix(targetPath, Context_.Config->Prefix);
    auto newLinkPath = AddPathPrefix(linkPath, Context_.Config->Prefix);
    auto future = Client_->LinkNode(newTargetPath, newLinkPath, SerializeOptionsForLink(mutationId, transactionId, options));
    auto result = WaitFor(future).ValueOrThrow();
    return UtilGuidFromYtGuid(result);
}

TLockId TRpcRawClient::Lock(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TYPath& path,
    ELockMode mode,
    const TLockOptions& options)
{
    auto newPath = AddPathPrefix(path, Context_.Config->Prefix);
    auto future = Client_->LockNode(newPath, ToApiLockMode(mode), SerializeOptionsForLock(mutationId, transactionId, options));
    auto result = WaitFor(future).ValueOrThrow();
    return UtilGuidFromYtGuid(result.LockId);
}

void TRpcRawClient::Unlock(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TUnlockOptions& options)
{
    auto newPath = AddPathPrefix(path, Context_.Config->Prefix);
    auto future = Client_->UnlockNode(newPath, SerializeOptionsForUnlock(mutationId, transactionId, options));
    WaitFor(future).ThrowOnError();
}

TTransactionId TRpcRawClient::StartTransaction(
    TMutationId& mutationId,
    const TTransactionId& parentId,
    const TStartTransactionOptions& options)
{
    auto future = Client_->StartTransaction(
        NTransactionClient::ETransactionType::Master,
        SerializeOptionsForStartTransaction(mutationId, parentId, Context_.Config->TxTimeout, options));
    auto result = WaitFor(future).ValueOrThrow();
    return UtilGuidFromYtGuid(result->GetId());
}

void TRpcRawClient::PingTransaction(const TTransactionId& transactionId)
{
    auto tx = Client_->AttachTransaction(YtGuidFromUtilGuid(transactionId));
    WaitFor(tx->Ping()).ThrowOnError();
}

void TRpcRawClient::AbortTransaction(
    TMutationId& mutationId,
    const TTransactionId& transactionId)
{
    auto tx = Client_->AttachTransaction(YtGuidFromUtilGuid(transactionId));
    WaitFor(tx->Abort(SerializeOptionsForAbortTransaction(mutationId))).ThrowOnError();
}

void TRpcRawClient::CommitTransaction(
    TMutationId& mutationId,
    const TTransactionId& transactionId)
{
    auto tx = Client_->AttachTransaction(YtGuidFromUtilGuid(transactionId));
    WaitFor(tx->Commit(SerializeOptionsForCommitTransaction(mutationId))).ThrowOnError();
}

TOperationId TRpcRawClient::StartOperation(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    EOperationType type,
    const TNode& spec)
{
    auto future = Client_->StartOperation(
        NScheduler::EOperationType(type),
        NYson::TYsonString(NodeToYsonString(spec)),
        SerializeOptionsForStartOperation(mutationId, transactionId));
    auto result = WaitFor(future).ValueOrThrow();
    return UtilGuidFromYtGuid(result.Underlying());
}

TOperationAttributes ParseOperationAttributes(const NApi::TOperation& operation)
{
    TOperationAttributes result;
    if (operation.Id) {
        result.Id = UtilGuidFromYtGuid(operation.Id->Underlying());
    }
    if (operation.Type) {
        result.Type = EOperationType(*operation.Type);
    }
    if (operation.State) {
        result.State = ToString(*operation.State);
        if (*result.State == "completed") {
            result.BriefState = EOperationBriefState::Completed;
        } else if (*result.State == "aborted") {
            result.BriefState = EOperationBriefState::Aborted;
        } else if (*result.State == "failed") {
            result.BriefState = EOperationBriefState::Failed;
        } else {
            result.BriefState = EOperationBriefState::InProgress;
        }
    }
    if (operation.AuthenticatedUser) {
        result.AuthenticatedUser = *operation.AuthenticatedUser;
    }
    if (operation.StartTime) {
        result.StartTime = *operation.StartTime;
    }
    if (operation.FinishTime) {
        result.FinishTime = *operation.FinishTime;
    }
    if (operation.BriefProgress) {
        auto briefProgressNode = NodeFromYsonString(operation.BriefProgress.AsStringBuf());
        if (briefProgressNode.HasKey("jobs")) {
            result.BriefProgress.ConstructInPlace();
            static auto load = [] (const TNode& item) {
                // Backward compatibility with old YT versions
                return item.IsInt64() ? item.AsInt64() : item["total"].AsInt64();
            };
            const auto& jobs = briefProgressNode["jobs"];
            result.BriefProgress->Aborted = load(jobs["aborted"]);
            result.BriefProgress->Completed = load(jobs["completed"]);
            result.BriefProgress->Running = jobs["running"].AsInt64();
            result.BriefProgress->Total = jobs["total"].AsInt64();
            result.BriefProgress->Failed = jobs["failed"].AsInt64();
            result.BriefProgress->Lost = jobs["lost"].AsInt64();
            result.BriefProgress->Pending = jobs["pending"].AsInt64();
        }
    }
    if (operation.BriefSpec) {
        result.BriefSpec = NodeFromYsonString(operation.BriefSpec.AsStringBuf());
    }
    if (operation.FullSpec) {
        result.FullSpec = NodeFromYsonString(operation.FullSpec.AsStringBuf());
    }
    if (operation.UnrecognizedSpec) {
        result.UnrecognizedSpec = NodeFromYsonString(operation.UnrecognizedSpec.AsStringBuf());
    }
    if (operation.Suspended) {
        result.Suspended = *operation.Suspended;
    }
    if (operation.Result) {
        auto resultNode = NodeFromYsonString(operation.Result.AsStringBuf());
        result.Result.ConstructInPlace();
        auto error = TYtError(resultNode["error"]);
        if (error.GetCode() != 0) {
            result.Result->Error = std::move(error);
        }
    }
    if (operation.Progress) {
        auto progressMap = NodeFromYsonString(operation.Progress.AsStringBuf()).AsMap();
        TMaybe<TInstant> buildTime;
        if (auto buildTimeNode = progressMap.FindPtr("build_time")) {
            buildTime = TInstant::ParseIso8601(buildTimeNode->AsString());
        }
        TJobStatistics jobStatistics;
        if (auto jobStatisticsNode = progressMap.FindPtr("job_statistics")) {
            jobStatistics = TJobStatistics(*jobStatisticsNode);
        }
        TJobCounters jobCounters;
        if (auto jobCountersNode = progressMap.FindPtr("total_job_counter")) {
            jobCounters = TJobCounters(*jobCountersNode);
        }
        result.Progress = TOperationProgress{
            .JobStatistics = std::move(jobStatistics),
            .JobCounters = std::move(jobCounters),
            .BuildTime = buildTime,
        };
    }
    if (operation.Events) {
        auto eventsNode = NodeFromYsonString(operation.Events.AsStringBuf());
        result.Events.ConstructInPlace().reserve(eventsNode.Size());
        for (const auto& eventNode : eventsNode.AsList()) {
            result.Events->push_back(TOperationEvent{
                eventNode["state"].AsString(),
                TInstant::ParseIso8601(eventNode["time"].AsString()),
            });
        }
    }
    if (operation.Alerts) {
        auto alertsNode = NodeFromYsonString(operation.Alerts.AsStringBuf());
        result.Alerts.ConstructInPlace();
        for (const auto& [alertType, alertError] : alertsNode.AsMap()) {
            result.Alerts->emplace(alertType, alertError);
        }
    }
    return result;
}

TOperationAttributes TRpcRawClient::GetOperation(
    const TOperationId& operationId,
    const TGetOperationOptions& options)
{
    auto future = Client_->GetOperation(
        NScheduler::TOperationId(YtGuidFromUtilGuid(operationId)),
        SerializeOptionsForGetOperation(options));
    auto result = WaitFor(future).ValueOrThrow();
    return ParseOperationAttributes(result);
}

TOperationAttributes TRpcRawClient::GetOperation(
    const TString& alias,
    const TGetOperationOptions& options)
{
    auto future = Client_->GetOperation(alias, SerializeOptionsForGetOperation(options));
    auto result = WaitFor(future).ValueOrThrow();
    return ParseOperationAttributes(result);
}

void TRpcRawClient::AbortOperation(
    TMutationId& /*mutationId*/,
    const TOperationId& operationId)
{
    auto future = Client_->AbortOperation(NScheduler::TOperationId(YtGuidFromUtilGuid(operationId)));
    WaitFor(future).ThrowOnError();
}

void TRpcRawClient::CompleteOperation(
    TMutationId& /*mutationId*/,
    const TOperationId& operationId)
{
    auto future = Client_->CompleteOperation(NScheduler::TOperationId(YtGuidFromUtilGuid(operationId)));
    WaitFor(future).ThrowOnError();
}

void TRpcRawClient::SuspendOperation(
    TMutationId& /*mutationId*/,
    const TOperationId& operationId,
    const TSuspendOperationOptions& options)
{
    auto future = Client_->SuspendOperation(
        NScheduler::TOperationId(YtGuidFromUtilGuid(operationId)),
        SerializeOptionsForSuspendOperation(options));
    WaitFor(future).ThrowOnError();
}

void TRpcRawClient::ResumeOperation(
    TMutationId& /*mutationId*/,
    const TOperationId& operationId,
    const TResumeOperationOptions& /*options*/)
{
    auto future = Client_->ResumeOperation(NScheduler::TOperationId(YtGuidFromUtilGuid(operationId)));
    WaitFor(future).ThrowOnError();
}

TMaybe<TYPath> TRpcRawClient::GetFileFromCache(
    const TTransactionId& transactionId,
    const TString& md5Signature,
    const TYPath& cachePath,
    const TGetFileFromCacheOptions& options)
{
    auto future = Client_->GetFileFromCache(md5Signature, SerializeOptionsForGetFileFromCache(transactionId, cachePath, options));
    auto result = WaitFor(future).ValueOrThrow();
    return result.Path.empty() ? Nothing() : TMaybe<TYPath>(result.Path);
}

TYPath TRpcRawClient::PutFileToCache(
    const TTransactionId& transactionId,
    const TYPath& filePath,
    const TString& md5Signature,
    const TYPath& cachePath,
    const TPutFileToCacheOptions& options)
{
    auto future = Client_->PutFileToCache(filePath, md5Signature, SerializeOptionsForPutFileToCache(transactionId, cachePath, options));
    auto result = WaitFor(future).ValueOrThrow();
    return result.Path;
}

void TRpcRawClient::MountTable(
    TMutationId& mutationId,
    const TYPath& path,
    const TMountTableOptions& options)
{
    auto future = Client_->MountTable(path, SerializeOptionsForMountTable(mutationId, options));
    WaitFor(future).ThrowOnError();
}

void TRpcRawClient::UnmountTable(
    TMutationId& mutationId,
    const TYPath& path,
    const TUnmountTableOptions& options)
{
    auto future = Client_->UnmountTable(path, SerializeOptionsForUnmountTable(mutationId, options));
    WaitFor(future).ThrowOnError();
}

void TRpcRawClient::RemountTable(
    TMutationId& mutationId,
    const TYPath& path,
    const TRemountTableOptions& options)
{
    auto future = Client_->RemountTable(path, SerializeOptionsForRemountTable(mutationId, options));
    WaitFor(future).ThrowOnError();
}

void TRpcRawClient::FreezeTable(
    const TYPath& path,
    const TFreezeTableOptions& options)
{
    auto future = Client_->FreezeTable(path, SerializeOptionsForFreezeTable(options));
    WaitFor(future).ThrowOnError();
}

void TRpcRawClient::UnfreezeTable(
    const TYPath& path,
    const TUnfreezeTableOptions& options)
{
    auto future = Client_->UnfreezeTable(path, SerializeOptionsForUnfreezeTable(options));
    WaitFor(future).ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
