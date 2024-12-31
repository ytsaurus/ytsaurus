#include "rpc_parameters_serialization.h"

#include <yt/cpp/mapreduce/common/helpers.h>

#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/interface/operation.h>

#include <library/cpp/yson/node/node_io.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

TGuid YtGuidFromUtilGuid(TGUID guid)
{
    return {guid.dw[0], guid.dw[1], guid.dw[2], guid.dw[3]};
}

TGUID UtilGuidFromYtGuid(TGuid guid)
{
    return {guid.Parts32[0], guid.Parts32[1], guid.Parts32[2], guid.Parts32[3]};
}

NObjectClient::EObjectType ToApiObjectType(ENodeType type)
{
    switch (type) {
        case NT_STRING:
            return NObjectClient::EObjectType::StringNode;
        case NT_INT64:
            return NObjectClient::EObjectType::Int64Node;
        case NT_UINT64:
            return NObjectClient::EObjectType::Uint64Node;
        case NT_DOUBLE:
            return NObjectClient::EObjectType::DoubleNode;
        case NT_BOOLEAN:
            return NObjectClient::EObjectType::BooleanNode;
        case NT_MAP:
            return NObjectClient::EObjectType::MapNode;
        case NT_LIST:
            return NObjectClient::EObjectType::ListNode;
        case NT_FILE:
            return NObjectClient::EObjectType::File;
        case NT_TABLE:
            return NObjectClient::EObjectType::Table;
        case NT_DOCUMENT:
            return NObjectClient::EObjectType::Document;
        case NT_REPLICATED_TABLE:
            return NObjectClient::EObjectType::ReplicatedTable;
        case NT_TABLE_REPLICA:
            return NObjectClient::EObjectType::TableReplica;
        case NT_USER:
            return NObjectClient::EObjectType::User;
        case NT_SCHEDULER_POOL:
            return NObjectClient::EObjectType::SchedulerPool;
        case NT_LINK:
            return NObjectClient::EObjectType::Link;
        case NT_GROUP:
            return NObjectClient::EObjectType::Group;
        case NT_PORTAL:
            return NObjectClient::EObjectType::PortalEntrance;
        case NT_CHAOS_TABLE_REPLICA:
            return NObjectClient::EObjectType::ChaosTableReplica;
    }
    YT_ABORT();
}

NCypressClient::ELockMode ToApiLockMode(ELockMode mode)
{
    switch (mode) {
        case LM_EXCLUSIVE:
            return NCypressClient::ELockMode::Exclusive;
        case LM_SHARED:
            return NCypressClient::ELockMode::Shared;
        case LM_SNAPSHOT:
            return NCypressClient::ELockMode::Snapshot;
    }
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

// Generates a new mutation ID based on the given conditions.
// The retry logic in a higher layer resets the mutation ID if the 'useSameMutationId' parameter is set to false.
// If 'useSameMutationId' is true, the function marks the operation for a retry and maintains the same mutation ID.
void SetMutationId(
    NApi::TMutatingOptions* options,
    TMutationId* mutationId)
{
    if (mutationId->IsEmpty()) {
        CreateGuid(mutationId);
        mutationId->dw[2] = GetPID() ^ MicroSeconds();
    } else {
        options->Retry = true;
    }
    options->MutationId = YtGuidFromUtilGuid(*mutationId);
}

void SetTransactionId(
    NApi::TTransactionalOptions* options,
    const TTransactionId& transactionId)
{
    options->TransactionId = YtGuidFromUtilGuid(transactionId);
}

////////////////////////////////////////////////////////////////////////////////

NApi::TGetNodeOptions SerializeOptionsForGet(
    const TTransactionId& transactionId,
    const TGetOptions& options)
{
    NApi::TGetNodeOptions result;
    SetTransactionId(&result, transactionId);
    if (options.AttributeFilter_) {
        result.Attributes = options.AttributeFilter_->Attributes_;
    }
    if (options.MaxSize_) {
        result.MaxSize = *options.MaxSize_;
    }
    if (options.ReadFrom_) {
        result.ReadFrom = NApi::EMasterChannelKind(*options.ReadFrom_);
    }
    return result;
}

NApi::TSetNodeOptions SerializeOptionsForSet(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TSetOptions& options)
{
    NApi::TSetNodeOptions result;
    SetMutationId(&result, &mutationId);
    SetTransactionId(&result, transactionId);
    if (options.Force_) {
        result.Force = *options.Force_;
    }
    result.Recursive = options.Recursive_;
    return result;
}

NApi::TNodeExistsOptions SerializeOptionsForExists(
    const TTransactionId& transactionId,
    const TExistsOptions& options)
{
    NApi::TNodeExistsOptions result;
    SetTransactionId(&result, transactionId);
    if (options.ReadFrom_) {
        result.ReadFrom = NApi::EMasterChannelKind(*options.ReadFrom_);
    }
    return result;
}

NApi::TMultisetAttributesNodeOptions SerializeOptionsForMultisetAttributes(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TMultisetAttributesOptions& options)
{
    NApi::TMultisetAttributesNodeOptions result;
    SetMutationId(&result, &mutationId);
    SetTransactionId(&result, transactionId);
    if (options.Force_) {
        result.Force = *options.Force_;
    }
    return result;
}

NApi::TCreateNodeOptions SerializeOptionsForCreate(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TCreateOptions& options)
{
    NApi::TCreateNodeOptions result;
    SetMutationId(&result, &mutationId);
    SetTransactionId(&result, transactionId);
    result.Force = options.Force_;
    if (options.Attributes_) {
        result.Attributes = NYTree::ConvertToAttributes(
            NYson::TYsonString(NodeToYsonString(*options.Attributes_, NYson::EYsonFormat::Binary)));
    }
    result.IgnoreExisting = options.IgnoreExisting_;
    result.Recursive = options.Recursive_;
    return result;
}

NApi::TCopyNodeOptions SerializeOptionsForCopy(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TCopyOptions& options)
{
    NApi::TCopyNodeOptions result;
    SetMutationId(&result, &mutationId);
    SetTransactionId(&result, transactionId);
    result.Force = options.Force_;
    result.PreserveAccount = options.PreserveAccount_;
    if (options.PreserveExpirationTime_) {
        result.PreserveExpirationTime = *options.PreserveExpirationTime_;
    }
    result.Recursive = options.Recursive_;
    return result;
}

NApi::TMoveNodeOptions SerializeOptionsForMove(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TMoveOptions& options)
{
    NApi::TMoveNodeOptions result;
    SetMutationId(&result, &mutationId);
    SetTransactionId(&result, transactionId);
    result.Force = options.Force_;
    result.PreserveAccount = options.PreserveAccount_;
    if (options.PreserveExpirationTime_) {
        result.PreserveExpirationTime = *options.PreserveExpirationTime_;
    }
    result.Recursive = options.Recursive_;
    return result;
}

NApi::TRemoveNodeOptions SerializeOptionsForRemove(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TRemoveOptions& options)
{
    NApi::TRemoveNodeOptions result;
    SetMutationId(&result, &mutationId);
    SetTransactionId(&result, transactionId);
    result.Force = options.Force_;
    result.Recursive = options.Recursive_;
    return result;
}

NApi::TListNodeOptions SerializeOptionsForList(
    const TTransactionId& transactionId,
    const TListOptions& options)
{
    NApi::TListNodeOptions result;
    SetTransactionId(&result, transactionId);
    if (options.AttributeFilter_) {
        result.Attributes = options.AttributeFilter_->Attributes_;
    }
    if (options.MaxSize_) {
        result.MaxSize = *options.MaxSize_;
    }
    if (options.ReadFrom_) {
        result.ReadFrom = NApi::EMasterChannelKind(*options.ReadFrom_);
    }
    return result;
}

NApi::TLinkNodeOptions SerializeOptionsForLink(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TLinkOptions& options)
{
    NApi::TLinkNodeOptions result;
    SetMutationId(&result, &mutationId);
    SetTransactionId(&result, transactionId);
    result.Force = options.Force_;
    if (options.Attributes_) {
        result.Attributes = NYTree::ConvertToAttributes(
            NYson::TYsonString(NodeToYsonString(*options.Attributes_, NYson::EYsonFormat::Binary)));
    }
    result.IgnoreExisting = options.IgnoreExisting_;
    result.Recursive = options.Recursive_;
    return result;
}

NApi::TLockNodeOptions SerializeOptionsForLock(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TLockOptions& options)
{
    NApi::TLockNodeOptions result;
    SetMutationId(&result, &mutationId);
    SetTransactionId(&result, transactionId);
    result.Waitable = options.Waitable_;
    if (options.AttributeKey_) {
        result.AttributeKey = *options.AttributeKey_;
    }
    if (options.ChildKey_) {
        result.ChildKey = *options.ChildKey_;
    }
    return result;
}

NApi::TUnlockNodeOptions SerializeOptionsForUnlock(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TUnlockOptions& /*options*/)
{
    NApi::TUnlockNodeOptions result;
    SetMutationId(&result, &mutationId);
    SetTransactionId(&result, transactionId);
    return result;
}

NApi::TTransactionStartOptions SerializeOptionsForStartTransaction(
    TMutationId& mutationId,
    const TTransactionId& parentId,
    TDuration timeout,
    const TStartTransactionOptions& options)
{
    NApi::TTransactionStartOptions result;
    SetMutationId(&result, &mutationId);
    result.ParentId = YtGuidFromUtilGuid(parentId);
    result.Timeout = options.Timeout_.GetOrElse(timeout);
    if (options.Deadline_) {
        result.Deadline = *options.Deadline_;
    }
    result.PingAncestors = options.PingAncestors_;

    if (options.Attributes_ && !options.Attributes_->IsMap()) {
        ythrow TApiUsageError() << "Attributes must be a Map node";
    }

    auto attributes = NYTree::ConvertToAttributes(
        NYson::TYsonString(NodeToYsonString(*options.Attributes_, NYson::EYsonFormat::Binary)));
    if (options.Title_) {
        attributes->Set("title", *options.Title_);
    } else if (!attributes->Contains("title")) {
        attributes->Set("title", GetDefaultTransactionTitle());
    }
    result.Attributes = std::move(attributes);

    return result;
}

NApi::TTransactionAbortOptions SerializeOptionsForAbortTransaction(TMutationId& mutationId)
{
    NApi::TTransactionAbortOptions result;
    SetMutationId(&result, &mutationId);
    return result;
}

NApi::TTransactionCommitOptions SerializeOptionsForCommitTransaction(TMutationId& mutationId)
{
    NApi::TTransactionCommitOptions result;
    SetMutationId(&result, &mutationId);
    return result;
}

NApi::TStartOperationOptions SerializeOptionsForStartOperation(
    TMutationId& mutationId,
    const TTransactionId& transactionId)
{
    NApi::TStartOperationOptions result;
    SetMutationId(&result, &mutationId);
    SetTransactionId(&result, transactionId);
    return result;
}

NApi::TGetOperationOptions SerializeOptionsForGetOperation(const TGetOperationOptions& options)
{
    NApi::TGetOperationOptions result;
    if (options.IncludeRuntime_) {
        result.IncludeRuntime = *options.IncludeRuntime_;
    }
    if (options.AttributeFilter_) {
        result.Attributes = THashSet<TString>();
        for (const auto& attribute : options.AttributeFilter_->Attributes_) {
            result.Attributes->emplace(ToString(attribute));
        }
    }
    return result;
}

NApi::TSuspendOperationOptions SerializeOptionsForSuspendOperation(const TSuspendOperationOptions& options)
{
    NApi::TSuspendOperationOptions result;
    if (options.AbortRunningJobs_) {
        result.AbortRunningJobs = *options.AbortRunningJobs_;
    }
    return result;
}

NApi::TGetFileFromCacheOptions SerializeOptionsForGetFileFromCache(
    const TTransactionId& transactionId,
    const TYPath& cachePath,
    const TGetFileFromCacheOptions& /*options*/)
{
    NApi::TGetFileFromCacheOptions result;
    SetTransactionId(&result, transactionId);
    result.CachePath = cachePath;
    return result;
}

NApi::TPutFileToCacheOptions SerializeOptionsForPutFileToCache(
    const TTransactionId& transactionId,
    const TYPath& cachePath,
    const TPutFileToCacheOptions& options)
{
    NApi::TPutFileToCacheOptions result;
    SetTransactionId(&result, transactionId);
    result.CachePath = cachePath;
    if (options.PreserveExpirationTimeout_) {
        result.PreserveExpirationTimeout = *options.PreserveExpirationTimeout_;
    }
    return result;
}

NApi::TMountTableOptions SerializeOptionsForMountTable(
    TMutationId& mutationId,
    const TMountTableOptions& options)
{
    NApi::TMountTableOptions result;
    SetMutationId(&result, &mutationId);
    if (options.CellId_) {
        result.CellId = YtGuidFromUtilGuid(*options.CellId_);
    }
    if (options.FirstTabletIndex_) {
        result.FirstTabletIndex = *options.FirstTabletIndex_;
    }
    if (options.LastTabletIndex_) {
        result.LastTabletIndex = *options.LastTabletIndex_;
    }
    result.Freeze = options.Freeze_;
    return result;
}

NApi::TUnmountTableOptions SerializeOptionsForUnmountTable(
    TMutationId& mutationId,
    const TUnmountTableOptions& options)
{
    NApi::TUnmountTableOptions result;
    SetMutationId(&result, &mutationId);
    if (options.FirstTabletIndex_) {
        result.FirstTabletIndex = *options.FirstTabletIndex_;
    }
    if (options.LastTabletIndex_) {
        result.LastTabletIndex = *options.LastTabletIndex_;
    }
    result.Force = options.Force_;
    return result;
}

NApi::TRemountTableOptions SerializeOptionsForRemountTable(
    TMutationId& mutationId,
    const TRemountTableOptions& options)
{
    NApi::TRemountTableOptions result;
    SetMutationId(&result, &mutationId);
    if (options.FirstTabletIndex_) {
        result.FirstTabletIndex = *options.FirstTabletIndex_;
    }
    if (options.LastTabletIndex_) {
        result.LastTabletIndex = *options.LastTabletIndex_;
    }
    return result;
}

NApi::TFreezeTableOptions SerializeOptionsForFreezeTable(const TFreezeTableOptions& options)
{
    NApi::TFreezeTableOptions result;
    if (options.FirstTabletIndex_) {
        result.FirstTabletIndex = *options.FirstTabletIndex_;
    }
    if (options.LastTabletIndex_) {
        result.LastTabletIndex = *options.LastTabletIndex_;
    }
    return result;
}

NApi::TUnfreezeTableOptions SerializeOptionsForUnfreezeTable(const TUnfreezeTableOptions& options)
{
    NApi::TUnfreezeTableOptions result;
    if (options.FirstTabletIndex_) {
        result.FirstTabletIndex = *options.FirstTabletIndex_;
    }
    if (options.LastTabletIndex_) {
        result.LastTabletIndex = *options.LastTabletIndex_;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
