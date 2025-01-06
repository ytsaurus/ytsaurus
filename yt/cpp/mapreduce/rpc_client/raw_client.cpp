#include "raw_client.h"

#include "rpc_parameters_serialization.h"

#include <yt/cpp/mapreduce/common/helpers.h>

#include <yt/cpp/mapreduce/interface/errors.h>

#include <yt/yt/client/api/rpc_proxy/client_base.h>

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
