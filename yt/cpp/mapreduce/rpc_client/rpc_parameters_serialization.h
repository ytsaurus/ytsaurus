#pragma once

#include <yt/cpp/mapreduce/interface/client_method_options.h>

#include <yt/yt/client/api/cypress_client.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

TGuid YtGuidFromUtilGuid(TGUID guid);

TGUID UtilGuidFromYtGuid(TGuid guid);

NObjectClient::EObjectType ToApiObjectType(ENodeType type);

NCypressClient::ELockMode ToApiLockMode(ELockMode mode);

////////////////////////////////////////////////////////////////////////////////

NApi::TGetNodeOptions SerializeOptionsForGet(
    const TTransactionId& transactionId,
    const TGetOptions& options);

NApi::TSetNodeOptions SerializeOptionsForSet(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TSetOptions& options);

NApi::TNodeExistsOptions SerializeOptionsForExists(
    const TTransactionId& transactionId,
    const TExistsOptions& options);

NApi::TMultisetAttributesNodeOptions SerializeOptionsForMultisetAttributes(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TMultisetAttributesOptions& options);

NApi::TCreateNodeOptions SerializeOptionsForCreate(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TCreateOptions& options);

NApi::TCopyNodeOptions SerializeOptionsForCopy(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TCopyOptions& options);

NApi::TMoveNodeOptions SerializeOptionsForMove(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TMoveOptions& options);

NApi::TRemoveNodeOptions SerializeOptionsForRemove(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TRemoveOptions& options);

NApi::TListNodeOptions SerializeOptionsForList(
    const TTransactionId& transactionId,
    const TListOptions& options);

NApi::TLinkNodeOptions SerializeOptionsForLink(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TLinkOptions& options);

NApi::TLockNodeOptions SerializeOptionsForLock(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TLockOptions& options);

NApi::TUnlockNodeOptions SerializeOptionsForUnlock(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TUnlockOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
