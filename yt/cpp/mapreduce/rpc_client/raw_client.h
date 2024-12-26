#pragma once

#include <yt/cpp/mapreduce/http/context.h>

#include <yt/cpp/mapreduce/interface/client_method_options.h>
#include <yt/cpp/mapreduce/interface/raw_client.h>

#include <yt/yt/client/api/public.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

class TRpcRawClient
    : public IRawClient
{
public:
    TRpcRawClient(
        NApi::IClientPtr client,
        const TClientContext& context);

    // Cypress

    TNode Get(
        const TTransactionId& transactionId,
        const TYPath& path,
        const TGetOptions& options = {});

    void Set(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const TNode& value,
        const TSetOptions& options = {});

    bool Exists(
        const TTransactionId& transactionId,
        const TYPath& path,
        const TExistsOptions& options = {});

    void MultisetAttributes(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const TNode::TMapType& value,
        const TMultisetAttributesOptions& options = {});

    TNodeId Create(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const ENodeType& type,
        const TCreateOptions& options = {});

    TNodeId CopyWithoutRetries(
        const TTransactionId& transactionId,
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TCopyOptions& options = {});

    TNodeId CopyInsideMasterCell(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TCopyOptions& options = {});

    TNodeId MoveWithoutRetries(
        const TTransactionId& transactionId,
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TMoveOptions& options = {});

    TNodeId MoveInsideMasterCell(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TMoveOptions& options = {});

    void Remove(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const TRemoveOptions& options = {});

    TNode::TListType List(
        const TTransactionId& transactionId,
        const TYPath& path,
        const TListOptions& options = {});

    TNodeId Link(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& targetPath,
        const TYPath& linkPath,
        const TLinkOptions& options = {});

    TLockId Lock(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        ELockMode mode,
        const TLockOptions& options = {});

    void Unlock(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const TUnlockOptions& options = {});

private:
    const NApi::IClientPtr Client_;
    const TClientContext Context_;
};

////////////////////////////////////////////////////////////////////////////////

}
