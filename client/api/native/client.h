#pragma once

#include "public.h"

#include "request.h"

#include <yt/core/actions/future.h>

namespace NYP::NClient::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

struct IClient
    : public virtual TRefCounted
{
    virtual TFuture<TGenerateTimestampResult> GenerateTimestamp() = 0;

    virtual TFuture<TSelectObjectsResult> SelectObjects(
        EObjectType objectType,
        const TAttributeSelector& selector,
        const TSelectObjectsOptions& options = TSelectObjectsOptions()) = 0;

    virtual TFuture<TGetObjectResult> GetObject(
        TObjectId objectId,
        EObjectType objectType,
        const TAttributeSelector& selector,
        const TGetObjectOptions& options = TGetObjectOptions()) = 0;

    virtual TFuture<TUpdateObjectResult> UpdateObject(
        TObjectId objectId,
        EObjectType objectType,
        std::vector<TUpdate> updates,
        std::vector<TAttributeTimestampPrerequisite> attributeTimestampPrerequisites = {},
        const TTransactionId& transactionId = TTransactionId()) = 0;

    virtual TFuture<TCreateObjectResult> CreateObject(
        EObjectType objectType,
        TPayload attributesPayload = TNullPayload(),
        const TTransactionId& transactionId = TTransactionId()) = 0;

    virtual TFuture<TRemoveObjectResult> RemoveObject(
        TObjectId objectId,
        EObjectType objectType,
        const TTransactionId& transactionId = TTransactionId()) = 0;

    virtual TFuture<TStartTransactionResult> StartTransaction() = 0;

    virtual TFuture<TAbortTransactionResult> AbortTransaction(
        const TTransactionId& id) = 0;

    virtual TFuture<TCommitTransactionResult> CommitTransaction(
        const TTransactionId& id) = 0;
};

DEFINE_REFCOUNTED_TYPE(IClient);

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(TClientConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NClient::NApi::NNative
