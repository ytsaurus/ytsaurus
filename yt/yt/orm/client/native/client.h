#pragma once

#include "public.h"
#include "request.h"
#include "response.h"

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/logging/public.h>

namespace NYT::NOrm::NClient::NNative {

////////////////////////////////////////////////////////////////////////////////

struct IClient
    : public virtual TRefCounted
{
    virtual TFuture<TGenerateTimestampResult> GenerateTimestamp() = 0;

    virtual TFuture<TSelectObjectsResult> SelectObjects(
        NObjects::TObjectTypeValue objectType,
        const TAttributeSelector& selector,
        const TSelectObjectsOptions& options = TSelectObjectsOptions()) = 0;

    virtual TFuture<TGetObjectResult> GetObject(
        TObjectIdentity objectIdentity,
        NObjects::TObjectTypeValue objectType,
        const TAttributeSelector& selector,
        const TGetObjectOptions& options = TGetObjectOptions()) = 0;

    virtual TFuture<TGetObjectsResult> GetObjects(
        std::vector<TObjectIdentity> objectIdentities,
        NObjects::TObjectTypeValue objectType,
        const TAttributeSelector& selector,
        const TGetObjectOptions& options = TGetObjectOptions()) = 0;

    virtual TFuture<TUpdateObjectResult> UpdateObject(
        TObjectIdentity objectIdentity,
        NObjects::TObjectTypeValue objectType,
        std::vector<TUpdate> updates,
        std::vector<TAttributeTimestampPrerequisite> attributeTimestampPrerequisites = {},
        const TUpdateObjectOptions& options = TUpdateObjectOptions()) = 0;

    virtual TFuture<TUpdateObjectsResult> UpdateObjects(
        std::vector<TUpdateObjectsSubrequest> updateSubrequests,
        const TUpdateObjectOptions& options = TUpdateObjectOptions()) = 0;

    virtual TFuture<TCreateObjectResult> CreateObject(
        NObjects::TObjectTypeValue objectType,
        TPayload attributesPayload = TNullPayload(),
        const TCreateObjectOptions& options = TCreateObjectOptions(),
        std::optional<TUpdateIfExisting> updateIfExisting = std::nullopt) = 0;

    virtual TFuture<TCreateObjectsResult> CreateObjects(
        std::vector<TCreateObjectsSubrequest> subrequests,
        const TCreateObjectOptions& options = TCreateObjectOptions()) = 0;

    virtual TFuture<TRemoveObjectResult> RemoveObject(
        TObjectIdentity objectIdentity,
        NObjects::TObjectTypeValue objectType,
        const TRemoveObjectOptions& options = TRemoveObjectOptions()) = 0;

    virtual TFuture<TRemoveObjectsResult> RemoveObjects(
        std::vector<TRemoveObjectsSubrequest> subrequests,
        const TRemoveObjectOptions& options = TRemoveObjectOptions()) = 0;

    virtual TFuture<TStartTransactionResult> StartTransaction(
        const TStartTransactionOptions& options = TStartTransactionOptions()) = 0;

    virtual TFuture<TAbortTransactionResult> AbortTransaction(
        TTransactionId id) = 0;

    virtual TFuture<TCommitTransactionResult> CommitTransaction(
        TTransactionId id,
        const TCommitTransactionOptions& options = TCommitTransactionOptions()) = 0;

    virtual TFuture<TWatchObjectsResult> WatchObjects(
        NObjects::TObjectTypeValue objectType,
        const TWatchObjectsOptions& options = TWatchObjectsOptions()) = 0;

    virtual TFuture<TSelectObjectHistoryResult> SelectObjectHistory(
        TObjectIdentity objectIdentity,
        NObjects::TObjectTypeValue objectType,
        const TAttributeSelector& attributes,
        const std::optional<TAttributeSelector>& distinctBy,
        const TSelectObjectHistoryOptions& options) = 0;

    virtual TFuture<TGetMastersResult> GetMasters() = 0;
};

DEFINE_REFCOUNTED_TYPE(IClient)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative
