#pragma once

#include <yt/yt/orm/client/native/client.h>
#include <yt/yt/orm/client/native/response.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NOrm::NClient::NNative {

struct TMockOrmClient : public IClient {
    MOCK_METHOD(
        TFuture<TGenerateTimestampResult>,
        GenerateTimestamp,
        (),
        (override));

    MOCK_METHOD(
        TFuture<TSelectObjectsResult>,
        SelectObjects,
        (NObjects::TObjectTypeValue objectType,
         const TAttributeSelector& selector,
         const TSelectObjectsOptions& options),
        (override));

    MOCK_METHOD(
        TFuture<TGetObjectResult>,
        GetObject,
        (TObjectIdentity objectIdentity,
         NObjects::TObjectTypeValue objectType,
         const TAttributeSelector& selector,
         const TGetObjectOptions& options),
        (override));

    MOCK_METHOD(
        TFuture<TGetObjectsResult>,
        GetObjects,
        (std::vector<TObjectIdentity> objectIdentities,
         NObjects::TObjectTypeValue objectType,
         const TAttributeSelector& selector,
         const TGetObjectOptions& options),
        (override));

    MOCK_METHOD(
        TFuture<TUpdateObjectResult>,
        UpdateObject,
        (TObjectIdentity objectIdentity,
         NObjects::TObjectTypeValue objectType,
         std::vector<TUpdate> updates,
         std::vector<TAttributeTimestampPrerequisite> attributeTimestampPrerequisites,
         const TUpdateObjectOptions& options),
         (override));

    MOCK_METHOD(
        TFuture<TUpdateObjectsResult>,
        UpdateObjects,
        (std::vector<TUpdateObjectsSubrequest> updateSubrequests,
         const TUpdateObjectOptions& options),
        (override));

    MOCK_METHOD(
        TFuture<TCreateObjectResult>,
        CreateObject,
        (NObjects::TObjectTypeValue objectType,
         TPayload attributesPayload,
         const TCreateObjectOptions& options,
         std::optional<TUpdateIfExisting> updateIfExisting),
        (override));

    MOCK_METHOD(
        TFuture<TCreateObjectsResult>,
        CreateObjects,
        ((std::vector<TCreateObjectsSubrequest> subrequests),
         const TCreateObjectOptions& options),
        (override));

    MOCK_METHOD(
        TFuture<TRemoveObjectResult>,
        RemoveObject,
        (TObjectIdentity objectIdentity,
         NObjects::TObjectTypeValue objectType,
         const TRemoveObjectOptions& options),
        (override));

    MOCK_METHOD(
        TFuture<TRemoveObjectsResult>,
        RemoveObjects,
        ((std::vector<TRemoveObjectsSubrequest> subrequests),
         const TRemoveObjectOptions& options),
        (override));

    MOCK_METHOD(
        TFuture<TStartTransactionResult>,
        StartTransaction,
        (const TStartTransactionOptions& options),
        (override));

    MOCK_METHOD(
        TFuture<TAbortTransactionResult>,
        AbortTransaction,
        (TTransactionId id),
        (override));

    MOCK_METHOD(
        TFuture<TCommitTransactionResult>,
        CommitTransaction,
        (TTransactionId id,
         const TCommitTransactionOptions& options),
        (override));

    MOCK_METHOD(
        TFuture<TWatchObjectsResult>,
        WatchObjects,
        (NObjects::TObjectTypeValue objectType,
         const TWatchObjectsOptions& options),
        (override));

    MOCK_METHOD(
        TFuture<TSelectObjectHistoryResult>,
        SelectObjectHistory,
        (TObjectIdentity objectIdentity,
         NObjects::TObjectTypeValue objectType,
         const TAttributeSelector& selector,
         const std::optional<TAttributeSelector>& distinctBy,
         const TSelectObjectHistoryOptions& options),
        (override));

    MOCK_METHOD(
        TFuture<TGetMastersResult>,
        GetMasters,
        (),
        (override));
};

} // NYT::NOrm::NClient::NNative
