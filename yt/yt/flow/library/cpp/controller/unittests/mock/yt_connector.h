#pragma once

#include <yt/yt/flow/library/cpp/controller/yt_connector.h>

#include <library/cpp/testing/gtest_extensions/gtest_extensions.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TMockYTConnector);

struct TMockYTConnector
    : public IYTConnector
{
    // clang-format off

    MOCK_METHOD(TFuture<TPipelineAttributes>, GetPipelineAttributes, (), (override));
    MOCK_METHOD(TFuture<TFlowTablesBundleInfo>, GetFlowTablesBundle, (), (override));
    MOCK_METHOD(NYPath::TRichYPath, GetPipelinePath, (), (override));
    MOCK_METHOD(NApi::IClientPtr, GetClient, (), (override));
    MOCK_METHOD(NClient::NCache::IClientsCachePtr, GetClientsCache, (), (override));
    MOCK_METHOD(void, Start, (), (override));
    MOCK_METHOD(EYTConnectorState, GetState, (), (const, override));
    MOCK_METHOD(bool, IsLeader, (), (const, override));
    MOCK_METHOD(void, ValidateLeader, (), (const, override));
    MOCK_METHOD(void, Disconnect, (), (override));
    MOCK_METHOD(TFuture<NApi::ITransactionPtr>, StartTransaction, (
        NTransactionClient::ETransactionType type,
        NApi::TTransactionStartOptions options),
        (override));
    MOCK_METHOD(NPrerequisiteClient::TPrerequisiteId, GetPrerequisiteId, (), (const, override));

    MOCK_METHOD(void, SubscribeLeadingStarted, (const TCallback<void()>& callback), (override));
    MOCK_METHOD(void, UnsubscribeLeadingStarted, (const TCallback<void()>& callback), (override));

    MOCK_METHOD(void, SubscribeLeadingEnded, (const TCallback<void()>& callback), (override));
    MOCK_METHOD(void, UnsubscribeLeadingEnded, (const TCallback<void()>& callback), (override));

    // clang-format on
};

DEFINE_REFCOUNTED_TYPE(TMockYTConnector);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
