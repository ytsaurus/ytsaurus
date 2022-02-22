#pragma once

#include <yt/yt/client/hedging/cache.h>

#include <library/cpp/testing/gtest_extensions/gtest_extensions.h>

namespace NYT::NHedgingClient {

class TMockClientsCache : public NYT::NHedgingClient::NRpc::IClientsCache {
public:
    MOCK_METHOD(NYT::NApi::IClientPtr, GetClient, (TStringBuf url), (override));
};

} // namespace NYT::NHedgingClient
