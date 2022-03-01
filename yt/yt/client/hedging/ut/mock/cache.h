#pragma once

#include <yt/yt/client/hedging/cache.h>

#include <library/cpp/testing/gtest_extensions/gtest_extensions.h>

namespace NYT::NClient::NHedging {

class TMockClientsCache : public NYT::NClient::NHedging::NRpc::IClientsCache {
public:
    MOCK_METHOD(NYT::NApi::IClientPtr, GetClient, (TStringBuf url), (override));
};

} // namespace NYT::NClient::NHedging
