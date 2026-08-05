#pragma once

#include <yt/yt/client/hedging/cache.h>

#include <library/cpp/testing/gtest_extensions/gtest_extensions.h>

namespace NYT::NClient::NHedging {

class TMockClientsCache : public NRpc::IClientsCache {
public:
    MOCK_METHOD(NApi::IClientPtr, GetClient, (TStringBuf url), (override));
};

} // namespace NYT::NClient::NHedging
