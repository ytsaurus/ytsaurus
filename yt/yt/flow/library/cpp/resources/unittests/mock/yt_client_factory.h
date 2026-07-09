#pragma once

#include <yt/yt/flow/library/cpp/resources/yt_client_factory.h>

#include <library/cpp/testing/gtest_extensions/gtest_extensions.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TMockYTClientFactory
    : public IYTClientFactory
{
public:
    TMockYTClientFactory();
    MOCK_METHOD(NApi::IClientPtr, GetClient, (TStringBuf url), (override));
};

DEFINE_REFCOUNTED_TYPE(TMockYTClientFactory)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
