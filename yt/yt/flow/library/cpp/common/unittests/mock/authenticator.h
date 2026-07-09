#pragma once

#include <yt/yt/flow/library/cpp/common/authenticator.h>

#include <yt/yt/core/rpc/authenticator.h>

#include <yt/yt/library/tvm/service/tvm_service.h>

#include <library/cpp/testing/gtest_extensions/gtest_extensions.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TMockPipelineAuthenticator
    : public IPipelineAuthenticator
{
    MOCK_METHOD(NAuth::IDynamicTvmServicePtr, GetTvmService, (), (override));
    MOCK_METHOD(NApi::TClientOptions, GetClientOptions, (), (override));
    MOCK_METHOD(NRpc::IChannelFactoryPtr, CreateSelfCredentialsInjectingChannelFactory, (NRpc::IChannelFactoryPtr underlying), (override));
    MOCK_METHOD(NRpc::IAuthenticatorPtr, CreateSelfRpcAuthenticator, (), (override));
    MOCK_METHOD(NRpc::IAuthenticatorPtr, CreateYTControllerRpcAuthenticator, (), (override));
    MOCK_METHOD(std::string, GetAuthDescription, (), (override));
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
