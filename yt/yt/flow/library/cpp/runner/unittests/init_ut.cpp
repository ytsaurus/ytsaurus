#include <yt/yt/flow/library/cpp/misc/testing/env_guard.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/public.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/system/env.h>

namespace NYT::NFlow {
namespace {

using namespace NTesting;

////////////////////////////////////////////////////////////////////////////////

TEST(TInitializeTest, DefaultsGrpcDnsResolverToNative)
{
    TEnvGuard flowModeGuard{std::string(FlowModeEnvVarName)};
    TEnvGuard resolverGuard("GRPC_DNS_RESOLVER");
    const char* argv[] = {"flow_test"};

    Initialize(1, argv);

    EXPECT_EQ("native", GetEnv("GRPC_DNS_RESOLVER"));
}

TEST(TInitializeTest, PreservesExplicitGrpcDnsResolver)
{
    TEnvGuard flowModeGuard{std::string(FlowModeEnvVarName)};
    TEnvGuard resolverGuard("GRPC_DNS_RESOLVER", "ares");
    const char* argv[] = {"flow_test"};

    Initialize(1, argv);

    EXPECT_EQ("ares", GetEnv("GRPC_DNS_RESOLVER"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
