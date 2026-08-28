#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/flow/library/cpp/companion/companion_proxy.h>

#include <yt/yt/core/ytree/convert.h>

#include <contrib/libs/grpc/include/grpc/impl/grpc_types.h>

namespace NYT::NFlow::NCompanion {
namespace {

////////////////////////////////////////////////////////////////////////////////

// Every per-computation companion channel targets the same address with the same
// args; gRPC's global subchannel pool would dedup them to one connection and pin all
// RPCs to a single worker. The local-subchannel-pool argument defeats that dedup so
// SO_REUSEPORT fans the channels across workers. This test guards against a regression
// dropping the argument.
TEST(TCompanionProxyTest, GrpcArgumentsEnableLocalSubchannelPool)
{
    auto arguments = BuildCompanionGrpcArguments();

    auto it = arguments.find(GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL);
    ASSERT_NE(it, arguments.end()) << "missing " << GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL;
    EXPECT_EQ(1, NYTree::ConvertTo<int>(it->second));
}

TEST(TCompanionProxyTest, GrpcArgumentsKeepMessageSizeLimits)
{
    auto arguments = BuildCompanionGrpcArguments();

    EXPECT_TRUE(arguments.contains(GRPC_ARG_MAX_SEND_MESSAGE_LENGTH));
    EXPECT_TRUE(arguments.contains(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH));
}

// CreateCompanionProxy builds its channel from this config, so this covers the wiring
// half the args-map tests above cannot: a config that stops carrying the arguments
// would revert production to one deduped connection pinning all RPCs to one worker.
TEST(TCompanionProxyTest, ChannelConfigCarriesAddressAndGrpcArguments)
{
    auto config = BuildCompanionChannelConfig("localhost:12345");

    EXPECT_EQ("localhost:12345", config->Address);
    EXPECT_TRUE(config->GrpcArguments.contains(GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL));
    EXPECT_TRUE(config->GrpcArguments.contains(GRPC_ARG_MAX_SEND_MESSAGE_LENGTH));
    EXPECT_TRUE(config->GrpcArguments.contains(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanion
