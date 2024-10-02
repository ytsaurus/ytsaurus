#include <yt/yt/orm/client/native/config.h>
#include <yt/yt/orm/client/native/connection_impl.h>

#include <yt/yt/core/crypto/config.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NOrm::NClient::NNative::NTests {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TConfigTest, GrpcChannelAlias1)
{
    auto config = ConvertTo<TConnectionConfigPtr>(NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item("grpc_channel")
                .BeginMap()
                    .Item("grpc_arguments")
                        .BeginMap()
                            .Item("test_argument").Value("test_value")
                        .EndMap()
                    .Item("address").Value("test_address")
                .EndMap()
        .EndMap());

    ASSERT_GE(std::ssize(config->GrpcChannel->GrpcArguments), 1);
    ASSERT_EQ(config->GrpcChannel->Address, "test_address");
    ASSERT_EQ(config->GrpcChannel->GrpcArguments["test_argument"]->AsString()->GetValue(), "test_value");
    ASSERT_EQ(config->GrpcChannelTemplate->GrpcArguments["test_argument"]->AsString()->GetValue(), "test_value");
}

TEST(TConfigTest, GrpcChannelAlias2)
{
    auto config = ConvertTo<TConnectionConfigPtr>(NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item("grpc_channel_template")
                .BeginMap()
                    .Item("grpc_arguments")
                        .BeginMap()
                            .Item("test_argument").Value("test_value")
                        .EndMap()
                    .Item("address").Value("test_address")
                .EndMap()
        .EndMap());

    ASSERT_GE(std::ssize(config->GrpcChannelTemplate->GrpcArguments), 1);
    ASSERT_EQ(config->GrpcChannelTemplate->GrpcArguments["test_argument"]->AsString()->GetValue(), "test_value");
}

TEST(TConfigTest, GrpcChannelDefaults)
{
    auto config = ConvertTo<TConnectionConfigPtr>(NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item("grpc_channel")
                .BeginMap()
                    .Item("grpc_arguments")
                        .BeginMap()
                        .EndMap()
                .EndMap()
        .EndMap());

    ASSERT_GE(std::ssize(config->GrpcChannelTemplate->GrpcArguments), 7);
    ASSERT_EQ(config->GrpcChannelTemplate->GrpcArguments["grpc.http2.max_pings_without_data"]->AsInt64()->GetValue(), 0);
    ASSERT_EQ(config->GrpcChannelTemplate->GrpcArguments["grpc.keepalive_permit_without_calls"]->AsInt64()->GetValue(), 1);
    ASSERT_EQ(config->GrpcChannelTemplate->GrpcArguments["grpc.keepalive_time_ms"]->AsInt64()->GetValue(), 60'000);
    ASSERT_EQ(config->GrpcChannelTemplate->GrpcArguments["grpc.keepalive_timeout_ms"]->AsInt64()->GetValue(), 15'000);
    ASSERT_EQ(config->GrpcChannelTemplate->GrpcArguments["grpc.max_receive_message_length"]->AsUint64()->GetValue(), 128_MB);
    ASSERT_EQ(config->GrpcChannelTemplate->GrpcArguments["grpc.max_send_message_length"]->AsUint64()->GetValue(), 128_MB);
    ASSERT_EQ(config->GrpcChannelTemplate->GrpcArguments["grpc.max_metadata_size"]->AsUint64()->GetValue(), 16_KB);
}

TEST(TConfigTest, GrpcChannelDefaultsOverride)
{
    auto config = ConvertTo<TConnectionConfigPtr>(NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item("grpc_channel_template")
                .BeginMap()
                    .Item("grpc_arguments")
                        .BeginMap()
                            .Item("grpc.keepalive_time_ms").Value(30'000)
                            .Item("grpc.keepalive_permit_without_calls").Value(5)
                        .EndMap()
                .EndMap()
        .EndMap());

    ASSERT_GE(std::ssize(config->GrpcChannelTemplate->GrpcArguments), 7);
    ASSERT_EQ(config->GrpcChannelTemplate->GrpcArguments["grpc.keepalive_time_ms"]->AsInt64()->GetValue(), 30'000);
    ASSERT_EQ(config->GrpcChannelTemplate->GrpcArguments["grpc.keepalive_permit_without_calls"]->AsInt64()->GetValue(), 5);
    ASSERT_EQ(config->GrpcChannelTemplate->GrpcArguments["grpc.max_receive_message_length"]->AsUint64()->GetValue(), 128_MB);
}

TEST(TConfigTest, CredentialsCompatibility)
{
    auto connectionConfig = ConvertTo<TConnectionConfigPtr>(NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item("secure").Value(true)
            .Item("grpc_channel_template")
                .BeginMap()
                .EndMap()
        .EndMap());
    ASSERT_GT(connectionConfig->GrpcChannel->Credentials->PemRootCerts->Value->Size(), 0u);
    ASSERT_GT(connectionConfig->GrpcChannelTemplate->Credentials->PemRootCerts->Value->Size(), 0u);
}

TEST(TConfigTest, BalancingChannelConfig)
{
    auto connectionConfig = ConvertTo<TConnectionConfigPtr>(NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item("discovery_address").Value("test_address")
        .EndMap());
    auto balancingChannelConfig = NDetail::MakeBalancingChannelConfig(connectionConfig);
    ASSERT_TRUE(balancingChannelConfig->Addresses);
    ASSERT_EQ(std::ssize(*balancingChannelConfig->Addresses), 1);
}

TEST(TConfigTest, EndpointBasedConnectionConfig)
{
    auto connectionConfig = ConvertTo<TConnectionConfigPtr>(NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item("endpoints")
                .BeginMap()
                    .Item("cluster").Value("some_cluster")
                    .Item("endpoint_set_id").Value("some_endpoint_set_id")
                .EndMap()
        .EndMap());
    auto balancingChannelConfig = NDetail::MakeBalancingChannelConfig(connectionConfig);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NClient::NNative::NTests
