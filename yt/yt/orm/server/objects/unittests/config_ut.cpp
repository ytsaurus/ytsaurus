#include <yt/yt/orm/server/master/config.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NOrm::NServer::NObjects::NTests {
namespace {

using namespace NAccessControl;
using namespace NMaster;

////////////////////////////////////////////////////////////////////////////////

class TDerivedMasterConfig
    : public TMasterConfig
{
    TObjectManagerConfigPtr GetObjectManagerConfig() const override
    {
        return nullptr;
    }

    TAccessControlManagerConfigPtr GetAccessControlManagerConfig() const override
    {
        return nullptr;
    }

    REGISTER_YSON_STRUCT(TDerivedMasterConfig);

    static void Register(TRegistrar registrar)
    {
        TMasterConfig::DoRegister(registrar);
    }
};

DEFINE_REFCOUNTED_TYPE(TDerivedMasterConfig)

////////////////////////////////////////////////////////////////////////////////

TEST(TConfigTest, GrpcDefaults)
{
    auto configNode = NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item("secure_client_grpc_server")
                .BeginMap()
                    .Item("grpc_arguments")
                        .BeginMap()
                        .EndMap()
                    .Item("addresses")
                        .BeginList()
                            .Item()
                                .BeginMap()
                                    .Item("address").Value("test_address")
                                .EndMap()
                        .EndList()
                .EndMap()
            .Item("db_name").Value("test_db_name")
        .EndMap();
    auto config = NYTree::ConvertTo<TIntrusivePtr<TDerivedMasterConfig>>(configNode);

    ASSERT_EQ(config->ClientGrpcServer.Get(), nullptr);

    ASSERT_GE(std::ssize(config->SecureClientGrpcServer->GrpcArguments), 8);
    ASSERT_EQ(config->SecureClientGrpcServer->GrpcArguments["grpc.http2.max_pings_without_data"]->AsInt64()->GetValue(), 0);
    ASSERT_EQ(config->SecureClientGrpcServer->GrpcArguments["grpc.http2.min_ping_interval_without_data_ms"]->AsInt64()->GetValue(), 10'000);
    ASSERT_EQ(config->SecureClientGrpcServer->GrpcArguments["grpc.keepalive_permit_without_calls"]->AsInt64()->GetValue(), 1);
    ASSERT_EQ(config->SecureClientGrpcServer->GrpcArguments["grpc.keepalive_time_ms"]->AsInt64()->GetValue(), 60'000);
    ASSERT_EQ(config->SecureClientGrpcServer->GrpcArguments["grpc.keepalive_timeout_ms"]->AsInt64()->GetValue(), 15'000);
    ASSERT_EQ(config->SecureClientGrpcServer->GrpcArguments["grpc.max_receive_message_length"]->AsUint64()->GetValue(), 128_MB);
    ASSERT_EQ(config->SecureClientGrpcServer->GrpcArguments["grpc.max_send_message_length"]->AsUint64()->GetValue(), 128_MB);
    ASSERT_EQ(config->SecureClientGrpcServer->GrpcArguments["grpc.max_metadata_size"]->AsUint64()->GetValue(), 16_KB);
}

TEST(TConfigTest, GrpcDefaultsOverride)
{
    auto configNode = NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item("client_grpc_server")
                .BeginMap()
                    .Item("grpc_arguments")
                        .BeginMap()
                            .Item("grpc.http2.max_pings_without_data").Value(1)
                            .Item("grpc.keepalive_permit_without_calls").Value(5)
                            .Item("grpc.keepalive_time_ms").Value(30'000)
                            .Item("grpc.max_metadata_size").Value(24_KB)
                            .Item("test_argument").Value("test_value")
                        .EndMap()
                    .Item("addresses")
                        .BeginList()
                            .Item()
                                .BeginMap()
                                    .Item("address").Value("test_address")
                                .EndMap()
                        .EndList()
                .EndMap()
            .Item("secure_client_grpc_server")
                .BeginMap()
                    .Item("grpc_arguments")
                        .BeginMap()
                            .Item("grpc.keepalive_permit_without_calls").Value(10)
                            .Item("grpc.keepalive_timeout_ms").Value(10'000)
                            .Item("other_test_argument").Value("other_test_value")
                        .EndMap()
                    .Item("addresses")
                        .BeginList()
                            .Item()
                                .BeginMap()
                                    .Item("address").Value("test_address")
                                .EndMap()
                        .EndList()
                .EndMap()
            .Item("db_name").Value("test_db_name")
        .EndMap();
    auto config = NYTree::ConvertTo<TIntrusivePtr<TDerivedMasterConfig>>(configNode);

    ASSERT_GE(std::ssize(config->ClientGrpcServer->GrpcArguments), 8);
    ASSERT_EQ(config->ClientGrpcServer->GrpcArguments["grpc.http2.max_pings_without_data"]->AsInt64()->GetValue(), 1);
    ASSERT_EQ(config->ClientGrpcServer->GrpcArguments["grpc.http2.min_ping_interval_without_data_ms"]->AsInt64()->GetValue(), 10'000);
    ASSERT_EQ(config->ClientGrpcServer->GrpcArguments["grpc.keepalive_permit_without_calls"]->AsInt64()->GetValue(), 5);
    ASSERT_EQ(config->ClientGrpcServer->GrpcArguments["grpc.keepalive_time_ms"]->AsInt64()->GetValue(), 30'000);
    ASSERT_EQ(config->ClientGrpcServer->GrpcArguments["grpc.keepalive_timeout_ms"]->AsInt64()->GetValue(), 15'000);
    ASSERT_EQ(config->ClientGrpcServer->GrpcArguments["grpc.max_receive_message_length"]->AsUint64()->GetValue(), 128_MB);
    ASSERT_EQ(config->ClientGrpcServer->GrpcArguments["grpc.max_send_message_length"]->AsUint64()->GetValue(), 128_MB);
    ASSERT_EQ(config->ClientGrpcServer->GrpcArguments["grpc.max_metadata_size"]->AsUint64()->GetValue(), 24_KB);
    ASSERT_EQ(config->ClientGrpcServer->GrpcArguments["test_argument"]->AsString()->GetValue(), "test_value");

    ASSERT_GE(std::ssize(config->SecureClientGrpcServer->GrpcArguments), 8);
    ASSERT_EQ(config->SecureClientGrpcServer->GrpcArguments["grpc.http2.max_pings_without_data"]->AsInt64()->GetValue(), 0);
    ASSERT_EQ(config->SecureClientGrpcServer->GrpcArguments["grpc.http2.min_ping_interval_without_data_ms"]->AsInt64()->GetValue(), 10'000);
    ASSERT_EQ(config->SecureClientGrpcServer->GrpcArguments["grpc.keepalive_permit_without_calls"]->AsInt64()->GetValue(), 10);
    ASSERT_EQ(config->SecureClientGrpcServer->GrpcArguments["grpc.keepalive_time_ms"]->AsInt64()->GetValue(), 60'000);
    ASSERT_EQ(config->SecureClientGrpcServer->GrpcArguments["grpc.keepalive_timeout_ms"]->AsInt64()->GetValue(), 10'000);
    ASSERT_EQ(config->SecureClientGrpcServer->GrpcArguments["grpc.max_receive_message_length"]->AsUint64()->GetValue(), 128_MB);
    ASSERT_EQ(config->SecureClientGrpcServer->GrpcArguments["grpc.max_send_message_length"]->AsUint64()->GetValue(), 128_MB);
    ASSERT_EQ(config->SecureClientGrpcServer->GrpcArguments["grpc.max_metadata_size"]->AsUint64()->GetValue(), 16_KB);
    ASSERT_EQ(config->SecureClientGrpcServer->GrpcArguments["other_test_argument"]->AsString()->GetValue(), "other_test_value");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NServer::NObjects::NTests
