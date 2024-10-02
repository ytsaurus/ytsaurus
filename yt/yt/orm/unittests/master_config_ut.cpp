#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/orm/server/master/config.h>

#include <yt/yt/ytlib/api/native/config.h>

namespace NYT::NOrm::NServer::NTests {

using namespace NYT::NOrm::NServer::NMaster;

////////////////////////////////////////////////////////////////////////////////

TYTConnectorConfigPtr CreateYTConnectorConfig(bool requireAuthentication)
{
    if (requireAuthentication) {
        return NYT::New<TDataModelYTConnectorConfig<true>>();
    } else {
        return NYT::New<TDataModelYTConnectorConfig<false>>();
    }
}

TEST(TMasterConfigTest, RequireConnectionClusterUrl)
{
    auto config = CreateYTConnectorConfig(true);
    config->Connection = New<NYT::NApi::NNative::TConnectionCompoundConfig>();
    EXPECT_THROW(config->Postprocess(), TErrorException);
    config->ConnectionClusterUrl = "localhost";
    EXPECT_NO_THROW(config->Postprocess());
}

TEST(TMasterConfigTest, IgnoreConnectionClusterUrl)
{
    auto config = CreateYTConnectorConfig(false);
    config->Connection = New<NYT::NApi::NNative::TConnectionCompoundConfig>();
    EXPECT_NO_THROW(config->Postprocess());
}

TEST(TMasterConfigTest, ConnectionClusterUrlOnlyWithConnection)
{
    auto config = CreateYTConnectorConfig(false);
    config->SetDefaults();
    config->RpcProxyConnection = New<NYT::NApi::NRpcProxy::TConnectionConfig>();
    config->RpcProxyConnection->ClusterUrl = "localhost";
    config->ConnectionClusterUrl = "localhost";
    EXPECT_THROW(config->Postprocess(), TErrorException);
}

TEST(TMasterConfigTest, OneOfConnectionOrRpcProxyConnection)
{
    auto config = CreateYTConnectorConfig(false);
    config->Connection = New<NYT::NApi::NNative::TConnectionCompoundConfig>();
    config->RpcProxyConnection = New<NYT::NApi::NRpcProxy::TConnectionConfig>();
    config->RpcProxyConnection->ClusterUrl = "localhost";
    EXPECT_THROW(config->Postprocess(), TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NTests
