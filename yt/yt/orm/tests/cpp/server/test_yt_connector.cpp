#include "bootstrap_mock.h"

#include <yt/yt/orm/server/master/config.h>
#include <yt/yt/orm/server/objects/public.h>
#include <yt/yt/orm/server/access_control/access_control_manager.h>
#include <yt/yt/orm/server/master/yt_connector.h>

#include <yt/yt/ytlib/api/connection.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/library/auth_server/helpers.h>

#include <yt/yt/library/tracing/jaeger/sampler.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/rpc/authentication_identity.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/system/env.h>

namespace {

using namespace NYT::NOrm::NServer::NAccessControl;
using namespace NYT::NOrm::NServer::NMaster;
using namespace NYT::NOrm::NServer::NObjects;
using namespace NYT::NConcurrency;

using ::testing::Return;
using ::testing::ReturnRef;

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

class TYTConnectorTest
    : public ::testing::Test
{
public:
    TYTConnectorTest()
        : ActionQueue_(NYT::New<TActionQueue>())
        , Config_(LoadConfig("YT_DRIVER_CONFIG_PATH"))
        , NativeConfig_(LoadConfig("YT_NATIVE_DRIVER_CONFIG_PATH"))
        , User_("orm_user")
        , Token_("abcdef")
    {
        auto configNode = NYT::NYTree::BuildYsonNodeFluently()
            .BeginMap()
                .Item("db_name").Value("test_db_name")
            .EndMap();
        MasterConfig_ = NYT::NYTree::ConvertTo<NYT::TIntrusivePtr<TDerivedMasterConfig>>(configNode);
        EXPECT_CALL(BootstrapMock_, GetInitialConfig()).WillRepeatedly(ReturnRef(MasterConfig_));

        EXPECT_CALL(BootstrapMock_, GetControlInvoker()).WillRepeatedly(ReturnRef(ActionQueue_->GetInvoker()));
        EXPECT_CALL(BootstrapMock_, GetWorkerPoolInvoker()).WillRepeatedly(Return(ActionQueue_->GetInvoker()));

        auto connection = NYT::NApi::CreateConnection(NativeConfig_);
        auto client = connection->CreateClient(NYT::NApi::TClientOptions::FromUser("root"));
        auto tokenHash = NYT::NAuth::GetCryptoHash(Token_);
        if (!WaitFor(client->NodeExists("//sys/users/" + User_)).ValueOrThrow()) {
            NYT::NApi::TCreateObjectOptions options;
            auto attributes = NYT::NYTree::CreateEphemeralAttributes();
            attributes->Set("name", User_);
            options.Attributes = std::move(attributes);
            WaitFor(client->CreateObject(NYT::NObjectClient::EObjectType::User, options))
                .ThrowOnError();
            WaitFor(client->SetNode("//sys/tokens/" + tokenHash, NYT::NYson::ConvertToYsonString(User_)))
                .ThrowOnError();
        }
    }

    static NYT::NYTree::INodePtr LoadConfig(const TString& varName)
    {
        auto configPath = GetEnv(varName);
        YT_VERIFY(configPath);
        TIFStream configStream(configPath);
        return NYT::NYTree::ConvertToNode(&configStream);
    }

protected:
    const TActionQueuePtr ActionQueue_;
    const NYT::NYTree::INodePtr Config_;
    const NYT::NYTree::INodePtr NativeConfig_;
    const std::string User_;
    const TString Token_;
    TMasterConfigPtr MasterConfig_;

    ::testing::StrictMock<TBootstrapMock> BootstrapMock_;
};

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////

TYTConnectorConfigPtr CreateYTConnectorConfig(
    const std::string& user,
    NYT::NYPath::TYPath rootPath,
    bool requireAuthentication)
{
    TYTConnectorConfigPtr result;
    if (requireAuthentication) {
        result = NYT::New<TDataModelYTConnectorConfig<true>>();
    } else {
        result = NYT::New<TDataModelYTConnectorConfig<false>>();
    }

    result->User = user;
    result->RootPath = rootPath;

    return result;
}

TEST_F(TYTConnectorTest, TestDiscovery)
{
    auto config = CreateYTConnectorConfig(User_, "//test", true);
    config->ConnectionClusterUrl = NYT::NYTree::ConvertTo<TString>(Config_->AsMap()->FindChild("cluster_url"));
    EXPECT_THROW(NYT::New<TYTConnector>(&BootstrapMock_, config, 1), NYT::TErrorException);

    config->Token = Token_;

    EXPECT_NO_THROW(NYT::New<TYTConnector>(&BootstrapMock_, config, 1));
}

TEST_F(TYTConnectorTest, TestRequireAuthentication)
{
    auto config = CreateYTConnectorConfig(User_, "//test", true);
    config->ConnectionClusterUrl = NYT::NYTree::ConvertTo<TString>(Config_->AsMap()->FindChild("cluster_url"));
    config->Connection = NYT::NYTree::ConvertTo<NYT::NApi::NNative::TConnectionCompoundConfigPtr>(NativeConfig_);
    EXPECT_THROW(NYT::New<TYTConnector>(&BootstrapMock_, config, 1), NYT::TErrorException);
    config->Token = Token_;
    EXPECT_NO_THROW(NYT::New<TYTConnector>(&BootstrapMock_, config, 1));
}

TEST_F(TYTConnectorTest, TestDoesNotRequireAuthentication)
{
    auto config = CreateYTConnectorConfig(User_, "//test", false);
    config->Connection = NYT::NYTree::ConvertTo<NYT::NApi::NNative::TConnectionCompoundConfigPtr>(NativeConfig_);
    EXPECT_NO_THROW(NYT::New<TYTConnector>(&BootstrapMock_, config, 1));
}

TEST_F(TYTConnectorTest, TestUserTagSetProperly)
{
    auto config = CreateYTConnectorConfig(User_, "//test", true);
    config->ConnectionClusterUrl = NYT::NYTree::ConvertTo<TString>(Config_->AsMap()->FindChild("cluster_url"));
    config->Connection = NYT::NYTree::ConvertTo<NYT::NApi::NNative::TConnectionCompoundConfigPtr>(NativeConfig_);
    config->Token = Token_;
    auto connector = NYT::New<TYTConnector>(&BootstrapMock_, config, 1);
    {
        auto client = connector->GetNativeClient(connector->FormatUserTag("yp-planner"));
        EXPECT_EQ(client->GetOptions().UserTag, "orm_user:yp-planner");
    }
    {
        auto client = connector->GetNativeClient(connector->FormatUserTag());
        EXPECT_EQ(client->GetOptions().UserTag, "orm_user:root");
    }
    {
        auto rpcIdentity = NYT::NRpc::TAuthenticationIdentity("tvm:42", "good-client");
        auto guard = NYT::NRpc::TCurrentAuthenticationIdentityGuard(&rpcIdentity);

        auto client = connector->GetNativeClient(connector->FormatUserTag());
        EXPECT_EQ(client->GetOptions().UserTag, "orm_user:tvm:42:good-client");
    }
}

////////////////////////////////////////////////////////////////////////////////
