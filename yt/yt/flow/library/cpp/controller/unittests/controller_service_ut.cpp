#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/controller/controller_service.h>
#include <yt/yt/flow/library/cpp/controller/flow_executor.h>

#include <yt/yt/flow/library/cpp/client/authentication.h>
#include <yt/yt/flow/library/cpp/client/controller/controller_service_proxy.h>

#include <yt/yt/flow/library/cpp/common/authenticator.h>

#include <yt/yt/library/auth/credentials_injecting_channel.h>

#include <yt/yt/library/tvm/tvm_base.h>

#include <yt/yt/client/api/options.h>
#include <yt/yt/client/unittests/mock/client.h>
#include <yt/yt/client/unittests/mock/connection.h>

#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/rpc/authenticator.h>
#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/bus/server.h>
#include <yt/yt/core/rpc/server.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/testing/common/network.h>

namespace NYT::NFlow::NController {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NRpc;
using namespace NYson;
using namespace NYTree;

using ::testing::_;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::StrictMock;
using ::testing::Throw;

////////////////////////////////////////////////////////////////////////////////

class TMockFlowExecutor
    : public IFlowExecutor
{
public:
    MOCK_METHOD(TYsonString, Execute, (const std::string& command, const TYsonString& argument, const std::string& user), (override));
    MOCK_METHOD(void, AuthorizeCommand, (const std::string& command, const std::string& user), (override));
    MOCK_METHOD(TGetFlowViewResult, GetFlowView, (const TGetFlowViewArg& argument), (override));
    MOCK_METHOD(TGetPipelineDynamicSpecResult, GetPipelineDynamicSpec, (const TGetPipelineDynamicSpecArg& argument), (override));
    MOCK_METHOD(TSetPipelineDynamicSpecResult, SetPipelineDynamicSpec, (const TSetPipelineDynamicSpecArg& argument), (override));
    MOCK_METHOD(TGetPipelineSpecResult, GetPipelineSpec, (const TGetPipelineSpecArg& argument), (override));
    MOCK_METHOD(TSetPipelineSpecResult, SetPipelineSpec, (const TSetPipelineSpecArg& argument), (override));
    MOCK_METHOD(TSetPipelineSpecsResult, SetPipelineSpecs, (const TSetPipelineSpecsArg& argument), (override));
    MOCK_METHOD(TGetPipelineStateResult, GetPipelineState, (const TGetPipelineStateArg& argument), (override));
    MOCK_METHOD(TSetTargetPipelineStateResult, SetTargetPipelineState, (const TSetTargetPipelineStateArg& argument), (override));
    MOCK_METHOD(TGetFlowCoreTargetResult, GetFlowCoreTarget, (const TGetFlowCoreTargetArg& argument), (override));
    MOCK_METHOD(TSetFlowCoreTargetResult, SetFlowCoreTarget, (const TSetFlowCoreTargetArg& argument), (override));
};

//! Stands for the proxy signature authenticator: claims every request the client credentials
//! authenticator has left.
class TFakeProxySignatureAuthenticator
    : public IAuthenticator
{
public:
    bool CanAuthenticate(const TAuthenticationContext& /*context*/) override
    {
        return true;
    }

    TFuture<TAuthenticationResult> AsyncAuthenticate(const TAuthenticationContext& /*context*/) override
    {
        TAuthenticationResult result;
        result.User = "yt-proxy";
        result.Realm = "test";
        return MakeFuture(result);
    }
};

//! Gives the service the same pair of authenticators the pipeline authenticator builds, with the
//! real client credentials one; the rest of the pipeline authentication is not used here.
class TFakePipelineAuthenticator
    : public IPipelineAuthenticator
{
public:
    explicit TFakePipelineAuthenticator(IConnectionPtr connection)
        : Connection_(std::move(connection))
    { }

    NAuth::IDynamicTvmServicePtr GetTvmService() override
    {
        return nullptr;
    }

    TClientOptions GetClientOptions() override
    {
        return {};
    }

    IChannelFactoryPtr CreateSelfCredentialsInjectingChannelFactory(IChannelFactoryPtr underlying) override
    {
        return underlying;
    }

    IAuthenticatorPtr CreateSelfRpcAuthenticator() override
    {
        return New<TFakeProxySignatureAuthenticator>();
    }

    IAuthenticatorPtr CreateYTControllerRpcAuthenticator() override
    {
        return CreateCompositeAuthenticator({
            CreateClientCredentialsAuthenticator(Connection_),
            New<TFakeProxySignatureAuthenticator>(),
        });
    }

    TPipelineAuthenticationDescriptionPtr GetPipelineAuthenticationDescription() override
    {
        return nullptr;
    }

private:
    const IConnectionPtr Connection_;
};

////////////////////////////////////////////////////////////////////////////////

class TControllerServiceTest
    : public ::testing::Test
{
protected:
    const TIntrusivePtr<StrictMock<TMockFlowExecutor>> FlowExecutor_ = New<StrictMock<TMockFlowExecutor>>();
    //! The cluster the controller asks about the caller's credentials. Strict: a forwarded
    //! request must never get there.
    const TIntrusivePtr<StrictMock<TMockConnection>> Connection_ = New<StrictMock<TMockConnection>>();
    const TIntrusivePtr<StrictMock<TMockClient>> Client_ = New<StrictMock<TMockClient>>();
    const TActionQueuePtr Queue_ = New<TActionQueue>("ControllerService");
    const NTesting::TPortHolder Port_ = NTesting::GetFreePort();
    IServerPtr Server_;

    void SetUp() override
    {
        Server_ = NRpc::NBus::CreateBusServer(
            NYT::NBus::NTcp::CreateBusServer(NYT::NBus::NTcp::TBusServerConfig::CreateTcp(Port_)));
        Server_->RegisterService(CreateControllerService(
            FlowExecutor_,
            New<TFakePipelineAuthenticator>(Connection_)->CreateYTControllerRpcAuthenticator(),
            Queue_->GetInvoker()));
        Server_->Start();
    }

    void TearDown() override
    {
        Server_->Stop()
            .BlockingGet()
            .ThrowOnError();
    }

    //! On a cluster with TVM the RPC proxy comes with a service ticket issued for the controller;
    //! it must not be taken for the caller's credentials.
    static TClientOptions ProxyOptions()
    {
        return TClientOptions::FromServiceTicketAuth(New<NAuth::TServiceTicketFixedAuth>("proxy-ticket"));
    }

    //! The cluster names alice as the owner of the token a direct request carries.
    void ExpectClusterAuthentication()
    {
        EXPECT_CALL(*Connection_, CreateClient(_))
            .WillOnce([this] (const TClientOptions& options) {
                EXPECT_EQ(options.Token, "secret");
                return Client_;
            });
        EXPECT_CALL(*Client_, GetCurrentUser(_))
            .WillOnce(Return(MakeFuture(TGetCurrentUserResult{.User = "alice"})));
    }

    //! |options| carry the caller's own credentials for a direct request and those of the RPC
    //! proxy for a forwarded one.
    TControllerServiceProxy CreateProxy(const TClientOptions& options)
    {
        auto channel = NRpc::NBus::CreateTcpBusChannelFactory(New<NYT::NBus::NTcp::TBusConfig>())
            ->CreateChannel(Format("localhost:%v", static_cast<ui16>(Port_)));
        TControllerServiceProxy proxy(NAuth::CreateCredentialsInjectingChannel(std::move(channel), options));
        proxy.SetDefaultTimeout(TDuration::Seconds(10));
        return proxy;
    }
};

TEST_F(TControllerServiceTest, DirectFlowExecuteChecksPermission)
{
    // The caller is authenticated first, and the command runs only after it is authorized.
    InSequence sequence;
    ExpectClusterAuthentication();
    EXPECT_CALL(*FlowExecutor_, AuthorizeCommand("get-pipeline-state", "alice"));
    EXPECT_CALL(*FlowExecutor_, Execute("get-pipeline-state", _, "alice"))
        .WillOnce(Return(TYsonString(TStringBuf("{pipeline_state=working}"))));

    auto proxy = CreateProxy(TClientOptions::FromUserAndToken("alice", "secret"));
    auto req = proxy.FlowExecute();
    MarkDirectRequest(&req->Header());
    req->set_command("get-pipeline-state");
    auto rsp = req->Invoke()
        .BlockingGet()
        .ValueOrThrow();
    EXPECT_EQ(rsp->result(), "{pipeline_state=working}");
}

TEST_F(TControllerServiceTest, DirectFlowExecuteStopsOnDeniedPermission)
{
    // No Execute expectation: the strict mock fails the test if the command runs anyway.
    EXPECT_CALL(*FlowExecutor_, AuthorizeCommand("set-target-pipeline-state", "alice"))
        .WillOnce(Throw(TErrorException() <<= TError("No \"write\" permission for pipeline")));

    ExpectClusterAuthentication();

    auto proxy = CreateProxy(TClientOptions::FromUserAndToken("alice", "secret"));
    auto req = proxy.FlowExecute();
    MarkDirectRequest(&req->Header());
    req->set_command("set-target-pipeline-state");
    auto error = req->Invoke()
        .BlockingGet();
    ASSERT_FALSE(error.IsOK());
    EXPECT_TRUE(error.FindMatching([] (const TError& inner) {
        return inner.GetMessage() == "No \"write\" permission for pipeline";
    })) << ToString(error);
}

TEST_F(TControllerServiceTest, ProxyFlowExecuteSkipsPermissionCheck)
{
    // The RPC proxy has checked the permission already and names the user in the request. No
    // AuthorizeCommand expectation: the strict mock fails the test if the controller checks again.
    EXPECT_CALL(*FlowExecutor_, Execute("set-target-pipeline-state", _, "bob"))
        .WillOnce(Return(TYsonString(TStringBuf("{}"))));

    auto proxy = CreateProxy(ProxyOptions());
    auto req = proxy.FlowExecute();
    req->set_command("set-target-pipeline-state");
    req->set_user("bob");
    req->Invoke()
        .BlockingGet()
        .ThrowOnError();
}

TEST_F(TControllerServiceTest, DirectRequestReachesFlowExecuteOnly)
{
    ExpectClusterAuthentication();

    auto proxy = CreateProxy(TClientOptions::FromUserAndToken("alice", "secret"));
    auto req = proxy.StopPipeline();
    MarkDirectRequest(&req->Header());
    auto error = req->Invoke()
        .BlockingGet();
    ASSERT_FALSE(error.IsOK());
    EXPECT_TRUE(error.FindMatching(NRpc::EErrorCode::AuthenticationError)) << ToString(error);
    EXPECT_THAT(error.GetMessage(), ::testing::HasSubstr("accepts requests forwarded by the RPC proxy only"));
}

TEST_F(TControllerServiceTest, ProxyRequestReachesOtherMethods)
{
    EXPECT_CALL(*FlowExecutor_, SetTargetPipelineState(_))
        .WillOnce(Return(TSetTargetPipelineStateResult()));

    // A forwarded request is not marked as direct; the proxy signature authenticates it.
    auto proxy = CreateProxy(ProxyOptions());
    proxy.StopPipeline()
        ->Invoke()
        .BlockingGet()
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NController
