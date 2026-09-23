#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/pipeline_helpers/flow_execute/flow_execute.h>

#include <yt/yt/flow/library/cpp/client/authentication.h>
#include <yt/yt/flow/library/cpp/client/controller/controller_service_proxy.h>
#include <yt/yt/flow/library/cpp/client/public.h>
#include <yt/yt/flow/library/cpp/misc/self_signed_certificate.h>

#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/unittests/mock/client.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/crypto/config.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/bus/server.h>
#include <yt/yt/core/rpc/server.h>
#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>

#include <library/cpp/testing/common/network.h>

namespace NYT::NFlow {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NController;
using namespace NRpc;
using namespace NTableClient;
using namespace NYson;
using namespace NYTree;

using ::testing::_;
using ::testing::Return;
using ::testing::ReturnRef;
using ::testing::StrictMock;

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf PipelinePath = "//tmp/pipeline";

TDirectControllerCommandsConfigPtr MakeDirectConfig()
{
    auto config = New<TDirectControllerCommandsConfig>();
    config->Enabled = true;
    return config;
}

//! Replies to every command with the command name and a fixed pipeline state, recording what
//! the request carried.
class TFakeControllerService
    : public TServiceBase
{
public:
    struct TRecordedRequest
    {
        std::string Command;
        std::string Argument;
        std::string User;
        std::string Token;
        bool Direct = false;
    };

    explicit TFakeControllerService(IInvokerPtr invoker)
        : TServiceBase(
            std::move(invoker),
            TControllerServiceProxy::GetDescriptor(),
            NLogging::TLogger("FakeController"))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FlowExecute));
    }

    TRecordedRequest GetLastRequest()
    {
        auto guard = Guard(Lock_);
        return LastRequest_;
    }

    //! Makes every reply wait for |delay|.
    void SetReplyDelay(TDuration delay)
    {
        ReplyDelay_ = delay;
    }

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    TRecordedRequest LastRequest_;
    std::atomic<TDuration> ReplyDelay_ = TDuration::Zero();

    DECLARE_RPC_SERVICE_METHOD(NController::NProto, FlowExecute)
    {
        const auto& header = context->GetRequestHeader();
        TRecordedRequest recorded{
            .Command = request->command(),
            .Argument = request->argument(),
            .User = header.user(),
            .Direct = IsDirectRequest(header),
        };
        if (header.HasExtension(NRpc::NProto::TCredentialsExt::credentials_ext)) {
            recorded.Token = header.GetExtension(NRpc::NProto::TCredentialsExt::credentials_ext).token();
        }
        {
            auto guard = Guard(Lock_);
            LastRequest_ = std::move(recorded);
        }

        if (auto delay = ReplyDelay_.load()) {
            TDelayedExecutor::WaitForDuration(delay);
        }

        response->set_result(BuildYsonStringFluently()
                .BeginMap()
                .Item("command")
                .Value(request->command())
                .Item("pipeline_state")
                .Value(EPipelineState::Working)
                .EndMap()
                .ToString());
        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDirectFlowExecuteTest
    : public ::testing::Test
{
protected:
    const NTesting::TPortHolder Port_ = NTesting::GetFreePort();
    const TActionQueuePtr Queue_ = New<TActionQueue>("FakeController");
    const TIntrusivePtr<TFakeControllerService> Service_ = New<TFakeControllerService>(Queue_->GetInvoker());
    const IServerPtr Server_ = NRpc::NBus::CreateBusServer(
        NYT::NBus::NTcp::CreateBusServer(NYT::NBus::NTcp::TBusServerConfig::CreateTcp(Port_)));

    const TIntrusivePtr<StrictMock<TMockClient>> Client_ = New<StrictMock<TMockClient>>();
    const TClientOptions ClientOptions_ = TClientOptions::FromUserAndToken("alice", "secret");
    const TDirectControllerCommandsConfigPtr Config_ = MakeDirectConfig();

    void SetUp() override
    {
        Server_->RegisterService(Service_);
        Server_->Start();

        EXPECT_CALL(*Client_, GetOptions())
            .WillRepeatedly(ReturnRef(ClientOptions_));
    }

    void TearDown() override
    {
        Server_->Stop()
            .BlockingGet()
            .ThrowOnError();
    }

    std::string GetControllerAddress() const
    {
        return Format("localhost:%v", static_cast<ui16>(Port_));
    }

    //! Makes the flow_control lookup return |leaderInfo| under the leader key, or a missing row.
    void ExpectLeaderInfo(const std::optional<std::string>& leaderInfo)
    {
        auto rowBuffer = New<TRowBuffer>();
        std::vector<TUnversionedRow> rows;
        if (leaderInfo) {
            auto row = rowBuffer->AllocateUnversioned(1);
            row[0] = rowBuffer->CaptureValue(MakeUnversionedAnyValue(*leaderInfo, /*id*/ 0));
            rows.push_back(row);
        } else {
            rows.push_back(TUnversionedRow());
        }
        auto schema = New<TTableSchema>(std::vector{TColumnSchema("value", EValueType::Any)});
        auto rowset = CreateRowset(std::move(schema), MakeSharedRange(std::move(rows), std::move(rowBuffer)));

        EXPECT_CALL(*Client_, LookupRows(TYPath(Format("%v/flow_control", PipelinePath)), _, _, _))
            .WillRepeatedly(Return(MakeFuture(TUnversionedLookupRowsResult{.Rowset = rowset})));
    }

    void ExpectPublishedLeader(const std::string& address)
    {
        ExpectLeaderInfo(Format("{rpc_address=%Qv}", address));
    }

    void ExpectPublishedLeader(const std::string& address, const TSelfSignedCertificate& certificate)
    {
        ExpectLeaderInfo(Format("{rpc_address=%Qv; certificate_pem=%Qv; certificate_sha256=%Qv}",
            address,
            certificate.CertificatePem,
            certificate.CertificateSha256));
    }

    TFlowExecuteTarget DirectTarget() const
    {
        return TFlowExecuteTarget(Client_, Config_);
    }

    TYsonString Execute(
        const std::string& command,
        const TYsonString& argument = TYsonString(TStringBuf("{}")),
        const TFlowExecuteOptions& options = {})
    {
        return FlowExecute(DirectTarget(), TYPath(PipelinePath), command, argument, options);
    }

    //! Runs a command against a controller that replies after |ReplyDelay| and expects the
    //! request to time out before the reply.
    void ExpectTimeout(const TFlowExecuteOptions& options)
    {
        Service_->SetReplyDelay(ReplyDelay);
        ExpectPublishedLeader(GetControllerAddress());

        try {
            Execute("get-pipeline-state", TYsonString(TStringBuf("{}")), options);
            GTEST_FAIL() << "The request was expected to time out";
        } catch (const TErrorException& ex) {
            EXPECT_TRUE(ex.Error().FindMatching(NYT::EErrorCode::Timeout)) << ToString(ex.Error());
        }
    }

    static constexpr auto ReplyDelay = TDuration::Seconds(2);
    static constexpr auto ShortTimeout = TDuration::MilliSeconds(100);
};

TEST_F(TDirectFlowExecuteTest, SendsCommandWithClientCredentials)
{
    ExpectPublishedLeader(GetControllerAddress());

    auto argument = TYsonString(TStringBuf("{target_pipeline_state=paused}"));
    auto result = Execute("set-target-pipeline-state", argument);
    EXPECT_EQ(ConvertTo<IMapNodePtr>(result)->GetChildValueOrThrow<std::string>("command"), "set-target-pipeline-state");

    auto recorded = Service_->GetLastRequest();
    EXPECT_EQ(recorded.Command, "set-target-pipeline-state");
    EXPECT_EQ(recorded.Argument, argument.ToString());
    EXPECT_EQ(recorded.User, "alice");
    EXPECT_EQ(recorded.Token, "secret");
    EXPECT_TRUE(recorded.Direct);
}

TEST_F(TDirectFlowExecuteTest, TypedCommandGoesToController)
{
    ExpectPublishedLeader(GetControllerAddress());

    auto state = FlowExecute(DirectTarget(), TYPath(PipelinePath), TGetPipelineStateArg());
    EXPECT_EQ(state.PipelineState, EPipelineState::Working);
    EXPECT_EQ(Service_->GetLastRequest().Command, "get-pipeline-state");
}

TEST_F(TDirectFlowExecuteTest, AppliesConfiguredRpcTimeout)
{
    Config_->RpcTimeout = ShortTimeout;

    ExpectTimeout({});
}

TEST_F(TDirectFlowExecuteTest, ExplicitTimeoutOverridesConfiguredOne)
{
    // The configured timeout outlasts the reply delay: only the explicit one can fire.
    Config_->RpcTimeout = 10 * ReplyDelay;

    TFlowExecuteOptions options;
    options.Timeout = ShortTimeout;
    ExpectTimeout(options);
}

TEST_F(TDirectFlowExecuteTest, RejectsCommandsOutsideTheRunnerSet)
{
    ExpectPublishedLeader(GetControllerAddress());

    EXPECT_THROW_WITH_SUBSTRING(
        Execute("get-flow-view"),
        "Command \"get-flow-view\" cannot be sent to the pipeline controller directly");
}

TEST_F(TDirectFlowExecuteTest, RequiresPublishedLeader)
{
    ExpectLeaderInfo(std::nullopt);

    EXPECT_THROW_WITH_SUBSTRING(Execute("get-pipeline-state"), "Cannot discover pipeline controller");
}

TEST_F(TDirectFlowExecuteTest, RequiresLeaderAddress)
{
    // The leader row is there, but the controller published no RPC address.
    ExpectPublishedLeader("");

    EXPECT_THROW_WITH_SUBSTRING(Execute("get-pipeline-state"), "Cannot discover pipeline controller");
}

TEST_F(TDirectFlowExecuteTest, ReportsUnreachableLeader)
{
    // A port nobody listens on.
    auto unusedPort = NTesting::GetFreePort();
    ExpectPublishedLeader(Format("localhost:%v", static_cast<ui16>(unusedPort)));

    EXPECT_THROW_WITH_SUBSTRING(Execute("get-pipeline-state"), "Cannot connect to pipeline controller leader directly");
}

////////////////////////////////////////////////////////////////////////////////

NCrypto::TPemBlobConfigPtr MakePemBlob(std::string value)
{
    auto blob = New<NCrypto::TPemBlobConfig>();
    blob->Value = std::move(value);
    return blob;
}

//! The controller serves TLS only, with its incarnation certificate: a plain TCP client cannot reach it.
class TDirectFlowExecuteTlsTest
    : public TDirectFlowExecuteTest
{
protected:
    const NTesting::TPortHolder TlsPort_ = NTesting::GetFreePort();
    const TSelfSignedCertificate Certificate_ = GenerateSelfSignedCertificate({
        .CommonName = "yt-flow-controller-test",
        .IPAddresses = {"127.0.0.1", "::1"},
    });
    IServerPtr TlsServer_;

    void SetUp() override
    {
        TDirectFlowExecuteTest::SetUp();

        StartTlsServer(Certificate_);
    }

    void TearDown() override
    {
        StopTlsServer();

        TDirectFlowExecuteTest::TearDown();
    }

    void StartTlsServer(const TSelfSignedCertificate& certificate, IServicePtr service = nullptr)
    {
        auto config = NYT::NBus::NTcp::TBusServerConfig::CreateTcp(TlsPort_);
        config->EncryptionMode = NYT::NBus::EEncryptionMode::Required;
        config->CertificateChain = MakePemBlob(certificate.CertificatePem);
        config->PrivateKey = MakePemBlob(certificate.PrivateKeyPem);
        TlsServer_ = NRpc::NBus::CreateBusServer(NYT::NBus::NTcp::CreateBusServer(config));
        TlsServer_->RegisterService(service ? std::move(service) : Service_);
        TlsServer_->Start();
    }

    void StopTlsServer()
    {
        WaitFor(TlsServer_->Stop())
            .ThrowOnError();
    }

    std::string GetTlsControllerAddress() const
    {
        return Format("localhost:%v", static_cast<ui16>(TlsPort_));
    }
};

TEST_F(TDirectFlowExecuteTlsTest, PinsPublishedCertificate)
{
    ExpectPublishedLeader(GetTlsControllerAddress(), Certificate_);

    auto state = FlowExecute(DirectTarget(), TYPath(PipelinePath), TGetPipelineStateArg());
    EXPECT_EQ(state.PipelineState, EPipelineState::Working);

    auto recorded = Service_->GetLastRequest();
    EXPECT_EQ(recorded.User, "alice");
    EXPECT_EQ(recorded.Token, "secret");
}

TEST_F(TDirectFlowExecuteTlsTest, FollowsCertificateRotationOnTheSameTarget)
{
    // The runner keeps one target through the release while the controller restarts on the same address.
    auto target = DirectTarget();

    ExpectPublishedLeader(GetTlsControllerAddress(), Certificate_);
    EXPECT_EQ(FlowExecute(target, TYPath(PipelinePath), TGetPipelineStateArg()).PipelineState, EPipelineState::Working);

    StopTlsServer();
    auto rotated = GenerateSelfSignedCertificate({.CommonName = "yt-flow-controller-rotated"});
    // The stopped server has stopped its services as well: the new incarnation brings its own.
    auto rotatedService = New<TFakeControllerService>(Queue_->GetInvoker());
    StartTlsServer(rotated, rotatedService);
    ExpectPublishedLeader(GetTlsControllerAddress(), rotated);

    // A channel pinned to the previous certificate would fail the handshake with the new incarnation.
    EXPECT_EQ(FlowExecute(target, TYPath(PipelinePath), TGetPipelineStateArg()).PipelineState, EPipelineState::Working);
    EXPECT_EQ(rotatedService->GetLastRequest().Command, "get-pipeline-state");
}

TEST_F(TDirectFlowExecuteTlsTest, RejectsControllerWithOtherCertificate)
{
    // Another incarnation's certificate is published: the controller behind the address cannot prove it.
    auto other = GenerateSelfSignedCertificate({.CommonName = "yt-flow-controller-other"});
    ExpectPublishedLeader(GetTlsControllerAddress(), other);

    try {
        Execute("get-pipeline-state");
        GTEST_FAIL() << "The request was expected to fail";
    } catch (const TErrorException& ex) {
        EXPECT_THAT(ex.what(), ::testing::HasSubstr("Cannot connect to pipeline controller leader directly"));
        EXPECT_TRUE(ex.Error().FindMatching(NYT::NBus::EErrorCode::SslError)) << ToString(ex.Error());
        EXPECT_EQ(
            ex.Error().Attributes().Get<std::string>("pipeline_controller_leader_certificate_sha256"),
            other.CertificateSha256);
    }
    EXPECT_TRUE(Service_->GetLastRequest().Command.empty());
}

TEST_F(TDirectFlowExecuteTlsTest, NoVerificationWithoutPublishedCertificate)
{
    // A leader of an older binary publishes no certificate; the runner keeps the default bus config
    // and connects to whatever serves the address.
    ExpectPublishedLeader(GetTlsControllerAddress());

    auto state = FlowExecute(DirectTarget(), TYPath(PipelinePath), TGetPipelineStateArg());
    EXPECT_EQ(state.PipelineState, EPipelineState::Working);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
