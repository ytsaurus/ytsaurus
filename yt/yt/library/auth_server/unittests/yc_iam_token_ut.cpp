#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <yt/yt/core/http/config.h>
#include <yt/yt/core/http/connection_pool.h>
#include <yt/yt/core/http/helpers.h>
#include <yt/yt/core/http/http.h>
#include <yt/yt/core/http/server.h>
#include <yt/yt/core/http/stream.h>

#include <yt/yt/core/https/config.h>
#include <yt/yt/core/https/server.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/auth_server/config.h>
#include <yt/yt/library/auth_server/cypress_user_manager.h>
#include <yt/yt/library/auth_server/yc_iam_token_authenticator.h>

#include <yt/yt/library/tvm/service/mock/mock_tvm_service.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/json_reader.h>

#include <library/cpp/http/server/http.h>

#include <library/cpp/testing/common/network.h>

namespace NYT::NAuth {
namespace {

using namespace NConcurrency;
using namespace NHttp;
using namespace NJson;
using namespace NNet;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TYCIAMHandler
    : public IHttpHandler
{
public:
    TYCIAMHandler()
    { }

    void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    {
        auto body = TString(req->ReadAll().ToStringBuf());
        TStringStream in;
        in << body;
        TJsonValue value;
        ReadJsonTree(&in, &value);
        auto token = value["iam-token"].GetString();

        if (token == GoodToken_) {
            rsp->SetStatus(EStatusCode::OK);
            TStringStream out;
            TJsonWriter json(&out, false);
            json.OpenMap();
            json.Write("subject", Login_);
            json.CloseMap();

            json.Flush();

            WaitFor(rsp->Write(TSharedRef::FromString(out.Str()))).ThrowOnError();
        } else if (token == BadToken_) {
            rsp->SetStatus(EStatusCode::Forbidden);

            TStringStream out;
            TJsonWriter json(&out, false);
            json.OpenMap();
            json.Write("error", "permission denied");
            json.CloseMap();

            json.Flush();

            WaitFor(rsp->Write(TSharedRef::FromString(out.Str()))).ThrowOnError();

        } else if (token == IssueToken_) {
            rsp->SetStatus(EStatusCode::InternalServerError);

            TStringStream out;
            TJsonWriter json(&out, false);
            json.OpenMap();
            json.Write("error", "some server error");
            json.CloseMap();

            json.Flush();

            WaitFor(rsp->Write(TSharedRef::FromString(out.Str()))).ThrowOnError();
        }

        WaitFor(rsp->Close()).ThrowOnError();
    }

private:
    std::string GoodToken_ = "good_token";
    std::string BadToken_ = "bad_token";
    std::string IssueToken_ = "issue_token";
    std::string Login_ = "user";
};

class TYCIAMServerTest
    : public ::testing::Test
{
protected:
    IPollerPtr Poller;
    IServerPtr Server;

    ::NTesting::TPortHolder TestPort;
    TString TestUrl;

private:
    void SetupServer(const TServerConfigPtr& config)
    {
        config->Port = TestPort;
    }

    void SetUp() override
    {
        TestPort = ::NTesting::GetFreePort();
        TestUrl = Format("http://localhost:%v", TestPort);
        Poller = CreateThreadPoolPoller(1, "HttpTest");

        auto serverConfig = New<TServerConfig>();

        SetupServer(serverConfig);
        Server = CreateServer(serverConfig, Poller);

        auto path = NYT::Format("/authenticate");

        Server->AddHandler(path, New<TYCIAMHandler>());

        Server->Start();
    }

    void TearDown() override
    {
        Server->Stop();
        Server.Reset();
        Poller->Shutdown();
        Poller.Reset();
        TestPort.Reset();
    }
};

////////////////////////////////////////////////////////////////////////////////

TYCIAMTokenAuthenticatorConfigPtr CreateYCIAMTokenAuthenticatorConfig(int port)
{
    auto config = New<TYCIAMTokenAuthenticatorConfig>();
    config->Host = "localhost";
    config->Port = port;
    config->Secure = false;
    config->RetryAllServerErrors = true;

    return config;
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TYCIAMServerTest, GoodToken)
{
    auto config = CreateYCIAMTokenAuthenticatorConfig(TestPort);
    auto poller = CreateThreadPoolPoller(1, "HttpPoller");
    auto authenticator = CreateYCIAMTokenAuthenticator(config, poller, CreateNullCypressUserManager());
    TAuthenticationResult result = WaitFor(authenticator->Authenticate(TTokenCredentials{
        "good_token",
        TNetworkAddress::Parse("127.0.0.1")
    })).ValueOrThrow();
    EXPECT_EQ(result.Login, "user");
}

TEST_F(TYCIAMServerTest, BadToken)
{
    auto config = CreateYCIAMTokenAuthenticatorConfig(TestPort);
    auto poller = CreateThreadPoolPoller(1, "HttpPoller");
    auto authenticator = CreateYCIAMTokenAuthenticator(config, poller, CreateNullCypressUserManager());
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        WaitFor(authenticator->Authenticate(TTokenCredentials{
            "bad_token",
            TNetworkAddress::Parse("127.0.0.1")
        })).ValueOrThrow(),
        std::exception,
        "Access is prohibited for this user"
    );
}

TEST_F(TYCIAMServerTest, IssueToken)
{
    auto config = CreateYCIAMTokenAuthenticatorConfig(TestPort);
    auto poller = CreateThreadPoolPoller(1, "HttpPoller");
    auto authenticator = CreateYCIAMTokenAuthenticator(config, poller, CreateNullCypressUserManager());
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        WaitFor(authenticator->Authenticate(TTokenCredentials{
            "issue_token",
            TNetworkAddress::Parse("127.0.0.1")
        })).ValueOrThrow(),
        std::exception,
        "YC IAM token authentication service response has non-ok status code"
    );
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

} // namespace NYT::NAuth
