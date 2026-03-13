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
#include <yt/yt/library/auth_server/credentials.h>
#include <yt/yt/library/auth_server/cypress_user_manager.h>
#include <yt/yt/library/auth_server/yc_authenticator.h>

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

class TYCHandler
    : public IHttpHandler
{
public:
    void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    {
        auto body = TString(req->ReadAll().ToStringBuf());
        TStringStream in;
        in << body;
        TJsonValue value;
        ReadJsonTree(&in, &value);
        auto token = value["iam-token"].GetString();
        auto cookie = value["yc-session-cookie"].GetString();

        if (token == GoodToken_ || cookie == GoodCookie_ ) {
            rsp->SetStatus(EStatusCode::OK);
            ReplyJson(rsp, [&] (IYsonConsumer* json) {
                BuildYsonFluently(json)
                    .DoMap([&] (auto map) {
                        map.Item("subject").Value(Login_);
                    });
            });
        } else if (token == BadToken_ || cookie == BadCookie_) {
            rsp->SetStatus(EStatusCode::Forbidden);
            ReplyJson(rsp, [] (IYsonConsumer* consumer) {
                BuildYsonFluently(consumer)
                    .DoMap([] (auto map) {
                        map.Item("error").Value("permission denied");
                    });
            });
        } else if (token == IssueToken_ || cookie == IssueCookie_) {
            rsp->SetStatus(EStatusCode::InternalServerError);
            ReplyJson(rsp, [] (IYsonConsumer* consumer) {
                BuildYsonFluently(consumer)
                    .DoMap([] (auto map) {
                        map.Item("error").Value("some server error");
                    });
            });
        } else if (token == CreateUserToken_ || cookie == CreateUserCookie_) {
            rsp->SetStatus(EStatusCode::OK);
            ReplyJson(rsp, [&] (IYsonConsumer* json) {
                BuildYsonFluently(json)
                    .DoMap([&] (auto map) {
                        map.Item("subject").Value(Login_);
                    });
            });
        } else if (token == AddUserInGroupToken_ || cookie == AddUserInGroupCookie_) {
            rsp->SetStatus(EStatusCode::OK);
            ReplyJson(rsp, [&] (IYsonConsumer* json) {
                BuildYsonFluently(json)
                    .DoMap([&] (auto map) {
                        map.Item("subject").Value(Login_);
                        map.Item("groups")
                            .DoListFor(Groups_, [] (auto list, const std::string& group) {
                                list.Item().Value(group);
                            });
                    });
            });
        }

        WaitFor(rsp->Close()).ThrowOnError();
    }

private:
    std::string GoodToken_ = "good_token";
    std::string BadToken_ = "bad_token";
    std::string IssueToken_ = "issue_token";
    std::string GoodCookie_ = "good_cookie";
    std::string BadCookie_ = "bad_cookie";
    std::string IssueCookie_ = "issue_cookie";
    std::string CreateUserToken_ = "create_user_token";
    std::string CreateUserCookie_ = "create_user_cookie";
    std::string AddUserInGroupToken_ = "add_user_token";
    std::string AddUserInGroupCookie_ = "add_user_cookie";
    std::string Login_ = "user";
    std::vector<std::string> Groups_ = {"managers", "random-group"};
};

class TYCServerTest
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

        Server->AddHandler(path, New<TYCHandler>());

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

TYCAuthenticatorConfigPtr CreateYCAuthenticatorConfig(int port)
{
    auto config = New<TYCAuthenticatorConfig>();
    config->Host = "localhost";
    config->Port = port;
    config->Secure = false;
    config->RetryAllServerErrors = true;

    return config;
}

TCookieCredentials NewCookieCredentials(const std::string& cookie)
{
    TCookieCredentials credentials;
    credentials.Cookies[YCSessionCookieName] = cookie;
    credentials.UserIP = TNetworkAddress::Parse("127.0.0.1");
    return credentials;
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TYCServerTest, SuccessToken)
{
    auto config = CreateYCAuthenticatorConfig(TestPort);
    auto poller = CreateThreadPoolPoller(1, "HttpPoller");
    auto authenticator = CreateYCIamTokenAuthenticator(config, poller, CreateNullCypressUserManager());
    auto result = WaitFor(authenticator->Authenticate(TTokenCredentials{
        .Token = "good_token",
        .UserIP = TNetworkAddress::Parse("127.0.0.1"),
    })).ValueOrThrow();
    EXPECT_EQ(result.Login, "user");
}

TEST_F(TYCServerTest, SuccessCookie)
{
    auto config = CreateYCAuthenticatorConfig(TestPort);
    auto poller = CreateThreadPoolPoller(1, "HttpPoller");
    auto authenticator = CreateYCSessionCookieAuthenticator(config, poller, CreateNullCypressUserManager());
    auto result = WaitFor(authenticator->Authenticate(NewCookieCredentials("good_cookie"))).ValueOrThrow();
    EXPECT_EQ(result.Login, "user");
}

TEST_F(TYCServerTest, ForbiddenToken)
{
    auto config = CreateYCAuthenticatorConfig(TestPort);
    auto poller = CreateThreadPoolPoller(1, "HttpPoller");
    auto authenticator = CreateYCIamTokenAuthenticator(config, poller, CreateNullCypressUserManager());
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        WaitFor(authenticator->Authenticate(TTokenCredentials{
            .Token = "bad_token",
            .UserIP = TNetworkAddress::Parse("127.0.0.1"),
        })).ValueOrThrow(),
        std::exception,
        "Access is prohibited for this user");
}

TEST_F(TYCServerTest, NoToken)
{
    auto config = CreateYCAuthenticatorConfig(TestPort);
    auto poller = CreateThreadPoolPoller(1, "HttpPoller");
    auto authenticator = CreateYCIamTokenAuthenticator(config, poller, CreateNullCypressUserManager());
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        WaitFor(authenticator->Authenticate(TTokenCredentials{
            .UserIP = TNetworkAddress::Parse("127.0.0.1"),
        })).ValueOrThrow(),
        std::exception,
        "Token should be provided");

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        WaitFor(authenticator->Authenticate(TTokenCredentials{
            .TokenSha256 = "some_sha256",
            .UserIP = TNetworkAddress::Parse("127.0.0.1"),
        })).ValueOrThrow(),
        std::exception,
        "Token should be provided");
}

TEST_F(TYCServerTest, ForbiddenCookie)
{
    auto config = CreateYCAuthenticatorConfig(TestPort);
    auto poller = CreateThreadPoolPoller(1, "HttpPoller");
    auto authenticator = CreateYCSessionCookieAuthenticator(config, poller, CreateNullCypressUserManager());
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        WaitFor(authenticator->Authenticate(NewCookieCredentials
        ("bad_cookie"))).ValueOrThrow(),
        std::exception,
        "Access is prohibited for this user");
}

TEST_F(TYCServerTest, InternalErrorToken)
{
    auto config = CreateYCAuthenticatorConfig(TestPort);
    auto poller = CreateThreadPoolPoller(1, "HttpPoller");
    auto authenticator = CreateYCIamTokenAuthenticator(config, poller, CreateNullCypressUserManager());
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        WaitFor(authenticator->Authenticate(TTokenCredentials{
            .Token = "issue_token",
            .UserIP = TNetworkAddress::Parse("127.0.0.1"),
        })).ValueOrThrow(),
        std::exception,
        "YC authentication service response has non-ok status code");
}

TEST_F(TYCServerTest, InternalErrorCookie)
{
    auto config = CreateYCAuthenticatorConfig(TestPort);
    auto poller = CreateThreadPoolPoller(1, "HttpPoller");
    auto authenticator = CreateYCSessionCookieAuthenticator(config, poller, CreateNullCypressUserManager());
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        WaitFor(authenticator->Authenticate(NewCookieCredentials("issue_cookie"))).ValueOrThrow(),
        std::exception,
        "YC authentication service response has non-ok status code");
}

TEST_F(TYCServerTest, SuccessCreateUserToken)
{
    auto config = CreateYCAuthenticatorConfig(TestPort);
    auto poller = CreateThreadPoolPoller(1, "HttpPoller");
    auto userManager = CreateInMemoryCypressUserManager();
    auto authenticator = CreateYCIamTokenAuthenticator(config, poller, userManager);
    auto result = WaitFor(authenticator->Authenticate(TTokenCredentials{
        .Token = "create_user_token",
        .UserIP = TNetworkAddress::Parse("127.0.0.1"),
    })).ValueOrThrow();
    EXPECT_EQ(result.Login, "user");
    EXPECT_TRUE(userManager->CheckUserExists("user"));
    EXPECT_EQ(userManager->GetUserGroups("user").Get().Value(), std::vector<std::string>());
}

TEST_F(TYCServerTest, SuccessCreateUserCookie)
{
    auto config = CreateYCAuthenticatorConfig(TestPort);
    auto poller = CreateThreadPoolPoller(1, "HttpPoller");
    auto userManager = CreateInMemoryCypressUserManager();
    auto authenticator = CreateYCSessionCookieAuthenticator(config, poller, userManager);
    auto result = WaitFor(authenticator->Authenticate(NewCookieCredentials("create_user_cookie"))).ValueOrThrow();
    EXPECT_EQ(result.Login, "user");
    EXPECT_TRUE(userManager->CheckUserExists("user"));
    EXPECT_EQ(userManager->GetUserGroups("user").Get().Value(), std::vector<std::string>());
}

TEST_F(TYCServerTest, SuccessAddUserInGroupsToken)
{
    auto config = CreateYCAuthenticatorConfig(TestPort);
    auto poller = CreateThreadPoolPoller(1, "HttpPoller");
    auto userManager = CreateInMemoryCypressUserManager();
    auto authenticator = CreateYCIamTokenAuthenticator(config, poller, userManager);
    auto result = WaitFor(authenticator->Authenticate(TTokenCredentials{
        .Token = "add_user_token",
        .UserIP = TNetworkAddress::Parse("127.0.0.1"),
    })).ValueOrThrow();
    EXPECT_EQ(result.Login, "user");
    EXPECT_TRUE(userManager->CheckUserExists("user"));
    EXPECT_EQ(userManager->GetUserGroups("user").Get().Value(), std::vector<std::string>({"managers", "random-group"}));
}

TEST_F(TYCServerTest, SuccessAddUserInGroupsCookie)
{
    auto config = CreateYCAuthenticatorConfig(TestPort);
    auto poller = CreateThreadPoolPoller(1, "HttpPoller");
    auto userManager = CreateInMemoryCypressUserManager();
    auto authenticator = CreateYCSessionCookieAuthenticator(config, poller, userManager);
    auto result = WaitFor(authenticator->Authenticate(NewCookieCredentials("add_user_cookie"))).ValueOrThrow();
    EXPECT_EQ(result.Login, "user");
    EXPECT_TRUE(userManager->CheckUserExists("user"));
    EXPECT_EQ(userManager->GetUserGroups("user").Get().Value(), std::vector<std::string>({"managers", "random-group"}));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

} // namespace NYT::NAuth
