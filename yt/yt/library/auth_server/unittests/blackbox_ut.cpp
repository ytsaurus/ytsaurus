#include "mock_http_server.h"

#include <yt/yt/library/auth_server/blackbox_cookie_authenticator.h>
#include <yt/yt/library/auth_server/blackbox_service.h>
#include <yt/yt/library/auth_server/cookie_authenticator.h>
#include <yt/yt/library/auth_server/config.h>
#include <yt/yt/library/auth_server/credentials.h>
#include <yt/yt/library/auth_server/helpers.h>
#include <yt/yt/library/auth_server/ticket_authenticator.h>
#include <yt/yt/library/auth_server/token_authenticator.h>

#include <yt/yt/library/tvm/service/mock/mock_tvm_service.h>

#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <yt/yt/core/http/helpers.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/re2/re2.h>

namespace NYT::NAuth {
namespace {

using namespace NConcurrency;
using namespace NTests;
using namespace NYTree;
using namespace NYson;

using ::testing::AllOf;
using ::testing::AnyOf;
using ::testing::HasSubstr;
using ::testing::NiceMock;
using ::testing::Return;
using ::testing::Throw;
using ::testing::_;

////////////////////////////////////////////////////////////////////////////////

class TBlackboxTest
    : public ::testing::Test
{
protected:
    TBlackboxServiceConfigPtr CreateBlackboxServiceConfig()
    {
        auto config = New<TBlackboxServiceConfig>();
        config->Host = MockHttpServer_.IsStarted() ? MockHttpServer_.GetHost() : "localhost";
        config->Port = MockHttpServer_.IsStarted() ? MockHttpServer_.GetPort() : static_cast<ui16>(0);
        config->Secure = false;
        config->RequestTimeout = TDuration::Seconds(10);
        config->AttemptTimeout = TDuration::Seconds(10);
        config->BackoffTimeout = TDuration::Seconds(10);
        return config;
    }

    IBlackboxServicePtr CreateBlackboxService(
        TBlackboxServiceConfigPtr config = {},
        bool mockTvmService = true)
    {
        if (mockTvmService) {
            MockTvmService_ = New<NiceMock<TMockTvmService>>();
            ON_CALL(*MockTvmService_, GetServiceTicket("blackbox"))
                .WillByDefault(Return(TString("blackbox_ticket")));
        }

        return NAuth::CreateBlackboxService(
            config ? config : CreateBlackboxServiceConfig(),
            MockTvmService_,
            CreateThreadPoolPoller(1, "HttpPoller"));
    }

    void SetUp() override
    {
        MockHttpServer_.Start();
    }

    void TearDown() override
    {
        if (MockHttpServer_.IsStarted()) {
            MockHttpServer_.Stop();
        }
    }

    void SetCallback(TMockHttpServer::TCallback callback)
    {
        MockHttpServer_.SetCallback(std::move(callback));
    }

    TMockHttpServer MockHttpServer_;
    TIntrusivePtr<TMockTvmService> MockTvmService_;
};

TEST_F(TBlackboxTest, FailOnBadHost)
{
    auto config = CreateBlackboxServiceConfig();
    config->Host = "lokalhozd";
    config->Port = 1;
    auto service = CreateBlackboxService(config);
    auto result = service->Call("hello", {}).Get();
    ASSERT_TRUE(!result.IsOK());
    EXPECT_THAT(CollectMessages(result), AnyOf(HasSubstr("Domain name"), HasSubstr("DNS")));
}

TEST_F(TBlackboxTest, FailOn5xxResponse)
{
    SetCallback([&] (TClientRequest* request) {
        EXPECT_THAT(request->Input().FirstLine(), HasSubstr("/blackbox?method=hello"));
        request->Output() << HttpResponse(500, "");
    });
    auto service = CreateBlackboxService();
    auto result = service->Call("hello", {}).Get();
    ASSERT_TRUE(!result.IsOK());
    Cerr << ToString(result) << Endl;
    EXPECT_THAT(CollectMessages(result), HasSubstr("Blackbox call returned HTTP status code 500"));
}

TEST_F(TBlackboxTest, FailOn4xxResponse)
{
    SetCallback([&] (TClientRequest* request) {
        EXPECT_THAT(request->Input().FirstLine(), HasSubstr("/blackbox?method=hello"));
        request->Output() << HttpResponse(404, "");
    });
    auto service = CreateBlackboxService();
    auto result = service->Call("hello", {}).Get();
    ASSERT_TRUE(!result.IsOK());
    EXPECT_THAT(CollectMessages(result), HasSubstr("Blackbox call returned HTTP status code 404"));
}

TEST_F(TBlackboxTest, FailOnEmptyResponse)
{
    SetCallback([&] (TClientRequest* request) {
        EXPECT_THAT(request->Input().FirstLine(), HasSubstr("/blackbox?method=hello"));
        request->Output() << HttpResponse(200, "");
    });
    auto service = CreateBlackboxService();
    auto result = service->Call("hello", {}).Get();
    ASSERT_TRUE(!result.IsOK());
    EXPECT_THAT(CollectMessages(result), HasSubstr("Error parsing JSON"));
}

TEST_F(TBlackboxTest, FailOnMalformedResponse)
{
    SetCallback([&] (TClientRequest* request) {
        EXPECT_THAT(request->Input().FirstLine(), HasSubstr("/blackbox?method=hello"));
        request->Output() << HttpResponse(200, "#$&(^$#@(^");
    });
    auto service = CreateBlackboxService();
    auto result = service->Call("hello", {}).Get();
    ASSERT_TRUE(!result.IsOK());
    EXPECT_THAT(CollectMessages(result), HasSubstr("Error parsing JSON"));
}

TEST_F(TBlackboxTest, FailOnBlackboxException)
{
    SetCallback([&] (TClientRequest* request) {
        EXPECT_THAT(request->Input().FirstLine(), HasSubstr("/blackbox?method=hello"));
        request->Output() << HttpResponse(200, R"jj({"exception":{"id": 666, "value": "bad stuff happened"}})jj");
    });
    auto service = CreateBlackboxService();
    auto result = service->Call("hello", {}).Get();
    ASSERT_TRUE(!result.IsOK());
    EXPECT_THAT(CollectMessages(result), HasSubstr("Blackbox has raised an exception"));
}

TEST_F(TBlackboxTest, FailOnTvmException)
{
    auto service = CreateBlackboxService();
    EXPECT_CALL(*MockTvmService_, GetServiceTicket("blackbox"))
        .WillOnce(Throw(std::exception()));
    auto result = service->Call("hello", {}).Get();
    EXPECT_FALSE(result.IsOK());
}

TEST_F(TBlackboxTest, Success)
{
    SetCallback([&] (TClientRequest* request) {
        EXPECT_THAT(request->Input().FirstLine(), HasSubstr("/blackbox?method=hello&foo=bar&spam=ham"));
        auto header = request->Input().Headers().FindHeader(NHttp::NHeaders::ServiceTicketHeaderName);
        EXPECT_NE(nullptr, header);
        EXPECT_EQ("blackbox_ticket", header->Value());
        request->Output() << HttpResponse(200, R"jj({"status": "ok"})jj");
    });
    auto service = CreateBlackboxService();
    auto result = service->Call("hello", {{"foo", "bar"}, {"spam", "ham"}}).Get();
    ASSERT_TRUE(result.IsOK());
    EXPECT_TRUE(AreNodesEqual(result.ValueOrThrow(), ConvertTo<INodePtr>(TYsonString(TStringBuf("{status=ok}")))));
}

TEST_F(TBlackboxTest, NoTvmService)
{
    SetCallback([&] (TClientRequest* request) {
        EXPECT_THAT(request->Input().FirstLine(), HasSubstr("/blackbox"));
        auto header = request->Input().Headers().FindHeader(NHttp::NHeaders::ServiceTicketHeaderName);
        EXPECT_EQ(nullptr, header);
        request->Output() << HttpResponse(200, R"jj({"status": "ok"})jj");
    });
    auto service = CreateBlackboxService(/*config*/ nullptr, /*useTvmService*/ false);
    auto result = service->Call("hello", {}).Get();
    ASSERT_TRUE(result.IsOK());
    EXPECT_TRUE(AreNodesEqual(result.ValueOrThrow(), ConvertTo<INodePtr>(TYsonString(TStringBuf("{status=ok}")))));
}

TEST_F(TBlackboxTest, RetriesErrors)
{
    std::atomic<int> counter = {0};
    SetCallback([&] (TClientRequest* request) {
        switch (counter) {
            case 0:  request->Output() << HttpResponse(500, ""); break;
            case 1:  request->Output() << HttpResponse(404, ""); break;
            case 2:  request->Output() << HttpResponse(200, ""); break;
            case 3:  request->Output() << HttpResponse(200, "#$&(^$#@(^"); break;
            case 4:  request->Output() << HttpResponse(200, R"jj({"exception":{"id": 9, "value": "DB_FETCHFAILED"}})jj"); break;
            case 5:  request->Output() << HttpResponse(200, R"jj({"exception":{"id": 10, "value": "DB_EXCEPTION"}})jj"); break;
            default: request->Output() << HttpResponse(200, R"jj({"exception":{"id": 0, "value": "OK"}})jj"); break;
        }
        ++counter;
    });

    auto config = CreateBlackboxServiceConfig();
    config->BackoffTimeout = TDuration::MilliSeconds(0);
    config->AttemptTimeout = TDuration::Seconds(30);
    config->RequestTimeout = TDuration::Seconds(30);
    auto service = CreateBlackboxService(config);
    auto result = service->Call("hello", {}).Get();
    ASSERT_TRUE(result.IsOK());
    EXPECT_EQ(7, counter.load());
}

////////////////////////////////////////////////////////////////////////////////

class TMockBlackboxService
    : public IBlackboxService
{
public:
    TMockBlackboxService(bool useLowercaseLogin = false)
        : UseLowercaseLogin_(useLowercaseLogin)
    { }

    MOCK_METHOD(TFuture<INodePtr>, Call, (
        const TString&,
        (const THashMap<TString, TString>&)), (override));

    TErrorOr<TString> GetLogin(const NYTree::INodePtr& reply) const override
    {
        if (UseLowercaseLogin_) {
            return GetByYPath<TString>(reply, "/attributes/1008");
        } else {
            return GetByYPath<TString>(reply, "/login");
        }
    }

private:
    bool UseLowercaseLogin_ = false;
};

////////////////////////////////////////////////////////////////////////////////

class TTokenAuthenticatorTest
    : public ::testing::Test
{
protected:
    TTokenAuthenticatorTest()
        : Config_(New<TBlackboxTokenAuthenticatorConfig>())
        , Blackbox_(New<TMockBlackboxService>())
        , Authenticator_(CreateBlackboxTokenAuthenticator(Config_, Blackbox_))
    { }

    void MockCall(const TString& yson)
    {
        EXPECT_CALL(*Blackbox_, Call("oauth", _))
            .WillOnce(Return(MakeFuture<INodePtr>(ConvertTo<INodePtr>(TYsonString(yson)))));
    }

    TFuture<TAuthenticationResult> Invoke(const std::string& token, const TString& userIP)
    {
        return Authenticator_->Authenticate(TTokenCredentials{
            token,
            NNet::TNetworkAddress::Parse(userIP)
        });
    }

    TBlackboxTokenAuthenticatorConfigPtr Config_;
    TIntrusivePtr<TMockBlackboxService> Blackbox_;
    TIntrusivePtr<ITokenAuthenticator> Authenticator_;
};

TEST_F(TTokenAuthenticatorTest, FailOnUnderlyingFailure)
{
    EXPECT_CALL(*Blackbox_, Call("oauth", _))
        .WillOnce(Return(MakeFuture<INodePtr>(TError("Underlying failure"))));
    auto result = Invoke("mytoken", "127.0.0.1").Get();
    ASSERT_TRUE(!result.IsOK());
    EXPECT_THAT(CollectMessages(result), HasSubstr("Underlying failure"));
}

TEST_F(TTokenAuthenticatorTest, FailOnInvalidResponse1)
{
    MockCall("{}");
    auto result = Invoke("mytoken", "127.0.0.1").Get();
    ASSERT_TRUE(!result.IsOK());
    EXPECT_THAT(CollectMessages(result), HasSubstr("invalid response"));
}

TEST_F(TTokenAuthenticatorTest, FailOnInvalidResponse2)
{
    MockCall("{status={id=0}}");
    auto result = Invoke("mytoken", "127.0.0.1").Get();
    ASSERT_TRUE(!result.IsOK());
    EXPECT_THAT(CollectMessages(result), AllOf(
        HasSubstr("invalid response"),
        HasSubstr("/login"),
        HasSubstr("/oauth/client_id"),
        HasSubstr("/oauth/scope")));
}

TEST_F(TTokenAuthenticatorTest, FailOnRejection)
{
    MockCall("{status={id=5}}");
    auto result = Invoke("mytoken", "127.0.0.1").Get();
    ASSERT_TRUE(!result.IsOK());
    EXPECT_THAT(CollectMessages(result), HasSubstr("rejected token"));
}

TEST_F(TTokenAuthenticatorTest, FailOnInvalidScope)
{
    Config_->Scope = "yt:api";
    MockCall(R"yy(
        {
            status={id=0};
            oauth={scope="i-am-hacker";client_id="i-am-hacker";client_name="yes-i-am"};
            login=hacker;
            user_ticket=good_ticket_maybe
        })yy");
    auto result = Invoke("mytoken", "127.0.0.1").Get();
    ASSERT_TRUE(!result.IsOK());
    EXPECT_THAT(CollectMessages(result), HasSubstr("does not provide a valid scope"));
}

TEST_F(TTokenAuthenticatorTest, Success)
{
    Config_->Scope = "yt:api";
    MockCall(R"yy(
        {
            status={id=0};
            oauth={scope="x:1 yt:api x:2";client_id="cid";client_name="nm"};
            login=sandello;
            user_ticket=good_ticket
        })yy");
    auto result = Invoke("mytoken", "127.0.0.1").Get();
    ASSERT_TRUE(result.IsOK());
    EXPECT_EQ("sandello", result.Value().Login);
    EXPECT_EQ("blackbox:token:cid:nm", result.Value().Realm);
    EXPECT_EQ("good_ticket", result.Value().UserTicket);
}

TEST_F(TTokenAuthenticatorTest, SuccessWithoutTicket)
{
    Config_->Scope = "yt:api";
    Config_->GetUserTicket = false;
    MockCall(R"yy(
        {
            status={id=0};
            oauth={scope="x:1 yt:api x:2";client_id="cid";client_name="nm"};
            login=sandello})yy");
    auto result = Invoke("mytoken", "127.0.0.1").Get();
    ASSERT_TRUE(result.IsOK());
    EXPECT_EQ("sandello", result.Value().Login);
    EXPECT_EQ("blackbox:token:cid:nm", result.Value().Realm);
    EXPECT_EQ("", result.Value().UserTicket);
}

////////////////////////////////////////////////////////////////////////////////

class TCachingTokenAuthenticatorTest
    : public TTokenAuthenticatorTest
{
public:
    TCachingTokenAuthenticatorTest()
    {
        Config_->Scope = "yt:api";

        auto config = New<TCachingTokenAuthenticatorConfig>();
        config->Cache->CacheTtl = TDuration::Seconds(1);
        config->Cache->ErrorTtl = TDuration::Seconds(1);
        config->Cache->OptimisticCacheTtl = TDuration::Seconds(5);

        Authenticator_ = CreateCachingTokenAuthenticator(
            config,
            Authenticator_);

        GoodResult = MakeFuture<INodePtr>(ConvertTo<INodePtr>(TYsonString(TStringBuf(R"yy(
            {
                status={id=0};
                oauth={scope="x:1 yt:api x:2";client_id="cid";client_name="nm"};
                login=sandello;
                user_ticket=good_ticket
            })yy"))));
        RejectResult = MakeFuture<INodePtr>(ConvertTo<INodePtr>(TYsonString(TStringBuf(R"yy(
            {
                status={id=5};
                oauth={scope="x:1 yt:api x:2";client_id="cid";client_name="nm"};
                login=sandello;
                user_ticket=good_ticket
            })yy"))));
        ErrorResult = MakeFuture<INodePtr>(TError("Internal Server Error"));
    }

    TFuture<INodePtr> GoodResult, RejectResult, ErrorResult;
};

TEST_F(TCachingTokenAuthenticatorTest, GoodCaching)
{

    EXPECT_CALL(*Blackbox_, Call("oauth", _))
        .WillRepeatedly(Return(GoodResult));

    auto result = Invoke("mytoken", "127.0.0.1").Get();
    result.ValueOrThrow();
}

TEST_F(TCachingTokenAuthenticatorTest, OptimisticCaching)
{
    {
        testing::InSequence s;
        EXPECT_CALL(*Blackbox_, Call("oauth", _))
            .WillOnce(Return(GoodResult));
        EXPECT_CALL(*Blackbox_, Call("oauth", _))
            .WillRepeatedly(Return(ErrorResult));
    }

    auto result = Invoke("mytoken", "127.0.0.1").Get();
    result.ValueOrThrow();

    Sleep(TDuration::Seconds(1));

    result = Invoke("mytoken", "127.0.0.1").Get();
    result.ValueOrThrow();

    Sleep(TDuration::Seconds(1));

    result = Invoke("mytoken", "127.0.0.1").Get();
    result.ValueOrThrow();
}

TEST_F(TCachingTokenAuthenticatorTest, ErrorCaching)
{
    {
        testing::InSequence s;
        EXPECT_CALL(*Blackbox_, Call("oauth", _))
            .WillOnce(Return(ErrorResult));
        EXPECT_CALL(*Blackbox_, Call("oauth", _))
            .WillRepeatedly(Return(GoodResult));
    }

    auto result = Invoke("mytoken", "127.0.0.1").Get();
    ASSERT_THROW(result.ValueOrThrow(), TErrorException);

    Sleep(TDuration::Seconds(2));
    result = Invoke("mytoken", "127.0.0.1").Get();

    Sleep(TDuration::Seconds(1));
    result = Invoke("mytoken", "127.0.0.1").Get();
    result.ValueOrThrow();
}

TEST_F(TCachingTokenAuthenticatorTest, TokenInvalidation)
{
    {
        testing::InSequence s;
        EXPECT_CALL(*Blackbox_, Call("oauth", _))
            .WillOnce(Return(GoodResult));
        EXPECT_CALL(*Blackbox_, Call("oauth", _))
            .WillRepeatedly(Return(RejectResult));
    }

    auto result = Invoke("mytoken", "127.0.0.1").Get();
    result.ValueOrThrow();

    Sleep(TDuration::Seconds(2));

    result = Invoke("mytoken", "127.0.0.1").Get();

    Sleep(TDuration::Seconds(1));

    result = Invoke("mytoken", "127.0.0.1").Get();
    ASSERT_FALSE(result.IsOK());
}

TEST_F(TCachingTokenAuthenticatorTest, EntryCleanup)
{
    {
        testing::InSequence s;
        EXPECT_CALL(*Blackbox_, Call("oauth", _))
            .WillOnce(Return(GoodResult));
        EXPECT_CALL(*Blackbox_, Call("oauth", _))
            .WillRepeatedly(Return(ErrorResult));
    }

    auto result = Invoke("mytoken", "127.0.0.1").Get();
    result.ValueOrThrow();

    Sleep(TDuration::Seconds(15));

    result = Invoke("mytoken", "127.0.0.1").Get();
    ASSERT_FALSE(result.IsOK());
}

////////////////////////////////////////////////////////////////////////////////

class TCookieAuthenticatorTest
    : public ::testing::Test
{
protected:
    TCookieAuthenticatorTest()
        : Config_(CreateBlackboxCookieAuthenticatorConfig())
        , Blackbox_(New<TMockBlackboxService>())
        , Authenticator_(CreateBlackboxCookieAuthenticator(Config_, Blackbox_))
    { }

    void MockCall(const TString& yson)
    {
        EXPECT_CALL(*Blackbox_, Call("sessionid", _))
            .WillOnce(Return(MakeFuture<INodePtr>(ConvertTo<INodePtr>(TYsonString(yson)))));
    }

    TFuture<TAuthenticationResult> Authenticate(
        const TString& sessionId,
        const TString& sslSessionId,
        const TString& userIP)
    {
        TCookieCredentials credentials;
        credentials.Cookies[BlackboxSessionIdCookieName] = sessionId;
        credentials.Cookies[BlackboxSslSessionIdCookieName] = sslSessionId;
        credentials.UserIP = NNet::TNetworkAddress::Parse(userIP);
        return Authenticator_->Authenticate(credentials);
    }

protected:
    TBlackboxCookieAuthenticatorConfigPtr Config_;
    TIntrusivePtr<TMockBlackboxService> Blackbox_;
    TIntrusivePtr<ICookieAuthenticator> Authenticator_;

    static TBlackboxCookieAuthenticatorConfigPtr CreateBlackboxCookieAuthenticatorConfig()
    {
        auto config = New<TBlackboxCookieAuthenticatorConfig>();
        config->Domain = "myhost";
        return config;
    }
};

TEST_F(TCookieAuthenticatorTest, FailOnUnderlyingFailure)
{
    EXPECT_CALL(*Blackbox_, Call("sessionid", _))
        .WillOnce(Return(MakeFuture<INodePtr>(TError("Underlying failure"))));
    auto result = Authenticate("mysessionid", "mysslsessionid", "127.0.0.1").Get();
    ASSERT_TRUE(!result.IsOK());
    EXPECT_THAT(CollectMessages(result), HasSubstr("Underlying failure"));
}

TEST_F(TCookieAuthenticatorTest, FailOnInvalidResponse1)
{
    MockCall("{}");
    auto result = Authenticate("mysessionid", "mysslsessionid", "127.0.0.1").Get();
    ASSERT_TRUE(!result.IsOK());
    EXPECT_THAT(CollectMessages(result), HasSubstr("invalid response"));
}

TEST_F(TCookieAuthenticatorTest, FailOnInvalidResponse2)
{
    MockCall("{status={id=0}}");
    auto result = Authenticate("mysessionid", "mysslsessionid", "127.0.0.1").Get();
    ASSERT_TRUE(!result.IsOK());
    EXPECT_THAT(CollectMessages(result), AllOf(
        HasSubstr("invalid response"),
        HasSubstr("/login")));
}

TEST_F(TCookieAuthenticatorTest, FailOnRejection)
{
    MockCall("{status={id=5}}");
    auto result = Authenticate("mysessionid", "mysslsessionid", "127.0.0.1").Get();
    ASSERT_TRUE(!result.IsOK());
    EXPECT_THAT(CollectMessages(result), HasSubstr("rejected session cookie"));
}

TEST_F(TCookieAuthenticatorTest, Success)
{
    MockCall("{status={id=0};login=sandello;user_ticket=good_ticket}");
    auto result = Authenticate("mysessionid", "mysslsessionid", "127.0.0.1").Get();
    ASSERT_TRUE(result.IsOK());
    EXPECT_EQ("sandello", result.Value().Login);
    EXPECT_EQ("blackbox:cookie", result.Value().Realm);
    EXPECT_EQ("good_ticket", result.Value().UserTicket);
}

TEST_F(TCookieAuthenticatorTest, SuccessWithoutTicket)
{
    Config_->GetUserTicket = false;
    MockCall("{status={id=0};login=sandello}");
    auto result = Authenticate("mysessionid", "mysslsessionid", "127.0.0.1").Get();
    ASSERT_TRUE(result.IsOK());
    EXPECT_EQ("sandello", result.Value().Login);
    EXPECT_EQ("blackbox:cookie", result.Value().Realm);
    EXPECT_EQ("", result.Value().UserTicket);
}

TEST_F(TCookieAuthenticatorTest, SessguardGoodOrigin)
{
    Config_->EnableSessguard = true;
    Config_->SessguardOriginPatterns = {
        New<NRe2::TRe2>("good.ytsaurus"),
        New<NRe2::TRe2>(".*.good.ytsaurus"),
    };
    auto authenticateSessguard = [&] (const std::optional<std::string>& origin) {
        TCookieCredentials credentials;
        credentials.Cookies[BlackboxSessionIdCookieName] = "mysessionid";
        credentials.Cookies[BlackboxSslSessionIdCookieName] = "mysslsessionid";
        credentials.Cookies[BlackboxSessguardCookieName] = "sessguard";
        credentials.UserIP = NNet::TNetworkAddress::Parse("127.0.0.1");
        credentials.Origin = origin;
        return Authenticator_->Authenticate(credentials).Get();
    };

    MockCall("{status={id=0};login=sandello;user_ticket=good_ticket}");
    EXPECT_NO_THROW(authenticateSessguard("good.ytsaurus").ThrowOnError());

    MockCall("{status={id=0};login=sandello;user_ticket=good_ticket}");
    EXPECT_NO_THROW(authenticateSessguard("http://good.ytsaurus").ThrowOnError());

    MockCall("{status={id=0};login=sandello;user_ticket=good_ticket}");
    EXPECT_NO_THROW(authenticateSessguard("https://good.ytsaurus").ThrowOnError());

    EXPECT_THROW_THAT(
        authenticateSessguard("https://bad.ytsaurus").ThrowOnError(),
        ::testing::HasSubstr("Sessguard cookie from disallowed origin:"));

    EXPECT_THROW_THAT(
        authenticateSessguard({}).ThrowOnError(),
        ::testing::HasSubstr("Sessguard cookie is provided but origin header is empty"));
}

////////////////////////////////////////////////////////////////////////////////

class TTicketAuthenticatorTest
    : public ::testing::TestWithParam<bool>
{
protected:
    void SetUp() override
    {
        Config_ = New<TBlackboxTicketAuthenticatorConfig>();
        Blackbox_ = New<TMockBlackboxService>(GetParam());
        Tvm_ = New<TMockTvmService>();
        Authenticator_ = CreateBlackboxTicketAuthenticator(Config_, Blackbox_, Tvm_);

        Config_->EnableScopeCheck = true;
        Config_->Scopes.insert("foo");
        Config_->Scopes.insert("bar");

        ON_CALL(*Tvm_, ParseUserTicket("good_ticket"))
            .WillByDefault(Return(TParsedTicket{42, {"bar", "baz"}}));
        ON_CALL(*Blackbox_, Call("user_ticket", TicketParam("good_ticket")))
            .WillByDefault(Return(Response("{users=[{login=TheUser;attributes={\"1008\"=the_user}}]}")));

        ON_CALL(*Tvm_, ParseUserTicket("bad_ticket"))
            .WillByDefault(Return(TParsedTicket{43, {"bad", "scope"}}));
        ON_CALL(*Blackbox_, Call("user_ticket", TicketParam("bad_ticket")))
            .WillByDefault(Return(Response("{users=[{login=ScopelessUser;attributes={\"1008\"=scopeless_user}}]}")));

    }

    THashMap<TString, TString> TicketParam(const TString& ticket)
    {
        return THashMap<TString, TString>{{"user_ticket", ticket}};
    }

    TFuture<INodePtr> Response(const TString& yson)
    {
        return MakeFuture<INodePtr>(ConvertTo<INodePtr>(TYsonString(yson)));
    }

    TFuture<TAuthenticationResult> Invoke(const TString& ticket)
    {
        return Authenticator_->Authenticate(TTicketCredentials{ticket});
    }

    TBlackboxTicketAuthenticatorConfigPtr Config_;
    TIntrusivePtr<TMockBlackboxService> Blackbox_;
    TIntrusivePtr<TMockTvmService> Tvm_;
    TIntrusivePtr<ITicketAuthenticator> Authenticator_;
};

TEST_P(TTicketAuthenticatorTest, Success)
{
    auto result = Invoke("good_ticket").Get();
    ASSERT_TRUE(result.IsOK());
    if (GetParam()) {
        EXPECT_EQ("the_user", result.Value().Login);
    } else {
        EXPECT_EQ("TheUser", result.Value().Login);
    }
    EXPECT_EQ("blackbox:user-ticket", result.Value().Realm);
    EXPECT_EQ("good_ticket", result.Value().UserTicket);
}

TEST_P(TTicketAuthenticatorTest, ScopeFailure)
{
    auto result = Invoke("bad_ticket").Get();
    ASSERT_FALSE(result.IsOK());
}

TEST_P(TTicketAuthenticatorTest, DisableScopeCheck)
{
    Config_->EnableScopeCheck = false;
    EXPECT_CALL(*Tvm_, ParseUserTicket(_)).Times(0);
    auto result = Invoke("bad_ticket").Get();
    ASSERT_TRUE(result.IsOK());
    if (GetParam()) {
        EXPECT_EQ("scopeless_user", result.Value().Login);
    } else {
        EXPECT_EQ("ScopelessUser", result.Value().Login);
    }
}

TEST_P(TTicketAuthenticatorTest, FailOnTvmFailure)
{
    EXPECT_CALL(*Tvm_, ParseUserTicket(_))
        .WillOnce(Throw(std::exception()));
    auto result = Invoke("good_ticket").Get();
    ASSERT_FALSE(result.IsOK());
}

TEST_P(TTicketAuthenticatorTest, FailOnBlackboxFailure)
{
    EXPECT_CALL(*Blackbox_, Call("user_ticket", _))
        .WillOnce(Return(MakeFuture<INodePtr>(TError("Blackbox failure"))));
    auto result = Invoke("good_ticket").Get();
    ASSERT_TRUE(!result.IsOK());
    EXPECT_THAT(CollectMessages(result), HasSubstr("Blackbox failure"));
}

TEST_P(TTicketAuthenticatorTest, FailOnBlackboxError)
{
    EXPECT_CALL(*Blackbox_, Call("user_ticket", _))
        .WillOnce(Return(Response("{error=unhappy}")));
    auto result = Invoke("good_ticket").Get();
    ASSERT_TRUE(!result.IsOK());
    EXPECT_THAT(CollectMessages(result), HasSubstr("unhappy"));
}

INSTANTIATE_TEST_SUITE_P(UseLowercaseLogin, TTicketAuthenticatorTest, ::testing::Values(true));
INSTANTIATE_TEST_SUITE_P(UseLogin, TTicketAuthenticatorTest, ::testing::Values(false));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NAuth
