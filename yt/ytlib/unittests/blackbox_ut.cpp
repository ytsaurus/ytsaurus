#include <yt/core/test_framework/framework.h>

#include <yt/ytlib/auth/token_authenticator.h>
#include <yt/ytlib/auth/cookie_authenticator.h>
#include <yt/ytlib/auth/blackbox_service.h>
#include <yt/ytlib/auth/default_blackbox_service.h>
#include <yt/ytlib/auth/config.h>
#include <yt/ytlib/auth/helpers.h>

#include <library/http/server/http.h>

namespace NYT {

using namespace NAuth;
using namespace NYTree;
using namespace NYson;

using ::testing::_;
using ::testing::HasSubstr;
using ::testing::Return;
using ::testing::AllOf;

////////////////////////////////////////////////////////////////////////////////

TString CollectMessages(const TError& error)
{
    TString result;
    std::function<void(const TError&)> impl = [&] (const TError& e) {
        result += e.GetMessage();
        for (const auto& ie : e.InnerErrors()) {
            result += "\n";
            impl(ie);
        }
    };
    impl(error);
    return result;
}

class TDefaultBlackboxTest
    : public ::testing::Test
{
    class TMockClientRequest
        : public TClientRequest
    {
        virtual bool Reply(void* opaque) override
        {
            TDefaultBlackboxTest* test = static_cast<TDefaultBlackboxTest*>(opaque);
            if (!test || !test->OnCall_) {
                Output() << "HTTP/1.0 501 Not Implemented\r\n\r\n";
            } else {
                test->OnCall_(this);
            }
            return true;
        }
    };

    class TMockCallback
        : public THttpServer::ICallBack
    {
    public:
        explicit TMockCallback(TDefaultBlackboxTest* test)
            : Test_(test)
        { }

        virtual TClientRequest* CreateClient() override
        {
            return new TMockClientRequest();
        }

        virtual void* CreateThreadSpecificResource() override
        {
            return Test_;
        }

        virtual void DestroyThreadSpecificResource(void*) override
        {
        }

    private:
        TDefaultBlackboxTest* const Test_;
    };

protected:
    TDefaultBlackboxServiceConfigPtr CreateDefaultBlackboxServiceConfig()
    {
        auto config = New<TDefaultBlackboxServiceConfig>();
        config->Host = MockServer_ ? MockServer_->Options().Host : "localhost";
        config->Port = MockServer_ ? MockServer_->Options().Port : static_cast<ui16>(0);
        config->Secure = false;
        config->RequestTimeout = TDuration::MilliSeconds(10);
        config->AttemptTimeout = TDuration::MilliSeconds(10);
        config->BackoffTimeout = TDuration::MilliSeconds(10);
        return config;
    }

    IBlackboxServicePtr CreateDefaultSyncDefaultBlackboxService()
    {
        return CreateDefaultBlackboxService(
            CreateDefaultBlackboxServiceConfig(),
            GetSyncInvoker()
        );
    }

    TString HttpResponse(int code, TString body)
    {
        TString result;
        result += "HTTP/1.1 " + ToString(code) + " ";
        switch (code) {
            case 200: result += "Found"; break;
            case 404: result += "Not Found"; break;
            case 500: result += "Internal Server Error"; break;
            default: Y_UNREACHABLE();
        }
        result += "\r\n";
        result += "Connection: close\r\n";
        result += "Content-Length: " + ToString(body.length()) + "\r\n";
        result += "\r\n";
        result += body;
        return result;
    }

    virtual void SetUp() override
    {
        MockCallback_ = std::make_unique<TMockCallback>(this);
        MockServer_ = std::make_unique<THttpServer>(MockCallback_.get(), THttpServerOptions().SetHost("localhost"));
        MockServer_->Start();
    }

    virtual void TearDown() override
    {
        if (MockServer_) {
            MockServer_->Stop();
            MockServer_.reset();
        }
        if (MockCallback_) {
            MockCallback_.reset();
        }
    }

    std::unique_ptr<TMockCallback> MockCallback_;
    std::unique_ptr<THttpServer> MockServer_;
    std::function<void(TClientRequest*)> OnCall_;
};

TEST_F(TDefaultBlackboxTest, FailOnBadHost)
{
    auto config = CreateDefaultBlackboxServiceConfig();
    config->Host = "lokalhozd";
    config->Port = 1;
    auto service = CreateDefaultBlackboxService(config, GetSyncInvoker());
    auto result = service->Call("hello", {}).Get();
    ASSERT_TRUE(!result.IsOK());
    EXPECT_THAT(CollectMessages(result), HasSubstr("Resolve of lokalhozd"));
}

TEST_F(TDefaultBlackboxTest, FailOn5xxResponse)
{
    OnCall_ = [&] (TClientRequest* request) {
        EXPECT_THAT(request->Input().FirstLine(), HasSubstr("/blackbox?method=hello"));
        request->Output() << HttpResponse(500, "");
    };
    auto service = CreateDefaultSyncDefaultBlackboxService();
    auto result = service->Call("hello", {}).Get();
    ASSERT_TRUE(!result.IsOK());
    EXPECT_THAT(CollectMessages(result), HasSubstr("Got 500"));
}

TEST_F(TDefaultBlackboxTest, FailOn4xxResponse)
{
    OnCall_ = [&] (TClientRequest* request) {
        EXPECT_THAT(request->Input().FirstLine(), HasSubstr("/blackbox?method=hello"));
        request->Output() << HttpResponse(404, "");
    };
    auto service = CreateDefaultSyncDefaultBlackboxService();
    auto result = service->Call("hello", {}).Get();
    ASSERT_TRUE(!result.IsOK());
    EXPECT_THAT(CollectMessages(result), HasSubstr("Got 404"));
}

TEST_F(TDefaultBlackboxTest, FailOnEmptyResponse)
{
    OnCall_ = [&] (TClientRequest* request) {
        EXPECT_THAT(request->Input().FirstLine(), HasSubstr("/blackbox?method=hello"));
        request->Output() << HttpResponse(200, "");
    };
    auto service = CreateDefaultSyncDefaultBlackboxService();
    auto result = service->Call("hello", {}).Get();
    ASSERT_TRUE(!result.IsOK());
    EXPECT_THAT(CollectMessages(result), HasSubstr("Error parsing JSON"));
}

TEST_F(TDefaultBlackboxTest, FailOnMalformedResponse)
{
    OnCall_ = [&] (TClientRequest* request) {
        EXPECT_THAT(request->Input().FirstLine(), HasSubstr("/blackbox?method=hello"));
        request->Output() << HttpResponse(200, "#$&(^$#@(^");
    };
    auto service = CreateDefaultSyncDefaultBlackboxService();
    auto result = service->Call("hello", {}).Get();
    ASSERT_TRUE(!result.IsOK());
    EXPECT_THAT(CollectMessages(result), HasSubstr("Error parsing JSON"));
}

TEST_F(TDefaultBlackboxTest, FailOnBlackboxException)
{
    OnCall_ = [&] (TClientRequest* request) {
        EXPECT_THAT(request->Input().FirstLine(), HasSubstr("/blackbox?method=hello"));
        request->Output() << HttpResponse(200, R"jj({"exception":{"id": 666, "value": "bad stuff happened"}})jj");
    };
    auto service = CreateDefaultSyncDefaultBlackboxService();
    auto result = service->Call("hello", {}).Get();
    ASSERT_TRUE(!result.IsOK());
    EXPECT_THAT(CollectMessages(result), HasSubstr("Blackbox has raised an exception"));
}


TEST_F(TDefaultBlackboxTest, Success)
{
    OnCall_ = [&] (TClientRequest* request) {
        EXPECT_THAT(request->Input().FirstLine(), HasSubstr("/blackbox?method=hello&foo=bar&spam=ham"));
        request->Output() << HttpResponse(200, R"jj({"status": "ok"})jj");
    };
    auto service = CreateDefaultSyncDefaultBlackboxService();
    auto result = service->Call("hello", {{"foo", "bar"}, {"spam", "ham"}}).Get();
    ASSERT_TRUE(result.IsOK());
    EXPECT_TRUE(AreNodesEqual(result.ValueOrThrow(), ConvertTo<INodePtr>(TYsonString("{status=ok}"))));
}

TEST_F(TDefaultBlackboxTest, RetriesErrors)
{
    std::atomic<int> counter = {0};
    OnCall_ = [&] (TClientRequest* request) {
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
    };

    auto config = CreateDefaultBlackboxServiceConfig();
    config->BackoffTimeout = TDuration::MilliSeconds(0);
    config->AttemptTimeout = TDuration::Seconds(30);
    config->RequestTimeout = TDuration::Seconds(30);
    auto service = CreateDefaultBlackboxService(config, GetSyncInvoker());
    auto result = service->Call("hello", {}).Get();
    ASSERT_TRUE(result.IsOK());
    EXPECT_EQ(7, counter.load());
}

////////////////////////////////////////////////////////////////////////////////

class TMockBlackboxService
    : public IBlackboxService
{
public:
    MOCK_METHOD3(Call, TFuture<INodePtr>(
        const TString&,
        const THashMap<TString, TString>&,
        const THashMap<TString, TString>&));

    virtual TErrorOr<TString> GetLogin(const NYTree::INodePtr& reply) const override
    {
        return GetByYPath<TString>(reply, "/login");
    }
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
        EXPECT_CALL(*Blackbox_, Call("oauth", _, _))
            .WillOnce(Return(MakeFuture<INodePtr>(ConvertTo<INodePtr>(TYsonString(yson)))));
    }

    TFuture<TAuthenticationResult> Invoke(const TString& token, const TString& userIP)
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
    EXPECT_CALL(*Blackbox_, Call("oauth", _, _))
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
    MockCall(R"yy({status={id=0};oauth={scope="i-am-hacker";client_id="i-am-hacker";client_name="yes-i-am"};login=hacker})yy");
    auto result = Invoke("mytoken", "127.0.0.1").Get();
    ASSERT_TRUE(!result.IsOK());
    EXPECT_THAT(CollectMessages(result), HasSubstr("does not provide a valid scope"));
}

TEST_F(TTokenAuthenticatorTest, Success)
{
    Config_->Scope = "yt:api";
    MockCall(R"yy({status={id=0};oauth={scope="x:1 yt:api x:2";client_id="cid";client_name="nm"};login=sandello})yy");
    auto result = Invoke("mytoken", "127.0.0.1").Get();
    ASSERT_TRUE(result.IsOK());
    EXPECT_EQ("sandello", result.Value().Login);
    EXPECT_EQ("blackbox:token:cid:nm", result.Value().Realm);
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
        EXPECT_CALL(*Blackbox_, Call("sessionid", _, _))
            .WillOnce(Return(MakeFuture<INodePtr>(ConvertTo<INodePtr>(TYsonString(yson)))));
    }

    TFuture<TAuthenticationResult> Authenticate(
        const TString& sessionId,
        const TString& sslSessionId,
        const TString& userIP)
    {
        TCookieCredentials credentials;
        credentials.SessionId = sessionId;
        credentials.SslSessionId = sslSessionId;
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
    EXPECT_CALL(*Blackbox_, Call("sessionid", _, _))
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
    MockCall("{status={id=0};login=sandello}");
    auto result = Authenticate("mysessionid", "mysslsessionid", "127.0.0.1").Get();
    ASSERT_TRUE(result.IsOK());
    EXPECT_EQ("sandello", result.Value().Login);
    EXPECT_EQ("blackbox:cookie", result.Value().Realm);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
