#include <yt/core/test_framework/framework.h>

#include "test_key.h"

#include <yt/core/http/server.h>
#include <yt/core/http/client.h>
#include <yt/core/http/private.h>
#include <yt/core/http/http.h>
#include <yt/core/http/stream.h>
#include <yt/core/http/config.h>
#include <yt/core/http/helpers.h>

#include <yt/core/https/server.h>
#include <yt/core/https/client.h>
#include <yt/core/https/config.h>

#include <yt/core/net/connection.h>
#include <yt/core/net/listener.h>
#include <yt/core/net/dialer.h>
#include <yt/core/net/config.h>

#include <yt/core/concurrency/poller.h>
#include <yt/core/concurrency/thread_pool_poller.h>
#include <yt/core/concurrency/async_stream.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/rpc/grpc/dispatcher.h>

#include <yt/core/crypto/tls.h>

#include <yt/core/misc/error.h>
#include <yt/core/https/config.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace {

using namespace NYT::NConcurrency;
using namespace NYT::NNet;
using namespace NYT::NHttp;
using namespace NYT::NCrypto;
using namespace NYT::NLogging;

////////////////////////////////////////////////////////////////////////////////

TEST(THttpUrlParse, Simple)
{
    TString example = "https://user@google.com:12345/a/b/c?foo=bar&zog=%20";
    auto url = ParseUrl(example);

    ASSERT_EQ(url.Protocol, AsStringBuf("https"));
    ASSERT_EQ(url.Host, AsStringBuf("google.com"));
    ASSERT_EQ(url.User, AsStringBuf("user"));
    ASSERT_EQ(url.PortStr, AsStringBuf("12345"));
    ASSERT_TRUE(url.Port);
    ASSERT_EQ(*url.Port, 12345);
    ASSERT_EQ(url.Path, AsStringBuf("/a/b/c"));
    ASSERT_EQ(url.RawQuery, AsStringBuf("foo=bar&zog=%20"));

    ASSERT_THROW(ParseUrl(AsStringBuf("\0")), TErrorException);
}

TEST(THttpUrlParse, IPv4)
{
    TString example = "https://1.2.3.4:12345/";
    auto url = ParseUrl(example);

    ASSERT_EQ(url.Host, AsStringBuf("1.2.3.4"));
    ASSERT_EQ(*url.Port, 12345);
}

TEST(THttpUrlParse, IPv6)
{
    TString example = "https://[::1]:12345/";
    auto url = ParseUrl(example);

    ASSERT_EQ(url.Host, AsStringBuf("::1"));
    ASSERT_EQ(*url.Port, 12345);
}

TEST(THttpCookie, ParseCookie)
{
    TString cookieString = "yandexuid=706216621492423338; yandex_login=prime; _ym_d=1529669659; Cookie_check=1; _ym_isad=1";
    auto cookie = ParseCookies(cookieString);

    ASSERT_EQ("706216621492423338", cookie["yandexuid"]);
    ASSERT_EQ("prime", cookie["yandex_login"]);
    ASSERT_EQ("1529669659", cookie["_ym_d"]);
    ASSERT_EQ("1", cookie["_ym_isad"]);
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TString> ToVector(const SmallVector<TString, 1>& v)
{
    return std::vector<TString>(v.begin(), v.end());
}

TEST(THttpHeaders, Simple)
{
    auto headers = New<THeaders>();

    headers->Set("X-Test", "F");

    ASSERT_EQ(std::vector<TString>{{"F"}}, ToVector(headers->GetAll("X-Test")));
    ASSERT_EQ(TString{"F"}, headers->GetOrThrow("X-Test"));
    ASSERT_EQ(TString{"F"}, *headers->Find("X-Test"));

    ASSERT_THROW(headers->GetAll("X-Test2"), TErrorException);
    ASSERT_THROW(headers->GetOrThrow("X-Test2"), TErrorException);
    ASSERT_FALSE(headers->Find("X-Test2"));

    headers->Add("X-Test", "H");
    std::vector<TString> expected = {"F", "H"};
    ASSERT_EQ(expected, ToVector(headers->GetAll("X-Test")));

    headers->Set("X-Test", "J");
    ASSERT_EQ(std::vector<TString>{{"J"}}, ToVector(headers->GetAll("X-Test")));
}

TEST(THttpHeaders, HeaderCaseIsIrrelevant)
{
    auto headers = New<THeaders>();

    headers->Set("x-tEsT", "F");
    ASSERT_EQ(TString("F"), headers->GetOrThrow("x-test"));
    ASSERT_EQ(TString("F"), headers->GetOrThrow("X-Test"));

    TString buffer;
    TStringOutput output(buffer);
    headers->WriteTo(&output);

    TString expected = "x-tEsT: F\r\n";
    ASSERT_EQ(expected, buffer);
}


TEST(THttpHeaders, MessedUpHeaderValuesAreNotAllowed)
{
    auto headers = New<THeaders>();

    EXPECT_THROW(headers->Set("X-Newlines", "aaa\r\nbbb\nccc"), TErrorException);
    EXPECT_THROW(headers->Add("X-Newlines", "aaa\r\nbbb\nccc"), TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

struct TFakeConnection
    : public IConnection
{
    TString Input;
    TString Output;

    virtual bool SetNoDelay() override
    {
        return true;
    }

    virtual bool SetKeepAlive() override
    {
        return true;
    }

    virtual TFuture<size_t> Read(const TSharedMutableRef& ref) override
    {
        size_t toCopy = std::min(ref.Size(), Input.size());
        std::copy_n(Input.data(), toCopy, ref.Begin());
        Input = Input.substr(toCopy);
        return MakeFuture(toCopy);
    }
    
    virtual TFuture<void> Write(const TSharedRef& ref) override
    {
        Output += TString(ref.Begin(), ref.Size());
        return VoidFuture;
    }

    virtual TFuture<void> WriteV(const TSharedRefArray& refs) override
    {
        for (const auto& ref : refs) {
            Output += TString(ref.Begin(), ref.Size());
        }
        return VoidFuture;
    }

    virtual TFuture<void> Close() override
    {
        THROW_ERROR_EXCEPTION("Not implemented");
    }

    virtual bool IsIdle() const override
    {
        return true;
    }

    virtual TFuture<void> Abort() override
    {
        THROW_ERROR_EXCEPTION("Not implemented");
    }

    virtual TFuture<void> CloseRead() override
    {
        THROW_ERROR_EXCEPTION("Not implemented");
    }

    virtual TFuture<void> CloseWrite() override
    {
        THROW_ERROR_EXCEPTION("Not implemented");
    }

    virtual const TNetworkAddress& LocalAddress() const override
    {
        THROW_ERROR_EXCEPTION("Not implemented");
    }
    
    virtual const TNetworkAddress& RemoteAddress() const override
    {
        THROW_ERROR_EXCEPTION("Not implemented");
    }

    virtual int GetHandle() const override
    {
        THROW_ERROR_EXCEPTION("Not implemented");
    }

    virtual TConnectionStatistics GetReadStatistics() const override
    {
        return {};
    }

    virtual TConnectionStatistics GetWriteStatistics() const override
    {
        return {};
    }

    virtual i64 GetReadByteCount() const override
    {
        return 0;
    }

    virtual i64 GetWriteByteCount() const override
    {
        return 0;
    }

    virtual void SetReadDeadline(TInstant deadline) override
    { }

    virtual void SetWriteDeadline(TInstant deadline) override
    { }
};

DEFINE_REFCOUNTED_TYPE(TFakeConnection)

void FinishBody(THttpOutput* out)
{
    WaitFor(out->Close()).ThrowOnError();    
}

void WriteChunk(THttpOutput* out, TStringBuf chunk)
{
    WaitFor(out->Write(TSharedRef::FromString(TString(chunk)))).ThrowOnError();
}

void WriteBody(THttpOutput* out, TStringBuf body)
{
    WaitFor(out->WriteBody(TSharedRef::FromString(TString(body)))).ThrowOnError();
}

TEST(THttpOutputTest, Full)
{
    typedef std::tuple<EMessageType, TString, std::function<void(THttpOutput*)>> TTestCase;
    std::vector<TTestCase> table = {
        TTestCase{
            EMessageType::Request,
            "GET / HTTP/1.1\r\n"
            "\r\n",
            [] (THttpOutput* out) {
                out->WriteRequest(EMethod::Get, "/");
                FinishBody(out);
            }
        },
        TTestCase{
            EMessageType::Request,
            "POST / HTTP/1.1\r\n"
            "Content-Length: 0\r\n"
            "\r\n",
            [] (THttpOutput* out) {
                out->WriteRequest(EMethod::Post, "/");
                FinishBody(out);
            }
        },
        TTestCase{
            EMessageType::Request,
            "POST / HTTP/1.1\r\n"
            "Content-Length: 1\r\n"
            "\r\n"
            "x",
            [] (THttpOutput* out) {
                out->WriteRequest(EMethod::Post, "/");
                WriteBody(out, AsStringBuf("x"));
            }
        },
        TTestCase{
            EMessageType::Request,
            "POST / HTTP/1.1\r\n"
            "Transfer-Encoding: chunked\r\n"
            "\r\n"
            "1\r\n"
            "X\r\n"
            "A\r\n" // hex(10)
            "0123456789\r\n"
            "0\r\n"
            "\r\n",
            [] (THttpOutput* out) {
                out->WriteRequest(EMethod::Post, "/");

                WriteChunk(out, AsStringBuf("X"));
                WriteChunk(out, AsStringBuf("0123456789"));
                FinishBody(out);
            }
        },
        TTestCase{
            EMessageType::Response,
            "HTTP/1.1 200 OK\r\n"
            "Content-Length: 0\r\n"
            "\r\n",
            [] (THttpOutput* out) {
                out->SetStatus(EStatusCode::OK);
                FinishBody(out);
            }
        },
        TTestCase{
            EMessageType::Response,
            "HTTP/1.1 400 Bad Request\r\n"
            "Content-Length: 0\r\n"
            "X-YT-Response-Code: 500\r\n"
            "\r\n",
            [] (THttpOutput* out) {
                out->SetStatus(EStatusCode::BadRequest);
                out->GetTrailers()->Add("X-YT-Response-Code", "500");
                FinishBody(out);
            }
        },
        TTestCase{
            EMessageType::Response,
            "HTTP/1.1 500 Internal Server Error\r\n"
            "Content-Length: 4\r\n"
            "\r\n"
            "fail",
            [] (THttpOutput* out) {
                out->SetStatus(EStatusCode::InternalServerError);
                WriteBody(out, AsStringBuf("fail"));
            }
        },
        TTestCase{
            EMessageType::Response,
            "HTTP/1.1 200 OK\r\n"
            "Transfer-Encoding: chunked\r\n"
            "\r\n"
            "1\r\n"
            "X\r\n"
            "A\r\n" // hex(10)
            "0123456789\r\n"
            "0\r\n"
            "\r\n",
            [] (THttpOutput* out) {
                out->SetStatus(EStatusCode::OK);

                WriteChunk(out, AsStringBuf("X"));
                WriteChunk(out, AsStringBuf("0123456789"));
                FinishBody(out);
            }
        },
    };

    for (auto testCase : table) {
        auto fake = New<TFakeConnection>();
        auto config = New<THttpIOConfig>();
        auto output = New<THttpOutput>(fake, std::get<0>(testCase), config);

        try {
            std::get<2>(testCase)(output.Get());
        } catch (const std::exception& ex) {
            ADD_FAILURE() << "Failed to write output"
                << std::get<1>(testCase)
                << ex.what();
        }
        ASSERT_EQ(fake->Output, std::get<1>(testCase));
    }
}

////////////////////////////////////////////////////////////////////////////////

void ExpectBodyPart(THttpInput* in, TStringBuf chunk)
{
    ASSERT_EQ(chunk, ToString(WaitFor(in->Read()).ValueOrThrow()));
}

void ExpectBodyEnd(THttpInput* in)
{
    ASSERT_EQ(0, WaitFor(in->Read()).ValueOrThrow().Size());
}

TEST(THttpInputTest, Simple)
{
    typedef std::tuple<EMessageType, TString, std::function<void(THttpInput*)>> TTestCase;
    std::vector<TTestCase> table = {
        TTestCase{
            EMessageType::Response,
            "HTTP/1.1 200 OK\r\n"
            "\r\n",
            [] (THttpInput* in) {
                EXPECT_EQ(in->GetStatusCode(), EStatusCode::OK);
                ExpectBodyEnd(in);
            }
        },
        TTestCase{
            EMessageType::Response,
            "HTTP/1.1 500 Internal Server Error\r\n"
            "\r\n",
            [] (THttpInput* in) {
                EXPECT_EQ(in->GetStatusCode(), EStatusCode::InternalServerError);
                ExpectBodyEnd(in);
            }
        },
        TTestCase{
            EMessageType::Request,
            "GET / HTTP/1.1\r\n"
            "\r\n",
            [] (THttpInput* in) {
                EXPECT_EQ(in->GetMethod(), EMethod::Get);
                EXPECT_EQ(in->GetUrl().Path, AsStringBuf("/"));
                ExpectBodyEnd(in);
            }
        },
        TTestCase{
            EMessageType::Request,
            "POST / HTTP/1.1\r\n"
            "Content-Length: 6\r\n"
            "\r\n"
            "foobar",
            [] (THttpInput* in) {
                EXPECT_EQ(in->GetMethod(), EMethod::Post);
                ExpectBodyPart(in, "foobar");
                ExpectBodyEnd(in);
            }
        },
        TTestCase{
            EMessageType::Request,
            "POST /chunked_w_trailing_headers HTTP/1.1\r\n"
            "Transfer-Encoding: chunked\r\n"
            "X-Foo: test\r\n"
            "Connection: close\r\n"
            "\r\n"
            "5\r\nhello\r\n"
            "6\r\n world\r\n"
            "0\r\n"
            "Vary: *\r\n"
            "Content-Type: text/plain\r\n"
            "\r\n",
            [] (THttpInput* in) {
                EXPECT_EQ(in->GetMethod(), EMethod::Post);
                EXPECT_EQ(in->GetUrl().Path, AsStringBuf("/chunked_w_trailing_headers"));

                auto headers = in->GetHeaders();
                ASSERT_EQ(TString("test"), headers->GetOrThrow("X-Foo"));

                ASSERT_THROW(in->GetTrailers(), TErrorException);

                ExpectBodyPart(in, "hello");
                ExpectBodyPart(in, " world");
                ExpectBodyEnd(in);

                auto trailers = in->GetTrailers();
                ASSERT_EQ(TString("*"), trailers->GetOrThrow("Vary"));
                ASSERT_EQ(TString("text/plain"), trailers->GetOrThrow("Content-Type"));
            }
        },
        TTestCase{
            EMessageType::Request,
            "GET http://yt/foo HTTP/1.1\r\n"
            "\r\n",
            [] (THttpInput* in) {
                EXPECT_EQ(AsStringBuf("yt"), in->GetUrl().Host);
            }
        }
    };

    for (auto testCase : table) {
        auto fake = New<TFakeConnection>();
        fake->Input = std::get<1>(testCase);
        auto config = New<THttpIOConfig>();
        config->ReadBufferSize = 1024;

        auto input = New<THttpInput>(fake, TNetworkAddress(), GetSyncInvoker(), std::get<0>(testCase), config);

        try {
            std::get<2>(testCase)(input.Get());
        } catch (const std::exception& ex) {
            ADD_FAILURE() << "Failed to parse input:"
                << std::endl << "==============" << std::endl
                << std::get<1>(testCase)
                << std::endl << "==============" << std::endl
                << ex.what();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

struct THttpServerTest
    : public ::testing::TestWithParam<bool>
{
    IPollerPtr Poller;
    IServerPtr Server;
    IClientPtr Client;

    int TestPort;
    TString TestUrl;

    THttpServerTest()
    {
        Poller = CreateThreadPoolPoller(4, "HttpTest");
        if (!GetParam()) {
            auto serverConfig = New<NHttp::TServerConfig>();
            SetupServer(serverConfig);
            Server = NHttp::CreateServer(serverConfig, Poller);

            auto clientConfig = New<NHttp::TClientConfig>();
            SetupClient(clientConfig);
            Client = NHttp::CreateClient(clientConfig, Poller);
        } else {
            auto serverConfig = New<NHttps::TServerConfig>();
            serverConfig->Credentials = New<NHttps::TServerCredentialsConfig>();
            serverConfig->Credentials->PrivateKey = New<TPemBlobConfig>();
            serverConfig->Credentials->PrivateKey->Value = TestCertificate;
            serverConfig->Credentials->CertChain = New<TPemBlobConfig>();
            serverConfig->Credentials->CertChain->Value = TestCertificate;
            SetupServer(serverConfig);
            Server = NHttps::CreateServer(serverConfig, Poller);

            auto clientConfig = New<NHttps::TClientConfig>();
            clientConfig->Credentials = New<NHttps::TClientCredentialsConfig>();
            clientConfig->Credentials->PrivateKey = New<TPemBlobConfig>();
            clientConfig->Credentials->PrivateKey->Value = TestCertificate;
            clientConfig->Credentials->CertChain = New<TPemBlobConfig>();
            clientConfig->Credentials->CertChain->Value = TestCertificate;
            SetupClient(clientConfig);
            Client = NHttps::CreateClient(clientConfig, Poller);
        }

        TestPort = Server->GetAddress().GetPort();
        TestUrl = Format("http://localhost:%v", TestPort);
    }

    ~THttpServerTest()
    {
        Server->Stop();
        Server.Reset();
        Poller->Shutdown();
    }

private:
    void SetupServer(const NHttp::TServerConfigPtr& config)
    {
        config->Port = 0;
    }

    void SetupClient(const NHttp::TClientConfigPtr& /*config*/)
    { }
};

class TOKHttpHandler
    : public IHttpHandler
{
public:
    virtual void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    {
        rsp->SetStatus(EStatusCode::OK);
        WaitFor(rsp->Close()).ThrowOnError();
    }
};

TEST_P(THttpServerTest, SimpleRequest)
{
    Server->AddHandler("/ok", New<TOKHttpHandler>());
    Server->Start();

    auto rsp = WaitFor(Client->Get(TestUrl + "/ok")).ValueOrThrow();
    ASSERT_EQ(EStatusCode::OK, rsp->GetStatusCode());

    Server->Stop();
    Sleep(TDuration::MilliSeconds(10));
}

class TEchoHttpHandler
    : public IHttpHandler
{
public:
    virtual void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    {
        rsp->SetStatus(EStatusCode::OK);
        while (true) {
            auto data = WaitFor(req->Read()).ValueOrThrow();
            if (data.Size() == 0) {
                break;
            }
            WaitFor(rsp->Write(data)).ThrowOnError();
        }

        WaitFor(rsp->Close()).ThrowOnError();
    }    
};

TString ReadAll(const IAsyncZeroCopyInputStreamPtr& in)
{
    TString buf;
    while (true) {
        auto data = WaitFor(in->Read()).ValueOrThrow();
        if (data.Size() == 0) {
            break;
        }

        buf += ToString(data);
    }

    return buf;
}


TEST_P(THttpServerTest, TransferSmallBody)
{
    Server->AddHandler("/echo", New<TEchoHttpHandler>());
    Server->Start();

    auto reqBody = TSharedMutableRef::Allocate(1024);
    std::fill(reqBody.Begin(), reqBody.End(), 0xab);
    
    auto rsp = WaitFor(Client->Post(TestUrl + "/echo", reqBody)).ValueOrThrow();
    ASSERT_EQ(EStatusCode::OK, rsp->GetStatusCode());

    auto rspBody = ReadAll(rsp);
    ASSERT_EQ(TString(reqBody.Begin(), reqBody.Size()), rspBody);

    Server->Stop();
    Sleep(TDuration::MilliSeconds(10));
}

class TTestStatusCodeHandler
    : public IHttpHandler
{
public:
    virtual void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    {
        rsp->SetStatus(Code);
        WaitFor(rsp->Close()).ThrowOnError();
    }

    EStatusCode Code = EStatusCode::OK;
};

TEST_P(THttpServerTest, StatusCode)
{
    auto handler = New<TTestStatusCodeHandler>();
    Server->AddHandler("/code", handler);
    Server->Start();

    handler->Code = EStatusCode::NotFound;
    ASSERT_EQ(EStatusCode::NotFound,
        WaitFor(Client->Get(TestUrl + "/code"))
            .ValueOrThrow()
            ->GetStatusCode());

    handler->Code = EStatusCode::Forbidden;
    ASSERT_EQ(EStatusCode::Forbidden,
        WaitFor(Client->Get(TestUrl + "/code"))
            .ValueOrThrow()
            ->GetStatusCode());

    Server->Stop();
    Sleep(TDuration::MilliSeconds(10));
}

class TTestHeadersHandler
    : public IHttpHandler
{
public:
    virtual void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    {
        for (const auto& header : ExpectedHeaders) {
            EXPECT_EQ(header.second, req->GetHeaders()->GetOrThrow(header.first));
        }

        for (const auto& header : ReplyHeaders) {
            rsp->GetHeaders()->Add(header.first, header.second);
        }

        rsp->SetStatus(EStatusCode::OK);
        WaitFor(rsp->Close()).ThrowOnError();
    }

    std::vector<std::pair<TString, TString>> ReplyHeaders, ExpectedHeaders;
};

TEST_P(THttpServerTest, HeadersTest)
{
    auto handler = New<TTestHeadersHandler>();
    handler->ExpectedHeaders = {
        { "X-Yt-Test", "foo; bar; zog" },
        { "Accept-Charset", "utf-8" }
    };
    handler->ReplyHeaders = {
        { "Content-Type", "test/plain; charset=utf-8" },
        { "Cache-Control", "nocache" }
    };

    Server->AddHandler("/headers", handler);
    Server->Start();

    auto headers = New<THeaders>();
    headers->Add("X-Yt-Test", "foo; bar; zog");
    headers->Add("Accept-Charset", "utf-8");

    auto rsp = WaitFor(Client->Get(TestUrl + "/headers", headers)).ValueOrThrow();
    EXPECT_EQ("nocache", rsp->GetHeaders()->GetOrThrow("Cache-Control"));
    EXPECT_EQ("test/plain; charset=utf-8", rsp->GetHeaders()->GetOrThrow("Content-Type"));

    Server->Stop();
    Sleep(TDuration::MilliSeconds(10));
}

class TTestTrailersHandler
    : public IHttpHandler
{
public:
    virtual void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    {
        WaitFor(rsp->Write(TSharedRef::FromString("test"))).ThrowOnError();

        rsp->GetTrailers()->Set("X-Yt-Test", "foo; bar");
        WaitFor(rsp->Close()).ThrowOnError();
    }
};

TEST_P(THttpServerTest, TrailersTest)
{
    auto handler = New<TTestTrailersHandler>();

    Server->AddHandler("/trailers", handler);
    Server->Start();

    auto rsp = WaitFor(Client->Get(TestUrl + "/trailers")).ValueOrThrow();
    auto body = ReadAll(rsp);
    EXPECT_EQ("foo; bar", rsp->GetTrailers()->GetOrThrow("X-Yt-Test"));

    Server->Stop();
    Sleep(TDuration::MilliSeconds(10));
}

class THangingHandler
    : public IHttpHandler
{
public:
    virtual void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    { }
};

class TImpatientHandler
    : public IHttpHandler
{
public:
    virtual void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    {
        WaitFor(rsp->Write(TSharedRef::FromString("body"))).ThrowOnError();
        WaitFor(rsp->Close()).ThrowOnError();
    }
};

class TForgetfulHandler
    : public IHttpHandler
{
public:
    virtual void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    {
        rsp->SetStatus(EStatusCode::OK);
    }
};

TEST_P(THttpServerTest, WierdHandlers)
{
    auto hanging = New<THangingHandler>();
    auto impatient = New<TImpatientHandler>();
    auto forgetful = New<TForgetfulHandler>();

    Server->AddHandler("/hanging", hanging);
    Server->AddHandler("/impatient", impatient);
    Server->AddHandler("/forgetful", forgetful);
    Server->Start();

    EXPECT_THROW(
        WaitFor(Client->Get(TestUrl + "/hanging"))
            .ValueOrThrow()
            ->GetStatusCode(),
        TErrorException);
    EXPECT_EQ(
        WaitFor(Client->Get(TestUrl + "/impatient"))
            .ValueOrThrow()
            ->GetStatusCode(),
        EStatusCode::InternalServerError);
    EXPECT_THROW(
        WaitFor(Client->Get(TestUrl + "/forgetful"))
            .ValueOrThrow()
            ->GetStatusCode(),
        TErrorException);

    Server->Stop();
    Sleep(TDuration::MilliSeconds(10));
}

class TThrowingHandler
    : public IHttpHandler
{
public:
    virtual void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    {
        THROW_ERROR_EXCEPTION("Your request is bad");
    }
};

TEST_P(THttpServerTest, ThrowingHandler)
{
    auto throwing = New<TThrowingHandler>();

    Server->AddHandler("/throwing", throwing);
    Server->Start();

    ASSERT_EQ(EStatusCode::InternalServerError,
        WaitFor(Client->Get(TestUrl + "/throwing"))
            .ValueOrThrow()
            ->GetStatusCode());

    Server->Stop();
    Sleep(TDuration::MilliSeconds(10));
}

class TConsumingHandler
    : public IHttpHandler
{
public:
    virtual void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    {
        while (WaitFor(req->Read()).ValueOrThrow().Size() != 0)
        { }

        rsp->SetStatus(EStatusCode::OK);
        WaitFor(rsp->Close()).ThrowOnError();
    }
};

TEST_P(THttpServerTest, RequestStreaming)
{
    Server->AddHandler("/consuming", New<TConsumingHandler>());
    Server->Start();

    auto body = TSharedMutableRef::Allocate(128 * 1024 * 1024);
    ASSERT_EQ(EStatusCode::OK,
        WaitFor(Client->Post(TestUrl + "/consuming", body))
            .ValueOrThrow()->GetStatusCode());

    Server->Stop();
    Sleep(TDuration::MilliSeconds(10));
}

class TStreamingHandler
    : public IHttpHandler
{
public:
    virtual void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    {
        rsp->SetStatus(EStatusCode::OK);
        auto data = TSharedRef::FromString(TString(1024, 'f'));
        for (int i = 0; i < 16 * 1024; i++) {
            WaitFor(rsp->Write(data));
        }

        WaitFor(rsp->Close());
    }
};

TEST_P(THttpServerTest, ResponseStreaming)
{
    Server->AddHandler("/streaming", New<TStreamingHandler>());
    Server->Start();

    auto rsp = WaitFor(Client->Get(TestUrl + "/streaming")).ValueOrThrow();
    ASSERT_EQ(16 * 1024 * 1024, ReadAll(rsp).Size());

    Server->Stop();
    Sleep(TDuration::MilliSeconds(10));
}

class TValidateErrorHandler
    : public IHttpHandler
{
public:
    virtual void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    {
        ASSERT_THROW(ReadAll(req), TErrorException);
        Ok = true;
    }

    bool Ok = false;
};

TEST_P(THttpServerTest, RequestHangUp)
{
    if (GetParam()) {
        // This test is not TLS-specific.
        return;
    }

    auto validating = New<TValidateErrorHandler>();
    Server->AddHandler("/validating", validating);
    Server->Start();

    auto dialer = CreateDialer(New<TDialerConfig>(), Poller, HttpLogger);
    auto connection = WaitFor(dialer->Dial(TNetworkAddress::CreateIPv6Loopback(TestPort)))
        .ValueOrThrow();
    WaitFor(connection->Write(TSharedRef::FromString("POST /validating HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n")))
        .ThrowOnError();
    WaitFor(connection->CloseWrite())
        .ThrowOnError();
    auto bytesRead = WaitFor(connection->Read(TSharedMutableRef::Allocate(1)))
        .ValueOrThrow();
    ASSERT_EQ(0, bytesRead);

    Server->Stop();
    Sleep(TDuration::MilliSeconds(10));

    EXPECT_TRUE(validating->Ok);
}

TEST_P(THttpServerTest, ConnectionKeepAlive)
{
    if (GetParam()) {
        // This test is not TLS-specific.
        return;
    }

    Server->AddHandler("/echo", New<TEchoHttpHandler>());
    Server->Start();

    auto dialer = CreateDialer(New<TDialerConfig>(), Poller, HttpLogger);

    // Many requests.
    {
        auto connection = WaitFor(dialer->Dial(TNetworkAddress::CreateIPv6Loopback(TestPort)))
            .ValueOrThrow();

        auto request = New<THttpOutput>(
            connection,
            EMessageType::Request,
            New<THttpIOConfig>());

        auto response = New<THttpInput>(
            connection,
            connection->RemoteAddress(),
            Poller->GetInvoker(),
            EMessageType::Response,
            New<THttpIOConfig>());

        for (int i = 0; i < 10; ++i) {
            request->WriteRequest(EMethod::Post, "/echo");
            WaitFor(request->Write(TSharedRef::FromString("foo")))
                .ThrowOnError();
            WaitFor(request->Close())
                .ThrowOnError();

            response->GetStatusCode();
            auto body = response->ReadBody();

            ASSERT_TRUE(response->IsSafeToReuse());
            ASSERT_TRUE(request->IsSafeToReuse());
            response->Reset();
            request->Reset();
        }
    }

    // Pipelining
    {
        auto connection = WaitFor(dialer->Dial(TNetworkAddress::CreateIPv6Loopback(TestPort)))
            .ValueOrThrow();

        auto request = New<THttpOutput>(
            connection,
            EMessageType::Request,
            New<THttpIOConfig>());

        auto response = New<THttpInput>(
            connection,
            connection->RemoteAddress(),
            Poller->GetInvoker(),
            EMessageType::Response,
            New<THttpIOConfig>());

        for (int i = 0; i < 10; ++i) {
            request->WriteRequest(EMethod::Post, "/echo");
            WaitFor(request->Write(TSharedRef::FromString("foo")))
                .ThrowOnError();
            WaitFor(request->Close())
                .ThrowOnError();

            ASSERT_TRUE(request->IsSafeToReuse());
            request->Reset();
        }

        for (int i = 0; i < 10; ++i) {
            response->GetStatusCode();
            auto body = response->ReadBody();

            ASSERT_TRUE(response->IsSafeToReuse());
            response->Reset();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

INSTANTIATE_TEST_CASE_P(WithoutTls, THttpServerTest, ::testing::Values(false));
INSTANTIATE_TEST_CASE_P(WithTls, THttpServerTest, ::testing::Values(true));

////////////////////////////////////////////////////////////////////////////////

TEST(THttpHandlerMatchingTest, Simple)
{
    auto h1 = New<TOKHttpHandler>();
    auto h2 = New<TOKHttpHandler>();
    auto h3 = New<TOKHttpHandler>();

    TRequestPathMatcher handlers;
    handlers.Add("/", h1);
    handlers.Add("/a", h2);
    handlers.Add("/a/b", h3);

    EXPECT_EQ(h1.Get(), handlers.Match(AsStringBuf("/")).Get());
    EXPECT_EQ(h1.Get(), handlers.Match(AsStringBuf("/c")).Get());

    EXPECT_EQ(h2.Get(), handlers.Match(AsStringBuf("/a")).Get());
    EXPECT_EQ(h1.Get(), handlers.Match(AsStringBuf("/a/")).Get());

    EXPECT_EQ(h3.Get(), handlers.Match(AsStringBuf("/a/b")).Get());
    EXPECT_EQ(h1.Get(), handlers.Match(AsStringBuf("/a/b/")).Get());

    TRequestPathMatcher handlers2;
    handlers2.Add("/a/", h2);
    EXPECT_FALSE(handlers2.Match(AsStringBuf("/")).Get());
    EXPECT_EQ(h2.Get(), handlers2.Match(AsStringBuf("/a")).Get());
    EXPECT_EQ(h2.Get(), handlers2.Match(AsStringBuf("/a/")).Get());
    EXPECT_EQ(h2.Get(), handlers2.Match(AsStringBuf("/a/b")).Get());

    TRequestPathMatcher handlers3;
    handlers3.Add("/a/", h2);
    handlers3.Add("/a", h3);
   
    EXPECT_EQ(h3.Get(), handlers3.Match(AsStringBuf("/a")).Get());
    EXPECT_EQ(h2.Get(), handlers3.Match(AsStringBuf("/a/")).Get());
    EXPECT_EQ(h2.Get(), handlers3.Match(AsStringBuf("/a/b")).Get());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
