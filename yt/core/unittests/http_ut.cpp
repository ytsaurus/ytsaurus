#include <yt/core/test_framework/framework.h>

#include <yt/core/http/server.h>
#include <yt/core/http/client.h>
#include <yt/core/http/private.h>
#include <yt/core/http/http.h>
#include <yt/core/http/stream.h>
#include <yt/core/http/config.h>

#include <yt/core/net/connection.h>
#include <yt/core/net/listener.h>
#include <yt/core/net/dialer.h>
#include <yt/core/net/config.h>

#include <yt/core/concurrency/poller.h>
#include <yt/core/concurrency/thread_pool_poller.h>
#include <yt/core/concurrency/async_stream.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/error.h>

////////////////////////////////////////////////////////////////////////////////

using namespace NYT;
using namespace NYT::NConcurrency;
using namespace NYT::NNet;
using namespace NYT::NHttp;

////////////////////////////////////////////////////////////////////////////////

TEST(THttpUrlParse, Simple)
{
    TString example = "https://user@google.com:12345/a/b/c?foo=bar&zog=%20";
    auto url = ParseUrl(example);

    ASSERT_EQ(url.Protocol, STRINGBUF("https"));
    ASSERT_EQ(url.Host, STRINGBUF("google.com"));
    ASSERT_EQ(url.User, STRINGBUF("user"));
    ASSERT_EQ(url.PortStr, STRINGBUF("12345"));
    ASSERT_TRUE(url.Port);
    ASSERT_EQ(*url.Port, 12345);
    ASSERT_EQ(url.Path, STRINGBUF("/a/b/c"));
    ASSERT_EQ(url.RawQuery, STRINGBUF("foo=bar&zog=%20"));

    ASSERT_THROW(ParseUrl(STRINGBUF("\0")), TErrorException);
}

TEST(THttpUrlParse, IPv4)
{
    TString example = "https://1.2.3.4:12345/";
    auto url = ParseUrl(example);

    ASSERT_EQ(url.Host, STRINGBUF("1.2.3.4"));
    ASSERT_EQ(*url.Port, 12345);
}

TEST(THttpUrlParse, IPv6)
{
    TString example = "https://[::1]:12345/";
    auto url = ParseUrl(example);

    ASSERT_EQ(url.Host, STRINGBUF("::1"));
    ASSERT_EQ(*url.Port, 12345);
}

////////////////////////////////////////////////////////////////////////////////

TEST(THttpHeaders, Simple)
{
    auto headers = New<THeaders>();

    headers->Set("X-Test", "F");

    ASSERT_EQ(std::vector<TString>{{"F"}}, headers->GetAll("X-Test"));
    ASSERT_EQ(TString{"F"}, headers->Get("X-Test"));
    ASSERT_EQ(TString{"F"}, *headers->Find("X-Test"));

    ASSERT_THROW(headers->GetAll("X-Test2"), TErrorException);
    ASSERT_THROW(headers->Get("X-Test2"), TErrorException);
    ASSERT_EQ(nullptr, headers->Find("X-Test2"));

    headers->Add("X-Test", "H");
    std::vector<TString> expected = {"F", "H"};
    ASSERT_EQ(expected, headers->GetAll("X-Test"));

    headers->Set("X-Test", "J");
    ASSERT_EQ(std::vector<TString>{{"J"}}, headers->GetAll("X-Test"));
}

TEST(THttpHeaders, HeaderCaseIsIrrelevant)
{
    auto headers = New<THeaders>();

    headers->Set("x-tEsT", "F");
    ASSERT_EQ(TString("F"), headers->Get("x-test"));
    ASSERT_EQ(TString("F"), headers->Get("X-Test"));

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
                WriteBody(out, STRINGBUF("x"));
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

                WriteChunk(out, STRINGBUF("X"));
                WriteChunk(out, STRINGBUF("0123456789"));
                FinishBody(out);
            }
        },
        TTestCase{
            EMessageType::Response,
            "HTTP/1.1 200 OK\r\n"
            "Content-Length: 0\r\n"
            "\r\n",
            [] (THttpOutput* out) {
                out->SetStatus(EStatusCode::Ok);
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
                WriteBody(out, STRINGBUF("fail"));
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
                out->SetStatus(EStatusCode::Ok);

                WriteChunk(out, STRINGBUF("X"));
                WriteChunk(out, STRINGBUF("0123456789"));
                FinishBody(out);
            }
        },
    };

    for (auto testCase : table) {
        auto fake = New<TFakeConnection>();
        auto output = New<THttpOutput>(fake, std::get<0>(testCase), 1024);

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
                EXPECT_EQ(in->GetStatusCode(), EStatusCode::Ok);
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
                EXPECT_EQ(in->GetUrl().Path, STRINGBUF("/"));
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
                EXPECT_EQ(in->GetUrl().Path, STRINGBUF("/chunked_w_trailing_headers"));

                auto headers = in->GetHeaders();
                ASSERT_EQ(TString("test"), headers->Get("X-Foo"));

                ASSERT_THROW(in->GetTrailers(), TErrorException);

                ExpectBodyPart(in, "hello");
                ExpectBodyPart(in, " world");
                ExpectBodyEnd(in);

                auto trailers = in->GetTrailers();
                ASSERT_EQ(TString("*"), trailers->Get("Vary"));
                ASSERT_EQ(TString("text/plain"), trailers->Get("Content-Type"));
            }
        },
        TTestCase{
            EMessageType::Request,
            "GET http://yt/foo HTTP/1.1\r\n"
            "\r\n",
            [] (THttpInput* in) {
                EXPECT_EQ(STRINGBUF("yt"), in->GetUrl().Host);
            }
        }
    };

    for (auto testCase : table) {
        auto fake = New<TFakeConnection>();
        fake->Input = std::get<1>(testCase);

        auto input = New<THttpInput>(fake, TNetworkAddress(), GetSyncInvoker(), std::get<0>(testCase), 1024);

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
    : public ::testing::Test
{
    IPollerPtr Poller;
    IListenerPtr Listener;
    IServerPtr Server;
    IClientPtr Client;

    int TestPort = -1;
    TString TestUrl;

    THttpServerTest()
    {
        Poller = CreateThreadPoolPoller(4, "HttpTest");
        Listener = CreateListener(TNetworkAddress::CreateIPv6Loopback(0), Poller);
        TestPort = Listener->Address().GetPort();
        TestUrl = Format("http://localhost:%d", TestPort);
        
        Server = CreateServer(New<TServerConfig>(), Listener, Poller);
        Client = CreateClient(New<TClientConfig>(), Poller);
    }

    ~THttpServerTest()
    {
        Poller->Shutdown();
    }
};

class TOKHttpHandler
    : public IHttpHandler
{
public:
    virtual void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    {
        rsp->SetStatus(EStatusCode::Ok);
        WaitFor(rsp->Close()).ThrowOnError();
    }
};

TEST_F(THttpServerTest, SimpleRequest)
{
    Server->AddHandler("/ok", New<TOKHttpHandler>());
    Server->Start();

    auto rsp = WaitFor(Client->Get(TestUrl + "/ok")).ValueOrThrow();
    ASSERT_EQ(EStatusCode::Ok, rsp->GetStatusCode());

    Server->Stop();
    Sleep(TDuration::MilliSeconds(10));
}

class TEchoHttpHandler
    : public IHttpHandler
{
public:
    virtual void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    {
        rsp->SetStatus(EStatusCode::Ok);
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


TEST_F(THttpServerTest, TransferSmallBody)
{
    Server->AddHandler("/echo", New<TEchoHttpHandler>());
    Server->Start();

    auto reqBody = TSharedMutableRef::Allocate(1024);
    std::fill(reqBody.Begin(), reqBody.End(), 0xab);
    
    auto rsp = WaitFor(Client->Post(TestUrl + "/echo", reqBody)).ValueOrThrow();
    ASSERT_EQ(EStatusCode::Ok, rsp->GetStatusCode());

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

    EStatusCode Code = EStatusCode::Ok;
};

TEST_F(THttpServerTest, StatusCode)
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
            EXPECT_EQ(header.second, req->GetHeaders()->Get(header.first));
        }

        for (const auto& header : ReplyHeaders) {
            rsp->GetHeaders()->Add(header.first, header.second);
        }

        rsp->SetStatus(EStatusCode::Ok);
        WaitFor(rsp->Close()).ThrowOnError();
    }

    std::vector<std::pair<TString, TString>> ReplyHeaders, ExpectedHeaders;
};

TEST_F(THttpServerTest, HeadersTest)
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
    EXPECT_EQ("nocache", rsp->GetHeaders()->Get("Cache-Control"));
    EXPECT_EQ("test/plain; charset=utf-8", rsp->GetHeaders()->Get("Content-Type"));

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

TEST_F(THttpServerTest, TrailersTest)
{
    auto handler = New<TTestTrailersHandler>();

    Server->AddHandler("/trailers", handler);
    Server->Start();

    auto rsp = WaitFor(Client->Get(TestUrl + "/trailers")).ValueOrThrow();
    auto body = ReadAll(rsp);
    EXPECT_EQ("foo; bar", rsp->GetTrailers()->Get("X-Yt-Test"));

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
        rsp->SetStatus(EStatusCode::Ok);
    }
};

TEST_F(THttpServerTest, WierdHandlers)
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


class TThrowingInTheMiddleHandler
    : public IHttpHandler
{
public:
    virtual void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    {
        rsp->SetStatus(EStatusCode::Ok);
        WaitFor(rsp->Write(TSharedRef::FromString(""))).ThrowOnError();

        THROW_ERROR_EXCEPTION("Your request is bad");
    }
};

TEST_F(THttpServerTest, ThrowingHandler)
{
    auto throwing = New<TThrowingHandler>();
    auto throwingInTheMiddle = New<TThrowingInTheMiddleHandler>();

    Server->AddHandler("/throwing", throwing);
    Server->AddHandler("/throwing_in_the_middle", throwingInTheMiddle);
    Server->Start();

    ASSERT_EQ(EStatusCode::InternalServerError,
        WaitFor(Client->Get(TestUrl + "/throwing"))
            .ValueOrThrow()
            ->GetStatusCode());
    ASSERT_THROW(
        ReadAll(WaitFor(Client->Get(TestUrl + "/throwing_in_the_middle")).ValueOrThrow()),
        TErrorException);

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

        rsp->SetStatus(EStatusCode::Ok);
        WaitFor(rsp->Close()).ThrowOnError();
    }
};

TEST_F(THttpServerTest, RequestStreaming)
{
    Server->AddHandler("/consuming", New<TConsumingHandler>());
    Server->Start();

    auto body = TSharedMutableRef::Allocate(128 * 1024 * 1024);
    ASSERT_EQ(EStatusCode::Ok,
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
        rsp->SetStatus(EStatusCode::Ok);
        auto data = TSharedRef::FromString(TString(1024, 'f'));
        for (int i = 0; i < 16 * 1024; i++) {
            WaitFor(rsp->Write(data));
        }

        WaitFor(rsp->Close());
    }
};

TEST_F(THttpServerTest, ResponseStreaming)
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

TEST_F(THttpServerTest, RequestHangUp)
{
    auto validating = New<TValidateErrorHandler>();
    Server->AddHandler("/validating", validating);
    Server->Start();

    auto dialer = CreateDialer(New<TDialerConfig>(), Poller, HttpLogger);
    auto conn = WaitFor(dialer->Dial(TNetworkAddress::CreateIPv6Loopback(TestPort)))
        .ValueOrThrow();
    WaitFor(conn->Write(TSharedRef::FromString("POST /validating HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n")));
    WaitFor(conn->Close());

    Server->Stop();
    Sleep(TDuration::MilliSeconds(10));

    EXPECT_TRUE(validating->Ok);
}

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

    EXPECT_EQ(h1.Get(), handlers.Match(STRINGBUF("/")).Get());
    EXPECT_EQ(h1.Get(), handlers.Match(STRINGBUF("/c")).Get());

    EXPECT_EQ(h2.Get(), handlers.Match(STRINGBUF("/a")).Get());
    EXPECT_EQ(h1.Get(), handlers.Match(STRINGBUF("/a/")).Get());

    EXPECT_EQ(h3.Get(), handlers.Match(STRINGBUF("/a/b")).Get());
    EXPECT_EQ(h1.Get(), handlers.Match(STRINGBUF("/a/b/")).Get());

    TRequestPathMatcher handlers2;
    handlers2.Add("/a/", h2);
    EXPECT_FALSE(handlers2.Match(STRINGBUF("/")).Get());
    EXPECT_EQ(h2.Get(), handlers2.Match(STRINGBUF("/a")).Get());
    EXPECT_EQ(h2.Get(), handlers2.Match(STRINGBUF("/a/")).Get());
    EXPECT_EQ(h2.Get(), handlers2.Match(STRINGBUF("/a/b")).Get());

    TRequestPathMatcher handlers3;
    handlers3.Add("/a/", h2);
    handlers3.Add("/a", h3);
   
    EXPECT_EQ(h3.Get(), handlers3.Match(STRINGBUF("/a")).Get());
    EXPECT_EQ(h2.Get(), handlers3.Match(STRINGBUF("/a/")).Get());
    EXPECT_EQ(h2.Get(), handlers3.Match(STRINGBUF("/a/b")).Get());
}
