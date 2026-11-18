#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/runner/companion_profiling_proxy.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/config.h>
#include <yt/yt/core/http/helpers.h>
#include <yt/yt/core/http/http.h>
#include <yt/yt/core/http/server.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/connection.h>
#include <yt/yt/core/net/listener.h>

#include <library/cpp/testing/common/network.h>

#include <util/generic/size_literals.h>

namespace NYT::NFlow {
namespace {

using namespace NConcurrency;
using namespace NHttp;
using namespace NHttp::NHeaders;
using namespace NNet;

using ::testing::HasSubstr;
using ::testing::Not;

////////////////////////////////////////////////////////////////////////////////

//! Stands in for the companion's ytprof handlers: echoes the path and query it was asked for.
class TEchoHandler
    : public IHttpHandler
{
public:
    void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    {
        const auto& url = req->GetUrl();
        rsp->SetStatus(EStatusCode::OK);
        auto target = std::string(url.Path);
        if (!url.RawQuery.empty()) {
            target += Format("?%v", url.RawQuery);
        }
        WaitFor(rsp->WriteBody(TSharedRef::FromString(target)))
            .ThrowOnError();
    }
};

class TBusyHandler
    : public IHttpHandler
{
public:
    void HandleRequest(const IRequestPtr& /*req*/, const IResponseWriterPtr& rsp) override
    {
        rsp->SetStatus(EStatusCode::TooManyRequests);
        WaitFor(rsp->WriteBody(TSharedRef::FromString(std::string("Profile fetch already running"))))
            .ThrowOnError();
    }
};

//! Streams a body many times larger than the HTTP read buffer, in uneven chunks,
//! with content that depends on the position, so that a reordered, repeated or
//! overwritten piece changes it.
class TLargeBodyHandler
    : public IHttpHandler
{
public:
    static constexpr TStringBuf ContentType = "application/octet-stream";
    static constexpr TStringBuf ExtraHeader = "X-Test-Header";

    void HandleRequest(const IRequestPtr& /*req*/, const IResponseWriterPtr& rsp) override
    {
        rsp->SetStatus(EStatusCode::OK);
        rsp->GetHeaders()->Set(ContentTypeHeaderName, std::string(ContentType));
        rsp->GetHeaders()->Set(std::string(ExtraHeader), "1");

        const auto& body = GetBody();
        i64 offset = 0;
        for (int index = 0; offset < std::ssize(body); ++index) {
            auto size = std::min<i64>(1'000 + (index * 7'919) % 50'000, std::ssize(body) - offset);
            WaitFor(rsp->Write(TSharedRef::FromString(body.substr(offset, size))))
                .ThrowOnError();
            offset += size;
        }
        WaitFor(rsp->Close())
            .ThrowOnError();
    }

    static const std::string& GetBody()
    {
        static const std::string Body = [] {
            std::string body(1_MB + 13, '\0');
            for (int index = 0; index < std::ssize(body); ++index) {
                body[index] = static_cast<char>(index % 251);
            }
            return body;
        }();
        return Body;
    }
};

//! Replies with a body far larger than the socket buffers, so that an unread reply blocks its writer.
class THugeBodyHandler
    : public IHttpHandler
{
public:
    static constexpr i64 BodySize = 32_MB;

    void HandleRequest(const IRequestPtr& /*req*/, const IResponseWriterPtr& rsp) override
    {
        rsp->SetStatus(EStatusCode::OK);
        WaitFor(rsp->WriteBody(TSharedRef::FromString(std::string(BodySize, 'x'))))
            .ThrowOnError();
    }
};

//! Replies after a delay, the way a CPU profile does.
class TSlowHandler
    : public IHttpHandler
{
public:
    static constexpr auto Delay = TDuration::MilliSeconds(500);

    void HandleRequest(const IRequestPtr& /*req*/, const IResponseWriterPtr& rsp) override
    {
        TDelayedExecutor::WaitForDuration(Delay);
        rsp->SetStatus(EStatusCode::OK);
        WaitFor(rsp->WriteBody(TSharedRef::FromString(std::string("profile"))))
            .ThrowOnError();
    }
};

IServerPtr CreateTestServer(int port, const IPollerPtr& poller)
{
    auto config = New<TServerConfig>();
    config->Port = port;
    return CreateServer(config, poller);
}

////////////////////////////////////////////////////////////////////////////////

class TCompanionProfilingProxyTest
    : public ::testing::Test
{
protected:
    const NTesting::TPortHolder WorkerPort_ = NTesting::GetFreePort();
    const NTesting::TPortHolder CompanionPort_ = NTesting::GetFreePort();
    //! Reserved but never listened on.
    const NTesting::TPortHolder UnboundPort_ = NTesting::GetFreePort();

    IPollerPtr Poller_;
    IClientPtr Client_;
    IServerPtr CompanionServer_;
    IServerPtr WorkerServer_;

    void SetUp() override
    {
        Poller_ = CreateThreadPoolPoller(2, "TestPoller");
        Client_ = CreateClient(New<TClientConfig>(), Poller_);

        CompanionServer_ = CreateTestServer(static_cast<int>(CompanionPort_), Poller_);
        CompanionServer_->AddHandler("/ytprof/cpu/profile", New<TEchoHandler>());
        CompanionServer_->AddHandler("/ytprof/heap", New<TBusyHandler>());
        CompanionServer_->AddHandler("/ytprof/tcmalloc/current", New<TLargeBodyHandler>());
        CompanionServer_->AddHandler("/ytprof/tcmalloc/peak", New<TSlowHandler>());
        CompanionServer_->AddHandler("/ytprof/tcmalloc/fragmentation", New<THugeBodyHandler>());
        // Must stay unreachable through the proxy.
        CompanionServer_->AddHandler("/ytprof/binary", New<TEchoHandler>());
        CompanionServer_->AddHandler("/metrics", New<TEchoHandler>());
        CompanionServer_->Start();
    }

    void TearDown() override
    {
        if (WorkerServer_) {
            WorkerServer_->Stop();
        }
        CompanionServer_->Stop();
        Poller_->Shutdown();
    }

    void StartWorkerServer(int companionMonitoringPort, TClientConfigPtr clientConfig = New<TClientConfig>())
    {
        WorkerServer_ = CreateTestServer(static_cast<int>(WorkerPort_), Poller_);
        RegisterCompanionProfilingProxy(
            WorkerServer_,
            clientConfig,
            Poller_,
            companionMonitoringPort);
        WorkerServer_->Start();
    }

    IResponsePtr Get(TStringBuf path)
    {
        return WaitFor(Client_->Get(Format("http://localhost:%v%v", static_cast<int>(WorkerPort_), path)))
            .ValueOrThrow();
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TCompanionProfilingProxyTest, ForwardsPathAndQuery)
{
    StartWorkerServer(static_cast<int>(CompanionPort_));

    auto response = Get("/companion/ytprof/cpu/profile?d=30s&symbolize=0");
    EXPECT_EQ(response->GetStatusCode(), EStatusCode::OK);
    EXPECT_EQ(response->ReadAll().ToStringBuf(), "/ytprof/cpu/profile?d=30s&symbolize=0");
}

TEST_F(TCompanionProfilingProxyTest, ForwardsPathWithoutQuery)
{
    StartWorkerServer(static_cast<int>(CompanionPort_));

    auto response = Get("/companion/ytprof/cpu/profile");
    EXPECT_EQ(response->GetStatusCode(), EStatusCode::OK);
    EXPECT_EQ(response->ReadAll().ToStringBuf(), "/ytprof/cpu/profile");
}

TEST_F(TCompanionProfilingProxyTest, RejectsNonGetRequests)
{
    StartWorkerServer(static_cast<int>(CompanionPort_));

    auto response = WaitFor(Client_->Post(
        Format("http://localhost:%v/companion/ytprof/cpu/profile", static_cast<int>(WorkerPort_)),
        TSharedRef::FromString(std::string("request"))))
        .ValueOrThrow();
    EXPECT_EQ(response->GetStatusCode(), EStatusCode::MethodNotAllowed);
    EXPECT_EQ(response->ReadAll().ToStringBuf(), "Only GET requests are supported");
    EXPECT_EQ(response->GetHeaders()->GetOrThrow(ContentTypeHeaderName), "text/plain; charset=utf-8");
}

TEST_F(TCompanionProfilingProxyTest, ForwardsCompanionStatus)
{
    StartWorkerServer(static_cast<int>(CompanionPort_));

    auto response = Get("/companion/ytprof/heap");
    EXPECT_EQ(response->GetStatusCode(), EStatusCode::TooManyRequests);
    EXPECT_EQ(response->ReadAll().ToStringBuf(), "Profile fetch already running");
}

// The worker's request and trace ids must not be joined by the companion's ones.
TEST_F(TCompanionProfilingProxyTest, ForwardsBodyIntactWithoutCompanionHeaders)
{
    StartWorkerServer(static_cast<int>(CompanionPort_));

    auto response = Get("/companion/ytprof/tcmalloc/current");
    EXPECT_EQ(response->GetStatusCode(), EStatusCode::OK);
    // Compared as a flag: a mismatch would otherwise print a megabyte of binary data.
    EXPECT_TRUE(response->ReadAll().ToStringBuf() == TStringBuf(TLargeBodyHandler::GetBody()));

    const auto& headers = response->GetHeaders();
    EXPECT_EQ(headers->Find(ContentTypeHeaderName), nullptr);
    EXPECT_EQ(headers->Find(TLargeBodyHandler::ExtraHeader), nullptr);
    EXPECT_EQ(headers->GetAll(RequestIdHeaderName).size(), 1u);
}

// Profiles reply only once collected, so the proxy must outwait a short header timeout.
TEST_F(TCompanionProfilingProxyTest, WaitsForSlowProfiles)
{
    auto clientConfig = New<TClientConfig>();
    clientConfig->HeaderReadTimeout = TSlowHandler::Delay / 5;
    StartWorkerServer(static_cast<int>(CompanionPort_), clientConfig);

    auto response = Get("/companion/ytprof/tcmalloc/peak");
    EXPECT_EQ(response->GetStatusCode(), EStatusCode::OK);
    EXPECT_EQ(response->ReadAll().ToStringBuf(), "profile");
}

// A reply stays in the worker until its client reads it, after the companion has moved on.
TEST_F(TCompanionProfilingProxyTest, LimitsRepliesHeldForClients)
{
    StartWorkerServer(static_cast<int>(CompanionPort_));

    std::vector<IResponsePtr> unreadResponses;
    for (int index = 0; index < MaxCompanionProfilingRequestsInFlight; ++index) {
        unreadResponses.push_back(Get("/companion/ytprof/tcmalloc/fragmentation"));
        EXPECT_EQ(unreadResponses.back()->GetStatusCode(), EStatusCode::OK);
    }

    auto rejected = Get("/companion/ytprof/cpu/profile");
    EXPECT_EQ(rejected->GetStatusCode(), EStatusCode::TooManyRequests);
    EXPECT_THAT(std::string(rejected->ReadAll().ToStringBuf()), HasSubstr("served at once"));

    for (const auto& response : unreadResponses) {
        EXPECT_EQ(response->ReadAll().Size(), static_cast<size_t>(THugeBodyHandler::BodySize));
    }

    // The worker frees a slot only after its write completes, which may lag behind the client's read.
    WaitForPredicate([&] {
        return Get("/companion/ytprof/cpu/profile")->GetStatusCode() == EStatusCode::OK;
    });
}

// Longer profiles would outlast the proxy's header timeout.
TEST_F(TCompanionProfilingProxyTest, RejectsTooLongProfiles)
{
    StartWorkerServer(static_cast<int>(CompanionPort_));

    auto response = Get("/companion/ytprof/cpu/profile?d=1h");
    EXPECT_EQ(response->GetStatusCode(), EStatusCode::BadRequest);
    EXPECT_EQ(response->ReadAll().ToStringBuf(), "Profile duration 1h exceeds the limit of 5 minutes");
}

TEST_F(TCompanionProfilingProxyTest, RefusesCompanionBinary)
{
    StartWorkerServer(static_cast<int>(CompanionPort_));

    auto response = Get("/companion/ytprof/binary");
    EXPECT_EQ(response->GetStatusCode(), EStatusCode::NotFound);
    EXPECT_THAT(std::string(response->ReadAll().ToStringBuf()), HasSubstr("companion monitoring port"));
}

// A reply interrupted in the middle of the body must not pass for a complete one.
TEST_F(TCompanionProfilingProxyTest, ReportsCompanionFailureInTheMiddleOfTheBody)
{
    // A raw upstream: the HTTP server always terminates the replies it has started.
    auto listener = CreateListener(TNetworkAddress::CreateIPv6Loopback(0), Poller_, Poller_);
    auto shutdownGuard = Finally([&] {
        listener->Shutdown();
    });
    auto served = BIND([listener] {
        auto connection = WaitFor(listener->Accept())
            .ValueOrThrow();

        // Consume the request, so that closing the socket ends the reply with EOF rather than a reset.
        auto buffer = TSharedMutableRef::Allocate(4096);
        WaitFor(connection->Read(buffer))
            .ValueOrThrow();

        // A chunked reply with a single chunk and no terminating one.
        WaitFor(connection->Write(TSharedRef::FromString(std::string(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n"))))
            .ThrowOnError();
        WaitFor(connection->Close())
            .ThrowOnError();
    })
        .AsyncVia(Poller_->GetInvoker())
        .Run();

    StartWorkerServer(listener->GetAddress().GetPort());

    auto response = Get("/companion/ytprof/cpu/profile");
    EXPECT_EQ(response->GetStatusCode(), EStatusCode::BadGateway);
    EXPECT_THAT(std::string(response->ReadAll().ToStringBuf()), Not(HasSubstr("hello")));

    // Proves that the 502 comes from the interrupted reply and not from an unreachable upstream.
    EXPECT_TRUE(WaitFor(served.WithTimeout(TDuration::Seconds(10))).IsOK());
}

TEST_F(TCompanionProfilingProxyTest, ExposesOnlyTheProfilingSubtree)
{
    StartWorkerServer(static_cast<int>(CompanionPort_));

    EXPECT_EQ(Get("/companion/metrics")->GetStatusCode(), EStatusCode::NotFound);
    EXPECT_EQ(Get("/companion/ytprof")->GetStatusCode(), EStatusCode::NotFound);
}

TEST_F(TCompanionProfilingProxyTest, ReportsUnreachableCompanion)
{
    StartWorkerServer(static_cast<int>(UnboundPort_));

    auto response = Get("/companion/ytprof/cpu/profile");
    EXPECT_EQ(response->GetStatusCode(), EStatusCode::BadGateway);
    EXPECT_THAT(std::string(response->ReadAll().ToStringBuf()), HasSubstr("Failed to fetch /ytprof/cpu/profile"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
