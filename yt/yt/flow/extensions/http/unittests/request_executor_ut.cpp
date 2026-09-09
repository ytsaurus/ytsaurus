#include <yt/yt/flow/extensions/http/sink.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/http/mock/client.h>
#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/profiling/solomon/registry.h>

#include <atomic>
#include <deque>
#include <mutex>
#include <thread>

namespace NYT::NFlow {
namespace {

using namespace NConcurrency;
using namespace NHttp;
using namespace testing;

class TScriptedResponse
    : public IResponse
{
public:
    TScriptedResponse(int statusCode, std::vector<TFuture<TSharedRef>> chunks)
        : StatusCode_(static_cast<EStatusCode>(statusCode))
        , Chunks_(chunks.begin(), chunks.end())
        , Headers_(New<THeaders>())
        , Trailers_(New<THeaders>())
    { }

    EStatusCode GetStatusCode() override
    {
        return StatusCode_;
    }

    const THeadersPtr& GetHeaders() override
    {
        return Headers_;
    }

    const THeadersPtr& GetTrailers() override
    {
        return Trailers_;
    }

    TFuture<TSharedRef> Read() override
    {
        if (Chunks_.empty()) {
            return MakeFuture(TSharedRef());
        }
        auto chunk = std::move(Chunks_.front());
        Chunks_.pop_front();
        return chunk;
    }

private:
    const EStatusCode StatusCode_;
    std::deque<TFuture<TSharedRef>> Chunks_;
    const THeadersPtr Headers_;
    const THeadersPtr Trailers_;
};

DEFINE_REFCOUNTED_TYPE(TScriptedResponse);

class TAsyncMockClient
    : public TMockClient
{
public:
    TFuture<IResponsePtr> Post(
        const std::string& url,
        const TSharedRef& body,
        const THeadersPtr& headers) override
    {
        Urls.push_back(url);
        Bodies.push_back(body);
        Headers.push_back(headers);
        YT_VERIFY(!Responses.empty());
        auto response = std::move(Responses.front());
        Responses.pop_front();
        return response;
    }

    std::deque<TFuture<IResponsePtr>> Responses;
    std::vector<std::string> Urls;
    std::vector<TSharedRef> Bodies;
    std::vector<THeadersPtr> Headers;
};

DEFINE_REFCOUNTED_TYPE(TAsyncMockClient);

IResponsePtr MakeResponse(
    int statusCode,
    std::vector<TFuture<TSharedRef>> chunks = {},
    THeadersPtr headers = New<THeaders>())
{
    auto response = New<TScriptedResponse>(statusCode, std::move(chunks));
    for (const auto& [name, value] : headers->Dump()) {
        response->GetHeaders()->Set(name, value);
    }
    return response;
}

TDynamicAsyncHttpSinkParametersPtr MakeDynamicParameters()
{
    auto parameters = New<TDynamicAsyncHttpSinkParameters>();
    parameters->RequestTimeout = TDuration::Seconds(15);
    parameters->AttemptTimeout = TDuration::Seconds(10);
    parameters->RetryInitialDelay = TDuration::Seconds(1);
    parameters->RetryMinimumDelay = TDuration::MilliSeconds(100);
    parameters->RetryMultiplier = 2.0;
    parameters->RetryMaximumDelay = TDuration::Seconds(30);
    parameters->RetryJitterRatio = 0.2;
    parameters->MaxAttemptCount = 3;
    return parameters;
}

TAsyncHttpRequestExecutorPtr MakeExecutor(
    const IClientPtr& client,
    const THeadersPtr& headers,
    std::vector<TDuration>* delays,
    NProfiling::TProfiler profiler = {})
{
    auto statusProfiler = CreateSyncStatusProfiler();
    return New<TAsyncHttpRequestExecutor>(
        client,
        "https://example.test/post",
        headers,
        MakeDynamicParameters(),
        GetSyncInvoker(),
        std::move(profiler),
        statusProfiler->ErrorState("/test"),
        BIND([delays] (TDuration delay) {
            delays->push_back(delay);
            return OKFuture;
        }),
        BIND([] {
            return 0.0;
        }),
        BIND([] {
            return TInstant::Seconds(100);
        }));
}

TAsyncHttpRequestExecutorPtr MakeExecutor(
    TAsyncHttpClients clients,
    std::string url,
    int maxRedirectCount,
    NProfiling::TProfiler profiler = {},
    THeadersPtr headers = New<THeaders>())
{
    auto statusProfiler = CreateSyncStatusProfiler();
    return New<TAsyncHttpRequestExecutor>(
        std::move(clients),
        std::move(url),
        std::move(headers),
        MakeDynamicParameters(),
        GetSyncInvoker(),
        std::move(profiler),
        statusProfiler->ErrorState("/test"),
        BIND([] (TDuration) {
            return OKFuture;
        }),
        BIND([] {
            return 0.0;
        }),
        BIND([] {
            return TInstant::Seconds(100);
        }),
        std::string(),
        maxRedirectCount);
}

i64 ReadStatusCounter(
    const NProfiling::NProto::TSensorDump& dump,
    TStringBuf sensorName,
    TStringBuf statusCode)
{
    for (const auto& cube : dump.cubes()) {
        if (cube.name() != sensorName) {
            continue;
        }
        for (const auto& projection : cube.projections()) {
            bool matches = false;
            for (int tagIndex = 0; tagIndex < projection.tag_ids_size(); ++tagIndex) {
                const auto& tag = dump.tags().Get(projection.tag_ids().Get(tagIndex));
                matches |= tag.key() == "status_code" && tag.value() == statusCode;
            }
            if (matches && projection.has_counter()) {
                return projection.counter();
            }
        }
    }
    return 0;
}

i64 ReadCounter(
    const NProfiling::NProto::TSensorDump& dump,
    TStringBuf sensorName)
{
    for (const auto& cube : dump.cubes()) {
        if (cube.name() != sensorName) {
            continue;
        }
        for (const auto& projection : cube.projections()) {
            if (projection.has_counter()) {
                return projection.counter();
            }
        }
    }
    return 0;
}

NProfiling::NProto::TSensorDump CollectSensors(
    const NProfiling::TSolomonRegistryPtr& registry)
{
    registry->ProcessRegistrations();
    registry->Collect();
    return registry->DumpSensors();
}

TEST(TAsyncHttpRetryTest, CalculatesJitteredDelay)
{
    auto parameters = MakeDynamicParameters();

    EXPECT_EQ(CalculateAsyncHttpRetryDelay(*parameters, 0, -1.0), TDuration::MilliSeconds(800));
    EXPECT_EQ(CalculateAsyncHttpRetryDelay(*parameters, 0, 0.0), TDuration::Seconds(1));
    EXPECT_EQ(CalculateAsyncHttpRetryDelay(*parameters, 0, 1.0), TDuration::MilliSeconds(1200));
    EXPECT_GE(CalculateAsyncHttpRetryDelay(*parameters, 99, -1.0), TDuration::Seconds(24));
    EXPECT_LE(CalculateAsyncHttpRetryDelay(*parameters, 99, 1.0), TDuration::Seconds(30));
}

TEST(TAsyncHttpRetryTest, FollowsHttpToHttpsRedirectWithTheTlsClient)
{
    auto httpClient = New<TAsyncMockClient>();
    auto httpsClient = New<TAsyncMockClient>();
    auto registry = New<NProfiling::TSolomonRegistry>();
    registry->SetWindowSize(12);
    auto redirectHeaders = New<THeaders>();
    redirectHeaders->Set("Location", "https://receiver.test/post");
    httpClient->Responses.push_back(MakeFuture<IResponsePtr>(MakeResponse(307, {}, redirectHeaders)));
    httpsClient->Responses.push_back(MakeFuture<IResponsePtr>(MakeResponse(204)));
    auto executor = MakeExecutor(
        {.Http = httpClient, .Https = httpsClient},
        "http://sender.test/redirect",
        1,
        NProfiling::TProfiler(registry, "/test"));

    EXPECT_TRUE(WaitFor(executor->Post(TSharedRef::FromString(std::string("payload")))).IsOK());
    EXPECT_EQ(httpClient->Urls, std::vector<std::string>{"http://sender.test/redirect"});
    EXPECT_EQ(httpsClient->Urls, std::vector<std::string>{"https://receiver.test/post"});
    EXPECT_EQ(ToString(httpsClient->Bodies.front()), "payload");
    const auto dump = CollectSensors(registry);
    EXPECT_EQ(ReadStatusCounter(dump, "yt/test/responses", "307"), 1);
    EXPECT_EQ(ReadStatusCounter(dump, "yt/test/responses", "204"), 1);
}

TEST(TAsyncHttpRetryTest, ResolvesRelativeRedirectLocations)
{
    struct TCase
    {
        std::string Location;
        std::string ExpectedUrl;
    };

    for (const auto& testCase : std::vector<TCase>{
            {"https://receiver.test/absolute", "https://receiver.test/absolute"},
            {"/v2/events", "https://sender.test/v2/events"},
            {"next", "https://sender.test/v1/parent/next"},
            {"../events", "https://sender.test/v1/events"},
            {"?new=query", "https://sender.test/v1/parent/item?new=query"},
            {"//receiver.test/scheme-relative", "https://receiver.test/scheme-relative"},
         }) {
        auto client = New<TAsyncMockClient>();
        auto headers = New<THeaders>();
        headers->Set("X-Flow-Test", "configured");
        auto redirectHeaders = New<THeaders>();
        redirectHeaders->Set("Location", testCase.Location);
        client->Responses.push_back(MakeFuture<IResponsePtr>(MakeResponse(307, {}, redirectHeaders)));
        client->Responses.push_back(MakeFuture<IResponsePtr>(MakeResponse(204)));
        auto executor = MakeExecutor(
            {.Http = client, .Https = client},
            "https://sender.test/v1/parent/item?old=query",
            1,
            {},
            headers);

        EXPECT_TRUE(WaitFor(executor->Post(TSharedRef::FromString(std::string("payload")))).IsOK())
            << testCase.Location;
        EXPECT_EQ(client->Urls, (std::vector<std::string>{
                    "https://sender.test/v1/parent/item?old=query",
                    testCase.ExpectedUrl,
                                }))
            << testCase.Location;
        EXPECT_EQ(ToString(client->Bodies[1]), "payload") << testCase.Location;
        ASSERT_NE(client->Headers[1]->Find("X-Flow-Test"), nullptr) << testCase.Location;
        EXPECT_EQ(*client->Headers[1]->Find("X-Flow-Test"), "configured") << testCase.Location;
    }
}

TEST(TAsyncHttpRetryTest, RejectsUnresolvableRedirectLocation)
{
    auto client = New<TAsyncMockClient>();
    auto redirectHeaders = New<THeaders>();
    redirectHeaders->Set("Location", "http://[::1");
    client->Responses.push_back(MakeFuture<IResponsePtr>(MakeResponse(307, {}, redirectHeaders)));
    auto executor = MakeExecutor(
        {.Http = client, .Https = client},
        "https://sender.test/post",
        1);
    auto parameters = MakeDynamicParameters();
    parameters->MaxAttemptCount = 1;
    executor->Reconfigure(parameters);

    auto result = WaitFor(executor->Post(TSharedRef::FromString(std::string("payload"))));
    EXPECT_FALSE(result.IsOK());
    EXPECT_THAT(ToString(result), HasSubstr("Async HTTP sink could not resolve redirect URL"));
    EXPECT_EQ(client->Urls, std::vector<std::string>{"https://sender.test/post"});
}

TEST(TAsyncHttpRetryTest, StopsFollowingAfterConfiguredNumberOfRedirects)
{
    auto httpClient = New<TAsyncMockClient>();
    auto firstRedirectHeaders = New<THeaders>();
    firstRedirectHeaders->Set("Location", "http://receiver.test/first");
    auto secondRedirectHeaders = New<THeaders>();
    secondRedirectHeaders->Set("Location", "http://receiver.test/second");
    httpClient->Responses.push_back(MakeFuture<IResponsePtr>(MakeResponse(307, {}, firstRedirectHeaders)));
    httpClient->Responses.push_back(MakeFuture<IResponsePtr>(MakeResponse(307, {}, secondRedirectHeaders)));
    auto executor = MakeExecutor(
        {.Http = httpClient, .Https = httpClient},
        "http://sender.test/redirect",
        1);
    auto parameters = MakeDynamicParameters();
    parameters->MaxAttemptCount = 1;
    executor->Reconfigure(parameters);

    EXPECT_FALSE(WaitFor(executor->Post(TSharedRef::FromString(std::string("payload")))).IsOK());
    EXPECT_EQ(httpClient->Urls, (std::vector<std::string>{
            "http://sender.test/redirect",
            "http://receiver.test/first",
                                }));
}

TEST(TAsyncHttpRetryTest, AppliesRedirectLimitAcrossRetryAttempts)
{
    auto client = New<TAsyncMockClient>();
    auto firstRedirectHeaders = New<THeaders>();
    firstRedirectHeaders->Set("Location", "http://receiver.test/first");
    auto secondRedirectHeaders = New<THeaders>();
    secondRedirectHeaders->Set("Location", "http://receiver.test/second");
    client->Responses.push_back(MakeFuture<IResponsePtr>(MakeResponse(307, {}, firstRedirectHeaders)));
    client->Responses.push_back(MakeFuture<IResponsePtr>(MakeResponse(503)));
    client->Responses.push_back(MakeFuture<IResponsePtr>(MakeResponse(307, {}, secondRedirectHeaders)));
    client->Responses.push_back(MakeFuture<IResponsePtr>(MakeResponse(204)));
    auto registry = New<NProfiling::TSolomonRegistry>();
    registry->SetWindowSize(12);
    auto executor = MakeExecutor(
        {.Http = client, .Https = client},
        "http://sender.test/redirect",
        1,
        NProfiling::TProfiler(registry, "/test"));
    auto parameters = MakeDynamicParameters();
    parameters->MaxAttemptCount = 2;
    executor->Reconfigure(parameters);

    EXPECT_FALSE(WaitFor(executor->Post(TSharedRef::FromString(std::string("payload")))).IsOK());
    EXPECT_EQ(client->Urls, (std::vector<std::string>{
            "http://sender.test/redirect",
            "http://receiver.test/first",
            "http://sender.test/redirect",
                            }));

    const auto dump = CollectSensors(registry);
    EXPECT_EQ(ReadStatusCounter(dump, "yt/test/responses", "307"), 2);
    EXPECT_EQ(ReadStatusCounter(dump, "yt/test/responses", "503"), 1);
    EXPECT_EQ(ReadStatusCounter(dump, "yt/test/responses", "204"), 0);
}

TEST(TAsyncHttpRetryTest, RejectsHttpsToHttpRedirectWithoutSendingTheBody)
{
    auto httpClient = New<TAsyncMockClient>();
    auto httpsClient = New<TAsyncMockClient>();
    auto redirectHeaders = New<THeaders>();
    redirectHeaders->Set("Location", "http://receiver.test/post");
    httpsClient->Responses.push_back(MakeFuture<IResponsePtr>(MakeResponse(307, {}, redirectHeaders)));
    auto executor = MakeExecutor(
        {.Http = httpClient, .Https = httpsClient},
        "https://sender.test/redirect",
        1);
    auto parameters = MakeDynamicParameters();
    parameters->MaxAttemptCount = 1;
    executor->Reconfigure(parameters);

    auto result = WaitFor(executor->Post(TSharedRef::FromString(std::string("payload"))));
    EXPECT_FALSE(result.IsOK());
    EXPECT_TRUE(httpClient->Urls.empty());
    EXPECT_EQ(httpsClient->Urls, std::vector<std::string>{"https://sender.test/redirect"});
}

TEST(TAsyncHttpRetryTest, AcceptsAndDrainsSuccess)
{
    auto client = New<StrictMock<TMockClient>>();
    EXPECT_CALL(*client, Post("https://example.test/post", "payload", _))
        .WillOnce(Return(TMockResponse{
            .StatusCode = EStatusCode::Created,
            .Body = "response body",
        }));

    std::vector<TDuration> delays;
    auto executor = MakeExecutor(client, New<THeaders>(), &delays);
    EXPECT_TRUE(WaitFor(executor->Post(TSharedRef::FromString(std::string("payload")))).IsOK());
    EXPECT_TRUE(delays.empty());
}

TEST(TAsyncHttpRetryTest, AddsStableConfigurableIdempotencyHeader)
{
    auto client = New<StrictMock<TMockClient>>();
    auto headers = New<THeaders>();
    headers->Set("X-Flow-Message-Id", "static-value");
    EXPECT_CALL(*client, Post("https://example.test/post", "payload", UnorderedElementsAre(Pair("X-Flow-Message-Id", "message-42"))))
        .Times(2)
        .WillOnce(Return(TMockResponse{
            .StatusCode = EStatusCode::ServiceUnavailable,
        }))
        .WillOnce(Return(TMockResponse{
            .StatusCode = EStatusCode::NoContent,
        }));

    std::vector<TDuration> delays;
    auto statusProfiler = CreateSyncStatusProfiler();
    auto executor = New<TAsyncHttpRequestExecutor>(
        client,
        "https://example.test/post",
        headers,
        MakeDynamicParameters(),
        GetSyncInvoker(),
        NProfiling::TProfiler(),
        statusProfiler->ErrorState("/test"),
        BIND([&delays] (TDuration delay) {
            delays.push_back(delay);
            return OKFuture;
        }),
        BIND([] {
            return 0.0;
        }),
        BIND([] {
            return TInstant::Seconds(100);
        }),
        "X-Flow-Message-Id");

    EXPECT_TRUE(WaitFor(executor->Post(
        TSharedRef::FromString(std::string("payload")),
        "message-42"))
            .IsOK());
    EXPECT_EQ(headers->GetOrThrow("X-Flow-Message-Id"), "static-value");
}

TEST(TAsyncHttpRetryTest, AllowsDisablingIdempotencyHeader)
{
    auto client = New<StrictMock<TMockClient>>();
    EXPECT_CALL(*client, Post("https://example.test/post", "payload", UnorderedElementsAre()))
        .WillOnce(Return(TMockResponse{
            .StatusCode = EStatusCode::NoContent,
        }));

    std::vector<TDuration> delays;
    auto executor = MakeExecutor(client, New<THeaders>(), &delays);
    EXPECT_TRUE(WaitFor(executor->Post(
        TSharedRef::FromString(std::string("payload")),
        "message-42"))
            .IsOK());
}

TEST(TAsyncHttpRetryTest, RetriesNonSuccessWithStablePayloadAndHeaders)
{
    auto client = New<StrictMock<TMockClient>>();
    auto headers = New<THeaders>();
    headers->Set("Content-Type", "application/octet-stream");
    headers->Set("X-Flow-Test", "ytflow-851");

    for (const auto& payload : std::vector<std::string>{
            std::string("arbitrary\0bytes", 15),
            std::string("\x0a\x03"
                        "abc",
                5),
            "{key=value;}",
            R"({"key":"value"})",
         }) {
        EXPECT_CALL(*client, Post("https://example.test/post", payload, UnorderedElementsAre(Pair("Content-Type", "application/octet-stream"), Pair("X-Flow-Test", "ytflow-851"))))
            .Times(2)
            .WillOnce(Return(TMockResponse{
                .StatusCode = EStatusCode::ServiceUnavailable,
                .Body = "retry response",
            }))
            .WillOnce(Return(TMockResponse{
                .StatusCode = EStatusCode::NoContent,
            }));
    }

    std::vector<TDuration> delays;
    auto executor = MakeExecutor(client, headers, &delays);
    for (const auto& payload : std::vector<std::string>{
            std::string("arbitrary\0bytes", 15),
            std::string("\x0a\x03"
                        "abc",
                5),
            "{key=value;}",
            R"({"key":"value"})",
         }) {
        EXPECT_TRUE(WaitFor(executor->Post(TSharedRef::FromString(payload))).IsOK());
    }
    ASSERT_EQ(delays.size(), 4u);
    for (auto delay : delays) {
        EXPECT_EQ(delay, TDuration::Seconds(1));
    }
}

TEST(TAsyncHttpRetryTest, ClassifiesEveryNumericStatus)
{
    const std::vector<int> accepted = {200, 201, 204, 206, 299};
    const std::vector<int> retried = {99, 199, 300, 404, 429, 500, 503, 599, 700};
    auto client = New<StrictMock<TMockClient>>();
    auto registry = New<NProfiling::TSolomonRegistry>();
    registry->SetWindowSize(12);
    std::vector<TDuration> delays;
    auto executor = MakeExecutor(
        client,
        New<THeaders>(),
        &delays,
        NProfiling::TProfiler(registry, "/test"));

    for (int statusCode : accepted) {
        EXPECT_CALL(*client, Post(_, _, _))
            .WillOnce(Return(TMockResponse{
                .StatusCode = static_cast<EStatusCode>(statusCode),
            }));
        EXPECT_TRUE(WaitFor(executor->Post(TSharedRef::FromString(std::string("payload")))).IsOK());
    }
    for (int statusCode : retried) {
        EXPECT_CALL(*client, Post(_, _, _))
            .WillOnce(Return(TMockResponse{
                .StatusCode = static_cast<EStatusCode>(statusCode),
                .Body = "drained",
            }))
            .WillOnce(Return(TMockResponse{
                .StatusCode = EStatusCode::NoContent,
            }));
        EXPECT_TRUE(WaitFor(executor->Post(TSharedRef::FromString(std::string("payload")))).IsOK());
    }

    const auto dump = CollectSensors(registry);
    for (int statusCode : accepted) {
        if (statusCode != 204) {
            EXPECT_EQ(ReadStatusCounter(dump, "yt/test/responses", ToString(statusCode)), 1);
        }
    }
    for (int statusCode : retried) {
        EXPECT_EQ(ReadStatusCounter(dump, "yt/test/responses", ToString(statusCode)), 1);
    }
    EXPECT_EQ(ReadStatusCounter(dump, "yt/test/responses", "204"), 1 + std::ssize(retried));
}

TEST(TAsyncHttpRetryTest, ReconfigurationAffectsNextRetryDecision)
{
    auto client = New<TAsyncMockClient>();
    auto firstResponse = NewPromise<IResponsePtr>();
    client->Responses.push_back(firstResponse.ToFuture());
    client->Responses.push_back(MakeFuture<IResponsePtr>(MakeResponse(204)));

    auto dynamicParameters = MakeDynamicParameters();
    std::vector<TDuration> delays;
    auto statusProfiler = CreateSyncStatusProfiler();
    auto executor = New<TAsyncHttpRequestExecutor>(
        client,
        "https://example.test/post",
        New<THeaders>(),
        dynamicParameters,
        GetSyncInvoker(),
        NProfiling::TProfiler(),
        statusProfiler->ErrorState("/test"),
        BIND([&delays] (TDuration delay) {
            delays.push_back(delay);
            return OKFuture;
        }),
        BIND([] {
            return 0.0;
        }),
        BIND([] {
            return TInstant::Seconds(100);
        }));

    auto result = executor->Post(TSharedRef::FromString(std::string("payload")));
    auto reconfigured = MakeDynamicParameters();
    reconfigured->RetryInitialDelay = TDuration::MilliSeconds(25);
    reconfigured->RetryMinimumDelay = TDuration::MilliSeconds(25);
    executor->Reconfigure(reconfigured);
    firstResponse.Set(MakeResponse(503));

    EXPECT_TRUE(WaitFor(result).IsOK());
    ASSERT_EQ(delays, std::vector{TDuration::MilliSeconds(25)});
    ASSERT_EQ(client->Bodies.size(), 2u);
    EXPECT_EQ(ToString(client->Bodies[0]), "payload");
    EXPECT_EQ(ToString(client->Bodies[1]), "payload");
}

TEST(TAsyncHttpRetryTest, TracksConcurrentDeliveryErrorsIndependently)
{
    auto client = New<TAsyncMockClient>();
    client->Responses.push_back(MakeFuture<IResponsePtr>(MakeResponse(503)));
    client->Responses.push_back(MakeFuture<IResponsePtr>(MakeResponse(204)));
    client->Responses.push_back(MakeFuture<IResponsePtr>(MakeResponse(503)));
    client->Responses.push_back(MakeFuture<IResponsePtr>(MakeResponse(204)));
    client->Responses.push_back(MakeFuture<IResponsePtr>(MakeResponse(204)));

    auto firstDelay = NewPromise<void>();
    auto secondDelay = NewPromise<void>();
    std::deque<TFuture<void>> delayFutures{
        firstDelay.ToFuture(),
        secondDelay.ToFuture(),
    };
    auto registry = New<NProfiling::TSolomonRegistry>();
    registry->SetWindowSize(12);
    auto statusProfiler = CreateSyncStatusProfiler();
    auto executor = New<TAsyncHttpRequestExecutor>(
        client,
        "https://example.test/post",
        New<THeaders>(),
        MakeDynamicParameters(),
        GetSyncInvoker(),
        NProfiling::TProfiler(registry, "/test"),
        statusProfiler->ErrorState("/test"),
        BIND([&delayFutures] (TDuration) {
            YT_VERIFY(!delayFutures.empty());
            auto future = std::move(delayFutures.front());
            delayFutures.pop_front();
            return future;
        }),
        BIND([] {
            return 0.0;
        }),
        BIND([] {
            return TInstant::Seconds(100);
        }));

    auto first = executor->Post(TSharedRef::FromString(std::string("first")));
    EXPECT_FALSE(first.IsSet());
    EXPECT_TRUE(statusProfiler->GetStatus().Errors.contains("/test"));

    auto successful = executor->Post(TSharedRef::FromString(std::string("successful")));
    EXPECT_TRUE(WaitFor(successful).IsOK());
    EXPECT_TRUE(statusProfiler->GetStatus().Errors.contains("/test"));

    auto second = executor->Post(TSharedRef::FromString(std::string("second")));
    EXPECT_FALSE(second.IsSet());
    firstDelay.Set();
    EXPECT_TRUE(WaitFor(first).IsOK());
    EXPECT_TRUE(statusProfiler->GetStatus().Errors.contains("/test"));

    secondDelay.Set();
    EXPECT_TRUE(WaitFor(second).IsOK());
    EXPECT_FALSE(statusProfiler->GetStatus().Errors.contains("/test"));

    const auto dump = CollectSensors(registry);
    EXPECT_EQ(ReadStatusCounter(dump, "yt/test/responses", "503"), 2);
    EXPECT_EQ(ReadStatusCounter(dump, "yt/test/responses", "204"), 3);
    EXPECT_EQ(ReadCounter(dump, "yt/test/attempt_failures"), 0);
}

TEST(TAsyncHttpRetryTest, SerializesConcurrentResponseCompletionsOnActionQueue)
{
    constexpr int RequestCount = 64;
    constexpr int CompletionThreadCount = 4;

    auto actionQueue = New<TActionQueue>("AsyncHttpResponseCompletion");
    auto serializedInvoker = CreateSerializedInvoker(actionQueue->GetInvoker());
    auto completionPool = CreateThreadPool(CompletionThreadCount, "AsyncHttpCompletion");
    auto client = New<TAsyncMockClient>();
    std::vector<TPromise<IResponsePtr>> responsePromises;
    responsePromises.reserve(RequestCount);
    for (int index = 0; index < RequestCount; ++index) {
        auto promise = NewPromise<IResponsePtr>();
        client->Responses.push_back(promise.ToFuture());
        responsePromises.push_back(std::move(promise));
    }

    auto statusProfiler = CreateSyncStatusProfiler();
    auto executor = New<TAsyncHttpRequestExecutor>(
        client,
        "https://example.test/post",
        New<THeaders>(),
        MakeDynamicParameters(),
        serializedInvoker,
        NProfiling::TProfiler(),
        statusProfiler->ErrorState("/test"),
        BIND([] (TDuration) {
            return OKFuture;
        }),
        BIND([] {
            return 0.0;
        }),
        BIND([] {
            return TInstant::Seconds(100);
        }));

    const auto actionQueueThreadId = WaitFor(
        BIND([] {
            return std::this_thread::get_id();
        })
            .AsyncVia(serializedInvoker)
            .Run()
            .WithTimeout(TDuration::Seconds(5)))
        .ValueOrThrow();

    auto callbacksCompleted = NewPromise<void>();
    std::atomic<int> callbacksRemaining = RequestCount;
    std::mutex callbackMutex;
    std::vector<TError> completionErrors;
    std::vector<std::thread::id> completionThreadIds;
    std::vector<TFuture<void>> responseFutures;
    responseFutures.reserve(RequestCount);
    for (int index = 0; index < RequestCount; ++index) {
        auto responseFuture = executor->Post(TSharedRef::FromString(Format("payload-%v", index)));
        responseFuture.Subscribe(BIND([
            &callbackMutex,
            &completionErrors,
            &completionThreadIds,
            &callbacksRemaining,
            callbacksCompleted] (const TError& error) {
            {
                std::lock_guard guard(callbackMutex);
                completionErrors.push_back(error);
                completionThreadIds.push_back(std::this_thread::get_id());
            }
            if (callbacksRemaining.fetch_sub(1) == 1) {
                callbacksCompleted.Set();
            }
        }));
        responseFutures.push_back(std::move(responseFuture));
    }

    WaitFor(
        BIND([] {
        })
            .AsyncVia(serializedInvoker)
            .Run()
            .WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();
    ASSERT_EQ(client->Bodies.size(), static_cast<size_t>(RequestCount));

    auto completionThreadsStarted = NewPromise<void>();
    auto releaseCompletions = NewPromise<void>();
    std::atomic<int> startedCompletionThreads = 0;
    std::vector<TFuture<void>> completionFutures;
    completionFutures.reserve(RequestCount);
    for (auto& responsePromise : responsePromises) {
        completionFutures.push_back(BIND([
            responsePromise = std::move(responsePromise),
            completionThreadsStarted,
            releaseCompletions,
            &startedCompletionThreads] {
            if (startedCompletionThreads.fetch_add(1) + 1 == CompletionThreadCount) {
                completionThreadsStarted.Set();
            }
            WaitFor(releaseCompletions.ToFuture()).ThrowOnError();
            responsePromise.Set(MakeResponse(204));
        })
                .AsyncVia(completionPool->GetInvoker())
                .Run());
    }

    WaitFor(completionThreadsStarted.ToFuture().WithTimeout(TDuration::Seconds(5))).ThrowOnError();
    releaseCompletions.Set();
    WaitFor(AllSucceeded(completionFutures).WithTimeout(TDuration::Seconds(5))).ThrowOnError();
    WaitFor(AllSucceeded(responseFutures).WithTimeout(TDuration::Seconds(5))).ThrowOnError();
    WaitFor(callbacksCompleted.ToFuture().WithTimeout(TDuration::Seconds(5))).ThrowOnError();

    {
        std::lock_guard guard(callbackMutex);
        ASSERT_EQ(completionErrors.size(), static_cast<size_t>(RequestCount));
        ASSERT_EQ(completionThreadIds.size(), static_cast<size_t>(RequestCount));
        for (const auto& error : completionErrors) {
            EXPECT_TRUE(error.IsOK());
        }
        for (auto completionThreadId : completionThreadIds) {
            EXPECT_EQ(completionThreadId, actionQueueThreadId);
        }
    }

    completionPool->Shutdown();
    actionQueue->Shutdown();
}

TEST(TAsyncHttpRetryTest, RetainsTerminalDeliveryErrorAfterConcurrentSuccess)
{
    auto client = New<TAsyncMockClient>();
    client->Responses.push_back(MakeFuture<IResponsePtr>(MakeResponse(503)));
    client->Responses.push_back(MakeFuture<IResponsePtr>(MakeResponse(204)));
    auto parameters = MakeDynamicParameters();
    parameters->MaxAttemptCount = 1;
    auto registry = New<NProfiling::TSolomonRegistry>();
    registry->SetWindowSize(12);
    auto statusProfiler = CreateSyncStatusProfiler();
    int delayCallCount = 0;
    auto executor = New<TAsyncHttpRequestExecutor>(
        client,
        "https://example.test/post",
        New<THeaders>(),
        parameters,
        GetSyncInvoker(),
        NProfiling::TProfiler(registry, "/test"),
        statusProfiler->ErrorState("/test"),
        BIND([&delayCallCount] (TDuration) {
            ++delayCallCount;
            return OKFuture;
        }),
        BIND([] {
            return 0.0;
        }),
        BIND([] {
            return TInstant::Seconds(100);
        }));

    EXPECT_FALSE(WaitFor(executor->Post(TSharedRef::FromString(std::string("failed")))).IsOK());
    EXPECT_TRUE(WaitFor(executor->Post(TSharedRef::FromString(std::string("successful")))).IsOK());
    ASSERT_TRUE(statusProfiler->GetStatus().Errors.contains("/test"));
    EXPECT_THAT(
        ToString(statusProfiler->GetStatus().Errors.at("/test")),
        HasSubstr("Async HTTP POST retry policy exhausted"));
    EXPECT_EQ(delayCallCount, 0);

    const auto dump = CollectSensors(registry);
    EXPECT_EQ(ReadStatusCounter(dump, "yt/test/responses", "503"), 1);
    EXPECT_EQ(ReadStatusCounter(dump, "yt/test/responses", "204"), 1);
    EXPECT_EQ(ReadCounter(dump, "yt/test/attempt_failures"), 0);
}

TEST(TAsyncHttpRetryTest, DoesNotScheduleAtTotalDeadline)
{
    auto client = New<TAsyncMockClient>();
    client->Responses.push_back(MakeFuture<IResponsePtr>(MakeResponse(503)));
    auto statusProfiler = CreateSyncStatusProfiler();
    int nowCallCount = 0;
    int delayCallCount = 0;
    auto executor = New<TAsyncHttpRequestExecutor>(
        client,
        "https://example.test/post",
        New<THeaders>(),
        MakeDynamicParameters(),
        GetSyncInvoker(),
        NProfiling::TProfiler(),
        statusProfiler->ErrorState("/test"),
        BIND([&delayCallCount] (TDuration) {
            ++delayCallCount;
            return OKFuture;
        }),
        BIND([] {
            return 0.0;
        }),
        BIND([&nowCallCount] {
            return TInstant::Seconds(nowCallCount++ < 2 ? 100 : 115);
        }));

    auto result = WaitFor(executor->Post(TSharedRef::FromString(std::string("payload"))));
    EXPECT_FALSE(result.IsOK());
    EXPECT_EQ(delayCallCount, 0);
    EXPECT_EQ(client->Bodies.size(), 1u);
}

TEST(TAsyncHttpRetryTest, TimesOutStalledResponseBodyByAttemptDeadline)
{
    auto readPromise = NewPromise<TSharedRef>();
    auto client = New<TAsyncMockClient>();
    client->Responses.push_back(MakeFuture<IResponsePtr>(MakeResponse(
        201,
        {readPromise.ToFuture()})));
    client->Responses.push_back(MakeFuture<IResponsePtr>(MakeResponse(204)));
    auto parameters = MakeDynamicParameters();
    parameters->RequestTimeout = TDuration::Seconds(10);
    parameters->AttemptTimeout = TDuration::MilliSeconds(1);
    auto registry = New<NProfiling::TSolomonRegistry>();
    registry->SetWindowSize(12);
    std::vector<TDuration> delays;
    auto statusProfiler = CreateSyncStatusProfiler();
    auto executor = New<TAsyncHttpRequestExecutor>(
        client,
        "https://example.test/post",
        New<THeaders>(),
        parameters,
        GetSyncInvoker(),
        NProfiling::TProfiler(registry, "/test"),
        statusProfiler->ErrorState("/test"),
        BIND([&delays] (TDuration delay) {
            delays.push_back(delay);
            return OKFuture;
        }),
        BIND([] {
            return 0.0;
        }),
        BIND([] {
            return TInstant::Seconds(100);
        }));

    auto result = WaitFor(executor->Post(TSharedRef::FromString(std::string("payload")))
            .WithTimeout(TDuration::Seconds(1)));
    EXPECT_TRUE(result.IsOK());
    EXPECT_EQ(client->Bodies.size(), 2u);
    EXPECT_EQ(delays, std::vector{TDuration::Seconds(1)});
    const auto dump = CollectSensors(registry);
    EXPECT_EQ(ReadStatusCounter(dump, "yt/test/responses", "201"), 1);
    EXPECT_EQ(ReadStatusCounter(dump, "yt/test/responses", "204"), 1);
    EXPECT_EQ(ReadCounter(dump, "yt/test/attempt_failures"), 1);

    readPromise.Set(TSharedRef());
    EXPECT_EQ(client->Bodies.size(), 2u);
    EXPECT_EQ(delays, std::vector{TDuration::Seconds(1)});
}

TEST(TAsyncHttpRetryTest, CapsCompleteAttemptByRemainingDeadline)
{
    auto client = New<TAsyncMockClient>();
    auto readPromise = NewPromise<TSharedRef>();
    client->Responses.push_back(MakeFuture<IResponsePtr>(MakeResponse(
        201,
        {readPromise.ToFuture()})));
    auto parameters = MakeDynamicParameters();
    parameters->RequestTimeout = TDuration::Seconds(10);
    parameters->AttemptTimeout = TDuration::Seconds(10);
    parameters->MaxAttemptCount = 1;
    std::vector<TInstant> times = {
        TInstant::Seconds(100),
        TInstant::Seconds(100) + TDuration::Seconds(10) - TDuration::MicroSeconds(1),
        TInstant::Seconds(110),
    };
    int timeIndex = 0;
    std::vector<TDuration> delays;
    auto registry = New<NProfiling::TSolomonRegistry>();
    registry->SetWindowSize(12);
    auto statusProfiler = CreateSyncStatusProfiler();
    auto executor = New<TAsyncHttpRequestExecutor>(
        client,
        "https://example.test/post",
        New<THeaders>(),
        parameters,
        GetSyncInvoker(),
        NProfiling::TProfiler(registry, "/test"),
        statusProfiler->ErrorState("/test"),
        BIND([&delays] (TDuration delay) {
            delays.push_back(delay);
            return OKFuture;
        }),
        BIND([] {
            return 0.0;
        }),
        BIND([&] {
            return times.at(timeIndex++);
        }));

    auto result = WaitFor(executor->Post(TSharedRef::FromString(std::string("payload")))
            .WithTimeout(TDuration::Seconds(1)));
    EXPECT_FALSE(result.IsOK());
    EXPECT_TRUE(delays.empty());
    EXPECT_EQ(client->Bodies.size(), 1u);
    const auto dump = CollectSensors(registry);
    EXPECT_EQ(ReadStatusCounter(dump, "yt/test/responses", "201"), 1);
    EXPECT_EQ(ReadCounter(dump, "yt/test/attempt_failures"), 1);

    readPromise.Set(TSharedRef());
    EXPECT_TRUE(delays.empty());
    EXPECT_EQ(client->Bodies.size(), 1u);
}

TEST(TAsyncHttpRetryTest, RetriesResponseReadFailureAndCountsIt)
{
    auto client = New<TAsyncMockClient>();
    client->Responses.push_back(MakeFuture<IResponsePtr>(MakeResponse(
        201,
        {MakeFuture<TSharedRef>(TError("read failed"))})));
    client->Responses.push_back(MakeFuture<IResponsePtr>(MakeResponse(204)));
    auto registry = New<NProfiling::TSolomonRegistry>();
    registry->SetWindowSize(12);
    auto delay = NewPromise<void>();
    auto statusProfiler = CreateSyncStatusProfiler();
    auto executor = New<TAsyncHttpRequestExecutor>(
        client,
        "https://example.test/post",
        New<THeaders>(),
        MakeDynamicParameters(),
        GetSyncInvoker(),
        NProfiling::TProfiler(registry, "/test"),
        statusProfiler->ErrorState("/test"),
        BIND([delayFuture = delay.ToFuture()] (TDuration) {
            return delayFuture;
        }),
        BIND([] {
            return 0.0;
        }),
        BIND([] {
            return TInstant::Seconds(100);
        }));

    auto result = executor->Post(TSharedRef::FromString(std::string("payload")));
    EXPECT_FALSE(result.IsSet());
    ASSERT_TRUE(statusProfiler->GetStatus().Errors.contains("/test"));
    EXPECT_THAT(
        ToString(statusProfiler->GetStatus().Errors.at("/test")),
        HasSubstr("Failed to drain async HTTP response"));
    auto failedAttemptDump = CollectSensors(registry);
    EXPECT_EQ(ReadStatusCounter(failedAttemptDump, "yt/test/responses", "201"), 1);
    EXPECT_EQ(ReadCounter(failedAttemptDump, "yt/test/attempt_failures"), 1);

    delay.Set();
    EXPECT_TRUE(WaitFor(result).IsOK());
    EXPECT_FALSE(statusProfiler->GetStatus().Errors.contains("/test"));
    const auto recoveredDump = CollectSensors(registry);
    EXPECT_EQ(ReadStatusCounter(recoveredDump, "yt/test/responses", "201"), 0);
    EXPECT_EQ(ReadStatusCounter(recoveredDump, "yt/test/responses", "204"), 1);
    EXPECT_EQ(ReadCounter(recoveredDump, "yt/test/attempt_failures"), 0);
}

TEST(TAsyncHttpRetryTest, RetriesTransportAndTimeoutFailures)
{
    for (const auto& error : std::vector{
            TError("transport failed"),
            TError(NYT::EErrorCode::Timeout, "attempt timed out"),
         }) {
        auto client = New<TAsyncMockClient>();
        client->Responses.push_back(MakeFuture<IResponsePtr>(error));
        client->Responses.push_back(MakeFuture<IResponsePtr>(MakeResponse(204)));
        auto registry = New<NProfiling::TSolomonRegistry>();
        registry->SetWindowSize(12);
        std::vector<TDuration> delays;
        auto executor = MakeExecutor(
            client,
            New<THeaders>(),
            &delays,
            NProfiling::TProfiler(registry, "/test"));

        EXPECT_TRUE(WaitFor(executor->Post(TSharedRef::FromString(std::string("payload")))).IsOK());
        EXPECT_EQ(delays, std::vector{TDuration::Seconds(1)});
        const auto dump = CollectSensors(registry);
        EXPECT_EQ(ReadStatusCounter(dump, "yt/test/responses", "204"), 1);
        EXPECT_EQ(ReadCounter(dump, "yt/test/attempt_failures"), 1);
    }
}

TEST(TAsyncHttpRetryTest, FailsAfterThreeAttempts)
{
    auto client = New<TAsyncMockClient>();
    for (int attempt = 0; attempt < 3; ++attempt) {
        client->Responses.push_back(MakeFuture<IResponsePtr>(MakeResponse(503)));
    }
    std::vector<TDuration> delays;
    auto executor = MakeExecutor(client, New<THeaders>(), &delays);

    auto result = WaitFor(executor->Post(TSharedRef::FromString(std::string("payload"))));
    EXPECT_FALSE(result.IsOK());
    EXPECT_EQ(client->Bodies.size(), 3u);
    EXPECT_EQ(delays, std::vector({TDuration::Seconds(1), TDuration::Seconds(2)}));
}

TEST(TAsyncHttpRetryTest, RetainsPendingRequestDependencies)
{
    auto readPromise = NewPromise<TSharedRef>();
    auto response = MakeResponse(204, {readPromise.ToFuture()});
    auto client = New<TAsyncMockClient>();
    client->Responses.push_back(MakeFuture<IResponsePtr>(response));
    auto headers = New<THeaders>();
    auto body = TSharedRef::FromString(std::string("payload"));
    auto weakClient = MakeWeak(client);
    auto weakHeaders = MakeWeak(headers);
    auto weakResponse = MakeWeak(response);
    auto weakBodyHolder = MakeWeak(body.GetHolder());
    std::vector<TDuration> delays;
    auto executor = MakeExecutor(client, headers, &delays);

    auto result = executor->Post(body);
    client.Reset();
    headers.Reset();
    response.Reset();
    body.Reset();
    EXPECT_TRUE(weakClient.Lock());
    EXPECT_TRUE(weakHeaders.Lock());
    EXPECT_TRUE(weakResponse.Lock());
    EXPECT_TRUE(weakBodyHolder.Lock());

    readPromise.Set(TSharedRef());
    EXPECT_TRUE(WaitFor(result).IsOK());
    EXPECT_FALSE(weakResponse.Lock());
    executor.Reset();
    EXPECT_FALSE(weakClient.Lock());
    EXPECT_FALSE(weakHeaders.Lock());
    EXPECT_FALSE(weakBodyHolder.Lock());
}

TEST(TAsyncHttpRetryTest, WaitsForPromisedDelayBeforeNextAttempt)
{
    auto client = New<TAsyncMockClient>();
    client->Responses.push_back(MakeFuture<IResponsePtr>(MakeResponse(503)));
    client->Responses.push_back(MakeFuture<IResponsePtr>(MakeResponse(204)));
    auto delayPromise = NewPromise<void>();
    auto statusProfiler = CreateSyncStatusProfiler();
    auto executor = New<TAsyncHttpRequestExecutor>(
        client,
        "https://example.test/post",
        New<THeaders>(),
        MakeDynamicParameters(),
        GetSyncInvoker(),
        NProfiling::TProfiler(),
        statusProfiler->ErrorState("/test"),
        BIND([delayFuture = delayPromise.ToFuture()] (TDuration) {
            return delayFuture;
        }),
        BIND([] {
            return 0.0;
        }),
        BIND([] {
            return TInstant::Seconds(100);
        }));

    auto result = executor->Post(TSharedRef::FromString(std::string("payload")));
    EXPECT_EQ(client->Bodies.size(), 1u);
    EXPECT_FALSE(result.IsSet());
    delayPromise.Set();
    EXPECT_TRUE(WaitFor(result).IsOK());
    EXPECT_EQ(client->Bodies.size(), 2u);
}

TEST(TAsyncHttpRetryTest, PropagatesCanceledDelay)
{
    auto client = New<TAsyncMockClient>();
    client->Responses.push_back(MakeFuture<IResponsePtr>(MakeResponse(503)));
    auto statusProfiler = CreateSyncStatusProfiler();
    auto executor = New<TAsyncHttpRequestExecutor>(
        client,
        "https://example.test/post",
        New<THeaders>(),
        MakeDynamicParameters(),
        GetSyncInvoker(),
        NProfiling::TProfiler(),
        statusProfiler->ErrorState("/test"),
        BIND([] (TDuration) {
            return MakeFuture<void>(TError("delay canceled"));
        }),
        BIND([] {
            return 0.0;
        }),
        BIND([] {
            return TInstant::Seconds(100);
        }));

    auto result = WaitFor(executor->Post(TSharedRef::FromString(std::string("payload"))));
    EXPECT_FALSE(result.IsOK());
    EXPECT_EQ(client->Bodies.size(), 1u);
}

} // namespace
} // namespace NYT::NFlow
