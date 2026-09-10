#include <yt/yt/flow/extensions/http/sink.h>

#include <yt/yt/flow/library/cpp/common/distributing_tracker.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>
#include <yt/yt/flow/library/cpp/common/unittests/mock/state.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>
#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/config.h>
#include <yt/yt/core/http/http.h>
#include <yt/yt/core/http/server.h>
#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/testing/common/network.h>

#include <util/network/socket.h>

#include <array>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>

#include <arpa/inet.h>
#include <sys/socket.h>

namespace NYT::NFlow {
namespace {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NYTree;

const TStreamId StreamId("input");
const auto CompletionTimeout = TDuration::Seconds(10);

NHttp::IClientPtr CreateTestHttpClient(
    const TAsyncHttpSinkParameters& parameters,
    const NConcurrency::IPollerPtr& poller)
{
    auto config = New<NHttp::TClientConfig>();
    config->MaxRedirectCount = 0;
    config->MaxIdleConnections = parameters.KeepAlive ? parameters.MaxIdleConnections : 0;
    config->OmitQuestionMarkForEmptyQuery = true;
    return NHttp::CreateClient(std::move(config), poller);
}

TComputationStreamSpecStoragePtr MakeStreamSpecStorage(const TTableSchemaPtr& schema)
{
    auto streamSpec = New<TStreamSpec>();
    streamSpec->Schema = schema;
    THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> specs;
    specs[StreamId][TStreamSpecId(1)] = streamSpec;
    return New<TComputationStreamSpecStorage>(
        New<TStreamSpecs>(specs),
        New<TTableSchema>(),
        nullptr);
}

std::pair<TSinkContextPtr, TDynamicSinkContextPtr> MakeContexts(
    TTableSchemaPtr schema,
    const NConcurrency::IPollerPtr& poller = nullptr,
    std::string payloadColumn = "payload",
    std::string url = "http://localhost:1/post")
{
    auto spec = New<TSinkSpec>();
    spec->SinkClassName = TypeName<TAsyncHttpSink>();
    spec->InputStreamIds = {StreamId};
    spec->Parameters->AddChild("url", ConvertToNode(url));
    spec->Parameters->AddChild("payload_column", ConvertToNode(payloadColumn));

    auto dynamicSpec = New<TDynamicSinkSpec>();
    auto context = New<TSinkContext>();
    context->SinkSpec = std::move(spec);
    context->StreamSpecStorage = MakeStreamSpecStorage(schema);
    context->SerializedInvoker = GetSyncInvoker();
    context->StatusProfiler = CreateSyncStatusProfiler();
    context->Poller = poller;

    auto dynamicContext = New<TDynamicSinkContext>();
    dynamicContext->DynamicSinkSpec = std::move(dynamicSpec);
    return {std::move(context), std::move(dynamicContext)};
}

TOutputMessagePtr MakeMessage(
    const TTableSchemaPtr& schema,
    const TComputationStreamSpecStoragePtr& streamSpecStorage,
    TStringBuf messageId,
    TStringBuf payload,
    TStringBuf payloadColumn = "payload")
{
    TMessageBuilder builder(StreamId, schema);
    builder.SetMessageId(TMessageId(messageId));
    builder.SetSystemTimestamp(TSystemTimestamp(100));
    builder.SetAlignmentTimestamp(TSystemTimestamp(100));
    builder.SetEventTimestamp(TSystemTimestamp(100));
    builder.Payload().Set(payload, payloadColumn);
    return New<TOutputMessage>(builder.Finish(), streamSpecStorage);
}

class TNoContentHandler
    : public NHttp::IHttpHandler
{
public:
    void HandleRequest(
        const NHttp::IRequestPtr& request,
        const NHttp::IResponseWriterPtr& response) override
    {
        std::string body;
        while (true) {
            auto chunk = WaitFor(request->Read()).ValueOrThrow();
            if (chunk.Empty()) {
                break;
            }
            body += ToString(chunk);
        }

        response->SetStatus(NHttp::EStatusCode::NoContent);
        WaitFor(response->Close()).ThrowOnError();

        {
            std::lock_guard guard(Mutex_);
            Bodies_.push_back(std::move(body));
            Headers_.push_back(request->GetHeaders()->Duplicate());
        }
        ConditionVariable_.notify_all();
    }

    void WaitForRequestCount(int requestCount)
    {
        std::unique_lock lock(Mutex_);
        ConditionVariable_.wait(lock, [&] {
            return std::ssize(Bodies_) >= requestCount;
        });
    }

    std::vector<std::string> GetBodies() const
    {
        std::lock_guard guard(Mutex_);
        return Bodies_;
    }

    std::vector<std::string> GetHeaderValues(TStringBuf name) const
    {
        std::lock_guard guard(Mutex_);
        std::vector<std::string> values;
        values.reserve(Headers_.size());
        for (const auto& headers : Headers_) {
            const auto* value = headers->Find(name);
            values.push_back(value ? *value : std::string());
        }
        return values;
    }

private:
    mutable std::mutex Mutex_;
    std::condition_variable ConditionVariable_;
    std::vector<std::string> Bodies_;
    std::vector<NHttp::THeadersPtr> Headers_;
};

DEFINE_REFCOUNTED_TYPE(TNoContentHandler);

class TRawRequestTargetServer
{
public:
    explicit TRawRequestTargetServer(ui16 port)
        : Listener_(::socket(AF_INET, SOCK_STREAM, 0))
    {
        if (Listener_ == INVALID_SOCKET) {
            ythrow TSystemError() << "Failed to create raw request target test socket";
        }

        int reuseAddress = 1;
        if (::setsockopt(Listener_, SOL_SOCKET, SO_REUSEADDR, &reuseAddress, sizeof(reuseAddress)) != 0) {
            ythrow TSystemError() << "Failed to configure raw request target test socket";
        }

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = htons(port);
        if (::bind(Listener_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
            ythrow TSystemError() << "Failed to bind raw request target test socket";
        }
        if (::listen(Listener_, 1) != 0) {
            ythrow TSystemError() << "Failed to listen on raw request target test socket";
        }

        Worker_ = std::thread([this] {
            Serve();
        });
    }

    ~TRawRequestTargetServer()
    {
        Worker_.join();
    }

    std::string WaitForRequestLine()
    {
        std::unique_lock lock(Mutex_);
        ConditionVariable_.wait(lock, [&] {
            return RequestLine_.has_value();
        });
        return *RequestLine_;
    }

private:
    void Serve()
    {
        TSocketHolder connection(::accept(Listener_, nullptr, nullptr));
        std::string request;
        std::array<char, 1024> buffer{};
        while (request.find("\r\n") == std::string::npos) {
            auto readCount = ::recv(connection, buffer.data(), buffer.size(), 0);
            if (readCount <= 0) {
                break;
            }
            request.append(buffer.data(), readCount);
        }

        auto lineEnd = request.find("\r\n");
        {
            std::lock_guard guard(Mutex_);
            RequestLine_ = request.substr(0, lineEnd);
        }
        ConditionVariable_.notify_all();

        static constexpr TStringBuf response = "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        ::send(connection, response.data(), response.size(), 0);
    }

    TSocketHolder Listener_;
    std::thread Worker_;
    std::mutex Mutex_;
    std::condition_variable ConditionVariable_;
    std::optional<std::string> RequestLine_;
};

class TControlledCompletionHandler
    : public NHttp::IHttpHandler
{
public:
    explicit TControlledCompletionHandler(NHttp::EStatusCode firstStatus)
        : FirstStatus_(firstStatus)
    { }

    void HandleRequest(
        const NHttp::IRequestPtr& request,
        const NHttp::IResponseWriterPtr& response) override
    {
        std::string body;
        while (true) {
            auto chunk = WaitFor(request->Read()).ValueOrThrow();
            if (chunk.Empty()) {
                break;
            }
            body += ToString(chunk);
        }

        if (body == "first") {
            std::unique_lock lock(Mutex_);
            FirstRequestReceived_ = true;
            ConditionVariable_.notify_all();
            ConditionVariable_.wait(lock, [&] {
                return ReleaseFirstResponse_;
            });
        }

        response->SetStatus(body == "first" ? FirstStatus_ : NHttp::EStatusCode::NoContent);
        WaitFor(response->Close()).ThrowOnError();

        if (body == "second") {
            std::lock_guard guard(Mutex_);
            SecondResponseClosed_ = true;
            ConditionVariable_.notify_all();
        }

        {
            std::lock_guard guard(Mutex_);
            ++RequestCount_;
        }
        ConditionVariable_.notify_all();
    }

    void WaitForFirstRequest()
    {
        std::unique_lock lock(Mutex_);
        ConditionVariable_.wait(lock, [&] {
            return FirstRequestReceived_;
        });
    }

    void WaitForSecondResponse()
    {
        std::unique_lock lock(Mutex_);
        ConditionVariable_.wait(lock, [&] {
            return SecondResponseClosed_;
        });
    }

    void WaitForRequestCount(int requestCount)
    {
        std::unique_lock lock(Mutex_);
        ConditionVariable_.wait(lock, [&] {
            return RequestCount_ >= requestCount;
        });
    }

    int GetRequestCount()
    {
        std::lock_guard guard(Mutex_);
        return RequestCount_;
    }

    void ReleaseFirstResponse()
    {
        {
            std::lock_guard guard(Mutex_);
            ReleaseFirstResponse_ = true;
        }
        ConditionVariable_.notify_all();
    }

private:
    const NHttp::EStatusCode FirstStatus_;
    std::mutex Mutex_;
    std::condition_variable ConditionVariable_;
    bool FirstRequestReceived_ = false;
    bool SecondResponseClosed_ = false;
    bool ReleaseFirstResponse_ = false;
    int RequestCount_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TControlledCompletionHandler);

std::shared_ptr<std::atomic<bool>> DistributeWithFlag(
    const TAsyncHttpSinkPtr& sink,
    const TOutputMessageConstPtr& message)
{
    auto fired = std::make_shared<std::atomic<bool>>(false);
    TDistributingTracker tracker([fired] {
        fired->store(true);
    });
    sink->Distribute(message, tracker.AddDestination());
    tracker.Activate();
    return fired;
}

void RunEpochsUntilFired(
    const TAsyncHttpSinkPtr& sink,
    const TStateManagerMockPtr& stateManager,
    const std::shared_ptr<std::atomic<bool>>& fired)
{
    const auto deadline = TInstant::Now() + CompletionTimeout;
    while (!fired->load() && TInstant::Now() < deadline) {
        sink->Sync(nullptr);
        stateManager->Sync();
        sink->Commit();
        std::this_thread::yield();
    }
    EXPECT_TRUE(fired->load());
}

void RunEpochs(
    const TAsyncHttpSinkPtr& sink,
    const TStateManagerMockPtr& stateManager,
    int iterationCount = 10000)
{
    for (int iteration = 0; iteration < iterationCount; ++iteration) {
        sink->Sync(nullptr);
        stateManager->Sync();
        sink->Commit();
        std::this_thread::yield();
    }
}

TEST(TAsyncHttpOrderedStateTest, ValidatesInputSchema)
{
    for (const auto& [schema, payloadColumn] : std::vector<std::pair<TTableSchemaPtr, std::string>>{
            {New<TTableSchema>(), "payload"},
            {New<TTableSchema>(std::vector{
                 TColumnSchema("payload", EValueType::String),
                 TColumnSchema("extra", EValueType::String),
             }),
                "payload"},
            {New<TTableSchema>(std::vector{TColumnSchema("payload", EValueType::String)}), "other"},
            {New<TTableSchema>(std::vector{TColumnSchema("payload", EValueType::Int64)}), "payload"},
            {New<TTableSchema>(std::vector{TColumnSchema("payload", EValueType::Any)}), "payload"},
         }) {
        auto [context, dynamicContext] = MakeContexts(schema, nullptr, payloadColumn);
        EXPECT_THROW(New<TAsyncHttpSink>(context, dynamicContext), std::exception);
    }

    auto poller = CreateThreadPoolPoller(1, "AsyncHttpSinkTest");
    auto [context, dynamicContext] = MakeContexts(
        New<TTableSchema>(std::vector{TColumnSchema("payload", EValueType::String)}),
        poller);
    EXPECT_NO_THROW(New<TAsyncHttpSink>(context, dynamicContext));
    poller->Shutdown();
}

TEST(TAsyncHttpOrderedStateTest, RuntimeNullIsSkippedAndPersistedInOrder)
{
    auto schema = New<TTableSchema>(std::vector{TColumnSchema("payload", EValueType::String)});
    auto poller = CreateThreadPoolPoller(2, "AsyncHttpSinkNullTest");
    auto port = NTesting::GetFreePort();
    auto serverConfig = New<NHttp::TServerConfig>();
    serverConfig->Port = port;
    auto server = NHttp::CreateServer(serverConfig, poller);
    auto handler = New<TNoContentHandler>();
    server->AddHandler("/post", handler);
    server->Start();
    auto [context, dynamicContext] = MakeContexts(
        schema,
        poller,
        "payload",
        Format("http://localhost:%v/post", port));
    auto sink = New<TAsyncHttpSink>(context, dynamicContext);
    auto stateManager = New<TStateManagerMock>();
    sink->Init(stateManager->CreateContext());

    TMessageBuilder builder(StreamId, schema);
    builder.SetMessageId(TMessageId("message-0"));
    builder.SetSystemTimestamp(TSystemTimestamp(100));
    builder.SetAlignmentTimestamp(TSystemTimestamp(100));
    builder.SetEventTimestamp(TSystemTimestamp(100));
    auto nullMessage = New<TOutputMessage>(builder.Finish(), context->StreamSpecStorage);
    auto validMessage = MakeMessage(schema, context->StreamSpecStorage, "message-1", "valid");
    auto nullFired = DistributeWithFlag(sink, nullMessage);
    auto validFired = DistributeWithFlag(sink, validMessage);

    sink->Sync(nullptr);
    stateManager->Sync();
    sink->Commit();
    handler->WaitForRequestCount(1);
    RunEpochsUntilFired(sink, stateManager, nullFired);
    RunEpochsUntilFired(sink, stateManager, validFired);

    sink.Reset();

    auto recreatedSink = New<TAsyncHttpSink>(context, dynamicContext);
    recreatedSink->Init(stateManager->CreateContext());
    auto replayedNullFired = DistributeWithFlag(recreatedSink, nullMessage);
    auto replayedValidFired = DistributeWithFlag(recreatedSink, validMessage);
    RunEpochsUntilFired(recreatedSink, stateManager, replayedNullFired);
    RunEpochsUntilFired(recreatedSink, stateManager, replayedValidFired);
    EXPECT_EQ(handler->GetBodies(), std::vector<std::string>({"valid"}));

    recreatedSink.Reset();
    server->Stop();
    poller->Shutdown();
}

TEST(TAsyncHttpOrderedStateTest, AddsConfiguredMessageIdHeader)
{
    auto schema = New<TTableSchema>(std::vector{TColumnSchema("payload", EValueType::String)});
    auto poller = CreateThreadPoolPoller(2, "AsyncHttpSinkHeaderTest");
    auto port = NTesting::GetFreePort();
    auto serverConfig = New<NHttp::TServerConfig>();
    serverConfig->Port = port;
    auto server = NHttp::CreateServer(serverConfig, poller);
    auto handler = New<TNoContentHandler>();
    server->AddHandler("/post", handler);
    server->Start();
    auto [context, dynamicContext] = MakeContexts(
        schema,
        poller,
        "payload",
        Format("http://localhost:%v/post", port));
    context->SinkSpec->Parameters->AddChild(
        "idempotency_header",
        ConvertToNode("X-Flow-Message-Id"));
    auto sink = New<TAsyncHttpSink>(context, dynamicContext);
    auto stateManager = New<TStateManagerMock>();
    sink->Init(stateManager->CreateContext());

    const std::string messageId("message\n\0", 9);
    auto message = MakeMessage(schema, context->StreamSpecStorage, messageId, "payload");
    auto fired = DistributeWithFlag(sink, message);
    sink->Sync(nullptr);
    stateManager->Sync();
    sink->Commit();
    handler->WaitForRequestCount(1);
    RunEpochsUntilFired(sink, stateManager, fired);

    EXPECT_EQ(
        handler->GetHeaderValues("X-Flow-Message-Id"),
        std::vector<std::string>{"6D6573736167650A00"});

    sink.Reset();
    server->Stop();
    poller->Shutdown();
}

TEST(TAsyncHttpOrderedStateTest, SendsEmptyQueryWithoutQuestionMarkOnTheWire)
{
    auto poller = CreateThreadPoolPoller(1, "AsyncHttpSinkRawTargetTest");
    auto port = NTesting::GetFreePort();
    TRawRequestTargetServer server(port);
    auto parameters = New<TAsyncHttpSinkParameters>();
    parameters->Url = Format("http://127.0.0.1:%v/post", port);
    auto client = CreateTestHttpClient(*parameters, poller);

    auto response = WaitFor(client->Post(
        parameters->Url,
        TSharedRef::FromString(std::string("payload")),
        New<NHttp::THeaders>()))
        .ValueOrThrow();
    EXPECT_EQ(response->GetStatusCode(), NHttp::EStatusCode::NoContent);
    EXPECT_EQ(server.WaitForRequestLine(), "POST /post HTTP/1.1");

    poller->Shutdown();
}

TEST(TAsyncHttpOrderedStateTest, PersistsOnlyAfterRemoteSuccessAndRepostsBeforePersistence)
{
    auto schema = New<TTableSchema>(std::vector{TColumnSchema("payload", EValueType::String)});
    auto poller = CreateThreadPoolPoller(2, "AsyncHttpSinkTest");
    auto port = NTesting::GetFreePort();
    auto serverConfig = New<NHttp::TServerConfig>();
    serverConfig->Port = port;
    auto server = NHttp::CreateServer(serverConfig, poller);
    auto handler = New<TNoContentHandler>();
    server->AddHandler("/post", handler);
    server->Start();
    auto url = Format("http://localhost:%v/post", port);

    auto persistedStateManager = New<TStateManagerMock>();
    auto [persistedContext, persistedDynamicContext] = MakeContexts(schema, poller, "payload", url);
    auto persistedMessage = MakeMessage(
        schema,
        persistedContext->StreamSpecStorage,
        "persisted-message",
        "persisted-payload");
    auto persistedSink = New<TAsyncHttpSink>(persistedContext, persistedDynamicContext);
    persistedSink->Init(persistedStateManager->CreateContext());
    auto persistedFired = DistributeWithFlag(persistedSink, persistedMessage);
    persistedSink->Sync(nullptr);
    persistedStateManager->Sync();
    persistedSink->Commit();
    handler->WaitForRequestCount(1);
    EXPECT_FALSE(persistedFired->load());
    RunEpochsUntilFired(persistedSink, persistedStateManager, persistedFired);
    persistedSink.Reset();

    auto persistedRecreatedSink = New<TAsyncHttpSink>(persistedContext, persistedDynamicContext);
    persistedRecreatedSink->Init(persistedStateManager->CreateContext());
    auto recreatedPersistedFired = DistributeWithFlag(persistedRecreatedSink, persistedMessage);
    EXPECT_TRUE(recreatedPersistedFired->load());
    EXPECT_EQ(handler->GetBodies(), std::vector<std::string>{"persisted-payload"});
    persistedRecreatedSink.Reset();

    auto replayedStateManager = New<TStateManagerMock>();
    auto [replayedContext, replayedDynamicContext] = MakeContexts(schema, poller, "payload", url);
    auto replayedMessage = MakeMessage(
        schema,
        replayedContext->StreamSpecStorage,
        "replayed-message",
        "replayed-payload");
    auto failedSink = New<TAsyncHttpSink>(replayedContext, replayedDynamicContext);
    failedSink->Init(replayedStateManager->CreateContext());
    auto failedFired = DistributeWithFlag(failedSink, replayedMessage);
    failedSink->Sync(nullptr);
    replayedStateManager->Sync();
    failedSink->Commit();
    handler->WaitForRequestCount(2);
    EXPECT_FALSE(failedFired->load());
    failedSink.Reset();

    auto replayedSink = New<TAsyncHttpSink>(replayedContext, replayedDynamicContext);
    replayedSink->Init(replayedStateManager->CreateContext());
    auto replayedFired = DistributeWithFlag(replayedSink, replayedMessage);
    replayedSink->Sync(nullptr);
    replayedStateManager->Sync();
    replayedSink->Commit();
    handler->WaitForRequestCount(3);
    EXPECT_FALSE(replayedFired->load());
    RunEpochsUntilFired(replayedSink, replayedStateManager, replayedFired);
    const std::vector<std::string> expectedBodies = {
        "persisted-payload",
        "replayed-payload",
        "replayed-payload",
    };
    EXPECT_EQ(handler->GetBodies(), expectedBodies);

    replayedSink.Reset();
    server->Stop();
    poller->Shutdown();
}

TEST(TAsyncHttpOrderedStateTest, PersistsConcurrentCompletionsInMessageOrder)
{
    auto schema = New<TTableSchema>(std::vector{TColumnSchema("payload", EValueType::String)});
    auto poller = CreateThreadPoolPoller(4, "AsyncHttpSinkOrderTest");
    auto port = NTesting::GetFreePort();
    auto serverConfig = New<NHttp::TServerConfig>();
    serverConfig->Port = port;
    auto server = NHttp::CreateServer(serverConfig, poller);
    auto handler = New<TControlledCompletionHandler>(NHttp::EStatusCode::NoContent);
    server->AddHandler("/post", handler);
    server->Start();

    auto stateManager = New<TStateManagerMock>();
    auto [context, dynamicContext] = MakeContexts(
        schema,
        poller,
        "payload",
        Format("http://localhost:%v/post", port));
    auto sink = New<TAsyncHttpSink>(context, dynamicContext);
    sink->Init(stateManager->CreateContext());
    auto firstMessage = MakeMessage(schema, context->StreamSpecStorage, "message-0", "first");
    auto secondMessage = MakeMessage(schema, context->StreamSpecStorage, "message-1", "second");
    auto firstFired = DistributeWithFlag(sink, firstMessage);
    auto secondFired = DistributeWithFlag(sink, secondMessage);

    sink->Sync(nullptr);
    stateManager->Sync();
    sink->Commit();
    handler->WaitForFirstRequest();
    handler->WaitForSecondResponse();
    RunEpochs(sink, stateManager);

    if (secondFired->load()) {
        ADD_FAILURE() << "Later delivery was persisted before the pending head";
        handler->ReleaseFirstResponse();
        sink.Reset();
        server->Stop();
        poller->Shutdown();
        return;
    }

    handler->ReleaseFirstResponse();
    RunEpochsUntilFired(sink, stateManager, firstFired);
    RunEpochsUntilFired(sink, stateManager, secondFired);
    sink.Reset();

    auto recreatedSink = New<TAsyncHttpSink>(context, dynamicContext);
    recreatedSink->Init(stateManager->CreateContext());
    EXPECT_TRUE(DistributeWithFlag(recreatedSink, firstMessage)->load());
    EXPECT_TRUE(DistributeWithFlag(recreatedSink, secondMessage)->load());

    recreatedSink.Reset();
    server->Stop();
    poller->Shutdown();
}

TEST(TAsyncHttpOrderedStateTest, TerminalHeadFailureDoesNotPostLaterMessages)
{
    auto schema = New<TTableSchema>(std::vector{TColumnSchema("payload", EValueType::String)});
    auto poller = CreateThreadPoolPoller(4, "AsyncHttpSinkFailureOrderTest");
    auto port = NTesting::GetFreePort();
    auto serverConfig = New<NHttp::TServerConfig>();
    serverConfig->Port = port;
    auto server = NHttp::CreateServer(serverConfig, poller);
    auto handler = New<TControlledCompletionHandler>(NHttp::EStatusCode::ServiceUnavailable);
    server->AddHandler("/post", handler);
    server->Start();

    auto stateManager = New<TStateManagerMock>();
    auto [context, dynamicContext] = MakeContexts(
        schema,
        poller,
        "payload",
        Format("http://localhost:%v/post", port));
    dynamicContext->DynamicSinkSpec->Parameters->AddChild("max_attempt_count", ConvertToNode(1));
    auto sink = New<TAsyncHttpSink>(context, dynamicContext);
    sink->Init(stateManager->CreateContext());
    auto firstFired = DistributeWithFlag(
        sink,
        MakeMessage(schema, context->StreamSpecStorage, "message-0", "first"));
    auto secondFired = DistributeWithFlag(
        sink,
        MakeMessage(schema, context->StreamSpecStorage, "message-1", "second"));

    sink->Sync(nullptr);
    stateManager->Sync();
    sink->Commit();
    handler->WaitForFirstRequest();
    handler->WaitForSecondResponse();
    RunEpochs(sink, stateManager);
    EXPECT_FALSE(firstFired->load());
    EXPECT_FALSE(secondFired->load());

    handler->ReleaseFirstResponse();
    RunEpochs(sink, stateManager);
    EXPECT_FALSE(firstFired->load());
    EXPECT_FALSE(secondFired->load());

    auto thirdFired = DistributeWithFlag(
        sink,
        MakeMessage(schema, context->StreamSpecStorage, "message-2", "third"));
    sink->Sync(nullptr);
    stateManager->Sync();
    sink->Commit();
    RunEpochs(sink, stateManager);
    EXPECT_FALSE(thirdFired->load());
    EXPECT_EQ(handler->GetRequestCount(), 2);

    sink.Reset();
    server->Stop();
    poller->Shutdown();
}

TEST(TAsyncHttpOrderedStateTest, CompletesHighCardinalityDelayedHeadIterativelyInMessageOrder)
{
    constexpr int MessageCount = 4096;
    auto schema = New<TTableSchema>(std::vector{TColumnSchema("payload", EValueType::String)});
    auto poller = CreateThreadPoolPoller(4, "AsyncHttpSinkHighCardinalityOrderTest");
    auto port = NTesting::GetFreePort();
    auto serverConfig = New<NHttp::TServerConfig>();
    serverConfig->Port = port;
    auto server = NHttp::CreateServer(serverConfig, poller);
    auto handler = New<TControlledCompletionHandler>(NHttp::EStatusCode::NoContent);
    server->AddHandler("/post", handler);
    server->Start();

    auto stateManager = New<TStateManagerMock>();
    auto [context, dynamicContext] = MakeContexts(
        schema,
        poller,
        "payload",
        Format("http://localhost:%v/post", port));
    auto sink = New<TAsyncHttpSink>(context, dynamicContext);
    sink->Init(stateManager->CreateContext());

    std::vector<std::shared_ptr<std::atomic<bool>>> fired;
    fired.reserve(MessageCount);
    std::vector<int> completionOrder;
    std::mutex completionOrderMutex;
    for (int index = 0; index < MessageCount; ++index) {
        auto completion = std::make_shared<std::atomic<bool>>(false);
        TDistributingTracker tracker([&, completion, index] {
            completion->store(true);
            std::lock_guard guard(completionOrderMutex);
            completionOrder.push_back(index);
        });
        sink->Distribute(
            MakeMessage(
                schema,
                context->StreamSpecStorage,
                Format("message-%08d", index),
                index == 0 ? "first" : Format("message-%08d", index)),
            tracker.AddDestination());
        tracker.Activate();
        fired.push_back(std::move(completion));
    }

    sink->Sync(nullptr);
    stateManager->Sync();
    sink->Commit();
    handler->WaitForFirstRequest();
    handler->WaitForRequestCount(MessageCount - 1);
    RunEpochs(sink, stateManager, 100);
    for (const auto& completion : fired) {
        EXPECT_FALSE(completion->load());
    }

    handler->ReleaseFirstResponse();
    handler->WaitForRequestCount(MessageCount);
    RunEpochsUntilFired(sink, stateManager, fired.back());
    for (const auto& completion : fired) {
        EXPECT_TRUE(completion->load());
    }
    ASSERT_EQ(completionOrder.size(), MessageCount);
    for (int index = 0; index < MessageCount; ++index) {
        EXPECT_EQ(completionOrder[index], index);
    }

    sink.Reset();
    server->Stop();
    poller->Shutdown();
}

TEST(TAsyncHttpOrderedStateTest, PreservesRepresentativePayloadBytesThroughConfiguredColumn)
{
    const std::string payloadColumn = "wire_payload";
    auto schema = New<TTableSchema>(std::vector{TColumnSchema(payloadColumn, EValueType::String)});
    auto poller = CreateThreadPoolPoller(2, "AsyncHttpSinkPayloadTest");
    auto port = NTesting::GetFreePort();
    auto serverConfig = New<NHttp::TServerConfig>();
    serverConfig->Port = port;
    auto server = NHttp::CreateServer(serverConfig, poller);
    auto handler = New<TNoContentHandler>();
    server->AddHandler("/post", handler);
    server->Start();

    auto stateManager = New<TStateManagerMock>();
    auto [context, dynamicContext] = MakeContexts(
        schema,
        poller,
        payloadColumn,
        Format("http://localhost:%v/post", port));
    auto sink = New<TAsyncHttpSink>(context, dynamicContext);
    sink->Init(stateManager->CreateContext());

    const std::vector<std::string> payloads = {
        std::string("\x0a\x03"
                    "abc",
            5),
        R"({kind="yson";})",
        R"({"kind":"json"})",
        std::string("\x00\xffopaque", 8),
    };
    for (int index = 0; index < std::ssize(payloads); ++index) {
        auto message = MakeMessage(
            schema,
            context->StreamSpecStorage,
            Format("message-%v", index),
            payloads[index],
            payloadColumn);
        auto fired = DistributeWithFlag(sink, message);
        sink->Sync(nullptr);
        stateManager->Sync();
        sink->Commit();
        handler->WaitForRequestCount(index + 1);
        EXPECT_FALSE(fired->load());
        RunEpochsUntilFired(sink, stateManager, fired);
    }

    EXPECT_EQ(handler->GetBodies(), payloads);

    sink.Reset();
    server->Stop();
    poller->Shutdown();
}

} // namespace
} // namespace NYT::NFlow
