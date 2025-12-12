#include <yt/yt/tests/cpp/test_base/api_test_base.h>
#include <yt/yt/tests/cpp/test_base/private.h>

#include <yt/yt/server/lib/cross_cluster_replicated_state/config.h>
#include <yt/yt/server/lib/cross_cluster_replicated_state/cross_cluster_client.h>
#include <yt/yt/server/lib/cross_cluster_replicated_state/cross_cluster_client_detail.h>
#include <yt/yt/server/lib/cross_cluster_replicated_state/cross_cluster_replicated_value.h>
#include <yt/yt/server/lib/cross_cluster_replicated_state/cross_cluster_replicated_state.h>
#include <yt/yt/server/lib/cross_cluster_replicated_state/cross_cluster_replica_lock_waiter.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/hive/cluster_directory.h>
#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/core/ytree/ypath_client.h>

#include <library/cpp/iterator/enumerate.h>

namespace NYT::NCppTests {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NCrossClusterReplicatedState;
using namespace NYson;
using namespace NYTree;
using namespace std::literals;

////////////////////////////////////////////////////////////////////////////////

constexpr auto Logger = CppTestsLogger;

class TCrossClusterManualCallbackExecutor
    : public TRefCounted
{
public:
    explicit TCrossClusterManualCallbackExecutor(std::size_t clientCount)
        : CallbackQueues_(clientCount)
    { }

    IMultiClusterClientPtr CreateClient(
        const NNative::IConnectionPtr& connection,
        const TCrossClusterReplicatedStateConfigPtr& config,
        const NNative::TClientOptions& options)
    {
        return New<TManualMultiClusterClient>(connection, config, options, *this);
    }

    std::optional<TFuture<void>> ExecuteNext(std::size_t clientIndex, TDuration timeout = TDuration::Seconds(3))
    {
        auto start = TInstant::Now();
        while (TInstant::Now() < start + timeout) {
            if (!CallbackQueues_[clientIndex].empty()) {
                break;
            }
            Sleep(TDuration::MilliSeconds(100));
        }
        if (TInstant::Now() >= start + timeout) {
            return std::nullopt;
        }

        return ExecuteCallback(CallbackQueues_[clientIndex].begin(), clientIndex);
    }

    std::optional<TFuture<void>> ExecuteNextWithTag(const std::string& callbackTag, std::size_t clientIndex, TDuration timeout = TDuration::Seconds(3))
    {
        std::optional<TCallbackQueue::iterator> callbackIt;
        auto start = TInstant::Now();
        while (TInstant::Now() < start + timeout) {
            auto& queue = CallbackQueues_[clientIndex];
            for (auto it = queue.begin(); it != queue.end(); ++it) {
                if (std::get<2>(*it) == callbackTag) {
                    callbackIt = it;
                    break;
                }
            }
            if (callbackIt) {
                break;
            }
            Sleep(TDuration::MilliSeconds(100));
        }

        if (!callbackIt) {
            return std::nullopt;
        }

        return ExecuteCallback(*callbackIt, clientIndex);
    }

private:
    using TCallbackQueue = std::list<std::tuple<
        TCallback<TFuture<std::any>()>,
        TPromise<std::any>,
        std::string,
        std::size_t>>;
    std::vector<TCallbackQueue> CallbackQueues_;
    TPromise<void> ParentCompletionHandler_;
    std::size_t CallbackIndex_{0};

    class TManualSingleClusterClient
        : public ISingleClusterClient
    {
    public:
        TManualSingleClusterClient(
            int index,
            IClientBasePtr client,
            TCrossClusterManualCallbackExecutor& executor)
            : Client_(std::move(client))
            , Index_(index)
            , Executor_(&executor)
        { }

        const NApi::IClientBasePtr& GetClient() override
        {
            return Client_;
        }

        int GetIndex() const override
        {
            return Index_;
        }

        TFuture<std::any> DoExecuteCallback(
            std::string tag,
            TCallback<TFuture<std::any>(const ISingleClusterClientPtr&)> callback) override
        {
            auto result = Executor_->AddCallback(
                std::move(tag),
                MakeStrong(this),
                std::move(callback));
            return result;
        }

    private:
        IClientBasePtr Client_;
        int Index_;
        TCrossClusterManualCallbackExecutor* Executor_;
    };

    class TManualMultiClusterClient
        : public IMultiClusterClient
    {
    public:
        TManualMultiClusterClient(
            const NNative::IConnectionPtr& connection,
            const TCrossClusterReplicatedStateConfigPtr& config,
            const NNative::TClientOptions& options,
            TCrossClusterManualCallbackExecutor& executor)
            : Connections_(CreateClusterConnections(connection, config))
            , Clients_(CreateClusterClients(Connections_, options))
            , Executor_(&executor)
        { }

    protected:
        TFuture<std::vector<TErrorOr<std::any>>> DoExecuteCallback(
            std::string tag,
            TCallback<TFuture<std::any>(const ISingleClusterClientPtr&)> callback) override
        {
            YT_VERIFY(Clients_.size() == Executor_->CallbackQueues_.size());

            std::vector<TFuture<std::any>> clusterFutures;
            clusterFutures.reserve(Clients_.size());

            for (int i = 0; i < std::ssize(Clients_); ++i) {
                auto cbIndex = Executor_->CallbackIndex_++;
                Executor_->CallbackQueues_[i].emplace_back(
                    BIND(callback, New<TManualSingleClusterClient>(i, Clients_[i], *Executor_)),
                    NewPromise<std::any>(),
                    tag,
                    cbIndex);
                clusterFutures.push_back(std::get<1>(Executor_->CallbackQueues_[i].back()).ToFuture());
            }

            if (Executor_->ParentCompletionHandler_) {
                Executor_->ParentCompletionHandler_.TrySet();
            }

            return AllSet(std::move(clusterFutures));
        }

    private:
        std::vector<NNative::IConnectionPtr> Connections_;
        std::vector<IClientBasePtr> Clients_;
        TCrossClusterManualCallbackExecutor* Executor_;
    };

    TFuture<std::any> AddCallback(
        std::string tag,
        const ISingleClusterClientPtr& client,
        TCallback<TFuture<std::any>(const ISingleClusterClientPtr&)> callback)
    {
        auto cbIndex = CallbackIndex_++;
        auto index = client->GetIndex();

        CallbackQueues_[index].emplace_back(
            BIND(callback, New<TManualSingleClusterClient>(index, client->GetClient(), *this)),
            NewPromise<std::any>(),
            tag,
            cbIndex);

        auto result = std::get<1>(CallbackQueues_[index].back()).ToFuture();

        if (ParentCompletionHandler_) {
            ParentCompletionHandler_.TrySet();
        }
        return result;
    }

    TFuture<void> ExecuteCallback(TCallbackQueue::iterator it, std::size_t index)
    {
        auto& [callback, resultPromise, tag, cbIndex] = *it;

        YT_VERIFY(!ParentCompletionHandler_ || ParentCompletionHandler_.IsSet());
        auto completionHandler = NewPromise<void>();
        ParentCompletionHandler_ = completionHandler;

        auto callbackToExecute = std::move(callback);
        auto resultPromiseSetter = BIND([promise_ = std::move(resultPromise), tag] (TErrorOr<std::any>&& result) {
            promise_.Set(std::move(result));
        });

        CallbackQueues_[index].erase(it);
        auto result = ParentCompletionHandler_.ToFuture();
        YT_UNUSED_FUTURE(std::invoke(callbackToExecute)
            .AsUnique()
            .Apply(std::move(resultPromiseSetter))
            .Apply(BIND([p = completionHandler] (const TError&) {
                p.TrySet();
            })));
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCrossClusterReplicatedStateTest
    : public TApiTestBase
{
public:
    void SetUp() override
    {
        TApiTestBase::SetUp();

        auto connection = DynamicPointerCast<NNative::IConnection>(Connection_);
        auto client = connection->CreateNativeClient(NNative::TClientOptions::Root());

        TCreateNodeOptions options;
        options.IgnoreExisting = true;

        client->CreateNode("//tmp/test1", EObjectType::MapNode, options).Get()
            .ValueOrThrow();
        client->CreateNode("//tmp/test2", EObjectType::MapNode, options).Get()
            .ValueOrThrow();
        client->CreateNode("//tmp/test3", EObjectType::MapNode, options).Get()
            .ValueOrThrow();

        TCreateObjectOptions userOptions;
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("name", "cross_cluster_test_user");
        userOptions.Attributes = std::move(attributes);
        userOptions.IgnoreExisting = true;
        client->CreateObject(EObjectType::User, userOptions).Get().ThrowOnError();
    }

protected:
    TCrossClusterReplicatedStateConfigPtr CreateXConfig()
    {
        auto config = New<TCrossClusterReplicatedStateConfig>();
        config->User = "cross_cluster_test_user";
        config->Replicas = {New<TCrossClusterStateReplicaConfig>(), New<TCrossClusterStateReplicaConfig>(), New<TCrossClusterStateReplicaConfig>()};
        config->Replicas[0]->StateDirectory = "//tmp/test1";
        config->Replicas[0]->ClusterName = GetClusterName();
        config->Replicas[1]->StateDirectory = "//tmp/test2";
        config->Replicas[1]->ClusterName = GetClusterName();
        config->Replicas[2]->StateDirectory = "//tmp/test3";
        config->Replicas[2]->ClusterName = GetClusterName();
        return config;
    }

    ICrossClusterReplicatedStatePtr CreateXState(TCrossClusterReplicatedStateConfigPtr config, TCrossClusterManualCallbackExecutor& callbackExecutor)
    {
        auto nativeConnection = DynamicPointerCast<NNative::IConnection>(Connection_);
        nativeConnection->GetClusterDirectorySynchronizer()->Sync(true).Get();
        auto client = callbackExecutor.CreateClient(
            nativeConnection, config, NNative::TClientOptions::FromUser(config->User));
        auto lockWaiter = CreateCrossClusterReplicaLockWaiter(Connection_->GetInvoker(), config, Logger());
        lockWaiter->Start();
        return CreateCrossClusterReplicatedState(std::move(client), std::move(lockWaiter), std::move(config), Logger());
    }

    NNative::IClientPtr GetRootClient()
    {
        auto connection = DynamicPointerCast<NNative::IConnection>(Connection_);
        return connection->CreateNativeClient(NNative::TClientOptions::Root());
    }

    void SetAcl(NYPath::TYPath path, bool deny)
    {
        auto connection = DynamicPointerCast<NNative::IConnection>(Connection_);
        auto client = connection->CreateNativeClient(NNative::TClientOptions::Root());
        auto allowAll = R"([{subjects=["cross_cluster_test_user"];permissions=["read";"write";"modify_children"];action="allow"}])"sv;
        auto denyAll = R"([{subjects=["cross_cluster_test_user"];permissions=["read";"write";"modify_children"];action="deny"}])"sv;
        client->SetNode(std::move(path) + "/@acl", NYson::TYsonString(deny ? denyAll : allowAll)).Get().ThrowOnError();
    }

    std::string GetClusterName()
    {
        auto nativeConnection = DynamicPointerCast<NNative::IConnection>(Connection_);
        auto clusterName = nativeConnection->GetClusterName();
        YT_VERIFY(clusterName);
        return *std::move(clusterName);
    }
};

void ExecuteN(
    TCrossClusterManualCallbackExecutor& callbackExecutor,
    int n,
    TDuration timeout = TDuration::Seconds(3))
{
    for (auto i = 0; i < n; ++i) {
        for (auto j = 0; j < 3; ++j) {
            auto opt = callbackExecutor.ExecuteNext(j, timeout);
            YT_VERIFY(opt);
            opt->Get().ThrowOnError();
        }
    }
}

void ExecuteN(
    TCrossClusterManualCallbackExecutor& callbackExecutor,
    const std::string& tag,
    int n,
    TDuration timeout = TDuration::Seconds(3))
{
    for (auto i = 0; i < n; ++i) {
        for (auto j = 0; j < 3; ++j) {
            auto opt = callbackExecutor.ExecuteNextWithTag(tag, j, timeout);
            YT_VERIFY(opt);
            opt->Get().ThrowOnError();
        }
    }
}

void ExecuteNAndCheck(TCrossClusterManualCallbackExecutor& callbackExecutor, int n)
{
    ExecuteN(callbackExecutor, n);

    YT_VERIFY(!callbackExecutor.ExecuteNext(0, TDuration::Seconds(3)));
    YT_VERIFY(!callbackExecutor.ExecuteNext(1, TDuration::Seconds(3)));
    YT_VERIFY(!callbackExecutor.ExecuteNext(2, TDuration::Seconds(3)));
}

TEST_F(TCrossClusterReplicatedStateTest, TestLoadEmpty)
{
    auto xConfig = CreateXConfig();

    auto callbackExecutor = New<TCrossClusterManualCallbackExecutor>(3);
    auto xState = CreateXState(xConfig, *callbackExecutor);

    auto validated = xState->ValidateStateDirectories();
    ExecuteNAndCheck(*callbackExecutor, 1);
    validated.Get().ThrowOnError();

    auto emptyEntry = xState->Value("a", "empty");
    auto emptyLoadResult = emptyEntry->Load();

    EXPECT_FALSE(emptyLoadResult.IsSet());

    // 0 - Fetch.
    ExecuteNAndCheck(*callbackExecutor, 1);

    auto emptyValue = emptyLoadResult.Get()
        .ValueOrThrow();

    EXPECT_TRUE(!emptyValue);

    auto emptyVersions = xState->FetchVersions();

    EXPECT_FALSE(emptyVersions.IsSet());

    ExecuteNAndCheck(*callbackExecutor, 1);

    auto emptyVersionsMap = emptyVersions.Get()
        .ValueOrThrow();

    EXPECT_TRUE(emptyVersionsMap.empty());
}

TEST_F(TCrossClusterReplicatedStateTest, TestLoadCreated)
{
    auto xConfig = CreateXConfig();

    auto callbackExecutor = New<TCrossClusterManualCallbackExecutor>(3);
    auto xState = CreateXState(xConfig, *callbackExecutor);
    auto emptyEntry = xState->Value("a", "empty1");
    auto storeResult = emptyEntry->Store(ConvertToNode(TYsonString("{value=1}"sv))->AsMap());

    EXPECT_FALSE(storeResult.IsSet());

    // 0 - Fetch, 1 - Create, 2 - Set.
    ExecuteNAndCheck(*callbackExecutor, 4);

    storeResult.Get().ThrowOnError();

    auto loadResult = emptyEntry->Load();

    ExecuteNAndCheck(*callbackExecutor, 1);

    auto value = loadResult.Get()
        .ValueOrThrow();

    EXPECT_TRUE(value->AsMap()->FindChild("value")->AsInt64()->GetValue() == 1u);

    auto versions = xState->FetchVersions();

    ExecuteNAndCheck(*callbackExecutor, 1);

    auto versionsMap = versions.Get()
        .ValueOrThrow();

    EXPECT_EQ(versionsMap["empty1"], "0000000000000001;a");
}

TEST_F(TCrossClusterReplicatedStateTest, TestConcurrentCreation)
{
    auto xConfig = CreateXConfig();

    auto callbackExecutor = New<TCrossClusterManualCallbackExecutor>(3);
    auto xState = CreateXState(xConfig, *callbackExecutor);
    auto emptyEntryA = xState->Value("a", "emptyC");
    auto emptyEntryB = xState->Value("b", "emptyC");
    auto storeResultA = emptyEntryA->Store(ConvertToNode(TYsonString("{value=1}"sv))->AsMap());
    auto storeResultB = emptyEntryB->Store(ConvertToNode(TYsonString("{value=2}"sv))->AsMap());

    // Fetch
    ExecuteN(*callbackExecutor, "a", 1);
    ExecuteN(*callbackExecutor, "b", 1);

    // Create and gather lock
    ExecuteN(*callbackExecutor, "a", 1);
    // Waiting for lock
    ExecuteN(*callbackExecutor, "b", 1);

    // Set and release lock
    ExecuteN(*callbackExecutor, "a", 2);
    // Gather lock, set and release lock
    ExecuteN(*callbackExecutor, "b", 2);

    EXPECT_TRUE(storeResultA.Get().IsOK());
    EXPECT_TRUE(storeResultB.Get().IsOK());

    auto loadResult = emptyEntryA->Load();
    ExecuteNAndCheck(*callbackExecutor, 1);

    auto value = loadResult.Get()
        .ValueOrThrow();

    EXPECT_EQ(value->AsMap()->FindChild("version")->AsUint64()->GetValue(), 1u);
    EXPECT_EQ(value->AsMap()->FindChild("version_tag")->AsString()->GetValue(), "b");
    EXPECT_EQ(value->AsMap()->FindChild("value")->AsInt64()->GetValue(), 2);
}

TEST_F(TCrossClusterReplicatedStateTest, TestConcurrentStores)
{
    auto xConfig = CreateXConfig();

    auto callbackExecutor = New<TCrossClusterManualCallbackExecutor>(3);
    auto xState = CreateXState(xConfig, *callbackExecutor);
    auto entryA = xState->Value("a", "empty3");
    auto entryB = xState->Value("b", "empty3");

    auto storeResult0 = entryA->Store(ConvertToNode(TYsonString("{value=1}"sv))->AsMap());
    ExecuteNAndCheck(*callbackExecutor, 4);
    storeResult0.Get().ThrowOnError();

    auto loadResult = entryA->Load();
    ExecuteNAndCheck(*callbackExecutor, 1);
    auto value = loadResult.Get()
        .ValueOrThrow();

    EXPECT_EQ(value->AsMap()->FindChild("value")->AsInt64()->GetValue(), 1);
    EXPECT_EQ(value->AsMap()->FindChild("version")->AsUint64()->GetValue(), 1u);
    EXPECT_EQ(value->AsMap()->FindChild("version_tag")->AsString()->GetValue(), "a");

    auto storeResultB = entryB->Store(ConvertToNode(TYsonString("{value=4}"sv))->AsMap());
    auto storeResultA = entryA->Store(ConvertToNode(TYsonString("{value=2}"sv))->AsMap());

    // Fetch
    ExecuteN(*callbackExecutor, "a", 1);
    ExecuteN(*callbackExecutor, "b", 1);

    // Gather lock
    ExecuteN(*callbackExecutor, "a", 1);
    // Waiting for lock
    ExecuteN(*callbackExecutor, "b", 1);

    // Set and release lock
    ExecuteN(*callbackExecutor, "a", 2);
    // Gather lock, set and release lock
    ExecuteN(*callbackExecutor, "b", 2);

    EXPECT_TRUE(storeResultA.Get().IsOK());
    EXPECT_TRUE(storeResultB.Get().IsOK());

    loadResult = entryA->Load();
    ExecuteNAndCheck(*callbackExecutor, 1);

    value = loadResult.Get()
        .ValueOrThrow();

    EXPECT_EQ(value->AsMap()->FindChild("version")->AsUint64()->GetValue(), 2u);
    EXPECT_EQ(value->AsMap()->FindChild("version_tag")->AsString()->GetValue(), "b");
    EXPECT_EQ(value->AsMap()->FindChild("value")->AsInt64()->GetValue(), 4);

    auto storeResultB1 = entryB->Store(ConvertToNode(TYsonString("{value=8}"sv))->AsMap());
    auto storeResultA1 = entryA->Store(ConvertToNode(TYsonString("{value=6}"sv))->AsMap());

    // Fetch
    ExecuteN(*callbackExecutor, "a", 1);
    ExecuteN(*callbackExecutor, "b", 1);

    // Gather lock
    ExecuteN(*callbackExecutor, "b", 1);
    // Waiting for lock
    ExecuteN(*callbackExecutor, "a", 1);

    // Set and release lock
    ExecuteN(*callbackExecutor, "b", 2);
    // Abort
    ExecuteN(*callbackExecutor, "a", 1);

    EXPECT_TRUE(storeResultB1.Get().IsOK());
    EXPECT_TRUE(storeResultA1.Get().IsOK());

    loadResult = entryA->Load();
    ExecuteNAndCheck(*callbackExecutor, 1);

    value = loadResult.Get()
        .ValueOrThrow();

    EXPECT_EQ(value->AsMap()->FindChild("version")->AsUint64()->GetValue(), 3u);
    EXPECT_EQ(value->AsMap()->FindChild("version_tag")->AsString()->GetValue(), "b");
    EXPECT_EQ(value->AsMap()->FindChild("value")->AsInt64()->GetValue(), 8);
}

TEST_F(TCrossClusterReplicatedStateTest, TestLockAcquisionFailed)
{
    auto xConfig = CreateXConfig();
    xConfig->UpdateTransactionTimeout = TDuration::Seconds(100);

    auto callbackExecutor = New<TCrossClusterManualCallbackExecutor>(3);
    auto xState = CreateXState(xConfig, *callbackExecutor);
    auto entryA = xState->Value("a", "emptyLF");
    auto entryB = xState->Value("b", "emptyLF");

    auto storeResultB = entryB->Store(ConvertToNode(TYsonString("{value=4}"sv))->AsMap());
    auto storeResultA = entryA->Store(ConvertToNode(TYsonString("{value=2}"sv))->AsMap());

    // Fetch
    ExecuteN(*callbackExecutor, "a", 1);
    ExecuteN(*callbackExecutor, "b", 1);

    // Gather lock
    ExecuteN(*callbackExecutor, "a", 1);
    // Failed to gather lock
    ExecuteN(*callbackExecutor, "b", 1);
    // Timeout, abort
    ExecuteN(*callbackExecutor, "b", 1, TDuration::Seconds(6));
    // Set and release
    ExecuteN(*callbackExecutor, "a", 2);

    EXPECT_TRUE(storeResultA.Get().IsOK());
    EXPECT_THROW_WITH_SUBSTRING(storeResultB.Get().ThrowOnError();, "Can't update values on cluster quorum");

    auto loadResult = entryA->Load();
    ExecuteN(*callbackExecutor, 1);

    auto value = loadResult.Get()
        .ValueOrThrow();

    EXPECT_EQ(value->AsMap()->FindChild("version")->AsUint64()->GetValue(), 1u);
    EXPECT_EQ(value->AsMap()->FindChild("value")->AsInt64()->GetValue(), 2);
    EXPECT_EQ(value->AsMap()->FindChild("version_tag")->AsString()->GetValue(), "a");
}

TEST_F(TCrossClusterReplicatedStateTest, TestQuorumUnavailable)
{
    SetAcl("//tmp/test1", true);
    SetAcl("//tmp/test2", true);
    SetAcl("//tmp/test3", true);

    auto xConfig = CreateXConfig();

    auto callbackExecutor = New<TCrossClusterManualCallbackExecutor>(3);
    auto xState = CreateXState(xConfig, *callbackExecutor);
    auto emptyEntry = xState->Value("a", "empty7");
    auto storeResult = emptyEntry->Store(ConvertToNode(TYsonString("{value=2}"sv))->AsMap());
    ExecuteNAndCheck(*callbackExecutor, 2);
    EXPECT_THROW_WITH_SUBSTRING(storeResult.Get().ThrowOnError();, "Can't update values on cluster quorum");

    SetAcl("//tmp/test1", false);
    SetAcl("//tmp/test2", false);
    SetAcl("//tmp/test3", false);
}

TEST_F(TCrossClusterReplicatedStateTest, TestLoadWrites)
{
    SetAcl("//tmp/test1", true);

    auto xConfig = CreateXConfig();

    auto callbackExecutor = New<TCrossClusterManualCallbackExecutor>(3);
    auto xState = CreateXState(xConfig, *callbackExecutor);
    auto emptyEntry = xState->Value("a", "empty4");
    auto storeResult = emptyEntry->Store(ConvertToNode(TYsonString("{value=1}"sv))->AsMap());

    EXPECT_FALSE(storeResult.IsSet());

    for (auto j = 0; j < 2; ++j) {
        for (auto i = 0; i < 3; ++i) {
            auto opt = callbackExecutor->ExecuteNext(i);
            EXPECT_TRUE(opt);
            EXPECT_NO_THROW(opt->Get().ThrowOnError());
        }
    }

    EXPECT_FALSE(callbackExecutor->ExecuteNext(0));

    for (auto j = 0; j < 2; ++j) {
        for (auto i = 1; i < 3; ++i) {
            auto opt = callbackExecutor->ExecuteNext(i);
            EXPECT_TRUE(opt);
            EXPECT_NO_THROW(opt->Get().ThrowOnError());
        }
    }

    EXPECT_NO_THROW(storeResult.Get().ThrowOnError());
    EXPECT_FALSE(GetRootClient()->NodeExists("//tmp/test1/empty4").Get().ValueOrThrow());
    SetAcl("//tmp/test1", false);

    auto loadResult = emptyEntry->Load();

    for (auto j = 0; j < 2; ++j) {
        for (auto i = 0; i < 3; ++i) {
            auto opt = callbackExecutor->ExecuteNext(i);
            EXPECT_TRUE(opt);
            EXPECT_NO_THROW(opt->Get().ThrowOnError());
        }
    }

    for (auto j = 0; j < 2; ++j) {
        auto opt = callbackExecutor->ExecuteNext(0);
        EXPECT_TRUE(opt);
        EXPECT_NO_THROW(opt->Get().ThrowOnError());
    }

    EXPECT_FALSE(callbackExecutor->ExecuteNext(0));
    EXPECT_FALSE(callbackExecutor->ExecuteNext(1));
    EXPECT_FALSE(callbackExecutor->ExecuteNext(2));
    EXPECT_TRUE(GetRootClient()->NodeExists("//tmp/test1/empty4").Get().ValueOrThrow());

    auto value = loadResult.Get().ValueOrThrow();
    EXPECT_TRUE(value->AsMap()->FindChild("value")->AsInt64()->GetValue() == 1u);
}

TEST_F(TCrossClusterReplicatedStateTest, TestQuorumIntersection)
{
    SetAcl("//tmp/test1", true);

    auto xConfig = CreateXConfig();

    auto callbackExecutor = New<TCrossClusterManualCallbackExecutor>(3);
    auto xState = CreateXState(xConfig, *callbackExecutor);
    auto emptyEntry = xState->Value("a", "empty5");
    auto storeResult = emptyEntry->Store(ConvertToNode(TYsonString("{value=1}"sv))->AsMap());

    EXPECT_FALSE(storeResult.IsSet());

    for (auto j = 0; j < 2; ++j) {
        for (auto i = 0; i < 3; ++i) {
            auto opt = callbackExecutor->ExecuteNext(i);
            EXPECT_TRUE(opt);
            EXPECT_NO_THROW(opt->Get().ThrowOnError());
        }
    }

    for (auto j = 0; j < 2; ++j) {
        for (auto i = 1; i < 3; ++i) {
            auto opt = callbackExecutor->ExecuteNext(i);
            EXPECT_TRUE(opt);
            EXPECT_NO_THROW(opt->Get().ThrowOnError());
        }
    }

    EXPECT_FALSE(callbackExecutor->ExecuteNext(0));
    storeResult.Get().ThrowOnError();

    EXPECT_FALSE(GetRootClient()->NodeExists("//tmp/test1/empty5").Get().ValueOrThrow());
    SetAcl("//tmp/test1", false);
    SetAcl("//tmp/test3", true);

    auto versions = xState->FetchVersions();

    ExecuteNAndCheck(*callbackExecutor, 1);

    auto versionsMap = versions.Get()
        .ValueOrThrow();

    EXPECT_EQ(versionsMap["empty5"], "0000000000000001;a");

    auto loadResult = emptyEntry->Load();

    for (auto j = 0; j < 2; ++j) {
        for (auto i = 0; i < 3; ++i) {
            auto opt = callbackExecutor->ExecuteNext(i);
            EXPECT_TRUE(opt);
            EXPECT_NO_THROW(opt->Get().ThrowOnError());
        }
    }

    for (auto j = 0; j < 2; ++j) {
        auto opt = callbackExecutor->ExecuteNext(0);
        EXPECT_TRUE(opt);
        EXPECT_NO_THROW(opt->Get().ThrowOnError());
    }

    EXPECT_FALSE(callbackExecutor->ExecuteNext(1));
    EXPECT_FALSE(callbackExecutor->ExecuteNext(2));

    EXPECT_TRUE(GetRootClient()->NodeExists("//tmp/test1/empty5").Get().ValueOrThrow());

    auto value = loadResult.Get()
        .ValueOrThrow();
    EXPECT_EQ(value->AsMap()->FindChild("value")->AsInt64()->GetValue(), 1);

    SetAcl("//tmp/test3", false);
}

TEST_F(TCrossClusterReplicatedStateTest, TestSingleClusterAvailableOnWrite)
{
    SetAcl("//tmp/test1", true);
    SetAcl("//tmp/test2", true);
    auto xConfig = CreateXConfig();

    auto callbackExecutor = New<TCrossClusterManualCallbackExecutor>(3);
    auto xState = CreateXState(xConfig, *callbackExecutor);
    auto emptyEntry = xState->Value("a", "empty8");

    auto storeResult = emptyEntry->Store(ConvertToNode(TYsonString("{value=2}"sv))->AsMap());
    ExecuteN(*callbackExecutor, 2);
    for (auto i = 0; i < 2; ++i) {
        auto opt = callbackExecutor->ExecuteNext(2);
        EXPECT_TRUE(opt);
        EXPECT_NO_THROW(opt->Get().ThrowOnError());
    }
    EXPECT_THROW_WITH_SUBSTRING(storeResult.Get().ThrowOnError();, "Can't update values on cluster quorum");

    auto loadResult = emptyEntry->Load();
    ExecuteN(*callbackExecutor, 2);
    EXPECT_THROW_WITH_SUBSTRING(storeResult.Get().ThrowOnError();, "Can't update values on cluster quorum");

    auto versions = xState->FetchVersions();

    ExecuteNAndCheck(*callbackExecutor, 1);

    EXPECT_THROW_WITH_SUBSTRING(versions.Get().ValueOrThrow(), "Can't get versions from cluster quorum");

    SetAcl("//tmp/test1", false);
    SetAcl("//tmp/test2", false);

    loadResult = emptyEntry->Load();
    ExecuteN(*callbackExecutor, 2);
    for (auto i = 0; i < 2; ++i) {
        for (auto j = 0; j < 2; ++j) {
            auto opt = callbackExecutor->ExecuteNext(j);
            EXPECT_TRUE(opt);
            EXPECT_NO_THROW(opt->Get().ThrowOnError());
        }
    }
    auto value = loadResult.Get()
        .ValueOrThrow();
    EXPECT_EQ(value->AsMap()->FindChild("value")->AsInt64()->GetValue(), 2);
    EXPECT_EQ(value->AsMap()->FindChild("version")->AsUint64()->GetValue(), 1u);
}

TEST_F(TCrossClusterReplicatedStateTest, Test100ConcurrentStores)
{
    auto xConfig = CreateXConfig();

    auto callbackExecutor = New<TCrossClusterManualCallbackExecutor>(3);
    auto xState = CreateXState(xConfig, *callbackExecutor);
    std::vector<ICrossClusterReplicatedValuePtr> entries;
    entries.reserve(100);
    for (auto i = 0; i < 100; ++i) {
        entries.push_back(xState->Value(std::format("{:03}", i), "empty100"));
    }
    std::vector<TFuture<void>> storeResults;
    storeResults.reserve(entries.size());
    for (auto& entry : entries) {
        storeResults.push_back(entry->Store(ConvertToNode(TYsonString("{value=1}"sv))->AsMap()));
    }

    ExecuteN(*callbackExecutor, 100);
    std::vector<int> indices(100);
    std::ranges::copy(std::views::iota(0, 100), indices.begin());
    std::ranges::shuffle(indices, std::mt19937(1111));
    auto curMax = -1;
    for (auto i : indices) {
        ExecuteN(*callbackExecutor, std::format("{:03}", i), i > curMax ? 3 : 2);
        curMax = std::max(i, curMax);
    }

    for (auto& storeResult : storeResults) {
        storeResult.Get().ThrowOnError();
    }

    auto loadResult = entries.back()->Load();
    ExecuteNAndCheck(*callbackExecutor, 1);
    auto value = loadResult.Get().ValueOrThrow();

    EXPECT_EQ(value->AsMap()->FindChild("value")->AsInt64()->GetValue(), 1);
    EXPECT_EQ(value->AsMap()->FindChild("version")->AsUint64()->GetValue(), 1u);
    EXPECT_EQ(value->AsMap()->FindChild("version_tag")->AsString()->GetValue(), "099");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCppTests
