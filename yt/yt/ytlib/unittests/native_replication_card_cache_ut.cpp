#include <yt/yt/ytlib/chaos_client/chaos_cell_directory_synchronizer.h>
#include <yt/yt/ytlib/chaos_client/chaos_node_service_proxy.h>
#include <yt/yt/ytlib/chaos_client/native_replication_card_cache_detail.h>

#include <yt/yt/ytlib/hive/cell_directory.h>
#include <yt/yt/ytlib/hive/config.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/test_framework/test_connection.h>

#include <yt/yt/client/chaos_client/config.h>
#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/test_framework/test_proxy_service.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NChaosClient {
namespace {

using namespace NConcurrency;
using namespace NHiveClient;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NRpc;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger TestLogger("NativeReplicationCardCacheTest");

constexpr auto ChaosCacheAddress = "chaos-cache";
constexpr TReplicationEra MinimalEraBase = 100;
constexpr TReplicationEra ProgressEraBase = 200;

////////////////////////////////////////////////////////////////////////////////

class TNoopChaosCellDirectorySynchronizer
    : public IChaosCellDirectorySynchronizer
{
public:
    void AddCellIds(const std::vector<TCellId>& /*cellIds*/) override
    { }

    void AddCellTag(TCellTag /*cellTag*/) override
    { }

    TFuture<void> Sync() override
    {
        return MakeFuture<void>(TError());
    }

    void Start() override
    { }

    void Stop() override
    { }
};

////////////////////////////////////////////////////////////////////////////////

class TReplicationCardCacheTestConnection
    : public TTestConnection
{
public:
    TReplicationCardCacheTestConnection(
        IChannelFactoryPtr channelFactory,
        IInvokerPtr invoker,
        INodeMemoryTrackerPtr memoryTracker)
        : TTestConnection(
            channelFactory,
            {"default"},
            New<TNodeDirectory>(),
            /*nodeStatusDirectory*/ nullptr,
            invoker,
            std::move(memoryTracker))
        , CellDirectory_(CreateCellDirectory(
            New<TCellDirectoryConfig>(),
            std::move(channelFactory),
            /*clusterDirectory*/ {},
            {"default"},
            TestLogger))
        , ChaosCellDirectorySynchronizer_(New<TNoopChaosCellDirectorySynchronizer>())
    { }

    const ICellDirectoryPtr& GetCellDirectory() override
    {
        return CellDirectory_;
    }

    const IChaosCellDirectorySynchronizerPtr& GetChaosCellDirectorySynchronizer() override
    {
        return ChaosCellDirectorySynchronizer_;
    }

    const IChaosResidencyCachePtr& GetChaosResidencyCache() override
    {
        return ChaosResidencyCache_;
    }

private:
    const ICellDirectoryPtr CellDirectory_;
    const IChaosCellDirectorySynchronizerPtr ChaosCellDirectorySynchronizer_;
    const IChaosResidencyCachePtr ChaosResidencyCache_;
};

DEFINE_REFCOUNTED_TYPE(TReplicationCardCacheTestConnection)

////////////////////////////////////////////////////////////////////////////////

class TFakeChaosNodeService
    : public TServiceBase
{
public:
    explicit TFakeChaosNodeService(IInvokerPtr invoker)
        : TServiceBase(
            std::move(invoker),
            TChaosNodeServiceProxy::GetDescriptor(),
            TestLogger)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetReplicationCard));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(WatchReplicationCard));
    }

    int GetFetchCount(bool includeProgress) const
    {
        auto guard = Guard(Lock_);
        return includeProgress ? ProgressFetchCount_ : MinimalFetchCount_;
    }

    TReplicationCardFetchOptions GetFetchOptions(int index) const
    {
        auto guard = Guard(Lock_);
        return FetchOptions_.at(index);
    }

    bool HasPendingWatch() const
    {
        auto guard = Guard(Lock_);
        return static_cast<bool>(WatchContext_);
    }

    void ReplyWatchChanged(TReplicationEra era)
    {
        auto context = TakeWatchContext();
        auto* changed = context->Response().mutable_replication_card_changed();
        changed->set_replication_card_cache_timestamp(era);
        ToProto(changed->mutable_replication_card(), *CreateCard(era), MinimalFetchOptions);
        context->Reply();
    }

    void ReplyWatchDeleted()
    {
        auto context = TakeWatchContext();
        context->Response().mutable_replication_card_deleted();
        context->Reply();
    }

    void ReplyPendingWatchDeleted()
    {
        TCtxWatchReplicationCardPtr context;
        {
            auto guard = Guard(Lock_);
            context = std::move(WatchContext_);
        }
        if (context) {
            context->Response().mutable_replication_card_deleted();
            context->Reply();
        }
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NProto, GetReplicationCard)
    {
        TReplicationCardFetchOptions options;
        FromProto(&options, request->fetch_options());

        TReplicationEra era;
        {
            auto guard = Guard(Lock_);
            FetchOptions_.push_back(options);
            if (options.IncludeProgress) {
                era = ProgressEraBase + ++ProgressFetchCount_;
            } else {
                era = MinimalEraBase + ++MinimalFetchCount_;
            }
        }

        ToProto(response->mutable_replication_card(), *CreateCard(era), options);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, WatchReplicationCard)
    {
        auto guard = Guard(Lock_);
        YT_VERIFY(!WatchContext_);
        WatchContext_ = context;
    }

    mutable YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    int MinimalFetchCount_ = 0;
    int ProgressFetchCount_ = 0;
    std::vector<TReplicationCardFetchOptions> FetchOptions_;
    TCtxWatchReplicationCardPtr WatchContext_;

    static TReplicationCardPtr CreateCard(TReplicationEra era)
    {
        auto card = New<TReplicationCard>();
        card->Era = era;
        card->CurrentTimestamp = NTransactionClient::TTimestamp(era);
        return card;
    }

    TCtxWatchReplicationCardPtr TakeWatchContext()
    {
        auto guard = Guard(Lock_);
        YT_VERIFY(WatchContext_);
        return std::move(WatchContext_);
    }
};

DEFINE_REFCOUNTED_TYPE(TFakeChaosNodeService)

////////////////////////////////////////////////////////////////////////////////

class TNativeReplicationCardCacheTest
    : public ::testing::Test
{
protected:
    TActionQueuePtr ConnectionQueue_;
    TActionQueuePtr ServiceQueue_;
    INodeMemoryTrackerPtr MemoryTracker_;
    TIntrusivePtr<TFakeChaosNodeService> Service_;
    TIntrusivePtr<TReplicationCardCacheTestConnection> Connection_;
    IReplicationCardCachePtr Cache_;

    const TReplicationCardId CardId_ = MakeRandomId(EObjectType::ReplicationCard, TCellTag(0xf001));

    void SetUp() override
    {
        ConnectionQueue_ = New<TActionQueue>("ReplicationCardCacheConnection");
        ServiceQueue_ = New<TActionQueue>("ReplicationCardCacheService");
        MemoryTracker_ = CreateNodeMemoryTracker(32_MB, New<TNodeMemoryTrackerConfig>(), {});
        Service_ = New<TFakeChaosNodeService>(ServiceQueue_->GetInvoker());

        THashMap<std::string, IServicePtr> addressToService;
        addressToService[ChaosCacheAddress] = Service_;
        auto channelFactory = CreateTestChannelFactory(
            addressToService,
            THashMap<std::string, IServicePtr>{});
        Connection_ = New<TReplicationCardCacheTestConnection>(
            std::move(channelFactory),
            ConnectionQueue_->GetInvoker(),
            MemoryTracker_);
    }

    void TearDown() override
    {
        if (Cache_) {
            Cache_->Clear();
            Cache_ = nullptr;
        }
        Service_->ReplyPendingWatchDeleted();
        Connection_ = nullptr;
        Service_ = nullptr;
        MemoryTracker_->ClearTrackers();
        MemoryTracker_ = nullptr;
        // Drain the service queue first: sending the RPC reply schedules the watcher callback on the connection queue.
        ServiceQueue_->Shutdown(/*graceful*/ true);
        ConnectionQueue_->Shutdown(/*graceful*/ true);
    }

    TReplicationCardCacheConfigPtr CreateConfig(
        TDuration progressExpirationTime = TDuration::Days(1))
    {
        auto config = New<TReplicationCardCacheConfig>();
        config->Addresses = std::vector<std::string>{ChaosCacheAddress};
        config->EnableWatching = true;

        config->ExpireAfterAccessTime = TDuration::Days(1);
        config->ExpireAfterSuccessfulUpdateTime = progressExpirationTime;
        config->ExpireAfterFailedUpdateTime = TDuration::Days(1);
        config->RefreshTime = std::nullopt;
        config->ExpirationPeriod = std::nullopt;

        config->WatchedCacheConfig->ExpireAfterAccessTime = TDuration::Days(1);
        config->WatchedCacheConfig->ExpireAfterSuccessfulUpdateTime = TDuration::Days(1);
        config->WatchedCacheConfig->ExpireAfterFailedUpdateTime = TDuration::Days(1);
        config->WatchedCacheConfig->RefreshTime = std::nullopt;
        config->WatchedCacheConfig->ExpirationPeriod = std::nullopt;

        config->WatchedCacheConfig->Postprocess();
        config->Postprocess();
        return config;
    }

    void CreateCache(TReplicationCardCacheConfigPtr config)
    {
        Cache_ = CreateNativeReplicationCardCache(
            std::move(config),
            Connection_,
            TestLogger);
    }

    TReplicationCardPtr GetCard(const TReplicationCardFetchOptions& options)
    {
        return WaitFor(Cache_->GetReplicationCard(TReplicationCardCacheKey{
            .CardId = CardId_,
            .FetchOptions = options,
        })).ValueOrThrow();
    }

    void WaitForPendingWatch()
    {
        WaitForPredicate(
            [this] {
                return Service_->HasPendingWatch();
            },
            "Replication card watch request was not issued");
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TNativeReplicationCardCacheTest, WatcherUpdatesOnlyMinimalCache)
{
    CreateCache(CreateConfig());

    auto minimalRequestOptions = TReplicationCardFetchOptions{
        .IncludeCoordinators = true,
    };

    EXPECT_EQ(MinimalEraBase + 1, GetCard(minimalRequestOptions)->Era);
    EXPECT_EQ(ProgressEraBase + 1, GetCard(FetchOptionsWithProgress)->Era);
    EXPECT_EQ(1, Service_->GetFetchCount(/*includeProgress*/ false));
    EXPECT_EQ(1, Service_->GetFetchCount(/*includeProgress*/ true));

    EXPECT_EQ(MinimalFetchOptions, Service_->GetFetchOptions(0));
    EXPECT_EQ(FetchOptionsWithProgress, Service_->GetFetchOptions(1));

    WaitForPendingWatch();
    Service_->ReplyWatchChanged(/*era*/ 301);

    WaitForPredicate([&] {
        return GetCard(minimalRequestOptions)->Era == 301;
    });
    EXPECT_EQ(ProgressEraBase + 1, GetCard(FetchOptionsWithProgress)->Era);
    EXPECT_EQ(1, Service_->GetFetchCount(/*includeProgress*/ false));
    EXPECT_EQ(1, Service_->GetFetchCount(/*includeProgress*/ true));

    WaitForPendingWatch();
    Service_->ReplyWatchDeleted();

    WaitForPredicate([&] {
        return GetCard(minimalRequestOptions)->Era == MinimalEraBase + 2;
    });
    EXPECT_EQ(ProgressEraBase + 1, GetCard(FetchOptionsWithProgress)->Era);
    EXPECT_EQ(2, Service_->GetFetchCount(/*includeProgress*/ false));
    EXPECT_EQ(1, Service_->GetFetchCount(/*includeProgress*/ true));
}

TEST_F(TNativeReplicationCardCacheTest, CachesHaveIndependentExpirationPolicies)
{
    CreateCache(CreateConfig(/*progressExpirationTime*/ TDuration::Zero()));

    EXPECT_EQ(MinimalEraBase + 1, GetCard(MinimalFetchOptions)->Era);
    EXPECT_EQ(ProgressEraBase + 1, GetCard(FetchOptionsWithProgress)->Era);
    WaitForPendingWatch();

    EXPECT_EQ(MinimalEraBase + 1, GetCard(MinimalFetchOptions)->Era);
    EXPECT_EQ(ProgressEraBase + 2, GetCard(FetchOptionsWithProgress)->Era);
    EXPECT_EQ(1, Service_->GetFetchCount(/*includeProgress*/ false));
    EXPECT_EQ(2, Service_->GetFetchCount(/*includeProgress*/ true));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChaosClient
