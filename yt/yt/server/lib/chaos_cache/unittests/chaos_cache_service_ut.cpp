#include <yt/yt/server/lib/chaos_cache/chaos_cache.h>
#include <yt/yt/server/lib/chaos_cache/chaos_cache_service.h>
#include <yt/yt/server/lib/chaos_cache/config.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/chaos_client/chaos_node_service_proxy.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/test_framework/chaos_client.h>
#include <yt/yt/ytlib/test_framework/chaos_node_service.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/test_framework/test_proxy_service.h>

namespace NYT::NChaosCache {
namespace {

using namespace NApi::NNative;
using namespace NChaosClient;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NRpc;
using namespace NTransactionClient;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger TestLogger("ChaosCacheServiceTest");

constexpr auto UpstreamAddress = "chaos-cell";
constexpr auto DownstreamAddress = "chaos-cache";

////////////////////////////////////////////////////////////////////////////////

class TChaosCacheServiceTest
    : public ::testing::Test
{
protected:
    const TChaosLeaseId ChaosLeaseId_ = MakeRandomId(EObjectType::ChaosLease, TCellTag(0xf002));

    TActionQueuePtr ConnectionQueue_;
    TActionQueuePtr CacheServiceQueue_;
    TActionQueuePtr UpstreamServiceQueue_;
    INodeMemoryTrackerPtr MemoryTracker_;
    TTestChaosNodeServicePtr UpstreamService_;
    TTestChaosResidencyCachePtr ResidencyCache_;
    TTestChaosConnectionPtr Connection_;
    IClientPtr Client_;
    TChaosCachePtr Cache_;
    IServicePtr CacheService_;
    IChannelPtr DownstreamChannel_;

    void SetUp() override
    {
        ConnectionQueue_ = New<TActionQueue>("ChaosCacheServiceConnection");
        CacheServiceQueue_ = New<TActionQueue>("ChaosCacheService");
        UpstreamServiceQueue_ = New<TActionQueue>("ChaosCacheUpstreamService");
        MemoryTracker_ = CreateNodeMemoryTracker(32_MB, New<TNodeMemoryTrackerConfig>(), {});
        UpstreamService_ = New<TTestChaosNodeService>(UpstreamServiceQueue_->GetInvoker(), TestLogger);
        ResidencyCache_ = New<TTestChaosResidencyCache>();

        THashMap<std::string, IServicePtr> upstreamAddressToService;
        upstreamAddressToService[UpstreamAddress] = UpstreamService_;
        auto upstreamChannelFactory = CreateTestChannelFactory(
            upstreamAddressToService,
            THashMap<std::string, IServicePtr>{});
        auto upstreamChannel = upstreamChannelFactory->CreateChannel(UpstreamAddress);
        Connection_ = New<TTestChaosConnection>(
            std::move(upstreamChannelFactory),
            std::move(upstreamChannel),
            ConnectionQueue_->GetInvoker(),
            MemoryTracker_,
            ResidencyCache_);

        Client_ = NApi::NNative::CreateClient(
            Connection_,
            NApi::NNative::TClientOptions::FromUser("root"),
            MemoryTracker_);

        auto config = New<TChaosCacheConfig>();
        config->Capacity = 1_MB;
        config->ReplicationCardUpdateBatcher->Enable = false;
        config->ChaosLeasesWatcher->PollExpirationTime = TDuration::Days(1);
        config->ChaosLeasesWatcher->ExpirationSweepPeriod = TDuration::Days(1);
        config->Postprocess();

        Cache_ = New<TChaosCache>(config, TProfiler(), TestLogger);
        CacheService_ = CreateChaosCacheService(
            config,
            CacheServiceQueue_->GetInvoker(),
            Client_,
            Cache_,
            /*authenticator*/ nullptr,
            TestLogger);

        THashMap<std::string, IServicePtr> downstreamAddressToService;
        downstreamAddressToService[DownstreamAddress] = CacheService_;
        auto downstreamChannelFactory = CreateTestChannelFactory(
            downstreamAddressToService,
            THashMap<std::string, IServicePtr>{});
        DownstreamChannel_ = downstreamChannelFactory->CreateChannel(DownstreamAddress);
    }

    void TearDown() override
    {
        // Complete pending RPCs while service and callback invokers are alive.
        DrainInvoker(CacheServiceQueue_->GetInvoker());
        DrainInvoker(UpstreamServiceQueue_->GetInvoker());
        UpstreamService_->ReplyAllChaosLeaseWatchesDeleted();
        DrainInvoker(CacheServiceQueue_->GetInvoker());

        DownstreamChannel_ = nullptr;
        CacheService_ = nullptr;
        Cache_ = nullptr;
        Client_ = nullptr;
        Connection_ = nullptr;
        ResidencyCache_ = nullptr;
        UpstreamService_ = nullptr;

        UpstreamServiceQueue_->Shutdown(/*graceful*/ true);
        CacheServiceQueue_->Shutdown(/*graceful*/ true);
        ConnectionQueue_->Shutdown(/*graceful*/ true);

        MemoryTracker_->ClearTrackers();
        MemoryTracker_ = nullptr;
    }

    TFuture<TChaosNodeServiceProxy::TRspWatchChaosLeasePtr> WatchChaosLease(TTimestamp timestamp)
    {
        auto proxy = TChaosNodeServiceProxy(DownstreamChannel_);
        auto request = proxy.WatchChaosLease();
        ToProto(request->mutable_chaos_lease_id(), ChaosLeaseId_);
        request->set_chaos_lease_cache_timestamp(ToProto(timestamp));
        return request->Invoke();
    }

    void WaitForSingleUpstreamWatch()
    {
        WaitFor(UpstreamService_->GetChaosLeaseWatchReceivedFuture())
            .ThrowOnError();
        DrainInvoker(CacheServiceQueue_->GetInvoker());
        DrainInvoker(UpstreamServiceQueue_->GetInvoker());
        EXPECT_EQ(1, UpstreamService_->GetPendingChaosLeaseWatchCount());
    }

    static void DrainInvoker(const IInvokerPtr& invoker)
    {
        WaitFor(BIND([] { }).AsyncVia(invoker).Run())
            .ThrowOnError();
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TChaosCacheServiceTest, AggregatesChaosLeaseWatches)
{
    auto firstWatch = WatchChaosLease(MinTimestamp);
    auto secondWatch = WatchChaosLease(MinTimestamp);

    WaitForSingleUpstreamWatch();
    EXPECT_EQ(MinTimestamp, UpstreamService_->GetPendingChaosLeaseTimestamp());

    const auto responseTimestamp = TTimestamp(12345);
    const auto timeout = TDuration::Seconds(30);
    const auto coordinatorCellIds = std::vector<TCellId>{
        MakeRandomId(EObjectType::ChaosCell, TCellTag(0xf003)),
        MakeRandomId(EObjectType::ChaosCell, TCellTag(0xf004)),
    };
    UpstreamService_->ReplyChaosLeaseChanged(responseTimestamp, timeout, coordinatorCellIds);

    auto firstResponse = WaitFor(firstWatch)
        .ValueOrThrow();
    auto secondResponse = WaitFor(secondWatch)
        .ValueOrThrow();
    ASSERT_TRUE(firstResponse->has_chaos_lease_changed());
    ASSERT_TRUE(secondResponse->has_chaos_lease_changed());
    EXPECT_EQ(
        responseTimestamp,
        FromProto<TTimestamp>(firstResponse->chaos_lease_changed().chaos_lease_cache_timestamp()));
    EXPECT_EQ(
        responseTimestamp,
        FromProto<TTimestamp>(secondResponse->chaos_lease_changed().chaos_lease_cache_timestamp()));
    EXPECT_EQ(timeout, FromProto<TDuration>(firstResponse->chaos_lease_changed().timeout()));

    EXPECT_EQ(
        coordinatorCellIds,
        FromProto<std::vector<TCellId>>(firstResponse->chaos_lease_changed().coordinator_cell_ids()));
    EXPECT_EQ(
        coordinatorCellIds,
        FromProto<std::vector<TCellId>>(secondResponse->chaos_lease_changed().coordinator_cell_ids()));

    WaitForSingleUpstreamWatch();
    EXPECT_EQ(responseTimestamp, UpstreamService_->GetPendingChaosLeaseTimestamp());

    auto thirdWatch = WatchChaosLease(responseTimestamp);
    auto fourthWatch = WatchChaosLease(responseTimestamp);
    WaitForSingleUpstreamWatch();
    UpstreamService_->ReplyChaosLeaseDeleted();

    auto thirdResponse = WaitFor(thirdWatch)
        .ValueOrThrow();
    auto fourthResponse = WaitFor(fourthWatch)
        .ValueOrThrow();
    EXPECT_TRUE(thirdResponse->has_chaos_lease_deleted());
    EXPECT_TRUE(fourthResponse->has_chaos_lease_deleted());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChaosCache
