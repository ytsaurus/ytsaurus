#include <yt/yt/ytlib/chaos_client/chaos_leases_watcher_client.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/test_framework/chaos_client.h>
#include <yt/yt/ytlib/test_framework/chaos_node_service.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/test_framework/test_proxy_service.h>

namespace NYT::NChaosClient {
namespace {

using namespace NConcurrency;
using namespace NObjectClient;
using namespace NRpc;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger TestLogger("ChaosLeasesWatcherClientTest");

constexpr auto ServiceAddress = "chaos-cache";

////////////////////////////////////////////////////////////////////////////////

class TChaosLeasesWatcherClientTest
    : public ::testing::Test
{
protected:
    const TChaosLeaseId ChaosLeaseId_ = MakeRandomId(EObjectType::ChaosLease, TCellTag(0xf001));

    TActionQueuePtr ConnectionQueue_;
    TActionQueuePtr ServiceQueue_;
    INodeMemoryTrackerPtr MemoryTracker_;
    TTestChaosNodeServicePtr Service_;
    TTestChaosResidencyCachePtr ResidencyCache_;
    TTestChaosConnectionPtr Connection_;
    IChannelPtr Channel_;
    TTestChaosLeaseWatcherClientCallbackStatePtr CallbackState_;
    IChaosLeasesWatcherClientPtr WatcherClient_;

    void SetUp() override
    {
        ConnectionQueue_ = New<TActionQueue>("ChaosLeaseWatcherClientConnection");
        ServiceQueue_ = New<TActionQueue>("ChaosLeaseWatcherClientService");
        MemoryTracker_ = CreateNodeMemoryTracker(32_MB, New<TNodeMemoryTrackerConfig>(), {});
        Service_ = New<TTestChaosNodeService>(ServiceQueue_->GetInvoker(), TestLogger);
        ResidencyCache_ = New<TTestChaosResidencyCache>();
        CallbackState_ = New<TTestChaosLeaseWatcherClientCallbackState>();

        THashMap<std::string, IServicePtr> addressToService;
        addressToService[ServiceAddress] = Service_;
        auto channelFactory = CreateTestChannelFactory(
            addressToService,
            THashMap<std::string, IServicePtr>{});
        Channel_ = channelFactory->CreateChannel(ServiceAddress);
        Connection_ = New<TTestChaosConnection>(
            std::move(channelFactory),
            Channel_,
            ConnectionQueue_->GetInvoker(),
            MemoryTracker_,
            ResidencyCache_);
    }

    void TearDown() override
    {
        // Complete pending RPCs while service and callback invokers are alive.
        DrainInvoker(ConnectionQueue_->GetInvoker());
        DrainInvoker(ServiceQueue_->GetInvoker());
        Service_->ReplyAllChaosLeaseWatchesDeleted();
        DrainInvoker(ConnectionQueue_->GetInvoker());

        WatcherClient_ = nullptr;
        Channel_ = nullptr;
        Connection_ = nullptr;
        ResidencyCache_ = nullptr;
        Service_ = nullptr;
        CallbackState_ = nullptr;

        ServiceQueue_->Shutdown(/*graceful*/ true);
        ConnectionQueue_->Shutdown(/*graceful*/ true);

        MemoryTracker_->ClearTrackers();
        MemoryTracker_ = nullptr;
    }

    void CreateWatcherClient()
    {
        auto callbacks = CreateTestChaosLeaseWatcherClientCallbacks(CallbackState_);
        WatcherClient_ = CreateChaosLeasesWatcherClient(
            std::move(callbacks),
            Channel_,
            Connection_);
    }

    void WaitForPendingWatch()
    {
        WaitFor(Service_->GetChaosLeaseWatchReceivedFuture())
            .ThrowOnError();
        EXPECT_EQ(1, Service_->GetPendingChaosLeaseWatchCount());
    }

    static void DrainInvoker(const IInvokerPtr& invoker)
    {
        WaitFor(BIND([] { }).AsyncVia(invoker).Run())
            .ThrowOnError();
    }

    void WatchChaosLease()
    {
        WaitFor(BIND([this] {
            WatcherClient_->WatchChaosLease(ChaosLeaseId_);
        }).AsyncVia(ConnectionQueue_->GetInvoker()).Run())
            .ThrowOnError();
    }

    void DrainQueues()
    {
        DrainInvoker(ConnectionQueue_->GetInvoker());
        DrainInvoker(ServiceQueue_->GetInvoker());
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TChaosLeasesWatcherClientTest, ChangedAndNotChangedRearmWithCurrentTimestamp)
{
    CreateWatcherClient();

    WatchChaosLease();
    WaitForPendingWatch();
    EXPECT_EQ(ChaosLeaseId_, Service_->GetPendingChaosLeaseId());
    EXPECT_EQ(MinTimestamp, Service_->GetPendingChaosLeaseTimestamp());

    WatchChaosLease();
    DrainQueues();
    EXPECT_EQ(1, Service_->GetPendingChaosLeaseWatchCount());

    const auto responseTimestamp = TTimestamp(12345);
    const auto timeout = TDuration::Seconds(30);
    const auto coordinatorCellIds = std::vector<TCellId>{
        MakeRandomId(EObjectType::ChaosCell, TCellTag(0xf002)),
        MakeRandomId(EObjectType::ChaosCell, TCellTag(0xf003)),
    };
    Service_->ReplyChaosLeaseChanged(responseTimestamp, timeout, coordinatorCellIds);

    WaitFor(CallbackState_->GetUpdatedFuture())
        .ThrowOnError();
    WaitForPendingWatch();
    auto update = CallbackState_->GetLastUpdate();
    ASSERT_TRUE(update);
    EXPECT_EQ(ChaosLeaseId_, update->ChaosLeaseId);
    EXPECT_EQ(responseTimestamp, update->Timestamp);
    EXPECT_EQ(timeout, update->ChaosLease->Timeout);
    EXPECT_EQ(coordinatorCellIds, update->ChaosLease->CoordinatorCellIds);
    EXPECT_EQ(ChaosLeaseId_, ResidencyCache_->GetLastPingedObjectId());
    EXPECT_EQ(responseTimestamp, Service_->GetPendingChaosLeaseTimestamp());

    Service_->ReplyChaosLeaseNotChanged();
    WaitFor(CallbackState_->GetUnchangedFuture())
        .ThrowOnError();
    WaitForPendingWatch();
    EXPECT_EQ(ChaosLeaseId_, CallbackState_->GetLastUnchangedChaosLeaseId());
    EXPECT_EQ(responseTimestamp, Service_->GetPendingChaosLeaseTimestamp());
}

TEST_F(TChaosLeasesWatcherClientTest, UnknownAndDeletedTerminateWatch)
{
    CreateWatcherClient();

    WatchChaosLease();
    WaitForPendingWatch();
    Service_->ReplyChaosLeaseUnknown();
    WaitFor(CallbackState_->GetUnknownFuture())
        .ThrowOnError();
    EXPECT_EQ(ChaosLeaseId_, CallbackState_->GetLastUnknownChaosLeaseId());
    EXPECT_EQ(0, Service_->GetPendingChaosLeaseWatchCount());

    WatchChaosLease();
    WaitForPendingWatch();
    EXPECT_EQ(MinTimestamp, Service_->GetPendingChaosLeaseTimestamp());
    Service_->ReplyChaosLeaseDeleted();
    WaitFor(CallbackState_->GetDeletedFuture())
        .ThrowOnError();
    EXPECT_EQ(ChaosLeaseId_, CallbackState_->GetLastDeletedChaosLeaseId());
    EXPECT_EQ(0, Service_->GetPendingChaosLeaseWatchCount());
}

TEST_F(TChaosLeasesWatcherClientTest, RefreshesResidencyAfterRoutingChanges)
{
    CreateWatcherClient();

    WatchChaosLease();
    WaitForPendingWatch();

    auto destinationCellId = MakeRandomId(EObjectType::ChaosCell, TCellTag(0xf004));
    Service_->ReplyChaosLeaseMigrated(destinationCellId);
    WaitFor(CallbackState_->GetMigratedFuture())
        .ThrowOnError();
    WaitForPendingWatch();
    EXPECT_EQ(ChaosLeaseId_, CallbackState_->GetLastMigratedChaosLeaseId());
    EXPECT_EQ(
        std::make_pair(TObjectId(ChaosLeaseId_), CellTagFromId(destinationCellId)),
        ResidencyCache_->GetLastUpdatedResidency());

    Service_->ReplyChaosLeaseInstanceIsNotLeader();
    WaitFor(CallbackState_->GetUnchangedFuture())
        .ThrowOnError();
    WaitForPendingWatch();
    EXPECT_EQ(ChaosLeaseId_, CallbackState_->GetLastUnchangedChaosLeaseId());
    EXPECT_EQ(ChaosLeaseId_, ResidencyCache_->GetLastRemovedObjectId());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChaosClient
