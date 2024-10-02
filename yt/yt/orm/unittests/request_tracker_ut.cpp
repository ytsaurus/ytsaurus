#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/orm/server/access_control/config.h>
#include <yt/yt/orm/server/access_control/request_tracker.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NOrm::NServer::NAccessControl::NTests {
namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr bool Soft = false;

TEST(TRequestTrackerTest, Generic)
{
    TRequestTrackerPtr tracker;
    {
        auto config = New<TRequestTrackerConfig>();
        config->Enabled = true;
        config->ReconfigureBatchSize = 500;

        tracker = New<TRequestTracker>(std::move(config));
    }

    EXPECT_THROW_WITH_SUBSTRING(
        YT_UNUSED_FUTURE(tracker->ThrottleUserRequest("fake_user", 1, Soft)),
        "Request throttlers for user");

    EXPECT_THROW_WITH_SUBSTRING(
        tracker->TryIncreaseRequestQueueSize("fake_user", 1, Soft),
        "Request throttlers for user");

    NObjects::TObjectId fastJack{"jack"};
    NObjects::TObjectId slowLoris{"loris"};
    tracker->ReconfigureUsersBatch({
        {fastJack,  0, 0},  // NB: All unlimited.
        {slowLoris, 1, 1}});

    {
        tracker->TryIncreaseRequestQueueSize(fastJack, 10, Soft);
        tracker->DecreaseRequestQueueSize(fastJack, 10);
        auto rv = tracker->ThrottleUserRequest(fastJack, 1000, Soft);
        EXPECT_TRUE(rv.IsSet());
        Y_UNUSED(rv.Get());
    }

    {
        EXPECT_TRUE(tracker->TryIncreaseRequestQueueSize(slowLoris, 1, Soft));
        EXPECT_FALSE(tracker->TryIncreaseRequestQueueSize(slowLoris, 1, Soft));
        tracker->ReconfigureUsersBatch({{slowLoris, 1, 2}});
        EXPECT_TRUE(tracker->TryIncreaseRequestQueueSize(slowLoris, 1, Soft));
        EXPECT_FALSE(tracker->TryIncreaseRequestQueueSize(slowLoris, 1, Soft));
        tracker->DecreaseRequestQueueSize(slowLoris, 2);
        auto rv1 = tracker->ThrottleUserRequest(slowLoris, 10, Soft);
        EXPECT_TRUE(rv1.IsSet());
        Y_UNUSED(rv1.Get());
        auto rv2 = tracker->ThrottleUserRequest(slowLoris, 10, Soft);
        EXPECT_FALSE(rv2.IsSet());
        tracker->ReconfigureUsersBatch({
            {slowLoris,  0, 0}});
        Y_UNUSED(rv2.Get());
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TRequestTrackerTest, SecondLimit)
{
    TRequestTrackerPtr tracker;
    {
        auto config = New<TRequestTrackerConfig>();
        config->Enabled = true;

        tracker = New<TRequestTracker>(std::move(config));
    }

    NObjects::TObjectId alice{"alice"};
    tracker->ReconfigureUsersBatch({{alice, 1, 1}});

    // Start tx and make counter and weight requets throttlers counters equal zero
    EXPECT_TRUE(tracker->TryIncreaseRequestQueueSize(alice, 1, Soft));
    auto txStart = tracker->ThrottleUserRequest(alice, 1, Soft);
    EXPECT_TRUE(txStart.IsSet());
    Y_UNUSED(txStart.Get());

    // We can make request despite negative counter.
    EXPECT_TRUE(tracker->TryIncreaseRequestQueueSize(alice, 5, !Soft));
    auto requestOfFirstTx = tracker->ThrottleUserRequest(alice, 5, !Soft);
    EXPECT_TRUE(requestOfFirstTx.IsSet());
    Y_UNUSED(requestOfFirstTx.Get());

    // Exceed second limit.
    EXPECT_FALSE(tracker->TryIncreaseRequestQueueSize(alice, 100500, !Soft));
    auto requestWithHugeRequirements = tracker->ThrottleUserRequest(alice, 100500, !Soft);
    EXPECT_FALSE(requestWithHugeRequirements.IsSet());
    tracker->ReconfigureUsersBatch({{alice,  0, 0}});
    Y_UNUSED(requestWithHugeRequirements.Get());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TRequestTrackerTest, ThrottleNewTransactionIfHasDebt)
{
    TRequestTrackerPtr tracker;
    {
        auto config = New<TRequestTrackerConfig>();
        config->Enabled = true;

        tracker = New<TRequestTracker>(std::move(config));
    }

    NObjects::TObjectId alice{"alice"};
    tracker->ReconfigureUsersBatch({{alice, 1, 1}});

    EXPECT_TRUE(tracker->TryIncreaseRequestQueueSize(alice, 1, Soft));
    auto firstTxStart = tracker->ThrottleUserRequest(alice, 1, Soft);
    EXPECT_TRUE(firstTxStart.IsSet());
    Y_UNUSED(firstTxStart.Get());

    EXPECT_TRUE(tracker->TryIncreaseRequestQueueSize(alice, 2, !Soft));
    auto requestOfFirstTx = tracker->ThrottleUserRequest(alice, 2, !Soft);
    EXPECT_TRUE(requestOfFirstTx.IsSet());
    Y_UNUSED(requestOfFirstTx.Get());

    EXPECT_FALSE(tracker->TryIncreaseRequestQueueSize(alice, 2, Soft));
    auto secondTxStart = tracker->ThrottleUserRequest(alice, 2, Soft);
    EXPECT_FALSE(secondTxStart.IsSet());
    tracker->ReconfigureUsersBatch({{alice,  0, 0}});
    Y_UNUSED(secondTxStart.Get());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NServer::NAccessControl::NTests
