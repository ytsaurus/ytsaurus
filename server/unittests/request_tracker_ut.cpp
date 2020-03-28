#include <yt/core/test_framework/framework.h>

#include <yp/server/access_control/config.h>
#include <yp/server/access_control/request_tracker.h>

#include <yt/core/actions/future.h>

namespace NYP::NServer::NAccessControl::NTests {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TRequestTracker, Generic)
{
    TRequestTrackerPtr tracker;
    {
        auto config = New<TRequestTrackerConfig>();
        config->SetDefaults();
        config->Enabled = true;
        config->ReconfigureBatchSize = 500;

        tracker = New<TRequestTracker>(std::move(config));
    }

    EXPECT_THROW_WITH_SUBSTRING(
        tracker->ThrottleUserRequest("fake_user", 1),
        "Request throttlers for user");

    EXPECT_THROW_WITH_SUBSTRING(
        tracker->TryIncreaseRequestQueueSize("fake_user", 1),
        "Request throttlers for user");

    NObjects::TObjectId fastJack{"jack"};
    NObjects::TObjectId slowLoris{"loris"};
    tracker->ReconfigureUsersBatch({
        {fastJack,  0, 0},  // NB: all unlimited
        {slowLoris, 1, 1}});

    {
        tracker->TryIncreaseRequestQueueSize(fastJack, 10);
        tracker->DecreaseRequestQueueSize(fastJack, 10);
        const auto& rv = tracker->ThrottleUserRequest(fastJack, 1000);
        EXPECT_TRUE(rv.IsSet());
        Y_UNUSED(rv.Get());
    }

    {
        EXPECT_TRUE(tracker->TryIncreaseRequestQueueSize(slowLoris, 1));
        EXPECT_FALSE(tracker->TryIncreaseRequestQueueSize(slowLoris, 1));
        tracker->ReconfigureUsersBatch({{slowLoris, 1, 2}});
        EXPECT_TRUE(tracker->TryIncreaseRequestQueueSize(slowLoris, 1));
        EXPECT_FALSE(tracker->TryIncreaseRequestQueueSize(slowLoris, 1));
        tracker->DecreaseRequestQueueSize(slowLoris, 2);
        const auto& rv1 = tracker->ThrottleUserRequest(slowLoris, 10);
        EXPECT_TRUE(rv1.IsSet());
        Y_UNUSED(rv1.Get());
        const auto& rv2 = tracker->ThrottleUserRequest(slowLoris, 10);
        EXPECT_FALSE(rv2.IsSet());
        tracker->ReconfigureUsersBatch({
            {slowLoris,  0, 0}});
        Y_UNUSED(rv2.Get());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYP::NServer::NAccessControl::NTests
