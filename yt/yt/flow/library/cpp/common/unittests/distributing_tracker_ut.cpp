#include <yt/yt/flow/library/cpp/common/distributing_tracker.h>

#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

const TCallback<void()> NoOpCallback = BIND_NO_PROPAGATE([] {
});

////////////////////////////////////////////////////////////////////////////////

TEST(TDistributingTrackerTest, NoCallbacks)
{
    auto fired = std::make_shared<bool>(false);
    auto tracker = TDistributingTracker([fired] {
        *fired = true;
    });
    tracker.Activate();
    EXPECT_TRUE(*fired);
}

TEST(TDistributingTrackerTest, OneCallback)
{
    auto fired = std::make_shared<bool>(false);
    auto tracker = TDistributingTracker([fired] {
        *fired = true;
    });
    auto callback = tracker.AddDestination();
    tracker.Activate();
    EXPECT_FALSE(*fired);
    callback();
    EXPECT_TRUE(*fired);
}

TEST(TDistributingTrackerTest, MultipleCallbacks)
{
    auto fired = std::make_shared<bool>(false);
    auto tracker = TDistributingTracker([fired] {
        *fired = true;
    });
    auto callback1 = tracker.AddDestination();
    auto callback2 = tracker.AddDestination();
    auto callback3 = tracker.AddDestination();
    tracker.Activate();
    EXPECT_FALSE(*fired);
    callback1();
    EXPECT_FALSE(*fired);
    callback2();
    EXPECT_FALSE(*fired);
    callback3();
    EXPECT_TRUE(*fired);
}

TEST(TDistributingTrackerTest, CallbackCalledAfterActivate)
{
    auto fired = std::make_shared<bool>(false);
    auto tracker = TDistributingTracker([fired] {
        *fired = true;
    });
    auto callback = tracker.AddDestination();
    tracker.Activate();
    EXPECT_FALSE(*fired);
    callback();
    EXPECT_TRUE(*fired);
}

TEST(TDistributingTrackerTest, DropCallbackWithoutCall)
{
    auto fired = std::make_shared<bool>(false);
    auto tracker = TDistributingTracker([fired] {
        *fired = true;
    });
    {
        auto callback = tracker.AddDestination();
        tracker.Activate();
        EXPECT_FALSE(*fired);
    }
    EXPECT_FALSE(*fired);
}

TEST(TDistributingTrackerDeathTest, DoubleCallbackCall)
{
    auto tracker = TDistributingTracker(NoOpCallback);
    auto callback = tracker.AddDestination();
    tracker.Activate();
    callback();
    EXPECT_DEATH({ callback(); }, "");
}

TEST(TDistributingTrackerDeathTest, DoubleActivate)
{
    auto tracker = TDistributingTracker(NoOpCallback);
    tracker.Activate();
    EXPECT_DEATH({ tracker.Activate(); }, "");
}

TEST(TDistributingTrackerDeathTest, AddDestinationAfterActivate)
{
    auto tracker = TDistributingTracker(NoOpCallback);
    tracker.Activate();
    EXPECT_DEATH({ tracker.AddDestination(); }, "");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TDistributingTrackerTest, TrackerBoolOperator)
{
    auto tracker = TDistributingTracker(NoOpCallback);
    EXPECT_TRUE(static_cast<bool>(tracker));
    tracker.Activate();
    EXPECT_FALSE(static_cast<bool>(tracker));
}

TEST(TDistributingTrackerTest, DefaultConstructedTrackerIsFalse)
{
    TDistributingTracker tracker;
    EXPECT_FALSE(static_cast<bool>(tracker));
}

TEST(TDistributingTrackerTest, CallbackBoolOperator)
{
    auto fired = std::make_shared<bool>(false);
    auto tracker = TDistributingTracker([fired] {
        *fired = true;
    });
    auto callback = tracker.AddDestination();
    tracker.Activate();

    EXPECT_TRUE(static_cast<bool>(callback));
    callback();
    EXPECT_FALSE(static_cast<bool>(callback));
}

TEST(TDistributingTrackerTest, DefaultConstructedCallbackIsFalse)
{
    TOnDistributedCallback callback;
    EXPECT_FALSE(static_cast<bool>(callback));
}

TEST(TDistributingTrackerTest, MoveTracker)
{
    auto fired = std::make_shared<bool>(false);
    auto tracker = TDistributingTracker([fired] {
        *fired = true;
    });
    auto callback = tracker.AddDestination();

    auto tracker2 = std::move(tracker);
    EXPECT_FALSE(static_cast<bool>(tracker)); // NOLINT(bugprone-use-after-move)
    EXPECT_TRUE(static_cast<bool>(tracker2));

    tracker2.Activate();
    EXPECT_FALSE(*fired);
    callback();
    EXPECT_TRUE(*fired);
}

TEST(TDistributingTrackerTest, MoveCallback)
{
    auto fired = std::make_shared<bool>(false);
    auto tracker = TDistributingTracker([fired] {
        *fired = true;
    });
    auto callback = tracker.AddDestination();
    tracker.Activate();

    auto callback2 = std::move(callback);
    EXPECT_FALSE(static_cast<bool>(callback)); // NOLINT(bugprone-use-after-move)
    EXPECT_TRUE(static_cast<bool>(callback2));

    EXPECT_FALSE(*fired);
    callback2();
    EXPECT_TRUE(*fired);
}

TEST(TDistributingTrackerTest, FromCallback)
{
    auto fired = std::make_shared<bool>(false);
    auto callback = TOnDistributedCallback::FromCallback([fired] {
        *fired = true;
    });
    EXPECT_TRUE(static_cast<bool>(callback));
    EXPECT_FALSE(*fired);
    callback();
    EXPECT_TRUE(*fired);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
