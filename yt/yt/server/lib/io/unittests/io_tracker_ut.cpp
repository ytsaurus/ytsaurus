#include <yt/yt/server/lib/io/config.h>
#include <yt/yt/server/lib/io/io_tracker.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/atomic_object.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <util/string/cast.h>

namespace NYT::NIO {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TIOEvent CreateEvent(
    i64 byteCount,
    i64 ioCount,
    TString firstTagName = "",
    TString firstTagValue = "",
    TString secondTagName = "",
    TString secondTagValue = "")
{
    TIOEvent ioEvent;
    ioEvent.Counters.ByteCount = byteCount;
    ioEvent.Counters.IOCount = ioCount;
    if (!firstTagName.empty()) {
        EmplaceOrCrash(ioEvent.LocalTags, std::move(firstTagName), std::move(firstTagValue));
    }
    if (!secondTagName.empty()) {
        EmplaceOrCrash(ioEvent.LocalTags, std::move(secondTagName), std::move(secondTagValue));
    }
    return ioEvent;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TIOTrackerTest, Simple)
{
    auto config = New<TIOTrackerConfig>();
    config->Enable = true;
    config->EnableRaw = true;
    auto ioTracker = CreateIOTracker(std::move(config));

    std::atomic<int> eventCount = 0;
    std::atomic<int> totalByteCount = 0;
    std::atomic<int> totalIOCount = 0;
    std::atomic<int> totalTagCount = 0;

    ioTracker->SubscribeOnRawEventLogged(BIND([&] (const TIOCounters& counters, const TIOTagList& tags) {
        eventCount += 1;
        totalByteCount += counters.ByteCount;
        totalIOCount += counters.IOCount;
        totalTagCount += ssize(tags);
    }));

    ioTracker->Enqueue(CreateEvent(3, 5, "key@", "value", "key2@", "value2"));
    ioTracker->Enqueue(CreateEvent(1, 1, "key@", "value", "key3", "value3"));
    ioTracker->Enqueue(CreateEvent(42, 85, "key@", "value", "key2", "value2"));
    ioTracker->Enqueue(CreateEvent(19, 84, "key@", "value"));
    ioTracker->Enqueue(CreateEvent(1, 1, "key2", "value2"));
    ioTracker->Enqueue(CreateEvent(1, 1, "key3", "value3"));

    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));

    EXPECT_EQ(6, eventCount);
    EXPECT_EQ(67, totalByteCount);
    EXPECT_EQ(177, totalIOCount);
    EXPECT_EQ(9, totalTagCount);
}

TEST(TIOTrackerTest, QueueOverflow)
{
    auto config = New<TIOTrackerConfig>();
    config->Enable = true;
    config->EnableRaw = true;
    config->QueueSizeLimit = 10;
    auto ioTracker = CreateIOTracker(std::move(config));

    std::atomic<int> eventCount = 0;
    std::atomic<int> goodEventCount = 0;

    ioTracker->SubscribeOnRawEventLogged(BIND([&] (const TIOCounters&, const TIOTagList& tags) {
        eventCount += 1;
        if (tags[0].second == "true") {
            goodEventCount += 1;
        }
    }));

    for (int i = 0; i < 50; ++i) {
        ioTracker->Enqueue(CreateEvent(1, 1, "good", i < 10 ? "true" : "false"));
    }

    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));

    EXPECT_EQ(10, eventCount);
    // Verify that reordering doesn't happen. We are unsure whether this guarantee is actually needed, but
    // the code currently doesn't reorder event. So, we verify this guarantee here.
    EXPECT_EQ(10, goodEventCount);
}

TEST(TIOTrackerTest, Aggregate)
{
    auto config = New<TIOTrackerConfig>();
    config->Enable = true;
    config->AggregationPeriod = TDuration::MilliSeconds(500);
    auto ioTracker = CreateIOTracker(std::move(config));

    std::atomic<int> eventCount = 0;
    std::atomic<int> totalByteCount = 0;
    std::atomic<int> totalIOCount = 0;
    std::atomic<int> totalTagCount = 0;

    ioTracker->SubscribeOnAggregateEventLogged(BIND([&] (const TIOCounters& counters, const TIOTagList& tags) {
        eventCount += 1;
        totalByteCount += counters.ByteCount;
        totalIOCount += counters.IOCount;
        totalTagCount += ssize(tags);
    }));

    ioTracker->Enqueue(CreateEvent(3, 5, "key@", "value", "key2@", "value2"));
    ioTracker->Enqueue(CreateEvent(1, 1, "key@", "value", "key3", "value3"));
    ioTracker->Enqueue(CreateEvent(42, 85, "key@", "value", "key2", "value2"));
    ioTracker->Enqueue(CreateEvent(19, 84, "key@", "value"));
    ioTracker->Enqueue(CreateEvent(1, 1, "key2", "value2"));
    ioTracker->Enqueue(CreateEvent(1, 1, "key3", "value3"));

    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(750));

    EXPECT_EQ(3, eventCount);
    EXPECT_EQ(67, totalByteCount);
    EXPECT_EQ(177, totalIOCount);
    EXPECT_EQ(3, totalTagCount);
}

TEST(TIOTrackerTest, Concurrent)
{
    auto config = New<TIOTrackerConfig>();
    config->Enable = true;
    config->EnableRaw = true;
    config->QueueSizeLimit = 10'000;
    auto ioTracker = CreateIOTracker(std::move(config));

    const int threadCount = 8;
    const int enqueuePerThreadCount = 200;
    std::vector<std::atomic<int>> eventsByIter(threadCount);

    ioTracker->SubscribeOnRawEventLogged(BIND([&] (const TIOCounters&, const TIOTagList& tags) {
        eventsByIter[FromString(tags[0].second)] += 1;
    }));

    auto threadPool = New<TThreadPool>(threadCount, "TrackerTestThread");
    std::vector<TFuture<void>> asyncResults;
    for (int i = 0; i < threadCount; ++i) {
        auto asyncResult = BIND([&, i] () {
            for (int j = 0; j < enqueuePerThreadCount; ++j) {
                ioTracker->Enqueue(CreateEvent(1, 1, "iter", ToString(i)));
            }
        })
            .AsyncVia(threadPool->GetInvoker())
            .Run();
        asyncResults.push_back(std::move(asyncResult));
    }
    AllSucceeded(asyncResults)
        .Get()
        .ThrowOnError();
    threadPool->Shutdown();

    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));

    for (int i = 0; i < threadCount; ++i) {
        EXPECT_EQ(enqueuePerThreadCount, eventsByIter[i]);
    }
}

TEST(TIOTrackerTest, Disable)
{
    auto createConfig = [](bool enable, bool enableRaw) {
        auto config = New<TIOTrackerConfig>();
        config->Enable = enable;
        config->EnableRaw = enableRaw;
        config->AggregationPeriod = TDuration::MilliSeconds(100);
        return config;
    };

    auto ioTracker = CreateIOTracker(createConfig(false, true));

    std::atomic<int> rawEventCount = 0;
    std::atomic<int> aggregateEventCount = 0;

    ioTracker->SubscribeOnRawEventLogged(BIND([&] (const TIOCounters&, const TIOTagList&) {
        rawEventCount += 1;
    }));
    ioTracker->SubscribeOnAggregateEventLogged(BIND([&] (const TIOCounters&, const TIOTagList&) {
        aggregateEventCount += 1;
    }));

    auto addEventsAndWait = [&] {
        for (int i = 0; i < 5; ++i) {
            ioTracker->Enqueue(CreateEvent(1, 1, "tag@", ToString(i)));
        }
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));
    };

    addEventsAndWait();
    EXPECT_EQ(0, rawEventCount);
    EXPECT_EQ(0, aggregateEventCount);

    ioTracker->SetConfig(createConfig(true, true));
    addEventsAndWait();
    EXPECT_EQ(5, rawEventCount);
    EXPECT_EQ(5, aggregateEventCount);

    ioTracker->SetConfig(createConfig(true, false));
    addEventsAndWait();
    EXPECT_EQ(5, rawEventCount);
    EXPECT_EQ(10, aggregateEventCount);

    ioTracker->SetConfig(createConfig(false, false));
    addEventsAndWait();
    EXPECT_EQ(5, rawEventCount);
    EXPECT_EQ(10, aggregateEventCount);
}

TEST(TIOTrackerTest, Baggage)
{
    auto config = New<TIOTrackerConfig>();
    config->Enable = true;
    config->EnableRaw = true;
    config->AggregationPeriod = TDuration::MilliSeconds(100);
    auto ioTracker = CreateIOTracker(std::move(config));

    TAtomicObject<TIOTagList> rawTagList;
    TAtomicObject<TIOTagList> aggregateTagList;

    ioTracker->SubscribeOnRawEventLogged(BIND([&] (const TIOCounters&, const TIOTagList& list) {
        rawTagList.Store(std::move(list));
    }));
    ioTracker->SubscribeOnAggregateEventLogged(BIND([&] (const TIOCounters&, const TIOTagList& list) {
        aggregateTagList.Store(std::move(list));
    }));

    TIOEvent event;
    event.Counters = TIOCounters{
        .ByteCount = 1,
        .IOCount = 1
    };
    auto attributes = NYTree::CreateEphemeralAttributes();
    attributes->Set("firstKey", "firstValue");
    attributes->Set("nonStringKey", 42);
    attributes->Set("secondKey@", "secondValue");
    attributes->Set("boolKey", true);
    attributes->Set("sharedKey", "sharedValue");
    event.LocalTags.emplace("thirdKey", "thirdValue");
    event.LocalTags.emplace("fourthKey@", "fourthValue");
    event.LocalTags.emplace("sharedKey", "sharedValue");
    event.Baggage = NYTree::ConvertToYsonString(attributes);

    ioTracker->Enqueue(std::move(event));
    NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));

    {
        TIOTagList expected = {
            {"firstKey", "firstValue"},
            {"secondKey@", "secondValue"},
            {"thirdKey", "thirdValue"},
            {"fourthKey@", "fourthValue"},
            {"sharedKey", "sharedValue"}
        };
        auto found = rawTagList.Load();
        sort(expected.begin(), expected.end());
        sort(found.begin(), found.end());
        EXPECT_EQ(expected, found);
    }

    {
        TIOTagList expected = {
            {"secondKey@", "secondValue"},
            {"fourthKey@", "fourthValue"}
        };
        auto found = aggregateTagList.Load();
        sort(expected.begin(), expected.end());
        sort(found.begin(), found.end());
        EXPECT_EQ(expected, found);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
