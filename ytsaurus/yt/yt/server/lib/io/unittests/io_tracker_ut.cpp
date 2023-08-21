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
    i64 bytes,
    i64 ioRequests,
    TString firstTagName = "",
    TString firstTagValue = "",
    TString secondTagName = "",
    TString secondTagValue = "",
    TString thirdTagName = "",
    TString thirdTagValue = "")
{
    TIOEvent ioEvent;
    ioEvent.Counters.Bytes = bytes;
    ioEvent.Counters.IORequests = ioRequests;
    if (!firstTagName.empty()) {
        EmplaceOrCrash(ioEvent.LocalTags, std::move(firstTagName), std::move(firstTagValue));
    }
    if (!secondTagName.empty()) {
        EmplaceOrCrash(ioEvent.LocalTags, std::move(secondTagName), std::move(secondTagValue));
    }
    if (!thirdTagName.empty()) {
        EmplaceOrCrash(ioEvent.LocalTags, std::move(thirdTagName), std::move(thirdTagValue));
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

    std::atomic<int> events = 0;
    std::atomic<int> totalBytes = 0;
    std::atomic<int> totalIORequests = 0;
    std::atomic<int> totalTags = 0;

    ioTracker->SubscribeOnRawEventLogged(BIND([&] (const TIOCounters& counters, const TIOTagList& tags) {
        events += 1;
        totalBytes += counters.Bytes;
        totalIORequests += counters.IORequests;
        totalTags += ssize(tags);
    }));

    ioTracker->Enqueue(CreateEvent(3, 5, "key@", "value", "key2@", "value2"));
    ioTracker->Enqueue(CreateEvent(1, 1, "key@", "value", "key3", "value3"));
    ioTracker->Enqueue(CreateEvent(42, 85, "key@", "value", "key2", "value2"));
    ioTracker->Enqueue(CreateEvent(19, 84, "key@", "value"));
    ioTracker->Enqueue(CreateEvent(1, 1, "key2", "value2"));
    ioTracker->Enqueue(CreateEvent(1, 1, "key3", "value3"));

    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));

    EXPECT_EQ(6, events);
    EXPECT_EQ(67, totalBytes);
    EXPECT_EQ(177, totalIORequests);
    EXPECT_EQ(9, totalTags);
}

TEST(TIOTrackerTest, QueueOverflow)
{
    auto config = New<TIOTrackerConfig>();
    config->Enable = true;
    config->EnableRaw = true;
    config->EnableEventDequeue = false;
    config->QueueSizeLimit = 10;
    auto ioTracker = CreateIOTracker(config);

    std::atomic<int> eventCount = 0;
    std::atomic<int> goodEventCount = 0;

    ioTracker->SubscribeOnRawEventLogged(BIND([&] (const TIOCounters& /*counters*/, const TIOTagList& tags) {
        eventCount += 1;
        if (tags[0].second == "true") {
            goodEventCount += 1;
        }
    }));

    for (int i = 0; i < 50; ++i) {
        ioTracker->Enqueue(CreateEvent(1, 1, "good", i < 10 ? "true" : "false"));
    }

    auto newConfig = CloneYsonStruct(config);
    newConfig->EnableEventDequeue = true;
    ioTracker->SetConfig(std::move(newConfig));

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

    std::atomic<int> events = 0;
    std::atomic<int> totalBytes = 0;
    std::atomic<int> totalIORequests = 0;
    std::atomic<int> totalTags = 0;

    ioTracker->SubscribeOnAggregateEventLogged(BIND([&] (const TIOCounters& counters, const TIOTagList& tags) {
        events += 1;
        totalBytes += counters.Bytes;
        totalIORequests += counters.IORequests;
        totalTags += ssize(tags);
    }));

    ioTracker->Enqueue(CreateEvent(3, 5, "key@", "value", "key2@", "value2"));
    ioTracker->Enqueue(CreateEvent(1, 1, "key@", "value", "key3", "value3"));
    ioTracker->Enqueue(CreateEvent(42, 85, "key@", "value", "key2", "value2"));
    ioTracker->Enqueue(CreateEvent(19, 84, "key@", "value"));
    ioTracker->Enqueue(CreateEvent(1, 1, "key2", "value2"));
    ioTracker->Enqueue(CreateEvent(1, 1, "key3", "value3"));

    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(750));

    EXPECT_EQ(3, events);
    EXPECT_EQ(67, totalBytes);
    EXPECT_EQ(177, totalIORequests);
    EXPECT_EQ(3, totalTags);
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

    ioTracker->SubscribeOnRawEventLogged(BIND([&] (const TIOCounters& /*counters*/, const TIOTagList& tags) {
        eventsByIter[FromString(tags[0].second)] += 1;
    }));

    auto threadPool = CreateThreadPool(threadCount, "TrackerTestThread");
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

    ioTracker->SubscribeOnRawEventLogged(BIND([&] (const TIOCounters& /*counters*/, const TIOTagList&) {
        rawEventCount += 1;
    }));
    ioTracker->SubscribeOnAggregateEventLogged(BIND([&] (const TIOCounters& /*counters*/, const TIOTagList&) {
        aggregateEventCount += 1;
    }));

    auto setAndWaitForConfig = [&](const TIOTrackerConfigPtr& config) {
        ioTracker->SetConfig(config);
        // Wait until the new config applies. When it happens, GetConfig() will return the
        // same pointer that we passed to SetConfig above. So, it is OK to compare configs by
        // pointer, not by value.
        WaitForPredicate([&] {
            return config == ioTracker->GetConfig();
        });
    };

    auto addEventsAndWait = [&] {
        for (int i = 0; i < 5; ++i) {
            ioTracker->Enqueue(CreateEvent(1, 1, "tag@", ToString(i)));
        }
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));
    };

    addEventsAndWait();
    EXPECT_EQ(0, rawEventCount);
    EXPECT_EQ(0, aggregateEventCount);

    setAndWaitForConfig(createConfig(true, true));
    addEventsAndWait();
    EXPECT_EQ(5, rawEventCount);
    EXPECT_EQ(5, aggregateEventCount);

    setAndWaitForConfig(createConfig(true, false));
    addEventsAndWait();
    EXPECT_EQ(5, rawEventCount);
    EXPECT_EQ(10, aggregateEventCount);

    setAndWaitForConfig(createConfig(false, false));
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

    ioTracker->SubscribeOnRawEventLogged(BIND([&] (const TIOCounters& /*counters*/, const TIOTagList& list) {
        rawTagList.Store(std::move(list));
    }));
    ioTracker->SubscribeOnAggregateEventLogged(BIND([&] (const TIOCounters& /*counters*/, const TIOTagList& list) {
        aggregateTagList.Store(std::move(list));
    }));

    TIOEvent event;
    event.Counters = TIOCounters{
        .Bytes = 1,
        .IORequests = 1
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
    event.Baggage = NYson::ConvertToYsonString(attributes);

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

TEST(TIOTrackerTest, PathAggr)
{
    auto config = New<TIOTrackerConfig>();
    config->Enable = true;
    config->EnableRaw = true;
    config->EnablePath = true;
    config->AggregationPeriod = TDuration::MilliSeconds(100);
    config->PathAggregateTags = {"first@", "third@"};
    auto ioTracker = CreateIOTracker(std::move(config));

    NThreading::TSpinLock lock;
    std::vector<TIOTagList> events;
    int totalBytes = 0;

    ioTracker->SubscribeOnPathAggregateEventLogged(BIND([&] (const TIOCounters& counters, const TIOTagList& list) {
        auto guard = Guard(lock);
        events.push_back(list);
        totalBytes += counters.Bytes;
    }));

    ioTracker->Enqueue(CreateEvent(1, 1, "object_path", "mypath", "first@", "value1", "second@", "a"));
    ioTracker->Enqueue(CreateEvent(1, 1, "object_path", "mypath", "first@", "value1", "second@", "b"));

    ioTracker->Enqueue(CreateEvent(1, 1, "object_path", "mypath", "first@", "value2", "third@", "a"));

    ioTracker->Enqueue(CreateEvent(1, 1, "first@", "value1"));
    ioTracker->Enqueue(CreateEvent(1, 1, "first@", "value1", "second@", "qqq"));
    ioTracker->Enqueue(CreateEvent(1, 1, "first@", "value1", "rawtag", "a"));
    ioTracker->Enqueue(CreateEvent(1, 1, "first@", "value1", "hello", "world", "nice", "tag"));

    ioTracker->Enqueue(CreateEvent(1, 1, "object_path", "otherpath", "first@", "value2", "third@", "a"));

    ioTracker->Enqueue(CreateEvent(1, 1, "object_path", "otherpath", "first@", "value2", "third@", "b"));

    ioTracker->Enqueue(CreateEvent(1, 1, "object_path", "otherpath", "second@", "c", "third@", "b"));

    NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));

    {
        auto guard = Guard(lock);
        EXPECT_EQ(totalBytes, 10);
        EXPECT_EQ(std::ssize(events), 6);
        std::sort(events.begin(), events.end());

        std::vector<TIOTagList> expected = {
            {{"first@", "value1"}},
            {{"object_path", "mypath"}, {"first@", "value1"}},
            {{"object_path", "mypath"}, {"first@", "value2"}, {"third@", "a"}},
            {{"object_path", "otherpath"}, {"first@", "value2"}, {"third@", "a"}},
            {{"object_path", "otherpath"}, {"first@", "value2"}, {"third@", "b"}},
            {{"object_path", "otherpath"}, {"third@", "b"}},
        };

        EXPECT_EQ(events, expected);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
