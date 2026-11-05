#include <yt/yt/flow/library/cpp/computation/key_count_observer.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

#include <thread>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TKeyCountObserverTest, LifetimeKeysKeepActualEncounterTimes)
{
    TKeyCountObserver observer;
    const auto now = TInstant::Seconds(100);
    for (int key = 0; key < 1200; ++key) {
        observer.ObserveKey(MakeKey(static_cast<ui64>(key), "tail"), now);
    }
    observer.ObserveKey(MakeKey(1u, "tail"), now + TDuration::Seconds(2));
    auto keys = observer.GetKeys();
    EXPECT_EQ(keys.size(), 1200u);
    EXPECT_EQ(observer.GetKeyCount(), 1200);
    EXPECT_EQ(GetOrCrash(keys, MakeKey(1u, "tail")), now + TDuration::Seconds(2));
    EXPECT_EQ(GetOrCrash(keys, MakeKey(2u, "tail")), now);
}

TEST(TKeyCountObserverTest, SnapshotsAreDetachedFromLaterObservations)
{
    TKeyCountObserver observer;
    const auto now = TInstant::Seconds(100);
    observer.ObserveKey(MakeKey("key"), now);
    auto first = observer.GetKeys();
    observer.ObserveKey(MakeKey("key"), now + TDuration::Seconds(1));
    observer.ObserveKey(MakeKey("other"), now);
    EXPECT_EQ(first.size(), 1u);
    EXPECT_EQ(GetOrCrash(first, MakeKey("key")), now);
    auto second = observer.GetKeys();
    EXPECT_EQ(second.size(), 2u);
    EXPECT_EQ(GetOrCrash(second, MakeKey("key")), now + TDuration::Seconds(1));
}

TEST(TKeyCountObserverTest, ConcurrentObservationAndSnapshotsRetainEveryKey)
{
    TKeyCountObserver observer;
    const auto now = TInstant::Seconds(100);
    std::vector<std::thread> writers;
    for (int writer = 0; writer < 4; ++writer) {
        writers.emplace_back([&, writer] {
            for (int key = 0; key < 300; ++key) {
                observer.ObserveKey(MakeKey(writer, key), now);
            }
        });
    }
    for (int iteration = 0; iteration < 100; ++iteration) {
        EXPECT_LE(observer.GetKeys().size(), 1200u);
        EXPECT_LE(observer.GetKeyCount(), 1200);
    }
    for (auto& writer : writers) {
        writer.join();
    }
    auto keys = observer.GetKeys();
    EXPECT_EQ(keys.size(), 1200u);
    for (int writer = 0; writer < 4; ++writer) {
        for (int key = 0; key < 300; ++key) {
            EXPECT_EQ(GetOrCrash(keys, MakeKey(writer, key)), now);
        }
    }
}

TEST(TKeyCountObserverTest, JobStatusMapRoundTripsArbitraryCompositeKeys)
{
    THashMap<TKey, TInstant> keys{
        {MakeKey("string", -42, 1.5), TInstant::Seconds(100)},
        {MakeKey(17u, std::string("a\0b", 3)), TInstant::Seconds(200)},
    };
    auto job = New<TJobStatus>();
    job->ObservedKeys = keys;
    auto jobCopy = NYTree::ConvertTo<TJobStatusPtr>(NYson::ConvertToYsonString(job));
    EXPECT_EQ(jobCopy->ObservedKeys, keys);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
