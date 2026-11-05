#include <yt/yt/flow/library/cpp/computation/stores/timer_page_data.h>

#include <yt/yt/flow/library/cpp/common/key.h>

#include <yt/yt/core/test_framework/framework.h>

#include <algorithm>
#include <map>
#include <random>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TTimer MakeTimer(ui64 trigger, ui64 event, ui64 system, TStringBuf messageId, ui64 key = 1)
{
    TTimer timer;
    timer.MessageId = TMessageId(messageId);
    timer.Key = MakeKey(key);
    timer.TriggerTimestamp = TSystemTimestamp(trigger);
    timer.EventTimestamp = TSystemTimestamp(event);
    timer.SystemTimestamp = TSystemTimestamp(system);
    timer.AlignmentTimestamp = timer.SystemTimestamp;
    return timer;
}

std::vector<TMessageId> GetMessageIds(const TTimerPageData& page)
{
    std::vector<TMessageId> result;
    for (const auto& entry : page.GetEntries()) {
        result.push_back(entry.MessageId);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TTimerPageEntryTest, RoundTrip)
{
    auto timer = MakeTimer(30, 20, 10, "id", 7);
    for (auto kind : {ETimerPageKind::Trigger, ETimerPageKind::Event}) {
        auto entry = TTimerPageEntry::FromTimer(kind, timer);
        EXPECT_EQ(entry.GetTimestamp(), GetTimerPageTimestamp(kind, timer));
        EXPECT_EQ(entry.GetEventTimestamp(kind), timer.EventTimestamp);
        EXPECT_EQ(entry.GetSystemTimestamp(kind), timer.SystemTimestamp);

        auto restored = entry.ToTimer(kind);
        EXPECT_EQ(restored.MessageId, timer.MessageId);
        EXPECT_EQ(restored.Key, timer.Key);
        EXPECT_EQ(restored.TriggerTimestamp, timer.TriggerTimestamp);
        EXPECT_EQ(restored.EventTimestamp, timer.EventTimestamp);
        EXPECT_EQ(restored.SystemTimestamp, timer.SystemTimestamp);
        EXPECT_EQ(restored.AlignmentTimestamp, timer.SystemTimestamp);
    }
}

TEST(TTimerPageDataTest, OrderAndMinima)
{
    TTimerPageData page(ETimerPageKind::Trigger);
    EXPECT_TRUE(page.IsEmpty());
    EXPECT_EQ(page.GetStatistics({}, {}).Count, 0);

    EXPECT_TRUE(page.Insert(MakeTimer(20, 5, 9, "b")));
    EXPECT_TRUE(page.Insert(MakeTimer(10, 7, 8, "c")));
    EXPECT_TRUE(page.Insert(MakeTimer(10, 7, 8, "a")));
    EXPECT_TRUE(page.Insert(MakeTimer(10, 6, 100, "d")));
    EXPECT_FALSE(page.Insert(MakeTimer(10, 7, 8, "a")));

    EXPECT_EQ(page.GetCount(), 4);
    // Within a timestamp the ids order the entries.
    EXPECT_EQ(GetMessageIds(page), (std::vector<TMessageId>{TMessageId("a"), TMessageId("c"), TMessageId("d"), TMessageId("b")}));
    EXPECT_EQ(page.GetStatistics({}, {}).MinEvent, TSystemTimestamp(5));
    EXPECT_EQ(page.GetStatistics({}, {}).MinSystem, TSystemTimestamp(8));
    EXPECT_GT(page.GetByteSize(), 4 * static_cast<i64>(sizeof(TTimerPageEntry)));

    // Erasing a holder of a minimum recomputes it.
    EXPECT_TRUE(page.Erase(MakeTimer(20, 5, 9, "b")));
    EXPECT_FALSE(page.Erase(MakeTimer(20, 5, 9, "b")));
    EXPECT_EQ(page.GetStatistics({}, {}).MinEvent, TSystemTimestamp(6));
    EXPECT_EQ(page.GetStatistics({}, {}).MinSystem, TSystemTimestamp(8));
    EXPECT_TRUE(page.Erase(MakeTimer(10, 7, 8, "a")));
    EXPECT_EQ(page.GetStatistics({}, {}).MinSystem, TSystemTimestamp(8));
    EXPECT_TRUE(page.Erase(MakeTimer(10, 7, 8, "c")));
    EXPECT_EQ(page.GetStatistics({}, {}).MinEvent, TSystemTimestamp(6));
    EXPECT_EQ(page.GetStatistics({}, {}).MinSystem, TSystemTimestamp(100));

    // Include late data in the interval statistics.
    EXPECT_TRUE(page.Insert(MakeTimer(30, 1, 2, "late")));
    EXPECT_EQ(page.GetStatistics({}, {}).MinEvent, TSystemTimestamp(1));
    EXPECT_EQ(page.GetStatistics({}, {}).MinSystem, TSystemTimestamp(2));

    EXPECT_TRUE(page.Contains(MakeTimer(30, 1, 2, "late")));
    EXPECT_FALSE(page.Contains(MakeTimer(30, 1, 2, "absent")));
}

TEST(TTimerPageDataTest, EventKind)
{
    TTimerPageData page(ETimerPageKind::Event);
    EXPECT_TRUE(page.Insert(MakeTimer(1, 20, 9, "b")));
    EXPECT_TRUE(page.Insert(MakeTimer(2, 10, 8, "a")));
    EXPECT_EQ(GetMessageIds(page), (std::vector<TMessageId>{TMessageId("a"), TMessageId("b")}));
    EXPECT_EQ(page.GetStatistics({}, {}).MinEvent, TSystemTimestamp(10));
    EXPECT_EQ(page.GetStatistics({}, {}).MinSystem, TSystemTimestamp(8));
    EXPECT_EQ(page.GetEntries().begin()->ToTimer(ETimerPageKind::Event).TriggerTimestamp, TSystemTimestamp(2));
}

TEST(TTimerPageDataTest, IntervalStatisticsAndEviction)
{
    TTimerPageData family(ETimerPageKind::Trigger);
    for (i64 index = 0; index < 10; ++index) {
        ASSERT_TRUE(family.Insert(MakeTimer(index, index, 10 - index, Format("id-%v", index))));
    }
    auto head = family.GetStatistics(ZeroSystemTimestamp, TSystemTimestamp(7));
    auto tail = family.GetStatistics(TSystemTimestamp(7), {});
    EXPECT_EQ(head.Count, 7);
    EXPECT_EQ(tail.Count, 3);
    EXPECT_EQ(head.MinEvent, TSystemTimestamp(0));
    EXPECT_EQ(head.MinSystem, TSystemTimestamp(4));
    EXPECT_EQ(tail.MinEvent, TSystemTimestamp(7));
    EXPECT_EQ(tail.MinSystem, TSystemTimestamp(1));
    EXPECT_EQ(head.ByteSize + tail.ByteSize, family.GetByteSize());
    auto split = family.FindSplitPoint(2, TSystemTimestamp(5), TSystemTimestamp(9));
    ASSERT_TRUE(split);
    EXPECT_EQ(*split, TSystemTimestamp(7));
    family.EraseInterval(TSystemTimestamp(5), TSystemTimestamp(7));
    EXPECT_EQ(family.GetCount(), 8);
    EXPECT_EQ(family.GetStatistics(TSystemTimestamp(5), TSystemTimestamp(7)).Count, 0);
    EXPECT_EQ(family.GetEntries(TSystemTimestamp(7), {}).begin()->MessageId, TMessageId("id-7"));
    family.EraseInterval(TSystemTimestamp(7), {});
    EXPECT_EQ(family.GetCount(), 5);
    EXPECT_EQ(family.GetByteSize(), family.GetStatistics(ZeroSystemTimestamp, {}).ByteSize);
}

TEST(TTimerPageDataTest, SplitPointSkipsEqualTimestamps)
{
    // Timestamps 1, 1, 1, 2, 2, 3: a cut inside a run moves to the next change.
    TTimerPageData page(ETimerPageKind::Trigger);
    int index = 0;
    for (i64 timestamp : {1, 1, 1, 2, 2, 3}) {
        EXPECT_TRUE(page.Insert(MakeTimer(timestamp, 1, 1, Format("id-%v", index++))));
    }
    auto split = page.FindSplitPoint(1);
    ASSERT_TRUE(split);
    EXPECT_EQ(*split, TSystemTimestamp(2));
    split = page.FindSplitPoint(4);
    ASSERT_TRUE(split);
    EXPECT_EQ(*split, TSystemTimestamp(3));
    EXPECT_FALSE(page.FindSplitPoint(6));

    // One timestamp only: the page cannot be cut at all.
    TTimerPageData single(ETimerPageKind::Trigger);
    for (int entryIndex = 0; entryIndex < 4; ++entryIndex) {
        EXPECT_TRUE(single.Insert(MakeTimer(9, 1, 1, Format("s-%v", entryIndex))));
    }
    EXPECT_FALSE(single.FindSplitPoint(1));
}

TEST(TTimerPageDataTest, RandomAgainstSet)
{
    std::mt19937 rng(42);
    TTimerPageData page(ETimerPageKind::Trigger);
    // An entry is identified by its trigger and id; the other timestamps are its payload.
    std::map<std::pair<ui64, std::string>, std::pair<ui64, ui64>> model;

    for (int step = 0; step < 5000; ++step) {
        ui64 trigger = rng() % 50;
        ui64 event = rng() % 50;
        ui64 system = rng() % 50;
        auto id = Format("id-%v", rng() % 20);
        auto key = std::pair(trigger, id);
        if (rng() % 3 == 0) {
            auto it = model.find(key);
            if (it != model.end()) {
                EXPECT_TRUE(page.Erase(MakeTimer(trigger, it->second.first, it->second.second, id)));
                model.erase(it);
            } else {
                EXPECT_FALSE(page.Erase(MakeTimer(trigger, event, system, id)));
            }
        } else {
            EXPECT_EQ(page.Insert(MakeTimer(trigger, event, system, id)), model.emplace(key, std::pair(event, system)).second);
        }
        if (step % 100 == 0) {
            ASSERT_EQ(page.GetCount(), std::ssize(model));
            std::optional<ui64> minEvent;
            std::optional<ui64> minSystem;
            auto it = page.GetEntries().begin();
            for (const auto& [modelKey, modelValue] : model) {
                const auto& [modelTrigger, modelId] = modelKey;
                const auto& [modelEvent, modelSystem] = modelValue;
                ASSERT_EQ(it->MessageId, TMessageId(modelId));
                ASSERT_EQ(it->Timestamps[0], TSystemTimestamp(modelTrigger));
                ++it;
                minEvent = std::min(minEvent.value_or(modelEvent), modelEvent);
                minSystem = std::min(minSystem.value_or(modelSystem), modelSystem);
            }
            EXPECT_EQ((page.GetStatistics({}, {}).Count != 0), minEvent.has_value());
            if (minEvent) {
                EXPECT_EQ(page.GetStatistics({}, {}).MinEvent, TSystemTimestamp(*minEvent));
                EXPECT_EQ(page.GetStatistics({}, {}).MinSystem, TSystemTimestamp(*minSystem));
            }
        }
    }
}

TEST(TTimerPageDataTest, SlotIndexFollowsEntries)
{
    TTimerPageData indexed(ETimerPageKind::Trigger, /*indexSlots*/ true);
    TTimerPageData plain(ETimerPageKind::Trigger);

    for (auto* page : {&indexed, &plain}) {
        EXPECT_TRUE(page->Insert(MakeTimer(10, 1, 1, "a", /*key*/ 1)));
        EXPECT_TRUE(page->Insert(MakeTimer(10, 1, 1, "b", /*key*/ 2)));
        EXPECT_TRUE(page->Insert(MakeTimer(11, 1, 1, "c", /*key*/ 1)));
        // The same slot under another id, as a page without slots may hold.
        EXPECT_TRUE(page->Insert(MakeTimer(10, 1, 1, "d", /*key*/ 1)));
        EXPECT_FALSE(page->Insert(MakeTimer(10, 1, 1, "a", /*key*/ 1)));
    }
    EXPECT_EQ(indexed.GetSlotIndexSize(), 4);
    EXPECT_EQ(plain.GetSlotIndexSize(), 0);
    // The index is accounted for in the byte size.
    EXPECT_GT(indexed.GetByteSize(), plain.GetByteSize());

    auto ids = [&] (int trigger, int key) {
        std::vector<std::string> result;
        for (const auto& timer : indexed.FindEntriesByKey(TSystemTimestamp(trigger), MakeKey(static_cast<ui64>(key)))) {
            result.push_back(std::string(timer.MessageId.Underlying()));
        }
        std::sort(result.begin(), result.end());
        return result;
    };
    EXPECT_EQ(ids(10, 1), (std::vector<std::string>{"a", "d"}));
    EXPECT_EQ(ids(10, 2), (std::vector<std::string>{"b"}));
    EXPECT_EQ(ids(11, 1), (std::vector<std::string>{"c"}));
    EXPECT_TRUE(ids(11, 2).empty());

    // A snapshot of the entries is unaffected by later changes of the index.
    auto snapshot = indexed.GetEntries();
    i64 count = std::distance(snapshot.begin(), snapshot.end());
    EXPECT_TRUE(indexed.Erase(MakeTimer(10, 1, 1, "a", /*key*/ 1)));
    EXPECT_EQ(std::distance(snapshot.begin(), snapshot.end()), count);
    EXPECT_EQ(ids(10, 1), (std::vector<std::string>{"d"}));
    EXPECT_EQ(indexed.GetSlotIndexSize(), 3);

    indexed.EraseInterval(TSystemTimestamp(10), TSystemTimestamp(11));
    EXPECT_EQ(indexed.GetSlotIndexSize(), 1);
    EXPECT_TRUE(ids(10, 1).empty());
    EXPECT_EQ(ids(11, 1), (std::vector<std::string>{"c"}));
    EXPECT_EQ(indexed.GetSlotIndexSize(), indexed.GetCount());
    indexed.EraseInterval(ZeroSystemTimestamp, std::nullopt);
    EXPECT_EQ(indexed.GetSlotIndexSize(), 0);
    EXPECT_EQ(indexed.GetByteSize(), 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
