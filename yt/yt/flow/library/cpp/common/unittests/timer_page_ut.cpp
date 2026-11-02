#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/timer_page.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TTimer MakeTimer(ui64 trigger, ui64 event, ui64 system, TStringBuf messageId, ui64 key = 1)
{
    TTimer timer;
    timer.MessageId = TMessageId(messageId);
    timer.StreamId = TStreamId("timer");
    timer.TriggerTimestamp = TSystemTimestamp(trigger);
    timer.EventTimestamp = TSystemTimestamp(event);
    timer.SystemTimestamp = TSystemTimestamp(system);
    timer.AlignmentTimestamp = timer.SystemTimestamp;
    timer.Key = MakeKey(key);
    return timer;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TTimerPageRowTest, RowKeyOfKindAndSlots)
{
    auto timer = MakeTimer(30, 20, 10, "id", 7);
    auto familyId = TTimerFamilyId(5);

    for (auto kind : {ETimerPageKind::Trigger, ETimerPageKind::Event}) {
        for (bool dedupSlots : {false, true}) {
            auto rowKey = MakeTimerPageRowKey(familyId, kind, timer, dedupSlots);
            EXPECT_EQ(rowKey.FamilyId, familyId);
            EXPECT_EQ(rowKey.Kind, kind);
            EXPECT_EQ(rowKey.Key, timer.Key);
            EXPECT_EQ(rowKey.Timestamp, kind == ETimerPageKind::Trigger ? timer.TriggerTimestamp : timer.EventTimestamp);
            EXPECT_EQ(rowKey.RowId, dedupSlots ? TMessageId() : timer.MessageId);
        }
    }
}

TEST(TTimerPageRowTest, RowKeyOrder)
{
    auto make = [] (ui64 family, ETimerPageKind kind, ui64 timestamp, ui64 key, TStringBuf id) {
        return TTimerPageRowKey{
            .FamilyId = TTimerFamilyId(family),
            .Kind = kind,
            .Timestamp = TSystemTimestamp(timestamp),
            .Key = MakeKey(key),
            .RowId = TMessageId(id),
        };
    };
    std::vector<TTimerPageRowKey> keys{
        make(1, ETimerPageKind::Event, 0, 0, ""),
        make(1, ETimerPageKind::Trigger, 1, 1, ""),
        make(1, ETimerPageKind::Trigger, 1, 1, "a"),
        make(1, ETimerPageKind::Trigger, 1, 2, ""),
        make(1, ETimerPageKind::Trigger, 2, 0, ""),
        make(2, ETimerPageKind::Trigger, 0, 0, ""),
    };
    for (int keyIndex = 0; keyIndex < std::ssize(keys); ++keyIndex) {
        EXPECT_EQ(keys[keyIndex], keys[keyIndex]);
        for (int otherKeyIndex = keyIndex + 1; otherKeyIndex < std::ssize(keys); ++otherKeyIndex) {
            EXPECT_LT(keys[keyIndex], keys[otherKeyIndex]);
            EXPECT_FALSE(keys[otherKeyIndex] < keys[keyIndex]);
            EXPECT_FALSE(keys[keyIndex] == keys[otherKeyIndex]);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TTimerPageFlagsTest, UpdateAppliesPresentFlagsOnly)
{
    TTimerPageFlags flags{.OnceShared = true, .DirtyMinima = true};
    TTimerPageFlagsUpdate update{.DirtyMinima = false, .DedupSlots = true};
    EXPECT_FALSE(update.IsEmpty());
    update.ApplyTo(&flags);
    EXPECT_EQ(flags, (TTimerPageFlags{.OnceShared = true, .DedupSlots = true}));

    EXPECT_TRUE(TTimerPageFlagsUpdate().IsEmpty());
    TTimerPageFlags all{.OnceShared = true, .DirtyMinima = true, .EventDone = true, .DedupSlots = true};
    TTimerPageFlags target;
    TTimerPageFlagsUpdate::All(all).ApplyTo(&target);
    EXPECT_EQ(target, all);

    EXPECT_EQ(ToString(all), "once_shared,dirty_minima,event_done,dedup_slots");
    EXPECT_EQ(ToString(TTimerPageFlags()), "");
}

TEST(TTimerJournalEntryTest, FromPage)
{
    TTimerPageMeta page{
        .ComputationId = TComputationId("c"),
        .FamilyId = TTimerFamilyId(5),
        .StreamId = TStreamId("timer"),
        .KeyRange = MakeUintKeyRange(1, 10),
        .Kind = ETimerPageKind::Event,
        .Begin = TSystemTimestamp(6),
        .Flags = {.OnceShared = true, .DirtyMinima = true, .EventDone = true},
        .Count = 3,
    };

    auto create = TTimerJournalEntry::Create(page);
    EXPECT_EQ(create.Op, ETimerJournalOp::Create);
    EXPECT_EQ(create.ComputationId, page.ComputationId);
    EXPECT_EQ(create.FamilyId, page.FamilyId);
    EXPECT_EQ(create.Begin, page.Begin);
    EXPECT_EQ(create.StreamId, page.StreamId);
    EXPECT_EQ(create.KeyRange, page.KeyRange);
    EXPECT_EQ(create.Kind, page.Kind);
    // Every journaled flag is present, the private one is not.
    EXPECT_EQ(create.Flags, (TTimerPageFlagsUpdate{.OnceShared = true, .EventDone = true, .DedupSlots = false}));

    auto setFlags = TTimerJournalEntry::SetFlags(page, {.DedupSlots = true});
    EXPECT_EQ(setFlags.Op, ETimerJournalOp::SetFlags);
    EXPECT_EQ(setFlags.Flags, (TTimerPageFlagsUpdate{.DedupSlots = true}));

    auto remove = TTimerJournalEntry::Delete(page);
    EXPECT_EQ(remove.Op, ETimerJournalOp::Delete);
    EXPECT_TRUE(remove.Flags.IsEmpty());
    EXPECT_EQ(remove.Begin, page.Begin);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
