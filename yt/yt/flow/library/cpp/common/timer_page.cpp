#include "timer_page.h"

#include <library/cpp/yt/assert/assert.h>
#include <library/cpp/yt/string/string_builder.h>

#include <algorithm>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

bool TTimerPageRowKey::operator==(const TTimerPageRowKey& other) const
{
    return FamilyId == other.FamilyId &&
        Kind == other.Kind &&
        Key == other.Key &&
        Timestamp == other.Timestamp &&
        RowId == other.RowId;
}

bool TTimerPageRowKey::operator<(const TTimerPageRowKey& other) const
{
    if (FamilyId != other.FamilyId) {
        return FamilyId < other.FamilyId;
    }
    if (Kind != other.Kind) {
        return FormatEnum(Kind) < FormatEnum(other.Kind);
    }
    if (Timestamp != other.Timestamp) {
        return Timestamp < other.Timestamp;
    }
    if (Key != other.Key) {
        return Key < other.Key;
    }
    return RowId < other.RowId;
}

TSystemTimestamp GetTimerPageTimestamp(ETimerPageKind kind, const TTimer& timer)
{
    switch (kind) {
        case ETimerPageKind::Trigger:
            return timer.TriggerTimestamp;
        case ETimerPageKind::Event:
            return timer.EventTimestamp;
    }
}

TTimerPageRowKey MakeTimerPageRowKey(
    TTimerFamilyId familyId,
    ETimerPageKind kind,
    const TTimer& timer,
    bool dedupSlots)
{
    return TTimerPageRowKey{
        .FamilyId = familyId,
        .Kind = kind,
        .Timestamp = GetTimerPageTimestamp(kind, timer),
        .Key = timer.Key,
        .RowId = dedupSlots ? TMessageId() : timer.MessageId,
    };
}

void TTimerPageStatistics::Add(const TTimer& timer, ETimerPageKind kind)
{
    ++Count;
    auto timestamp = GetTimerPageTimestamp(kind, timer);
    MinTimestamp = std::min(MinTimestamp, timestamp);
    MaxTimestamp = std::max(MaxTimestamp, timestamp);
    MinEvent = std::min(MinEvent, timer.EventTimestamp);
    MinSystem = std::min(MinSystem, timer.SystemTimestamp);
}

void TTimerPageStatistics::Add(const TTimerPageStatistics& other)
{
    if (!other.Count) {
        return;
    }
    if (!Count) {
        *this = other;
        return;
    }
    Count += other.Count;
    ByteSize += other.ByteSize;
    MinTimestamp = std::min(MinTimestamp, other.MinTimestamp);
    MaxTimestamp = std::max(MaxTimestamp, other.MaxTimestamp);
    MinEvent = std::min(MinEvent, other.MinEvent);
    MinSystem = std::min(MinSystem, other.MinSystem);
}

////////////////////////////////////////////////////////////////////////////////

bool TTimerPageFlagsUpdate::IsEmpty() const
{
    return !OnceShared && !DirtyMinima && !EventDone && !DedupSlots;
}

void TTimerPageFlagsUpdate::ApplyTo(TTimerPageFlags* flags) const
{
    if (OnceShared) {
        flags->OnceShared = *OnceShared;
    }
    if (DirtyMinima) {
        flags->DirtyMinima = *DirtyMinima;
    }
    if (EventDone) {
        flags->EventDone = *EventDone;
    }
    if (DedupSlots) {
        flags->DedupSlots = *DedupSlots;
    }
}

TTimerPageFlagsUpdate TTimerPageFlagsUpdate::All(TTimerPageFlags flags)
{
    return TTimerPageFlagsUpdate{
        .OnceShared = flags.OnceShared,
        .DirtyMinima = flags.DirtyMinima,
        .EventDone = flags.EventDone,
        .DedupSlots = flags.DedupSlots,
    };
}

////////////////////////////////////////////////////////////////////////////////

namespace {

TTimerJournalEntry MakeJournalEntry(ETimerJournalOp op, const TTimerPageMeta& page, TTimerPageFlagsUpdate flags)
{
    return TTimerJournalEntry{
        .Op = op,
        .ComputationId = page.ComputationId,
        .FamilyId = page.FamilyId,
        .StreamId = page.StreamId,
        .KeyRange = page.KeyRange,
        .Kind = page.Kind,
        .Begin = page.Begin,
        .Flags = flags,
    };
}

} // namespace

TTimerJournalEntry TTimerJournalEntry::Create(const TTimerPageMeta& page)
{
    return MakeJournalEntry(ETimerJournalOp::Create, page, GetJournaledTimerPageFlags(page.Flags));
}

TTimerJournalEntry TTimerJournalEntry::Delete(const TTimerPageMeta& page)
{
    return MakeJournalEntry(ETimerJournalOp::Delete, page, {});
}

TTimerJournalEntry TTimerJournalEntry::SetFlags(const TTimerPageMeta& page, TTimerPageFlagsUpdate flags)
{
    YT_VERIFY(!flags.DirtyMinima);
    return MakeJournalEntry(ETimerJournalOp::SetFlags, page, flags);
}

TTimerPageFlagsUpdate GetJournaledTimerPageFlags(TTimerPageFlags flags)
{
    return TTimerPageFlagsUpdate{
        .OnceShared = flags.OnceShared,
        .EventDone = flags.EventDone,
        .DedupSlots = flags.DedupSlots,
    };
}

ETimerPageClass ClassifyTimerPage(const TKeyRange& pageRange, bool onceShared, const TKeyRange& partitionRange)
{
    if (pageRange == partitionRange && !onceShared) {
        return ETimerPageClass::Canonical;
    }
    if (!(pageRange.Lower < partitionRange.Lower) && !(partitionRange.Upper < pageRange.Upper)) {
        return ETimerPageClass::Nested;
    }
    return ETimerPageClass::Shared;
}

void FormatValue(TStringBuilderBase* builder, TTimerPageFlags flags, TStringBuf /*spec*/)
{
    TDelimitedStringBuilderWrapper delimited(builder, ",");
    if (flags.OnceShared) {
        delimited->AppendString("once_shared");
    }
    if (flags.DirtyMinima) {
        delimited->AppendString("dirty_minima");
    }
    if (flags.EventDone) {
        delimited->AppendString("event_done");
    }
    if (flags.DedupSlots) {
        delimited->AppendString("dedup_slots");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
