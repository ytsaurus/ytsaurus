#include "timer_page_data.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace {

// The message id string block: refcount, hash and the length header before the characters.
constexpr i64 MessageIdBlockOverhead = 24;

// A slot index node: the hash node, its bucket share and the (timestamp, key, id) triple.
constexpr i64 SlotIndexEntryOverhead = 64;

} // namespace

////////////////////////////////////////////////////////////////////////////////

TTimerPageEntry TTimerPageEntry::FromTimer(ETimerPageKind kind, const TTimer& timer)
{
    TTimerPageEntry entry;
    switch (kind) {
        case ETimerPageKind::Trigger:
            entry.Timestamps = {timer.TriggerTimestamp, timer.EventTimestamp, timer.SystemTimestamp};
            break;
        case ETimerPageKind::Event:
            entry.Timestamps = {timer.EventTimestamp, timer.TriggerTimestamp, timer.SystemTimestamp};
            break;
    }
    entry.MessageId = timer.MessageId;
    entry.Key = timer.Key;
    return entry;
}

TTimer TTimerPageEntry::ToTimer(ETimerPageKind kind) const
{
    TTimer timer;
    timer.MessageId = MessageId;
    timer.Key = Key;
    switch (kind) {
        case ETimerPageKind::Trigger:
            timer.TriggerTimestamp = Timestamps[0];
            timer.EventTimestamp = Timestamps[1];
            break;
        case ETimerPageKind::Event:
            timer.EventTimestamp = Timestamps[0];
            timer.TriggerTimestamp = Timestamps[1];
            break;
    }
    timer.SystemTimestamp = Timestamps[2];
    timer.AlignmentTimestamp = Timestamps[2];
    return timer;
}

TSystemTimestamp TTimerPageEntry::GetTimestamp() const
{
    return Timestamps[0];
}

TSystemTimestamp TTimerPageEntry::GetEventTimestamp(ETimerPageKind kind) const
{
    return kind == ETimerPageKind::Trigger ? Timestamps[1] : Timestamps[0];
}

TSystemTimestamp TTimerPageEntry::GetSystemTimestamp(ETimerPageKind /*kind*/) const
{
    return Timestamps[2];
}

////////////////////////////////////////////////////////////////////////////////

bool TTimerPageEntryLess::operator()(const TTimerPageEntry& lhs, const TTimerPageEntry& rhs) const
{
    if (lhs.Timestamps[0] != rhs.Timestamps[0]) {
        return lhs.Timestamps[0] < rhs.Timestamps[0];
    }
    return lhs.MessageId.Underlying() < rhs.MessageId.Underlying();
}

bool TTimerPageEntryLess::operator()(const TTimerPageEntry& lhs, TSystemTimestamp rhs) const
{
    return lhs.Timestamps[0] < rhs;
}

bool TTimerPageEntryLess::operator()(TSystemTimestamp lhs, const TTimerPageEntry& rhs) const
{
    return lhs < rhs.Timestamps[0];
}

////////////////////////////////////////////////////////////////////////////////

TTimerPageData::TTimerPageData(ETimerPageKind kind, bool indexSlots)
    : Kind_(kind)
    , IndexSlots_(indexSlots)
{ }

bool TTimerPageData::Insert(const TTimer& timer)
{
    auto entry = TTimerPageEntry::FromTimer(Kind_, timer);
    // The index goes first: a rejected duplicate is undone there, not in |Entries_|.
    std::optional<decltype(SlotIndex_)::iterator> indexIt;
    if (IndexSlots_) {
        indexIt = SlotIndex_.emplace(std::pair(entry.GetTimestamp(), entry.Key), entry.MessageId);
    }
    auto [it, inserted] = Entries_.insert(std::move(entry));
    if (inserted) {
        OnInserted(*it);
    } else if (indexIt) {
        SlotIndex_.erase(*indexIt);
    }
    return inserted;
}

bool TTimerPageData::Erase(const TTimer& timer)
{
    auto it = std::as_const(Entries_).find(TTimerPageEntry::FromTimer(Kind_, timer));
    if (it == Entries_.end()) {
        return false;
    }
    auto entry = *it;
    Entries_.erase(it);
    OnErased(entry);
    return true;
}

bool TTimerPageData::Contains(const TTimer& timer) const
{
    return Entries_.contains(TTimerPageEntry::FromTimer(Kind_, timer));
}

TTimerPageData::TEntryRange TTimerPageData::GetEntries(TSystemTimestamp begin, std::optional<TSystemTimestamp> end) const
{
    return TEntryRange(Entries_.lower_bound(begin), end ? Entries_.lower_bound(*end) : Entries_.end());
}

std::vector<TTimer> TTimerPageData::FindEntriesByKey(TSystemTimestamp timestamp, const TKey& key) const
{
    YT_VERIFY(IndexSlots_);
    std::vector<TTimer> result;
    auto [begin, end] = SlotIndex_.equal_range(std::pair(timestamp, key));
    for (auto it = begin; it != end; ++it) {
        TTimerPageEntry probe;
        probe.Timestamps[0] = timestamp;
        probe.MessageId = it->second;
        auto entryIt = Entries_.find(probe);
        YT_VERIFY(entryIt != Entries_.end());
        result.push_back(entryIt->ToTimer(Kind_));
    }
    return result;
}

i64 TTimerPageData::GetSlotIndexSize() const
{
    return std::ssize(SlotIndex_);
}

TTimerPageStatistics TTimerPageData::GetStatistics(TSystemTimestamp begin, std::optional<TSystemTimestamp> end) const
{
    TTimerPageStatistics result;
    for (const auto& entry : GetEntries(begin, end)) {
        ++result.Count;
        result.MinTimestamp = std::min(result.MinTimestamp, entry.GetTimestamp());
        result.MaxTimestamp = std::max(result.MaxTimestamp, entry.GetTimestamp());
        result.MinEvent = std::min(result.MinEvent, entry.GetEventTimestamp(Kind_));
        result.MinSystem = std::min(result.MinSystem, entry.GetSystemTimestamp(Kind_));
        result.ByteSize += GetEntryByteSize(entry);
    }
    return result;
}

void TTimerPageData::EraseInterval(TSystemTimestamp begin, std::optional<TSystemTimestamp> end)
{
    while (true) {
        auto it = std::as_const(Entries_).lower_bound(begin);
        if (it == Entries_.end() || (end && it->GetTimestamp() >= *end)) {
            break;
        }
        auto entry = *it;
        Entries_.erase(it);
        OnErased(entry);
    }
}

i64 TTimerPageData::GetCount() const
{
    return std::ssize(Entries_);
}

bool TTimerPageData::IsEmpty() const
{
    return Entries_.empty();
}

i64 TTimerPageData::GetByteSize() const
{
    return ByteSize_;
}

std::optional<TSystemTimestamp> TTimerPageData::FindSplitPoint(i64 head, TSystemTimestamp begin, std::optional<TSystemTimestamp> end) const
{
    YT_VERIFY(head > 0);
    i64 index = 0;
    std::optional<TSystemTimestamp> previous;
    for (const auto& entry : GetEntries(begin, end)) {
        if (index >= head && entry.Timestamps[0] != *previous) {
            return entry.GetTimestamp();
        }
        previous = entry.Timestamps[0];
        ++index;
    }
    return std::nullopt;
}

i64 TTimerPageData::GetEntryByteSize(const TTimerPageEntry& entry) const
{
    return sizeof(TTimerPageEntry) +
        static_cast<i64>(entry.Key.Underlying().GetSpaceUsed()) +
        MessageIdBlockOverhead + std::ssize(entry.MessageId.Underlying());
}

void TTimerPageData::OnInserted(const TTimerPageEntry& entry)
{
    ByteSize_ += GetEntryByteSize(entry) + (IndexSlots_ ? SlotIndexEntryOverhead : 0);
}

void TTimerPageData::OnErased(const TTimerPageEntry& entry)
{
    ByteSize_ -= GetEntryByteSize(entry) + (IndexSlots_ ? SlotIndexEntryOverhead : 0);
    if (IndexSlots_) {
        auto [begin, end] = SlotIndex_.equal_range(std::pair(entry.GetTimestamp(), entry.Key));
        for (auto it = begin; it != end; ++it) {
            if (it->second == entry.MessageId) {
                SlotIndex_.erase(it);
                return;
            }
        }
        YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
