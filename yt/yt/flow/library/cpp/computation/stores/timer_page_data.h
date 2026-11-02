#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/timer.h>
#include <yt/yt/flow/library/cpp/common/timer_page.h>

#include <yt/yt/flow/library/cpp/misc/cow_tree.h>

#include <util/generic/hash_multi_map.h>

#include <array>
#include <optional>
#include <ranges>
#include <vector>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! A timer held by a resident page: the timestamps are stored in the order of the page kind,
//! so entries compare the same way regardless of the kind.
struct TTimerPageEntry
{
    std::array<TSystemTimestamp, 3> Timestamps{};
    TMessageId MessageId;
    TKey Key;

    static TTimerPageEntry FromTimer(ETimerPageKind kind, const TTimer& timer);
    //! Restores the timer; |StreamId| and |KeySchema| are left to the caller.
    TTimer ToTimer(ETimerPageKind kind) const;
    //! The timestamp the pages are cut by.
    TSystemTimestamp GetTimestamp() const;
    TSystemTimestamp GetEventTimestamp(ETimerPageKind kind) const;
    TSystemTimestamp GetSystemTimestamp(ETimerPageKind kind) const;
};

//! Orders entries by (timestamp of the kind, id), the firing order of the in-memory store; a
//! bare timestamp precedes every entry with it.
struct TTimerPageEntryLess
{
    using is_transparent = void;

    bool operator()(const TTimerPageEntry& lhs, const TTimerPageEntry& rhs) const;
    bool operator()(const TTimerPageEntry& lhs, TSystemTimestamp rhs) const;
    bool operator()(TSystemTimestamp lhs, const TTimerPageEntry& rhs) const;
};

////////////////////////////////////////////////////////////////////////////////

//! The resident timers of a family, ordered by timestamp and ID.
class TTimerPageData
{
public:
    using TEntries = TCowSet<TTimerPageEntry, TTimerPageEntryLess>;

    //! With |indexSlots| the entries are also indexed by (timestamp, key), see #FindEntriesByKey().
    explicit TTimerPageData(ETimerPageKind kind, bool indexSlots = false);

    //! Returns false when the timer is already present.
    bool Insert(const TTimer& timer);
    //! Returns false when there is no such timer.
    bool Erase(const TTimer& timer);
    bool Contains(const TTimer& timer) const;
    using TEntryRange = std::ranges::subrange<TEntries::const_iterator>;
    TEntryRange GetEntries(TSystemTimestamp begin = ZeroSystemTimestamp, std::optional<TSystemTimestamp> end = {}) const;

    //! Returns the timers with the given timestamp and key. Requires the slot index.
    std::vector<TTimer> FindEntriesByKey(TSystemTimestamp timestamp, const TKey& key) const;
    //! The number of entries in the slot index; zero without the index.
    i64 GetSlotIndexSize() const;

    TTimerPageStatistics GetStatistics(TSystemTimestamp begin, std::optional<TSystemTimestamp> end) const;
    void EraseInterval(TSystemTimestamp begin, std::optional<TSystemTimestamp> end);
    i64 GetCount() const;
    bool IsEmpty() const;
    //! Memory held by the entries, without the tree overhead.
    i64 GetByteSize() const;

    //! The first timestamp change at or after the |head|-th entry; null when the entries from
    //! there on share one timestamp, which a page cannot be cut inside of.
    std::optional<TSystemTimestamp> FindSplitPoint(i64 head, TSystemTimestamp begin = ZeroSystemTimestamp, std::optional<TSystemTimestamp> end = {}) const;

private:
    const ETimerPageKind Kind_;
    const bool IndexSlots_;
    TEntries Entries_;
    i64 ByteSize_ = 0;

    // Not copy-on-write: read only through #FindEntriesByKey(), which returns copies, and
    // mutated together with |Entries_|, so entry snapshots never observe it. Keys and ids are
    // shared with the entries, not copied.
    THashMultiMap<std::pair<TSystemTimestamp, TKey>, TMessageId> SlotIndex_;

    i64 GetEntryByteSize(const TTimerPageEntry& entry) const;
    void OnInserted(const TTimerPageEntry& entry);
    void OnErased(const TTimerPageEntry& entry);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
