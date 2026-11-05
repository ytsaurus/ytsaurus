#pragma once

#include "public.h"

#include "key.h"
#include "timer.h"

#include <library/cpp/yt/misc/enum.h>

#include <optional>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_STRONG_TYPEDEF(TTimerFamilyId, ui64);

//! The row format of the pages this code writes and understands.
constexpr i64 CurrentTimerPageFormatVersion = 1;

//! The timestamp that defines page intervals.
DEFINE_ENUM(ETimerPageKind,
    ((Trigger) (0))
    ((Event)   (1))
);

//! How a page relates to a partition.
DEFINE_ENUM(ETimerPageClass,
    // The key range equals the partition's and the page was never shared: new timers go here.
    ((Canonical) (0))
    // The key range lies inside the partition's: every timer belongs to the partition, but no
    // new timers go here.
    ((Nested)    (1))
    // The key range crosses a partition boundary: only a key subrange belongs to the partition.
    ((Shared)    (2))
);

DEFINE_ENUM(ETimerJournalOp,
    ((Create)   (0))
    ((Delete)   (1))
    ((SetFlags) (2))
);

//! The flags of a page; the journaled ones are the ones that decide who may write the page.
struct TTimerPageFlags
{
    //! The page has been seen as shared by some partition; never reset. Journaled.
    bool OnceShared = false;
    //! The stored minima are lower bounds until recomputed.
    bool DirtyMinima = false;
    //! Every timer of this trigger page has its event row. Journaled.
    bool EventDone = false;
    //! The rows are deduplication slots: |RowId| is empty. Journaled.
    bool DedupSlots = false;

    bool operator==(const TTimerPageFlags& other) const = default;
};

//! Flags to change; absent ones keep their values.
//! Each flag is its own column, so partitions may change different flags of a shared page blindly.
struct TTimerPageFlagsUpdate
{
    std::optional<bool> OnceShared;
    std::optional<bool> DirtyMinima;
    std::optional<bool> EventDone;
    std::optional<bool> DedupSlots;

    bool IsEmpty() const;
    void ApplyTo(TTimerPageFlags* flags) const;

    //! Every flag present.
    static TTimerPageFlagsUpdate All(TTimerPageFlags flags);

    bool operator==(const TTimerPageFlagsUpdate& other) const = default;
};

////////////////////////////////////////////////////////////////////////////////

//! Key of a |timer_pages| row: the timestamp of the kind is the key column; |RowId| is the
//! timer's message id, or empty when the page stores deduplication slots.
struct TTimerPageRowKey
{
    TTimerFamilyId FamilyId;
    ETimerPageKind Kind = ETimerPageKind::Trigger;
    TSystemTimestamp Timestamp;
    TKey Key;
    TMessageId RowId;

    bool operator==(const TTimerPageRowKey& other) const;
    bool operator<(const TTimerPageRowKey& other) const;
};

//! A |timer_pages| row: the three timestamps and the message id are value columns, so the
//! timer is restored without looking at the page kind. |StreamId| and |KeySchema| are the
//! page's and are left to the caller.
struct TTimerPageRow
{
    TTimerPageRowKey RowKey;
    TTimer Timer;
};

struct TTimerPageStatistics
{
    i64 Count = 0;
    i64 ByteSize = 0;
    TSystemTimestamp MinTimestamp = InfinitySystemTimestamp;
    TSystemTimestamp MaxTimestamp = ZeroSystemTimestamp;
    TSystemTimestamp MinEvent = InfinitySystemTimestamp;
    TSystemTimestamp MinSystem = InfinitySystemTimestamp;

    void Add(const TTimer& timer, ETimerPageKind kind);
    void Add(const TTimerPageStatistics& other);
};

struct TTimerPageBucket
{
    TSystemTimestamp Begin;
    TTimerPageStatistics Statistics;
};

//! The timestamp of |timer| the pages of |kind| are cut by.
TSystemTimestamp GetTimerPageTimestamp(ETimerPageKind kind, const TTimer& timer);

//! |dedupSlots| is the flag of the page the row belongs to.
TTimerPageRowKey MakeTimerPageRowKey(
    TTimerFamilyId familyId,
    ETimerPageKind kind,
    const TTimer& timer,
    bool dedupSlots);

////////////////////////////////////////////////////////////////////////////////

//! A |timer_page_meta| row. The page covers the timestamps of its kind in [|Begin|, |End|);
//! no |End| means unbounded.
struct TTimerPageMeta
{
    TComputationId ComputationId;
    TTimerFamilyId FamilyId;
    TStreamId StreamId;
    TKeyRange KeyRange;
    ETimerPageKind Kind = ETimerPageKind::Trigger;
    TSystemTimestamp Begin = ZeroSystemTimestamp;
    std::optional<TSystemTimestamp> End;
    TTimerPageFlags Flags;
    i64 Count = 0;
    TSystemTimestamp MinEvent;
    TSystemTimestamp MinSystem;
    //! An upper bound of the page's row timestamps: raised by inserts, never lowered. Null for legacy rows.
    std::optional<TSystemTimestamp> MaxTimestamp;
    //! The row format of the page; NULL in the table reads as 1.
    i64 FormatVersion = CurrentTimerPageFormatVersion;

    bool operator==(const TTimerPageMeta& other) const = default;
};

//! A |timer_page_journal| row: a page operation with the journaled fields of the page.
struct TTimerJournalEntry
{
    ETimerJournalOp Op = ETimerJournalOp::Create;
    TComputationId ComputationId;
    TTimerFamilyId FamilyId;
    TStreamId StreamId;
    TKeyRange KeyRange;
    ETimerPageKind Kind = ETimerPageKind::Trigger;
    TSystemTimestamp Begin = ZeroSystemTimestamp;
    //! The journaled flags: all of them for |Create|, the changed ones for |SetFlags|, none for
    //! |Delete|. |DirtyMinima| is private to the owner and never journaled.
    TTimerPageFlagsUpdate Flags;

    //! Set by the reader only.
    i64 RowIndex = -1;

    static TTimerJournalEntry Create(const TTimerPageMeta& page);
    static TTimerJournalEntry Delete(const TTimerPageMeta& page);
    static TTimerJournalEntry SetFlags(const TTimerPageMeta& page, TTimerPageFlagsUpdate flags);

    bool operator==(const TTimerJournalEntry& other) const = default;
};

//! The journaled subset of |flags|, every one of them present.
TTimerPageFlagsUpdate GetJournaledTimerPageFlags(TTimerPageFlags flags);

//! How a page with |pageRange| and |onceShared| relates to a partition with |partitionRange|.
ETimerPageClass ClassifyTimerPage(const TKeyRange& pageRange, bool onceShared, const TKeyRange& partitionRange);

void FormatValue(TStringBuilderBase* builder, TTimerPageFlags flags, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
