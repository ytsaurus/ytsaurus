#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/public.h>

#include <yt/yt/flow/library/cpp/tables/key_visitor_states.h>
#include <yt/yt/flow/library/cpp/tables/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Yson-serializable interval value. Persisted under each TKeyVisitorStates
//! row; also the in-memory unit owned by buckets.
struct TKeyVisitorInterval
    : public NYTree::TYsonStruct
{
    TKey Lower;
    TKey Upper;
    //! YT-clock timestamp of the scan that first moved this range out of
    //! Pending in the current pass. Persisted so the schedule-lag anchor
    //! survives worker restarts and partition rebalancing. Zero means
    //! "not stamped yet" — only valid for Pending intervals.
    TSystemTimestamp PassStartedAt;
    //! True iff this interval belongs to a final pass — visitor stops once such
    //! a pass commits everywhere.
    bool FinalPass = false;

    REGISTER_YSON_STRUCT(TKeyVisitorInterval);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TKeyVisitorInterval);

////////////////////////////////////////////////////////////////////////////////

//! Per-partition coverage book-keeper. Owns the bucket geometry plus a
//! snapshot of the persisted state-table rows; exposes a slim API for the
//! TKeyVisitor fill loop: ask for the next Pending range, mark coverage buffered
//! or committed, write the diff out under a transaction.
class TKeyVisitorStore
    : public TRefCounted
{
public:
    //! Lifecycle of a single coverage interval inside a bucket.
    enum class EIntervalState
    {
        //! Not yet visited in the current pass.
        Pending,
        //! Read into the consumer-facing buffer this pass; not yet durable.
        Buffered,
        //! Drained by the consumer; part of the durable cursor, persisted on
        //! the next Sync. Pending and Buffered intervals are never persisted.
        Committed,
    };

    //! In-memory slot: an interval plus its current lifecycle state.
    struct TIntervalSlot
    {
        TKeyVisitorIntervalPtr Interval;
        EIntervalState State = EIntervalState::Pending;
    };

    //! Per-bucket coverage geometry. A bucket owns a contiguous key range
    //! divided into a totally-ordered, disjoint, gapless sequence of
    //! intervals; each interval carries a lifecycle state.
    struct TBucket
    {
        TKeyRange Range;
        std::vector<TIntervalSlot> Intervals;

        //! Collapses Intervals to a single Pending interval over Range with the
        //! given FinalPass marker.
        void Reset(bool finalPass);

        //! Stamps Pending sub-intervals as Buffered with PassStartedAt = |when|.
        void MarkBuffered(const TKeyRange& range, TSystemTimestamp when);
        //! Flips Buffered sub-intervals to Committed; PassStartedAt is preserved.
        void MarkCommitted(const TKeyRange& range);
        //! Seeds the range as Committed; used by Init from persisted rows.
        void SeedCommitted(const TKeyRange& range, TSystemTimestamp stampedAt, bool finalPass);
    };

    TKeyVisitorStore(
        TComputationId computationId,
        TStreamId streamId,
        TKeyRange partitionRange,
        int bucketCount,
        NTables::IKeyVisitorStatesPtr backend,
        NLogging::TLogger logger);

    //! Pre-builds bucket geometry and seeds Committed coverage from the
    //! persisted snapshot in the state table.
    TFuture<void> Init();

    //! Stamps the sub-range as Buffered in the in-memory geometry. Buffered
    //! coverage is not visible to Sync. PassStartedAt is stamped with |when|
    //! for sub-intervals that were previously Pending.
    void MarkBuffered(const TKeyRange& range, TSystemTimestamp when);

    //! Stamps the sub-range as Committed. Committed coverage is written out
    //! by the next Sync. PassStartedAt is preserved.
    void MarkCommitted(const TKeyRange& range);

    //! Returns the next Pending sub-range to scan, or nullopt if every bucket is
    //! fully covered. Round-robin across buckets, left-to-right inside each.
    std::optional<TKeyRange> GetNextRange();

    //! Returns true if every interval in every bucket is Committed.
    bool IsAllCommitted() const;

    //! Collapses every bucket to a single Pending interval (FinalPass=false).
    //! Test-only — production code uses StartNewPass.
    void Reset();

    //! Starts a fresh pass. The persisted snapshot is wiped atomically by the
    //! next Sync, so a crash mid-rotation preserves the previous pass.
    void StartNewPass(bool finalPass);

    //! True iff the current pass was started with finalPass=true.
    bool IsCurrentPassFinal() const;

    //! Read-only view of the current bucket geometry. Intended for tests.
    const std::vector<TBucket>& GetBuckets() const;

    //! True if no coverage rows are currently persisted.
    bool IsRemoteStateEmpty() const;

    //! Fraction of the partition's hash range already scanned in the current
    //! pass, in [0; 1] (Buffered and Committed both count — a Committed region
    //! was scanned earlier this pass). Maps sweep progress onto the schedule.
    double GetScannedFraction() const;

    //! Minimum PassStartedAt across all non-Pending intervals — the YT-clock
    //! anchor for the current sweep. Returns nullopt if every interval is
    //! Pending (start of a fresh pass; visitor uses "now" instead).
    std::optional<TSystemTimestamp> GetMinPassStartedAt() const;

    //! Walks every bucket, computes the diff between the desired Committed
    //! snapshot and the cached remote snapshot, writes the diff under the
    //! |transaction|, and refreshes the cached snapshot.
    void Sync(NApi::IDynamicTableTransactionPtr transaction);

private:
    const TComputationId ComputationId_;
    const TStreamId StreamId_;
    const TKeyRange PartitionRange_;
    const int BucketCount_;
    const NTables::IKeyVisitorStatesPtr Backend_;
    const NLogging::TLogger Logger;

    //! Committed-only snapshot, one entry per stored row: each interval is
    //! present twice — at its lower bound under {Lower, true} and at its
    //! upper bound under {Upper, false} — mirroring the storage schema.
    //! Sentinel boundaries (MinKey / MaxKey) are not storable as `any`
    //! sort-keys, so the corresponding row is absent and the interval lives
    //! here as a single entry. An interval (MinKey; MaxKey) cannot be
    //! represented at all, which is fine: the partition would have no other
    //! coverage to track.
    THashMap<std::pair<TKey, bool>, TKeyVisitorIntervalPtr> RemoteIntervals_;

    std::vector<TBucket> Buckets_;

    //! Cursor for round-robin GetNextRange.
    int NextBucketIndex_ = 0;

    //! Index of the first bucket whose Range contains |key|, or
    //! Buckets_.size() if |key| lies past every bucket.
    int FindBucketContaining(const TKey& key) const;

    //! Walks Buckets_ left to right starting at the first overlap with
    //! [range.Lower; range.Upper) and stamps each slice with |state|. When
    //! `state == Buffered` the slice is stamped with PassStartedAt = |when|;
    //! for Committed the existing stamp is preserved.
    void StampAcrossBuckets(const TKeyRange& range, EIntervalState state, TSystemTimestamp when);
};

DEFINE_REFCOUNTED_TYPE(TKeyVisitorStore);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
