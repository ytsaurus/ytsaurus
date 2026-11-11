#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/computation.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/timer_page.h>

#include <yt/yt/flow/library/cpp/misc/cow_tree.h>

#include <yt/yt/flow/library/cpp/tables/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/system/spin_lock.h>

#include <map>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! The journaled fields of a page: what decides that the page exists and who may write it.
//! The private fields (upper timestamp bound, count, minima, dirty minima) are read from the table by
//! the store that needs them.
struct TTimerCataloguePage
{
    TComputationId ComputationId;
    TTimerFamilyId FamilyId;
    TSystemTimestamp Begin;
    TStreamId StreamId;
    TKeyRange KeyRange;
    ETimerPageKind Kind = ETimerPageKind::Trigger;
    bool OnceShared = false;
    bool EventDone = false;
    bool DedupSlots = false;

    static TTimerCataloguePage FromMeta(const TTimerPageMeta& page);
    static TTimerCataloguePage FromJournal(const TTimerJournalEntry& entry);

    bool operator==(const TTimerCataloguePage& other) const = default;
};

////////////////////////////////////////////////////////////////////////////////

//! An immutable snapshot of the pages known to the worker; copying is O(1).
class TTimerPageCatalogueSnapshot
{
public:
    struct TPageKey
    {
        TTimerFamilyId FamilyId;
        ETimerPageKind Kind = ETimerPageKind::Trigger;
        TSystemTimestamp Begin;

        bool operator<(const TPageKey& other) const;
    };

    //! Pages of the stream whose key ranges intersect |range|, by key range, family, kind and begin.
    std::vector<TTimerCataloguePage> FindPages(
        const TComputationId& computationId,
        const TStreamId& streamId,
        const TKeyRange& range) const;
    //! Pages of the family and kind, by timestamp.
    std::vector<TTimerCataloguePage> GetFamilyPages(TTimerFamilyId familyId, ETimerPageKind kind) const;
    std::optional<TTimerCataloguePage> FindPage(TTimerFamilyId familyId, ETimerPageKind kind, TSystemTimestamp beginTimestamp) const;
    std::vector<TTimerCataloguePage> GetPages() const;
    i64 GetPageCount() const;

private:
    friend class TTimerPageCatalogue;

    //! Pages of one stream ordered by key range: a partition's pages are found by scanning
    //! the key begins below its upper bound.
    struct TIndexKey
    {
        TComputationId ComputationId;
        TStreamId StreamId;
        TKey KeyBegin;
        TKey KeyEnd;
        TTimerFamilyId FamilyId;
        ETimerPageKind Kind = ETimerPageKind::Trigger;
        TSystemTimestamp Begin;

        bool operator<(const TIndexKey& other) const;
    };

    TCowMap<TPageKey, TTimerCataloguePage, std::less<TPageKey>> Pages_;
    TCowSet<TIndexKey, std::less<TIndexKey>> Index_;

    static TPageKey MakePageKey(const TTimerCataloguePage& page);
    static TIndexKey MakeIndexKey(const TTimerCataloguePage& page);

    void AddPage(const TTimerCataloguePage& page);
    //! Returns the removed page, if any.
    std::optional<TTimerCataloguePage> RemovePage(const TPageKey& key);
};

////////////////////////////////////////////////////////////////////////////////

struct TTimerPageCatalogueOptions
{
    TDuration JournalPollPeriod = TDuration::Seconds(1);
    i64 JournalReadLimit = 1000;
    //! Period of the empty page collection; null disables it.
    std::optional<TDuration> EmptyPageCollectionPeriod = TDuration::Minutes(5);
    //! Consecutive failed apply rounds after which the waiting synchronizations fail.
    int FailedRoundsToFailWaiters = 10;
};

//! Worker-wide page metadata snapshots, updated exclusively through the journal.
class TTimerPageCatalogue
    : public ITimerPageCatalogue
{
public:
    using TTransactionStarter = std::function<TFuture<NApi::ITransactionPtr>()>;

    TTimerPageCatalogue(
        NTables::ITimerPagesPtr tables,
        TTransactionStarter transactionStarter,
        IInvokerPtr invoker,
        NLogging::TLogger logger,
        NProfiling::TProfiler profiler,
        TTimerPageCatalogueOptions options = {});

    //! Bootstraps the catalogue on the first call, then completes once every journal entry
    //! committed before the call has been applied; fails when the catalogue keeps failing to
    //! read the tables or is stopped.
    TFuture<void> Synchronize();

    TTimerPageCatalogueSnapshot GetSnapshot() const;
    //! Increases on every change of the catalogue.
    i64 GetVersion() const;

    //! Removes the metadata of empty pages flagged once-shared; one pass.
    void CollectEmptyPages();

    TFuture<void> Stop();

private:
    const NTables::ITimerPagesPtr Tables_;
    const TTransactionStarter TransactionStarter_;
    const IInvokerPtr Invoker_;
    const NLogging::TLogger Logger;
    const TTimerPageCatalogueOptions Options_;

    struct TMetrics
    {
        NProfiling::TProfiler Profiler;
        NProfiling::TCounter Bootstraps = Profiler.Counter("/bootstraps");
        NProfiling::TCounter AppliedEntries = Profiler.Counter("/applied_entries");
        NProfiling::TGauge Pages = Profiler.Gauge("/pages");
        NProfiling::TGauge InternedKeys = Profiler.Gauge("/interned_keys");
        NProfiling::TCounter CollectedPages = Profiler.Counter("/collected_pages");
    };

    const TMetrics Metrics_;

    NConcurrency::TPeriodicExecutorPtr ApplyExecutor_;
    NConcurrency::TPeriodicExecutorPtr CollectExecutor_;

    //! Shares one key object among the pages with equal bounds.
    class TKeyInterner
    {
    public:
        TKey Intern(const TKey& key);
        void Release(const TKey& key);
        i64 GetSize() const;

    private:
        TCowMap<TKey, i64, std::less<TKey>> Uses_;
    };

    //! The snapshot with the journal position it is applied up to. The published one is
    //! read under |Lock_|; a bootstrap builds a private one and swaps it in.
    struct TState
    {
        TTimerPageCatalogueSnapshot Snapshot;
        TKeyInterner Interner;
        //! Journal row index the snapshot is applied up to.
        i64 NextRowIndex = 0;

        void Apply(const TTimerJournalEntry& entry, const NLogging::TLogger& Logger);
        void AddPage(TTimerCataloguePage page);
        void RemovePage(const TTimerPageCatalogueSnapshot::TPageKey& key);
    };

    YT_DECLARE_SPIN_LOCK(TSpinLock, Lock_);
    TState State_;
    i64 Version_ = 0;
    bool Bootstrapped_ = false;
    bool Stopped_ = false;

    struct TWaiter
    {
        i64 TargetRowIndex = 0;
        TPromise<void> Promise;
    };

    std::vector<TWaiter> Waiters_;

    //! Touched by the serialized applier only.
    int FailedRounds_ = 0;

    void Bootstrap();
    void ApplyJournal();
    void OnApplyRound();
    void FailWaiters(const TError& error);
    //! Reads the journal from the state's position until |targetRowIndex| is reached, applying
    //! the entries; returns false when a trim got ahead of the position, which may leave the
    //! state partially replayed.
    bool Replay(TState* state, i64 targetRowIndex, bool published);
    i64 GetAppliedRowIndex();

    //! Runs under |Lock_|.
    void Publish(std::vector<TPromise<void>>* fulfilled);

    void CollectFamilyPages(
        const TComputationId& computationId,
        TTimerFamilyId familyId,
        const std::vector<TTimerCataloguePage>& pages);
};

DEFINE_REFCOUNTED_TYPE(TTimerPageCatalogue);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
