#include "timer_page_catalogue.h"

#include <yt/yt/flow/library/cpp/tables/timer_pages.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/serialized_invoker.h>

#include <algorithm>
#include <map>
#include <type_traits>

namespace NYT::NFlow {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TTimerCataloguePage TTimerCataloguePage::FromMeta(const TTimerPageMeta& page)
{
    return TTimerCataloguePage{
        .ComputationId = page.ComputationId,
        .FamilyId = page.FamilyId,
        .Begin = page.Begin,
        .StreamId = page.StreamId,
        .KeyRange = page.KeyRange,
        .Kind = page.Kind,
        .OnceShared = page.Flags.OnceShared,
        .EventDone = page.Flags.EventDone,
        .DedupSlots = page.Flags.DedupSlots,
    };
}

TTimerCataloguePage TTimerCataloguePage::FromJournal(const TTimerJournalEntry& entry)
{
    return TTimerCataloguePage{
        .ComputationId = entry.ComputationId,
        .FamilyId = entry.FamilyId,
        .Begin = entry.Begin,
        .StreamId = entry.StreamId,
        .KeyRange = entry.KeyRange,
        .Kind = entry.Kind,
        .OnceShared = entry.Flags.OnceShared.value_or(false),
        .EventDone = entry.Flags.EventDone.value_or(false),
        .DedupSlots = entry.Flags.DedupSlots.value_or(false),
    };
}

////////////////////////////////////////////////////////////////////////////////

bool TTimerPageCatalogueSnapshot::TPageKey::operator<(const TPageKey& other) const
{
    if (FamilyId != other.FamilyId) {
        return FamilyId < other.FamilyId;
    }
    if (Kind != other.Kind) {
        return Kind < other.Kind;
    }
    return Begin < other.Begin;
}

bool TTimerPageCatalogueSnapshot::TIndexKey::operator<(const TIndexKey& other) const
{
    if (ComputationId != other.ComputationId) {
        return ComputationId < other.ComputationId;
    }
    if (StreamId != other.StreamId) {
        return StreamId < other.StreamId;
    }
    if (KeyBegin != other.KeyBegin) {
        return KeyBegin < other.KeyBegin;
    }
    if (KeyEnd != other.KeyEnd) {
        return KeyEnd < other.KeyEnd;
    }
    if (FamilyId != other.FamilyId) {
        return FamilyId < other.FamilyId;
    }
    if (Kind != other.Kind) {
        return Kind < other.Kind;
    }
    return Begin < other.Begin;
}

std::vector<TTimerCataloguePage> TTimerPageCatalogueSnapshot::FindPages(
    const TComputationId& computationId,
    const TStreamId& streamId,
    const TKeyRange& range) const
{
    std::vector<TTimerCataloguePage> result;
    auto it = Index_.lower_bound(TIndexKey{
        .ComputationId = computationId,
        .StreamId = streamId,
        .KeyBegin = MinKey(),
        .KeyEnd = MinKey(),
        .FamilyId = TTimerFamilyId(0),
        .Begin = TSystemTimestamp(0),
    });
    for (; it != Index_.end(); ++it) {
        const auto& key = *it;
        if (key.ComputationId != computationId || key.StreamId != streamId || !(key.KeyBegin < range.Upper)) {
            break;
        }
        if (range.Lower < key.KeyEnd) {
            auto page = FindPage(key.FamilyId, key.Kind, key.Begin);
            YT_VERIFY(page);
            result.push_back(std::move(*page));
        }
    }
    return result;
}

std::vector<TTimerCataloguePage> TTimerPageCatalogueSnapshot::GetFamilyPages(TTimerFamilyId familyId, ETimerPageKind kind) const
{
    std::vector<TTimerCataloguePage> result;
    auto it = Pages_.lower_bound(TPageKey{.FamilyId = familyId, .Kind = kind, .Begin = TSystemTimestamp(0)});
    for (; it != Pages_.end(); ++it) {
        const auto& [key, page] = *it;
        if (key.FamilyId != familyId || key.Kind != kind) {
            break;
        }
        result.push_back(page);
    }
    return result;
}

std::optional<TTimerCataloguePage> TTimerPageCatalogueSnapshot::FindPage(TTimerFamilyId familyId, ETimerPageKind kind, TSystemTimestamp beginTimestamp) const
{
    auto it = Pages_.find(TPageKey{.FamilyId = familyId, .Kind = kind, .Begin = beginTimestamp});
    if (it == Pages_.end()) {
        return std::nullopt;
    }
    return it->second;
}

std::vector<TTimerCataloguePage> TTimerPageCatalogueSnapshot::GetPages() const
{
    std::vector<TTimerCataloguePage> result;
    result.reserve(Pages_.size());
    for (const auto& [key, page] : Pages_) {
        result.push_back(page);
    }
    return result;
}

i64 TTimerPageCatalogueSnapshot::GetPageCount() const
{
    return std::ssize(Pages_);
}

TTimerPageCatalogueSnapshot::TPageKey TTimerPageCatalogueSnapshot::MakePageKey(const TTimerCataloguePage& page)
{
    return TPageKey{.FamilyId = page.FamilyId, .Kind = page.Kind, .Begin = page.Begin};
}

TTimerPageCatalogueSnapshot::TIndexKey TTimerPageCatalogueSnapshot::MakeIndexKey(const TTimerCataloguePage& page)
{
    return TIndexKey{
        .ComputationId = page.ComputationId,
        .StreamId = page.StreamId,
        .KeyBegin = page.KeyRange.Lower,
        .KeyEnd = page.KeyRange.Upper,
        .FamilyId = page.FamilyId,
        .Kind = page.Kind,
        .Begin = page.Begin,
    };
}

void TTimerPageCatalogueSnapshot::AddPage(const TTimerCataloguePage& page)
{
    RemovePage(MakePageKey(page));
    Pages_.insert(std::pair(MakePageKey(page), page));
    Index_.insert(MakeIndexKey(page));
}

std::optional<TTimerCataloguePage> TTimerPageCatalogueSnapshot::RemovePage(const TPageKey& key)
{
    auto it = Pages_.find(key);
    if (it == Pages_.end()) {
        return std::nullopt;
    }
    auto page = it->second;
    Index_.erase(MakeIndexKey(page));
    Pages_.erase(key);
    return page;
}

////////////////////////////////////////////////////////////////////////////////

TKey TTimerPageCatalogue::TKeyInterner::Intern(const TKey& key)
{
    auto [it, inserted] = Uses_.insert(std::pair(key, i64(0)));
    ++it->second;
    return it->first;
}

void TTimerPageCatalogue::TKeyInterner::Release(const TKey& key)
{
    auto it = Uses_.find(key);
    YT_VERIFY(it != Uses_.end() && it->second > 0);
    if (--it->second == 0) {
        Uses_.erase(it);
    }
}

i64 TTimerPageCatalogue::TKeyInterner::GetSize() const
{
    return std::ssize(Uses_);
}

////////////////////////////////////////////////////////////////////////////////

void TTimerPageCatalogue::TState::Apply(const TTimerJournalEntry& entry, const NLogging::TLogger& Logger)
{
    auto key = TTimerPageCatalogueSnapshot::TPageKey{.FamilyId = entry.FamilyId, .Kind = entry.Kind, .Begin = entry.Begin};
    switch (entry.Op) {
        case ETimerJournalOp::Create:
            AddPage(TTimerCataloguePage::FromJournal(entry));
            break;
        case ETimerJournalOp::Delete:
            RemovePage(key);
            break;
        case ETimerJournalOp::SetFlags:
            if (auto page = Snapshot.FindPage(entry.FamilyId, entry.Kind, entry.Begin)) {
                page->OnceShared = entry.Flags.OnceShared.value_or(page->OnceShared);
                page->EventDone = entry.Flags.EventDone.value_or(page->EventDone);
                page->DedupSlots = entry.Flags.DedupSlots.value_or(page->DedupSlots);
                AddPage(std::move(*page));
            } else {
                YT_TLOG_WARNING("Timer page flags journaled for an unknown page")
                    .With("ComputationId", entry.ComputationId)
                    .With("FamilyId", entry.FamilyId)
                    .With("Begin", entry.Begin);
            }
            break;
    }
}

void TTimerPageCatalogue::TState::AddPage(TTimerCataloguePage page)
{
    RemovePage(TTimerPageCatalogueSnapshot::MakePageKey(page));
    page.KeyRange.Lower = Interner.Intern(page.KeyRange.Lower);
    page.KeyRange.Upper = Interner.Intern(page.KeyRange.Upper);
    Snapshot.AddPage(page);
}

void TTimerPageCatalogue::TState::RemovePage(const TTimerPageCatalogueSnapshot::TPageKey& key)
{
    if (auto page = Snapshot.RemovePage(key)) {
        Interner.Release(page->KeyRange.Lower);
        Interner.Release(page->KeyRange.Upper);
    }
}

////////////////////////////////////////////////////////////////////////////////

TTimerPageCatalogue::TTimerPageCatalogue(
    NTables::ITimerPagesPtr tables,
    TTransactionStarter transactionStarter,
    IInvokerPtr invoker,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler,
    TTimerPageCatalogueOptions options)
    : Tables_(std::move(tables))
    , TransactionStarter_(std::move(transactionStarter))
    , Invoker_(std::move(invoker))
    , Logger(std::move(logger))
    , Options_(options)
    , Metrics_{.Profiler = profiler.WithPrefix("/timer_page_catalogue")}
    , ApplyExecutor_(New<TPeriodicExecutor>(
        CreateSerializedInvoker(Invoker_),
        BIND(&TTimerPageCatalogue::OnApplyRound, MakeWeak(this)),
        Options_.JournalPollPeriod))
    , CollectExecutor_(Options_.EmptyPageCollectionPeriod
            ? New<TPeriodicExecutor>(
                // Not serialized with the applier: a pass waits for it in |Synchronize|.
                Invoker_,
                BIND(&TTimerPageCatalogue::CollectEmptyPages, MakeWeak(this)),
                TPeriodicExecutorOptions::WithJitter(*Options_.EmptyPageCollectionPeriod))
            : nullptr)
{ }

TFuture<void> TTimerPageCatalogue::Synchronize()
{
    return Tables_->GetJournalRowRange()
        .Apply(BIND([this, this_ = MakeStrong(this)] (const NTables::ITimerPages::TJournalRowRange& range) -> TFuture<void> {
            auto promise = NewPromise<void>();
            {
                auto guard = Guard(Lock_);
                if (Stopped_) {
                    return MakeFuture<void>(TError("Timer page catalogue is stopped"));
                }
                if (Bootstrapped_ && State_.NextRowIndex >= range.TotalRowCount) {
                    return OKFuture;
                }
                Waiters_.push_back(TWaiter{.TargetRowIndex = range.TotalRowCount, .Promise = promise});
                ApplyExecutor_->Start();
                ApplyExecutor_->ScheduleOutOfBand();
            }
            return promise.ToFuture();
        }));
}

TTimerPageCatalogueSnapshot TTimerPageCatalogue::GetSnapshot() const
{
    auto guard = Guard(Lock_);
    return State_.Snapshot;
}

i64 TTimerPageCatalogue::GetVersion() const
{
    auto guard = Guard(Lock_);
    return Version_;
}

TFuture<void> TTimerPageCatalogue::Stop()
{
    {
        auto guard = Guard(Lock_);
        Stopped_ = true;
    }
    std::vector<TFuture<void>> futures{ApplyExecutor_->Stop()};
    if (CollectExecutor_) {
        futures.push_back(CollectExecutor_->Stop());
    }
    FailWaiters(TError("Timer page catalogue is stopped"));
    return AllSucceeded(std::move(futures));
}

void TTimerPageCatalogue::OnApplyRound()
{
    try {
        bool bootstrapped;
        {
            auto guard = Guard(Lock_);
            bootstrapped = Bootstrapped_;
        }
        if (!bootstrapped) {
            Bootstrap();
        }
        ApplyJournal();
        FailedRounds_ = 0;
    } catch (const std::exception& ex) {
        YT_TLOG_WARNING("Failed to apply the timer page journal")
            .With("FailedRounds", FailedRounds_ + 1)
            .With(ex);
        // A persistent failure (an unreadable table, a journal row of an unknown format) must
        // reach the waiting jobs as an error rather than hang them.
        if (++FailedRounds_ >= Options_.FailedRoundsToFailWaiters) {
            FailWaiters(TError(ex).Wrap("Timer page catalogue cannot be synchronized"));
        }
    }
}

void TTimerPageCatalogue::FailWaiters(const TError& error)
{
    std::vector<TWaiter> waiters;
    {
        auto guard = Guard(Lock_);
        waiters = std::exchange(Waiters_, {});
    }
    for (const auto& waiter : waiters) {
        waiter.Promise.TrySet(error);
    }
}

i64 TTimerPageCatalogue::GetAppliedRowIndex()
{
    auto guard = Guard(Lock_);
    return State_.NextRowIndex;
}

void TTimerPageCatalogue::Bootstrap()
{
    // The metadata read sees every entry committed before the first row count and some of the
    // entries committed before the second one. Replaying the journal between the two over the
    // metadata privately brings the state to the second row count: an entry older than the
    // metadata is overridden by the newer entries that follow it, and nobody observes the
    // intermediate states. Publishing the metadata as is would expose them.
    auto before = WaitFor(Tables_->GetJournalRowRange()).ValueOrThrow();
    auto pages = WaitFor(Tables_->LoadMeta({})).ValueOrThrow();
    auto after = WaitFor(Tables_->GetJournalRowRange()).ValueOrThrow();

    TState state;
    for (const auto& page : pages) {
        state.AddPage(TTimerCataloguePage::FromMeta(page));
    }
    state.NextRowIndex = before.TotalRowCount;
    // A trim inside the overlap leaves the private state half replayed: it is dropped, and the
    // next round bootstraps again from a fresh metadata read.
    if (!Replay(&state, after.TotalRowCount, /*published*/ false)) {
        THROW_ERROR_EXCEPTION("Timer page journal was trimmed during a bootstrap");
    }

    std::vector<TPromise<void>> fulfilled;
    {
        auto guard = Guard(Lock_);
        // The previous state leaves with |state| and is destroyed outside the lock.
        fulfilled.reserve(Waiters_.size());
        std::swap(State_, state);
        Bootstrapped_ = true;
        ++Version_;
        Publish(&fulfilled);
    }
    for (const auto& promise : fulfilled) {
        promise.Set();
    }
    Metrics_.Bootstraps.Increment();
    YT_TLOG_INFO("Timer page catalogue bootstrapped")
        .With("Pages", pages.size())
        .With("JournalRowIndex", after.TotalRowCount);

    auto guard = Guard(Lock_);
    if (CollectExecutor_ && !Stopped_) {
        CollectExecutor_->Start();
    }
}

void TTimerPageCatalogue::ApplyJournal()
{
    // A trim past the applied position loses entries for good: a read from there may even
    // come back empty, so the trimmed row count is checked rather than the read.
    auto range = WaitFor(Tables_->GetJournalRowRange()).ValueOrThrow();
    if (range.TrimmedRowCount > GetAppliedRowIndex()) {
        YT_TLOG_WARNING("Timer page journal was trimmed past the applied position; bootstrapping again")
            .With("AppliedRowIndex", GetAppliedRowIndex())
            .With("TrimmedRowCount", range.TrimmedRowCount);
        Bootstrap();
    }
    // A copy of the published state (the snapshot is copy-on-write) takes the batches and is
    // swapped in under the lock. Copies share nodes, so even this one is taken under it.
    TState state;
    {
        auto guard = Guard(Lock_);
        state = State_;
    }
    Replay(&state, range.TotalRowCount, /*published*/ true);
}

bool TTimerPageCatalogue::Replay(TState* state, i64 targetRowIndex, bool published)
{
    while (true) {
        i64 fromRowIndex = state->NextRowIndex;
        auto result = WaitFor(Tables_->ReadJournal(fromRowIndex, Options_.JournalReadLimit)).ValueOrThrow();
        if (!result.Entries.empty() && result.Entries.front().RowIndex > fromRowIndex) {
            // Trimmed since the range check; the next round bootstraps.
            YT_TLOG_WARNING("Timer page journal was trimmed during a read")
                .With("AppliedRowIndex", fromRowIndex)
                .With("FirstRowIndex", result.Entries.front().RowIndex);
            return false;
        }

        for (const auto& entry : result.Entries) {
            state->Apply(entry, Logger);
        }
        state->NextRowIndex = std::max(state->NextRowIndex, result.NextRowIndex);
        if (published) {
            std::vector<TPromise<void>> fulfilled;
            TState previous;
            {
                auto guard = Guard(Lock_);
                previous = *state;
                static_assert(std::is_nothrow_swappable_v<TState>);
                std::swap(State_, previous);
                if (!result.Entries.empty()) {
                    ++Version_;
                }
                Publish(&fulfilled);
            }
            for (const auto& promise : fulfilled) {
                promise.Set();
            }
        }
        Metrics_.AppliedEntries.Increment(std::ssize(result.Entries));

        if (state->NextRowIndex >= targetRowIndex) {
            return true;
        }
        if (std::ssize(result.Entries) < Options_.JournalReadLimit) {
            // The rows up to the target existed when it was observed, so they were trimmed.
            YT_TLOG_WARNING("Timer page journal was trimmed before the replay target")
                .With("AppliedRowIndex", state->NextRowIndex)
                .With("TargetRowIndex", targetRowIndex);
            return false;
        }
    }
}

void TTimerPageCatalogue::Publish(std::vector<TPromise<void>>* fulfilled)
{
    YT_ASSERT_SPINLOCK_AFFINITY(Lock_);
    fulfilled->reserve(Waiters_.size());
    Metrics_.Pages.Update(State_.Snapshot.GetPageCount());
    Metrics_.InternedKeys.Update(State_.Interner.GetSize());
    std::erase_if(Waiters_, [&] (const TWaiter& waiter) {
        if (waiter.TargetRowIndex > State_.NextRowIndex) {
            return false;
        }
        fulfilled->push_back(waiter.Promise);
        return true;
    });
}

////////////////////////////////////////////////////////////////////////////////

void TTimerPageCatalogue::CollectEmptyPages()
{
    // Page deletions of the previous pass reach the snapshot through the journal only. An
    // exception escaping the callback would stop the periodic executor for good.
    if (auto error = WaitFor(Synchronize()); !error.IsOK()) {
        YT_TLOG_WARNING("Failed to synchronize the timer page catalogue before a collection").With(error);
        return;
    }

    std::map<std::pair<TComputationId, TTimerFamilyId>, std::vector<TTimerCataloguePage>> candidates;
    for (const auto& page : GetSnapshot().GetPages()) {
        if (page.OnceShared) {
            candidates[std::pair(page.ComputationId, page.FamilyId)].push_back(page);
        }
    }
    for (const auto& [family, pages] : candidates) {
        try {
            CollectFamilyPages(family.first, family.second, pages);
        } catch (const std::exception& ex) {
            YT_TLOG_WARNING("Failed to collect empty timer pages")
                .With("ComputationId", family.first)
                .With("FamilyId", family.second)
                .With(ex);
        }
    }
}

void TTimerPageCatalogue::CollectFamilyPages(
    const TComputationId& computationId,
    TTimerFamilyId familyId,
    const std::vector<TTimerCataloguePage>& pages)
{
    auto load = [&] {
        return WaitFor(Tables_->LoadMeta({.ComputationId = computationId, .FamilyId = familyId})).ValueOrThrow();
    };
    auto metas = load();
    if (std::ranges::none_of(metas, [] (const auto& page) {
            return page.Flags.OnceShared && !page.Count;
        })) {
        return;
    }
    auto transaction = WaitFor(TransactionStarter_()).ValueOrThrow();
    metas = load();
    i64 collected = 0;
    auto previous = metas.end();
    for (auto it = metas.begin(); it != metas.end(); ++it) {
        auto candidate = std::pair(it->Kind, it->Begin);
        auto found = std::ranges::binary_search(pages, candidate, {}, [] (const auto& page) {
            return std::pair(page.Kind, page.Begin);
        });
        if (!it->Flags.OnceShared || it->Count || !found) {
            previous = it;
            continue;
        }
        // The count is a sum of blind deltas, and a page whose metadata vanished would orphan its rows.
        if (WaitFor(Tables_->Aggregate(*it)).ValueOrThrow().Count != 0) {
            YT_TLOG_WARNING("Empty-looking timer page still has rows; keeping it")
                .With("ComputationId", computationId)
                .With("FamilyId", familyId)
                .With("BeginTimestamp", it->Begin);
            previous = it;
            continue;
        }
        auto next = std::next(it);
        bool hasPrevious = previous != metas.end() && previous->Kind == it->Kind;
        bool hasNext = next != metas.end() && next->Kind == it->Kind;
        // Renaming a nonempty shared page would race its partitions' count deltas.
        if (!hasPrevious && hasNext) {
            previous = it;
            continue;
        }
        if (hasPrevious) {
            previous->End = it->End;
            Tables_->UpdatePage(transaction, {
                    .ComputationId = computationId,
                    .FamilyId = familyId,
                    .Kind = previous->Kind,
                    .Begin = previous->Begin,
                    .End = std::make_optional(previous->End),
                                             });
        }
        Tables_->ErasePage(transaction, computationId, familyId, it->Kind, it->Begin);
        Tables_->WriteJournal(transaction, {TTimerJournalEntry::Delete(*it)});
        ++collected;
    }
    if (collected && transaction) {
        WaitFor(transaction->Commit()).ThrowOnError();
    }
    Metrics_.CollectedPages.Increment(collected);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
