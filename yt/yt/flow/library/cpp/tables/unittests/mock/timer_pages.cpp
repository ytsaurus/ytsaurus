#include "timer_pages.h"

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

void TInMemoryTimerPages::Reconfigure(TDynamicTableRequestSpecPtr /*dynamicSpec*/)
{ }

TFuture<bool> TInMemoryTimerPages::TablesExist()
{
    return MakeFuture(true);
}

TFuture<std::vector<TTimerPageRow>> TInMemoryTimerPages::ReadPage(
    TTimerPageMeta page,
    std::optional<TKeyRange> keyRange)
{
    CallHook("ReadPage");
    auto guard = Guard(Lock_);
    ++PageReadCount_;
    std::vector<TTimerPageRow> result;
    auto begin = Rows_.lower_bound(TTimerPageRowKey{.FamilyId = page.FamilyId, .Kind = page.Kind, .Timestamp = page.Begin, .Key = MinKey()});
    for (auto it = begin; it != Rows_.end(); ++it) {
        const auto& [rowKey, timer] = *it;
        if (rowKey.FamilyId != page.FamilyId || rowKey.Kind != page.Kind || (page.End && rowKey.Timestamp >= *page.End)) {
            break;
        }
        if (keyRange && !keyRange->Contains(rowKey.Key)) {
            continue;
        }
        result.push_back(TTimerPageRow{.RowKey = rowKey, .Timer = timer});
    }
    return MakeFuture(std::move(result));
}

TFuture<TTimerPageStatistics> TInMemoryTimerPages::Aggregate(TTimerPageMeta page)
{
    auto buckets = DoAggregate(page, std::nullopt);
    return MakeFuture(buckets.empty() ? TTimerPageStatistics{} : buckets.front().Statistics);
}

TFuture<std::optional<TSystemTimestamp>> TInMemoryTimerPages::FindMedianTimestamp(TTimerPageMeta page)
{
    CallHook("FindMedianTimestamp");
    auto guard = Guard(Lock_);
    i64 offset = page.Count / 2;
    for (const auto& [key, timer] : Rows_) {
        if (key.FamilyId == page.FamilyId && key.Kind == page.Kind && key.Timestamp >= page.Begin &&
            (!page.End || key.Timestamp < *page.End) && offset-- == 0)
        {
            return MakeFuture(std::optional(key.Timestamp));
        }
    }
    return MakeFuture(std::optional<TSystemTimestamp>());
}

TFuture<std::pair<TTimerPageStatistics, TTimerPageStatistics>> TInMemoryTimerPages::AggregateSplit(
    TTimerPageMeta page,
    TSystemTimestamp cut)
{
    std::pair<TTimerPageStatistics, TTimerPageStatistics> result;
    for (const auto& bucket : DoAggregate(page, cut)) {
        (bucket.Begin < cut ? result.first : result.second) = bucket.Statistics;
    }
    return MakeFuture(result);
}

std::vector<TTimerPageBucket> TInMemoryTimerPages::DoAggregate(const TTimerPageMeta& page, std::optional<TSystemTimestamp> cut)
{
    CallHook(cut ? "AggregateSplit" : "Aggregate");
    auto guard = Guard(Lock_);
    ++AggregateCount_;
    std::map<TSystemTimestamp, TTimerPageStatistics> buckets;
    for (const auto& [key, timer] : Rows_) {
        if (key.FamilyId != page.FamilyId || key.Kind != page.Kind || key.Timestamp < page.Begin ||
            (page.End && key.Timestamp >= *page.End))
        {
            continue;
        }
        auto begin = cut && key.Timestamp >= *cut ? *cut : page.Begin;
        buckets[begin].Add(timer, page.Kind);
    }
    std::vector<TTimerPageBucket> result;
    for (const auto& [begin, statistics] : buckets) {
        result.push_back({.Begin = begin, .Statistics = statistics});
    }
    return result;
}

TFuture<std::vector<std::optional<TTimerPageRow>>> TInMemoryTimerPages::LookupRows(std::vector<TTimerPageRowKey> rowKeys)
{
    auto guard = Guard(Lock_);
    ++LookupCount_;
    std::vector<std::optional<TTimerPageRow>> result(rowKeys.size());
    for (int index = 0; index < std::ssize(rowKeys); ++index) {
        auto it = Rows_.find(rowKeys[index]);
        if (it != Rows_.end()) {
            result[index] = TTimerPageRow{.RowKey = rowKeys[index], .Timer = it->second};
        }
    }
    return MakeFuture(std::move(result));
}

void TInMemoryTimerPages::WriteRows(
    NApi::IDynamicTableTransactionPtr /*transaction*/,
    const std::vector<TTimerPageRow>& rows)
{
    auto guard = Guard(Lock_);
    for (const auto& [rowKey, timer] : rows) {
        auto stored = timer;
        // The table keeps the key and the four value columns only.
        stored.AlignmentTimestamp = timer.SystemTimestamp;
        stored.StreamId = TStreamId();
        stored.KeySchema = nullptr;
        Rows_[rowKey] = std::move(stored);
        ++RowWriteCount_;
    }
}

void TInMemoryTimerPages::EraseRows(
    NApi::IDynamicTableTransactionPtr /*transaction*/,
    const std::vector<TTimerPageRowKey>& rowKeys)
{
    auto guard = Guard(Lock_);
    for (const auto& rowKey : rowKeys) {
        Rows_.erase(rowKey);
    }
}

////////////////////////////////////////////////////////////////////////////////

TFuture<std::vector<TTimerPageMeta>> TInMemoryTimerPages::LoadMeta(TMetaFilter filter)
{
    CallHook("LoadMeta");
    auto guard = Guard(Lock_);
    std::vector<TTimerPageMeta> pages;
    for (const auto& [key, page] : Pages_) {
        if ((!filter.ComputationId || std::get<0>(key) == *filter.ComputationId) &&
            (!filter.FamilyId || std::get<1>(key) == *filter.FamilyId))
        {
            pages.push_back(page);
        }
    }
    return MakeFuture(std::move(pages));
}

void TInMemoryTimerPages::WritePage(
    NApi::IDynamicTableTransactionPtr /*transaction*/,
    const TTimerPageMeta& page)
{
    auto guard = Guard(Lock_);
    ++PageWriteCount_;
    Pages_[TMetaKey{page.ComputationId, page.FamilyId, page.Kind, page.Begin}] = page;
}

void TInMemoryTimerPages::UpdatePage(
    NApi::IDynamicTableTransactionPtr /*transaction*/,
    const TPageUpdate& update)
{
    auto guard = Guard(Lock_);
    if (update.End || !update.Flags.IsEmpty() || update.Minima || update.CountDelta || update.MaxTimestamp) {
        ++PageWriteCount_;
    }
    auto& page = Pages_[TMetaKey{update.ComputationId, update.FamilyId, update.Kind, update.Begin}];
    page.ComputationId = update.ComputationId;
    page.FamilyId = update.FamilyId;
    page.Kind = update.Kind;
    page.Begin = update.Begin;
    if (update.End) {
        page.End = *update.End;
    }
    update.Flags.ApplyTo(&page.Flags);
    if (update.Minima) {
        page.MinEvent = update.Minima->first;
        page.MinSystem = update.Minima->second;
    }
    page.Count += update.CountDelta;
    if (update.MaxTimestamp) {
        page.MaxTimestamp = page.MaxTimestamp ? std::max(*page.MaxTimestamp, *update.MaxTimestamp) : *update.MaxTimestamp;
    }
}

void TInMemoryTimerPages::ErasePage(
    NApi::IDynamicTableTransactionPtr /*transaction*/,
    const TComputationId& computationId,
    TTimerFamilyId familyId,
    ETimerPageKind kind,
    TSystemTimestamp begin)
{
    auto guard = Guard(Lock_);
    ++PageWriteCount_;
    Pages_.erase(TMetaKey{computationId, familyId, kind, begin});
}

////////////////////////////////////////////////////////////////////////////////

void TInMemoryTimerPages::WriteJournal(
    NApi::IDynamicTableTransactionPtr /*transaction*/,
    const std::vector<TTimerJournalEntry>& entries)
{
    auto guard = Guard(Lock_);
    for (auto entry : entries) {
        entry.RowIndex = JournalTrimmedRowCount_ + std::ssize(Journal_);
        Journal_.push_back(std::move(entry));
    }
}

TFuture<ITimerPages::TJournalRowRange> TInMemoryTimerPages::GetJournalRowRange()
{
    CallHook("GetJournalRowRange");
    auto guard = Guard(Lock_);
    return MakeFuture(TJournalRowRange{
        .TrimmedRowCount = JournalTrimmedRowCount_,
        .TotalRowCount = JournalTrimmedRowCount_ + std::ssize(Journal_),
    });
}

TFuture<ITimerPages::TJournalReadResult> TInMemoryTimerPages::ReadJournal(i64 fromRowIndex, i64 limit)
{
    CallHook("ReadJournal");
    auto guard = Guard(Lock_);
    TJournalReadResult result;
    result.NextRowIndex = fromRowIndex;
    i64 first = std::max<i64>(0, fromRowIndex - JournalTrimmedRowCount_);
    for (i64 index = first; index < std::ssize(Journal_) && std::ssize(result.Entries) < limit; ++index) {
        result.Entries.push_back(Journal_[index]);
    }
    if (!result.Entries.empty()) {
        result.NextRowIndex = result.Entries.back().RowIndex + 1;
    }
    return MakeFuture(std::move(result));
}

void TInMemoryTimerPages::SetHook(std::function<void(TStringBuf)> hook)
{
    auto guard = Guard(Lock_);
    Hook_ = std::move(hook);
}

void TInMemoryTimerPages::CallHook(TStringBuf method)
{
    std::function<void(TStringBuf)> hook;
    {
        auto guard = Guard(Lock_);
        hook = Hook_;
    }
    if (hook) {
        hook(method);
    }
}

void TInMemoryTimerPages::TrimJournal(i64 rowCount)
{
    auto guard = Guard(Lock_);
    i64 count = std::clamp<i64>(rowCount - JournalTrimmedRowCount_, 0, std::ssize(Journal_));
    Journal_.erase(Journal_.begin(), Journal_.begin() + count);
    JournalTrimmedRowCount_ += count;
}

////////////////////////////////////////////////////////////////////////////////

i64 TInMemoryTimerPages::GetRowCount() const
{
    auto guard = Guard(Lock_);
    return std::ssize(Rows_);
}

i64 TInMemoryTimerPages::GetPageReadCount() const
{
    auto guard = Guard(Lock_);
    return PageReadCount_;
}

i64 TInMemoryTimerPages::GetLookupCount() const
{
    auto guard = Guard(Lock_);
    return LookupCount_;
}

i64 TInMemoryTimerPages::GetAggregateCount() const
{
    auto guard = Guard(Lock_);
    return AggregateCount_;
}

i64 TInMemoryTimerPages::GetPageWriteCount() const
{
    auto guard = Guard(Lock_);
    return PageWriteCount_;
}

i64 TInMemoryTimerPages::GetRowWriteCount() const
{
    auto guard = Guard(Lock_);
    return RowWriteCount_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
