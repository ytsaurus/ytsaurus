#include "key_visitor_states.h"

#include <yt/yt/flow/library/cpp/common/key.h>

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

bool TInMemoryKeyVisitorStates::TTableKeyCompare::operator()(const TTableKey& a, const TTableKey& b) const
{
    if (a.ComputationId != b.ComputationId) {
        return a.ComputationId < b.ComputationId;
    }
    if (a.StreamId != b.StreamId) {
        return a.StreamId < b.StreamId;
    }
    if (a.Key != b.Key) {
        return a.Key < b.Key;
    }
    return static_cast<int>(a.IsLower) < static_cast<int>(b.IsLower);
}

////////////////////////////////////////////////////////////////////////////////

void TInMemoryKeyVisitorStates::Write(
    NApi::IDynamicTableTransactionPtr /*transaction*/,
    const std::vector<std::pair<TTableKey, std::optional<TValue>>>& mutations)
{
    for (const auto& [tableKey, maybeValue] : mutations) {
        // Mirror the production encoding: MinKey/MaxKey strings are not persisted.
        if (tableKey.Key == MinKey() || tableKey.Key == MaxKey()) {
            continue;
        }
        if (maybeValue) {
            Storage_[tableKey] = *maybeValue;
            ++WriteCount_;
        } else {
            Storage_.erase(tableKey);
            ++DeleteCount_;
        }
    }
}

TFuture<TInMemoryKeyVisitorStates::TReadResult> TInMemoryKeyVisitorStates::Read(
    TTableKeyFilter filter,
    i64 limit,
    std::optional<TTableKey> offsetExclusive)
{
    TReadResult result;
    i64 count = 0;
    for (const auto& [tableKey, value] : Storage_) {
        if (filter.ComputationId && tableKey.ComputationId != *filter.ComputationId) {
            continue;
        }
        if (filter.StreamId && tableKey.StreamId != *filter.StreamId) {
            continue;
        }
        // Bracket the half-open partition range [LowerKey; UpperKey): mirror
        // the production filter, which includes (Key=LowerKey, IsLower=true)
        // and (Key=UpperKey, IsLower=false) and excludes the neighbour-owned
        // (Key=LowerKey, IsLower=false) and (Key=UpperKey, IsLower=true).
        if (filter.LowerKey) {
            if (tableKey.Key < *filter.LowerKey) {
                continue;
            }
            if (tableKey.Key == *filter.LowerKey && !tableKey.IsLower) {
                continue;
            }
        }
        if (filter.UpperKey) {
            if (*filter.UpperKey < tableKey.Key) {
                continue;
            }
            if (tableKey.Key == *filter.UpperKey && tableKey.IsLower) {
                continue;
            }
        }
        if (offsetExclusive && !TTableKeyCompare{}(*offsetExclusive, tableKey)) {
            continue;
        }
        if (count >= limit) {
            result.ContinuationOffsetExclusive = tableKey;
            break;
        }
        result.Rows.push_back({tableKey, value});
        ++count;
    }
    return MakeFuture(std::move(result));
}

TFuture<std::vector<std::pair<TInMemoryKeyVisitorStates::TTableKey, TInMemoryKeyVisitorStates::TValue>>>
TInMemoryKeyVisitorStates::ReadAll(TTableKeyFilter filter)
{
    std::vector<std::pair<TTableKey, TValue>> result;
    std::optional<TTableKey> offsetExclusive;
    constexpr i64 BatchSize = 1000;
    while (true) {
        auto batch = NConcurrency::WaitFor(Read(filter, BatchSize, offsetExclusive)).ValueOrThrow();
        result.insert(result.end(),
            std::make_move_iterator(batch.Rows.begin()),
            std::make_move_iterator(batch.Rows.end()));
        if (!batch.ContinuationOffsetExclusive) {
            break;
        }
        offsetExclusive = batch.ContinuationOffsetExclusive;
    }
    return MakeFuture(std::move(result));
}

i64 TInMemoryKeyVisitorStates::GetWriteCount() const
{
    return WriteCount_;
}

i64 TInMemoryKeyVisitorStates::GetDeleteCount() const
{
    return DeleteCount_;
}

i64 TInMemoryKeyVisitorStates::GetRowCount() const
{
    return std::ssize(Storage_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
