#include "timers.h"

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

bool TInMemoryTimers::TStorageKey::operator<(const TStorageKey& other) const
{
    if (ComputationId != other.ComputationId) {
        return ComputationId < other.ComputationId;
    }
    if (Key != other.Key) {
        return Key < other.Key;
    }
    return MessageId < other.MessageId;
}

////////////////////////////////////////////////////////////////////////////////

TFuture<ITimers::TLoadResult> TInMemoryTimers::Load(
    TFilter filter,
    i64 limit,
    std::optional<TTableKey> offsetExclusive)
{
    TLoadResult result;
    i64 count = 0;
    for (const auto& [storageKey, timer] : Storage_) {
        if (filter.ComputationId && storageKey.ComputationId != *filter.ComputationId) {
            continue;
        }
        if (filter.ExactKey && storageKey.Key != *filter.ExactKey) {
            continue;
        }
        if (filter.LowerKey && storageKey.Key < *filter.LowerKey) {
            continue;
        }
        if (filter.UpperKey && !(*filter.UpperKey < storageKey.Key) && storageKey.Key != *filter.UpperKey) {
            // UpperKey is exclusive upper bound.
        }
        if (offsetExclusive) {
            TStorageKey offsetKey{
                .ComputationId = offsetExclusive->ComputationId,
                .Key = offsetExclusive->Key,
                .MessageId = offsetExclusive->MessageId,
            };
            if (!(offsetKey < storageKey)) {
                continue;
            }
        }
        if (count >= limit) {
            result.ContinuationOffsetExclusive = TTableKey{
                .ComputationId = storageKey.ComputationId,
                .Key = storageKey.Key,
                .MessageId = storageKey.MessageId,
            };
            break;
        }
        result.Timers.emplace_back(
            TTableKey{
                .ComputationId = storageKey.ComputationId,
                .Key = storageKey.Key,
                .MessageId = storageKey.MessageId,
            },
            timer);
        ++count;
    }
    return MakeFuture(std::move(result));
}

TFuture<std::vector<std::pair<ITimers::TTableKey, TTimer>>>
TInMemoryTimers::LoadAll(TFilter filter)
{
    // Simulate TSelectLimiter: call Load() in a loop with a large limit until no offset.
    std::vector<std::pair<TTableKey, TTimer>> result;
    std::optional<TTableKey> offsetExclusive;
    constexpr i64 BatchSize = 1000;
    while (true) {
        auto batchResult = NConcurrency::WaitFor(Load(filter, BatchSize, offsetExclusive)).ValueOrThrow();
        result.insert(result.end(), batchResult.Timers.begin(), batchResult.Timers.end());
        if (!batchResult.ContinuationOffsetExclusive) {
            break;
        }
        offsetExclusive = batchResult.ContinuationOffsetExclusive;
    }
    return MakeFuture(std::move(result));
}

i64 TInMemoryTimers::GetWriteCount() const
{
    return WriteCount_;
}

void TInMemoryTimers::Write(
    NApi::IDynamicTableTransactionPtr /*transaction*/,
    const TComputationId& computationId,
    const std::vector<TTimer>& timers)
{
    for (const auto& timer : timers) {
        Storage_[TStorageKey{
            .ComputationId = computationId,
            .Key = timer.Key,
            .MessageId = timer.MessageId,
        }] = timer;
        ++WriteCount_;
    }
}

void TInMemoryTimers::Erase(
    NApi::IDynamicTableTransactionPtr /*transaction*/,
    const std::vector<TTableKey>& tableKeys)
{
    for (const auto& tableKey : tableKeys) {
        Storage_.erase(TStorageKey{
            .ComputationId = tableKey.ComputationId,
            .Key = tableKey.Key,
            .MessageId = tableKey.MessageId,
        });
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
