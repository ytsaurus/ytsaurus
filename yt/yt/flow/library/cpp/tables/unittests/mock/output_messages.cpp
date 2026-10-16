#include "output_messages.h"

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

bool TInMemoryOutputMessages::TStorageKey::operator<(const TStorageKey& other) const
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

TFuture<IOutputMessages::TLoadResult> TInMemoryOutputMessages::Load(
    TFilter filter,
    i64 limit,
    std::optional<TTableKey> offsetExclusive)
{
    TLoadResult result;
    i64 count = 0;
    for (const auto& [storageKey, proto] : Storage_) {
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
        result.Messages.emplace_back(
            TTableKey{
                .ComputationId = storageKey.ComputationId,
                .Key = storageKey.Key,
                .MessageId = storageKey.MessageId,
            },
            proto);
        ++count;
    }
    return MakeFuture(std::move(result));
}

TFuture<std::vector<std::pair<IOutputMessages::TTableKey, NProto::TMessage>>>
TInMemoryOutputMessages::LoadAll(TFilter filter)
{
    // Simulate TSelectLimiter: call Load() in a loop with a large limit until no offset.
    std::vector<std::pair<TTableKey, NProto::TMessage>> result;
    std::optional<TTableKey> offsetExclusive;
    constexpr i64 BatchSize = 1000;
    while (true) {
        auto batchResult = NConcurrency::WaitFor(Load(filter, BatchSize, offsetExclusive)).ValueOrThrow();
        result.insert(result.end(), batchResult.Messages.begin(), batchResult.Messages.end());
        if (!batchResult.ContinuationOffsetExclusive) {
            break;
        }
        offsetExclusive = batchResult.ContinuationOffsetExclusive;
    }
    return MakeFuture(std::move(result));
}

i64 TInMemoryOutputMessages::GetWriteCount() const
{
    return WriteCount_;
}

void TInMemoryOutputMessages::Write(
    NApi::IDynamicTableTransactionPtr /*transaction*/,
    const std::vector<std::pair<TTableKey, NProto::TMessage>>& messages,
    NCompression::ECodec /*codecId*/)
{
    for (const auto& [tableKey, proto] : messages) {
        Storage_[TStorageKey{
            .ComputationId = tableKey.ComputationId,
            .Key = tableKey.Key,
            .MessageId = tableKey.MessageId,
        }] = proto;
        ++WriteCount_;
    }
}

void TInMemoryOutputMessages::Erase(
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
