#include "partition_output_messages.h"

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

bool TInMemoryPartitionOutputMessages::TStorageKey::operator<(const TStorageKey& other) const
{
    if (PartitionId != other.PartitionId) {
        return PartitionId < other.PartitionId;
    }
    return MessageId < other.MessageId;
}

////////////////////////////////////////////////////////////////////////////////

TFuture<IPartitionOutputMessages::TLoadResult> TInMemoryPartitionOutputMessages::Load(
    TFilter filter,
    i64 limit,
    std::optional<TTableKey> offsetExclusive)
{
    TLoadResult result;
    i64 count = 0;
    for (const auto& [storageKey, proto] : Storage_) {
        if (filter.PartitionId && storageKey.PartitionId != *filter.PartitionId) {
            continue;
        }
        if (offsetExclusive) {
            TStorageKey offsetKey{
                .PartitionId = offsetExclusive->PartitionId,
                .MessageId = offsetExclusive->MessageId,
            };
            if (!(offsetKey < storageKey)) {
                continue;
            }
        }
        if (count >= limit) {
            result.ContinuationOffsetExclusive = TTableKey{
                .PartitionId = storageKey.PartitionId,
                .MessageId = storageKey.MessageId,
            };
            break;
        }
        result.Messages.emplace_back(
            TTableKey{
                .PartitionId = storageKey.PartitionId,
                .MessageId = storageKey.MessageId,
            },
            proto);
        ++count;
    }
    return MakeFuture(std::move(result));
}

TFuture<std::vector<std::pair<IPartitionOutputMessages::TTableKey, NProto::TMessage>>>
TInMemoryPartitionOutputMessages::LoadAll(TFilter filter)
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

i64 TInMemoryPartitionOutputMessages::GetWriteCount() const
{
    return WriteCount_;
}

void TInMemoryPartitionOutputMessages::Write(
    NApi::IDynamicTableTransactionPtr /*transaction*/,
    const std::vector<std::pair<TTableKey, NProto::TMessage>>& messages,
    NCompression::ECodec /*codecId*/)
{
    for (const auto& [tableKey, proto] : messages) {
        Storage_[TStorageKey{
            .PartitionId = tableKey.PartitionId,
            .MessageId = tableKey.MessageId,
        }] = proto;
        ++WriteCount_;
    }
}

void TInMemoryPartitionOutputMessages::Erase(
    NApi::IDynamicTableTransactionPtr /*transaction*/,
    const std::vector<TTableKey>& tableKeys)
{
    for (const auto& tableKey : tableKeys) {
        Storage_.erase(TStorageKey{
            .PartitionId = tableKey.PartitionId,
            .MessageId = tableKey.MessageId,
        });
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
