#include "input_messages.h"

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

bool TInMemoryInputMessages::TStorageKey::operator<(const TStorageKey& other) const
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

TFuture<std::vector<bool>> TInMemoryInputMessages::Contains(
    const TComputationId& computationId,
    const std::vector<TMessage>& messages)
{
    std::vector<bool> result;
    result.reserve(messages.size());
    for (const auto& message : messages) {
        result.push_back(Storage_.contains(TStorageKey{
            .ComputationId = computationId,
            .Key = message.Key,
            .MessageId = message.MessageId,
        }));
    }
    return MakeFuture(std::move(result));
}

void TInMemoryInputMessages::Write(
    NApi::IDynamicTableTransactionPtr /*transaction*/,
    const TComputationId& computationId,
    const std::vector<TMessage>& messages)
{
    for (const auto& message : messages) {
        Storage_[TStorageKey{
            .ComputationId = computationId,
            .Key = message.Key,
            .MessageId = message.MessageId,
        }] = message;
        ++WriteCount_;
    }
}

bool TInMemoryInputMessages::ContainsDirect(
    const TComputationId& computationId,
    const TKey& key,
    const TMessageId& messageId) const
{
    return Storage_.contains(TStorageKey{
        .ComputationId = computationId,
        .Key = key,
        .MessageId = messageId,
    });
}

i64 TInMemoryInputMessages::GetWriteCount() const
{
    return WriteCount_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
