#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/tables/input_messages.h>

#include <map>

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

// In-memory implementation of IInputMessages for unit testing.
// Uses std::map (ordered) to emulate YT dynamic table key ordering.
// Transactions are ignored — Write() is applied immediately.
// Storage key matches the real YT table: (computation_id, key, message_id).
class TInMemoryInputMessages
    : public IInputMessages
{
public:
    TInMemoryInputMessages() = default;

    TFuture<std::vector<bool>> Contains(
        const TComputationId& computationId,
        const std::vector<TMessage>& messages) override;

    void Write(
        NApi::IDynamicTableTransactionPtr transaction,
        const TComputationId& computationId,
        const std::vector<TMessage>& messages) override;

    // Test helper: direct lookup without going through the async Contains() path.
    bool ContainsDirect(
        const TComputationId& computationId,
        const TKey& key,
        const TMessageId& messageId) const;

    // Test helper: total number of individual message writes across all Write() calls.
    i64 GetWriteCount() const;

private:
    // Matches the YT table key: (computation_id, key, message_id).
    struct TStorageKey
    {
        TComputationId ComputationId;
        TKey Key;
        TMessageId MessageId;

        bool operator<(const TStorageKey& other) const;
    };

    std::map<TStorageKey, TMessage> Storage_;
    i64 WriteCount_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TInMemoryInputMessages);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
