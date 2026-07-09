#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/timer.h>
#include <yt/yt/flow/library/cpp/tables/timers.h>

#include <map>

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

// In-memory implementation of ITimers for unit testing.
// Transactions are ignored — Write() is applied immediately, Erase() removes immediately.
class TInMemoryTimers
    : public ITimers
{
public:
    TInMemoryTimers() = default;

    void Reconfigure(TDynamicTableRequestSpecPtr /*dynamicSpec*/) override
    { }

    TFuture<TLoadResult> Load(
        TFilter filter,
        i64 limit,
        std::optional<TTableKey> offsetExclusive = std::nullopt) override;

    TFuture<std::vector<std::pair<TTableKey, TTimer>>> LoadAll(
        TFilter filter) override;

    void Write(
        NApi::IDynamicTableTransactionPtr transaction,
        const TComputationId& computationId,
        const std::vector<TTimer>& timers) override;

    void Erase(
        NApi::IDynamicTableTransactionPtr transaction,
        const std::vector<TTableKey>& tableKeys) override;

    // Test helper: total number of individual timer writes across all Write() calls.
    i64 GetWriteCount() const;

private:
    struct TStorageKey
    {
        TComputationId ComputationId;
        TKey Key;
        TMessageId MessageId;

        bool operator<(const TStorageKey& other) const;
    };

    std::map<TStorageKey, TTimer> Storage_;
    i64 WriteCount_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TInMemoryTimers);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
