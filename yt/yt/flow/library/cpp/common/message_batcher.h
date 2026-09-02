#pragma once

#include "public.h"

#include "key.h"
#include "message.h"
#include "timer.h"

#include <yt/yt/core/concurrency/nonblocking_batcher.h>

#include <library/cpp/containers/absl/flat_hash_set.h>

#include <deque>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TMessageBatchLimiter
{
public:
    explicit TMessageBatchLimiter(
        i64 maxRowsPerBatch,
        i64 maxBytesPerBatch,
        std::optional<i64> maxKeysPerBatch = std::nullopt);

    bool IsFull() const;

    // Add one message/timer with given size.
    void Add(i64 messageSize);

    void Add(const TMessage& message);
    void Add(const TInputMessageConstPtr& message);
    void Add(const TInputTimerConstPtr& timer);

    i64 GetMaxRowsPerBatch() const;

private:
    i64 CurrentRowsCount_ = 0;
    i64 CurrentByteSize_ = 0;
    i64 MaxRowsPerBatch_;
    i64 MaxBytesPerBatch_;
    std::optional<i64> MaxKeysPerBatch_;
    absl::flat_hash_set<TKey, ::THash<TKey>, ::TEqualTo<TKey>> Keys_;
};

////////////////////////////////////////////////////////////////////////////////

class TMessageBatcher
    : public NConcurrency::TNonblockingBatcher<TMessage, TMessageBatchLimiter>
{
public:
    explicit TMessageBatcher(const TMessageBatcherSettingsPtr& settings);
    void UpdateSettings(const TMessageBatcherSettingsPtr& settings);
};

DEFINE_REFCOUNTED_TYPE(TMessageBatcher);

//////////////////////////////////////////////////////////////////////////////

template <class TSet, class TPriority, class TMessageAccessor, class TOutputCallback>
void MergingExtractBatch(
    std::vector<std::pair<TSet*, std::function<TPriority()>>> inputQueues,
    TMessageAccessor&& messageAccessor,
    TMessageBatchLimiter& batchLimiter, // Gets messageAccessor(queueElement).
    TOutputCallback&& outputCallback    // Gets queueElement.
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define MESSAGE_BATCHER_INL_H_
#include "message_batcher-inl.h"
#undef MESSAGE_BATCHER_INL_H_
