#include "message_batcher.h"

#include "spec.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TMessageBatchLimiter::TMessageBatchLimiter(
    i64 maxRowsPerBatch,
    i64 maxBytesPerBatch,
    std::optional<i64> maxKeysPerBatch)
    : MaxRowsPerBatch_(maxRowsPerBatch)
    , MaxBytesPerBatch_(maxBytesPerBatch)
    , MaxKeysPerBatch_(maxKeysPerBatch)
{ }

bool TMessageBatchLimiter::IsFull() const
{
    return CurrentRowsCount_ >= MaxRowsPerBatch_ ||
        CurrentByteSize_ >= MaxBytesPerBatch_ ||
        (MaxKeysPerBatch_ && std::ssize(Keys_) >= *MaxKeysPerBatch_);
}

void TMessageBatchLimiter::Add(i64 messageSize)
{
    CurrentRowsCount_ += 1;
    CurrentByteSize_ += messageSize;
}

void TMessageBatchLimiter::Add(const TMessage& message)
{
    CurrentRowsCount_ += 1;
    CurrentByteSize_ += GetMessageByteSize(message);
}

void TMessageBatchLimiter::Add(const TInputMessageConstPtr& message)
{
    Add(message->ByteSize);
    if (MaxKeysPerBatch_) {
        Keys_.insert(message->Key);
    }
}

void TMessageBatchLimiter::Add(const TInputTimerConstPtr& timer)
{
    Add(timer->ByteSize);
}

i64 TMessageBatchLimiter::GetMaxRowsPerBatch() const
{
    return MaxRowsPerBatch_;
}

////////////////////////////////////////////////////////////////////////////////

TMessageBatcher::TMessageBatcher(const TMessageBatcherSettingsPtr& settings)
    : NConcurrency::TNonblockingBatcher<TMessage, TMessageBatchLimiter>(
        TMessageBatchLimiter(settings->MaxRowsPerBatch, settings->MaxBytesPerBatch), settings->BatchDuration, true)
{ }

void TMessageBatcher::UpdateSettings(const TMessageBatcherSettingsPtr& settings)
{
    NConcurrency::TNonblockingBatcher<TMessage, TMessageBatchLimiter>::UpdateSettings(
        settings->BatchDuration,
        TMessageBatchLimiter(settings->MaxRowsPerBatch, settings->MaxBytesPerBatch),
        true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
