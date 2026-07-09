#include "message_batcher.h"

#include "spec.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TMessageBatchLimiter::TMessageBatchLimiter(i64 maxRowsPerBatch, i64 maxBytesPerBatch)
    : MaxRowsPerBatch_(maxRowsPerBatch)
    , MaxBytesPerBatch_(maxBytesPerBatch)
{ }

bool TMessageBatchLimiter::IsFull() const
{
    return CurrentRowsCount_ >= MaxRowsPerBatch_ || CurrentByteSize_ >= MaxBytesPerBatch_;
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
    CurrentRowsCount_ += 1;
    CurrentByteSize_ += message->ByteSize;
}

void TMessageBatchLimiter::Add(const TInputTimerConstPtr& timer)
{
    CurrentRowsCount_ += 1;
    CurrentByteSize_ += timer->ByteSize;
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
