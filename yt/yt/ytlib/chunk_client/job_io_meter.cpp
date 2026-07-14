#include "job_io_meter.h"

namespace NYT::NChunkClient {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

TJobIoMeter::TJobIoMeter(TDuration maxHistoryDuration)
    : MaxHistoryDuration_(maxHistoryDuration)
{ }

void TJobIoMeter::AccountRead(i64 size)
{
    Account(size);
}

void TJobIoMeter::AccountWrite(i64 size)
{
    Account(size);
}

void TJobIoMeter::Account(i64 size)
{
    auto now = TInstant::Now();
    auto minute = TInstant::Minutes(now.Minutes());

    auto guard = WriterGuard(Lock_);

    // Accumulate into the current minute's bucket if it is already the newest
    // one, otherwise open a new bucket.
    if (!History_.empty() && History_.back().Minute == minute) {
        History_.back().Size += size;
    } else {
        History_.push_back(TIoBucket{
            .Minute = minute,
            .Size = size,
        });
    }

    // Drop buckets that fell out of the retention window.
    auto historyStart = now - MaxHistoryDuration_;
    while (!History_.empty() && History_.front().Minute < historyStart) {
        History_.pop_front();
    }
}

i64 TJobIoMeter::GetIoConsumedInWindow(TDuration window) const
{
    auto windowStart = TInstant::Now() - window;

    auto guard = ReaderGuard(Lock_);

    // The window is a suffix of the queue, so we walk back from the newest
    // bucket until we leave the window.
    i64 consumed = 0;
    for (auto it = History_.rbegin(); it != History_.rend(); ++it) {
        if (it->Minute < windowStart) {
            break;
        }
        consumed += it->Size;
    }

    return consumed;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
