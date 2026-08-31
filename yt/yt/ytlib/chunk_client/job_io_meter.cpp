#include "job_io_meter.h"

namespace NYT::NChunkClient {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

TJobIoMeter::TJobIoMeter(TDuration maxHistoryDuration, bool enabled)
    : MaxHistoryDuration_(maxHistoryDuration)
    , Enabled_(enabled)
{ }

bool TJobIoMeter::IsEnabled() const
{
    return Enabled_;
}

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
    if (!Enabled_) {
        return;
    }

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

void TJobIoMeter::SetIoFairShareWeight(double weight)
{
    IoFairShareWeight_.Store(weight);
}

std::optional<double> TJobIoMeter::GetIoFairShareWeight() const
{
    return IoFairShareWeight_.Load();
}

////////////////////////////////////////////////////////////////////////////////

std::optional<double> GetEffectiveIoFairShareWeight(
    std::optional<double> configuredWeight,
    const TJobIoMeterPtr& jobIoMeter)
{
    if (jobIoMeter && !jobIoMeter->IsEnabled()) {
        return std::nullopt;
    }

    if (configuredWeight) {
        return configuredWeight;
    }

    return jobIoMeter
        ? jobIoMeter->GetIoFairShareWeight()
        : std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
