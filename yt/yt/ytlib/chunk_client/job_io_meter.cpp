#include "job_io_meter.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

TJobIoMeter::TJobIoMeter(TDuration window, bool enabled)
    // A bucket expires when its end reaches the beginning of the window.
    : BucketLifetime_(window + TDuration::Seconds(1))
    , Enabled_(enabled)
{ }

bool TJobIoMeter::IsEnabled() const
{
    return Enabled_;
}

void TJobIoMeter::Account(i64 bytes)
{
    if (!Enabled_ || bytes <= 0) {
        return;
    }

    auto guard = Guard(Lock_);
    auto now = TClock::now();
    ExpireBuckets(now);
    auto bucketStart = std::chrono::floor<std::chrono::seconds>(now);
    if (!History_.empty() && History_.back().Start == bucketStart) {
        History_.back().Bytes += bytes;
    } else {
        History_.push_back({bucketStart, bytes});
    }
    BytesInWindow_ += bytes;
}

i64 TJobIoMeter::GetIoConsumedInWindow()
{
    auto guard = Guard(Lock_);
    ExpireBuckets(TClock::now());
    return BytesInWindow_;
}

void TJobIoMeter::ExpireBuckets(TClock::time_point now)
{
    while (!History_.empty()) {
        auto age = std::chrono::duration_cast<std::chrono::microseconds>(now - History_.front().Start);
        if (TDuration::MicroSeconds(age.count()) < BucketLifetime_) {
            break;
        }
        BytesInWindow_ -= History_.front().Bytes;
        History_.pop_front();
    }
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
