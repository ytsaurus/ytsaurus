#include "estimator.h"

#include <algorithm>
#include <cmath>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

double SecondsBetween(TInstant lhs, TInstant rhs)
{
    return (static_cast<double>(lhs.MicroSeconds()) - rhs.MicroSeconds()) / 1e6;
}

bool IsValid(const TSourceWork& value)
{
    return std::isfinite(value.Count) && value.Count >= 0 &&
        std::isfinite(value.Bytes) && value.Bytes >= 0;
}

TSourceWork Mix(const TSourceWork& lhs, const TSourceWork& rhs, double weight)
{
    return {
        .Count = std::lerp(lhs.Count, rhs.Count, weight),
        .Bytes = std::lerp(lhs.Bytes, rhs.Bytes, weight),
    };
}

TSourceWork Scale(const TSourceWork& value, double scale)
{
    return {.Count = value.Count * scale, .Bytes = value.Bytes * scale};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

void TSourceReadDelayEstimatorSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("smoothing_period", &TThis::SmoothingPeriod)
        .Default(TDuration::Minutes(1))
        .GreaterThan(TDuration::Zero());
    registrar.Parameter("max_observation_gap", &TThis::MaxObservationGap)
        .Default(TDuration::Minutes(5))
        .GreaterThan(TDuration::Zero());
    registrar.Parameter("max_density_age", &TThis::MaxDensityAge)
        .Default(TDuration::Minutes(5))
        .GreaterThan(TDuration::Zero());
    registrar.Parameter("max_density_ratio", &TThis::MaxDensityRatio)
        .Default(4.0)
        .GreaterThan(1.0);
    registrar.Parameter("max_event_offset_deviation", &TThis::MaxEventOffsetDeviation)
        .Default(TDuration::Minutes(1))
        .GreaterThan(TDuration::Zero());
    registrar.Parameter("warning_retention", &TThis::WarningRetention)
        .Default(TDuration::Minutes(5))
        .GreaterThan(TDuration::Zero());
}

void TSourceReadDelayEstimator::Register(TRegistrar registrar)
{
    registrar.Parameter("last_message_id", &TThis::LastMessageId_)
        .Default();
    registrar.Parameter("read_system_time", &TThis::ReadSystemTime_)
        .Default();
    registrar.Parameter("pending_count", &TThis::PendingCount_)
        .Default(0.0);
    registrar.Parameter("pending_bytes", &TThis::PendingBytes_)
        .Default(0.0);
    registrar.Parameter("density_count", &TThis::DensityCount_)
        .Default(0.0);
    registrar.Parameter("density_bytes", &TThis::DensityBytes_)
        .Default(0.0);
    registrar.Parameter("last_density_observation_time", &TThis::LastDensityObservationTime_)
        .Default();
    registrar.Parameter("offset_seconds", &TThis::OffsetSeconds_)
        .Default();
    registrar.Parameter("last_offset_deviation_time", &TThis::LastOffsetDeviationTime_)
        .Default();
    registrar.Parameter("last_offset_deviation_seconds", &TThis::LastOffsetDeviationSeconds_)
        .Default(0.0);
    registrar.Parameter("previous_observation_time", &TThis::PreviousObservationTime_)
        .Default();
    registrar.Parameter("previous_threshold", &TThis::PreviousThreshold_)
        .Default(TInstant::Zero());
    registrar.Parameter("new_count_rate", &TThis::NewCountRate_)
        .Default();
    registrar.Parameter("new_bytes_rate", &TThis::NewBytesRate_)
        .Default();
    registrar.Parameter("threshold_speed", &TThis::ThresholdSpeed_)
        .Default();
}

void TSourceReadDelayEstimator::Reset()
{
    *this = TSourceReadDelayEstimator();
}

void TSourceReadDelayEstimator::RegisterInput(
    std::string_view messageId,
    TInstant systemTime,
    std::optional<TInstant> eventTime,
    TSourceWork work,
    TInstant observationTime,
    const TSourceReadDelayEstimatorSpecPtr& config)
{
    if (messageId.empty() || !IsValid(work) || systemTime > observationTime) {
        return;
    }
    CurrentReadSystemTime_ = std::max(CurrentReadSystemTime_.value_or(systemTime), systemTime);
    if (messageId <= LastMessageId_) {
        return;
    }
    LastMessageId_ = messageId;

    if (ReadSystemTime_ && systemTime > *ReadSystemTime_) {
        const double elapsed = SecondsBetween(systemTime, *ReadSystemTime_);
        const double weight = LastDensityObservationTime_
            ? -std::expm1(-elapsed / config->SmoothingPeriod.SecondsFloat())
            : 1.0;
        // The completed timestamp group belongs to the interval preceding the new group.
        DensityCount_ = std::lerp(DensityCount_, PendingCount_ / elapsed, weight);
        DensityBytes_ = std::lerp(DensityBytes_, PendingBytes_ / elapsed, weight);
        LastDensityObservationTime_ = observationTime;
        PendingCount_ = 0;
        PendingBytes_ = 0;
    }
    ReadSystemTime_ = std::max(ReadSystemTime_.value_or(systemTime), systemTime);
    PendingCount_ += work.Count;
    PendingBytes_ += work.Bytes;
    if (eventTime) {
        const double offset = SecondsBetween(systemTime, *eventTime);
        if (OffsetSeconds_) {
            const double deviation = std::abs(offset - *OffsetSeconds_);
            if (deviation > config->MaxEventOffsetDeviation.SecondsFloat()) {
                LastOffsetDeviationTime_ = observationTime;
                LastOffsetDeviationSeconds_ = deviation;
            }
        }
        const double weight = OffsetSeconds_ && DensityCount_ > 0
            ? -std::expm1(-work.Count / (DensityCount_ * config->SmoothingPeriod.SecondsFloat()))
            : 1.0;
        OffsetSeconds_ = std::lerp(OffsetSeconds_.value_or(offset), offset, weight);
    }
}

TSourceReadDelayEstimate TSourceReadDelayEstimator::Estimate(
    const TSourceReadDelayObservation& observation,
    const TSourceReadDelayEstimatorSpecPtr& config)
{
    TSourceReadDelayEstimate result;
    if (observation.Unread && IsValid(*observation.Unread) &&
        observation.Unread->Count == 0 && observation.Unread->Bytes == 0)
    {
        result.Ready = TSourceWork{};
    }

    const auto now = observation.ObservationTime;
    const auto threshold = observation.ReadDelayEventThreshold;
    const bool validNew = observation.NewRate && IsValid(*observation.NewRate);
    const double elapsed = PreviousObservationTime_ ? SecondsBetween(now, *PreviousObservationTime_) : 0;
    const bool validInterval = elapsed > 0 && elapsed <= config->MaxObservationGap.SecondsFloat();
    if (!PreviousObservationTime_ || elapsed > 0) {
        if (validNew) {
            const double weight = validInterval && NewCountRate_ && NewBytesRate_
                ? -std::expm1(-elapsed / config->SmoothingPeriod.SecondsFloat())
                : 1.0;
            NewCountRate_ = std::lerp(NewCountRate_.value_or(observation.NewRate->Count), observation.NewRate->Count, weight);
            NewBytesRate_ = std::lerp(NewBytesRate_.value_or(observation.NewRate->Bytes), observation.NewRate->Bytes, weight);
        }
        if (validInterval) {
            const double speed = std::max(0.0, SecondsBetween(threshold, PreviousThreshold_) / elapsed);
            const double weight = ThresholdSpeed_
                ? -std::expm1(-elapsed / config->SmoothingPeriod.SecondsFloat())
                : 1.0;
            ThresholdSpeed_ = std::lerp(ThresholdSpeed_.value_or(speed), speed, weight);
        } else {
            ThresholdSpeed_.reset();
        }
        PreviousObservationTime_ = now;
        PreviousThreshold_ = threshold;
    }
    if (!OffsetSeconds_) {
        return result;
    }
    const double remainingDelay = SecondsBetween(now, threshold) - *OffsetSeconds_;
    result.SystemEventOffsetSeconds = *OffsetSeconds_;
    result.ThresholdSpeed = ThresholdSpeed_.value_or(0);
    result.HistoricalDensity = {DensityCount_, DensityBytes_};
    if (LastOffsetDeviationTime_ && now >= *LastOffsetDeviationTime_ &&
        now - *LastOffsetDeviationTime_ <= config->WarningRetention &&
        LastOffsetDeviationSeconds_ > config->MaxEventOffsetDeviation.SecondsFloat())
    {
        result.EventOffsetDeviationSeconds = LastOffsetDeviationSeconds_;
    }
    if (remainingDelay > 0 && validNew && NewCountRate_ && NewBytesRate_ && LastDensityObservationTime_ &&
        now >= *LastDensityObservationTime_ && now - *LastDensityObservationTime_ <= config->MaxDensityAge)
    {
        const auto disagree = [&] (double historical, double current) {
            return std::max(historical, current) > config->MaxDensityRatio * std::min(historical, current);
        };
        result.CountDensityMismatch = disagree(DensityCount_, *NewCountRate_);
        result.BytesDensityMismatch = disagree(DensityBytes_, *NewBytesRate_);
    }

    if (observation.Unread && IsValid(*observation.Unread) && CurrentReadSystemTime_) {
        const double unreadSpan = SecondsBetween(now, *CurrentReadSystemTime_);
        const double fraction = remainingDelay <= 0
            ? 1.0
            : unreadSpan > 0 ? std::clamp((unreadSpan - remainingDelay) / unreadSpan, 0.0, 1.0)
                             : 0.0;
        result.Ready = Scale(*observation.Unread, fraction);
    }
    if (!validNew || elapsed < 0 || !ThresholdSpeed_ || !NewCountRate_ || !NewBytesRate_) {
        return result;
    }
    const TSourceWork newRate{*NewCountRate_, *NewBytesRate_};
    if (remainingDelay <= 0) {
        result.Offered = newRate;
        result.NewRateWeight = 1;
        return result;
    }
    if (!ReadSystemTime_ || !LastDensityObservationTime_ ||
        SecondsBetween(now, *LastDensityObservationTime_) < 0 ||
        now - *LastDensityObservationTime_ > config->MaxDensityAge)
    {
        return result;
    }
    const double sourceLag = SecondsBetween(now, *ReadSystemTime_);
    const double excessLag = sourceLag - remainingDelay;
    result.NewRateWeight = sourceLag > 0 ? std::clamp(excessLag / sourceLag, 0.0, 1.0) : 0.0;
    result.Offered = Scale(Mix(result.HistoricalDensity, newRate, result.NewRateWeight), *ThresholdSpeed_);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
