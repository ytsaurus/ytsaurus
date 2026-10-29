#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <util/datetime/base.h>

#include <optional>
#include <string>
#include <string_view>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TSourceWork
{
    double Count = 0;
    double Bytes = 0;
};

struct TSourceReadDelayEstimatorSpec
    : public NYTree::TYsonStruct
{
    TDuration SmoothingPeriod;
    TDuration MaxObservationGap;
    TDuration MaxDensityAge;
    double MaxDensityRatio{};
    TDuration MaxEventOffsetDeviation;
    TDuration WarningRetention;

    REGISTER_YSON_STRUCT(TSourceReadDelayEstimatorSpec);
    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSourceReadDelayEstimatorSpec);

struct TSourceReadDelayObservation
{
    TInstant ObservationTime;
    TInstant ReadDelayEventThreshold;
    std::optional<TSourceWork> NewRate;
    std::optional<TSourceWork> Unread;
};

struct TSourceReadDelayEstimate
{
    std::optional<TSourceWork> Offered;
    //! Estimated eligible unread input, in records and bytes rather than per second.
    std::optional<TSourceWork> Ready;
    TSourceWork HistoricalDensity;
    double NewRateWeight = 0;
    double SystemEventOffsetSeconds = 0;
    double ThresholdSpeed = 0;
    bool CountDensityMismatch = false;
    bool BytesDensityMismatch = false;
    std::optional<double> EventOffsetDeviationSeconds;
};

//! Estimates delayed source demand in original input units with bounded, serializable state.
class TSourceReadDelayEstimator
    : public NYTree::TYsonStructLite
{
public:
    //! Registers ordered original input; repeated message ids do not train the estimator again.
    //! |eventTime| is the publication gate of the corresponding output, absent for discarded input.
    void RegisterInput(
        std::string_view messageId,
        TInstant systemTime,
        std::optional<TInstant> eventTime,
        TSourceWork work,
        TInstant observationTime,
        const TSourceReadDelayEstimatorSpecPtr& config);

    TSourceReadDelayEstimate Estimate(
        const TSourceReadDelayObservation& observation,
        const TSourceReadDelayEstimatorSpecPtr& config);
    void Reset();

    REGISTER_YSON_STRUCT_LITE(TSourceReadDelayEstimator);
    static void Register(TRegistrar registrar);

private:
    std::string LastMessageId_;
    std::optional<TInstant> ReadSystemTime_;
    double PendingCount_ = 0;
    double PendingBytes_ = 0;
    double DensityCount_ = 0;
    double DensityBytes_ = 0;
    std::optional<TInstant> LastDensityObservationTime_;
    std::optional<double> OffsetSeconds_;
    std::optional<TInstant> LastOffsetDeviationTime_;
    double LastOffsetDeviationSeconds_ = 0;
    std::optional<TInstant> PreviousObservationTime_;
    TInstant PreviousThreshold_;
    std::optional<double> NewCountRate_;
    std::optional<double> NewBytesRate_;
    std::optional<double> ThresholdSpeed_;

    // A restored source replays from persistence, independently of the saved learning frontier.
    std::optional<TInstant> CurrentReadSystemTime_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
