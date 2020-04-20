#include "historic_usage_aggregator.h"
#include "assert.h"

#include <util/generic/ymath.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

THistoricUsageAggregationParameters::THistoricUsageAggregationParameters(
    EHistoricUsageAggregationMode mode,
    double emaAlpha)
    : Mode(mode)
    , EmaAlpha(emaAlpha)
{ }

THistoricUsageAggregationParameters::THistoricUsageAggregationParameters(
    const THistoricUsageConfigPtr& config)
    : Mode(config->AggregationMode)
    , EmaAlpha(config->EmaAlpha)
{ }

bool THistoricUsageAggregationParameters::operator==(const THistoricUsageAggregationParameters& other)
{
    return Mode == other.Mode && EmaAlpha == other.EmaAlpha;
}

////////////////////////////////////////////////////////////////////////////////

THistoricUsageAggregator::THistoricUsageAggregator()
{
    Reset();
}

void THistoricUsageAggregator::UpdateParameters(
    const THistoricUsageAggregationParameters& newParameters)
{
    if (Parameters_ == newParameters) {
        return;
    }

    Parameters_ = newParameters;
    Reset();
}

void THistoricUsageAggregator::Reset()
{
    ExponentialMovingAverage_ = 0.0;
    LastExponentialMovingAverageUpdateTime_ = TInstant::Zero();
}

void THistoricUsageAggregator::UpdateAt(TInstant now, double value)
{
    YT_VERIFY(now >= LastExponentialMovingAverageUpdateTime_);

    // If LastExponentialMovingAverageUpdateTime_ is zero, this is the first update (after most
    // recent reset) and we just want to leave EMA = 0.0, as if there was no previous usage.
    if (Parameters_.Mode == EHistoricUsageAggregationMode::ExponentialMovingAverage &&
        LastExponentialMovingAverageUpdateTime_ != TInstant::Zero())
    {
        auto sinceLast = now - LastExponentialMovingAverageUpdateTime_;
        auto w = Exp2(-1. * Parameters_.EmaAlpha * sinceLast.SecondsFloat());
        ExponentialMovingAverage_ = w * ExponentialMovingAverage_ + (1 - w) * value;
    }

    LastExponentialMovingAverageUpdateTime_ = now;
}

double THistoricUsageAggregator::GetHistoricUsage() const
{
    return ExponentialMovingAverage_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
