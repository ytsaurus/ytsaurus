#pragma once

#include <yt/yt/core/yson/string.h>

#include <util/datetime/base.h>

#include <deque>
#include <optional>
#include <utility>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Retains peaks of measured steady demand across controller restarts.
class TSteadyDemandHistory
{
public:
    explicit TSteadyDemandHistory(NYson::TYsonString state = {});
    TSteadyDemandHistory(const TSteadyDemandHistory&) = delete;
    TSteadyDemandHistory& operator=(const TSteadyDemandHistory&) = delete;
    TSteadyDemandHistory(TSteadyDemandHistory&&) = default;
    TSteadyDemandHistory& operator=(TSteadyDemandHistory&&) = default;

    void Observe(TInstant measuredThrough, double demand, TDuration period);
    double GetPeak(TInstant now, TDuration window);
    const NYson::TYsonString& Save() const;

private:
    std::optional<TInstant> LastObservationTime_;
    std::deque<std::pair<TInstant, double>> Peaks_;
    mutable NYson::TYsonString Serialized_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
