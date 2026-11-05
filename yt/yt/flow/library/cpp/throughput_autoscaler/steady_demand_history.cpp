#include "steady_demand_history.h"

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <cmath>

namespace NYT::NFlow {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TSteadyDemandHistory::TSteadyDemandHistory(NYson::TYsonString state)
    : Serialized_(std::move(state))
{
    if (Serialized_) {
        const auto node = ConvertTo<IMapNodePtr>(Serialized_);
        LastObservationTime_ = ConvertTo<std::optional<TInstant>>(node->GetChildOrThrow("last_observation_time"));
        Peaks_ = ConvertTo<decltype(Peaks_)>(node->GetChildOrThrow("peaks"));
    }
}

void TSteadyDemandHistory::Observe(TInstant measuredThrough, double demand, TDuration period)
{
    YT_VERIFY(std::isfinite(demand) && demand >= 0);
    if (LastObservationTime_ &&
        (measuredThrough <= *LastObservationTime_ || measuredThrough - *LastObservationTime_ < period))
    {
        return;
    }
    LastObservationTime_ = measuredThrough;
    while (!Peaks_.empty() && Peaks_.back().second <= demand) {
        Peaks_.pop_back();
    }
    Peaks_.push_back({measuredThrough, demand});
    Serialized_ = {};
}

double TSteadyDemandHistory::GetPeak(TInstant now, TDuration window)
{
    while (!Peaks_.empty() && Peaks_.front().first <= now && now - Peaks_.front().first >= window) {
        Peaks_.pop_front();
        Serialized_ = {};
    }
    return Peaks_.empty() ? 0 : Peaks_.front().second;
}

const NYson::TYsonString& TSteadyDemandHistory::Save() const
{
    if (!Serialized_) {
        Serialized_ = BuildYsonStringFluently()
            .BeginMap()
            .Item("last_observation_time")
            .Value(LastObservationTime_)
            .Item("peaks")
            .Value(Peaks_)
            .EndMap();
    }
    return Serialized_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
