#ifndef MAX_MIN_BALANCER_INL_H_
#error "Direct inclusion of this file is not allowed, include max_min_balancer.h"
// For the sake of sane code completion.
#include "max_min_balancer.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <typename T, typename W>
TDecayingMaxMinBalancer<T, W>::TDecayingMaxMinBalancer(
    W decayFactor,
    TDuration decayInterval)
    : DecayFactor_(decayFactor)
    , DecayInterval_(NProfiling::DurationToCpuDuration(decayInterval))
    , NextDecayInstant_(NProfiling::GetCpuInstant() + DecayInterval_)
{ }

template <typename T, typename W>
void TDecayingMaxMinBalancer<T, W>::AddContender(T contender, W initialWeight)
{
    YT_ASSERT(ContenderToWeight_.emplace(contender, initialWeight).second);
}

template <typename T, typename W>
std::optional<T> TDecayingMaxMinBalancer<T, W>::ChooseWinner()
{
    if (ContenderToWeight_.empty()) {
        return std::nullopt;
    } else {
        auto it = std::min_element(
            ContenderToWeight_.begin(),
            ContenderToWeight_.end(),
            [] (const auto& lhs, const auto& rhs) { return lhs.second < rhs.second; });
        return it->first;
    }
}

template <typename T, typename W>
template <typename P>
std::optional<T> TDecayingMaxMinBalancer<T, W>::ChooseWinnerIf(P pred)
{
    auto resultIt = std::find_if(
        ContenderToWeight_.begin(),
        ContenderToWeight_.end(),
        [&] (const auto& w) { return pred(w.first); });

    for (auto it = resultIt; it != ContenderToWeight_.end(); ++it) {
        if (pred(it->first) && it->second < resultIt->second) {
            resultIt = it;
        }
    }

    if (resultIt == ContenderToWeight_.end()) {
        return std::nullopt;
    } else {
        return resultIt->first;
    }
}

template <typename T, typename W>
template <typename C>
std::optional<T> TDecayingMaxMinBalancer<T, W>::ChooseRangeWinner(C subset, W defaultWeight)
{
    std::optional<T> result;
    std::optional<W> resultWeight;
    for (auto contender : subset) {
        auto it = ContenderToWeight_.find(contender);
        auto contenderWeight = it == ContenderToWeight_.end() ? defaultWeight : it->second;

        if (!result || contenderWeight < resultWeight) {
            result = contender;
            resultWeight = contenderWeight;
        }
    }

    return result;
}

template <typename T, typename W>
void TDecayingMaxMinBalancer<T, W>::AddWeight(T winner, W extraWeight)
{
    MaybeDecay();

    auto it = ContenderToWeight_.find(winner);
    YT_VERIFY(it != ContenderToWeight_.end());

    it->second += extraWeight;
}

template <typename T, typename W>
void TDecayingMaxMinBalancer<T, W>::AddWeightWithDefault(T winner, W extraWeight, W defaultWeight)
{
    MaybeDecay();

    auto it = ContenderToWeight_.find(winner);
    if (it == ContenderToWeight_.end()) {
        it = ContenderToWeight_.emplace(winner, defaultWeight).first;
    }
    YT_VERIFY(it != ContenderToWeight_.end());

    it->second += extraWeight;
}

template <typename T, typename W>
void TDecayingMaxMinBalancer<T, W>::ResetWeights()
{
    for (auto& contender : ContenderToWeight_) {
        contender.second = W();
    }
}

template <typename T, typename W>
void TDecayingMaxMinBalancer<T, W>::MaybeDecay()
{
    for (auto now = NProfiling::GetCpuInstant(); NextDecayInstant_ <= now; NextDecayInstant_ += DecayInterval_) {
        for (auto& contender : ContenderToWeight_) {
            contender.second *= DecayFactor_;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
