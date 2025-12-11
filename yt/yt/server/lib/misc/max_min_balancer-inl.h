#ifndef MAX_MIN_BALANCER_INL_H_
#error "Direct inclusion of this file is not allowed, include max_min_balancer.h"
// For the sake of sane code completion.
#include "max_min_balancer.h"
#endif

#include <algorithm>
#include <ranges>

namespace NYT::NServer {

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
    YT_ASSERT(TryAddContender(contender, initialWeight));
}

template <typename T, typename W>
bool TDecayingMaxMinBalancer<T, W>::TryAddContender(T contender, W initialWeight)
{
    return ContenderToWeight_.emplace(contender, initialWeight).second;
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
    auto contenders = ContenderToWeight_
        | std::views::filter([&] (const auto& contender) {
            return pred(contender.first);
        });

    auto resultIt = std::ranges::min_element(
        contenders,
        [] (const auto& lhs, const auto& rhs) {
            return lhs.second < rhs.second;
        });

    return resultIt == std::ranges::end(contenders)
        ? std::nullopt
        : std::make_optional(resultIt->first);
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

} // namespace NYT::NServer
