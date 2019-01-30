#pragma once
#ifndef MAX_MIN_BALANCER_INL_H_
#error "Direct inclusion of this file is not allowed, include max_min_balancer.h"
// For the sake of sane code completion.
#include "max_min_balancer.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <typename T, typename W>
TDecayingMaxMinBalancer<T, W>::TWeighedContender::TWeighedContender(T contender, W weight)
    : Contender(contender)
    , Weight(weight)
{ }

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
    Y_ASSERT(FindContender(contender) == Contenders_.end());
    Contenders_.emplace_back(contender, initialWeight);
}

template <typename T, typename W>
std::optional<T> TDecayingMaxMinBalancer<T, W>::TakeWinner()
{
    if (Contenders_.empty()) {
        return std::nullopt;
    } else {
        auto it = std::min_element(Contenders_.begin(), Contenders_.end());
        return it->Contender;
    }
}

template <typename T, typename W>
template <typename P>
std::optional<T> TDecayingMaxMinBalancer<T, W>::TakeWinnerIf(P pred)
{
    auto resultIt = std::find_if(
        Contenders_.begin(),
        Contenders_.end(),
        [&] (const TWeighedContender& w) { return pred(w.Contender); });

    for (auto it = resultIt; it != Contenders_.end(); ++it) {
        if (pred(it->Contender) && it->Weight < resultIt->Weight) {
            resultIt = it;
        }
    }

    if (resultIt == Contenders_.end()) {
        return std::nullopt;
    } else {
        return resultIt->Contender;
    }
}

template <typename T, typename W>
void TDecayingMaxMinBalancer<T, W>::AddWeight(T winner, W extraWeight)
{
    MaybeDecay();

    auto it = FindContender(winner);
    YCHECK(it != Contenders_.end());

    it->Weight += extraWeight;
}

template <typename T, typename W>
void TDecayingMaxMinBalancer<T, W>::ResetWeights()
{
    for (auto& contender : Contenders_) {
        contender.Weight = W();
    }
}

template <typename T, typename W>
void TDecayingMaxMinBalancer<T, W>::MaybeDecay()
{
    for (auto now = NProfiling::GetCpuInstant(); NextDecayInstant_ <= now; NextDecayInstant_ += DecayInterval_) {
        for (auto& contender : Contenders_) {
            contender.Weight *= DecayFactor_;
        }
    }
}

template <typename T, typename W>
typename std::vector<typename TDecayingMaxMinBalancer<T, W>::TWeighedContender>::iterator
TDecayingMaxMinBalancer<T, W>::FindContender(T contender)
{
    return std::find_if(
        Contenders_.begin(),
        Contenders_.end(),
        [&] (const TWeighedContender& w) { return w.Contender == contender; });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
