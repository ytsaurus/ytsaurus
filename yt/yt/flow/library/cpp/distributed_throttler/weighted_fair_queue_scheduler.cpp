#include "weighted_fair_queue_scheduler.h"

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/error.h>

#include <algorithm>
#include <tuple>

namespace NYT::NFlow::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

TWeightedFairQueueScheduler::TWeightedFairQueueScheduler(
    const THashMap<TQuotaClassId, double>& classWeights,
    double renormalizationThreshold)
    : RenormalizationThreshold_(renormalizationThreshold)
{
    Classes_.emplace(DefaultQuotaClassId, TClassState{});
    Reconfigure(classWeights);
}

void TWeightedFairQueueScheduler::Reconfigure(
    const THashMap<TQuotaClassId, double>& classWeights)
{
    for (auto& [classId, state] : Classes_) {
        if (classId != DefaultQuotaClassId) {
            state.Accepting = false;
        }
    }

    for (const auto& [classId, weight] : classWeights) {
        auto [it, inserted] = Classes_.try_emplace(classId);
        auto& state = it->second;
        state.Weight = weight;
        state.Accepting = true;
        if (inserted) {
            state.VirtualTime = SystemVirtualTime_;
        }
    }

    auto& defaultState = Classes_.at(DefaultQuotaClassId);
    defaultState.Weight = 1.0;
    defaultState.Accepting = true;
}

bool TWeightedFairQueueScheduler::IsAccepting(const TQuotaClassId& classId) const
{
    auto it = Classes_.find(classId);
    return it != Classes_.end() && it->second.Accepting;
}

bool TWeightedFairQueueScheduler::Contains(const TQuotaClassId& classId) const
{
    return Classes_.contains(classId);
}

bool TWeightedFairQueueScheduler::IsRetired(const TQuotaClassId& classId) const
{
    auto it = Classes_.find(classId);
    return it != Classes_.end() && !it->second.Accepting;
}

double TWeightedFairQueueScheduler::GetWeight(const TQuotaClassId& classId) const
{
    return GetOrCrash(Classes_, classId).Weight;
}

void TWeightedFairQueueScheduler::Activate(
    const TQuotaClassId& classId,
    TPriority headPriority)
{
    auto& state = GetOrCrash(Classes_, classId);
    if (!state.Active) {
        const bool hasOtherActiveClass = std::any_of(
            Classes_.begin(),
            Classes_.end(),
            [&] (const auto& pair) {
                return pair.first != classId && pair.second.Active;
            });
        if (hasOtherActiveClass) {
            state.VirtualTime = std::max(state.VirtualTime, SystemVirtualTime_);
        }
        state.Active = true;
    }
    state.HeadPriority = headPriority;
}

void TWeightedFairQueueScheduler::Deactivate(const TQuotaClassId& classId)
{
    GetOrCrash(Classes_, classId).Active = false;
    MaybeReset();
}

void TWeightedFairQueueScheduler::UpdateHeadPriority(
    const TQuotaClassId& classId,
    TPriority headPriority)
{
    auto& state = GetOrCrash(Classes_, classId);
    YT_VERIFY(state.Active);
    state.HeadPriority = headPriority;
}

std::optional<TQuotaClassId> TWeightedFairQueueScheduler::SelectClass()
{
    auto selected = Classes_.end();
    for (auto it = Classes_.begin(); it != Classes_.end(); ++it) {
        if (!it->second.Active) {
            continue;
        }
        if (selected == Classes_.end() ||
            std::tie(it->second.VirtualTime, it->second.HeadPriority, it->first) <
                std::tie(selected->second.VirtualTime, selected->second.HeadPriority, selected->first))
        {
            selected = it;
        }
    }

    if (selected == Classes_.end()) {
        return std::nullopt;
    }

    SystemVirtualTime_ = selected->second.VirtualTime;
    MaybeRenormalize();
    return selected->first;
}

void TWeightedFairQueueScheduler::Charge(
    const TQuotaClassId& classId,
    i64 amount,
    double weight)
{
    ChargeVirtualTime(classId, static_cast<double>(amount) / weight);
}

void TWeightedFairQueueScheduler::ChargeVirtualTime(
    const TQuotaClassId& classId,
    double delta)
{
    GetOrCrash(Classes_, classId).VirtualTime += delta;
    MaybeRenormalize();
    MaybeReset();
}

void TWeightedFairQueueScheduler::RemoveRetiredClass(const TQuotaClassId& classId)
{
    auto it = Classes_.find(classId);
    YT_VERIFY(it != Classes_.end());
    YT_VERIFY(classId != DefaultQuotaClassId);
    YT_VERIFY(!it->second.Accepting);
    YT_VERIFY(!it->second.Active);
    Classes_.erase(it);
}

void TWeightedFairQueueScheduler::MaybeReset()
{
    if (std::any_of(Classes_.begin(), Classes_.end(), [] (const auto& pair) {
            return pair.second.Active;
        })) {
        return;
    }

    SystemVirtualTime_ = 0.0;
    for (auto& [_, state] : Classes_) {
        state.VirtualTime = 0.0;
    }
}

void TWeightedFairQueueScheduler::MaybeRenormalize()
{
    if (SystemVirtualTime_ <= RenormalizationThreshold_) {
        return;
    }

    const auto shift = SystemVirtualTime_;
    for (auto& [_, state] : Classes_) {
        if (state.Active) {
            state.VirtualTime -= shift;
        } else {
            state.VirtualTime = 0.0;
        }
    }
    SystemVirtualTime_ = 0.0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
