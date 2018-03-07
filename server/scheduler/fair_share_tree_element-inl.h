#pragma once
#ifndef FAIR_SHARE_TREE_ELEMENT_INL_H_
#error "Direct inclusion of this file is not allowed, include fair_share_tree_element.h"
#endif

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

inline bool TSchedulerElementSharedState::GetAlive() const
{
    return Alive_.load(std::memory_order_relaxed);
}

inline void TSchedulerElementSharedState::SetAlive(bool alive)
{
    Alive_ = alive;
}

inline double TSchedulerElementSharedState::GetFairShareRatio() const
{
    return FairShareRatio_.load(std::memory_order_relaxed);
}

inline void TSchedulerElementSharedState::SetFairShareRatio(double fairShareRatio)
{
    FairShareRatio_ = fairShareRatio;
}

////////////////////////////////////////////////////////////////////////////////

inline int TSchedulerElement::GetTreeIndex() const
{
    return TreeIndex_;
}

inline bool TSchedulerElement::IsAlive() const
{
    return SharedState_->GetAlive();
}

inline void TSchedulerElement::SetAlive(bool alive)
{
    SharedState_->SetAlive(alive);
}

inline void TSchedulerElement::SetFairShareRatio(double fairShareRatio)
{
    // This version is global and  used to balance preemption lists.
    SharedState_->SetFairShareRatio(fairShareRatio);
    // This version is local for tree and used to compute satisfaction ratios.
    Attributes_.FairShareRatio = fairShareRatio;
}

inline double TSchedulerElement::GetFairShareRatio() const
{
    return SharedState_->GetFairShareRatio();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
