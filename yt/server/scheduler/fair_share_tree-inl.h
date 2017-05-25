#pragma once
#ifndef FAIR_SHARE_TREE_INL_H_
#error "Direct inclusion of this file is not allowed, include fair_share_tree.h"
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
