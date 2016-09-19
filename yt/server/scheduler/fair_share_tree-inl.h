#pragma once
#ifndef FAIR_SHARE_TREE_INL_H_
#error "Direct inclusion of this file is not allowed, include fair_share_tree.h"
#endif

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

inline bool TSchedulerElementBaseSharedState::GetAlive() const
{
    return Alive_;
}

inline void TSchedulerElementBaseSharedState::SetAlive(bool alive)
{
    Alive_ = alive;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
