#pragma once
#ifndef FAIR_SHARE_TREE_ELEMENT_INL_H_
#error "Direct inclusion of this file is not allowed, include fair_share_tree_element.h"
// For the sake of sane code completion.
#include "fair_share_tree_element.h"
#endif

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

inline int TSchedulerElement::GetTreeIndex() const
{
    return TreeIndex_;
}

inline void TSchedulerElement::SetTreeIndex(int treeIndex)
{
    TreeIndex_ = treeIndex;
}

inline bool TSchedulerElement::IsAlive() const
{
    return ResourceTreeElement_->GetAlive();
}

inline void TSchedulerElement::SetAlive(bool alive)
{
    ResourceTreeElement_->SetAlive(alive);
}

inline void TSchedulerElement::SetFairShareRatio(double fairShareRatio)
{
    // This version is global and used to balance preemption lists.
    ResourceTreeElement_->SetFairShareRatio(fairShareRatio);
    // This version is local for tree and used to compute satisfaction ratios.
    Attributes_.FairShareRatio = fairShareRatio;
}

inline double TSchedulerElement::GetFairShareRatio() const
{
    return ResourceTreeElement_->GetFairShareRatio();
}

inline const NLogging::TLogger& TSchedulerElement::GetLogger() const
{
    return Logger;
}

inline bool TOperationElement::DetailedLogsEnabled() const
{
    return RuntimeParams_->EnableDetailedLogs;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
