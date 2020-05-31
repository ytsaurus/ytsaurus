#pragma once
#ifndef FAIR_SHARE_TREE_ELEMENT_CLASSIC_INL_H_
#error "Direct inclusion of this file is not allowed, include fair_share_tree_element_classic.h"
// For the sake of sane code completion.
#include "fair_share_tree_element_classic.h"
#endif

namespace NYT::NScheduler::NClassicScheduler {

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

inline void TSchedulerElement::PublishFairShareRatio()
{
    // ResourceTreeElement_.FairShareRatio is global and used to balance preemption lists.
    ResourceTreeElement_->SetFairShareRatio(Attributes_.FairShare.Total());
    // Attributes_.FairShare is local for tree and used to compute satisfaction ratios.
    // PersistentAttributes_.LastIntegralShareRatio is used to calculate consumed resources.
    PersistentAttributes_.LastIntegralShareRatio = Attributes_.FairShare.IntegralGuaranteeRatio;
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
    return RuntimeParameters_->EnableDetailedLogs;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NClassicScheduler
