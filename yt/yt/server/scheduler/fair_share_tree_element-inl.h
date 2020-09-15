#pragma once
#ifndef FAIR_SHARE_TREE_ELEMENT_INL_H_
#error "Direct inclusion of this file is not allowed, include fair_share_tree_element.h"
// For the sake of sane code completion.
#include "fair_share_tree_element.h"
#endif

namespace NYT::NScheduler::NVectorScheduler {

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

inline void TSchedulerElement::SetNonAlive()
{
    ResourceTreeElement_->SetNonAlive();
}

inline TResourceVector TSchedulerElement::GetFairShare() const
{
    return ResourceTreeElement_->GetFairShare();
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

} // namespace NYT::NScheduler::NVectorScheduler
