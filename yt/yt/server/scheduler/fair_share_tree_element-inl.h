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

inline bool TSchedulerElement::IsActive(const TDynamicAttributesList& dynamicAttributesList) const
{
    return dynamicAttributesList[GetTreeIndex()].Active;
}

////////////////////////////////////////////////////////////////////////////////

inline TDynamicAttributes& TScheduleJobsContext::DynamicAttributesFor(const TSchedulerElement* element)
{
    YT_ASSERT(Initialized_);

    int index = element->GetTreeIndex();
    YT_ASSERT(index != UnassignedTreeIndex && index < std::ssize(DynamicAttributesList_));
    return DynamicAttributesList_[index];
}

inline const TDynamicAttributes& TScheduleJobsContext::DynamicAttributesFor(const TSchedulerElement* element) const
{
    YT_ASSERT(Initialized_);

    int index = element->GetTreeIndex();
    YT_ASSERT(index != UnassignedTreeIndex && index < std::ssize(DynamicAttributesList_));
    return DynamicAttributesList_[index];
}

////////////////////////////////////////////////////////////////////////////////

inline bool TSchedulerOperationElement::AreDetailedLogsEnabled() const
{
    return RuntimeParameters_->EnableDetailedLogs;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
