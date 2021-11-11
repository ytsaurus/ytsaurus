#ifndef FAIR_SHARE_TREE_ELEMENT_INL_H_
#error "Direct inclusion of this file is not allowed, include fair_share_tree_element.h"
// For the sake of sane code completion.
#include "fair_share_tree_element.h"
#endif

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

inline int TSchedulerElement::GetSchedulingIndex() const
{
    return SchedulingIndex_;
}

inline void TSchedulerElement::SetSchedulingIndex(int index)
{
    SchedulingIndex_ = index;
}

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

inline const TString& TSchedulerElement::GetLoggingTags() const
{
    return Logger.GetTag();
}

inline bool TSchedulerElement::IsActive(const TDynamicAttributesList& dynamicAttributesList) const
{
    return dynamicAttributesList.AttributesOf(this).Active;
}

////////////////////////////////////////////////////////////////////////////////

inline TDynamicAttributesList::TDynamicAttributesList(int size)
    : Value_(static_cast<size_t>(size))
{ }

inline TDynamicAttributes& TDynamicAttributesList::AttributesOf(const TSchedulerElement* element)
{
    int index = element->GetTreeIndex();
    YT_ASSERT(index != UnassignedTreeIndex && index < std::ssize(Value_));
    return Value_[index];
}

inline const TDynamicAttributes& TDynamicAttributesList::AttributesOf(const TSchedulerElement* element) const
{
    int index = element->GetTreeIndex();
    YT_ASSERT(index != UnassignedTreeIndex && index < std::ssize(Value_));
    return Value_[index];
}

inline void TDynamicAttributesList::InitializeResourceUsage(
    TSchedulerRootElement* rootElement,
    const TResourceUsageSnapshotPtr& resourceUsageSnapshot,
    NProfiling::TCpuInstant now)
{
    Value_.resize(rootElement->GetSchedulableElementCount());
    rootElement->FillResourceUsageInDynamicAttributes(this, resourceUsageSnapshot);
    for (auto& attributes : Value_) {
        attributes.ResourceUsageUpdateTime = now;
    }
}

inline void TDynamicAttributesList::DeactivateAll()
{
    for (auto& attributes : Value_) {
        attributes.Active = false;
    }
}

////////////////////////////////////////////////////////////////////////////////

inline TDynamicAttributes& TScheduleJobsContext::DynamicAttributesFor(const TSchedulerElement* element)
{
    YT_ASSERT(Initialized_);

    return DynamicAttributesList_.AttributesOf(element);
}

inline const TDynamicAttributes& TScheduleJobsContext::DynamicAttributesFor(const TSchedulerElement* element) const
{
    YT_ASSERT(Initialized_);

    return DynamicAttributesList_.AttributesOf(element);
}

////////////////////////////////////////////////////////////////////////////////

inline bool TSchedulerOperationElement::AreDetailedLogsEnabled() const
{
    return RuntimeParameters_->EnableDetailedLogs;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
