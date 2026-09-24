#ifndef ATTRIBUTES_LIST_INL_H_
#error "Direct inclusion of this file is not allowed, include attributes_list.h"
// For the sake of sane code completion.
#include "attributes_list.h"
#endif

#include <yt/yt/server/scheduler/strategy/policy/attributes_list.h>
#include <yt/yt/server/scheduler/strategy/pool_tree_element.h>

namespace NYT::NScheduler::NStrategy::NPolicy {

////////////////////////////////////////////////////////////////////////////////

template <typename TAttributes>
TAttributesList<TAttributes>::TAttributesList(int size)
    : Attributes_(size)
{ }

template <typename TAttributes>
TAttributes& TAttributesList<TAttributes>::AttributesOf(const TPoolTreeElement* element)
{
    int index = element->GetTreeIndex();
    YT_ASSERT(index != UnassignedTreeIndex && index < std::ssize(Attributes_));
    return Attributes_[index];
}

template <typename TAttributes>
const TAttributes& TAttributesList<TAttributes>::AttributesOf(const TPoolTreeElement* element) const
{
    int index = element->GetTreeIndex();
    YT_ASSERT(index != UnassignedTreeIndex && index < std::ssize(Attributes_));
    return Attributes_[index];
}

template <typename TAttributes>
auto TAttributesList<TAttributes>::begin()
{
    return Attributes_.begin();
}

template <typename TAttributes>
auto TAttributesList<TAttributes>::end()
{
    return Attributes_.end();
}

template <typename TAttributes>
auto TAttributesList<TAttributes>::begin() const
{
    return Attributes_.begin();
}

template <typename TAttributes>
auto TAttributesList<TAttributes>::end() const
{
    return Attributes_.end();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy
