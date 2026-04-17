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
    : std::vector<TAttributes>(size)
{ }

template <typename TAttributes>
TAttributes& TAttributesList<TAttributes>::AttributesOf(const TPoolTreeElement* element)
{
    int index = element->GetTreeIndex();
    YT_ASSERT(index != UnassignedTreeIndex && index < std::ssize(*this));
    return (*this)[index];
}

template <typename TAttributes>
const TAttributes& TAttributesList<TAttributes>::AttributesOf(const TPoolTreeElement* element) const
{
    int index = element->GetTreeIndex();
    YT_ASSERT(index != UnassignedTreeIndex && index < std::ssize(*this));
    return (*this)[index];
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy
