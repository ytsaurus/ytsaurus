#ifndef FAIR_SHARE_TREE_ELEMENT_INL_H_
#error "Direct inclusion of this file is not allowed, include pool_tree_element.h"
// For the sake of sane code completion.
#include "pool_tree_element.h"
#endif

namespace NYT::NScheduler::NStrategy {

////////////////////////////////////////////////////////////////////////////////

template <class TAttributes>
const TAttributes& GetSchedulerElementAttributesFromVector(const std::vector<TAttributes>& vector, const TPoolTreeElement* element)
{
    int index = element->GetTreeIndex();
    YT_ASSERT(index != UnassignedTreeIndex && index < std::ssize(vector));
    return vector[index];
}

template <class TAttributes>
TAttributes& GetSchedulerElementAttributesFromVector(std::vector<TAttributes>& vector, const TPoolTreeElement* element)
{
    int index = element->GetTreeIndex();
    YT_ASSERT(index != UnassignedTreeIndex && index < std::ssize(vector));
    return vector[index];
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy
