#pragma once

#include <yt/yt/server/scheduler/strategy/pool_tree_element.h>

namespace NYT::NScheduler::NStrategy::NPolicy {

////////////////////////////////////////////////////////////////////////////////

template <typename TAttributes>
class TAttributesList final
    : public std::vector<TAttributes>
{
public:
    explicit TAttributesList(int size = 0);
    TAttributes& AttributesOf(const TPoolTreeElement* element);
    const TAttributes& AttributesOf(const TPoolTreeElement* element) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy

#define ATTRIBUTES_LIST_INL_H_
#include "attributes_list-inl.h"
#undef ATTRIBUTES_LIST_INL_H_
