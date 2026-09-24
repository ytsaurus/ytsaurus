#pragma once

#include <yt/yt/server/scheduler/strategy/pool_tree_element.h>

#include <vector>

namespace NYT::NScheduler::NStrategy::NPolicy {

////////////////////////////////////////////////////////////////////////////////

template <typename TAttributes>
class TAttributesList final
{
public:
    explicit TAttributesList(int size = 0);

    TAttributes& AttributesOf(const TPoolTreeElement* element);
    const TAttributes& AttributesOf(const TPoolTreeElement* element) const;

    auto begin();
    auto end();

    auto begin() const;
    auto end() const;

private:
    std::vector<TAttributes> Attributes_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy

#define ATTRIBUTES_LIST_INL_H_
#include "attributes_list-inl.h"
#undef ATTRIBUTES_LIST_INL_H_
