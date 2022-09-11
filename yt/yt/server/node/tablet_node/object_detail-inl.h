#ifndef OBJECT_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include object_detail.h"
// For the sake of sane code completion.
#include "object_detail.h"
#endif

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

template <class TObjectPtr>
inline bool TObjectIdComparer::operator()(const TObjectPtr& lhs, const TObjectPtr& rhs) const
{
    return Compare(lhs, rhs);
}

template <class TObjectPtr>
inline bool TObjectIdComparer::Compare(const TObjectPtr& lhs, const TObjectPtr& rhs)
{
    return lhs->GetId() < rhs->GetId();
}

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
std::vector<TValue*> GetValuesSortedByKey(const NHydra::TReadOnlyEntityMap<TValue>& entities)
{
    std::vector<TValue*> values;
    values.reserve(entities.size());

    for (const auto& [key, entity] : entities) {
        values.push_back(entity);
    }
    std::sort(values.begin(), values.end(), [] (const auto& lhs, const auto& rhs) {
        return lhs->GetId() < rhs->GetId();
    });
    return values;
}

template <class TValue>
std::vector<TValue*> GetValuesSortedByKey(const THashSet<TValue*>& entities)
{
    std::vector<TValue*> values;
    values.reserve(entities.size());

    for (auto* object : entities) {
        values.push_back(object);
    }
    std::sort(values.begin(), values.end(), [] (const auto& lhs, const auto& rhs) {
        return lhs->GetId() < rhs->GetId();
    });
    return values;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
