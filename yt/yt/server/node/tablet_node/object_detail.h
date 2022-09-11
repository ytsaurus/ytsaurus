#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra_common/entity_map.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TObjectBase
    : public NHydra::TEntityBase
{
public:
    explicit TObjectBase(NObjectClient::TObjectId id);

    NObjectClient::TObjectId GetId() const;

protected:
    const NObjectClient::TObjectId Id_;
};

////////////////////////////////////////////////////////////////////////////////

struct TObjectIdComparer
{
    template <class TObjectPtr>
    bool operator()(const TObjectPtr& lhs, const TObjectPtr& rhs) const;

    template <class TObjectPtr>
    static bool Compare(const TObjectPtr& lhs, const TObjectPtr& rhs);
};

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
std::vector<TValue*> GetValuesSortedByKey(const NHydra::TReadOnlyEntityMap<TValue>& entities);

template <class TValue>
std::vector<TValue*> GetValuesSortedByKey(const THashSet<TValue*>& entities);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

#define OBJECT_DETAIL_INL_H_
#include "object_detail-inl.h"
#undef OBJECT_DETAIL_INL_H_
