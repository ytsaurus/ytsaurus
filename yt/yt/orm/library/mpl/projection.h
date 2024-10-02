#pragma once

#include "public.h"

namespace NYT::NOrm::NMpl {

////////////////////////////////////////////////////////////////////////////////

template <class TProjected, TProjected Constant>
struct TConstantProjection
{
    template <class TValue>
    TProjected operator() (const TValue&)
    {
        return Constant;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TValue, std::invocable<TValue> TProj>
struct TByProjectionComparator
{
    bool operator() (const TValue& lhs, const TValue& rhs) const {
        TProj proj;
        return std::invoke(proj, lhs) < std::invoke(proj, rhs);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, std::invocable<TKey> TProj>
auto MakeProjectedMultiMap(TProj /*pathProj*/)
{
    return TMultiMap<TKey, TValue, TByProjectionComparator<TKey, TProj>>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NMpl
