#pragma once

#include "public.h"

#include <library/cpp/yt/misc/property.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

template <class TPayload>
class alignas(CacheLineSize) TBoundedPriorityQueue
{
public:
    struct TElement
    {
        double Cost;
        TPayload Payload;
    };

    DEFINE_BYREF_RW_PROPERTY(std::vector<TElement>, Elements);
    DEFINE_BYVAL_RO_PROPERTY(double, BestDiscardedCost);

public:
    explicit TBoundedPriorityQueue(int maxSize);

    bool IsEmpty() const;

    void Insert(double cost, TPayload&& payload);

    TElement ExtractMax();

    template <class TFilter>
    void Invalidate(TFilter&& filter);

    void Reset();

private:
    const int Capacity_;

    static bool LessComparator(const TElement& lhs, const TElement& rhs);
    static bool GreaterComparator(const TElement& lhs, const TElement& rhs);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer

#define BOUNDED_PRIORITY_QUEUE_INL_H_
#include "bounded_priority_queue-inl.h"
#undef BOUNDED_PRIORITY_QUEUE_INL_H_
