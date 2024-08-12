#pragma once

#include "public.h"

#include <yt/yt/core/misc/property.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

template <class TPayload>
class TBoundedPriorityQueue
{
public:
    DEFINE_BYVAL_RO_PROPERTY(double, BestDiscardedCost);

public:
    using TElement = std::pair<double, TPayload>;

    explicit TBoundedPriorityQueue(int maxSize);

    void Insert(double cost, TPayload payload);

    std::optional<TElement> ExtractMax();

    template <class TFilter>
    void Invalidate(TFilter&& filter);

    bool IsEmpty() const;

    void Reset();

private:
    const int Capacity_;

    std::vector<TElement> Elements_;

    static bool LessComparator(const TElement& lhs, const TElement& rhs);

    static bool GreaterComparator(const TElement& lhs, const TElement& rhs);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer

#define BOUNDED_PRIORITY_QUEUE_H
#include "bounded_priority_queue-inl.h"
#undef BOUNDED_PRIORITY_QUEUE_H
