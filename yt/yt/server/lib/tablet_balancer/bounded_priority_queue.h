#pragma once

#include "public.h"

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

template <class TPayload>
class TBoundedPriorityQueue
{
public:
    using TItem = std::pair<double, TPayload>;

    explicit TBoundedPriorityQueue(int maxSize);

    void Insert(double cost, TPayload payload);

    std::optional<TItem> ExtractMax();

    void Invalidate(std::function<bool(const TItem&)> filter);

    bool IsEmpty() const;

    void Reset();

private:
    static bool LessComparator(const TItem& lhs, const TItem& rhs);

    static bool GreaterComparator(const TItem& lhs, const TItem& rhs);

    const int MaxSize_;

    std::vector<TItem> Elements_;
    double BestDiscarded_ = std::numeric_limits<double>::min();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer

#define BOUNDED_PRIORITY_QUEUE_H
#include "bounded_priority_queue-inl.h"
#undef BOUNDED_PRIORITY_QUEUE_H
