#ifndef BOUNDED_PRIORITY_QUEUE_H
#error "Direct inclusion of this file is not allowed, include bounded_priority_queue.h"
// For the sake of sane code completion.
#include "bounded_priority_queue.h"
#endif

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

template <class TPayload>
TBoundedPriorityQueue<TPayload>::TBoundedPriorityQueue(int maxSize)
    : BestDiscardedCost_(0.0)
    , Capacity_(maxSize)
{
    Elements_.reserve(2 * Capacity_);
}

template <class TPayload>
void TBoundedPriorityQueue<TPayload>::Insert(double cost, TPayload payload)
{
    if (cost < BestDiscardedCost_) {
        return;
    }

    Elements_.emplace_back(cost, std::move(payload));

    if (std::ssize(Elements_) >= 2 * Capacity_) {
        std::nth_element(Elements_.begin(), Elements_.begin() + Capacity_, Elements_.end(), GreaterComparator);
        BestDiscardedCost_ = std::max(
            BestDiscardedCost_,
            std::max_element(
                Elements_.begin() + Capacity_,
                Elements_.end(),
                LessComparator)->first);
        Elements_.resize(Capacity_);
    }
}

template <class TPayload>
auto TBoundedPriorityQueue<TPayload>::ExtractMax() -> std::optional<TElement>
{
    YT_VERIFY(!IsEmpty());

    auto maxElementIterator = std::max_element(Elements_.begin(), Elements_.end(), LessComparator);
    auto maxElement = std::move(*maxElementIterator);

    if (maxElementIterator != Elements_.end() - 1) {
        std::swap(*maxElementIterator, Elements_.back());
    }

    Elements_.pop_back();

    return maxElement;
}

template <class TPayload>
template <class TFilter>
void TBoundedPriorityQueue<TPayload>::Invalidate(TFilter&& filter)
{
    std::erase_if(Elements_, std::forward<TFilter>(filter));
}

template <class TPayload>
bool TBoundedPriorityQueue<TPayload>::IsEmpty() const
{
    if (Elements_.empty()) {
        return true;
    }

    return std::max_element(Elements_.begin(), Elements_.end(), LessComparator)->first < BestDiscardedCost_;
}

template <class TPayload>
void TBoundedPriorityQueue<TPayload>::Reset()
{
    Elements_.clear();
    BestDiscardedCost_ = 0.0;
}

template <class TPayload>
bool TBoundedPriorityQueue<TPayload>::LessComparator(const TElement& a, const TElement& b)
{
    return a.first < b.first;
}

template <class TPayload>
bool TBoundedPriorityQueue<TPayload>::GreaterComparator(const TElement& a, const TElement& b)
{
    return a.first > b.first;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
