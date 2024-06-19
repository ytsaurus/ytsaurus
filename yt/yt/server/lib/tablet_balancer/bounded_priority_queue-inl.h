#ifndef BOUNDED_PRIORITY_QUEUE_H
#error "Direct inclusion of this file is not allowed, include bounded_priority_queue.h"
// For the sake of sane code completion.
#include "bounded_priority_queue.h"
#endif

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

template <class TPayload>
TBoundedPriorityQueue<TPayload>::TBoundedPriorityQueue(int maxSize)
    : MaxSize_(maxSize)
    , BestDiscarded_(std::numeric_limits<double>::min())
{
    Elements_.reserve(2 * MaxSize_);
}

template <class TPayload>
void TBoundedPriorityQueue<TPayload>::Insert(double cost, TPayload payload)
{
    if (cost < BestDiscarded_) {
        return;
    }

    Elements_.emplace_back(cost, std::move(payload));

    if (std::ssize(Elements_) >= 2 * MaxSize_) {
        std::nth_element(Elements_.begin(), Elements_.begin() + MaxSize_, Elements_.end(), GreaterComparator);
        BestDiscarded_ = std::max(
            BestDiscarded_,
            std::max_element(
                Elements_.begin() + MaxSize_,
                Elements_.end(),
                LessComparator)->first);
        Elements_.resize(MaxSize_);
    }
}

template <class TPayload>
auto TBoundedPriorityQueue<TPayload>::ExtractMax() -> std::optional<TItem>
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
void TBoundedPriorityQueue<TPayload>::Invalidate(std::function<bool(const TItem&)> filter)
{
    std::erase_if(Elements_, filter);
}

template <class TPayload>
bool TBoundedPriorityQueue<TPayload>::IsEmpty() const
{
    if (Elements_.empty()) {
        return true;
    }

    return std::max_element(Elements_.begin(), Elements_.end(), LessComparator)->first < BestDiscarded_;
}

template <class TPayload>
void TBoundedPriorityQueue<TPayload>::Reset()
{
    Elements_.clear();
    BestDiscarded_ = std::numeric_limits<double>::min();
}

template <class TPayload>
bool TBoundedPriorityQueue<TPayload>::LessComparator(const TItem& a, const TItem& b)
{
    return a.first < b.first;
}

template <class TPayload>
bool TBoundedPriorityQueue<TPayload>::GreaterComparator(const TItem& a, const TItem& b)
{
    return a.first > b.first;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
