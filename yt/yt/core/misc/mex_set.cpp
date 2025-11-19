#include "mex_set.h"

#include <library/cpp/yt/error/error.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

bool TMexIntSet::Insert(int value)
{
    ValidateBounds(value);

    auto [leftIt, rightIt] = FindAdjacentIntervals(Intervals_, value);

    if (leftIt != Intervals_.end() && leftIt->second > value) {
        // Already present.
        return false;
    }

    auto leftAdjacent = leftIt != Intervals_.end() && leftIt->second == value;
    auto rightAdjacent = rightIt != Intervals_.end() && rightIt->first == value + 1;

    if (value == Mex_) {
        if (rightAdjacent) {
            Mex_ = rightIt->second;
        } else {
            ++Mex_;
        }
    }

    if (leftAdjacent && rightAdjacent) {
        // Merge intervals.
        leftIt->second = rightIt->second;
        Intervals_.erase(rightIt);
    } else if (leftAdjacent) {
        // Extend left interval to the right.
        leftIt->second += 1;
    } else if (rightAdjacent) {
        // Extend right interval to the left.
        auto newRight = rightIt->second;
        Intervals_.erase(rightIt);
        Intervals_.emplace(value, newRight);
    } else {
        // Create a new interval.
        Intervals_.emplace(value, value + 1);
    }

    return true;
}

bool TMexIntSet::Erase(int value)
{
    ValidateBounds(value);

    auto [leftIt, rightIt] = FindAdjacentIntervals(Intervals_, value);
    if (leftIt == Intervals_.end() || leftIt->second <= value) {
        // Not present.
        return false;
    }

    if (leftIt->first == value && leftIt->second == value + 1) {
        // Remove the interval.
        Intervals_.erase(leftIt);
    } else if (leftIt->first == value) {
        // Shrink the interval from the left.
        auto newRight = leftIt->second;
        Intervals_.erase(leftIt);
        Intervals_[value + 1] = newRight;
    } else if (leftIt->second == value + 1) {
        // Shrink the interval from the right.
        leftIt->second = value;
    } else {
        // Split the interval.
        auto oldRight = leftIt->second;
        leftIt->second = value;
        Intervals_[value + 1] = oldRight;
    }

    if (value < Mex_) {
        Mex_ = value;
    }

    return true;
}

bool TMexIntSet::Contains(int value) const
{
    ValidateBounds(value);

    auto [leftIt, _] = FindAdjacentIntervals(Intervals_, value);
    return leftIt != Intervals_.end() && leftIt->second > value;
}

void TMexIntSet::Clear()
{
    Intervals_.clear();
    Mex_ = 0;
}

int TMexIntSet::GetMex() const
{
    return Mex_;
}

void TMexIntSet::ValidateBounds(int value)
{
    if (value < 0 || value == std::numeric_limits<int>::max()) {
        THROW_ERROR_EXCEPTION(
            "Value %v is out of bounds for TMexIntSet, must be in the range [0, %v)",
            value,
            std::numeric_limits<int>::max());
    }
}

template <class TMap>
auto TMexIntSet::FindAdjacentIntervals(TMap& intervals, int value) -> std::pair<decltype(intervals.begin()), decltype(intervals.begin())>
{
    auto rightIt = intervals.upper_bound(value);
    auto leftIt = intervals.end();

    if (rightIt != intervals.begin()) {
        leftIt = std::prev(rightIt);
    }

    return {leftIt, rightIt};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
