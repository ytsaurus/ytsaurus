#include "mex_set.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

// This could have been a privavte class helper, but we are fooled by const-ness issues.
// Patiently waiting for deducing this in C++23 to end this madness.
template <class TMap>
auto FindAdjacentIntervals(TMap& intervals, int value) -> std::pair<decltype(intervals.begin()), decltype(intervals.begin())>
{
    auto rightIt = intervals.upper_bound(value);
    auto leftIt = intervals.end();

    if (rightIt != intervals.begin()) {
        leftIt = std::prev(rightIt);
    }

    return {leftIt, rightIt};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

bool TMexSet::Insert(int value)
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
        // Merge both intervals.
        leftIt->second = rightIt->second;
        Intervals_.erase(rightIt);
    } else if (leftAdjacent) {
        // Extend left interval to the right.
        leftIt->second += 1;
    } else if (rightAdjacent) {
        // Extend right interval to the left.
        auto newRight = rightIt->second;
        Intervals_.erase(rightIt);
        Intervals_[value] = newRight;
    } else {
        // Create a new interval.
        Intervals_[value] = value + 1;
    }

    return true;
}

bool TMexSet::Erase(int value)
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

bool TMexSet::Contains(int value) const
{
    ValidateBounds(value);

    auto [leftIt, _] = FindAdjacentIntervals(Intervals_, value);
    return leftIt != Intervals_.end() && leftIt->second > value;
}

void TMexSet::Clear()
{
    Intervals_.clear();
    Mex_ = 0;
}

int TMexSet::GetMex() const
{
    return Mex_;
}

void TMexSet::ValidateBounds(int value)
{
    YT_VERIFY(value >= 0);
    YT_VERIFY(value < std::numeric_limits<int>::max());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
