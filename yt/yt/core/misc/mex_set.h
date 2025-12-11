#pragma once

#include "public.h"

#include <utility>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A simple set of non-negative integers supporting fast MEX (minimum excluded value) queries.
//! Implemented as a set of disjoint intervals. See complexities of operations below.
//! Feel free to extend however needed, or to replace with a more efficient implementation,
//! there are about a million ways to do MEX with different tradeoffs. This one makes sense
//! if the set is densely and consecutively populated, e.g. it is often just a few intervals.
//! NB: Value-related methods throw on out of bounds values (negative or equal to INT_MAX).
class TMexIntSet
{
public:
    TMexIntSet() = default;

    //! Inserts a value into the set. Value must be non-negative.
    //! Returns whether an insertion actually took place.
    //! Complexity: O(log N), where N is the number of disjoint intervals in the set.
    bool Insert(int value);

    //! Erases a value from the set.
    //! Returns whether an element was actually erased.
    //! Complexity: O(log N), where N is the number of disjoint intervals in the set.
    bool Erase(int value);

    //! Returns whether the set contains the value.
    //! Complexity: O(log N), where N is the number of disjoint intervals in the set.
    bool Contains(int value) const;

    //! Clears the set and resets MEX to 0.
    //! Complexity: O(N), where N is the number of disjoint intervals in the set.
    void Clear();

    //! Returns the minimum non-negative integer not contained in the set.
    //! Complexity: O(1).
    int GetMex() const;

private:
    // Maps left endpoint (inclusive) of an interval to its right endpoint (exclusive).
    std::map<int, int> Intervals_;
    // Current MEX value.
    int Mex_ = 0;

    // NB: Throws if value is out of the [0, INT_MAX) range.
    static void ValidateBounds(int value);

    // Finds iterators to intervals adjacent to the given value.
    template <class TMap>
    static auto FindAdjacentIntervals(TMap& intervals, int value) -> std::pair<decltype(intervals.begin()), decltype(intervals.begin())>;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
