#include "helpers.h"

#include <algorithm>
#include <iterator>

namespace NYT {
namespace NErasure {

////////////////////////////////////////////////////////////////////////////////

TBlockIndexList MakeSegment(int begin, int end)
{
    TBlockIndexList result(end - begin);
    for (int i = begin; i < end; ++i) {
        result[i - begin] = i;
    }
    return result;
}

TBlockIndexList MakeSingleton(int elem)
{
    TBlockIndexList result;
    result.push_back(elem);
    return result;
}

TBlockIndexList Difference(int begin, int end, const TBlockIndexList& subtrahend)
{
    int pos = 0;
    TBlockIndexList result;
    for (int i = begin; i < end; ++i) {
        while (pos < subtrahend.size() && subtrahend[pos] < i) {
            pos += 1;
        }
        if (pos == subtrahend.size() || subtrahend[pos] != i) {
            result.push_back(i);
        }
    }
    return result;
}

TBlockIndexList Difference(const TBlockIndexList& first, const TBlockIndexList& second)
{
    TBlockIndexList result;
    std::set_difference(first.begin(), first.end(), second.begin(), second.end(), std::back_inserter(result));
    return result;
}

TBlockIndexList Difference(const TBlockIndexList& set, int subtrahend)
{
    return Difference(set, MakeSingleton(subtrahend));
}

TBlockIndexList Intersection(const TBlockIndexList& first, const TBlockIndexList& second)
{
    TBlockIndexList result;
    std::set_intersection(first.begin(), first.end(), second.begin(), second.end(), std::back_inserter(result));
    return result;
}

TBlockIndexList Union(const TBlockIndexList& first, const TBlockIndexList& second)
{
    TBlockIndexList result;
    std::set_union(first.begin(), first.end(), second.begin(), second.end(), std::back_inserter(result));
    return result;
}

bool Contains(const TBlockIndexList& set, int elem)
{
    return std::binary_search(set.begin(), set.end(), elem);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NErasure
} // namespace NYT

