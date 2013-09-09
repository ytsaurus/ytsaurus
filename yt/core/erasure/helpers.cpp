#include "helpers.h"

#include <algorithm>
#include <iterator>

namespace NYT {
namespace NErasure {

////////////////////////////////////////////////////////////////////////////////

TPartIndexList MakeSegment(int begin, int end)
{
    TPartIndexList result(end - begin);
    for (int i = begin; i < end; ++i) {
        result[i - begin] = i;
    }
    return result;
}

TPartIndexList MakeSingleton(int elem)
{
    TPartIndexList result;
    result.push_back(elem);
    return result;
}

TPartIndexList Difference(int begin, int end, const TPartIndexList& subtrahend)
{
    int pos = 0;
    TPartIndexList result;
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

TPartIndexList Difference(const TPartIndexList& first, const TPartIndexList& second)
{
    TPartIndexList result;
    std::set_difference(first.begin(), first.end(), second.begin(), second.end(), std::back_inserter(result));
    return result;
}

TPartIndexList Difference(const TPartIndexList& set, int subtrahend)
{
    return Difference(set, MakeSingleton(subtrahend));
}

TPartIndexList Intersection(const TPartIndexList& first, const TPartIndexList& second)
{
    TPartIndexList result;
    std::set_intersection(first.begin(), first.end(), second.begin(), second.end(), std::back_inserter(result));
    return result;
}

TPartIndexList Union(const TPartIndexList& first, const TPartIndexList& second)
{
    TPartIndexList result;
    std::set_union(first.begin(), first.end(), second.begin(), second.end(), std::back_inserter(result));
    return result;
}

bool Contains(const TPartIndexList& set, int elem)
{
    return std::binary_search(set.begin(), set.end(), elem);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NErasure
} // namespace NYT

