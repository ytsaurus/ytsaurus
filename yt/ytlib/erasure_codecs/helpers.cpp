#include "helpers.h"

#include <algorithm>
#include <iterator>

namespace NYT {
namespace NErasure {

////////////////////////////////////////////////////////////////////////////////

std::vector<int> Segment(int begin, int end)
{
    std::vector<int> result(end - begin);
    for (int i = begin; i < end; ++i) {
        result[i - begin] = i;
    }
    return result;
}

std::vector<int> Element(int elem)
{
    return std::vector<int>(1, elem);
}

std::vector<int> Difference(int begin, int end, const std::vector<int>& subtrahend)
{
    int pos = 0;
    std::vector<int> result;
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

std::vector<int> Difference(const std::vector<int>& first, const std::vector<int>& second)
{
    std::vector<int> result;
    set_difference(first.begin(), first.end(), second.begin(), second.end(), back_inserter(result));
    return result;
}

std::vector<int> Difference(const std::vector<int>& set, int subtrahend)
{
    return Difference(set, std::vector<int>(1, subtrahend));
}

std::vector<int> Intersection(const std::vector<int>& first, const std::vector<int>& second)
{
    std::vector<int> result;
    set_intersection(first.begin(), first.end(), second.begin(), second.end(), back_inserter(result));
    return result;
}

std::vector<int> Union(const std::vector<int>& first, const std::vector<int>& second)
{
    std::vector<int> result;
    set_union(first.begin(), first.end(), second.begin(), second.end(), back_inserter(result));
    return result;
}

bool Contains(const std::vector<int>& set, int elem)
{
    return binary_search(set.begin(), set.end(), elem);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NErasure
} // namespace NYT

