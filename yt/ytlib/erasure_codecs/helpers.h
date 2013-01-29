#pragma once

#include <vector>

namespace NYT {

namespace NErasure {

////////////////////////////////////////////////////////////////////////////////

// All vectors should be sorted
std::vector<int> Segment(int begin, int end);

std::vector<int> Element(int elem);

std::vector<int> Difference(int begin, int end, const std::vector<int>& subtrahend);

std::vector<int> Difference(const std::vector<int>& first, const std::vector<int>& second);

std::vector<int> Difference(const std::vector<int>& first, int elem);

std::vector<int> Intersection(const std::vector<int>& first, const std::vector<int>& second);

std::vector<int> Union(const std::vector<int>& first, const std::vector<int>& second);

bool Contains(const std::vector<int>& set, int elem);

////////////////////////////////////////////////////////////////////////////////

} // namespace NErasure

} // namespace NYT

