#pragma once

#include "piecewise_linear_function.h"

#include <yt/core/logging/log.h>

#include <yt/core/misc/algorithm_helpers.h>

#include <yt/library/numeric/binary_search.h>

#include <contrib/libs/gtest/include/gtest/gtest_prod.h>

#include <vector>
#include <algorithm>
#include <random>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

static constexpr int MergeArity = 8;
using TPivotsVector = SmallVector<int, MergeArity + 1>;

void SortOrMergeImpl(
    std::vector<double>* vec,
    std::vector<double>* buffer,
    TPivotsVector* mergePivots,
    TPivotsVector* newPivots);

bool FindMergePivots(const std::vector<double>* vec, TPivotsVector* pivots) noexcept;

void SortOrMerge(std::vector<double>* vec);

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define PIECEWISE_LINEAR_FUNCTION_INL_H_
#include "piecewise_linear_function-inl.h"
#undef PIECEWISE_LINEAR_FUNCTION_INL_H_

