#pragma once

#include <yt/cpp/mapreduce/interface/fwd.h>
#include <yt/cpp/mapreduce/interface/operation.h>

namespace NYT {

// If `spec.SortBy' columns set is a prefix of "sorted_by" attributes of
// all input tables, run merge operation. Otherwise, reluctantly run sort.
IOperationPtr LazySort(
    const IClientBasePtr& client,
    const TSortOperationSpec& spec,
    const TOperationOptions& options = TOperationOptions());

} // namespace NYT
