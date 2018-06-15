#pragma once

#include <mapreduce/yt/interface/fwd.h>
#include <mapreduce/yt/interface/operation.h>

namespace NYT {

// If `spec.SortBy' columns set is a prefix of "sorted_by" attributes of
// all input tables, run merge operation. Otherwise, reluctantly run sort.
IOperationPtr LazySort(
    const IClientBasePtr& client,
    const TSortOperationSpec& spec,
    const TOperationOptions& options = TOperationOptions());

} // namespace NYT
