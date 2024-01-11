#pragma once

#include "partition.h"
#include "store.h"

namespace NYT::NLsm::NTesting {

////////////////////////////////////////////////////////////////////////////////

TTimestamp ComputeMajorTimestamp(
    TPartition* partition,
    const std::vector<TStore*>& stores);

std::vector<std::unique_ptr<TStore>> Compact(
    const std::vector<TStore*>& stores,
    TPartition* partition,
    const std::vector<TKey>& pivotKeys,
    TTimestamp retentionTimestamp,
    TTimestamp majorTimestamp,
    i64 minDataVersions,
    double compressionRatio);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting

