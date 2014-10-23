#pragma once

#include "public.h"
#include "callbacks.h"
#include "plan_fragment.h"
#include "query_statistics.h"
#include "key_trie.h"

#include <core/logging/log.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

std::pair<TConstQueryPtr, std::vector<TConstQueryPtr>> CoordinateQuery(
    const TConstQueryPtr& query,
    const std::vector<TKeyRange>& ranges);

TDataSplits GetPrunedSplits(
    const TConstQueryPtr& query,
    const TDataSplits& splits);

TKeyRange GetRange(const TDataSplits& splits);

std::vector<TKeyRange> GetRanges(const TGroupedDataSplits& groupedSplits);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

