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
    const std::vector<TKeyRange>& ranges,
    bool pushdownGroupClause = true);

TDataSplits GetPrunedSplits(
    const TConstQueryPtr& query,
    const TDataSplits& splits);

TKeyRange GetRange(const TDataSplits& splits);

std::vector<TKeyRange> GetRanges(const TGroupedDataSplits& groupedSplits);

typedef std::pair<ISchemafulReaderPtr, TFuture<TQueryStatistics>> TEvaluateResult;

TQueryStatistics CoordinateAndExecute(
    const TPlanFragmentPtr& fragment,
    ISchemafulWriterPtr writer,
    bool isOrdered,
    const std::vector<TKeyRange>& ranges,
    std::function<TEvaluateResult(const TConstQueryPtr&, size_t)> evaluateSubquery,
    std::function<TQueryStatistics(const TConstQueryPtr&, ISchemafulReaderPtr, ISchemafulWriterPtr)> evaluateTop,
    bool pushdownGroupOp = true);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

