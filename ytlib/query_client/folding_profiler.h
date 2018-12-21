#pragma once

#include "evaluation_helpers.h"
#include "query.h"
#include "llvm_folding_set.h"

#include <util/generic/hash_set.h>
#include <util/generic/noncopyable.h>

#include <limits>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

typedef std::function<TCGQueryCallback()> TCGQueryCallbackGenerator;
typedef std::function<TCGExpressionCallback()> TCGExpressionCallbackGenerator;

void Profile(
    const TTableSchema& tableSchema,
    llvm::FoldingSetNodeID* id);

TCGExpressionCallbackGenerator Profile(
    const TConstExpressionPtr& expr,
    const TTableSchema& schema,
    llvm::FoldingSetNodeID* id,
    TCGVariables* variables,
    const TConstFunctionProfilerMapPtr& functionProfilers = BuiltinFunctionProfilers.Get());

TCGQueryCallbackGenerator Profile(
    const TConstBaseQueryPtr& query,
    llvm::FoldingSetNodeID* id,
    TCGVariables* variables,
    TJoinSubqueryProfiler joinProfiler,
    bool useMultijoin,
    const TConstFunctionProfilerMapPtr& functionProfilers = BuiltinFunctionProfilers.Get(),
    const TConstAggregateProfilerMapPtr& aggregateProfilers = BuiltinAggregateProfilers.Get());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
