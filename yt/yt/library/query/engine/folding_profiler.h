#pragma once

#include "llvm_folding_set.h"

#include <yt/yt/library/query/engine_api/evaluation_helpers.h>

#include <util/generic/hash_set.h>
#include <util/generic/noncopyable.h>

#include <limits>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

typedef std::function<TCGQueryCallback()> TCGQueryCallbackGenerator;
typedef std::function<TCGExpressionCallback()> TCGExpressionCallbackGenerator;

void Profile(
    const TTableSchemaPtr& tableSchema,
    llvm::FoldingSetNodeID* id);

TCGExpressionCallbackGenerator Profile(
    const TConstExpressionPtr& expr,
    const TTableSchemaPtr& schema,
    llvm::FoldingSetNodeID* id,
    TCGVariables* variables,
    const TConstFunctionProfilerMapPtr& functionProfilers = BuiltinFunctionProfilers.Get());

TCGQueryCallbackGenerator Profile(
    const TConstBaseQueryPtr& query,
    llvm::FoldingSetNodeID* id,
    TCGVariables* variables,
    TJoinSubqueryProfiler joinProfiler,
    const TConstFunctionProfilerMapPtr& functionProfilers = BuiltinFunctionProfilers.Get(),
    const TConstAggregateProfilerMapPtr& aggregateProfilers = BuiltinAggregateProfilers.Get());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
