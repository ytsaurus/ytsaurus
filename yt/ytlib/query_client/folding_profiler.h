#pragma once

#include "evaluation_helpers.h"
#include "query.h"
#include "llvm_folding_set.h"

#include <util/generic/hash_set.h>
#include <util/generic/noncopyable.h>

#include <limits>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

typedef std::function<TCGQueryCallback()> TCGQueryCallbackGenerator;
typedef std::function<TCGExpressionCallback()> TCGExpressionCallbackGenerator;

void Profile(
    const TTableSchema& tableSchema,
    llvm::FoldingSetNodeID* id);

TCGExpressionCallbackGenerator Profile(
    TConstExpressionPtr expr,
    const TTableSchema& schema,
    llvm::FoldingSetNodeID* id,
    TCGVariables* variables,
    const TConstFunctionProfilerMapPtr& functionProfilers = BuiltinFunctionCG.Get());

TCGQueryCallbackGenerator Profile(
    TConstBaseQueryPtr query,
    llvm::FoldingSetNodeID* id,
    TCGVariables* variables,
    const TConstFunctionProfilerMapPtr& functionProfilers = BuiltinFunctionCG.Get(),
    const TConstAggregateProfilerMapPtr& aggregateProfilers = BuiltinAggregateCG.Get());

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
