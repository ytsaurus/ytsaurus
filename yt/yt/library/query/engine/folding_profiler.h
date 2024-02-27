#pragma once

#include "llvm_folding_set.h"

#include <yt/yt/library/codegen/execution_backend.h>

#include <yt/yt/library/query/engine_api/evaluation_helpers.h>

#include <util/generic/hash_set.h>
#include <util/generic/noncopyable.h>

#include <limits>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using TCGExpressionGenerator = std::function<TCGExpressionImage()>;
using TCGQueryGenerator = std::function<TCGQueryImage()>;

void Profile(
    const TTableSchemaPtr& tableSchema,
    llvm::FoldingSetNodeID* id);

TCGExpressionGenerator Profile(
    const TConstExpressionPtr& expr,
    const TTableSchemaPtr& schema,
    llvm::FoldingSetNodeID* id,
    TCGVariables* variables,
    bool useCanonicalNullRelations = false,
    NCodegen::EExecutionBackend executionBackend = NCodegen::EExecutionBackend::Native,
    const TConstFunctionProfilerMapPtr& functionProfilers = GetBuiltinFunctionProfilers().Get());

TCGQueryGenerator Profile(
    const TConstBaseQueryPtr& query,
    llvm::FoldingSetNodeID* id,
    TCGVariables* variables,
    TJoinSubqueryProfiler joinProfiler,
    bool useCanonicalNullRelations = false,
    NCodegen::EExecutionBackend executionBackend = NCodegen::EExecutionBackend::Native,
    const TConstFunctionProfilerMapPtr& functionProfilers = GetBuiltinFunctionProfilers().Get(),
    const TConstAggregateProfilerMapPtr& aggregateProfilers = GetBuiltinAggregateProfilers().Get());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
