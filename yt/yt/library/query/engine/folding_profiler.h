#pragma once

#include "llvm_folding_set.h"

#include <yt/yt/library/codegen_api/execution_backend.h>

#include <yt/yt/library/query/engine_api/evaluation_helpers.h>

#include <yt/yt/library/web_assembly/engine/builtins.h>

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
    const TConstFunctionProfilerMapPtr& functionProfilers = GetBuiltinFunctionProfilers().Get(),
    const NWebAssembly::TModuleBytecode& sdk = NWebAssembly::GetBuiltinSdk());

TCGQueryGenerator Profile(
    const TConstBaseQueryPtr& query,
    llvm::FoldingSetNodeID* id,
    TCGVariables* variables,
    const std::vector<IJoinProfilerPtr>& joinProfilers,
    bool useCanonicalNullRelations = false,
    NCodegen::EExecutionBackend executionBackend = NCodegen::EExecutionBackend::Native,
    NCodegen::EOptimizationLevel optimizationLevel = NCodegen::EOptimizationLevel::Default,
    const TConstFunctionProfilerMapPtr& functionProfilers = GetBuiltinFunctionProfilers().Get(),
    const TConstAggregateProfilerMapPtr& aggregateProfilers = GetBuiltinAggregateProfilers().Get(),
    const NWebAssembly::TModuleBytecode& sdk = NWebAssembly::GetBuiltinSdk(),
    bool allowUnorderedGroupByWithLimit = true,
    i64 maxJoinBatchSize = DefaultMaxJoinBatchSize);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
