#pragma once

#include "evaluation_helpers.h"
#include "plan_fragment.h"

#include <util/generic/hash_set.h>
#include <util/generic/noncopyable.h>

#include <limits>

#include <llvm/ADT/FoldingSet.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

typedef std::function<TCGQueryCallback()> TCGQueryCallbackGenerator;
typedef std::function<TCGExpressionCallback()> TCGExpressionCallbackGenerator;

TCGQueryCallbackGenerator Profile(
    TConstQueryPtr query,
    llvm::FoldingSetNodeID* id,
    TCGVariables* variables,
    std::vector<std::vector<bool>>* literalArgs,
    const IFunctionRegistryPtr functionRegistry);

TCGExpressionCallbackGenerator Profile(
    TConstExpressionPtr expr,
    const TTableSchema& schema,
    llvm::FoldingSetNodeID* id,
    TCGVariables* variables,
    std::vector<std::vector<bool>>* literalArgs,
    const IFunctionRegistryPtr functionRegistry);

void Profile(
    const TTableSchema& tableSchema,
    int keySize,
    llvm::FoldingSetNodeID* id,
    const IFunctionRegistryPtr functionRegistry);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

// A hasher for llvm::FoldingSetNodeID
template<>
struct hash<llvm::FoldingSetNodeID>
{
    inline size_t operator()(const llvm::FoldingSetNodeID& id) const
    {
        return id.ComputeHash();
    }
};

////////////////////////////////////////////////////////////////////////////////
