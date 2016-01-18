#pragma once

#include "plan_fragment.h"
#include "function_registry.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

std::pair<TQueryPtr, TDataRanges> PreparePlanFragment(
    IPrepareCallbacks* callbacks,
    const Stroka& source,
    IFunctionRegistryPtr functionRegistry,
    i64 inputRowLimit = std::numeric_limits<i64>::max(),
    i64 outputRowLimit = std::numeric_limits<i64>::max(),
    TTimestamp timestamp = NullTimestamp);


typedef std::pair<NAst::TQuery, NAst::TAliasMap> TParsedQueryInfo;

TParsedQueryInfo PrepareJobQueryAst(const Stroka& source);

std::vector<Stroka> GetExternalFunctions(
    const TParsedQueryInfo& ast,
    IFunctionRegistryPtr builtinRegistry);

TQueryPtr PrepareJobQuery(
    const Stroka& source,
    const TParsedQueryInfo& ast,
    const TTableSchema& tableSchema,
    IFunctionRegistryPtr functionRegistry);

TConstExpressionPtr PrepareExpression(
    const Stroka& source,
    TTableSchema initialTableSchema,
    IFunctionRegistryPtr functionRegistry = CreateBuiltinFunctionRegistry(),
    yhash_set<Stroka>* references = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
