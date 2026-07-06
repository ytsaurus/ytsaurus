#pragma once

#include <yt/yt/library/query/base/ast.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct TFilterSplit
{
    NQueryClient::NAst::TNullableExpressionList Where;
    NQueryClient::NAst::TNullableExpressionList Having;
};

struct TFilterHints
{
    THashSet<std::string> WhereAliases;
};

// Returns true if expression contains an aggregate function.
bool ContainsAggregateFunction(NQueryClient::NAst::TExpressionPtr expression);

// Splits expression into WHERE and HAVING parts using table aliases and aggregate functions.
TFilterSplit SplitFilter(
    NQueryClient::NAst::TExpressionPtr expression,
    const TFilterHints& hints,
    TObjectsHolder* objectsHolder);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
