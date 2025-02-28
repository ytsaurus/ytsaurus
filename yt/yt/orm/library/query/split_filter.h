#pragma once

#include <yt/yt/library/query/base/ast.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct TFilterSplit
{
    NQueryClient::NAst::TNullableExpressionList Where;
    NQueryClient::NAst::TNullableExpressionList Having;
    THashMap<TString, NQueryClient::NAst::TNullableExpressionList> JoinPredicates;
};

struct TFilterHints
{
    THashSet<NQueryClient::NAst::TExpressionPtr> Having;
    THashMap<NQueryClient::NAst::TExpressionPtr, TString> JoinPredicates;
};

// Splits expression into parts for different filters of select query using hints.
TFilterSplit SplitFilter(
    NQueryClient::NAst::TExpressionPtr expression,
    const TFilterHints& hints,
    TObjectsHolder* objectsHolder);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
