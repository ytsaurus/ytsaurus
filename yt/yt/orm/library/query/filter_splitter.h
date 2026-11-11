#pragma once

#include <yt/yt/library/query/base/ast.h>
#include <yt/yt/library/query/misc/objects_holder.h>

namespace NYT::NOrm::NQuery {

//! Predicate for AST nodes that access a column or attribute.
using TExpressionPredicate = std::function<bool(NQueryClient::NAst::TReferenceExpressionPtr)>;

////////////////////////////////////////////////////////////////////////////////

//! First part has no column-access nodes matching |predicate|; second part contains the remaining conjuncts.
std::pair<NQueryClient::NAst::TExpressionPtr, NQueryClient::NAst::TExpressionPtr> SplitFilterByPredicate(
    TObjectsHolder* context,
    TExpressionPredicate predicate,
    NQueryClient::NAst::TExpressionPtr filter);

//! Splits expression into a forest of subtrees, combined by AND construction after applying De Morgan's laws.
std::vector<NQueryClient::NAst::TExpressionPtr> SplitIntoSubTrees(TObjectsHolder* context, NQueryClient::NAst::TExpressionPtr expression);

//! Returns true if |predicate| matches at least one column-access node in the expression.
bool CheckExpressionByPredicate(
    NQueryClient::NAst::TExpressionPtr expression,
    TExpressionPredicate predicate);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
