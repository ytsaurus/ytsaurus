#pragma once

#include <yt/yt/library/query/base/ast.h>
#include <yt/yt/library/query/misc/objects_holder.h>

namespace NYT::NOrm::NQuery {

using TComputedFieldsDetector = std::function<bool(NQueryClient::NAst::TReferenceExpressionPtr)>;

////////////////////////////////////////////////////////////////////////////////

//! Splits filter expression into computable and non-computable parts.
//! First part in pair does not contain computed fields.
std::pair<NQueryClient::NAst::TExpressionPtr, NQueryClient::NAst::TExpressionPtr> SplitFilter(
    TObjectsHolder* context,
    TComputedFieldsDetector detector,
    NQueryClient::NAst::TExpressionPtr filter);

//! Splits expression into a forest of subtrees, combined by AND construction after applying De Morgan's laws.
std::vector<NQueryClient::NAst::TExpressionPtr> SplitIntoSubTrees(TObjectsHolder* context, NQueryClient::NAst::TExpressionPtr expression);

//! Checks if the expression contains computed fields using detector.
bool ContainsComputedFields(
    NQueryClient::NAst::TExpressionPtr expression,
    TComputedFieldsDetector detector);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
