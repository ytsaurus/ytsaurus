#include "misc.h"

#include <algorithm>
#include <functional>
#include <ranges>

namespace NYT::NOrm::NQuery {

using namespace NQueryClient::NAst;

////////////////////////////////////////////////////////////////////////////////

TString JoinFilters(const std::vector<TString>& filters)
{
    return JoinToString(std::views::filter(filters, std::not_fn(&TString::empty)), TStringBuf(" AND "));
}

bool IsTargetReference(const TExpressionList& exprs, const NQueryClient::NAst::TReference& reference)
{
    if (exprs.size() != 1) {
        return false;
    }
    if (auto* refExpr = exprs[0]->As<TReferenceExpression>()) {
        return refExpr->Reference == reference;
    }
    return false;
}

bool IsAnyExprATargetReference(const TExpressionList& exprs, const NQueryClient::NAst::TReference& reference)
{
    return std::ranges::any_of(exprs, [&] (const TExpressionPtr& expr) {
        auto* refExpr = expr->template As<TReferenceExpression>();
        return refExpr && refExpr->Reference == reference;
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
