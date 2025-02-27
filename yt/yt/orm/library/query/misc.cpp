#include "misc.h"

#include <algorithm>
#include <functional>
#include <ranges>

namespace NYT::NOrm::NQuery {

using namespace NQueryClient::NAst;

////////////////////////////////////////////////////////////////////////////////

std::string JoinFilters(const std::vector<std::string>& filters)
{
    return JoinToString(std::views::filter(filters, std::not_fn(&std::string::empty)), TStringBuf(" AND "));
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

bool IsConstant(const TExpressionPtr expr)
{
    if (auto* literal = expr->As<TLiteralExpression>()) {
        return true;
    }
    if (auto *function = expr->As<TFunctionExpression>()) {
        bool constantArguments = true;
        for (const auto arguments : function->Arguments) {
            constantArguments &= IsConstant(arguments);
        }
        return constantArguments;
    }
    return false;
}

bool IsSingleConstant(const TExpressionList& exprs)
{
    if (exprs.size() != 1) {
        return false;
    }
    return IsConstant(exprs[0]);
}

std::vector<TReference> ExtractAllReferences(const TExpressionList& exprs)
{
    std::vector<TReference> refs;
    for (int i = 0; i < std::ssize(exprs); ++i) {
        if (auto* refExpr = exprs[i]->As<TReferenceExpression>()) {
            refs.push_back(refExpr->Reference);
        } else {
            return {};
        }
    }
    return refs;
}

std::optional<TReference> TryExtractReference(const TExpressionList& exprs)
{
    if (exprs.size() != 1) {
        return std::nullopt;
    }
    if (auto* refExpr = exprs[0]->As<TReferenceExpression>()) {
        return refExpr->Reference;
    } else {
        return std::nullopt;
    }
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
