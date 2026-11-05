#include "misc.h"

#include <yt/yt/orm/client/misc/error.h>

#include <algorithm>
#include <functional>
#include <limits>
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
    if (expr->As<TLiteralExpression>()) {
        return true;
    }
    if (auto* function = expr->As<TFunctionExpression>()) {
        bool constantArguments = true;
        for (auto* arguments : function->Arguments) {
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

std::optional<TLiteralValue> TryExtractSingleLiteralValue(const TExpressionList& exprs)
{
    if (exprs.size() != 1) {
        return std::nullopt;
    }
    if (auto* literal = exprs[0]->As<TLiteralExpression>()) {
        return literal->Value;
    }
    if (auto* unary = exprs[0]->As<TUnaryOpExpression>(); unary && unary->Opcode == NQueryClient::EUnaryOp::Minus) {
        if (auto literal = TryExtractSingleLiteralValue(unary->Operand)) {
            if (auto* value = std::get_if<i64>(&*literal); value && *value != std::numeric_limits<i64>::min()) {
                return -*value;
            }
            if (auto* value = std::get_if<double>(&*literal)) {
                return -*value;
            }
        }
    }
    return std::nullopt;
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

bool HasScalarOperands(const TBinaryOpExpression& binaryExpr)
{
    return binaryExpr.Lhs.size() == 1 && binaryExpr.Rhs.size() == 1;
}

TError CheckScalarLogicalOperand(const TExpressionList& operand)
{
    if (operand.size() == 1) {
        return {};
    }
    return TError(
        NClient::EErrorCode::InvalidRequestArguments,
        "Expecting scalar operands of AND, OR and NOT, got %Qv",
        FormatExpression(operand));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
