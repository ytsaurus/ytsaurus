#include "stdafx.h"

#include "ast.h"

namespace NYT {
namespace NQueryClient {
namespace NAst {

////////////////////////////////////////////////////////////////////////////////

TStringBuf TExpression::GetSource(const TStringBuf& source) const
{
    auto begin = SourceLocation.first;
    auto end = SourceLocation.second;

    return source.substr(begin, end - begin);
}

Stroka InferName(const TExpression* expr)
{
    if (auto commaExpr = expr->As<TCommaExpression>()) {
        return InferName(commaExpr->Lhs.Get()) + ", " + InferName(commaExpr->Rhs.Get());
    } else if (auto literalExpr = expr->As<TLiteralExpression>()) {
        return ToString(literalExpr->Value);
    } else if (auto referenceExpr = expr->As<TReferenceExpression>()) {
        return referenceExpr->ColumnName;
    } else if (auto functionExpr = expr->As<TFunctionExpression>()) {
        Stroka result = functionExpr->FunctionName;
        result += "(";
        result += InferName(functionExpr->Arguments.Get());
        result += ")";
        return result;
    } else if (auto binaryExpr = expr->As<TBinaryOpExpression>()) {
        auto canOmitParenthesis = [] (const TExpression* expr) {
            return 
                expr->As<TLiteralExpression>() ||
                expr->As<TReferenceExpression>() ||
                expr->As<TFunctionExpression>();
        };
        auto lhsName = InferName(binaryExpr->Lhs.Get());
        if (!canOmitParenthesis(binaryExpr->Lhs.Get())) {
            lhsName = "(" + lhsName + ")";
        }
        auto rhsName = InferName(binaryExpr->Rhs.Get());
        if (!canOmitParenthesis(binaryExpr->Rhs.Get())) {
            rhsName = "(" + rhsName + ")";
        }
        return
            lhsName +
            " " + GetBinaryOpcodeLexeme(binaryExpr->Opcode) + " " +
            rhsName;
    } else if (auto inExpr = expr->As<TInExpression>()) {
        Stroka result = InferName(inExpr->Expr.Get());
        result += " in (";
        for (int i = 0; i < inExpr->Values.size(); ++i) {
            if (i) {
                result += ", ";
            }

            if (inExpr->Values[i].size() > 1) {
                result += "(";
            }

            for (int j = 0; j < inExpr->Values[i].size(); ++j) {
                if (j) {
                    result += ", ";
                }
                result += ToString(inExpr->Values[i][j]);
            }

            if (inExpr->Values[i].size() > 1) {
                result += ")";
            }
        }
        result += ")";
        return result;
    } else {
        YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NAst
} // namespace NQueryClient
} // namespace NYT
