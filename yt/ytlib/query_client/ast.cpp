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
    if (auto typedExpr = expr->As<TLiteralExpression>()) {
        return ToString(typedExpr->Value);
    } else if (auto typedExpr = expr->As<TReferenceExpression>()) {
        return typedExpr->ColumnName;
    } else if (auto typedExpr = expr->As<TFunctionExpression>()) {
        Stroka result = typedExpr->FunctionName;
        result += "(";
        for (int i = 0; i < typedExpr->Arguments.size(); ++i) {
            if (i) {
                result += ", ";
            }
            result += InferName(typedExpr->Arguments[i].Get());
        }
        result += ")";
        return result;
    } else if (auto typedExpr = expr->As<TBinaryOpExpression>()) {
        auto canOmitParenthesis = [] (const TExpression* expr) {
            return 
                expr->As<TLiteralExpression>() ||
                expr->As<TReferenceExpression>() ||
                expr->As<TFunctionExpression>();
        };
        auto lhsName = InferName(typedExpr->Lhs.Get());
        if (!canOmitParenthesis(typedExpr->Lhs.Get())) {
            lhsName = "(" + lhsName + ")";
        }
        auto rhsName = InferName(typedExpr->Rhs.Get());
        if (!canOmitParenthesis(typedExpr->Rhs.Get())) {
            rhsName = "(" + rhsName + ")";
        }
        return
            lhsName +
            " " + GetBinaryOpcodeLexeme(typedExpr->Opcode) + " " +
            rhsName;
    } else {
        YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NAst
} // namespace NQueryClient
} // namespace NYT
