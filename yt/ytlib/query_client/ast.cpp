#include "stdafx.h"

#include "ast.h"

namespace NYT {
namespace NQueryClient {
namespace NAst {

////////////////////////////////////////////////////////////////////////////////

Stroka LiteralValueToString(const TLiteralValue& literalValue)
{
    switch (literalValue.Tag()) {
        case NAst::TLiteralValue::TagOf<i64>():
            return ToString(literalValue.As<i64>());
        case NAst::TLiteralValue::TagOf<ui64>():
            return ToString(literalValue.As<ui64>());
        case NAst::TLiteralValue::TagOf<double>():
            return ToString(literalValue.As<double>());
        case NAst::TLiteralValue::TagOf<bool>():
            return ToString(literalValue.As<bool>());
        case NAst::TLiteralValue::TagOf<Stroka>():
            return literalValue.As<Stroka>().Quote();
        default:
            YUNREACHABLE();
    }
}

TStringBuf TExpression::GetSource(const TStringBuf& source) const
{
    auto begin = SourceLocation.first;
    auto end = SourceLocation.second;

    return source.substr(begin, end - begin);
}

TStringBuf GetSource(TSourceLocation sourceLocation, const TStringBuf& source)
{
    auto begin = sourceLocation.first;
    auto end = sourceLocation.second;

    return source.substr(begin, end - begin);
}


Stroka InferName(const TExpressionList& exprs, bool omitValues)
{
    auto canOmitParenthesis = [] (const TExpression* expr) {
        return
            expr->As<TLiteralExpression>() ||
            expr->As<TReferenceExpression>() ||
            expr->As<TFunctionExpression>();
    };

<<<<<<< HEAD
    return JoinToString(exprs, [&] (const TExpressionPtr& expr) {
            auto name = InferName(expr.Get(), omitValues);
            return canOmitParenthesis(expr.Get()) ? name : "(" + name + ")";
        }, ", ");
}

Stroka FormatColumn(const TStringBuf& name, const TStringBuf& tableName)
{
    return tableName.empty()
        ? Stroka(name)
        : Format("%v.%v", tableName, name);
}

Stroka InferName(const TExpression* expr, bool omitValues)
{
    if (auto literalExpr = expr->As<TLiteralExpression>()) {
        return omitValues
            ? ToString("?")
            : LiteralValueToString(literalExpr->Value);
=======
    if (auto commaExpr = expr->As<TCommaExpression>()) {
        return InferName(commaExpr->Lhs.Get()) + ", " + InferName(commaExpr->Rhs.Get());
    } else if (auto literalExpr = expr->As<TLiteralExpression>()) {
        return LiteralValueToString(literalExpr->Value);
>>>>>>> prestable/0.17.3
    } else if (auto referenceExpr = expr->As<TReferenceExpression>()) {
        return FormatColumn(referenceExpr->ColumnName, referenceExpr->TableName);
    } else if (auto functionExpr = expr->As<TFunctionExpression>()) {
        Stroka result = functionExpr->FunctionName;
        result += "(";
        result += InferName(functionExpr->Arguments, omitValues);
        result += ")";
        return result;
    } else if (auto unaryExpr = expr->As<TUnaryOpExpression>()) {
        return Stroka(GetUnaryOpcodeLexeme(unaryExpr->Opcode)) + " " + InferName(unaryExpr->Operand);
    } else if (auto binaryExpr = expr->As<TBinaryOpExpression>()) {
        return
            InferName(binaryExpr->Lhs, omitValues) +
            " " + GetBinaryOpcodeLexeme(binaryExpr->Opcode) + " " +
            InferName(binaryExpr->Rhs, omitValues);
    } else if (auto inExpr = expr->As<TInExpression>()) {
        auto result = InferName(inExpr->Expr, omitValues);
        result += " in (";

        if (omitValues) {
            result += "??";
        } else {
            for (int i = 0; i < inExpr->Values.size(); ++i) {
                if (i) {
                    result += ", ";
                }
<<<<<<< HEAD
=======
                result += LiteralValueToString(inExpr->Values[i][j]);
            }
>>>>>>> prestable/0.17.3

                if (inExpr->Values[i].size() > 1) {
                    result += "(";
                }

                for (int j = 0; j < inExpr->Values[i].size(); ++j) {
                    if (j) {
                        result += ", ";
                    }
                    result += LiteralValueToString(inExpr->Values[i][j]);
                }

                if (inExpr->Values[i].size() > 1) {
                    result += ")";
                }
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
