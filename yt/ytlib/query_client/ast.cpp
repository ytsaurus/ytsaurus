#include "ast.h"

namespace NYT {
namespace NQueryClient {
namespace NAst {

////////////////////////////////////////////////////////////////////////////////

TString LiteralValueToString(const TLiteralValue& literalValue)
{
    switch (literalValue.Tag()) {
        case NAst::TLiteralValue::TagOf<NAst::TNullLiteralValue>():
            return "null";
        case NAst::TLiteralValue::TagOf<i64>():
            return ToString(literalValue.As<i64>());
        case NAst::TLiteralValue::TagOf<ui64>():
            return ToString(literalValue.As<ui64>());
        case NAst::TLiteralValue::TagOf<double>():
            return ToString(literalValue.As<double>());
        case NAst::TLiteralValue::TagOf<bool>():
            return ToString(literalValue.As<bool>());
        case NAst::TLiteralValue::TagOf<TString>():
            return literalValue.As<TString>().Quote();
        default:
            Y_UNREACHABLE();
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


TString InferName(const TExpressionList& exprs, bool omitValues)
{
    auto canOmitParenthesis = [] (const TExpressionPtr& expr) {
        return
            expr->As<TLiteralExpression>() ||
            expr->As<TReferenceExpression>() ||
            expr->As<TFunctionExpression>();
    };

    return JoinToString(
        exprs,
        [&] (TStringBuilder* builder, const TExpressionPtr& expr) {
            auto name = InferName(expr.Get(), omitValues);
            if (canOmitParenthesis(expr)) {
                builder->AppendString(name);
            } else {
                builder->AppendChar('(');
                builder->AppendString(name);
                builder->AppendChar(')');
            }
        });
}

TString FormatColumn(const TStringBuf& name, const TStringBuf& tableName)
{
    return tableName.empty()
        ? TString(name)
        : Format("%v.%v", tableName, name);
}

TString InferName(const TExpression* expr, bool omitValues)
{
    if (auto literalExpr = expr->As<TLiteralExpression>()) {
        return omitValues
            ? ToString("?")
            : LiteralValueToString(literalExpr->Value);
    } else if (auto referenceExpr = expr->As<TReferenceExpression>()) {
        return FormatColumn(referenceExpr->ColumnName, referenceExpr->TableName);
    } else if (auto functionExpr = expr->As<TFunctionExpression>()) {
        TString result = functionExpr->FunctionName;
        result += "(";
        result += InferName(functionExpr->Arguments, omitValues);
        result += ")";
        return result;
    } else if (auto unaryExpr = expr->As<TUnaryOpExpression>()) {
        return TString(GetUnaryOpcodeLexeme(unaryExpr->Opcode)) + " " + InferName(unaryExpr->Operand);
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
        Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NAst
} // namespace NQueryClient
} // namespace NYT
