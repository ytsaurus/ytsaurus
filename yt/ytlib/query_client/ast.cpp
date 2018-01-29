#include "ast.h"

#include <util/string/escape.h>

namespace NYT {
namespace NQueryClient {
namespace NAst {

////////////////////////////////////////////////////////////////////////////////

bool operator==(TNullLiteralValue, TNullLiteralValue)
{
    return true;
}

bool operator!=(TNullLiteralValue, TNullLiteralValue)
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

TReference::operator size_t() const
{
    size_t result = 0;
    HashCombine(result, ColumnName);
    HashCombine(result, TableName);
    return result;
}

bool operator == (const TReference& lhs, const TReference& rhs)
{
    return
        std::tie(lhs.ColumnName, lhs.TableName) ==
        std::tie(rhs.ColumnName, rhs.TableName);
}

bool operator != (const TReference& lhs, const TReference& rhs)
{
    return !(lhs == rhs);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
bool ExpressionListEqual(const T& lhs, const T& rhs)
{
    if (lhs.size() != rhs.size()) {
        return false;
    }
    for (size_t index = 0; index < lhs.size(); ++index) {
        if (*lhs[index] != *rhs[index]) {
            return false;
        }
    }
    return true;
}

bool operator == (const TExpressionList& lhs, const TExpressionList& rhs)
{
    return ExpressionListEqual(lhs, rhs);
}

bool operator != (const TExpressionList& lhs, const TExpressionList& rhs)
{
    return !(lhs == rhs);
}

bool operator == (const TIdentifierList& lhs, const TIdentifierList& rhs)
{
    return ExpressionListEqual(lhs, rhs);
}

bool operator != (const TIdentifierList& lhs, const TIdentifierList& rhs)
{
    return !(lhs == rhs);
}

bool operator == (const TExpression& lhs, const TExpression& rhs)
{
    if (const auto* typedLhs = lhs.As<TLiteralExpression>()) {
        const auto* typedRhs = rhs.As<TLiteralExpression>();
        if (!typedRhs) {
            return false;
        }
        return typedLhs->Value == typedRhs->Value;
    } else if (const auto* typedLhs = lhs.As<TReferenceExpression>()) {
        const auto* typedRhs = rhs.As<TReferenceExpression>();
        if (!typedRhs) {
            return false;
        }
        return typedLhs->Reference == typedRhs->Reference;
    } else if (const auto* typedLhs = lhs.As<TAliasExpression>()) {
        const auto* typedRhs = rhs.As<TAliasExpression>();
        if (!typedRhs) {
            return false;
        }
        return
            typedLhs->Name == typedRhs->Name &&
            *typedLhs->Expression == *typedRhs->Expression;
    } else if (const auto* typedLhs = lhs.As<TFunctionExpression>()) {
        const auto* typedRhs = rhs.As<TFunctionExpression>();
        if (!typedRhs) {
            return false;
        }
        return
            typedLhs->FunctionName == typedRhs->FunctionName &&
            typedLhs->Arguments == typedRhs->Arguments;
    } else if (const auto* typedLhs = lhs.As<TUnaryOpExpression>()) {
        const auto* typedRhs = rhs.As<TUnaryOpExpression>();
        if (!typedRhs) {
            return false;
        }
        return
            typedLhs->Opcode == typedRhs->Opcode &&
            typedLhs->Operand == typedRhs->Operand;
    } else if (const auto* typedLhs = lhs.As<TBinaryOpExpression>()) {
        const auto* typedRhs = rhs.As<TBinaryOpExpression>();
        if (!typedRhs) {
            return false;
        }
        return
            typedLhs->Opcode == typedRhs->Opcode &&
            typedLhs->Lhs == typedRhs->Lhs &&
            typedLhs->Rhs == typedRhs->Rhs;
    } else if (const auto* typedLhs = lhs.As<TInExpression>()) {
        const auto* typedRhs = rhs.As<TInExpression>();
        if (!typedRhs) {
            return false;
        }
        return
            typedLhs->Expr == typedRhs->Expr &&
            typedLhs->Values == typedRhs->Values;
    } else if (const auto* typedLhs = lhs.As<TTransformExpression>()) {
        const auto* typedRhs = rhs.As<TTransformExpression>();
        if (!typedRhs) {
            return false;
        }
        return
            typedLhs->Expr == typedRhs->Expr &&
            typedLhs->From == typedRhs->From &&
            typedLhs->To == typedRhs->To &&
            typedLhs->DefaultExpr == typedRhs->DefaultExpr;
    } else {
        Y_UNREACHABLE();
    }
}

bool operator != (const TExpression& lhs, const TExpression& rhs)
{
    return !(lhs == rhs);
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

bool operator == (const TTableDescriptor& lhs, const TTableDescriptor& rhs)
{
    return
        std::tie(lhs.Path, rhs.Alias) ==
        std::tie(rhs.Path, rhs.Alias);
}

bool operator != (const TTableDescriptor& lhs, const TTableDescriptor& rhs)
{
    return !(lhs == rhs);
}

bool operator == (const TJoin& lhs, const TJoin& rhs)
{
    return
        std::tie(lhs.IsLeft, lhs.Table, lhs.Fields, lhs.Lhs, lhs.Rhs, lhs.Predicate) ==
        std::tie(rhs.IsLeft, rhs.Table, rhs.Fields, rhs.Lhs, rhs.Rhs, rhs.Predicate);
}

bool operator != (const TJoin& lhs, const TJoin& rhs)
{
    return !(lhs == rhs);
}

bool operator == (const TQuery& lhs, const TQuery& rhs)
{
    return
        std::tie(lhs.Table, lhs.Joins, lhs.SelectExprs, lhs.WherePredicate, lhs.GroupExprs, lhs.HavingPredicate, lhs.OrderExpressions, lhs.Limit) ==
        std::tie(rhs.Table, rhs.Joins, rhs.SelectExprs, rhs.WherePredicate, rhs.GroupExprs, rhs.HavingPredicate, rhs.OrderExpressions, rhs.Limit);
}

bool operator != (const TQuery& lhs, const TQuery& rhs)
{
    return !(lhs == rhs);
}

void FormatLiteralValue(TStringBuilder* builder, const TLiteralValue& value)
{
    switch (value.Tag()) {
        case TLiteralValue::TagOf<TNullLiteralValue>():
            builder->AppendString("null");
            break;
        case TLiteralValue::TagOf<i64>():
            builder->AppendFormat("%v", value.As<i64>());
            break;
        case TLiteralValue::TagOf<ui64>():
            builder->AppendFormat("%vu", value.As<ui64>());
            break;
        case TLiteralValue::TagOf<double>():
            builder->AppendFormat("%v", value.As<double>());
            break;
        case TLiteralValue::TagOf<bool>():
            builder->AppendFormat("%v", value.As<bool>() ? "true" : "false");
            break;
        case TLiteralValue::TagOf<TString>():
            builder->AppendChar('"');
            builder->AppendString(EscapeC(value.As<TString>()));
            builder->AppendChar('"');
            break;
        default:
            Y_UNREACHABLE();
    }
}

bool IsValidId(const TStringBuf& str)
{
    if (str.empty()) {
        return false;
    }

    auto isNum = [] (char ch) {
        return
            ch >= '0' && ch <= '9';
    };

    auto isAlpha = [] (char ch) {
        return
            ch >= 'a' && ch <= 'z' ||
            ch >= 'A' && ch <= 'Z' ||
            ch == '_';
    };

    if (!isAlpha(str[0])) {
        return false;
    }

    for (size_t index = 1; index < str.length(); ++index) {
        char ch = str[index];
        if (!isAlpha(ch) && !isNum(ch)) {
            return false;
        }
    }

    return true;
}

void FormatId(TStringBuilder* builder, const TStringBuf& id, bool isFinal = false)
{
    if (isFinal || IsValidId(id)) {
        builder->AppendString(id);
    } else {
        // TODO(babenko): escaping
        builder->AppendChar('[');
        builder->AppendString(id);
        builder->AppendChar(']');
    }
}

void FormatReference(TStringBuilder* builder, const TReference& ref, bool isFinal = false)
{
    if (ref.TableName) {
        builder->AppendString(*ref.TableName);
        builder->AppendChar('.');
    }
    FormatId(builder, ref.ColumnName, isFinal);
}

void FormatTableDescriptor(TStringBuilder* builder, const TTableDescriptor& descriptor)
{
    FormatId(builder, descriptor.Path);
    if (descriptor.Alias) {
        builder->AppendString(" AS ");
        FormatId(builder, *descriptor.Alias);
    }
}

void FormatExpressions(TStringBuilder* builder, const TExpressionList& exprs, bool expandAliases);
void FormatExpression(TStringBuilder* builder, const TExpression& expr, bool expandAliases, bool isFinal = false);
void FormatExpression(TStringBuilder* builder, const TExpressionList& expr, bool expandAliases);

void FormatExpression(TStringBuilder* builder, const TExpression& expr, bool expandAliases, bool isFinal)
{
    auto printValues = [] (TStringBuilder* builder, const TLiteralValueTupleList& list) {
        JoinToString(
            builder,
            list.begin(),
            list.end(),
            [] (TStringBuilder* builder, const TLiteralValueTuple& tuple) {
                bool needParens = tuple.size() > 1;
                if (needParens) {
                    builder->AppendChar('(');
                }
                JoinToString(
                    builder,
                    tuple.begin(),
                    tuple.end(),
                    [] (TStringBuilder* builder, const TLiteralValue& value) {
                        builder->AppendString(FormatLiteralValue(value));
                    });
                if (needParens) {
                    builder->AppendChar(')');
                }
            });
    };

    if (auto* typedExpr = expr.As<TLiteralExpression>()) {
        builder->AppendString(FormatLiteralValue(typedExpr->Value));
    } else if (auto* typedExpr = expr.As<TReferenceExpression>()) {
        FormatReference(builder, typedExpr->Reference, isFinal);
    } else if (auto* typedExpr = expr.As<TAliasExpression>()) {
        if (expandAliases) {
            builder->AppendChar('(');
            FormatExpression(builder, *typedExpr->Expression, expandAliases);
            builder->AppendString(" as ");
            FormatId(builder, typedExpr->Name, isFinal);
            builder->AppendChar(')');
        } else {
            FormatId(builder, typedExpr->Name, isFinal);
        }
    } else if (auto* typedExpr = expr.As<TFunctionExpression>()) {
        builder->AppendString(typedExpr->FunctionName);
        builder->AppendChar('(');
        FormatExpressions(builder, typedExpr->Arguments, expandAliases);
        builder->AppendChar(')');
    } else if (auto* typedExpr = expr.As<TUnaryOpExpression>()) {
        builder->AppendString(GetUnaryOpcodeLexeme(typedExpr->Opcode));
        builder->AppendChar('(');
        FormatExpression(builder, typedExpr->Operand, expandAliases);
        builder->AppendChar(')');
    } else if (auto* typedExpr = expr.As<TBinaryOpExpression>()) {
        builder->AppendChar('(');
        FormatExpression(builder, typedExpr->Lhs, expandAliases);
        builder->AppendChar(')');
        builder->AppendString(GetBinaryOpcodeLexeme(typedExpr->Opcode));
        builder->AppendChar('(');
        FormatExpression(builder, typedExpr->Rhs, expandAliases);
        builder->AppendChar(')');
    } else if (auto* typedExpr = expr.As<TInExpression>()) {
        builder->AppendChar('(');
        FormatExpressions(builder, typedExpr->Expr, expandAliases);
        builder->AppendString(") IN (");
        printValues(builder, typedExpr->Values);
        builder->AppendChar(')');
    } else if (auto* typedExpr = expr.As<TTransformExpression>()) {
        builder->AppendString("TRANSFORM(");
        size_t argumentCount = typedExpr->Expr.size();
        auto needParenthesis = argumentCount > 1;
        if (needParenthesis) {
            builder->AppendChar('(');
        }
        FormatExpressions(builder, typedExpr->Expr, expandAliases);
        if (needParenthesis) {
            builder->AppendChar(')');
        }
        builder->AppendString(", (");
        printValues(builder, typedExpr->From);
        builder->AppendString("), (");
        printValues(builder, typedExpr->To);
        builder->AppendChar(')');

        if (typedExpr->DefaultExpr) {
            builder->AppendString(", ");
            FormatExpression(builder, *typedExpr->DefaultExpr, expandAliases);
        }

        builder->AppendChar(')');
    } else {
        Y_UNREACHABLE();
    }
}

void FormatExpression(TStringBuilder* builder, const TExpressionList& exprs, bool expandAliases)
{
    YCHECK(exprs.size() == 1);
    FormatExpression(builder, *exprs[0], expandAliases);
}

void FormatExpressions(TStringBuilder* builder, const TExpressionList& exprs, bool expandAliases)
{
    JoinToString(
        builder,
        exprs.begin(),
        exprs.end(),
        [&] (TStringBuilder* builder, const TExpressionPtr& expr) {
            FormatExpression(builder, *expr, expandAliases);
        });
}

void FormatJoin(TStringBuilder* builder, const TJoin& join)
{
    if (join.IsLeft) {
        builder->AppendString(" LEFT");
    }
    builder->AppendString(" JOIN ");
    FormatTableDescriptor(builder, join.Table);
    if (join.Fields.empty()) {
        builder->AppendString(" ON (");
        FormatExpressions(builder, join.Lhs, true);
        builder->AppendString(") = (");
        FormatExpressions(builder, join.Rhs, true);
        builder->AppendChar(')');
    } else {
        builder->AppendString(" USING ");
        JoinToString(
            builder,
            join.Fields.begin(),
            join.Fields.end(),
            [] (TStringBuilder* builder, const TReferenceExpressionPtr& referenceExpr) {
                 FormatReference(builder, referenceExpr->Reference);
            });
    }
    if (join.Predicate) {
        builder->AppendString(" AND ");
        FormatExpression(builder, *join.Predicate, true);
    }
}

void FormatQuery(TStringBuilder* builder, const TQuery& query)
{
    if (query.SelectExprs) {
        JoinToString(
            builder,
            query.SelectExprs->begin(),
            query.SelectExprs->end(),
            [] (TStringBuilder* builder, const TExpressionPtr& expr) {
                FormatExpression(builder, *expr, true);
            });
    } else {
        builder->AppendString("*");
    }

    builder->AppendString(" FROM ");
    FormatTableDescriptor(builder, query.Table);

    for (const auto& join : query.Joins) {
        FormatJoin(builder, join);
    }

    if (query.WherePredicate) {
        builder->AppendString(" WHERE ");
        FormatExpression(builder, *query.WherePredicate, true);
    }

    if (query.GroupExprs) {
        builder->AppendString(" GROUP BY ");
        FormatExpressions(builder, query.GroupExprs->first, true);
        if (query.GroupExprs->second == ETotalsMode::BeforeHaving) {
            builder->AppendString(" WITH TOTALS");
        }
    }

    if (query.HavingPredicate) {
        builder->AppendString(" HAVING ");
        FormatExpression(builder, *query.HavingPredicate, true);
    }

    if (query.GroupExprs && query.GroupExprs->second == ETotalsMode::AfterHaving) {
        builder->AppendString(" WITH TOTALS");
    }

    if (!query.OrderExpressions.empty()) {
        builder->AppendString(" ORDER BY ");
        JoinToString(
            builder,
            query.OrderExpressions.begin(),
            query.OrderExpressions.end(),
            [] (TStringBuilder* builder, const std::pair<TExpressionList, bool>& pair) {
                FormatExpression(builder, pair.first, true);
                if (pair.second) {
                    builder->AppendString(" DESC");
                }
            });
    }

    if (query.Limit) {
        builder->AppendFormat(" LIMIT %v", *query.Limit);
    }
}

TString FormatLiteralValue(const TLiteralValue& value)
{
    TStringBuilder builder;
    FormatLiteralValue(&builder, value);
    return builder.Flush();
}

TString FormatId(const TStringBuf& id)
{
    TStringBuilder builder;
    FormatId(&builder, id);
    return builder.Flush();
}

TString FormatReference(const TReference& ref)
{
    TStringBuilder builder;
    FormatReference(&builder, ref);
    return builder.Flush();
}

TString FormatExpression(const TExpression& expr)
{
    TStringBuilder builder;
    FormatExpression(&builder, expr, true);
    return builder.Flush();
}

TString FormatExpression(const TExpressionList& exprs)
{
    YCHECK(exprs.size() == 1);
    return FormatExpression(*exprs[0]);
}

TString FormatJoin(const TJoin& join)
{
    TStringBuilder builder;
    FormatJoin(&builder, join);
    return builder.Flush();
}

TString FormatQuery(const TQuery& query)
{
    TStringBuilder builder;
    FormatQuery(&builder, query);
    return builder.Flush();
}

TString InferColumnName(const TExpression& expr)
{
    TStringBuilder builder;
    FormatExpression(&builder, expr, false, true);
    return builder.Flush();
}

TString InferColumnName(const TReference& ref)
{
    TStringBuilder builder;
    FormatReference(&builder, ref, true);
    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NAst
} // namespace NQueryClient
} // namespace NYT
