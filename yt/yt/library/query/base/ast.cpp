#include "ast.h"

#include <library/cpp/yt/misc/variant.h>
#include <library/cpp/yt/misc/optional.h>

#include <util/string/escape.h>

namespace NYT::NQueryClient::NAst {

////////////////////////////////////////////////////////////////////////////////

bool operator == (TNullLiteralValue, TNullLiteralValue)
{
    return true;
}

////////////////////////////////////////////////////////////////////////////////

i64 TDoubleOrDotIntToken::AsDotInt() const
{
    if (Representation.empty() || Representation[0] != '.') {
        THROW_ERROR_EXCEPTION("Expected dot token and then integer token, but got %Qv",
            Representation);
    }

    return FromString<ui64>(Representation.substr(1));
}

double TDoubleOrDotIntToken::AsDouble() const
{
    return FromString<double>(Representation);
}

////////////////////////////////////////////////////////////////////////////////

bool TCompositeTypeMemberAccessor::IsEmpty() const
{
    return NestedStructOrTupleItemAccessor.empty() && !DictOrListItemAccessor.has_value();
}

size_t TReferenceHasher::operator() (const TReference& reference) const
{
    size_t result = 0;
    HashCombine(result, reference.ColumnName);
    HashCombine(result, reference.TableName);

    for (const auto& item : reference.CompositeTypeAccessor.NestedStructOrTupleItemAccessor) {
        Visit(item,
            [&] (const TStructMemberAccessor& structMember) {
                HashCombine(result, structMember);
            },
            [&] (const TTupleItemIndexAccessor& index) {
                HashCombine(result, index);
            });
    }

    if (reference.CompositeTypeAccessor.DictOrListItemAccessor) {
        if (std::ssize(*reference.CompositeTypeAccessor.DictOrListItemAccessor) != 1) {
            THROW_ERROR_EXCEPTION("Expression inside of the list or dict item accessor should be scalar")
                << TErrorAttribute("source", FormatReference(reference));
        }
        HashCombine(result, reference.CompositeTypeAccessor.DictOrListItemAccessor->front());
    }

    return result;
}

bool TReferenceEqComparer::operator() (const TReference& lhs, const TReference& rhs) const
{
    return lhs == rhs;
}

size_t TCompositeAgnosticReferenceHasher::operator() (const TReference& reference) const
{
    size_t result = 0;
    HashCombine(result, reference.ColumnName);
    HashCombine(result, reference.TableName);
    return result;
}

bool TCompositeAgnosticReferenceEqComparer::operator() (const TReference& lhs, const TReference& rhs) const
{
    return
        std::tie(lhs.ColumnName, lhs.TableName) ==
        std::tie(rhs.ColumnName, rhs.TableName);
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

bool operator == (const TIdentifierList& lhs, const TIdentifierList& rhs)
{
    return ExpressionListEqual(lhs, rhs);
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
    } else if (const auto* typedLhs = lhs.As<TBetweenExpression>()) {
        const auto* typedRhs = rhs.As<TBetweenExpression>();
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
    } else if (const auto* typedLhs = lhs.As<TCaseExpression>()) {
        const auto* typedRhs = rhs.As<TCaseExpression>();
        if (!typedRhs) {
            return false;
        }
        return
            typedLhs->OptionalOperand == typedRhs->OptionalOperand &&
            typedLhs->WhenThenExpressions == typedRhs->WhenThenExpressions &&
            typedLhs->DefaultExpression == typedRhs->DefaultExpression;
    } else if (const auto* typedLhs = lhs.As<TLikeExpression>()) {
        const auto* typedRhs = rhs.As<TLikeExpression>();
        if (!typedRhs) {
            return false;
        }
        return
            typedLhs->Text == typedRhs->Text &&
            typedLhs->Opcode == typedRhs->Opcode &&
            typedLhs->Pattern == typedRhs->Pattern &&
            typedLhs->EscapeCharacter == typedRhs->EscapeCharacter;
    } else {
        YT_ABORT();
    }
}

TStringBuf TExpression::GetSource(TStringBuf source) const
{
    auto begin = SourceLocation.first;
    auto end = SourceLocation.second;

    return source.substr(begin, end - begin);
}

TStringBuf GetSource(TSourceLocation sourceLocation, TStringBuf source)
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

bool operator == (const TJoin& lhs, const TJoin& rhs)
{
    return
        std::tie(lhs.IsLeft, lhs.Table, lhs.Fields, lhs.Lhs, lhs.Rhs, lhs.Predicate) ==
        std::tie(rhs.IsLeft, rhs.Table, rhs.Fields, rhs.Lhs, rhs.Rhs, rhs.Predicate);
}

bool operator == (const TQuery& lhs, const TQuery& rhs)
{
    return
        std::tie(
            lhs.Table,
            lhs.WithIndex,
            lhs.Joins,
            lhs.SelectExprs,
            lhs.WherePredicate,
            lhs.GroupExprs,
            lhs.HavingPredicate,
            lhs.OrderExpressions,
            lhs.Offset,
            lhs.Limit) ==
        std::tie(
            rhs.Table,
            rhs.WithIndex,
            rhs.Joins,
            rhs.SelectExprs,
            rhs.WherePredicate,
            rhs.GroupExprs,
            rhs.HavingPredicate,
            rhs.OrderExpressions,
            rhs.Offset,
            rhs.Limit);
}

void FormatLiteralValue(TStringBuilderBase* builder, const TLiteralValue& value)
{
    Visit(value,
        [&] (TNullLiteralValue) {
            builder->AppendString("null");
        },
        [&] (i64 value) {
            builder->AppendFormat("%v", value);
        },
        [&] (ui64 value) {
            builder->AppendFormat("%vu", value);
        },
        [&] (double value) {
            builder->AppendFormat("%v", value);
        },
        [&] (bool value) {
            builder->AppendFormat("%v", value ? "true" : "false");
        },
        [&] (const TString& value) {
            builder->AppendChar('"');
            builder->AppendString(EscapeC(value));
            builder->AppendChar('"');
        });
}

std::vector<TStringBuf> GetKeywords()
{
    std::vector<TStringBuf> result;

#define XX(keyword) result.push_back(#keyword);

    XX(from)
    XX(index)
    XX(where)
    XX(having)
    XX(offset)
    XX(limit)
    XX(array)
    XX(join)
    XX(using)
    XX(group)
    XX(by)
    XX(with)
    XX(totals)
    XX(order)
    XX(by)
    XX(asc)
    XX(desc)
    XX(left)
    XX(as)
    XX(on)
    XX(and)
    XX(or)
    XX(not)
    XX(null)
    XX(between)
    XX(in)
    XX(transform)
    XX(case)
    XX(when)
    XX(then)
    XX(else)
    XX(end)
    XX(like)
    XX(ilike)
    XX(rlike)
    XX(regexp)
    XX(escape)
    XX(false)
    XX(true)

#undef XX

    std::sort(result.begin(), result.end());

    return result;
}

bool IsKeyword(TStringBuf str)
{
    static auto keywords = GetKeywords();

    return std::binary_search(keywords.begin(), keywords.end(), str, [] (TStringBuf str, TStringBuf keyword) {
        return std::lexicographical_compare(
            str.begin(),
            str.end(),
            keyword.begin(),
            keyword.end(), [] (char a, char b) {
                return tolower(a) < tolower(b);
            });
    });
}

bool IsValidId(TStringBuf str)
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

    if (IsKeyword(str)) {
        return false;
    }

    return true;
}

TString EscapeBackticks(TStringBuf data)
{
    TStringBuilder builder;
    builder.Reserve(data.size());

    for (char ch : data) {
        if (ch == '`') {
            builder.AppendChar('\\');
            builder.AppendChar('`');
        } else {
            builder.AppendChar(ch);
        }
    }

    return builder.Flush();
}

void FormatId(TStringBuilderBase* builder, TStringBuf id, bool isFinal = false)
{
    if (isFinal || IsValidId(id)) {
        builder->AppendString(id);
    } else {
        builder->AppendChar('`');
        builder->AppendString(EscapeBackticks(EscapeC(id)));
        builder->AppendChar('`');
    }
}

void FormatExpressions(TStringBuilderBase* builder, const TExpressionList& exprs, bool expandAliases);
void FormatExpression(TStringBuilderBase* builder, const TExpression& expr, bool expandAliases, bool isFinal = false);
void FormatExpression(TStringBuilderBase* builder, const TExpressionList& expr, bool expandAliases);

void FormatReference(TStringBuilderBase* builder, const TReference& ref, bool isFinal = false)
{
    if (ref.TableName) {
        builder->AppendString(*ref.TableName);
        builder->AppendChar('.');
    }

    FormatId(builder, ref.ColumnName, isFinal);

    for (const auto& item : ref.CompositeTypeAccessor.NestedStructOrTupleItemAccessor) {
        Visit(item,
            [&] (const TStructMemberAccessor& structMember) {
                builder->AppendChar('.');
                builder->AppendString(structMember);
            },
            [&] (const TTupleItemIndexAccessor& index) {
                builder->AppendChar('.');
                builder->AppendFormat("%v", index);
            });
    }

    if (ref.CompositeTypeAccessor.DictOrListItemAccessor) {
        builder->AppendChar('[');
        FormatExpressions(builder, *ref.CompositeTypeAccessor.DictOrListItemAccessor, /*expandAliases*/ false);
        builder->AppendChar(']');
    }
}

void FormatTableDescriptor(TStringBuilderBase* builder, const TTableDescriptor& descriptor)
{
    FormatId(builder, descriptor.Path);
    if (descriptor.Alias) {
        builder->AppendString(" AS ");
        FormatId(builder, *descriptor.Alias);
    }
}

void FormatExpression(TStringBuilderBase* builder, const TExpression& expr, bool expandAliases, bool isFinal)
{
    auto printTuple = [] (TStringBuilderBase* builder, const TLiteralValueTuple& tuple) {
        bool needParens = tuple.size() > 1;
        if (needParens) {
            builder->AppendChar('(');
        }
        JoinToString(
            builder,
            tuple.begin(),
            tuple.end(),
            [] (TStringBuilderBase* builder, const TLiteralValue& value) {
                builder->AppendString(FormatLiteralValue(value));
            });
        if (needParens) {
            builder->AppendChar(')');
        }
    };

    auto printTuples = [&] (TStringBuilderBase* builder, const TLiteralValueTupleList& list) {
        JoinToString(
            builder,
            list.begin(),
            list.end(),
            printTuple);
    };

    auto printRanges = [&] (TStringBuilderBase* builder, const TLiteralValueRangeList& list) {
        JoinToString(
            builder,
            list.begin(),
            list.end(),
            [&] (TStringBuilderBase* builder, const std::pair<TLiteralValueTuple, TLiteralValueTuple>& range) {
                printTuple(builder, range.first);
                builder->AppendString(" AND ");
                printTuple(builder, range.second);
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
        FormatExpressions(builder, typedExpr->Operand, expandAliases);
        builder->AppendChar(')');
    } else if (auto* typedExpr = expr.As<TBinaryOpExpression>()) {
        builder->AppendChar('(');
        FormatExpressions(builder, typedExpr->Lhs, expandAliases);
        builder->AppendChar(')');
        builder->AppendString(GetBinaryOpcodeLexeme(typedExpr->Opcode));
        builder->AppendChar('(');
        FormatExpressions(builder, typedExpr->Rhs, expandAliases);
        builder->AppendChar(')');
    } else if (auto* typedExpr = expr.As<TInExpression>()) {
        builder->AppendChar('(');
        FormatExpressions(builder, typedExpr->Expr, expandAliases);
        builder->AppendString(") IN (");
        printTuples(builder, typedExpr->Values);
        builder->AppendChar(')');
    } else if (auto* typedExpr = expr.As<TBetweenExpression>()) {
        builder->AppendChar('(');
        FormatExpressions(builder, typedExpr->Expr, expandAliases);
        builder->AppendString(") BETWEEN (");
        printRanges(builder, typedExpr->Values);
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
        printTuples(builder, typedExpr->From);
        builder->AppendString("), (");
        printTuples(builder, typedExpr->To);
        builder->AppendChar(')');

        if (typedExpr->DefaultExpr) {
            builder->AppendString(", ");
            FormatExpression(builder, *typedExpr->DefaultExpr, expandAliases);
        }

        builder->AppendChar(')');
    } else if (auto* typedExpr = expr.As<TCaseExpression>()) {
        builder->AppendString("CASE");

        if (typedExpr->OptionalOperand) {
            builder->AppendChar(' ');
            FormatExpression(builder, *typedExpr->OptionalOperand, expandAliases);
        }

        for (const auto& item : typedExpr->WhenThenExpressions) {
            builder->AppendString(" WHEN ");
            FormatExpression(builder, item.first, expandAliases);
            builder->AppendString(" THEN ");
            FormatExpression(builder, item.second, expandAliases);
        }

        if (typedExpr->DefaultExpression) {
            builder->AppendString(" ELSE ");
            FormatExpression(builder, *typedExpr->DefaultExpression, expandAliases);
        }

        builder->AppendString(" END");
    } else if (auto* typedExpr = expr.As<TLikeExpression>()) {
        FormatExpressions(builder, typedExpr->Text, expandAliases);

        builder->AppendChar(' ');
        builder->AppendString(GetStringMatchOpcodeLexeme(typedExpr->Opcode));
        builder->AppendChar(' ');

        FormatExpressions(builder, typedExpr->Pattern, expandAliases);

        if (typedExpr->EscapeCharacter) {
            builder->AppendString(" ESCAPE ");
            FormatExpressions(builder, *typedExpr->EscapeCharacter, expandAliases);
        }
    } else {
        YT_ABORT();
    }
}

void FormatExpression(TStringBuilderBase* builder, const TExpressionList& exprs, bool expandAliases)
{
    YT_VERIFY(exprs.size() > 0);
    if (exprs.size() > 1) {
        builder->AppendChar('(');
    }
    FormatExpressions(builder, exprs, expandAliases);
    if (exprs.size() > 1) {
        builder->AppendChar(')');
    }
}

void FormatExpressions(TStringBuilderBase* builder, const TExpressionList& exprs, bool expandAliases)
{
    JoinToString(
        builder,
        exprs.begin(),
        exprs.end(),
        [&] (TStringBuilderBase* builder, const TExpressionPtr& expr) {
            FormatExpression(builder, *expr, expandAliases);
        });
}

void FormatJoin(TStringBuilderBase* builder, const TJoin& join)
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
            [] (TStringBuilderBase* builder, const TReferenceExpressionPtr& referenceExpr) {
                FormatReference(builder, referenceExpr->Reference);
            });
    }
    if (join.Predicate) {
        builder->AppendString(" AND ");
        FormatExpression(builder, *join.Predicate, true);
    }
}

void FormatArrayJoin(TStringBuilderBase* builder, const TArrayJoin& join)
{
    if (join.IsLeft) {
        builder->AppendString(" LEFT");
    }
    builder->AppendString(" ARRAY JOIN ");
    JoinToString(
        builder,
        join.Columns.begin(),
        join.Columns.end(),
        [] (TStringBuilderBase* builder, const TExpressionPtr& expr) {
            auto* alias = expr->As<TAliasExpression>();
            YT_VERIFY(alias);
            FormatExpression(builder, *alias->Expression, /*expandAliases*/ true);
            builder->AppendString(" AS ");
            builder->AppendString(alias->Name);
        });
    if (join.Predicate) {
        builder->AppendString(" AND ");
        FormatExpression(builder, *join.Predicate, true);
    }
}

void FormatQuery(TStringBuilderBase* builder, const TQuery& query)
{
    if (query.SelectExprs) {
        JoinToString(
            builder,
            query.SelectExprs->begin(),
            query.SelectExprs->end(),
            [] (TStringBuilderBase* builder, const TExpressionPtr& expr) {
                FormatExpression(builder, *expr, true);
            });
    } else {
        builder->AppendString("*");
    }

    builder->AppendString(" FROM ");
    FormatTableDescriptor(builder, query.Table);

    if (query.WithIndex) {
        builder->AppendString(" WITH INDEX ");
        FormatTableDescriptor(builder, *query.WithIndex);
    }

    for (const auto& join : query.Joins) {
        Visit(join,
            [&] (const TJoin& tableJoin) {
                FormatJoin(builder, tableJoin);
            },
            [&] (const TArrayJoin& arrayJoin) {
                FormatArrayJoin(builder, arrayJoin);
            });
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
            [] (TStringBuilderBase* builder, const std::pair<TExpressionList, bool>& pair) {
                FormatExpression(builder, pair.first, true);
                if (pair.second) {
                    builder->AppendString(" DESC");
                }
            });
    }

    if (query.Offset) {
        builder->AppendFormat(" OFFSET %v", *query.Offset);
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

TString FormatId(TStringBuf id)
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
    TStringBuilder builder;
    FormatExpression(&builder, exprs, true);
    return builder.Flush();
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

} // namespace NYT::NQueryClient::NAst
