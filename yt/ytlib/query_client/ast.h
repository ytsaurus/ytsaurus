#pragma once

#include "public.h"
#include "query_common.h"

#include <yt/ytlib/ypath/rich.h>

#include <yt/core/misc/variant.h>

namespace NYT {
namespace NQueryClient {
namespace NAst {

////////////////////////////////////////////////////////////////////////////////

struct TNullLiteralValue {};
typedef TVariant<TNullLiteralValue, i64, ui64, double, bool, TString> TLiteralValue;
typedef std::vector<TLiteralValue> TLiteralValueList;
typedef std::vector<std::vector<TLiteralValue>> TLiteralValueTupleList;

TStringBuf GetSource(TSourceLocation sourceLocation, const TStringBuf& source);

struct TExpression
    : public TIntrinsicRefCounted
{
    explicit TExpression(const TSourceLocation& sourceLocation)
        : SourceLocation(sourceLocation)
    { }

    template <class TDerived>
    const TDerived* As() const
    {
        return dynamic_cast<const TDerived*>(this);
    }

    template <class TDerived>
    TDerived* As()
    {
        return dynamic_cast<TDerived*>(this);
    }

    TStringBuf GetSource(const TStringBuf& source) const;

    TSourceLocation SourceLocation;
};

DECLARE_REFCOUNTED_STRUCT(TExpression)
DEFINE_REFCOUNTED_TYPE(TExpression)

typedef std::vector<TExpressionPtr> TExpressionList;
typedef TNullable<TExpressionList> TNullableExpressionList;

template <class T, class... TArgs>
TExpressionList MakeExpr(TArgs&&... args)
{
    return TExpressionList(1, New<T>(args...));
}

struct TLiteralExpression
    : public TExpression
{
    TLiteralExpression(
        const TSourceLocation& sourceLocation,
        TLiteralValue value)
        : TExpression(sourceLocation)
        , Value(std::move(value))
    { }

    TLiteralValue Value;
};

struct TReferenceExpression
    : public TExpression
{
    TReferenceExpression(
        const TSourceLocation& sourceLocation,
        TStringBuf columnName,
        TStringBuf tableName = TStringBuf())
        : TExpression(sourceLocation)
        , ColumnName(columnName)
        , TableName(tableName)
    { }

    TString ColumnName;
    TString TableName;
};

DECLARE_REFCOUNTED_STRUCT(TReferenceExpression)
DEFINE_REFCOUNTED_TYPE(TReferenceExpression)

struct TFunctionExpression
    : public TExpression
{
    TFunctionExpression(
        const TSourceLocation& sourceLocation,
        const TStringBuf& functionName,
        TExpressionList arguments)
        : TExpression(sourceLocation)
        , FunctionName(functionName)
        , Arguments(std::move(arguments))
    { }

    TString FunctionName;
    TExpressionList Arguments;
};

struct TUnaryOpExpression
    : public TExpression
{
    TUnaryOpExpression(
        const TSourceLocation& sourceLocation,
        EUnaryOp opcode,
        TExpressionList operand)
        : TExpression(sourceLocation)
        , Opcode(opcode)
        , Operand(std::move(operand))
    { }

    EUnaryOp Opcode;
    TExpressionList Operand;
};

struct TBinaryOpExpression
    : public TExpression
{
    TBinaryOpExpression(
        const TSourceLocation& sourceLocation,
        EBinaryOp opcode,
        TExpressionList lhs,
        TExpressionList rhs)
        : TExpression(sourceLocation)
        , Opcode(opcode)
        , Lhs(std::move(lhs))
        , Rhs(std::move(rhs))
    { }

    EBinaryOp Opcode;
    TExpressionList Lhs;
    TExpressionList Rhs;
};

struct TInExpression
    : public TExpression
{
    TInExpression(
        const TSourceLocation& sourceLocation,
        TExpressionList expression,
        const TLiteralValueTupleList& values)
        : TExpression(sourceLocation)
        , Expr(std::move(expression))
        , Values(values)
    { }

    TExpressionList Expr;
    TLiteralValueTupleList Values;
};

TString FormatColumn(const TStringBuf& name, const TStringBuf& tableName = TStringBuf());
TString InferName(const TExpressionList& exprs, bool omitValues = false);
TString InferName(const TExpression* expr, bool omitValues = false);

////////////////////////////////////////////////////////////////////////////////

typedef std::vector<TReferenceExpressionPtr> TIdentifierList;
typedef TNullable<TIdentifierList> TNullableIdentifierList;

typedef std::vector<std::pair<TExpressionList, bool>> TOrderExpressionList;

struct TTableDescriptor
{
    TTableDescriptor()
    { }

    TTableDescriptor(
        const TString& path,
        const TString& alias)
        : Path(NYPath::TRichYPath::Parse(path))
        , Alias(alias)
    { }

    NYPath::TRichYPath Path;
    TString Alias;
};

struct TQuery
{
    TTableDescriptor Table;

    struct TJoin
    {
        TJoin(
            bool isLeft,
            const TTableDescriptor& table,
            const TIdentifierList& fields,
            const TNullableExpressionList& predicate)
            : IsLeft(isLeft)
            , Table(table)
            , Fields(fields)
            , Predicate(predicate)
        { }

        TJoin(
            bool isLeft,
            const TTableDescriptor& table,
            const TExpressionList& left,
            const TExpressionList& right,
            const TNullableExpressionList& predicate)
            : IsLeft(isLeft)
            , Table(table)
            , Left(left)
            , Right(right)
            , Predicate(predicate)
        { }

        bool IsLeft;
        TTableDescriptor Table;
        TIdentifierList Fields;

        TExpressionList Left;
        TExpressionList Right;

        TNullableExpressionList Predicate;
    };

    std::vector<TJoin> Joins;

    TNullableExpressionList SelectExprs;
    TNullableExpressionList WherePredicate;
    TNullable<std::pair<TExpressionList, ETotalsMode>> GroupExprs;
    TNullableExpressionList HavingPredicate;

    TOrderExpressionList OrderExpressions;

    i64 Limit = 0;
};

typedef yhash<TString, TExpressionPtr> TAliasMap;

typedef std::pair<TVariant<TQuery, TExpressionPtr>, TAliasMap> TAstHead;

////////////////////////////////////////////////////////////////////////////////

} // namespace NAst
} // namespace NQueryClient
} // namespace NYT
