#pragma once

#include "public.h"

#include "plan_fragment_common.h"

#include <core/misc/variant.h>

namespace NYT {
namespace NQueryClient {
namespace NAst {

////////////////////////////////////////////////////////////////////////////////

typedef TVariant<i64, ui64, double, bool, Stroka> TLiteralValue;
typedef std::vector<TLiteralValue> TLiteralValueList;
typedef std::vector<std::vector<TLiteralValue>> TLiteralValueTupleList;

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

    Stroka ColumnName;
    Stroka TableName;
};

DECLARE_REFCOUNTED_STRUCT(TReferenceExpression)
DEFINE_REFCOUNTED_TYPE(TReferenceExpression)

struct TCommaExpression
    : public TExpression
{
    TCommaExpression(
        const TSourceLocation& sourceLocation,
        TExpressionPtr lhs,
        TExpressionPtr rhs)
        : TExpression(sourceLocation)
        , Lhs(std::move(lhs))
        , Rhs(std::move(rhs))
    { }

    TExpressionPtr Lhs;
    TExpressionPtr Rhs;
};

struct TFunctionExpression
    : public TExpression
{
    TFunctionExpression(
        const TSourceLocation& sourceLocation,
        const TStringBuf& functionName,
        TExpressionPtr arguments)
        : TExpression(sourceLocation)
        , FunctionName(functionName)
        , Arguments(std::move(arguments))
    { }

    Stroka FunctionName;
    TExpressionPtr Arguments;
};

struct TEmptyExpression
    : public TExpression
{
    TEmptyExpression(
        const TSourceLocation& sourceLocation)
        : TExpression(sourceLocation)
    { }
};

struct TUnaryOpExpression
    : public TExpression
{
    TUnaryOpExpression(
        const TSourceLocation& sourceLocation,
        EUnaryOp opcode,
        TExpressionPtr operand)
        : TExpression(sourceLocation)
        , Opcode(opcode)
        , Operand(std::move(operand))
    { }

    EUnaryOp Opcode;
    TExpressionPtr Operand;
};

struct TBinaryOpExpression
    : public TExpression
{
    TBinaryOpExpression(
        const TSourceLocation& sourceLocation,
        EBinaryOp opcode,
        TExpressionPtr lhs,
        TExpressionPtr rhs)
        : TExpression(sourceLocation)
        , Opcode(opcode)
        , Lhs(std::move(lhs))
        , Rhs(std::move(rhs))
    { }

    EBinaryOp Opcode;
    TExpressionPtr Lhs;
    TExpressionPtr Rhs;
};

struct TInExpression
    : public TExpression
{
    TInExpression(
        const TSourceLocation& sourceLocation,
        TExpressionPtr expression,
        const TLiteralValueTupleList& values)
        : TExpression(sourceLocation)
        , Expr(std::move(expression))
        , Values(values)
    { }

    TExpressionPtr Expr;
    TLiteralValueTupleList Values;
};

Stroka InferName(const TExpression* expr);

////////////////////////////////////////////////////////////////////////////////

typedef std::pair<TExpressionPtr, Stroka> TNamedExpression;
typedef std::vector<TNamedExpression> TNamedExpressionList;
typedef TNullable<TNamedExpressionList> TNullableNamedExpressionList;

typedef std::vector<TReferenceExpressionPtr> TIdentifierList;
typedef TNullable<TIdentifierList> TNullableIdentifierList;

struct TTableDescriptor
{
    TTableDescriptor()
    { }

    TTableDescriptor(
        const Stroka& path,
        const Stroka& alias)
        : Path(path)
        , Alias(alias)
    { }

    Stroka Path;
    Stroka Alias;
};

struct TQuery
{
    TTableDescriptor Table;

    struct TJoin
    {
        TJoin(
            const TTableDescriptor& table,
            const TIdentifierList& fields)
            : Table(table)
            , Fields(fields)
        { }

        TJoin(
            const TTableDescriptor& table,
            const TExpressionPtr& left,
            const TExpressionPtr& right)
            : Table(table)
            , Left(left)
            , Right(right)
        { }

        TTableDescriptor Table;
        TIdentifierList Fields;

        TExpressionPtr Left;
        TExpressionPtr Right;
    };

    std::vector<TJoin> Joins;

    TNullableNamedExpressionList SelectExprs;
    TExpressionPtr WherePredicate;
    TNullableNamedExpressionList GroupExprs;
    TExpressionPtr HavingPredicate;

    TNullableIdentifierList OrderFields;
    bool IsDescendingOrder = false;

    i64 Limit = 0;
};

typedef TVariant<TQuery, TExpressionPtr> TAstHead;

////////////////////////////////////////////////////////////////////////////////

} // namespace NAst
} // namespace NQueryClient
} // namespace NYT
