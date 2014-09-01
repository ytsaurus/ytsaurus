#pragma once

#include "public.h"

#include "plan_fragment_common.h"

#include <ytlib/new_table_client/unversioned_row.h>

#include <core/misc/array_ref.h>

namespace NYT {
namespace NQueryClient {
namespace NAst {

////////////////////////////////////////////////////////////////////////////////

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

struct TLiteralExpression
    : public TExpression
{
    TLiteralExpression(
        const TSourceLocation& sourceLocation,
        TValue value)
        : TExpression(sourceLocation)
        , Value(value)
    { }

    TValue Value;

};

struct TReferenceExpression
    : public TExpression
{
    TReferenceExpression(
        const TSourceLocation& sourceLocation,
        TStringBuf columnName)
        : TExpression(sourceLocation)
        , ColumnName(columnName)
    { }

    Stroka ColumnName;

};

struct TFunctionExpression
    : public TExpression
{
    typedef std::vector<TExpressionPtr> TArguments;

    TFunctionExpression(
        const TSourceLocation& sourceLocation,
        const TStringBuf& functionName,
        const TArguments& arguments)
        : TExpression(sourceLocation)
        , FunctionName(functionName)
        , Arguments(arguments)
    { }

    Stroka FunctionName;
    TArguments Arguments;

};

struct TBinaryOpExpression
    : public TExpression
{
    TBinaryOpExpression(
        const TSourceLocation& sourceLocation,
        EBinaryOp opcode,
        const TExpressionPtr& lhs,
        const TExpressionPtr& rhs)
        : TExpression(sourceLocation)
        , Opcode(opcode)
        , Lhs(lhs)
        , Rhs(rhs)
    { }

    EBinaryOp Opcode;
    TExpressionPtr Lhs;
    TExpressionPtr Rhs;

};

Stroka InferName(const TExpression* expr);

////////////////////////////////////////////////////////////////////////////////

typedef std::pair<TExpressionPtr, Stroka> TNamedExpression;
typedef std::vector<TNamedExpression> TNamedExpressionList;
typedef TNullable<TNamedExpressionList> TNullableNamedExprs;

struct TQuery
{
    Stroka FromPath;
    TNullableNamedExprs SelectExprs;
    TExpressionPtr WherePredicate;
    TNullableNamedExprs GroupExprs;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NAst
} // namespace NQueryClient
} // namespace NYT
