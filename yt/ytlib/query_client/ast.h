#pragma once

#include "public.h"
#include "query_common.h"

#include <yt/ytlib/ypath/rich.h>

#include <yt/core/misc/variant.h>
#include <yt/core/misc/hash.h>

namespace NYT {
namespace NQueryClient {
namespace NAst {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TExpression)
DECLARE_REFCOUNTED_STRUCT(TReferenceExpression)
DECLARE_REFCOUNTED_STRUCT(TAliasExpression)
DECLARE_REFCOUNTED_STRUCT(TLiteralExpression)
DECLARE_REFCOUNTED_STRUCT(TFunctionExpression)
DECLARE_REFCOUNTED_STRUCT(TUnaryOpExpression)
DECLARE_REFCOUNTED_STRUCT(TBinaryOpExpression)
DECLARE_REFCOUNTED_STRUCT(TInExpression)
DECLARE_REFCOUNTED_STRUCT(TTransformExpression)

using TIdentifierList = std::vector<TReferenceExpressionPtr>;
using TExpressionList = std::vector<TExpressionPtr>;
using TNullableExpressionList = TNullable<TExpressionList>;
using TNullableIdentifierList = TNullable<TIdentifierList>;
using TOrderExpressionList = std::vector<std::pair<TExpressionList, bool>>;

////////////////////////////////////////////////////////////////////////////////

struct TNullLiteralValue
{ };

using TLiteralValue = TVariant<
    TNullLiteralValue,
    i64,
    ui64,
    double,
    bool,
    TString>;

using TLiteralValueList = std::vector<TLiteralValue>;
using TLiteralValueTuple = std::vector<TLiteralValue>;
using TLiteralValueTupleList = std::vector<TLiteralValueTuple>;

////////////////////////////////////////////////////////////////////////////////

struct TReference
{
    TReference() = default;

    TReference(const TString& columnName, const TNullable<TString>& tableName = Null)
        : ColumnName(columnName)
        , TableName(tableName)
    { }

    TString ColumnName;
    TNullable<TString> TableName;

    operator size_t() const;
};

bool operator == (const TReference& lhs, const TReference& rhs);
bool operator != (const TReference& lhs, const TReference& rhs);

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

DEFINE_REFCOUNTED_TYPE(TExpression)

template <class T, class... TArgs>
TExpressionList MakeExpression(TArgs&& ... args)
{
    return TExpressionList(1, New<T>(std::forward<TArgs>(args)...));
}

bool operator == (const TExpression& lhs, const TExpression& rhs);
bool operator != (const TExpression& lhs, const TExpression& rhs);

////////////////////////////////////////////////////////////////////////////////

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

DEFINE_REFCOUNTED_TYPE(TLiteralExpression)

////////////////////////////////////////////////////////////////////////////////

struct TReferenceExpression
    : public TExpression
{
    TReferenceExpression(
        const TSourceLocation& sourceLocation,
        const TStringBuf& columnName)
        : TExpression(sourceLocation)
        , Reference(TString(columnName))
    { }

    TReferenceExpression(
        const TSourceLocation& sourceLocation,
        const TStringBuf& columnName,
        const TStringBuf& tableName)
        : TExpression(sourceLocation)
        , Reference(TString(columnName), TString(tableName))
    { }

    TReference Reference;
};

DEFINE_REFCOUNTED_TYPE(TReferenceExpression)

////////////////////////////////////////////////////////////////////////////////

struct TAliasExpression
    : public TExpression
{
    TAliasExpression(
        const TSourceLocation& sourceLocation,
        const TExpressionPtr& expression,
        const TStringBuf& name)
        : TExpression(sourceLocation)
        , Expression(expression)
        , Name(TString(name))
    { }

    TExpressionPtr Expression;
    TString Name;
};

DEFINE_REFCOUNTED_TYPE(TAliasExpression)

////////////////////////////////////////////////////////////////////////////////

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

DEFINE_REFCOUNTED_TYPE(TFunctionExpression)

////////////////////////////////////////////////////////////////////////////////

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

DEFINE_REFCOUNTED_TYPE(TUnaryOpExpression)

////////////////////////////////////////////////////////////////////////////////

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

DEFINE_REFCOUNTED_TYPE(TBinaryOpExpression)

////////////////////////////////////////////////////////////////////////////////

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

DEFINE_REFCOUNTED_TYPE(TInExpression)

////////////////////////////////////////////////////////////////////////////////

struct TTransformExpression
    : public TExpression
{
    TTransformExpression(
        const TSourceLocation& sourceLocation,
        TExpressionList expression,
        const TLiteralValueTupleList& from,
        const TLiteralValueTupleList& to,
        TNullableExpressionList defaultExpr)
        : TExpression(sourceLocation)
        , Expr(std::move(expression))
        , From(from)
        , To(to)
        , DefaultExpr(std::move(defaultExpr))
    { }

    TExpressionList Expr;
    TLiteralValueTupleList From;
    TLiteralValueTupleList To;
    TNullableExpressionList DefaultExpr;
};

DEFINE_REFCOUNTED_TYPE(TTransformExpression)

////////////////////////////////////////////////////////////////////////////////
struct TTableDescriptor
{
    TTableDescriptor() = default;

    explicit TTableDescriptor(
        const NYPath::TYPath& path,
        const TNullable<TString>& alias = Null)
        : Path(path)
        , Alias(alias)
    { }

    NYPath::TYPath Path;
    TNullable<TString> Alias;
};

bool operator == (const TTableDescriptor& lhs, const TTableDescriptor& rhs);
bool operator != (const TTableDescriptor& lhs, const TTableDescriptor& rhs);

////////////////////////////////////////////////////////////////////////////////

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
        const TExpressionList& lhs,
        const TExpressionList& rhs,
        const TNullableExpressionList& predicate)
        : IsLeft(isLeft)
        , Table(table)
        , Lhs(lhs)
        , Rhs(rhs)
        , Predicate(predicate)
    { }

    bool IsLeft;
    TTableDescriptor Table;
    TIdentifierList Fields;

    TExpressionList Lhs;
    TExpressionList Rhs;

    TNullableExpressionList Predicate;
};

bool operator == (const TJoin& lhs, const TJoin& rhs);
bool operator != (const TJoin& lhs, const TJoin& rhs);

////////////////////////////////////////////////////////////////////////////////

struct TQuery
{
    TTableDescriptor Table;
    std::vector<TJoin> Joins;

    TNullableExpressionList SelectExprs;
    TNullableExpressionList WherePredicate;

    TNullable<std::pair<TExpressionList, ETotalsMode>> GroupExprs;
    TNullableExpressionList HavingPredicate;

    TOrderExpressionList OrderExpressions;

    TNullable<i64> Limit;
};

bool operator == (const TQuery& lhs, const TQuery& rhs);
bool operator != (const TQuery& lhs, const TQuery& rhs);

////////////////////////////////////////////////////////////////////////////////

using TAliasMap = THashMap<TString, TExpressionPtr>;

struct TAstHead
{
    static TAstHead MakeQuery()
    {
        return TAstHead{TVariantTypeTag<TQuery>(), TAliasMap()};
    }

    static TAstHead MakeExpression()
    {
        return TAstHead{TVariantTypeTag<TExpressionPtr>(), TAliasMap()};
    }

    TVariant<TQuery, TExpressionPtr> Ast;
    TAliasMap AliasMap;
};

////////////////////////////////////////////////////////////////////////////////

TStringBuf GetSource(TSourceLocation sourceLocation, const TStringBuf& source);

TString FormatId(const TStringBuf& id);
TString FormatLiteralValue(const TLiteralValue& value);
TString FormatReference(const TReference& ref);
TString FormatExpression(const TExpression& expr);
TString FormatExpression(const TExpressionList& exprs);
TString FormatJoin(const TJoin& join);
TString FormatQuery(const TQuery& query);
TString InferColumnName(const TExpression& expr);
TString InferColumnName(const TReference& ref);

////////////////////////////////////////////////////////////////////////////////

} // namespace NAst
} // namespace NQueryClient
} // namespace NYT
