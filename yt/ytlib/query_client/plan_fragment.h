#pragma once

#include "public.h"

#include "plan_fragment_common.h"

#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/row_buffer.h>

#include <core/misc/property.h>
#include <core/misc/guid.h>

#include <ytlib/node_tracker_client/node_directory.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EExpressionKind,
    (Literal)
    (Reference)
    (Function)
    (BinaryOp)
    (InOp)
);

DECLARE_ENUM(EOperatorKind,
    (Scan)
    (Filter)
    (Group)
    (Project)
);

DECLARE_ENUM(EAggregateFunctions,
    (Sum)
    (Min)
    (Max)
    (Average)
    (Count)
);

struct TExpression
    : public TIntrinsicRefCounted
{
    TExpression(
        const TSourceLocation& sourceLocation,
        EValueType type)
        : SourceLocation(sourceLocation)
        , Type(type)
    { }

    TSourceLocation SourceLocation;
    const EValueType Type;

    TStringBuf GetSource(const TStringBuf& source) const;
    Stroka GetName() const;

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
    
};

DEFINE_REFCOUNTED_TYPE(TExpression)
DEFINE_REFCOUNTED_TYPE(const TExpression)

struct TLiteralExpression
    : public TExpression
{
    TLiteralExpression(
        const TSourceLocation& sourceLocation,
        EValueType type)
        : TExpression(sourceLocation, type)
    { }

    TLiteralExpression(
        const TSourceLocation& sourceLocation,
        EValueType type,
        TOwningValue value)
        : TExpression(sourceLocation, type)
        , Value(value)
    { }

    TOwningValue Value;

};

struct TReferenceExpression
    : public TExpression
{
    TReferenceExpression(
        const TSourceLocation& sourceLocation,
        EValueType type)
        : TExpression(sourceLocation, type)
    { }

    TReferenceExpression(
        const TSourceLocation& sourceLocation,
        EValueType type,
        TStringBuf columnName)
        : TExpression(sourceLocation, type)
        , ColumnName(columnName)
    { }

    Stroka ColumnName;

};

typedef std::vector<TConstExpressionPtr> TArguments;

struct TFunctionExpression
    : public TExpression
{
    TFunctionExpression(
        const TSourceLocation& sourceLocation,
        EValueType type)
        : TExpression(sourceLocation, type)
    { }

    TFunctionExpression(
        const TSourceLocation& sourceLocation,
        EValueType type,
        const Stroka& functionName,
        const TArguments& arguments)
        : TExpression(sourceLocation, type)
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
        EValueType type)
        : TExpression(sourceLocation, type)
    { }

    TBinaryOpExpression(
        const TSourceLocation& sourceLocation,
        EValueType type,
        EBinaryOp opcode,
        const TConstExpressionPtr& lhs,
        const TConstExpressionPtr& rhs)
        : TExpression(sourceLocation, type)
        , Opcode(opcode)
        , Lhs(lhs)
        , Rhs(rhs)
    { }

    EBinaryOp Opcode;
    TConstExpressionPtr Lhs;
    TConstExpressionPtr Rhs;

};

struct TInOpExpression
    : public TExpression
{
    TInOpExpression(
        const TSourceLocation& sourceLocation,
        EValueType type)
        : TExpression(sourceLocation, type)
    { }

    TInOpExpression(
        const TSourceLocation& sourceLocation,
        const TArguments& arguments,
        const std::vector<TOwningRow>& values)
        : TExpression(sourceLocation, EValueType::Boolean)
        , Arguments(arguments)
        , Values(values)
    { }

    TArguments Arguments;
    std::vector<TOwningRow> Values;

};

EValueType InferBinaryExprType(EBinaryOp opCode, EValueType lhsType, EValueType rhsType, const TStringBuf& source);

////////////////////////////////////////////////////////////////////////////////

struct TOperator
    : public TIntrinsicRefCounted
{
    virtual TTableSchema GetTableSchema() const = 0;

    // TODO: Remove this function from here
    TKeyColumns GetKeyColumns() const;

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
};

DEFINE_REFCOUNTED_TYPE(TOperator)
DEFINE_REFCOUNTED_TYPE(const TOperator)

struct TScanOperator
    : public TOperator
{
    TDataSplits DataSplits;

    virtual TTableSchema GetTableSchema() const override;
        
};

struct TFilterOperator
    : public TOperator
{
    TConstOperatorPtr Source;

    TConstExpressionPtr Predicate;

    virtual TTableSchema GetTableSchema() const override;
    
};

struct TNamedItem
{
    TNamedItem()
    { }

    TNamedItem(const TConstExpressionPtr& expression, Stroka name)
        : Expression(expression)
        , Name(name)
    { }

    TConstExpressionPtr Expression;
    Stroka Name;
};

typedef std::vector<TNamedItem> TNamedItemList;

struct TAggregateItem
{
    TAggregateItem()
    { }

    TAggregateItem(const TConstExpressionPtr& expression, EAggregateFunctions aggregateFunction, Stroka name)
        : Expression(expression)
        , AggregateFunction(aggregateFunction)
        , Name(name)
    { }

    TConstExpressionPtr Expression;
    EAggregateFunctions AggregateFunction;
    Stroka Name;
};

typedef std::vector<TAggregateItem> TAggregateItemList;

struct TGroupOperator
    : public TOperator
{
    TConstOperatorPtr Source;

    TNamedItemList GroupItems;
    TAggregateItemList AggregateItems;

    virtual TTableSchema GetTableSchema() const override;    
};

struct TProjectOperator
    : public TOperator
{
    TConstOperatorPtr Source;

    TNamedItemList Projections;

    virtual TTableSchema GetTableSchema() const override;    
};

////////////////////////////////////////////////////////////////////////////////

class TPlanFragment
    : public TIntrinsicRefCounted
{
public:
    explicit TPlanFragment(
        TTimestamp timestamp,
        i64 inputRowLimit,
        i64 outputRowLimit,
        const TGuid& id = TGuid::Create(),
        const TStringBuf& source = TStringBuf())
        : Timestamp_(timestamp)
        , InputRowLimit_(inputRowLimit)
        , OutputRowLimit_(outputRowLimit)
        , Id_(id)
        , Source_(source)
    { }

    explicit TPlanFragment(
        TTimestamp timestamp,
        i64 inputRowLimit,
        i64 outputRowLimit,
        const TStringBuf& source)
        : Timestamp_(timestamp)
        , InputRowLimit_(inputRowLimit)
        , OutputRowLimit_(outputRowLimit)
        , Id_(TGuid::Create())
        , Source_(source)
    { }

    DEFINE_BYVAL_RO_PROPERTY(TTimestamp, Timestamp);
    DEFINE_BYVAL_RO_PROPERTY(i64, InputRowLimit);
    DEFINE_BYVAL_RO_PROPERTY(i64, OutputRowLimit);
    DEFINE_BYVAL_RO_PROPERTY(TGuid, Id);
    DEFINE_BYVAL_RO_PROPERTY(Stroka, Source);

    TPlanFragmentPtr RewriteWith(const TConstOperatorPtr& head) const;
    
    TNodeDirectoryPtr NodeDirectory;
    TConstOperatorPtr Head;

};

DEFINE_REFCOUNTED_TYPE(TPlanFragment)
DEFINE_REFCOUNTED_TYPE(const TPlanFragment)

void ToProto(NProto::TPlanFragment* serialized, const TConstPlanFragmentPtr& fragment);
TPlanFragmentPtr FromProto(const NProto::TPlanFragment& serialized);

TPlanFragmentPtr PreparePlanFragment(
    IPrepareCallbacks* callbacks,
    const Stroka& source,
    i64 inputRowLimit = std::numeric_limits<i64>::max(),
    i64 outputRowLimit = std::numeric_limits<i64>::max(),
    TTimestamp timestamp = NullTimestamp);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
