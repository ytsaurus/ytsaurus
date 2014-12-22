#pragma once

#include "public.h"

#include "plan_fragment_common.h"

#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/row_buffer.h>

#include <core/misc/property.h>
#include <core/misc/guid.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/query_client/plan_fragment.pb.h>

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

EValueType InferBinaryExprType(
    EBinaryOp opCode,
    EValueType lhsType,
    EValueType rhsType,
    const TStringBuf& source);

////////////////////////////////////////////////////////////////////////////////

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

    TColumnSchema GetColumnSchema() const
    {
        return TColumnSchema(Name, Expression->Type);
    }
};

typedef std::vector<TNamedItem> TNamedItemList;

struct TAggregateItem
    : public TNamedItem
{
    TAggregateItem()
    { }

    TAggregateItem(const TConstExpressionPtr& expression, EAggregateFunctions aggregateFunction, Stroka name)
        : TNamedItem(expression, name)
        , AggregateFunction(aggregateFunction)
    { }

    EAggregateFunctions AggregateFunction;
};

typedef std::vector<TAggregateItem> TAggregateItemList;

////////////////////////////////////////////////////////////////////////////////

struct TGroupClause
{
    TNamedItemList GroupItems;
    TAggregateItemList AggregateItems;

    TTableSchema GetTableSchema() const
    {
        TTableSchema result;

        for (const auto& groupItem : GroupItems) {
            result.Columns().emplace_back(groupItem.GetColumnSchema());
        }

        for (const auto& aggregateItem : AggregateItems) {
            result.Columns().emplace_back(aggregateItem.GetColumnSchema());
        }

        ValidateTableSchema(result);

        return result;
    }
};

struct TProjectClause
{
    TNamedItemList Projections;

    TTableSchema GetTableSchema() const
    {
        TTableSchema result;

        for (const auto& item : Projections) {
            result.Columns().emplace_back(item.GetColumnSchema());
        }

        ValidateTableSchema(result);

        return result;
    }
};

class TQuery
    : public TIntrinsicRefCounted
{
public:
    explicit TQuery(
        i64 inputRowLimit,
        i64 outputRowLimit,
        const TGuid& id = TGuid::Create())
        : InputRowLimit_(inputRowLimit)
        , OutputRowLimit_(outputRowLimit)
        , Id_(id)
    { }

    DEFINE_BYVAL_RO_PROPERTY(i64, InputRowLimit);
    DEFINE_BYVAL_RO_PROPERTY(i64, OutputRowLimit);
    DEFINE_BYVAL_RO_PROPERTY(TGuid, Id);

    // TODO: Rename to InitialTableSchema
    TTableSchema TableSchema;
    TKeyColumns KeyColumns;

    TConstExpressionPtr Predicate;

    TNullable<TGroupClause> GroupClause;

    TNullable<TProjectClause> ProjectClause;

    TTableSchema GetTableSchema() const
    {
        if (ProjectClause) {
            return ProjectClause->GetTableSchema();
        }

        if (GroupClause) {
            return GroupClause->GetTableSchema();
        }

        return TableSchema;
    }

    TKeyColumns GetKeyColumns() const
    {
        if (!GroupClause && !ProjectClause) {
            return KeyColumns;
        } else {
            return TKeyColumns();
        }        
    }
};

DEFINE_REFCOUNTED_TYPE(TQuery)

class TPlanFragment
    : public TIntrinsicRefCounted
{
public:
    explicit TPlanFragment(
        const TStringBuf& source = TStringBuf())
        : Source_(source)
    { }

    DEFINE_BYVAL_RO_PROPERTY(Stroka, Source);

    TNodeDirectoryPtr NodeDirectory;
    TDataSplits DataSplits;
    TConstQueryPtr Query;
    bool Ordered = false;

};

DEFINE_REFCOUNTED_TYPE(TPlanFragment)

void ToProto(NProto::TPlanFragment* serialized, const TConstPlanFragmentPtr& fragment);
TPlanFragmentPtr FromProto(const NProto::TPlanFragment& serialized);

TPlanFragmentPtr PreparePlanFragment(
    IPrepareCallbacks* callbacks,
    const Stroka& source,
    i64 inputRowLimit = std::numeric_limits<i64>::max(),
    i64 outputRowLimit = std::numeric_limits<i64>::max(),
    TTimestamp timestamp = NullTimestamp);

TPlanFragmentPtr PrepareJobPlanFragment(
    const Stroka& source,
    const TTableSchema& initialTableSchema);

TConstExpressionPtr PrepareExpression(
    const Stroka& source,
    const TTableSchema& initialTableSchema);

Stroka InferName(TConstExpressionPtr expr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
