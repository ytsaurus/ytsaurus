#pragma once

#include "public.h"
#include "function_registry.h"
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

DEFINE_ENUM(EExpressionKind,
    (Literal)
    (Reference)
    (Function)
    (UnaryOp)
    (BinaryOp)
    (InOp)
);

DEFINE_ENUM(EOperatorKind,
    (Scan)
    (Filter)
    (Group)
    (Project)
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

struct TUnaryOpExpression
    : public TExpression
{
    TUnaryOpExpression(
        const TSourceLocation& sourceLocation,
        EValueType type)
        : TExpression(sourceLocation, type)
    { }

    TUnaryOpExpression(
        const TSourceLocation& sourceLocation,
        EValueType type,
        EUnaryOp opcode,
        const TConstExpressionPtr& operand)
        : TExpression(sourceLocation, type)
        , Opcode(opcode)
        , Operand(operand)
    { }

    EUnaryOp Opcode;
    TConstExpressionPtr Operand;

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

    TAggregateItem(const TConstExpressionPtr& expression, EAggregateFunction aggregateFunction, Stroka name)
        : TNamedItem(expression, name)
        , AggregateFunction(aggregateFunction)
    { }

    EAggregateFunction AggregateFunction;
};

typedef std::vector<TAggregateItem> TAggregateItemList;

////////////////////////////////////////////////////////////////////////////////

struct TJoinClause
    : public TIntrinsicRefCounted
{
    TTableSchema ForeignTableSchema;
    TKeyColumns ForeignKeyColumns;
    std::vector<Stroka> JoinColumns;

    // TODO: Use ITableSchemaInterface
    TTableSchema JoinedTableSchema;

    TTableSchema GetTableSchema() const
    {
        return JoinedTableSchema;
    }

};

DEFINE_REFCOUNTED_TYPE(TJoinClause)

struct TGroupClause
    : public TIntrinsicRefCounted
{
    TNamedItemList GroupItems;
    TAggregateItemList AggregateItems;

    // TODO: Use ITableSchemaInterface
    TTableSchema GroupedTableSchema;

    void AddGroupItem(const TNamedItem& namedItem)
    {
        GroupItems.push_back(namedItem);
        GroupedTableSchema.Columns().emplace_back(namedItem.Name, namedItem.Expression->Type);
    }

    void AddAggregateItem(const TAggregateItem& aggregateItem)
    {
        AggregateItems.push_back(aggregateItem);
        GroupedTableSchema.Columns().emplace_back(aggregateItem.Name, aggregateItem.Expression->Type);
    }

    TTableSchema GetTableSchema() const
    {
        return GroupedTableSchema;
    }
};

DEFINE_REFCOUNTED_TYPE(TGroupClause)

struct TProjectClause
    : public TIntrinsicRefCounted
{
    TNamedItemList Projections;

    // TODO: Use ITableSchemaInterface
    TTableSchema ProjectTableSchema;

    void AddProjection(const TNamedItem& namedItem)
    {
        Projections.push_back(namedItem);
        ProjectTableSchema.Columns().emplace_back(namedItem.Name, namedItem.Expression->Type);
    }

    void AddProjection(const TConstExpressionPtr& expression, Stroka name)
    {
        AddProjection(TNamedItem(expression, name));
    }

    TTableSchema GetTableSchema() const
    {
        return ProjectTableSchema;
    }
};

DEFINE_REFCOUNTED_TYPE(TProjectClause)

struct TQuery
    : public TIntrinsicRefCounted
{
    TQuery(
        i64 inputRowLimit,
        i64 outputRowLimit,
        const TGuid& id = TGuid::Create())
        : InputRowLimit(inputRowLimit)
        , OutputRowLimit(outputRowLimit)
        , Id(id)
    { }

    i64 InputRowLimit;
    i64 OutputRowLimit;
    TGuid Id;

    // TODO: Move out TableSchema and KeyColumns 
    TTableSchema TableSchema;
    TKeyColumns KeyColumns;

    TConstJoinClausePtr JoinClause;
    TConstExpressionPtr WhereClause;
    TConstGroupClausePtr GroupClause;
    TConstProjectClausePtr ProjectClause;

    i64 Limit = std::numeric_limits<i64>::max();

    TTableSchema GetTableSchema() const
    {
        if (ProjectClause) {
            return ProjectClause->GetTableSchema();
        }

        if (GroupClause) {
            return GroupClause->GetTableSchema();
        }

        if (JoinClause) {
            return JoinClause->GetTableSchema();
        }

        return TableSchema;
    }

};

DEFINE_REFCOUNTED_TYPE(TQuery)

struct TDataSource
{
    NObjectClient::TObjectId Id;
    TKeyRange Range;
};

typedef std::vector<TDataSource> TDataSources;

struct TPlanFragment
    : public TIntrinsicRefCounted
{
    explicit TPlanFragment(const TStringBuf& source = TStringBuf())
        : Source(source)
    { }

    Stroka Source;

    TNodeDirectoryPtr NodeDirectory;
    
    TTimestamp Timestamp;

    TDataSources DataSources;
    TDataSource ForeignDataSource;

    TConstQueryPtr Query;
    bool Ordered = false;
    bool VerboseLogging = false;

};

DEFINE_REFCOUNTED_TYPE(TPlanFragment)

void ToProto(NProto::TPlanFragment* serialized, const TConstPlanFragmentPtr& fragment);
TPlanFragmentPtr FromProto(const NProto::TPlanFragment& serialized);

TPlanFragmentPtr PreparePlanFragment(
    IPrepareCallbacks* callbacks,
    const Stroka& source,
    const TFunctionRegistryPtr functionRegistry,
    i64 inputRowLimit = std::numeric_limits<i64>::max(),
    i64 outputRowLimit = std::numeric_limits<i64>::max(),
    TTimestamp timestamp = NullTimestamp);

TPlanFragmentPtr PrepareJobPlanFragment(
    const Stroka& source,
    const TTableSchema& initialTableSchema,
    const TFunctionRegistryPtr functionRegistry);

TConstExpressionPtr PrepareExpression(
    const Stroka& source,
    TTableSchema initialTableSchema);

Stroka InferName(TConstExpressionPtr expr);
Stroka InferName(TConstQueryPtr query);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
