#pragma once

#include "public.h"
#include "query_common.h"

#include <yt/ytlib/table_client/row_buffer.h>
#include <yt/ytlib/table_client/schema.h>

#include <yt/core/misc/guid.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/range.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

//! Computes key index for a given column name.
int ColumnNameToKeyPartIndex(const TKeyColumns& keyColumns, const TString& columnName);

struct TColumnDescriptor
{
    TString Name;
    size_t Index;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EExpressionKind,
    (None)
    (Literal)
    (Reference)
    (Function)
    (UnaryOp)
    (BinaryOp)
    (InOp)
);

struct TExpression
    : public TIntrinsicRefCounted
{
    TExpression(EValueType type)
        : Type(type)
    { }

    EValueType Type;

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
    TLiteralExpression(EValueType type)
        : TExpression(type)
    { }

    TLiteralExpression(EValueType type, TOwningValue value)
        : TExpression(type)
        , Value(value)
    { }

    TOwningValue Value;
};

struct TReferenceExpression
    : public TExpression
{
    TReferenceExpression(EValueType type)
        : TExpression(type)
    { }

    TReferenceExpression(EValueType type, TStringBuf columnName)
        : TExpression(type)
        , ColumnName(columnName)
    { }

    TString ColumnName;
};

struct TFunctionExpression
    : public TExpression
{
    TFunctionExpression(EValueType type)
        : TExpression(type)
    { }

    TFunctionExpression(
        EValueType type,
        const TString& functionName,
        const std::vector<TConstExpressionPtr>& arguments)
        : TExpression(type)
        , FunctionName(functionName)
        , Arguments(arguments)
    { }

    TString FunctionName;
    std::vector<TConstExpressionPtr> Arguments;
};

DEFINE_REFCOUNTED_TYPE(TFunctionExpression)

struct TUnaryOpExpression
    : public TExpression
{
    TUnaryOpExpression(EValueType type)
        : TExpression(type)
    { }

    TUnaryOpExpression(
        EValueType type,
        EUnaryOp opcode,
        TConstExpressionPtr operand)
        : TExpression(type)
        , Opcode(opcode)
        , Operand(operand)
    { }

    EUnaryOp Opcode;
    TConstExpressionPtr Operand;
};

struct TBinaryOpExpression
    : public TExpression
{
    TBinaryOpExpression(EValueType type)
        : TExpression(type)
    { }

    TBinaryOpExpression(
        EValueType type,
        EBinaryOp opcode,
        TConstExpressionPtr lhs,
        TConstExpressionPtr rhs)
        : TExpression(type)
        , Opcode(opcode)
        , Lhs(lhs)
        , Rhs(rhs)
    { }

    EBinaryOp Opcode;
    TConstExpressionPtr Lhs;
    TConstExpressionPtr Rhs;
};

struct TInOpExpressionValuesTag
{ };

struct TInOpExpression
    : public TExpression
{
    TInOpExpression(EValueType type)
        : TExpression(type)
    { }

    TInOpExpression(
        std::vector<TConstExpressionPtr> arguments,
        TSharedRange<TRow> values)
        : TExpression(EValueType::Boolean)
        , Arguments(std::move(arguments))
        , Values(std::move(values))
    { }

    std::vector<TConstExpressionPtr> Arguments;
    TSharedRange<TRow> Values;
};

void ThrowTypeMismatchError(
    EValueType lhsType,
    EValueType rhsType,
    const TStringBuf& source,
    const TStringBuf& lhsSource,
    const TStringBuf& rhsSource);

////////////////////////////////////////////////////////////////////////////////

struct TNamedItem
{
    TNamedItem()
    { }

    TNamedItem(
        TConstExpressionPtr expression,
        const TString& name)
        : Expression(expression)
        , Name(name)
    { }

    TConstExpressionPtr Expression;
    TString Name;
};

typedef std::vector<TNamedItem> TNamedItemList;

struct TAggregateItem
    : public TNamedItem
{
    TAggregateItem()
    { }

    TAggregateItem(
        TConstExpressionPtr expression,
        const TString& aggregateFunction,
        const TString& name,
        EValueType stateType,
        EValueType resultType)
        : TNamedItem(expression, name)
        , AggregateFunction(aggregateFunction)
        , StateType(stateType)
        , ResultType(resultType)
    { }

    TString AggregateFunction;
    EValueType StateType;
    EValueType ResultType;
};

typedef std::vector<TAggregateItem> TAggregateItemList;

////////////////////////////////////////////////////////////////////////////////

struct TJoinClause
    : public TIntrinsicRefCounted
{
    TTableSchema OriginalSchema;
    std::vector<TColumnDescriptor> SchemaMapping;
    std::vector<TString> SelfJoinedColumns;
    std::vector<TString> ForeignJoinedColumns;

    bool CanUseSourceRanges;
    std::vector<TConstExpressionPtr> ForeignEquations;
    std::vector<std::pair<TConstExpressionPtr, bool>> SelfEquations;

    size_t CommonKeyPrefix = 0;

    bool IsLeft = false;

    TGuid ForeignDataId;

    std::vector<TColumnDescriptor> GetOrderedSchemaMapping() const
    {
        auto orderedSchemaMapping = SchemaMapping;
        std::sort(orderedSchemaMapping.begin(), orderedSchemaMapping.end(),
            [] (const TColumnDescriptor& lhs, const TColumnDescriptor& rhs) {
                return lhs.Index < rhs.Index;
            });
        return orderedSchemaMapping;
    }

    TKeyColumns GetKeyColumns() const
    {
        TKeyColumns result(OriginalSchema.GetKeyColumnCount());
        for (const auto& item : SchemaMapping) {
            if (item.Index < OriginalSchema.GetKeyColumnCount()) {
                result[item.Index] = item.Name;
            }
        }
        return result;
    }

    TTableSchema GetRenamedSchema() const
    {
        TSchemaColumns result;
        for (const auto& item : GetOrderedSchemaMapping()) {
            result.emplace_back(item.Name, OriginalSchema.Columns()[item.Index].Type);
        }

        return TTableSchema(result);
    }

    TTableSchema GetTableSchema(const TTableSchema& source) const
    {
        TSchemaColumns result;

        auto selfColumnNames = SelfJoinedColumns;
        std::sort(selfColumnNames.begin(), selfColumnNames.end());
        for (const auto& column : source.Columns()) {
            if (std::binary_search(selfColumnNames.begin(), selfColumnNames.end(), column.Name)) {
                result.push_back(column);
            }
        }

        auto foreignColumnNames = ForeignJoinedColumns;
        std::sort(foreignColumnNames.begin(), foreignColumnNames.end());
        auto renamedSchema = GetRenamedSchema();
        for (const auto& column : renamedSchema.Columns()) {
            if (std::binary_search(foreignColumnNames.begin(), foreignColumnNames.end(), column.Name)) {
                result.push_back(column);
            }
        }

        return TTableSchema(result);
    }
};

DEFINE_REFCOUNTED_TYPE(TJoinClause)

struct TGroupClause
    : public TIntrinsicRefCounted
{
    TNamedItemList GroupItems;
    TAggregateItemList AggregateItems;
    bool IsMerge;
    bool IsFinal;
    ETotalsMode TotalsMode;

    void AddGroupItem(const TNamedItem& namedItem)
    {
        GroupItems.push_back(namedItem);
    }

    void AddGroupItem(TConstExpressionPtr expression, TString name)
    {
        AddGroupItem(TNamedItem(expression, name));
    }

    TTableSchema GetTableSchema() const
    {
        TSchemaColumns result;
        for (const auto& item : GroupItems) {
            result.emplace_back(item.Name, item.Expression->Type);
        }

        for (const auto& item : AggregateItems) {
            result.emplace_back(item.Name, IsFinal ? item.Expression->Type : item.StateType);
        }

        return TTableSchema(result);
    }
};

DEFINE_REFCOUNTED_TYPE(TGroupClause)

typedef std::pair<TConstExpressionPtr, bool> TOrderItem;

struct TOrderClause
    : public TIntrinsicRefCounted
{
    std::vector<TOrderItem> OrderItems;
};

DEFINE_REFCOUNTED_TYPE(TOrderClause)

struct TProjectClause
    : public TIntrinsicRefCounted
{
    TNamedItemList Projections;

    void AddProjection(const TNamedItem& namedItem)
    {
        Projections.push_back(namedItem);
    }

    void AddProjection(TConstExpressionPtr expression, TString name)
    {
        AddProjection(TNamedItem(expression, name));
    }

    TTableSchema GetTableSchema() const
    {
        TSchemaColumns result;
        for (const auto& item : Projections) {
            result.emplace_back(item.Name, item.Expression->Type);
        }

        return TTableSchema(result);
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

    TQuery(const TQuery& other)
        : InputRowLimit(other.InputRowLimit)
        , OutputRowLimit(other.OutputRowLimit)
        , Id(TGuid::Create())
        , OriginalSchema(other.OriginalSchema)
        , SchemaMapping(other.SchemaMapping)
        , JoinClauses(other.JoinClauses)
        , WhereClause(other.WhereClause)
        , GroupClause(other.GroupClause)
        , HavingClause(other.HavingClause)
        , ProjectClause(other.ProjectClause)
        , OrderClause(other.OrderClause)
        , Limit(other.Limit)
        , UseDisjointGroupBy(other.UseDisjointGroupBy)
        , InferRanges(other.InferRanges)
    { }

    i64 InputRowLimit;
    i64 OutputRowLimit;
    TGuid Id;

    TTableSchema OriginalSchema;
    std::vector<TColumnDescriptor> SchemaMapping;

    std::vector<TConstJoinClausePtr> JoinClauses;
    TConstExpressionPtr WhereClause;
    TConstGroupClausePtr GroupClause;
    TConstExpressionPtr HavingClause;
    TConstProjectClausePtr ProjectClause;
    TConstOrderClausePtr OrderClause;

    // TODO: Update protocol and fix it
    // If Limit == std::numeric_limits<i64>::max() - 1, then do ordered read with prefetch
    i64 Limit = std::numeric_limits<i64>::max();

    bool UseDisjointGroupBy = false;
    bool InferRanges = true;

    bool IsOrdered() const
    {
        if (Limit < std::numeric_limits<i64>::max()) {
            return !OrderClause && !GroupClause;
        } else {
            YCHECK(!OrderClause);
            return false;
        }
    }

    TKeyColumns GetKeyColumns() const
    {
        TKeyColumns result(OriginalSchema.GetKeyColumnCount());
        for (const auto& item : SchemaMapping) {
            if (item.Index < OriginalSchema.GetKeyColumnCount()) {
                result[item.Index] = item.Name;
            }
        }
        return result;
    }

    std::vector<TColumnDescriptor> GetOrderedSchemaMapping() const
    {
        auto orderedSchemaMapping = SchemaMapping;
        std::sort(orderedSchemaMapping.begin(), orderedSchemaMapping.end(),
            [] (const TColumnDescriptor& lhs, const TColumnDescriptor& rhs) {
                return lhs.Index < rhs.Index;
            });
        return orderedSchemaMapping;
    }

    TTableSchema GetReadSchema() const
    {
        TSchemaColumns result;
        for (const auto& item : GetOrderedSchemaMapping()) {
            result.emplace_back(
                OriginalSchema.Columns()[item.Index].Name,
                OriginalSchema.Columns()[item.Index].Type);
        }

        return TTableSchema(result);
    }

    TTableSchema GetRenamedSchema() const
    {
        TSchemaColumns result;
        for (const auto& item : GetOrderedSchemaMapping()) {
            result.emplace_back(item.Name, OriginalSchema.Columns()[item.Index].Type);
        }

        return TTableSchema(result);
    }

    TTableSchema GetTableSchema() const
    {
        if (ProjectClause) {
            return ProjectClause->GetTableSchema();
        }

        if (GroupClause) {
            return GroupClause->GetTableSchema();
        }

        TTableSchema result = GetRenamedSchema();

        for (const auto& joinClause : JoinClauses) {
            result = joinClause->GetTableSchema(result);
        }

        return TTableSchema(result);
    }
};

DEFINE_REFCOUNTED_TYPE(TQuery)

void ToProto(NProto::TQuery* serialized, const TConstQueryPtr& original);
void FromProto(TConstQueryPtr* original, const NProto::TQuery& serialized);

void ToProto(NProto::TQueryOptions* serialized, const TQueryOptions& original);
void FromProto(TQueryOptions* original, const NProto::TQueryOptions& serialized);

void ToProto(NProto::TDataRanges* serialized, const TDataRanges& original);
void FromProto(TDataRanges* original, const NProto::TDataRanges& serialized);

TString InferName(TConstExpressionPtr expr, bool omitValues = false);
TString InferName(TConstQueryPtr query, bool omitValues = false);

bool Compare(
    TConstExpressionPtr lhs,
    const TTableSchema& lhsSchema,
    TConstExpressionPtr rhs,
    const TTableSchema& rhsSchema,
    size_t maxIndex = std::numeric_limits<size_t>::max());

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
