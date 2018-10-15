#pragma once

#include "public.h"
#include "query_common.h"

#include <yt/client/table_client/row_buffer.h>
#include <yt/client/table_client/schema.h>

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
    int Index;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EExpressionKind,
    ((None)       (0))
    ((Literal)    (1))
    ((Reference)  (2))
    ((Function)   (3))
    ((UnaryOp)    (4))
    ((BinaryOp)   (5))
    ((In)         (6))
    ((Transform)  (7))
    ((Between)    (8))
);

struct TExpression
    : public TIntrinsicRefCounted
{
    explicit TExpression(EValueType type)
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
    explicit TLiteralExpression(EValueType type)
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
    explicit TReferenceExpression(EValueType type)
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
    explicit TFunctionExpression(EValueType type)
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
    explicit TUnaryOpExpression(EValueType type)
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
    explicit TBinaryOpExpression(EValueType type)
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

struct TExpressionRowsetTag
{ };

struct TInExpression
    : public TExpression
{
    explicit TInExpression(EValueType type)
        : TExpression(type)
    {
        YCHECK(type == EValueType::Boolean);
    }

    TInExpression(
        std::vector<TConstExpressionPtr> arguments,
        TSharedRange<TRow> values)
        : TExpression(EValueType::Boolean)
        , Arguments(std::move(arguments))
        , Values(std::move(values))
    { }

    std::vector<TConstExpressionPtr> Arguments;
    TSharedRange<TRow> Values;
};

struct TBetweenExpression
    : public TExpression
{
    explicit TBetweenExpression(EValueType type)
        : TExpression(type)
    {
        YCHECK(type == EValueType::Boolean);
    }

    TBetweenExpression(
        std::vector<TConstExpressionPtr> arguments,
        TSharedRange<TRowRange> ranges)
        : TExpression(EValueType::Boolean)
        , Arguments(std::move(arguments))
        , Ranges(std::move(ranges))
    { }

    std::vector<TConstExpressionPtr> Arguments;
    TSharedRange<TRowRange> Ranges;
};

struct TTransformExpression
    : public TExpression
{
    explicit TTransformExpression(EValueType type)
        : TExpression(type)
    { }

    TTransformExpression(
        EValueType type,
        std::vector<TConstExpressionPtr> arguments,
        TSharedRange<TRow> values,
        TConstExpressionPtr defaultExpression)
        : TExpression(type)
        , Arguments(std::move(arguments))
        , Values(std::move(values))
        , DefaultExpression(std::move(defaultExpression))
    { }

    std::vector<TConstExpressionPtr> Arguments;
    TSharedRange<TRow> Values;
    TConstExpressionPtr DefaultExpression;
};

void ThrowTypeMismatchError(
    EValueType lhsType,
    EValueType rhsType,
    TStringBuf source,
    TStringBuf lhsSource,
    TStringBuf rhsSource);

////////////////////////////////////////////////////////////////////////////////

struct TNamedItem
{
    TNamedItem() = default;

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
    TAggregateItem() = default;

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

    TConstExpressionPtr Predicate;

    std::vector<TConstExpressionPtr> ForeignEquations;
    std::vector<std::pair<TConstExpressionPtr, bool>> SelfEquations;

    size_t CommonKeyPrefix = 0;
    size_t ForeignKeyPrefix = 0;

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
            result.emplace_back(item.Name, OriginalSchema.Columns()[item.Index].LogicalType());
        }

        return TTableSchema(result);
    }

    TTableSchema GetTableSchema(const TTableSchema& source) const
    {
        TSchemaColumns result;

        auto selfColumnNames = SelfJoinedColumns;
        std::sort(selfColumnNames.begin(), selfColumnNames.end());
        for (const auto& column : source.Columns()) {
            if (std::binary_search(selfColumnNames.begin(), selfColumnNames.end(), column.Name())) {
                result.push_back(column);
            }
        }

        auto foreignColumnNames = ForeignJoinedColumns;
        std::sort(foreignColumnNames.begin(), foreignColumnNames.end());
        auto renamedSchema = GetRenamedSchema();
        for (const auto& column : renamedSchema.Columns()) {
            if (std::binary_search(foreignColumnNames.begin(), foreignColumnNames.end(), column.Name())) {
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
    ETotalsMode TotalsMode;

    void AddGroupItem(const TNamedItem& namedItem)
    {
        GroupItems.push_back(namedItem);
    }

    void AddGroupItem(TConstExpressionPtr expression, TString name)
    {
        AddGroupItem(TNamedItem(expression, name));
    }

    TTableSchema GetTableSchema(bool isFinal) const
    {
        TSchemaColumns result;
        for (const auto& item : GroupItems) {
            result.emplace_back(item.Name, item.Expression->Type);
        }

        for (const auto& item : AggregateItems) {
            result.emplace_back(item.Name, isFinal ? item.ResultType : item.StateType);
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

// Front Query is not Coordinatable
// IsMerge is always true for front Query and false for Bottom Query

struct TBaseQuery
    : public TIntrinsicRefCounted
{
    explicit TBaseQuery(TGuid id = TGuid::Create())
        : Id(id)
    { }

    TBaseQuery(const TBaseQuery& other)
        : Id(TGuid::Create())
        , IsFinal(other.IsFinal)
        , GroupClause(other.GroupClause)
        , HavingClause(other.HavingClause)
        , OrderClause(other.OrderClause)
        , ProjectClause(other.ProjectClause)
        , Limit(other.Limit)
        , UseDisjointGroupBy(other.UseDisjointGroupBy)
        , InferRanges(other.InferRanges)
    { }

    TGuid Id;

    // Merge and Final
    bool IsFinal = true;

    TConstGroupClausePtr GroupClause;
    TConstExpressionPtr HavingClause;
    TConstOrderClausePtr OrderClause;

    TConstProjectClausePtr ProjectClause;

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

    virtual TTableSchema GetReadSchema() const = 0;
    virtual TTableSchema GetTableSchema() const = 0;
};

DEFINE_REFCOUNTED_TYPE(TBaseQuery)

struct TQuery
    : public TBaseQuery
{
    explicit TQuery(TGuid id = TGuid::Create())
        : TBaseQuery(id)
    { }

    TQuery(const TQuery& other)
        : TBaseQuery(other)
        , OriginalSchema(other.OriginalSchema)
        , SchemaMapping(other.SchemaMapping)
        , JoinClauses(other.JoinClauses)
        , WhereClause(other.WhereClause)
    { }

    TTableSchema OriginalSchema;
    std::vector<TColumnDescriptor> SchemaMapping;

    // Bottom
    std::vector<TConstJoinClausePtr> JoinClauses;
    TConstExpressionPtr WhereClause;

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

    virtual TTableSchema GetReadSchema() const override
    {
        TSchemaColumns result;
        for (const auto& item : GetOrderedSchemaMapping()) {
            result.emplace_back(
                OriginalSchema.Columns()[item.Index].Name(),
                OriginalSchema.Columns()[item.Index].LogicalType());
        }

        return TTableSchema(result);
    }

    TTableSchema GetRenamedSchema() const
    {
        TSchemaColumns result;
        for (const auto& item : GetOrderedSchemaMapping()) {
            result.emplace_back(item.Name, OriginalSchema.Columns()[item.Index].LogicalType());
        }

        return TTableSchema(result);
    }

    virtual TTableSchema GetTableSchema() const override
    {
        if (ProjectClause) {
            return ProjectClause->GetTableSchema();
        }

        if (GroupClause) {
            return GroupClause->GetTableSchema(IsFinal);
        }

        TTableSchema result = GetRenamedSchema();

        for (const auto& joinClause : JoinClauses) {
            result = joinClause->GetTableSchema(result);
        }

        return TTableSchema(result);
    }

};

DEFINE_REFCOUNTED_TYPE(TQuery)

struct TFrontQuery
    : public TBaseQuery
{
    explicit TFrontQuery(TGuid id = TGuid::Create())
        : TBaseQuery(id)
    { }

    TFrontQuery(const TFrontQuery& other)
        : TBaseQuery(other)
    { }

    TTableSchema Schema;

    TTableSchema GetReadSchema() const
    {
        return Schema;
    }

    TTableSchema GetRenamedSchema() const
    {
        return Schema;
    }

    TTableSchema GetTableSchema() const
    {
        if (ProjectClause) {
            return ProjectClause->GetTableSchema();
        }

        if (GroupClause) {
            return GroupClause->GetTableSchema(IsFinal);
        }

        return Schema;
    }

};

DEFINE_REFCOUNTED_TYPE(TFrontQuery)

template <class TResult, class TDerived, class TNode, class... TArgs>
struct TAbstractVisitor
{
    TDerived* Derived()
    {
        return static_cast<TDerived*>(this);
    }

    TResult Visit(TNode node, TArgs... args)
    {
        auto expr = Derived()->GetExpression(node);

        if (auto literalExpr = expr->template As<TLiteralExpression>()) {
            return Derived()->OnLiteral(literalExpr, args...);
        } else if (auto referenceExpr = expr->template As<TReferenceExpression>()) {
            return Derived()->OnReference(referenceExpr, args...);
        } else if (auto unaryOp = expr->template As<TUnaryOpExpression>()) {
            return Derived()->OnUnary(unaryOp, args...);
        } else if (auto binaryOp = expr->template As<TBinaryOpExpression>()) {
            return Derived()->OnBinary(binaryOp, args...);
        } else if (auto functionExpr = expr->template As<TFunctionExpression>()) {
            return Derived()->OnFunction(functionExpr, args...);
        } else if (auto inExpr = expr->template As<TInExpression>()) {
            return Derived()->OnIn(inExpr, args...);
        } else if (auto betweenExpr = expr->template As<TBetweenExpression>()) {
            return Derived()->OnBetween(betweenExpr, args...);
        } else if (auto transformExpr = expr->template As<TTransformExpression>()) {
            return Derived()->OnTransform(transformExpr, args...);
        }
        Y_UNREACHABLE();
    }

};

template <class TResult, class TDerived>
struct TBaseVisitor
    : TAbstractVisitor<TResult, TDerived, TConstExpressionPtr>
{
    const TExpression* GetExpression(const TConstExpressionPtr& expr)
    {
        return &*expr;
    }

};

template <class TDerived>
struct TVisitor
    : public TBaseVisitor<void, TDerived>
{
    using TBase = TBaseVisitor<void, TDerived>;
    using TBase::Derived;
    using TBase::Visit;

    void OnLiteral(const TLiteralExpression* /*literalExpr*/)
    { }

    void OnReference(const TReferenceExpression* /*referenceExpr*/)
    { }

    void OnUnary(const TUnaryOpExpression* unaryExpr)
    {
        Visit(unaryExpr->Operand);
    }

    void OnBinary(const TBinaryOpExpression* binaryExpr)
    {
        Visit(binaryExpr->Lhs);
        Visit(binaryExpr->Rhs);
    }

    void OnFunction(const TFunctionExpression* functionExpr)
    {
        for (auto argument : functionExpr->Arguments) {
            Visit(argument);
        }
    }

    void OnIn(const TInExpression* inExpr)
    {
        for (auto argument : inExpr->Arguments) {
            Visit(argument);
        }
    }

    void OnBetween(const TBetweenExpression* betweenExpr)
    {
        for (auto argument : betweenExpr->Arguments) {
            Visit(argument);
        }
    }

    void OnTransform(const TTransformExpression* transformExpr)
    {
        for (auto argument : transformExpr->Arguments) {
            Visit(argument);
        }
    }

};

template <class TDerived>
struct TRewriter
    : public TBaseVisitor<TConstExpressionPtr, TDerived>
{
    using TBase = TBaseVisitor<TConstExpressionPtr, TDerived>;
    using TBase::Derived;
    using TBase::Visit;

    TConstExpressionPtr OnLiteral(const TLiteralExpression* literalExpr)
    {
        return literalExpr;
    }

    TConstExpressionPtr OnReference(const TReferenceExpression* referenceExpr)
    {
        return referenceExpr;
    }

    TConstExpressionPtr OnUnary(const TUnaryOpExpression* unaryExpr)
    {
        auto newOperand = Visit(unaryExpr->Operand);

        if (newOperand == unaryExpr->Operand) {
            return unaryExpr;
        }

        return New<TUnaryOpExpression>(
            unaryExpr->Type,
            unaryExpr->Opcode,
            newOperand);
    }

    TConstExpressionPtr OnBinary(const TBinaryOpExpression* binaryExpr)
    {
        auto newLhs = Visit(binaryExpr->Lhs);
        auto newRhs = Visit(binaryExpr->Rhs);

        if (newLhs == binaryExpr->Lhs && newRhs == binaryExpr->Rhs) {
            return binaryExpr;
        }

        return New<TBinaryOpExpression>(
            binaryExpr->Type,
            binaryExpr->Opcode,
            newLhs,
            newRhs);
    }

    TConstExpressionPtr OnFunction(const TFunctionExpression* functionExpr)
    {
        std::vector<TConstExpressionPtr> newArguments;
        bool allEqual = true;
        for (auto argument : functionExpr->Arguments) {
            auto newArgument = Visit(argument);
            allEqual = allEqual && newArgument == argument;
            newArguments.push_back(newArgument);
        }

        if (allEqual) {
            return functionExpr;
        }

        return New<TFunctionExpression>(
            functionExpr->Type,
            functionExpr->FunctionName,
            std::move(newArguments));
    }

    TConstExpressionPtr OnIn(const TInExpression* inExpr)
    {
        std::vector<TConstExpressionPtr> newArguments;
        bool allEqual = true;
        for (auto argument : inExpr->Arguments) {
            auto newArgument = Visit(argument);
            allEqual = allEqual && newArgument == argument;
            newArguments.push_back(newArgument);
        }

        if (allEqual) {
            return inExpr;
        }

        return New<TInExpression>(
            std::move(newArguments),
            inExpr->Values);
    }

    TConstExpressionPtr OnBetween(const TBetweenExpression* betweenExpr)
    {
        std::vector<TConstExpressionPtr> newArguments;
        bool allEqual = true;
        for (auto argument : betweenExpr->Arguments) {
            auto newArgument = Visit(argument);
            allEqual = allEqual && newArgument == argument;
            newArguments.push_back(newArgument);
        }

        if (allEqual) {
            return betweenExpr;
        }

        return New<TBetweenExpression>(
            std::move(newArguments),
            betweenExpr->Ranges);
    }

    TConstExpressionPtr OnTransform(const TTransformExpression* transformExpr)
    {
        std::vector<TConstExpressionPtr> newArguments;
        bool allEqual = true;
        for (auto argument : transformExpr->Arguments) {
            auto newArgument = Visit(argument);
            allEqual = allEqual && newArgument == argument;
            newArguments.push_back(newArgument);
        }

        TConstExpressionPtr newDefaultExpression;
        if (const auto& defaultExpression = transformExpr->DefaultExpression) {
            newDefaultExpression = Visit(defaultExpression);
            allEqual = allEqual && newDefaultExpression == defaultExpression;
        }

        if (allEqual) {
            return transformExpr;
        }

        return New<TTransformExpression>(
            transformExpr->Type,
            std::move(newArguments),
            transformExpr->Values,
            newDefaultExpression);
    }

};

template <class TDerived, class TNode, class... TArgs>
struct TAbstractExpressionPrinter
    : TAbstractVisitor<void, TDerived, TNode, TArgs...>
{
    using TBase = TAbstractVisitor<void, TDerived, TNode, TArgs...>;
    using TBase::Derived;
    using TBase::Visit;

    TStringBuilder* Builder;
    bool OmitValues;

    TAbstractExpressionPrinter(TStringBuilder* builder, bool omitValues)
        : Builder(builder)
        , OmitValues(omitValues)
    { }

    static bool CanOmitParenthesis(TConstExpressionPtr expr)
    {
        return
            expr->As<TLiteralExpression>() ||
            expr->As<TReferenceExpression>() ||
            expr->As<TFunctionExpression>();
    };

    const TExpression* GetExpression(const TConstExpressionPtr& expr)
    {
        return &*expr;
    }

    void OnOperand(const TUnaryOpExpression* unaryExpr, TArgs... args)
    {
        Visit(unaryExpr->Operand, args...);
    }

    void OnLhs(const TBinaryOpExpression* binaryExpr, TArgs... args)
    {
        Visit(binaryExpr->Lhs, args...);
    }

    void OnRhs(const TBinaryOpExpression* binaryExpr, TArgs... args)
    {
        Visit(binaryExpr->Rhs, args...);
    }

    void OnDefaultExpression(const TTransformExpression* transformExpr, TArgs... args)
    {
        if (const auto& defaultExpression = transformExpr->DefaultExpression) {
            Builder->AppendString(", ");
            Visit(defaultExpression, args...);
        }
    }

    template <class T>
    void OnArguments(const T* expr, TArgs... args)
    {
        bool needComma = false;
        for (const auto& argument : expr->Arguments) {
            if (needComma) {
                Builder->AppendString(", ");
            }
            Visit(argument, args...);
            needComma = true;
        }
    }

    void OnLiteral(const TLiteralExpression* literalExpr, TArgs... /*args*/)
    {
        if (OmitValues) {
            Builder->AppendChar('?');
        } else {
            Builder->AppendString(ToString(static_cast<TValue>(literalExpr->Value)));
        }
    }

    void OnReference(const TReferenceExpression* referenceExpr, TArgs... /*args*/)
    {
        Builder->AppendString(referenceExpr->ColumnName);
    }

    void OnUnary(const TUnaryOpExpression* unaryExpr, TArgs... args)
    {
        Builder->AppendString(GetUnaryOpcodeLexeme(unaryExpr->Opcode));
        Builder->AppendChar(' ');

        auto needParenthesis = !CanOmitParenthesis(unaryExpr->Operand);
        if (needParenthesis) {
            Builder->AppendChar('(');
        }
        Derived()->OnOperand(unaryExpr, args...);
        if (needParenthesis) {
            Builder->AppendChar(')');
        }
    }

    void OnBinary(const TBinaryOpExpression* binaryExpr, TArgs... args)
    {
        auto needParenthesisLhs = !CanOmitParenthesis(binaryExpr->Lhs);
        if (needParenthesisLhs) {
            Builder->AppendChar('(');
        }
        Derived()->OnLhs(binaryExpr, args...);
        if (needParenthesisLhs) {
            Builder->AppendChar(')');
        }

        Builder->AppendChar(' ');
        Builder->AppendString(GetBinaryOpcodeLexeme(binaryExpr->Opcode));
        Builder->AppendChar(' ');

        auto needParenthesisRhs = !CanOmitParenthesis(binaryExpr->Rhs);
        if (needParenthesisRhs) {
            Builder->AppendChar('(');
        }
        Derived()->OnRhs(binaryExpr, args...);
        if (needParenthesisRhs) {
            Builder->AppendChar(')');
        }
    }

    void OnFunction(const TFunctionExpression* functionExpr, TArgs... args)
    {
        Builder->AppendString(functionExpr->FunctionName);
        Builder->AppendChar('(');
        Derived()->OnArguments(functionExpr, args...);
        Builder->AppendChar(')');
    }

    void OnIn(const TInExpression* inExpr, TArgs... args)
    {
        auto needParenthesis = inExpr->Arguments.size() > 1;
        if (needParenthesis) {
            Builder->AppendChar('(');
        }
        Derived()->OnArguments(inExpr, args...);
        if (needParenthesis) {
            Builder->AppendChar(')');
        }

        Builder->AppendString(" IN (");

        if (OmitValues) {
            Builder->AppendString("??");
        } else {
            JoinToString(
                Builder,
                inExpr->Values.begin(),
                inExpr->Values.end(),
                [&] (TStringBuilder* builder, const TRow& row) {
                    builder->AppendString(ToString(row));
                });
        }
        Builder->AppendChar(')');
    }

    void OnBetween(const TBetweenExpression* betweenExpr, TArgs... args)
    {
        auto needParenthesis = betweenExpr->Arguments.size() > 1;
        if (needParenthesis) {
            Builder->AppendChar('(');
        }
        Derived()->OnArguments(betweenExpr, args...);
        if (needParenthesis) {
            Builder->AppendChar(')');
        }

        Builder->AppendString(" BETWEEN (");

        if (OmitValues) {
            Builder->AppendString("??");
        } else {
            JoinToString(
                Builder,
                betweenExpr->Ranges.begin(),
                betweenExpr->Ranges.end(),
                [&] (TStringBuilder* builder, const TRowRange& range) {
                    builder->AppendString(ToString(range.first));
                    builder->AppendString(" AND ");
                    builder->AppendString(ToString(range.second));
                });
        }
        Builder->AppendChar(')');
    }

    void OnTransform(const TTransformExpression* transformExpr, TArgs... args)
    {
        Builder->AppendString("TRANSFORM(");
        size_t argumentCount = transformExpr->Arguments.size();
        auto needParenthesis = argumentCount > 1;
        if (needParenthesis) {
            Builder->AppendChar('(');
        }
        Derived()->OnArguments(transformExpr, args...);
        if (needParenthesis) {
            Builder->AppendChar(')');
        }

        Builder->AppendString(", (");
        if (OmitValues) {
            Builder->AppendString("??");
        } else {
            JoinToString(
                Builder,
                transformExpr->Values.begin(),
                transformExpr->Values.end(),
                [&] (TStringBuilder* builder, const TRow& row) {
                    builder->AppendChar('[');
                    JoinToString(
                        builder,
                        row.Begin(),
                        row.Begin() + argumentCount,
                        [] (TStringBuilder* builder, const TValue& value) {
                            builder->AppendString(ToString(value));
                        });
                    builder->AppendChar(']');
                });
        }
        Builder->AppendString("), (");

        if (OmitValues) {
            Builder->AppendString("??");
        } else {
            JoinToString(
                Builder,
                transformExpr->Values.begin(),
                transformExpr->Values.end(),
                [&] (TStringBuilder* builder, const TRow& row) {
                    builder->AppendString(ToString(row[argumentCount]));
                });
        }

        Builder->AppendChar(')');

        Derived()->OnDefaultExpression(transformExpr, args...);

        Builder->AppendChar(')');
    }

};

void ToProto(NProto::TQuery* serialized, const TConstQueryPtr& original);
void FromProto(TConstQueryPtr* original, const NProto::TQuery& serialized);

void ToProto(NProto::TQueryOptions* serialized, const TQueryOptions& original);
void FromProto(TQueryOptions* original, const NProto::TQueryOptions& serialized);

void ToProto(NProto::TDataRanges* serialized, const TDataRanges& original);
void FromProto(TDataRanges* original, const NProto::TDataRanges& serialized);

void ToProto(NTableClient::NProto::TTabletInfo* protoTabletInfo, const NTabletClient::TTabletInfo& tabletInfo);
void FromProto(NTabletClient::TTabletInfo* tabletInfo, const NTableClient::NProto::TTabletInfo& protoTabletInfo);

void ToProto(NProto::TTableMountInfo* mountInfoProto, const NTabletClient::TTableMountInfoPtr& mountInfo);
void FromProto(const NTabletClient::TTableMountInfoPtr& mountInfo, const NProto::TTableMountInfo& mountInfoProto);

TString InferName(TConstExpressionPtr expr, bool omitValues = false);
TString InferName(TConstBaseQueryPtr query, bool omitValues = false);

bool Compare(
    TConstExpressionPtr lhs,
    const TTableSchema& lhsSchema,
    TConstExpressionPtr rhs,
    const TTableSchema& rhsSchema,
    size_t maxIndex = std::numeric_limits<size_t>::max());

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
