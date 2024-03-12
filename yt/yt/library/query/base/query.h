#pragma once

#include "public.h"
#include "query_common.h"

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/misc/guid.h>
#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/range.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

//! Computes key index for a given column name.
int ColumnNameToKeyPartIndex(const TKeyColumns& keyColumns, const TString& columnName);

//! Derives type of reference expression based on table column type.
//!
//! For historical reasons reference expressions used to have `wire type` of column i.e.
//! if column had `Int16` type its reference would have `Int64` type.
//! `DeriveReferenceType` keeps this behaviour for V1 types, but for V3 types actual type is returned.
NTableClient::TLogicalTypePtr ToQLType(const NTableClient::TLogicalTypePtr& columnType);

struct TColumnDescriptor
{
    // Renamed column.
    // TODO: Do not keep name but restore name from table alias and column name from original schema.
    TString Name;
    // Index in schema.
    int Index;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EExpressionKind,
    ((None)                    (0))
    ((Literal)                 (1))
    ((Reference)               (2))
    ((Function)                (3))
    ((UnaryOp)                 (4))
    ((BinaryOp)                (5))
    ((In)                      (6))
    ((Transform)               (7))
    ((Between)                 (8))
    ((Case)                    (9))
    ((Like)                    (10))
    ((CompositeMemberAccessor) (11))
);

struct TExpression
    : public TRefCounted
{
    NTableClient::TLogicalTypePtr LogicalType;

    explicit TExpression(NTableClient::TLogicalTypePtr type);

    explicit TExpression(EValueType type);

    EValueType GetWireType() const;

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
    TOwningValue Value;

    explicit TLiteralExpression(EValueType type);

    TLiteralExpression(EValueType type, TOwningValue value);
};

struct TReferenceExpression
    : public TExpression
{
    TString ColumnName;

    explicit TReferenceExpression(const NTableClient::TLogicalTypePtr& type);

    TReferenceExpression(const NTableClient::TLogicalTypePtr& type, TString columnName);
};

struct TFunctionExpression
    : public TExpression
{
    TString FunctionName;
    std::vector<TConstExpressionPtr> Arguments;

    explicit TFunctionExpression(EValueType type);

    TFunctionExpression(
        EValueType type,
        TString functionName,
        std::vector<TConstExpressionPtr> arguments);
};

DEFINE_REFCOUNTED_TYPE(TFunctionExpression)

struct TAggregateFunctionExpression
    : public TReferenceExpression
{
    std::vector<TConstExpressionPtr> Arguments;
    EValueType StateType;
    EValueType ResultType;
    TString FunctionName;

    TAggregateFunctionExpression(
        const NTableClient::TLogicalTypePtr& type,
        TString exprName,
        std::vector<TConstExpressionPtr> arguments,
        EValueType stateType,
        EValueType resultType,
        TString functionName);
};

DEFINE_REFCOUNTED_TYPE(TAggregateFunctionExpression)

struct TUnaryOpExpression
    : public TExpression
{
    EUnaryOp Opcode;
    TConstExpressionPtr Operand;

    explicit TUnaryOpExpression(EValueType type);

    TUnaryOpExpression(
        EValueType type,
        EUnaryOp opcode,
        TConstExpressionPtr operand);
};

struct TBinaryOpExpression
    : public TExpression
{
    EBinaryOp Opcode;
    TConstExpressionPtr Lhs;
    TConstExpressionPtr Rhs;

    explicit TBinaryOpExpression(EValueType type);

    TBinaryOpExpression(
        EValueType type,
        EBinaryOp opcode,
        TConstExpressionPtr lhs,
        TConstExpressionPtr rhs);
};

struct TExpressionRowsetTag
{ };

struct TInExpression
    : public TExpression
{
    std::vector<TConstExpressionPtr> Arguments;
    TSharedRange<TRow> Values;

    explicit TInExpression(EValueType type);

    TInExpression(
        std::vector<TConstExpressionPtr> arguments,
        TSharedRange<TRow> values);
};

struct TBetweenExpression
    : public TExpression
{
    std::vector<TConstExpressionPtr> Arguments;
    TSharedRange<TRowRange> Ranges;

    explicit TBetweenExpression(EValueType type);

    TBetweenExpression(
        std::vector<TConstExpressionPtr> arguments,
        TSharedRange<TRowRange> ranges);
};

struct TTransformExpression
    : public TExpression
{
    std::vector<TConstExpressionPtr> Arguments;
    TSharedRange<TRow> Values;
    TConstExpressionPtr DefaultExpression;

    explicit TTransformExpression(EValueType type);

    TTransformExpression(
        EValueType type,
        std::vector<TConstExpressionPtr> arguments,
        TSharedRange<TRow> values,
        TConstExpressionPtr defaultExpression);
};

struct TCaseExpression
    : public TExpression
{
    TConstExpressionPtr OptionalOperand;
    std::vector<TWhenThenExpressionPtr> WhenThenExpressions;
    TConstExpressionPtr DefaultExpression;

    explicit TCaseExpression(EValueType type)
        : TExpression(type)
    { }

    TCaseExpression(
        EValueType type,
        TConstExpressionPtr optionalOperand,
        std::vector<TWhenThenExpressionPtr> whenThenExpressions,
        TConstExpressionPtr defaultExpression)
        : TExpression(type)
        , OptionalOperand(std::move(optionalOperand))
        , WhenThenExpressions(std::move(whenThenExpressions))
        , DefaultExpression(std::move(defaultExpression))
    { }
};

struct TLikeExpression
    : public TExpression
{
    TConstExpressionPtr Text;
    EStringMatchOp Opcode;
    TConstExpressionPtr Pattern;
    TConstExpressionPtr EscapeCharacter;

    explicit TLikeExpression(EValueType type)
        : TExpression(type)
    { }

    TLikeExpression(
        TConstExpressionPtr text,
        EStringMatchOp opcode,
        TConstExpressionPtr pattern,
        TConstExpressionPtr escapeCharacter)
        : TExpression(EValueType::Boolean)
        , Text(std::move(text))
        , Opcode(opcode)
        , Pattern(std::move(pattern))
        , EscapeCharacter(std::move(escapeCharacter))
    { }
};

struct TCompositeMemberAccessorPath
{
    std::vector<NTableClient::ELogicalMetatype> NestedTypes;
    std::vector<TString> NamedStructMembers;
    std::vector<int> PositionalStructMembers;
    std::vector<int> TupleItemIndices;

    void AppendStructMember(const TString& name, int position);
    void AppendTupleItem(int index);
    void Reserve(int length);

    bool operator == (const TCompositeMemberAccessorPath& other) const = default;
};

struct TCompositeMemberAccessorExpression
    : public TExpression
{
    using TDictOrListItemAccessorExpression = TConstExpressionPtr;

    TConstExpressionPtr CompositeExpression;
    TCompositeMemberAccessorPath NestedStructOrTupleItemAccessor;
    TDictOrListItemAccessorExpression DictOrListItemAccessor;

    explicit TCompositeMemberAccessorExpression(NTableClient::TLogicalTypePtr type)
        : TExpression(std::move(type))
    { }

    TCompositeMemberAccessorExpression(
        NTableClient::TLogicalTypePtr type,
        TConstExpressionPtr compositeExpression,
        TCompositeMemberAccessorPath nestedStructOrTupleItemAccess,
        TDictOrListItemAccessorExpression dictOrListItemAccess)
        : TExpression(std::move(type))
        , CompositeExpression(std::move(compositeExpression))
        , NestedStructOrTupleItemAccessor(std::move(nestedStructOrTupleItemAccess))
        , DictOrListItemAccessor(std::move(dictOrListItemAccess))
    { }
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
    TConstExpressionPtr Expression;
    TString Name;

    TNamedItem() = default;

    TNamedItem(TConstExpressionPtr expression, TString name);
};

using TNamedItemList = std::vector<TNamedItem>;

struct TAggregateItem
{
    std::vector<TConstExpressionPtr> Arguments;
    TString Name;
    TString AggregateFunction;
    EValueType StateType;
    EValueType ResultType;

    TAggregateItem() = default;

    TAggregateItem(
        std::vector<TConstExpressionPtr> arguments,
        TString aggregateFunction,
        TString name,
        EValueType stateType,
        EValueType resultType);
};

using TAggregateItemList = std::vector<TAggregateItem>;

////////////////////////////////////////////////////////////////////////////////

struct TMappedSchema
{
    TTableSchemaPtr Original;
    std::vector<TColumnDescriptor> Mapping;

    std::vector<TColumnDescriptor> GetOrderedSchemaMapping() const;

    TKeyColumns GetKeyColumns() const;

    TTableSchemaPtr GetRenamedSchema() const;
};

struct TSelfEquation
{
    TConstExpressionPtr Expression;
    bool Evaluated;
};

struct TJoinClause
    : public TRefCounted
{
    TMappedSchema Schema;
    THashSet<TString> SelfJoinedColumns;
    THashSet<TString> ForeignJoinedColumns;

    TConstExpressionPtr Predicate;

    std::vector<TConstExpressionPtr> ForeignEquations;
    std::vector<TSelfEquation> SelfEquations;

    size_t CommonKeyPrefix = 0;
    size_t ForeignKeyPrefix = 0;

    // TODO(sabdenovch): introduce TArrayJoinClause and TTableJoinClause.
    // Currently non-empty ArrayExpressions renders fields *Prefix, *Id, *Equations and Predicate meaningless.
    std::vector<TConstExpressionPtr> ArrayExpressions;

    bool IsLeft = false;

    //! See #TDataSource::ObjectId.
    NObjectClient::TObjectId ForeignObjectId;
    //! See #TDataSource::CellId.
    NObjectClient::TCellId ForeignCellId;

    TTableSchemaPtr GetRenamedSchema() const;

    TKeyColumns GetKeyColumns() const;

    TTableSchemaPtr GetTableSchema(const TTableSchema& source) const;
};

DEFINE_REFCOUNTED_TYPE(TJoinClause)

struct TGroupClause
    : public TRefCounted
{
    TNamedItemList GroupItems;
    TAggregateItemList AggregateItems;
    ETotalsMode TotalsMode;
    size_t CommonPrefixWithPrimaryKey = 0;

    void AddGroupItem(TNamedItem namedItem);

    void AddGroupItem(TConstExpressionPtr expression, TString name);

    TTableSchemaPtr GetTableSchema(bool isFinal) const;
};

DEFINE_REFCOUNTED_TYPE(TGroupClause)

struct TOrderItem
{
    TConstExpressionPtr Expression;
    bool Descending;
};

struct TOrderClause
    : public TRefCounted
{
    std::vector<TOrderItem> OrderItems;
};

DEFINE_REFCOUNTED_TYPE(TOrderClause)

struct TProjectClause
    : public TRefCounted
{
    TNamedItemList Projections;

    void AddProjection(TNamedItem namedItem);

    void AddProjection(TConstExpressionPtr expression, TString name);

    TTableSchemaPtr GetTableSchema() const;
};

DEFINE_REFCOUNTED_TYPE(TProjectClause)

struct TWhenThenExpression
    : public TRefCounted
{
    TConstExpressionPtr Condition;
    TConstExpressionPtr Result;

    TWhenThenExpression() = default;

    TWhenThenExpression(TConstExpressionPtr condition, TConstExpressionPtr result)
        : Condition(std::move(condition))
        , Result(std::move(result))
    { }
};

DEFINE_REFCOUNTED_TYPE(TWhenThenExpression)

// Front Query is not Coordinatable
// IsMerge is always true for front Query and false for Bottom Query

struct TBaseQuery
    : public TRefCounted
{
    TGuid Id;

    // Merge and Final
    bool IsFinal = true;

    TConstGroupClausePtr GroupClause;
    TConstExpressionPtr HavingClause;
    TConstOrderClausePtr OrderClause;

    TConstProjectClausePtr ProjectClause;

    i64 Offset = 0;

    // TODO: Update protocol and fix it
    // If Limit == std::numeric_limits<i64>::max() - 1, then do ordered read with prefetch
    i64 Limit = std::numeric_limits<i64>::max();

    // True if the grouping key uses each column of primary key.
    // In this case, some additional optimizations can be applied.
    bool UseDisjointGroupBy = false;

    bool InferRanges = true;

    explicit TBaseQuery(TGuid id = TGuid::Create());

    TBaseQuery(const TBaseQuery& other);

    bool IsOrdered() const;

    virtual TTableSchemaPtr GetReadSchema() const = 0;
    virtual TTableSchemaPtr GetTableSchema() const = 0;
};

DEFINE_REFCOUNTED_TYPE(TBaseQuery)

struct TQuery
    : public TBaseQuery
{
    TMappedSchema Schema;

    // Bottom
    std::vector<TConstJoinClausePtr> JoinClauses;
    TConstExpressionPtr WhereClause;

    explicit TQuery(TGuid id = TGuid::Create());

    TQuery(const TQuery& other) = default;

    TKeyColumns GetKeyColumns() const;

    TTableSchemaPtr GetReadSchema() const override;

    TTableSchemaPtr GetRenamedSchema() const;

    TTableSchemaPtr GetTableSchema() const override;
};

DEFINE_REFCOUNTED_TYPE(TQuery)

struct TFrontQuery
    : public TBaseQuery
{
    explicit TFrontQuery(TGuid id = TGuid::Create());

    TFrontQuery(const TFrontQuery& other) = default;

    TTableSchemaPtr Schema;

    TTableSchemaPtr GetReadSchema() const override;

    TTableSchemaPtr GetRenamedSchema() const;

    TTableSchemaPtr GetTableSchema() const override;
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
        } else if (auto caseExpr = expr->template As<TCaseExpression>()) {
            return Derived()->OnCase(caseExpr, args...);
        } else if (auto likeExpr = expr->template As<TLikeExpression>()) {
            return Derived()->OnLike(likeExpr, args...);
        } else if (auto memberAccessorExpr = expr->template As<TCompositeMemberAccessorExpression>()) {
            return Derived()->OnCompositeMemberAccessor(memberAccessorExpr, args...);
        }
        YT_ABORT();
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

    void OnCase(const TCaseExpression* caseExpr)
    {
        for (const auto& whenThenExpression : caseExpr->WhenThenExpressions) {
            Visit(whenThenExpression->Condition);
            Visit(whenThenExpression->Result);
        }
    }

    void OnLike(const TLikeExpression* likeExpr)
    {
        Visit(likeExpr->Text);
        Visit(likeExpr->Pattern);
        Visit(likeExpr->EscapeCharacter);
    }

    void OnCompositeMemberAccessor(const TCompositeMemberAccessorExpression* memberAccessorExpr)
    {
        Visit(memberAccessorExpr->CompositeExpression);
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
            unaryExpr->GetWireType(),
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
            binaryExpr->GetWireType(),
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
            functionExpr->GetWireType(),
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
            transformExpr->GetWireType(),
            std::move(newArguments),
            transformExpr->Values,
            newDefaultExpression);
    }

    TConstExpressionPtr OnCase(const TCaseExpression* caseExpr)
    {
        bool allEqual = true;

        TConstExpressionPtr newOptionalOperand;
        if (const auto& optionalOperand = caseExpr->OptionalOperand) {
            newOptionalOperand = Visit(optionalOperand);
            allEqual = allEqual && newOptionalOperand == optionalOperand;
        }

        std::vector<TWhenThenExpressionPtr> newWhenThenExpressions;
        for (auto& caseClause : caseExpr->WhenThenExpressions) {
            auto newCondition = Visit(caseClause->Condition);
            allEqual = allEqual && newCondition == caseClause->Condition;

            auto newResult = Visit(caseClause->Result);
            allEqual = allEqual && newResult == caseClause->Result;

            newWhenThenExpressions.emplace_back(New<TWhenThenExpression>(newCondition, newResult));
        }

        TConstExpressionPtr newDefault;
        if (const auto& defaultExpression = caseExpr->DefaultExpression) {
            newDefault = Visit(defaultExpression);
            allEqual = allEqual && newDefault == defaultExpression;
        }

        if (allEqual) {
            return caseExpr;
        }

        return New<TCaseExpression>(
            caseExpr->GetWireType(),
            std::move(newOptionalOperand),
            std::move(newWhenThenExpressions),
            newDefault);
    }

    TConstExpressionPtr OnLike(const TLikeExpression* likeExpr)
    {
        bool allEqual = true;

        auto newText = Visit(likeExpr->Text);
        allEqual = allEqual && newText == likeExpr->Text;

        auto newPattern = Visit(likeExpr->Pattern);
        allEqual = allEqual && newPattern == likeExpr->Pattern;

        TConstExpressionPtr newEscapeCharacter;
        if (const auto& escapeCharacter = likeExpr->EscapeCharacter) {
            newEscapeCharacter = Visit(likeExpr->EscapeCharacter);
            allEqual = allEqual && newEscapeCharacter == likeExpr->EscapeCharacter;
        }

        if (allEqual) {
            return likeExpr;
        }

        return New<TLikeExpression>(
            std::move(newText),
            likeExpr->Opcode,
            std::move(newPattern),
            std::move(newEscapeCharacter));
    }

    TConstExpressionPtr OnCompositeMemberAccessor(const TCompositeMemberAccessorExpression* memberAccessorExpr)
    {
        auto newCompositeExpression = Visit(memberAccessorExpr->CompositeExpression);
        bool allEqual = newCompositeExpression == memberAccessorExpr->CompositeExpression;

        auto newDictOrListItemAccessor = TCompositeMemberAccessorExpression::TDictOrListItemAccessorExpression{};
        if (memberAccessorExpr->DictOrListItemAccessor) {
            newDictOrListItemAccessor = Visit(memberAccessorExpr->DictOrListItemAccessor);
            allEqual = allEqual && newDictOrListItemAccessor == memberAccessorExpr->DictOrListItemAccessor;
        }

        if (allEqual) {
            return memberAccessorExpr;
        }

        return New<TCompositeMemberAccessorExpression>(
            memberAccessorExpr->LogicalType,
            std::move(newCompositeExpression),
            memberAccessorExpr->NestedStructOrTupleItemAccessor,
            std::move(newDictOrListItemAccessor));
    }
};

template <class TDerived, class TNode, class... TArgs>
struct TAbstractExpressionPrinter
    : TAbstractVisitor<void, TDerived, TNode, TArgs...>
{
    using TBase = TAbstractVisitor<void, TDerived, TNode, TArgs...>;
    using TBase::Derived;
    using TBase::Visit;

    TStringBuilderBase* Builder;
    bool OmitValues;

    TAbstractExpressionPrinter(TStringBuilderBase* builder, bool omitValues)
        : Builder(builder)
        , OmitValues(omitValues)
    { }

    static int GetOpPriority(EBinaryOp op)
    {
        switch (op) {
        case EBinaryOp::Multiply:
        case EBinaryOp::Divide:
        case EBinaryOp::Modulo:
            return 0;

        case EBinaryOp::Plus:
        case EBinaryOp::Minus:
        case EBinaryOp::Concatenate:
            return 1;

        case EBinaryOp::LeftShift:
        case EBinaryOp::RightShift:
            return 2;

        case EBinaryOp::BitAnd:
            return 3;

        case EBinaryOp::BitOr:
            return 4;

        case EBinaryOp::Equal:
        case EBinaryOp::NotEqual:
        case EBinaryOp::Less:
        case EBinaryOp::LessOrEqual:
        case EBinaryOp::Greater:
        case EBinaryOp::GreaterOrEqual:
            return 5;

        case EBinaryOp::And:
            return 6;

        case EBinaryOp::Or:
            return 7;

        default:
            YT_ABORT();
        }
    }

    static bool CanOmitParenthesis(TConstExpressionPtr expr)
    {
        return
            expr->As<TLiteralExpression>() ||
            expr->As<TReferenceExpression>() ||
            expr->As<TFunctionExpression>() ||
            expr->As<TUnaryOpExpression>() ||
            expr->As<TTransformExpression>() ||
            expr->As<TCaseExpression>() ||
            expr->As<TLikeExpression>() ||
            expr->As<TCompositeMemberAccessorExpression>();
    }

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

    void OnOptionalOperand(const TCaseExpression* caseExpr, TArgs... args)
    {
        if (const auto& optionalOperand = caseExpr->OptionalOperand) {
            Builder->AppendChar(' ');
            Visit(optionalOperand, args...);
        }
    }

    void OnWhenThenExpressions(const TCaseExpression* caseExpr, TArgs... args)
    {
        for (const auto& caseClause : caseExpr->WhenThenExpressions) {
            Builder->AppendString(" WHEN ");
            Visit(caseClause->Condition, args...);
            Builder->AppendString(" THEN ");
            Visit(caseClause->Result, args...);
        }
    }

    void OnDefaultExpression(const TCaseExpression* caseExpr, TArgs... args)
    {
        if (const auto& defaultExpression = caseExpr->DefaultExpression) {
            Builder->AppendString(" ELSE ");
            Visit(defaultExpression, args...);
        }
    }

    void OnLikeText(const TLikeExpression* likeExpr, TArgs... args)
    {
        Visit(likeExpr->Text, args...);
    }

    void OnLikePattern(const TLikeExpression* likeExpr, TArgs... args)
    {
        Visit(likeExpr->Pattern, args...);
    }

    void OnLikeEscapeCharacter(const TLikeExpression* likeExpr, TArgs... args)
    {
        Visit(likeExpr->EscapeCharacter, args...);
    }

    void OnCompositeMemberAccessorColumnReference(const TCompositeMemberAccessorExpression* memberAccessorExpr, TArgs... args)
    {
        Visit(memberAccessorExpr->CompositeExpression, args...);
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
            if (const auto* lhs = binaryExpr->Lhs->As<TBinaryOpExpression>()) {
                if (GetOpPriority(lhs->Opcode) <= GetOpPriority(binaryExpr->Opcode)) {
                    needParenthesisLhs = false;
                }
            }
        }

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
            if (const auto* rhs = binaryExpr->Rhs->As<TBinaryOpExpression>()) {
                if (GetOpPriority(rhs->Opcode) <= GetOpPriority(binaryExpr->Opcode)) {
                    needParenthesisRhs = false;
                }
            }
        }

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
                [&] (TStringBuilderBase* builder, const TRow& row) {
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
                [&] (TStringBuilderBase* builder, const TRowRange& range) {
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
                [&] (TStringBuilderBase* builder, const TRow& row) {
                    builder->AppendChar('[');
                    JoinToString(
                        builder,
                        row.Begin(),
                        row.Begin() + argumentCount,
                        [] (TStringBuilderBase* builder, const TValue& value) {
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
                [&] (TStringBuilderBase* builder, const TRow& row) {
                    builder->AppendString(ToString(row[argumentCount]));
                });
        }

        Builder->AppendChar(')');

        Derived()->OnDefaultExpression(transformExpr, args...);

        Builder->AppendChar(')');
    }

    void OnCase(const TCaseExpression* caseExpr, TArgs... args)
    {
        Builder->AppendString("CASE");
        Derived()->OnOptionalOperand(caseExpr, args...);
        Derived()->OnWhenThenExpressions(caseExpr, args...);
        Derived()->OnDefaultExpression(caseExpr, args...);
        Builder->AppendString(" END");
    }

    void OnLike(const TLikeExpression* likeExpr, TArgs... args)
    {
        Derived()->OnLikeText(likeExpr, args...);

        Builder->AppendChar(' ');
        Builder->AppendString(GetStringMatchOpcodeLexeme(likeExpr->Opcode));
        Builder->AppendChar(' ');

        Derived()->OnLikePattern(likeExpr, args...);

        if (likeExpr->EscapeCharacter) {
            YT_ASSERT(likeExpr->Opcode != EStringMatchOp::Regex);
            Builder->AppendString(" ESCAPE ");
            Derived()->OnLikeEscapeCharacter(likeExpr, args...);
        }
    }

    void OnCompositeMemberAccessor(const TCompositeMemberAccessorExpression* memberAccessorExpr, TArgs... args)
    {
        Derived()->OnCompositeMemberAccessorColumnReference(memberAccessorExpr, args...);
    }
};

void ToProto(NProto::TQuery* serialized, const TConstQueryPtr& original);
void FromProto(TConstQueryPtr* original, const NProto::TQuery& serialized);

void ToProto(NProto::TQueryOptions* serialized, const TQueryOptions& original);
void FromProto(TQueryOptions* original, const NProto::TQueryOptions& serialized);

void ToProto(NProto::TDataSource* serialized, const TDataSource& original);
void FromProto(
    TDataSource* original,
    const NProto::TDataSource& serialized,
    IMemoryChunkProviderPtr memoryChunkProvider = GetDefaultMemoryChunkProvider());

struct TInferNameOptions
{
    bool OmitValues = false;
    bool OmitAliases = false;
    bool OmitJoinPredicate = false;
    bool OmitOffsetAndLimit = false;
};

TString InferName(TConstExpressionPtr expr, bool omitValues = false);
TString InferName(TConstBaseQueryPtr query, TInferNameOptions options = {});

bool Compare(
    TConstExpressionPtr lhs,
    const TTableSchema& lhsSchema,
    TConstExpressionPtr rhs,
    const TTableSchema& rhsSchema,
    size_t maxIndex = std::numeric_limits<size_t>::max());

std::vector<size_t> GetJoinGroups(
    const std::vector<TConstJoinClausePtr>& joinClauses,
    TTableSchemaPtr schema);

NLogging::TLogger MakeQueryLogger(TGuid queryId);
NLogging::TLogger MakeQueryLogger(TConstBaseQueryPtr query);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
