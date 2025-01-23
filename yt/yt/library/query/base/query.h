#pragma once

#include "public.h"
#include "query_common.h"

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/misc/guid.h>
#include <yt/yt/core/misc/property.h>

#include <library/cpp/yt/memory/range.h>

namespace NYT::NQueryClient {

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
    std::string ColumnName;

    explicit TReferenceExpression(const NTableClient::TLogicalTypePtr& type);

    TReferenceExpression(const NTableClient::TLogicalTypePtr& type, const std::string& columnName);
};

struct TFunctionExpression
    : public TExpression
{
    std::string FunctionName;
    std::vector<TConstExpressionPtr> Arguments;

    explicit TFunctionExpression(EValueType type);

    TFunctionExpression(
        EValueType type,
        const std::string& functionName,
        std::vector<TConstExpressionPtr> arguments);
};

DEFINE_REFCOUNTED_TYPE(TFunctionExpression)

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

struct TWhenThenExpression
    : public TRefCounted
{
    TConstExpressionPtr Condition;
    TConstExpressionPtr Result;

    TWhenThenExpression() = default;

    TWhenThenExpression(TConstExpressionPtr condition, TConstExpressionPtr result);
};

DEFINE_REFCOUNTED_TYPE(TWhenThenExpression)

struct TCaseExpression
    : public TExpression
{
    TConstExpressionPtr OptionalOperand;
    std::vector<TWhenThenExpressionPtr> WhenThenExpressions;
    TConstExpressionPtr DefaultExpression;

    explicit TCaseExpression(EValueType type);

    TCaseExpression(
        EValueType type,
        TConstExpressionPtr optionalOperand,
        std::vector<TWhenThenExpressionPtr> whenThenExpressions,
        TConstExpressionPtr defaultExpression);
};

struct TLikeExpression
    : public TExpression
{
    TConstExpressionPtr Text;
    EStringMatchOp Opcode;
    TConstExpressionPtr Pattern;
    TConstExpressionPtr EscapeCharacter;

    explicit TLikeExpression(EValueType type);

    TLikeExpression(
        TConstExpressionPtr text,
        EStringMatchOp opcode,
        TConstExpressionPtr pattern,
        TConstExpressionPtr escapeCharacter);
};

using TStructMemberAccessor = TString;
using TTupleItemIndexAccessor = i64;

struct TCompositeMemberAccessorPath
{
    std::vector<NTableClient::ELogicalMetatype> NestedTypes;
    std::vector<TStructMemberAccessor> NamedStructMembers;
    std::vector<int> PositionalStructMembers;
    std::vector<int> TupleItemIndices;

    void AppendStructMember(TStructMemberAccessor name, int position);
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

    explicit TCompositeMemberAccessorExpression(NTableClient::TLogicalTypePtr type);

    TCompositeMemberAccessorExpression(
        NTableClient::TLogicalTypePtr type,
        TConstExpressionPtr compositeExpression,
        TCompositeMemberAccessorPath nestedStructOrTupleItemAccess,
        TDictOrListItemAccessorExpression dictOrListItemAccess);
};

////////////////////////////////////////////////////////////////////////////////

struct TNamedItem
{
    TConstExpressionPtr Expression;
    std::string Name;

    TNamedItem() = default;
    TNamedItem(TConstExpressionPtr expression, const std::string& name);
};

using TNamedItemList = std::vector<TNamedItem>;

struct TAggregateItem
{
    std::vector<TConstExpressionPtr> Arguments;
    std::string Name;
    std::string AggregateFunction;
    EValueType StateType;
    EValueType ResultType;

    TAggregateItem() = default;

    TAggregateItem(
        std::vector<TConstExpressionPtr> arguments,
        const std::string& aggregateFunction,
        const std::string& name,
        EValueType stateType,
        EValueType resultType);
};

using TAggregateItemList = std::vector<TAggregateItem>;

struct TGroupClause
    : public TRefCounted
{
    TNamedItemList GroupItems;
    TAggregateItemList AggregateItems;
    ETotalsMode TotalsMode;
    size_t CommonPrefixWithPrimaryKey = 0;

    void AddGroupItem(TNamedItem namedItem);

    void AddGroupItem(TConstExpressionPtr expression, const std::string& name);

    TTableSchemaPtr GetTableSchema(bool isFinal) const;

    bool AllAggregatesAreFirst() const;
};

DEFINE_REFCOUNTED_TYPE(TGroupClause)

////////////////////////////////////////////////////////////////////////////////

struct TColumnDescriptor
{
    // Renamed column.
    // TODO: Do not keep name but restore name from table alias and column name from original schema.
    std::string Name;
    // Index in schema.
    int Index;
};

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
    THashSet<std::string> SelfJoinedColumns;
    THashSet<std::string> ForeignJoinedColumns;

    TConstExpressionPtr Predicate;

    std::vector<TConstExpressionPtr> ForeignEquations;
    std::vector<TSelfEquation> SelfEquations;

    size_t CommonKeyPrefix = 0;
    size_t ForeignKeyPrefix = 0;

    // TODO(sabdenovch): introduce TArrayJoinClause and TTableJoinClause.
    // Currently non-empty ArrayExpressions renders fields *KeyPrefix, Foreign*Id and *Equations meaningless.
    std::vector<TConstExpressionPtr> ArrayExpressions;

    bool IsLeft = false;

    //! See #TDataSource::ObjectId.
    NObjectClient::TObjectId ForeignObjectId;
    //! See #TDataSource::CellId.
    NObjectClient::TCellId ForeignCellId;

    TTableSchemaPtr GetRenamedSchema() const;

    TKeyColumns GetKeyColumns() const;

    TTableSchemaPtr GetTableSchema(const TTableSchema& source) const;

    TQueryPtr GetJoinSubquery() const;

    std::vector<size_t> GetForeignColumnIndices() const;
};

DEFINE_REFCOUNTED_TYPE(TJoinClause)

std::vector<size_t> GetJoinGroups(
    const std::vector<TConstJoinClausePtr>& joinClauses,
    TTableSchemaPtr schema);

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

struct TProjectClause
    : public TRefCounted
{
    TNamedItemList Projections;

    void AddProjection(TNamedItem namedItem);

    void AddProjection(TConstExpressionPtr expression, const std::string& name);

    TTableSchemaPtr GetTableSchema() const;
};

DEFINE_REFCOUNTED_TYPE(TProjectClause)

////////////////////////////////////////////////////////////////////////////////

// Front Query is not Coordinatable
// IsMerge is always true for front Query and false for Bottom Query

constexpr i64 UnorderedReadHint = std::numeric_limits<i64>::max();
constexpr i64 OrderedReadWithPrefetchHint = std::numeric_limits<i64>::max() - 1;

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
    i64 Limit = UnorderedReadHint;

    // True if the grouping key uses each column of primary key.
    // In this case, some additional optimizations can be applied.
    bool UseDisjointGroupBy = false;

    bool InferRanges = true;

    bool ForceLightRangeInference = false;

    explicit TBaseQuery(TGuid id = TGuid::Create());

    TBaseQuery(const TBaseQuery& other);

    bool IsOrdered(const TFeatureFlags& featureFlags) const;

    bool IsPrefetching() const;

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

void ToProto(NProto::TQuery* serialized, const TConstQueryPtr& original);
void FromProto(TConstQueryPtr* original, const NProto::TQuery& serialized);

////////////////////////////////////////////////////////////////////////////////

struct TInferNameOptions
{
    bool OmitValues = false;
    bool OmitAliases = false;
    bool OmitJoinPredicate = false;
    bool OmitOffsetAndLimit = false;
};

TString InferName(TConstExpressionPtr expr, bool omitValues = false);
TString InferName(TConstBaseQueryPtr query, TInferNameOptions options = {});

////////////////////////////////////////////////////////////////////////////////

class TSchemaAwareReferenceComparer
{
public:
    TSchemaAwareReferenceComparer(const TTableSchema& lhsSchema, const TTableSchema& rhsSchema, int maxIndex);

    bool operator()(const std::string& lhs, const std::string& rhs);

private:
    const TTableSchema& LhsSchema_;
    const TTableSchema& RhsSchema_;
    const int MaxIndex_;
};

struct TReferenceComparer
{
    bool operator()(const std::string& lhs, const std::string& rhs);
};

bool Compare(
    TConstExpressionPtr lhs,
    TConstExpressionPtr rhs,
    std::function<bool(const std::string&, const std::string&)> comparer = TReferenceComparer());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
