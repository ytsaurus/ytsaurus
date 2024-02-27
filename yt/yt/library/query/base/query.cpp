#include "query.h"
#include "private.h"

#include <yt/yt/library/query/proto/query.pb.h>

#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/core/ytree/serialize.h>
#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/yt/misc/cast.h>

#include <limits>

namespace NYT::NQueryClient {

using namespace NTableClient;
using namespace NObjectClient;

using NYT::ToProto;
using NYT::FromProto;

using NCodegen::EExecutionBackend;

////////////////////////////////////////////////////////////////////////////////

//! Computes key index for a given column name.
int ColumnNameToKeyPartIndex(const TKeyColumns& keyColumns, const TString& columnName)
{
    for (int index = 0; index < std::ssize(keyColumns); ++index) {
        if (keyColumns[index] == columnName) {
            return index;
        }
    }
    return -1;
}

TLogicalTypePtr ToQLType(const NTableClient::TLogicalTypePtr& columnType)
{
    if (IsV1Type(columnType)) {
        const auto wireType = GetWireType(columnType);
        return MakeLogicalType(GetLogicalType(wireType), false);
    } else {
        return columnType;
    }
}

////////////////////////////////////////////////////////////////////////////////

TExpression::TExpression(NTableClient::TLogicalTypePtr type)
    : LogicalType(std::move(type))
{ }

TExpression::TExpression(EValueType type)
    : LogicalType(MakeLogicalType(GetLogicalType(type), false))
{ }

EValueType TExpression::GetWireType() const
{
    return NTableClient::GetWireType(LogicalType);
}

////////////////////////////////////////////////////////////////////////////////

TLiteralExpression::TLiteralExpression(EValueType type)
    : TExpression(type)
{ }

TLiteralExpression::TLiteralExpression(EValueType type, TOwningValue value)
    : TExpression(type)
    , Value(std::move(value))
{ }

////////////////////////////////////////////////////////////////////////////////

TReferenceExpression::TReferenceExpression(const NTableClient::TLogicalTypePtr& type)
    : TExpression(ToQLType(type))
{ }

TReferenceExpression::TReferenceExpression(const NTableClient::TLogicalTypePtr& type, TString columnName)
    : TExpression(ToQLType(type))
    , ColumnName(std::move(columnName))
{ }

////////////////////////////////////////////////////////////////////////////////

TFunctionExpression::TFunctionExpression(EValueType type)
    : TExpression(type)
{ }

TFunctionExpression::TFunctionExpression(
    EValueType type,
    TString functionName,
    std::vector<TConstExpressionPtr> arguments)
    : TExpression(type)
    , FunctionName(std::move(functionName))
    , Arguments(std::move(arguments))
{ }

////////////////////////////////////////////////////////////////////////////////

TAggregateFunctionExpression::TAggregateFunctionExpression(
    const NTableClient::TLogicalTypePtr& type,
    TString exprName,
    std::vector<TConstExpressionPtr> arguments,
    EValueType stateType,
    EValueType resultType,
    TString functionName)
    : TReferenceExpression(type, std::move(exprName))
    , Arguments(std::move(arguments))
    , StateType(stateType)
    , ResultType(resultType)
    , FunctionName(std::move(functionName))
{ }

////////////////////////////////////////////////////////////////////////////////

TUnaryOpExpression::TUnaryOpExpression(EValueType type)
    : TExpression(type)
{ }

TUnaryOpExpression::TUnaryOpExpression(
    EValueType type,
    EUnaryOp opcode,
    TConstExpressionPtr operand)
    : TExpression(type)
    , Opcode(opcode)
    , Operand(operand)
{ }

////////////////////////////////////////////////////////////////////////////////

TBinaryOpExpression::TBinaryOpExpression(EValueType type)
    : TExpression(type)
{ }

TBinaryOpExpression::TBinaryOpExpression(
    EValueType type,
    EBinaryOp opcode,
    TConstExpressionPtr lhs,
    TConstExpressionPtr rhs)
    : TExpression(type)
    , Opcode(opcode)
    , Lhs(lhs)
    , Rhs(rhs)
{ }

////////////////////////////////////////////////////////////////////////////////

TInExpression::TInExpression(EValueType type)
    : TExpression(type)
{
    YT_VERIFY(type == EValueType::Boolean);
}

TInExpression::TInExpression(
    std::vector<TConstExpressionPtr> arguments,
    TSharedRange<TRow> values)
    : TExpression(EValueType::Boolean)
    , Arguments(std::move(arguments))
    , Values(std::move(values))
{ }

////////////////////////////////////////////////////////////////////////////////

TBetweenExpression::TBetweenExpression(EValueType type)
    : TExpression(type)
{
    YT_VERIFY(type == EValueType::Boolean);
}

TBetweenExpression::TBetweenExpression(
    std::vector<TConstExpressionPtr> arguments,
    TSharedRange<TRowRange> ranges)
    : TExpression(EValueType::Boolean)
    , Arguments(std::move(arguments))
    , Ranges(std::move(ranges))
{ }

////////////////////////////////////////////////////////////////////////////////

TTransformExpression::TTransformExpression(EValueType type)
    : TExpression(type)
{ }

TTransformExpression::TTransformExpression(
    EValueType type,
    std::vector<TConstExpressionPtr> arguments,
    TSharedRange<TRow> values,
    TConstExpressionPtr defaultExpression)
    : TExpression(type)
    , Arguments(std::move(arguments))
    , Values(std::move(values))
    , DefaultExpression(std::move(defaultExpression))
{ }

////////////////////////////////////////////////////////////////////////////////

void TCompositeMemberAccessorPath::AppendStructMember(const TString& name, int position)
{
    NestedTypes.push_back(ELogicalMetatype::Struct);
    NamedStructMembers.push_back(name);
    PositionalStructMembers.push_back(position);
    TupleItemIndices.push_back(-1); // Dummy.
}

void TCompositeMemberAccessorPath::AppendTupleItem(int index)
{
    NestedTypes.push_back(ELogicalMetatype::Tuple);
    NamedStructMembers.emplace_back(); // Dummy.
    PositionalStructMembers.push_back(-1); // Dummy.
    TupleItemIndices.push_back(index);
}

void TCompositeMemberAccessorPath::Reserve(int length)
{
    NestedTypes.reserve(length);
    NamedStructMembers.reserve(length);
    PositionalStructMembers.reserve(length);
    TupleItemIndices.reserve(length);
}

////////////////////////////////////////////////////////////////////////////////

TNamedItem::TNamedItem(
    TConstExpressionPtr expression,
    TString name)
    : Expression(std::move(expression))
    , Name(std::move(name))
{ }

////////////////////////////////////////////////////////////////////////////////

TAggregateItem::TAggregateItem(
    std::vector<TConstExpressionPtr> arguments,
    TString aggregateFunction,
    TString name,
    EValueType stateType,
    EValueType resultType)
    : Arguments(std::move(arguments))
    , Name(std::move(name))
    , AggregateFunction(std::move(aggregateFunction))
    , StateType(stateType)
    , ResultType(resultType)
{ }

////////////////////////////////////////////////////////////////////////////////

std::vector<TColumnDescriptor> TMappedSchema::GetOrderedSchemaMapping() const
{
    auto orderedSchemaMapping = Mapping;
    std::sort(orderedSchemaMapping.begin(), orderedSchemaMapping.end(),
        [] (const TColumnDescriptor& lhs, const TColumnDescriptor& rhs) {
            return lhs.Index < rhs.Index;
        });
    return orderedSchemaMapping;
}

TKeyColumns TMappedSchema::GetKeyColumns() const
{
    TKeyColumns result(Original->GetKeyColumnCount());
    for (const auto& item : Mapping) {
        if (item.Index < Original->GetKeyColumnCount()) {
            result[item.Index] = item.Name;
        }
    }
    return result;
}

TTableSchemaPtr TMappedSchema::GetRenamedSchema() const
{
    TSchemaColumns result;
    for (const auto& item : GetOrderedSchemaMapping()) {
        result.emplace_back(item.Name, Original->Columns()[item.Index].LogicalType());
    }
    return New<TTableSchema>(std::move(result));
}

////////////////////////////////////////////////////////////////////////////////

TTableSchemaPtr TJoinClause::GetRenamedSchema() const
{
    return Schema.GetRenamedSchema();
}

TKeyColumns TJoinClause::GetKeyColumns() const
{
    return Schema.GetKeyColumns();
}

TTableSchemaPtr TJoinClause::GetTableSchema(const TTableSchema& source) const
{
    TSchemaColumns result;

    for (const auto& column : source.Columns()) {
        if (SelfJoinedColumns.contains(column.Name())) {
            result.push_back(column);
        }
    }

    auto renamedSchema = Schema.GetRenamedSchema();
    for (const auto& column : renamedSchema->Columns()) {
        if (ForeignJoinedColumns.contains(column.Name())) {
            result.push_back(column);
        }
    }

    return New<TTableSchema>(std::move(result));
}

////////////////////////////////////////////////////////////////////////////////

void TGroupClause::AddGroupItem(TNamedItem namedItem)
{
    GroupItems.push_back(std::move(namedItem));
}

void TGroupClause::AddGroupItem(TConstExpressionPtr expression, TString name)
{
    AddGroupItem(TNamedItem(std::move(expression), std::move(name)));
}

TTableSchemaPtr TGroupClause::GetTableSchema(bool isFinal) const
{
    TSchemaColumns result;

    for (const auto& item : GroupItems) {
        result.emplace_back(item.Name, item.Expression->LogicalType);
    }

    for (const auto& item : AggregateItems) {
        result.emplace_back(item.Name, isFinal ? item.ResultType : item.StateType);
    }

    return New<TTableSchema>(std::move(result));
}

////////////////////////////////////////////////////////////////////////////////

void TProjectClause::AddProjection(TNamedItem namedItem)
{
    Projections.push_back(std::move(namedItem));
}

void TProjectClause::AddProjection(TConstExpressionPtr expression, TString name)
{
    AddProjection(TNamedItem(std::move(expression), std::move(name)));
}

TTableSchemaPtr TProjectClause::GetTableSchema() const
{
    TSchemaColumns result;

    for (const auto& item : Projections) {
        result.emplace_back(item.Name, item.Expression->LogicalType);
    }

    return New<TTableSchema>(std::move(result));
}

////////////////////////////////////////////////////////////////////////////////

TBaseQuery::TBaseQuery(TGuid id)
    : Id(id)
{ }

TBaseQuery::TBaseQuery(const TBaseQuery& other)
    : Id(TGuid::Create())
    , IsFinal(other.IsFinal)
    , GroupClause(other.GroupClause)
    , HavingClause(other.HavingClause)
    , OrderClause(other.OrderClause)
    , ProjectClause(other.ProjectClause)
    , Offset(other.Offset)
    , Limit(other.Limit)
    , UseDisjointGroupBy(other.UseDisjointGroupBy)
    , InferRanges(other.InferRanges)
{ }

bool TBaseQuery::IsOrdered() const
{
    if (Limit < std::numeric_limits<i64>::max()) {
        return !OrderClause;
    } else {
        YT_VERIFY(!OrderClause);
        return false;
    }
}

////////////////////////////////////////////////////////////////////////////////

TQuery::TQuery(TGuid id)
    : TBaseQuery(id)
{ }

TKeyColumns TQuery::GetKeyColumns() const
{
    return Schema.GetKeyColumns();
}

TTableSchemaPtr TQuery::GetReadSchema() const
{
    TSchemaColumns result;

    for (const auto& item : Schema.GetOrderedSchemaMapping()) {
        result.emplace_back(
            Schema.Original->Columns()[item.Index].Name(),
            Schema.Original->Columns()[item.Index].LogicalType());
    }

    return New<TTableSchema>(std::move(result));
}

TTableSchemaPtr TQuery::GetRenamedSchema() const
{
    return Schema.GetRenamedSchema();
}

TTableSchemaPtr TQuery::GetTableSchema() const
{
    if (ProjectClause) {
        return ProjectClause->GetTableSchema();
    }

    if (GroupClause) {
        return GroupClause->GetTableSchema(IsFinal);
    }

    auto result = GetRenamedSchema();

    for (const auto& joinClause : JoinClauses) {
        result = joinClause->GetTableSchema(*result);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TFrontQuery::TFrontQuery(TGuid id)
    : TBaseQuery(id)
{ }

TTableSchemaPtr TFrontQuery::GetReadSchema() const
{
    return Schema;
}

TTableSchemaPtr TFrontQuery::GetRenamedSchema() const
{
    return Schema;
}

TTableSchemaPtr TFrontQuery::GetTableSchema() const
{
    if (ProjectClause) {
        return ProjectClause->GetTableSchema();
    }

    if (GroupClause) {
        return GroupClause->GetTableSchema(IsFinal);
    }

    return Schema;
}

////////////////////////////////////////////////////////////////////////////////

struct TExpressionPrinter
    : TAbstractExpressionPrinter<TExpressionPrinter, TConstExpressionPtr>
{
    using TBase = TAbstractExpressionPrinter<TExpressionPrinter, TConstExpressionPtr>;
    TExpressionPrinter(TStringBuilderBase* builder, bool omitValues)
        : TBase(builder, omitValues)
    { }
};

TString InferName(TConstExpressionPtr expr, bool omitValues)
{
    if (!expr) {
        return TString();
    }
    TStringBuilder builder;
    TExpressionPrinter expressionPrinter(&builder, omitValues);
    expressionPrinter.Visit(expr);
    return builder.Flush();
}

TString InferName(TConstBaseQueryPtr query, TInferNameOptions options)
{
    auto namedItemFormatter = [&] (TStringBuilderBase* builder, const TNamedItem& item) {
        builder->AppendString(InferName(item.Expression, options.OmitValues));
        if (!options.OmitAliases) {
            builder->AppendFormat(" AS %v", item.Name);
        }
    };

    auto orderItemFormatter = [&] (TStringBuilderBase* builder, const TOrderItem& item) {
        builder->AppendFormat("%v %v",
            InferName(item.Expression, options.OmitValues),
            item.Descending ? "DESC" : "ASC");
    };

    std::vector<TString> clauses;
    TString str;

    if (query->ProjectClause) {
        str = JoinToString(query->ProjectClause->Projections, namedItemFormatter);
    } else {
        str = "*";
    }

    clauses.emplace_back("SELECT " + str);

    if (auto derivedQuery = dynamic_cast<const TQuery*>(query.Get())) {
        for (const auto& joinClause : derivedQuery->JoinClauses) {
            if (!joinClause->ArrayExpressions.empty()) {
                std::vector<TString> arrayExpressions;
                for (const auto& expression : joinClause->ArrayExpressions) {
                    arrayExpressions.push_back(InferName(expression, options.OmitValues));
                }
                clauses.push_back(Format(
                    "%v ARRAY JOIN %v",
                    joinClause->IsLeft ? "LEFT" : "",
                    JoinToString(arrayExpressions)));
            } else {
                std::vector<TString> selfJoinEquation;
                for (const auto& equation : joinClause->SelfEquations) {
                    selfJoinEquation.push_back(InferName(equation.Expression, options.OmitValues));
                }
                std::vector<TString> foreignJoinEquation;
                for (const auto& equation : joinClause->ForeignEquations) {
                    foreignJoinEquation.push_back(InferName(equation, options.OmitValues));
                }

                clauses.push_back(Format(
                    "%v JOIN[common prefix: %v, foreign prefix: %v] ON (%v) = (%v)",
                    joinClause->IsLeft ? "LEFT" : "INNER",
                    joinClause->CommonKeyPrefix,
                    joinClause->ForeignKeyPrefix,
                    JoinToString(selfJoinEquation),
                    JoinToString(foreignJoinEquation)));

                if (joinClause->Predicate && !options.OmitJoinPredicate) {
                    clauses.push_back("AND " + InferName(joinClause->Predicate, options.OmitValues));
                }
            }
        }

        if (derivedQuery->WhereClause) {
            clauses.push_back(TString("WHERE ") + InferName(derivedQuery->WhereClause, options.OmitValues));
        }
    }

    if (query->GroupClause) {
        clauses.push_back(Format("GROUP BY[common prefix: %v, disjoint: %v, aggregates: %v] %v",
            query->GroupClause->CommonPrefixWithPrimaryKey,
            query->UseDisjointGroupBy,
            MakeFormattableView(query->GroupClause->AggregateItems, [] (auto* builder, const auto& item) {
                builder->AppendString(item.AggregateFunction);
            }),
            JoinToString(query->GroupClause->GroupItems, namedItemFormatter)));
        if (query->GroupClause->TotalsMode == ETotalsMode::BeforeHaving) {
            clauses.push_back("WITH TOTALS");
        }
    }

    if (query->HavingClause) {
        clauses.push_back(TString("HAVING ") + InferName(query->HavingClause, options.OmitValues));
        if (query->GroupClause->TotalsMode == ETotalsMode::AfterHaving) {
            clauses.push_back("WITH TOTALS");
        }
    }

    if (query->OrderClause) {
        clauses.push_back(TString("ORDER BY ") + JoinToString(query->OrderClause->OrderItems, orderItemFormatter));
    }



    if (query->Limit < std::numeric_limits<i64>::max()) {
        clauses.push_back(TString("OFFSET ") + (options.OmitValues ? "?" : ToString(query->Offset)));
        clauses.push_back(TString("LIMIT ") + (options.OmitValues ? "?" : ToString(query->Limit)));
    }

    return JoinToString(clauses, TStringBuf(" "));
}

////////////////////////////////////////////////////////////////////////////////

bool Compare(
    TConstExpressionPtr lhs,
    const TTableSchema& lhsSchema,
    TConstExpressionPtr rhs,
    const TTableSchema& rhsSchema,
    size_t maxIndex)
{
#define CHECK(condition)    \
    do {                    \
        if (!(condition)) { \
            return false;   \
        }                   \
    } while (false)

    CHECK(*lhs->LogicalType == *rhs->LogicalType);

    if (auto literalLhs = lhs->As<TLiteralExpression>()) {
        auto literalRhs = rhs->As<TLiteralExpression>();
        CHECK(literalRhs);
        CHECK(literalLhs->Value == literalRhs->Value);
    } else if (auto referenceLhs = lhs->As<TReferenceExpression>()) {
        auto referenceRhs = rhs->As<TReferenceExpression>();
        CHECK(referenceRhs);
        auto lhsIndex = lhsSchema.GetColumnIndexOrThrow(referenceLhs->ColumnName);
        auto rhsIndex = rhsSchema.GetColumnIndexOrThrow(referenceRhs->ColumnName);
        CHECK(lhsIndex == rhsIndex);
        CHECK(static_cast<size_t>(lhsIndex) < maxIndex);
    } else if (auto functionLhs = lhs->As<TFunctionExpression>()) {
        auto functionRhs = rhs->As<TFunctionExpression>();
        CHECK(functionRhs);
        CHECK(functionLhs->FunctionName == functionRhs->FunctionName);
        CHECK(functionLhs->Arguments.size() == functionRhs->Arguments.size());

        for (size_t index = 0; index < functionLhs->Arguments.size(); ++index) {
            CHECK(Compare(functionLhs->Arguments[index], lhsSchema, functionRhs->Arguments[index], rhsSchema, maxIndex));
        }
    } else if (auto unaryLhs = lhs->As<TUnaryOpExpression>()) {
        auto unaryRhs = rhs->As<TUnaryOpExpression>();
        CHECK(unaryRhs);
        CHECK(unaryLhs->Opcode == unaryRhs->Opcode);
        CHECK(Compare(unaryLhs->Operand, lhsSchema, unaryRhs->Operand, rhsSchema, maxIndex));
    } else if (auto binaryLhs = lhs->As<TBinaryOpExpression>()) {
        auto binaryRhs = rhs->As<TBinaryOpExpression>();
        CHECK(binaryRhs);
        CHECK(binaryLhs->Opcode == binaryRhs->Opcode);
        CHECK(Compare(binaryLhs->Lhs, lhsSchema, binaryRhs->Lhs, rhsSchema, maxIndex));
        CHECK(Compare(binaryLhs->Rhs, lhsSchema, binaryRhs->Rhs, rhsSchema, maxIndex));
    } else if (auto inLhs = lhs->As<TInExpression>()) {
        auto inRhs = rhs->As<TInExpression>();
        CHECK(inRhs);
        CHECK(inLhs->Arguments.size() == inRhs->Arguments.size());
        for (size_t index = 0; index < inLhs->Arguments.size(); ++index) {
            CHECK(Compare(inLhs->Arguments[index], lhsSchema, inRhs->Arguments[index], rhsSchema, maxIndex));
        }

        CHECK(inLhs->Values.Size() == inRhs->Values.Size());
        for (size_t index = 0; index < inLhs->Values.Size(); ++index) {
            CHECK(inLhs->Values[index] == inRhs->Values[index]);
        }
    } else if (auto betweenLhs = lhs->As<TBetweenExpression>()) {
        auto betweenRhs = rhs->As<TBetweenExpression>();
        CHECK(betweenRhs);
        CHECK(betweenLhs->Arguments.size() == betweenRhs->Arguments.size());
        for (size_t index = 0; index < betweenLhs->Arguments.size(); ++index) {
            CHECK(Compare(betweenLhs->Arguments[index], lhsSchema, betweenRhs->Arguments[index], rhsSchema, maxIndex));
        }

        CHECK(betweenLhs->Ranges.Size() == betweenRhs->Ranges.Size());
        for (size_t index = 0; index < betweenLhs->Ranges.Size(); ++index) {
            CHECK(betweenLhs->Ranges[index] == betweenRhs->Ranges[index]);
        }
    } else if (auto transformLhs = lhs->As<TTransformExpression>()) {
        auto transformRhs = rhs->As<TTransformExpression>();
        CHECK(transformRhs);
        CHECK(transformLhs->Arguments.size() == transformRhs->Arguments.size());
        for (size_t index = 0; index < transformLhs->Arguments.size(); ++index) {
            CHECK(Compare(transformLhs->Arguments[index], lhsSchema, transformRhs->Arguments[index], rhsSchema, maxIndex));
        }

        CHECK(transformLhs->Values.Size() == transformRhs->Values.Size());
        for (size_t index = 0; index < transformLhs->Values.Size(); ++index) {
            CHECK(transformLhs->Values[index] == transformRhs->Values[index]);
        }
        CHECK(Compare(transformLhs->DefaultExpression, lhsSchema, transformRhs->DefaultExpression, rhsSchema, maxIndex));
    } else if (auto caseLhs = lhs->As<TCaseExpression>()) {
        auto caseRhs = rhs->As<TCaseExpression>();
        CHECK(caseRhs);

        CHECK(Compare(caseLhs->OptionalOperand, lhsSchema, caseRhs->OptionalOperand, rhsSchema, maxIndex));

        CHECK(caseLhs->WhenThenExpressions.size() == caseRhs->WhenThenExpressions.size());

        for (size_t index = 0; index < caseLhs->WhenThenExpressions.size(); ++index) {
            CHECK(Compare(caseLhs->WhenThenExpressions[index]->Condition, lhsSchema, caseRhs->WhenThenExpressions[index]->Condition, rhsSchema, maxIndex));
            CHECK(Compare(caseLhs->WhenThenExpressions[index]->Result, lhsSchema, caseRhs->WhenThenExpressions[index]->Result, rhsSchema, maxIndex));
        }

        CHECK(Compare(caseLhs->DefaultExpression, lhsSchema, caseRhs->DefaultExpression, rhsSchema, maxIndex));
    } else if (auto likeLhs = lhs->As<TLikeExpression>()) {
        auto likeRhs = rhs->As<TLikeExpression>();
        CHECK(likeRhs);

        CHECK(Compare(likeLhs->Text, lhsSchema, likeRhs->Text, rhsSchema, maxIndex));
        CHECK(likeLhs->Opcode == likeRhs->Opcode);
        CHECK(Compare(likeLhs->Pattern, lhsSchema, likeRhs->Pattern, rhsSchema, maxIndex));
        CHECK(Compare(likeLhs->EscapeCharacter, lhsSchema, likeRhs->EscapeCharacter, rhsSchema, maxIndex));
    } else if (auto memberAccessorLhs = lhs->As<TCompositeMemberAccessorExpression>()) {
        auto memberAccessorRhs = rhs->As<TCompositeMemberAccessorExpression>();
        CHECK(memberAccessorRhs);

        CHECK(Compare(memberAccessorLhs->CompositeExpression, lhsSchema, memberAccessorRhs->CompositeExpression, rhsSchema, maxIndex));
        CHECK(memberAccessorLhs->NestedStructOrTupleItemAccessor == memberAccessorRhs->NestedStructOrTupleItemAccessor);
        CHECK(Compare(memberAccessorLhs->DictOrListItemAccessor, lhsSchema, memberAccessorRhs->DictOrListItemAccessor, rhsSchema, maxIndex));
    } else {
        YT_ABORT();
    }
#undef CHECK

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void ThrowTypeMismatchError(
    EValueType lhsType,
    EValueType rhsType,
    TStringBuf source,
    TStringBuf lhsSource,
    TStringBuf rhsSource)
{
    THROW_ERROR_EXCEPTION("Type mismatch in expression %Qv", source)
        << TErrorAttribute("lhs_source", lhsSource)
        << TErrorAttribute("rhs_source", rhsSource)
        << TErrorAttribute("lhs_type", lhsType)
        << TErrorAttribute("rhs_type", rhsType);
}

struct TExtraColumnsChecker
    : public TVisitor<TExtraColumnsChecker>
{
    using TBase = TVisitor<TExtraColumnsChecker>;

    const THashSet<TString>& Names;
    bool HasExtraColumns = false;

    explicit TExtraColumnsChecker(const THashSet<TString>& names)
        : Names(names)
    { }

    void OnReference(const TReferenceExpression* referenceExpr)
    {
        HasExtraColumns |= Names.count(referenceExpr->ColumnName) == 0;
    }
};

////////////////////////////////////////////////////////////////////////////////

std::vector<size_t> GetJoinGroups(
    const std::vector<TConstJoinClausePtr>& joinClauses,
    TTableSchemaPtr schema)
{
    THashSet<TString> names;
    auto collectColumnNames = [&] (const TTableSchema& schema) {
        names.clear();
        for (const auto& column : schema.Columns()) {
            names.insert(column.Name());
        }
    };

    collectColumnNames(*schema);
    std::vector<size_t> joinGroups;
    size_t counter = 0;

    for (const auto& joinClause : joinClauses) {
        if (!joinClause->ArrayExpressions.empty()) {
            if (counter > 0) {
                joinGroups.push_back(counter);
            }
            joinGroups.push_back(1);
            counter = 0;
            collectColumnNames(*schema);
        } else {
            TExtraColumnsChecker extraColumnsChecker(names);

            for (const auto& equation : joinClause->SelfEquations) {
                if (!equation.Evaluated) {
                    extraColumnsChecker.Visit(equation.Expression);
                }
            }

            if (extraColumnsChecker.HasExtraColumns) {
                YT_VERIFY(counter > 0);
                joinGroups.push_back(counter);
                counter = 0;
                collectColumnNames(*schema);
            }

            ++counter;
        }

        schema = joinClause->GetTableSchema(*schema);
    }

    if (counter > 0) {
        joinGroups.push_back(counter);
        counter = 0;
    }

    return joinGroups;
}

NLogging::TLogger MakeQueryLogger(TGuid queryId)
{
    return QueryClientLogger.WithTag("FragmentId: %v", queryId);
}

NLogging::TLogger MakeQueryLogger(TConstBaseQueryPtr query)
{
    return MakeQueryLogger(query->Id);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TCompositeMemberAccessorPath* proto, const TCompositeMemberAccessorPath& original)
{
    ToProto(proto->mutable_nested_types(), original.NestedTypes);
    ToProto(proto->mutable_named_struct_members(), original.NamedStructMembers);
    ToProto(proto->mutable_positional_struct_members(), original.PositionalStructMembers);
    ToProto(proto->mutable_tuple_item_indices(), original.TupleItemIndices);
}

void FromProto(TCompositeMemberAccessorPath* original, const NProto::TCompositeMemberAccessorPath& serialized)
{
    FromProto(&original->NestedTypes, serialized.nested_types());
    FromProto(&original->NamedStructMembers, serialized.named_struct_members());
    FromProto(&original->PositionalStructMembers, serialized.positional_struct_members());
    FromProto(&original->TupleItemIndices, serialized.tuple_item_indices());
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TExpression* serialized, const TConstExpressionPtr& original)
{
    if (!original) {
        serialized->set_kind(static_cast<int>(EExpressionKind::None));
        return;
    }

    // N.B. backward compatibility old `type` proto field could contain only
    // Int64,Uint64,String,Boolean,Null,Any types.
    const auto wireType = NTableClient::GetPhysicalType(
        NTableClient::CastToV1Type(original->LogicalType).first);

    serialized->set_type(static_cast<int>(wireType));

    if (!IsV1Type(original->LogicalType) ||
        *original->LogicalType != *MakeLogicalType(GetLogicalType(wireType), false))
    {
        ToProto(serialized->mutable_logical_type(), original->LogicalType);
    }

    if (auto literalExpr = original->As<TLiteralExpression>()) {
        serialized->set_kind(static_cast<int>(EExpressionKind::Literal));
        auto* proto = serialized->MutableExtension(NProto::TLiteralExpression::literal_expression);
        auto value = TValue(literalExpr->Value);
        auto data = value.Data;

        switch (value.Type) {
            case EValueType::Int64: {
                proto->set_int64_value(data.Int64);
                break;
            }

            case EValueType::Uint64: {
                proto->set_uint64_value(data.Uint64);
                break;
            }

            case EValueType::Double: {
                proto->set_double_value(data.Double);
                break;
            }

            case EValueType::String: {
                proto->set_string_value(data.String, value.Length);
                break;
            }

            case EValueType::Boolean: {
                proto->set_boolean_value(data.Boolean);
                break;
            }

            case EValueType::Null: {
                break;
            }

            default:
                YT_ABORT();
        }

    } else if (auto referenceExpr = original->As<TReferenceExpression>()) {
        serialized->set_kind(static_cast<int>(EExpressionKind::Reference));
        auto* proto = serialized->MutableExtension(NProto::TReferenceExpression::reference_expression);
        proto->set_column_name(referenceExpr->ColumnName);
    } else if (auto functionExpr = original->As<TFunctionExpression>()) {
        serialized->set_kind(static_cast<int>(EExpressionKind::Function));
        auto* proto = serialized->MutableExtension(NProto::TFunctionExpression::function_expression);
        proto->set_function_name(functionExpr->FunctionName);
        ToProto(proto->mutable_arguments(), functionExpr->Arguments);
    } else if (auto unaryOpExpr = original->As<TUnaryOpExpression>()) {
        serialized->set_kind(static_cast<int>(EExpressionKind::UnaryOp));
        auto* proto = serialized->MutableExtension(NProto::TUnaryOpExpression::unary_op_expression);
        proto->set_opcode(static_cast<int>(unaryOpExpr->Opcode));
        ToProto(proto->mutable_operand(), unaryOpExpr->Operand);
    } else if (auto binaryOpExpr = original->As<TBinaryOpExpression>()) {
        serialized->set_kind(static_cast<int>(EExpressionKind::BinaryOp));
        auto* proto = serialized->MutableExtension(NProto::TBinaryOpExpression::binary_op_expression);
        proto->set_opcode(static_cast<int>(binaryOpExpr->Opcode));
        ToProto(proto->mutable_lhs(), binaryOpExpr->Lhs);
        ToProto(proto->mutable_rhs(), binaryOpExpr->Rhs);
    } else if (auto inExpr = original->As<TInExpression>()) {
        serialized->set_kind(static_cast<int>(EExpressionKind::In));
        auto* proto = serialized->MutableExtension(NProto::TInExpression::in_expression);
        ToProto(proto->mutable_arguments(), inExpr->Arguments);

        auto writer = CreateWireProtocolWriter();
        writer->WriteUnversionedRowset(inExpr->Values);
        ToProto(proto->mutable_values(), MergeRefsToString(writer->Finish()));
    } else if (auto betweenExpr = original->As<TBetweenExpression>()) {
        serialized->set_kind(static_cast<int>(EExpressionKind::Between));
        auto* proto = serialized->MutableExtension(NProto::TBetweenExpression::between_expression);
        ToProto(proto->mutable_arguments(), betweenExpr->Arguments);

        auto rangesWriter = CreateWireProtocolWriter();
        for (const auto& range : betweenExpr->Ranges) {
            rangesWriter->WriteUnversionedRow(range.first);
            rangesWriter->WriteUnversionedRow(range.second);
        }
        ToProto(proto->mutable_ranges(), MergeRefsToString(rangesWriter->Finish()));
    } else if (auto transformExpr = original->As<TTransformExpression>()) {
        serialized->set_kind(static_cast<int>(EExpressionKind::Transform));
        auto* proto = serialized->MutableExtension(NProto::TTransformExpression::transform_expression);
        ToProto(proto->mutable_arguments(), transformExpr->Arguments);

        auto writer = CreateWireProtocolWriter();
        writer->WriteUnversionedRowset(transformExpr->Values);
        ToProto(proto->mutable_values(), MergeRefsToString(writer->Finish()));
        if (transformExpr->DefaultExpression) {
            ToProto(proto->mutable_default_expression(), transformExpr->DefaultExpression);
        }
    } else if (auto caseExpr = original->As<TCaseExpression>()) {
        serialized->set_kind(static_cast<int>(EExpressionKind::Case));
        auto* proto = serialized->MutableExtension(NProto::TCaseExpression::case_expression);

        if (caseExpr->OptionalOperand) {
            ToProto(proto->mutable_optional_operand(), caseExpr->OptionalOperand);
        }

        ToProto(proto->mutable_when_then_expressions(), caseExpr->WhenThenExpressions);

        if (caseExpr->DefaultExpression) {
            ToProto(proto->mutable_default_expression(), caseExpr->DefaultExpression);
        }
    } else if (auto likeExpr = original->As<TLikeExpression>()) {
        serialized->set_kind(static_cast<int>(EExpressionKind::Like));
        auto* proto = serialized->MutableExtension(NProto::TLikeExpression::like_expression);

        ToProto(proto->mutable_text(), likeExpr->Text);
        proto->set_opcode(static_cast<int>(likeExpr->Opcode));
        ToProto(proto->mutable_pattern(), likeExpr->Pattern);
        if (likeExpr->EscapeCharacter) {
            ToProto(proto->mutable_escape_character(), likeExpr->EscapeCharacter);
        }
    } else if (auto memberAccessorExpr = original->As<TCompositeMemberAccessorExpression>()) {
        serialized->set_kind(static_cast<int>(EExpressionKind::CompositeMemberAccessor));
        auto* proto = serialized->MutableExtension(NProto::TCompositeMemberAccessor::composite_member_accessor_expression);

        ToProto(proto->mutable_composite(), memberAccessorExpr->CompositeExpression);
        ToProto(proto->mutable_nested_struct_or_tuple_item_accessor(), memberAccessorExpr->NestedStructOrTupleItemAccessor);
        if (memberAccessorExpr->DictOrListItemAccessor) {
            ToProto(proto->mutable_dict_or_list_item_accessor(), memberAccessorExpr->DictOrListItemAccessor);
        }
    }
}

void FromProto(TConstExpressionPtr* original, const NProto::TExpression& serialized)
{
    TLogicalTypePtr type;
    if (serialized.has_logical_type()) {
        FromProto(&type, serialized.logical_type());
    } else {
        auto wireType = CheckedEnumCast<EValueType>(serialized.type());
        type = MakeLogicalType(GetLogicalType(wireType), false);
    }

    auto kind = CheckedEnumCast<EExpressionKind>(serialized.kind());
    switch (kind) {
        case EExpressionKind::None: {
            *original = nullptr;
            return;
        }

        case EExpressionKind::Literal: {
            auto result = New<TLiteralExpression>(GetWireType(type));
            const auto& ext = serialized.GetExtension(NProto::TLiteralExpression::literal_expression);

            if (ext.has_int64_value()) {
                result->Value = MakeUnversionedInt64Value(ext.int64_value());
            } else if (ext.has_uint64_value()) {
                result->Value = MakeUnversionedUint64Value(ext.uint64_value());
            } else if (ext.has_double_value()) {
                result->Value = MakeUnversionedDoubleValue(ext.double_value());
            } else if (ext.has_string_value()) {
                result->Value = MakeUnversionedStringValue(ext.string_value());
            } else if (ext.has_boolean_value()) {
                result->Value = MakeUnversionedBooleanValue(ext.boolean_value());
            } else {
                result->Value = MakeUnversionedSentinelValue(EValueType::Null);
            }

            *original = result;
            return;
        }

        case EExpressionKind::Reference: {
            auto result = New<TReferenceExpression>(type);
            const auto& data = serialized.GetExtension(NProto::TReferenceExpression::reference_expression);
            result->ColumnName = data.column_name();
            *original = result;
            return;
        }

        case EExpressionKind::Function: {
            auto result = New<TFunctionExpression>(GetWireType(type));
            const auto& ext = serialized.GetExtension(NProto::TFunctionExpression::function_expression);
            result->FunctionName = ext.function_name();
            FromProto(&result->Arguments, ext.arguments());
            *original = result;
            return;
        }

        case EExpressionKind::UnaryOp: {
            auto result = New<TUnaryOpExpression>(GetWireType(type));
            const auto& ext = serialized.GetExtension(NProto::TUnaryOpExpression::unary_op_expression);
            result->Opcode = EUnaryOp(ext.opcode());
            FromProto(&result->Operand, ext.operand());
            *original = result;
            return;
        }

        case EExpressionKind::BinaryOp: {
            auto result = New<TBinaryOpExpression>(GetWireType(type));
            const auto& ext = serialized.GetExtension(NProto::TBinaryOpExpression::binary_op_expression);
            result->Opcode = EBinaryOp(ext.opcode());
            FromProto(&result->Lhs, ext.lhs());
            FromProto(&result->Rhs, ext.rhs());
            *original = result;
            return;
        }

        case EExpressionKind::In: {
            auto result = New<TInExpression>(GetWireType(type));
            const auto& ext = serialized.GetExtension(NProto::TInExpression::in_expression);
            FromProto(&result->Arguments, ext.arguments());
            auto reader = CreateWireProtocolReader(
                TSharedRef::FromString(ext.values()),
                New<TRowBuffer>(TExpressionRowsetTag()));
            result->Values = reader->ReadUnversionedRowset(true);
            *original = result;
            return;
        }

        case EExpressionKind::Between: {
            auto result = New<TBetweenExpression>(GetWireType(type));
            const auto& ext = serialized.GetExtension(NProto::TBetweenExpression::between_expression);
            FromProto(&result->Arguments, ext.arguments());

            TRowRanges ranges;
            auto rowBuffer = New<TRowBuffer>(TExpressionRowsetTag());
            auto rangesReader = CreateWireProtocolReader(
                TSharedRef::FromString<TExpressionRowsetTag>(ext.ranges()),
                rowBuffer);
            while (!rangesReader->IsFinished()) {
                auto lowerBound = rangesReader->ReadUnversionedRow(true);
                auto upperBound = rangesReader->ReadUnversionedRow(true);
                ranges.emplace_back(lowerBound, upperBound);
            }
            result->Ranges = MakeSharedRange(std::move(ranges), std::move(rowBuffer));
            *original = result;
            return;
        }

        case EExpressionKind::Transform: {
            auto result = New<TTransformExpression>(GetWireType(type));
            const auto& ext = serialized.GetExtension(NProto::TTransformExpression::transform_expression);
            FromProto(&result->Arguments, ext.arguments());
            auto reader = CreateWireProtocolReader(
                TSharedRef::FromString(ext.values()),
                New<TRowBuffer>(TExpressionRowsetTag()));
            result->Values = reader->ReadUnversionedRowset(true);
            if (ext.has_default_expression()) {
                FromProto(&result->DefaultExpression, ext.default_expression());
            }
            *original = result;
            return;
        }

        case EExpressionKind::Case: {
            auto result = New<TCaseExpression>(GetWireType(type));
            const auto& ext = serialized.GetExtension(NProto::TCaseExpression::case_expression);

            if (ext.has_optional_operand()) {
                FromProto(&result->OptionalOperand, ext.optional_operand());
            }

            FromProto(&result->WhenThenExpressions, ext.when_then_expressions());

            if (ext.has_default_expression()) {
                FromProto(&result->DefaultExpression, ext.default_expression());
            }

            *original = result;
            return;
        }

        case EExpressionKind::Like: {
            auto result = New<TLikeExpression>(GetWireType(type));
            const auto& ext = serialized.GetExtension(NProto::TLikeExpression::like_expression);

            FromProto(&result->Text, ext.text());
            result->Opcode = static_cast<EStringMatchOp>(ext.opcode());
            FromProto(&result->Pattern, ext.pattern());
            if (ext.has_escape_character()) {
                FromProto(&result->EscapeCharacter, ext.escape_character());
            }

            *original = result;
            return;
        }

        case EExpressionKind::CompositeMemberAccessor: {
            auto result = New<TCompositeMemberAccessorExpression>(type);
            const auto& ext = serialized.GetExtension(NProto::TCompositeMemberAccessor::composite_member_accessor_expression);

            FromProto(&result->CompositeExpression, ext.composite());

            FromProto(&result->NestedStructOrTupleItemAccessor, ext.nested_struct_or_tuple_item_accessor());

            if (ext.has_dict_or_list_item_accessor()) {
                FromProto(&result->DictOrListItemAccessor, ext.dict_or_list_item_accessor());
            }

            *original = result;
            return;
        }
    }
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TNamedItem* serialized, const TNamedItem& original)
{
    ToProto(serialized->mutable_expression(), original.Expression);
    ToProto(serialized->mutable_name(), original.Name);
}

void FromProto(TNamedItem* original, const NProto::TNamedItem& serialized)
{
    *original = TNamedItem(
        FromProto<TConstExpressionPtr>(serialized.expression()),
        serialized.name());
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TAggregateItem* serialized, const TAggregateItem& original)
{
    ToProto(serialized->mutable_expression(), original.Arguments.front());
    serialized->set_aggregate_function_name(original.AggregateFunction);
    serialized->set_state_type(static_cast<int>(original.StateType));
    serialized->set_result_type(static_cast<int>(original.ResultType));
    ToProto(serialized->mutable_name(), original.Name);
    ToProto(serialized->mutable_arguments(), original.Arguments);
}

void FromProto(TAggregateItem* original, const NProto::TAggregateItem& serialized)
{
    original->AggregateFunction = serialized.aggregate_function_name();
    original->Name = serialized.name();
    original->StateType = static_cast<EValueType>(serialized.state_type());
    original->ResultType = static_cast<EValueType>(serialized.result_type());
    // COMPAT(sabdenovch)
    if (serialized.arguments_size() > 0) {
        original->Arguments = FromProto<std::vector<TConstExpressionPtr>>(serialized.arguments());
    } else {
        original->Arguments = {FromProto<TConstExpressionPtr>(serialized.expression())};
    }
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TSelfEquation* proto, const TSelfEquation& original)
{
    ToProto(proto->mutable_expression(), original.Expression);
    proto->set_evaluated(original.Evaluated);
}

void FromProto(TSelfEquation* original, const NProto::TSelfEquation& serialized)
{
    FromProto(&original->Expression, serialized.expression());
    FromProto(&original->Evaluated, serialized.evaluated());
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TColumnDescriptor* proto, const TColumnDescriptor& original)
{
    proto->set_name(original.Name);
    proto->set_index(original.Index);
}

void FromProto(TColumnDescriptor* original, const NProto::TColumnDescriptor& serialized)
{
    FromProto(&original->Name, serialized.name());
    FromProto(&original->Index, serialized.index());
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TJoinClause* proto, const TConstJoinClausePtr& original)
{
    ToProto(proto->mutable_original_schema(), original->Schema.Original);
    ToProto(proto->mutable_schema_mapping(), original->Schema.Mapping);
    ToProto(proto->mutable_self_joined_columns(), original->SelfJoinedColumns);
    ToProto(proto->mutable_foreign_joined_columns(), original->ForeignJoinedColumns);

    ToProto(proto->mutable_foreign_equations(), original->ForeignEquations);
    ToProto(proto->mutable_self_equations(), original->SelfEquations);

    ToProto(proto->mutable_foreign_object_id(), original->ForeignObjectId);
    ToProto(proto->mutable_foreign_cell_id(), original->ForeignCellId);

    proto->set_is_left(original->IsLeft);

    // COMPAT(lukyan)
    bool canUseSourceRanges = original->ForeignKeyPrefix == original->ForeignEquations.size();
    proto->set_can_use_source_ranges(canUseSourceRanges);
    proto->set_common_key_prefix(canUseSourceRanges ? original->CommonKeyPrefix : 0);
    proto->set_common_key_prefix_new(original->CommonKeyPrefix);
    proto->set_foreign_key_prefix(original->ForeignKeyPrefix);

    if (original->Predicate) {
        ToProto(proto->mutable_predicate(), original->Predicate);
    }

    ToProto(proto->mutable_array_expressions(), original->ArrayExpressions);
}

void FromProto(TConstJoinClausePtr* original, const NProto::TJoinClause& serialized)
{
    auto result = New<TJoinClause>();
    FromProto(&result->Schema.Original, serialized.original_schema());
    FromProto(&result->Schema.Mapping, serialized.schema_mapping());
    FromProto(&result->SelfJoinedColumns, serialized.self_joined_columns());
    FromProto(&result->ForeignJoinedColumns, serialized.foreign_joined_columns());
    FromProto(&result->ForeignEquations, serialized.foreign_equations());
    FromProto(&result->SelfEquations, serialized.self_equations());
    FromProto(&result->ForeignObjectId, serialized.foreign_object_id());
    FromProto(&result->ForeignCellId, serialized.foreign_cell_id());
    FromProto(&result->IsLeft, serialized.is_left());
    FromProto(&result->CommonKeyPrefix, serialized.common_key_prefix());

    if (serialized.has_common_key_prefix_new()) {
        FromProto(&result->CommonKeyPrefix, serialized.common_key_prefix_new());
    }

    // COMPAT(lukyan)
    if (serialized.can_use_source_ranges()) {
        result->ForeignKeyPrefix = result->ForeignEquations.size();
    } else {
        FromProto(&result->ForeignKeyPrefix, serialized.foreign_key_prefix());
    }

    if (serialized.has_predicate()) {
        FromProto(&result->Predicate, serialized.predicate());
    }

    FromProto(&result->ArrayExpressions, serialized.array_expressions());

    *original = result;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TGroupClause* proto, const TConstGroupClausePtr& original)
{
    ToProto(proto->mutable_group_items(), original->GroupItems);
    ToProto(proto->mutable_aggregate_items(), original->AggregateItems);
    proto->set_totals_mode(static_cast<int>(original->TotalsMode));
    proto->set_common_prefix_with_primary_key(static_cast<int>(original->CommonPrefixWithPrimaryKey));
}

void FromProto(TConstGroupClausePtr* original, const NProto::TGroupClause& serialized)
{
    auto result = New<TGroupClause>();
    FromProto(&result->GroupItems, serialized.group_items());
    FromProto(&result->AggregateItems, serialized.aggregate_items());
    result->TotalsMode = ETotalsMode(serialized.totals_mode());
    result->CommonPrefixWithPrimaryKey = serialized.common_prefix_with_primary_key();

    *original = result;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TProjectClause* proto, const TConstProjectClausePtr& original)
{
    ToProto(proto->mutable_projections(), original->Projections);
}

void FromProto(TConstProjectClausePtr* original, const NProto::TProjectClause& serialized)
{
    auto result = New<TProjectClause>();
    result->Projections.reserve(serialized.projections_size());
    for (int i = 0; i < serialized.projections_size(); ++i) {
        result->AddProjection(FromProto<TNamedItem>(serialized.projections(i)));
    }
    *original = result;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TWhenThenExpression* proto, const TWhenThenExpressionPtr& original)
{
    ToProto(proto->mutable_condition(), original->Condition);
    ToProto(proto->mutable_result(), original->Result);
}

void FromProto(TWhenThenExpressionPtr* original, const NProto::TWhenThenExpression& serialized)
{
    auto result = New<TWhenThenExpression>();
    FromProto(&result->Condition, serialized.condition());
    FromProto(&result->Result, serialized.result());
    *original = result;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TOrderItem* serialized, const TOrderItem& original)
{
    ToProto(serialized->mutable_expression(), original.Expression);
    serialized->set_descending(original.Descending);
}

void FromProto(TOrderItem* original, const NProto::TOrderItem& serialized)
{
    FromProto(&original->Expression, serialized.expression());
    FromProto(&original->Descending, serialized.descending());
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TOrderClause* proto, const TConstOrderClausePtr& original)
{
    ToProto(proto->mutable_order_items(), original->OrderItems);
}

void FromProto(TConstOrderClausePtr* original, const NProto::TOrderClause& serialized)
{
    auto result = New<TOrderClause>();
    FromProto(&result->OrderItems, serialized.order_items());
    *original = result;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TQuery* serialized, const TConstQueryPtr& original)
{
    ToProto(serialized->mutable_id(), original->Id);

    serialized->set_offset(original->Offset);
    serialized->set_limit(original->Limit);
    serialized->set_use_disjoint_group_by(original->UseDisjointGroupBy);
    serialized->set_infer_ranges(original->InferRanges);
    serialized->set_is_final(original->IsFinal);

    ToProto(serialized->mutable_original_schema(), original->Schema.Original);
    ToProto(serialized->mutable_schema_mapping(), original->Schema.Mapping);

    ToProto(serialized->mutable_join_clauses(), original->JoinClauses);

    if (original->WhereClause) {
        ToProto(serialized->mutable_where_clause(), original->WhereClause);
    }

    if (original->GroupClause) {
        ToProto(serialized->mutable_group_clause(), original->GroupClause);
    }

    if (original->HavingClause) {
        ToProto(serialized->mutable_having_clause(), original->HavingClause);
    }

    if (original->OrderClause) {
        ToProto(serialized->mutable_order_clause(), original->OrderClause);
    }

    if (original->ProjectClause) {
        ToProto(serialized->mutable_project_clause(), original->ProjectClause);
    }
}

void FromProto(TConstQueryPtr* original, const NProto::TQuery& serialized)
{
    auto result = New<TQuery>(FromProto<TGuid>(serialized.id()));

    result->Offset = serialized.offset();
    result->Limit = serialized.limit();
    result->UseDisjointGroupBy = serialized.use_disjoint_group_by();
    result->InferRanges = serialized.infer_ranges();

    FromProto(&result->IsFinal, serialized.is_final());

    FromProto(&result->Schema.Original, serialized.original_schema());
    FromProto(&result->Schema.Mapping, serialized.schema_mapping());

    FromProto(&result->JoinClauses, serialized.join_clauses());

    if (serialized.has_where_clause()) {
        FromProto(&result->WhereClause, serialized.where_clause());
    }

    if (serialized.has_group_clause()) {
        FromProto(&result->GroupClause, serialized.group_clause());
    }

    if (serialized.has_having_clause()) {
        FromProto(&result->HavingClause, serialized.having_clause());
    }

    if (serialized.has_order_clause()) {
        FromProto(&result->OrderClause, serialized.order_clause());
    }

    if (serialized.has_project_clause()) {
        FromProto(&result->ProjectClause, serialized.project_clause());
    }

    *original = result;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TQueryOptions* serialized, const TQueryOptions& original)
{
    serialized->set_timestamp(original.TimestampRange.Timestamp);
    serialized->set_retention_timestamp(original.TimestampRange.RetentionTimestamp);
    serialized->set_verbose_logging(original.VerboseLogging);
    serialized->set_new_range_inference(original.NewRangeInference);
    serialized->set_use_canonical_null_relations(original.UseCanonicalNullRelations);
    serialized->set_execution_backend(static_cast<int>(original.ExecutionBackend));
    serialized->set_max_subqueries(original.MaxSubqueries);
    serialized->set_enable_code_cache(original.EnableCodeCache);
    ToProto(serialized->mutable_workload_descriptor(), original.WorkloadDescriptor);
    serialized->set_allow_full_scan(original.AllowFullScan);
    ToProto(serialized->mutable_read_session_id(), original.ReadSessionId);
    serialized->set_deadline(ToProto<ui64>(original.Deadline));
    serialized->set_memory_limit_per_node(original.MemoryLimitPerNode);
    if (original.ExecutionPool) {
        serialized->set_execution_pool(*original.ExecutionPool);
    }
    serialized->set_suppress_access_tracking(original.SuppressAccessTracking);
    serialized->set_range_expansion_limit(original.RangeExpansionLimit);
    serialized->set_merge_versioned_rows(original.MergeVersionedRows);
}

void FromProto(TQueryOptions* original, const NProto::TQueryOptions& serialized)
{
    original->TimestampRange.Timestamp = serialized.timestamp();
    original->TimestampRange.RetentionTimestamp = serialized.retention_timestamp();
    original->VerboseLogging = serialized.verbose_logging();
    original->MaxSubqueries = serialized.max_subqueries();
    original->EnableCodeCache = serialized.enable_code_cache();
    original->WorkloadDescriptor = serialized.has_workload_descriptor()
        ? FromProto<TWorkloadDescriptor>(serialized.workload_descriptor())
        : TWorkloadDescriptor();
    original->AllowFullScan = serialized.allow_full_scan();
    original->ReadSessionId  = serialized.has_read_session_id()
        ? FromProto<TReadSessionId>(serialized.read_session_id())
        : TReadSessionId::Create();

    if (serialized.has_memory_limit_per_node()) {
        original->MemoryLimitPerNode = serialized.memory_limit_per_node();
    }

    if (serialized.has_execution_pool()) {
        original->ExecutionPool = serialized.execution_pool();
    }

    original->Deadline = serialized.has_deadline()
        ? FromProto<TInstant>(serialized.deadline())
        : TInstant::Max();

    if (serialized.has_suppress_access_tracking()) {
        original->SuppressAccessTracking = serialized.suppress_access_tracking();
    }

    if (serialized.has_range_expansion_limit()) {
        original->RangeExpansionLimit = serialized.range_expansion_limit();
    }
    if (serialized.has_new_range_inference()) {
        original->NewRangeInference = serialized.new_range_inference();
    }
    if (serialized.has_use_canonical_null_relations()) {
        original->UseCanonicalNullRelations = serialized.use_canonical_null_relations();
    }
    if (serialized.has_merge_versioned_rows()) {
        original->MergeVersionedRows = serialized.merge_versioned_rows();
    }
    if (serialized.has_execution_backend()) {
        original->ExecutionBackend = static_cast<EExecutionBackend>(serialized.execution_backend());
    }
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TDataSource* serialized, const TDataSource& original)
{
    ToProto(serialized->mutable_object_id(), original.ObjectId);
    ToProto(serialized->mutable_cell_id(), original.CellId);
    serialized->set_mount_revision(original.MountRevision);

    auto rangesWriter = CreateWireProtocolWriter();
    for (const auto& range : original.Ranges) {
        rangesWriter->WriteUnversionedRow(range.first);
        rangesWriter->WriteUnversionedRow(range.second);
    }
    ToProto(serialized->mutable_ranges(), MergeRefsToString(rangesWriter->Finish()));

    if (original.Keys) {
        std::vector<TColumnSchema> columns;
        for (auto type : original.Schema) {
            columns.emplace_back("", type);
        }

        TTableSchema schema(columns);
        auto keysWriter = CreateWireProtocolWriter();
        keysWriter->WriteTableSchema(schema);
        keysWriter->WriteSchemafulRowset(original.Keys);
        ToProto(serialized->mutable_keys(), MergeRefsToString(keysWriter->Finish()));
    }
    serialized->set_lookup_supported(original.LookupSupported);
    serialized->set_key_width(original.KeyWidth);
}

void FromProto(TDataSource* original, const NProto::TDataSource& serialized)
{
    FromProto(&original->ObjectId, serialized.object_id());
    FromProto(&original->CellId, serialized.cell_id());
    original->MountRevision = serialized.mount_revision();

    struct TDataSourceBufferTag
    { };

    TRowRanges ranges;
    auto rowBuffer = New<TRowBuffer>(TDataSourceBufferTag());
    auto rangesReader = CreateWireProtocolReader(
        TSharedRef::FromString<TDataSourceBufferTag>(serialized.ranges()),
        rowBuffer);
    while (!rangesReader->IsFinished()) {
        auto lowerBound = rangesReader->ReadUnversionedRow(true);
        auto upperBound = rangesReader->ReadUnversionedRow(true);
        ranges.emplace_back(lowerBound, upperBound);
    }
    original->Ranges = MakeSharedRange(std::move(ranges), rowBuffer);

    if (serialized.has_keys()) {
        auto keysReader = CreateWireProtocolReader(
            TSharedRef::FromString<TDataSourceBufferTag>(serialized.keys()),
            rowBuffer);

        auto schema = keysReader->ReadTableSchema();
        auto schemaData = keysReader->GetSchemaData(schema, NTableClient::TColumnFilter());
        original->Keys = keysReader->ReadSchemafulRowset(schemaData, true);
    }
    original->LookupSupported = serialized.lookup_supported();
    original->KeyWidth = serialized.key_width();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
