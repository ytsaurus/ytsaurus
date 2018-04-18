#include "query.h"

#include <yt/ytlib/chunk_client/chunk_spec.pb.h>

#include <yt/ytlib/query_client/query.pb.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/ytlib/table_client/schema.h>

#include <yt/ytlib/tablet_client/wire_protocol.h>

#include <yt/core/ytree/serialize.h>
#include <yt/core/ytree/convert.h>

#include <limits>

namespace NYT {
namespace NQueryClient {

using namespace NTableClient;
using namespace NObjectClient;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

//! Computes key index for a given column name.
int ColumnNameToKeyPartIndex(const TKeyColumns& keyColumns, const TString& columnName)
{
    for (int index = 0; index < keyColumns.size(); ++index) {
        if (keyColumns[index] == columnName) {
            return index;
        }
    }
    return -1;
}

////////////////////////////////////////////////////////////////////////////////

struct TExpressionPrinter
    : TAbstractExpressionPrinter<TExpressionPrinter, TConstExpressionPtr>
{
    using TBase = TAbstractExpressionPrinter<TExpressionPrinter, TConstExpressionPtr>;
    TExpressionPrinter(TStringBuilder* builder, bool omitValues)
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

TString InferName(TConstBaseQueryPtr query, bool omitValues)
{
    auto namedItemFormatter = [&] (TStringBuilder* builder, const TNamedItem& item) {
        builder->AppendFormat("%v AS %v",
            InferName(item.Expression, omitValues),
            item.Name);
    };

    auto orderItemFormatter = [&] (TStringBuilder* builder, const TOrderItem& item) {
        builder->AppendFormat("%v %v",
            InferName(item.first, omitValues),
            item.second ? "DESC" : "ASC");
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
            std::vector<TString> selfJoinEquation;
            for (const auto& equation : joinClause->SelfEquations) {
                selfJoinEquation.push_back(InferName(equation.first, omitValues));
            }
            std::vector<TString> foreignJoinEquation;
            for (const auto& equation : joinClause->ForeignEquations) {
                foreignJoinEquation.push_back(InferName(equation, omitValues));
            }

            clauses.push_back(Format(
                "%v JOIN[common prefix: %v, foreign prefix: %v] (%v) = (%v)",
                joinClause->IsLeft ? "LEFT" : "INNER",
                joinClause->CommonKeyPrefix,
                joinClause->ForeignKeyPrefix,
                JoinToString(selfJoinEquation),
                JoinToString(foreignJoinEquation)));

            if (joinClause->Predicate) {
                clauses.push_back("AND" + InferName(joinClause->Predicate, omitValues));
            }
        }

        if (derivedQuery->WhereClause) {
            clauses.push_back(TString("WHERE ") + InferName(derivedQuery->WhereClause, omitValues));
        }
    }

    if (query->GroupClause) {
        clauses.push_back(TString("GROUP BY ") + JoinToString(query->GroupClause->GroupItems, namedItemFormatter));
        if (query->GroupClause->TotalsMode == ETotalsMode::BeforeHaving) {
            clauses.push_back("WITH TOTALS");
        }
    }

    if (query->HavingClause) {
        clauses.push_back(TString("HAVING ") + InferName(query->HavingClause, omitValues));
        if (query->GroupClause->TotalsMode == ETotalsMode::AfterHaving) {
            clauses.push_back("WITH TOTALS");
        }
    }

    if (query->OrderClause) {
        clauses.push_back(TString("ORDER BY ") + JoinToString(query->OrderClause->OrderItems, orderItemFormatter));
    }

    if (query->Limit < std::numeric_limits<i64>::max()) {
        clauses.push_back(TString("LIMIT ") + ToString(query->Limit));
    }

    return JoinToString(clauses, AsStringBuf(" "));
}

////////////////////////////////////////////////////////////////////////////////

bool Compare(
    TConstExpressionPtr lhs,
    const TTableSchema& lhsSchema,
    TConstExpressionPtr rhs,
    const TTableSchema& rhsSchema,
    size_t maxIndex)
{
#define CHECK(condition) \
    if (!(condition)) { \
        return false; \
    }

    CHECK(lhs->Type == rhs->Type)
    if (auto literalLhs = lhs->As<TLiteralExpression>()) {
        auto literalRhs = rhs->As<TLiteralExpression>();
        CHECK(literalRhs)
        CHECK(literalLhs->Value == literalRhs->Value)
    } else if (auto referenceLhs = lhs->As<TReferenceExpression>()) {
        auto referenceRhs = rhs->As<TReferenceExpression>();
        CHECK(referenceRhs)
        auto lhsIndex = lhsSchema.GetColumnIndexOrThrow(referenceLhs->ColumnName);
        auto rhsIndex = rhsSchema.GetColumnIndexOrThrow(referenceRhs->ColumnName);
        CHECK(lhsIndex == rhsIndex)
        CHECK(lhsIndex < maxIndex)
    } else if (auto functionLhs = lhs->As<TFunctionExpression>()) {
        auto functionRhs = rhs->As<TFunctionExpression>();
        CHECK(functionRhs)
        CHECK(functionLhs->FunctionName == functionRhs->FunctionName)
        CHECK(functionLhs->Arguments.size() == functionRhs->Arguments.size())

        for (size_t index = 0; index < functionLhs->Arguments.size(); ++index) {
            CHECK(Compare(functionLhs->Arguments[index], lhsSchema, functionRhs->Arguments[index], rhsSchema, maxIndex))
        }
    } else if (auto unaryLhs = lhs->As<TUnaryOpExpression>()) {
        auto unaryRhs = rhs->As<TUnaryOpExpression>();
        CHECK(unaryRhs)
        CHECK(unaryLhs->Opcode == unaryRhs->Opcode)
        CHECK(Compare(unaryLhs->Operand, lhsSchema, unaryRhs->Operand, rhsSchema, maxIndex))
    } else if (auto binaryLhs = lhs->As<TBinaryOpExpression>()) {
        auto binaryRhs = rhs->As<TBinaryOpExpression>();
        CHECK(binaryRhs)
        CHECK(binaryLhs->Opcode == binaryRhs->Opcode)
        CHECK(Compare(binaryLhs->Lhs, lhsSchema, binaryRhs->Lhs, rhsSchema, maxIndex))
        CHECK(Compare(binaryLhs->Rhs, lhsSchema, binaryRhs->Rhs, rhsSchema, maxIndex))
    } else if (auto inLhs = lhs->As<TInExpression>()) {
        auto inRhs = rhs->As<TInExpression>();
        CHECK(inRhs)
        CHECK(inLhs->Arguments.size() == inRhs->Arguments.size())
        for (size_t index = 0; index < inLhs->Arguments.size(); ++index) {
            CHECK(Compare(inLhs->Arguments[index], lhsSchema, inRhs->Arguments[index], rhsSchema, maxIndex))
        }

        CHECK(inLhs->Values.Size() == inRhs->Values.Size())
        for (size_t index = 0; index < inLhs->Values.Size(); ++index) {
            CHECK(inLhs->Values[index] == inRhs->Values[index])
        }
    } else if (auto transformLhs = lhs->As<TTransformExpression>()) {
        auto transformRhs = rhs->As<TTransformExpression>();
        CHECK(transformRhs)
        CHECK(transformLhs->Arguments.size() == transformRhs->Arguments.size())
        for (size_t index = 0; index < transformLhs->Arguments.size(); ++index) {
            CHECK(Compare(transformLhs->Arguments[index], lhsSchema, transformRhs->Arguments[index], rhsSchema, maxIndex))
        }

        CHECK(transformLhs->Values.Size() == transformRhs->Values.Size())
        for (size_t index = 0; index < transformLhs->Values.Size(); ++index) {
            CHECK(transformLhs->Values[index] == transformRhs->Values[index])
        }
        CHECK(Compare(transformRhs->DefaultExpression, lhsSchema, transformRhs->DefaultExpression, rhsSchema, maxIndex))
    } else {
        Y_UNREACHABLE();
    }
#undef CHECK

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void ThrowTypeMismatchError(
    EValueType lhsType,
    EValueType rhsType,
    const TStringBuf& source,
    const TStringBuf& lhsSource,
    const TStringBuf& rhsSource)
{
    THROW_ERROR_EXCEPTION(
            "Type mismatch in expression %Qv",
            source)
            << TErrorAttribute("lhs_source", lhsSource)
            << TErrorAttribute("rhs_source", rhsSource)
            << TErrorAttribute("lhs_type", lhsType)
            << TErrorAttribute("rhs_type", rhsType);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TExpression* serialized, const TConstExpressionPtr& original)
{
    if (!original) {
        serialized->set_kind(static_cast<int>(EExpressionKind::None));
        return;
    }

    serialized->set_type(static_cast<int>(original->Type));

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
                Y_UNREACHABLE();
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

        NTabletClient::TWireProtocolWriter writer;
        writer.WriteUnversionedRowset(inExpr->Values);
        ToProto(proto->mutable_values(), MergeRefsToString(writer.Finish()));
    } else if (auto transformExpr = original->As<TTransformExpression>()) {
        serialized->set_kind(static_cast<int>(EExpressionKind::Transform));
        auto* proto = serialized->MutableExtension(NProto::TTransformExpression::transform_expression);
        ToProto(proto->mutable_arguments(), transformExpr->Arguments);

        NTabletClient::TWireProtocolWriter writer;
        writer.WriteUnversionedRowset(transformExpr->Values);
        ToProto(proto->mutable_values(), MergeRefsToString(writer.Finish()));
        if (transformExpr->DefaultExpression) {
            ToProto(proto->mutable_default_expression(), transformExpr->DefaultExpression);
        }
    }
}

void FromProto(TConstExpressionPtr* original, const NProto::TExpression& serialized)
{
    auto type = EValueType(serialized.type());
    auto kind = EExpressionKind(serialized.kind());
    switch (kind) {
        case EExpressionKind::None: {
            *original = nullptr;
            return;
        }

        case EExpressionKind::Literal: {
            auto result = New<TLiteralExpression>(type);
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
            auto result = New<TFunctionExpression>(type);
            const auto& ext = serialized.GetExtension(NProto::TFunctionExpression::function_expression);
            result->FunctionName = ext.function_name();
            FromProto(&result->Arguments, ext.arguments());
            *original = result;
            return;
        }

        case EExpressionKind::UnaryOp: {
            auto result = New<TUnaryOpExpression>(type);
            const auto& ext = serialized.GetExtension(NProto::TUnaryOpExpression::unary_op_expression);
            result->Opcode = EUnaryOp(ext.opcode());
            FromProto(&result->Operand, ext.operand());
            *original = result;
            return;
        }

        case EExpressionKind::BinaryOp: {
            auto result = New<TBinaryOpExpression>(type);
            const auto& ext = serialized.GetExtension(NProto::TBinaryOpExpression::binary_op_expression);
            result->Opcode = EBinaryOp(ext.opcode());
            FromProto(&result->Lhs, ext.lhs());
            FromProto(&result->Rhs, ext.rhs());
            *original = result;
            return;
        }

        case EExpressionKind::In: {
            auto result = New<TInExpression>(type);
            const auto& ext = serialized.GetExtension(NProto::TInExpression::in_expression);
            FromProto(&result->Arguments, ext.arguments());
            NTabletClient::TWireProtocolReader reader(
                TSharedRef::FromString(ext.values()),
                New<TRowBuffer>(TInExpressionValuesTag()));
            result->Values = reader.ReadUnversionedRowset(true);
            *original = result;
            return;
        }

        case EExpressionKind::Transform: {
            auto result = New<TTransformExpression>(type);
            const auto& ext = serialized.GetExtension(NProto::TTransformExpression::transform_expression);
            FromProto(&result->Arguments, ext.arguments());
            NTabletClient::TWireProtocolReader reader(
                TSharedRef::FromString(ext.values()),
                New<TRowBuffer>());
            result->Values = reader.ReadUnversionedRowset(true);
            if (ext.has_default_expression()) {
                FromProto(&result->DefaultExpression, ext.default_expression());
            }
            *original = result;
            return;
        }

        default:
            Y_UNREACHABLE();
    }
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
    ToProto(serialized->mutable_expression(), original.Expression);
    serialized->set_aggregate_function_name(original.AggregateFunction);
    serialized->set_state_type(static_cast<int>(original.StateType));
    serialized->set_result_type(static_cast<int>(original.ResultType));
    ToProto(serialized->mutable_name(), original.Name);
}

void FromProto(TAggregateItem* original, const NProto::TAggregateItem& serialized)
{
    *original = TAggregateItem(
        FromProto<TConstExpressionPtr>(serialized.expression()),
        serialized.aggregate_function_name(),
        serialized.name(),
        EValueType(serialized.state_type()),
        EValueType(serialized.result_type()));
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TSelfEquation* proto, const std::pair<TConstExpressionPtr, bool>& original)
{
    ToProto(proto->mutable_expression(), original.first);
    proto->set_is_key(original.second);
}

void FromProto(std::pair<TConstExpressionPtr, bool>* original, const NProto::TSelfEquation& serialized)
{
    FromProto(&original->first, serialized.expression());
    FromProto(&original->second, serialized.is_key());
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
    ToProto(proto->mutable_original_schema(), original->OriginalSchema);
    ToProto(proto->mutable_schema_mapping(), original->SchemaMapping);
    ToProto(proto->mutable_self_joined_columns(), original->SelfJoinedColumns);
    ToProto(proto->mutable_foreign_joined_columns(), original->ForeignJoinedColumns);

    ToProto(proto->mutable_foreign_equations(), original->ForeignEquations);
    ToProto(proto->mutable_self_equations(), original->SelfEquations);

    ToProto(proto->mutable_foreign_data_id(), original->ForeignDataId);
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
}

void FromProto(TConstJoinClausePtr* original, const NProto::TJoinClause& serialized)
{
    auto result = New<TJoinClause>();
    FromProto(&result->OriginalSchema, serialized.original_schema());
    FromProto(&result->SchemaMapping, serialized.schema_mapping());
    FromProto(&result->SelfJoinedColumns, serialized.self_joined_columns());
    FromProto(&result->ForeignJoinedColumns, serialized.foreign_joined_columns());
    FromProto(&result->ForeignEquations, serialized.foreign_equations());
    FromProto(&result->SelfEquations, serialized.self_equations());
    FromProto(&result->ForeignDataId, serialized.foreign_data_id());
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

    *original = result;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TGroupClause* proto, const TConstGroupClausePtr& original)
{
    ToProto(proto->mutable_group_items(), original->GroupItems);
    ToProto(proto->mutable_aggregate_items(), original->AggregateItems);
    proto->set_totals_mode(static_cast<int>(original->TotalsMode));
}

void FromProto(TConstGroupClausePtr* original, const NProto::TGroupClause& serialized)
{
    auto result = New<TGroupClause>();

    result->TotalsMode = ETotalsMode(serialized.totals_mode());
    FromProto(&result->GroupItems, serialized.group_items());
    FromProto(&result->AggregateItems, serialized.aggregate_items());
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

void ToProto(NProto::TOrderItem* serialized, const TOrderItem& original)
{
    ToProto(serialized->mutable_expression(), original.first);
    serialized->set_is_descending(original.second);
}

void FromProto(TOrderItem* original, const NProto::TOrderItem& serialized)
{
    FromProto(&original->first, serialized.expression());
    FromProto(&original->second, serialized.is_descending());
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

    serialized->set_limit(original->Limit);
    serialized->set_use_disjoint_group_by(original->UseDisjointGroupBy);
    serialized->set_infer_ranges(original->InferRanges);
    serialized->set_is_final(original->IsFinal);

    ToProto(serialized->mutable_original_schema(), original->OriginalSchema);
    ToProto(serialized->mutable_schema_mapping(), original->SchemaMapping);

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

    result->Limit = serialized.limit();
    result->UseDisjointGroupBy = serialized.use_disjoint_group_by();
    result->InferRanges = serialized.infer_ranges();
    FromProto(&result->IsFinal, serialized.is_final());

    FromProto(&result->OriginalSchema, serialized.original_schema());
    FromProto(&result->SchemaMapping, serialized.schema_mapping());

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
    serialized->set_timestamp(original.Timestamp);
    serialized->set_verbose_logging(original.VerboseLogging);
    serialized->set_max_subqueries(original.MaxSubqueries);
    serialized->set_enable_code_cache(original.EnableCodeCache);
    ToProto(serialized->mutable_workload_descriptor(), original.WorkloadDescriptor);
    serialized->set_use_multijoin(original.UseMultijoin);
    serialized->set_allow_full_scan(original.AllowFullScan);
    ToProto(serialized->mutable_read_session_id(), original.ReadSessionId);
    serialized->set_deadline(ToProto<ui64>(original.Deadline));
}

void FromProto(TQueryOptions* original, const NProto::TQueryOptions& serialized)
{
    original->Timestamp = serialized.timestamp();
    original->VerboseLogging = serialized.verbose_logging();
    original->MaxSubqueries = serialized.max_subqueries();
    original->EnableCodeCache = serialized.enable_code_cache();
    original->WorkloadDescriptor = serialized.has_workload_descriptor()
        ? FromProto<TWorkloadDescriptor>(serialized.workload_descriptor())
        : TWorkloadDescriptor();
    original->UseMultijoin = serialized.use_multijoin();
    original->AllowFullScan = serialized.allow_full_scan();
    original->ReadSessionId  = serialized.has_read_session_id()
        ? FromProto<NChunkClient::TReadSessionId>(serialized.read_session_id())
        : NChunkClient::TReadSessionId::Create();
    original->Deadline = serialized.has_deadline()
        ? FromProto<TInstant>(serialized.deadline())
        : TInstant::Max();
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TDataRanges* serialized, const TDataRanges& original)
{
    ToProto(serialized->mutable_id(), original.Id);
    serialized->set_mount_revision(original.MountRevision);

    NTabletClient::TWireProtocolWriter rangesWriter;
    for (const auto& range : original.Ranges) {
        rangesWriter.WriteUnversionedRow(range.first);
        rangesWriter.WriteUnversionedRow(range.second);
    }
    ToProto(serialized->mutable_ranges(), MergeRefsToString(rangesWriter.Finish()));

    if (original.Keys) {
        std::vector<TColumnSchema> columns;
        for (auto type : original.Schema) {
            columns.emplace_back("", type);
        }

        TTableSchema schema(columns);
        NTabletClient::TWireProtocolWriter keysWriter;
        keysWriter.WriteTableSchema(schema);
        keysWriter.WriteSchemafulRowset(original.Keys);
        ToProto(serialized->mutable_keys(), MergeRefsToString(keysWriter.Finish()));
    }
    serialized->set_lookup_supported(original.LookupSupported);
    serialized->set_key_width(original.KeyWidth);
}

void FromProto(TDataRanges* original, const NProto::TDataRanges& serialized)
{
    FromProto(&original->Id, serialized.id());
    original->MountRevision = serialized.mount_revision();

    struct TDataRangesBufferTag
    { };

    TRowRanges ranges;
    auto rowBuffer = New<TRowBuffer>(TDataRangesBufferTag());
    NTabletClient::TWireProtocolReader rangesReader(
        TSharedRef::FromString<TDataRangesBufferTag>(serialized.ranges()),
        rowBuffer);
    while (!rangesReader.IsFinished()) {
        auto lowerBound = rangesReader.ReadUnversionedRow(true);
        auto upperBound = rangesReader.ReadUnversionedRow(true);
        ranges.emplace_back(lowerBound, upperBound);
    }
    original->Ranges = MakeSharedRange(std::move(ranges), rowBuffer);

    if (serialized.has_keys()) {
        NTabletClient::TWireProtocolReader keysReader(
            TSharedRef::FromString<TDataRangesBufferTag>(serialized.keys()),
            rowBuffer);

        auto schema = keysReader.ReadTableSchema();
        auto schemaData = keysReader.GetSchemaData(schema, NTableClient::TColumnFilter());
        original->Keys = keysReader.ReadSchemafulRowset(schemaData, true);
    }
    original->LookupSupported = serialized.lookup_supported();
    original->KeyWidth = serialized.key_width();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
