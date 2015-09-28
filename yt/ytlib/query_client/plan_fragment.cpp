#include "stdafx.h"

#include "plan_fragment.h"

#include <ytlib/tablet_client/wire_protocol.h>

#include <ytlib/table_client/schema.h>
#include <ytlib/table_client/chunk_meta_extensions.h>

#include <ytlib/query_client/plan_fragment.pb.h>

#include <ytlib/chunk_client/chunk_spec.pb.h>

#include <limits>
#include <cmath>

namespace NYT {
namespace NQueryClient {

using namespace NTableClient;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

Stroka InferName(TConstExpressionPtr expr, bool omitValues)
{
    bool newTuple = true;
    auto comma = [&] {
        bool isNewTuple = newTuple;
        newTuple = false;
        return Stroka(isNewTuple ? "" : ", ");
    };
    auto canOmitParenthesis = [] (TConstExpressionPtr expr) {
        return
            expr->As<TLiteralExpression>() ||
            expr->As<TReferenceExpression>() ||
            expr->As<TFunctionExpression>();
    };

    if (!expr) {
        return Stroka();
    } else if (auto literalExpr = expr->As<TLiteralExpression>()) {
        return omitValues
            ? ToString("?")
            : ToString(static_cast<TUnversionedValue>(literalExpr->Value));
    } else if (auto referenceExpr = expr->As<TReferenceExpression>()) {
        return referenceExpr->ColumnName;
    } else if (auto functionExpr = expr->As<TFunctionExpression>()) {
        auto str = functionExpr->FunctionName + "(";
        for (const auto& argument : functionExpr->Arguments) {
            str += comma() + InferName(argument, omitValues);
        }
        return str + ")";
    } else if (auto unaryOp = expr->As<TUnaryOpExpression>()) {
        auto rhsName = InferName(unaryOp->Operand, omitValues);
        if (!canOmitParenthesis(unaryOp->Operand)) {
            rhsName = "(" + rhsName + ")";
        }
        return Stroka() + GetUnaryOpcodeLexeme(unaryOp->Opcode) + " " + rhsName;
    } else if (auto binaryOp = expr->As<TBinaryOpExpression>()) {
        auto lhsName = InferName(binaryOp->Lhs, omitValues);
        if (!canOmitParenthesis(binaryOp->Lhs)) {
            lhsName = "(" + lhsName + ")";
        }
        auto rhsName = InferName(binaryOp->Rhs, omitValues);
        if (!canOmitParenthesis(binaryOp->Rhs)) {
            rhsName = "(" + rhsName + ")";
        }
        return
            lhsName +
            " " + GetBinaryOpcodeLexeme(binaryOp->Opcode) + " " +
            rhsName;
    } else if (auto inOp = expr->As<TInOpExpression>()) {
        Stroka str;
        for (const auto& argument : inOp->Arguments) {
            str += comma() + InferName(argument, omitValues);
        }
        if (inOp->Arguments.size() > 1) {
            str = "(" + str + ")";
        }
        str += " IN (";
        if (omitValues) {
            str += "??";
        } else {
            newTuple = true;
            for (const auto& row : inOp->Values) {
                str += comma() + ToString(row);
            }
        }
        return str + ")";
    } else {
        YUNREACHABLE();
    }
}

Stroka InferName(TConstQueryPtr query, bool omitValues)
{
    auto namedItemFormatter = [&] (const TNamedItem& item) {
        return Format("%v AS %v",
            InferName(item.Expression, omitValues),
            item.Name);
    };

    std::vector<Stroka> clauses;
    Stroka str;

    if (query->ProjectClause) {
        str = JoinToString(query->ProjectClause->Projections, namedItemFormatter);
    } else {
        str = "*";
    }
    clauses.emplace_back("SELECT " + str);

    if (query->WhereClause) {
        str = InferName(query->WhereClause, omitValues);
        clauses.push_back(Stroka("WHERE ") + str);
    }
    if (query->GroupClause) {
        str = JoinToString(query->GroupClause->GroupItems, namedItemFormatter);
        clauses.push_back(Stroka("GROUP BY ") + str);
    }
    if (query->HavingClause) {
        str = InferName(query->HavingClause, omitValues);
        clauses.push_back(Stroka("HAVING ") + str);
    }
    if (query->OrderClause) {
        str = JoinToString(query->OrderClause->OrderColumns);
        clauses.push_back(Stroka("ORDER BY ") + str);
    }
    if (query->Limit < std::numeric_limits<i64>::max()) {
        str = ToString(query->Limit);
        clauses.push_back(Stroka("LIMIT ") + str);
    }

    return JoinToString(clauses, Stroka(" "));
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TExpression* serialized, TConstExpressionPtr original)
{
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

            default:
                YUNREACHABLE();
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
    } else if (auto inOpExpr = original->As<TInOpExpression>()) {
        serialized->set_kind(static_cast<int>(EExpressionKind::InOp));
        auto* proto = serialized->MutableExtension(NProto::TInOpExpression::in_op_expression);
        ToProto(proto->mutable_arguments(), inOpExpr->Arguments);

        NTabletClient::TWireProtocolWriter writer;
        writer.WriteUnversionedRowset(inOpExpr->Values);
        ToProto(proto->mutable_values(), ToString(MergeRefs(writer.Flush())));
    } else {
        YUNREACHABLE();
    }
}

TExpressionPtr FromProto(const NProto::TExpression& serialized)
{
    auto kind = EExpressionKind(serialized.kind());
    auto type = EValueType(serialized.type());

    switch (kind) {
        case EExpressionKind::Literal: {
            auto typedResult = New<TLiteralExpression>(type);
            const auto& data = serialized.GetExtension(NProto::TLiteralExpression::literal_expression);

            switch (type) {
                case EValueType::Int64: {
                    typedResult->Value = MakeUnversionedInt64Value(data.int64_value());
                    break;
                }

                case EValueType::Uint64: {
                    typedResult->Value = MakeUnversionedUint64Value(data.uint64_value());
                    break;
                }

                case EValueType::Double: {
                    typedResult->Value = MakeUnversionedDoubleValue(data.double_value());
                    break;
                }

                case EValueType::String: {
                    typedResult->Value = MakeUnversionedStringValue(data.string_value());
                    break;
                }

                case EValueType::Boolean: {
                    typedResult->Value = MakeUnversionedBooleanValue(data.boolean_value());
                    break;
                }

                default:
                    YUNREACHABLE();
            }

            return typedResult;
        }

        case EExpressionKind::Reference: {
            auto typedResult = New<TReferenceExpression>(type);
            const auto& data = serialized.GetExtension(NProto::TReferenceExpression::reference_expression);
            typedResult->ColumnName = data.column_name();
            return typedResult;
        }

        case EExpressionKind::Function: {
            auto typedResult = New<TFunctionExpression>(type);
            const auto& data = serialized.GetExtension(NProto::TFunctionExpression::function_expression);
            typedResult->FunctionName = data.function_name();
            typedResult->Arguments.reserve(data.arguments_size());
            for (int i = 0; i < data.arguments_size(); ++i) {
                typedResult->Arguments.push_back(FromProto(data.arguments(i)));
            }
            return typedResult;
        }

        case EExpressionKind::UnaryOp: {
            auto typedResult = New<TUnaryOpExpression>(type);
            const auto& data = serialized.GetExtension(NProto::TUnaryOpExpression::unary_op_expression);
            typedResult->Opcode = EUnaryOp(data.opcode());
            typedResult->Operand = FromProto(data.operand());
            return typedResult;
        }

        case EExpressionKind::BinaryOp: {
            auto typedResult = New<TBinaryOpExpression>(type);
            const auto& data = serialized.GetExtension(NProto::TBinaryOpExpression::binary_op_expression);
            typedResult->Opcode = EBinaryOp(data.opcode());
            typedResult->Lhs = FromProto(data.lhs());
            typedResult->Rhs = FromProto(data.rhs());
            return typedResult;
        }

        case EExpressionKind::InOp: {
            auto typedResult = New<TInOpExpression>(type);
            const auto& data = serialized.GetExtension(NProto::TInOpExpression::in_op_expression);
            typedResult->Arguments.reserve(data.arguments_size());
            for (int i = 0; i < data.arguments_size(); ++i) {
                typedResult->Arguments.push_back(FromProto(data.arguments(i)));
            }

            NTabletClient::TWireProtocolReader reader(TSharedRef::FromString(data.values()));
            typedResult->Values = reader.ReadUnversionedRowset();

            return typedResult;
        } 
    }

    YUNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TNamedItem* serialized, const TNamedItem& original)
{
    ToProto(serialized->mutable_expression(), original.Expression);
    ToProto(serialized->mutable_name(), original.Name);
}

void ToProto(NProto::TAggregateItem* serialized, const TAggregateItem& original)
{
    ToProto(serialized->mutable_expression(), original.Expression);
    serialized->set_aggregate_function_name(original.AggregateFunction);
    serialized->set_state_type(static_cast<int>(original.StateType));
    serialized->set_result_type(static_cast<int>(original.ResultType));
    ToProto(serialized->mutable_name(), original.Name);
}

void ToProto(NProto::TEquation* proto, std::pair<TConstExpressionPtr, TConstExpressionPtr> original)
{
    ToProto(proto->mutable_left(), original.first);
    ToProto(proto->mutable_right(), original.second);
}

void ToProto(NProto::TJoinClause* proto, TConstJoinClausePtr original)
{
    ToProto(proto->mutable_equations(), original->Equations);
    ToProto(proto->mutable_joined_table_schema(), original->JoinedTableSchema);
    ToProto(proto->mutable_foreign_table_schema(), original->ForeignTableSchema);
    ToProto(proto->mutable_renamed_table_schema(), original->RenamedTableSchema);
    proto->set_foreign_key_columns_count(original->ForeignKeyColumnsCount);
    ToProto(proto->mutable_foreign_data_id(), original->ForeignDataId);
    proto->set_is_left(original->IsLeft);
}

void ToProto(NProto::TGroupClause* proto, TConstGroupClausePtr original)
{
    ToProto(proto->mutable_group_items(), original->GroupItems);
    ToProto(proto->mutable_aggregate_items(), original->AggregateItems);
    ToProto(proto->mutable_grouped_table_schema(), original->GroupedTableSchema);
    proto->set_is_merge(original->IsMerge);
    proto->set_is_final(original->IsFinal);
}

void ToProto(NProto::TProjectClause* proto, TConstProjectClausePtr original)
{
    ToProto(proto->mutable_projections(), original->Projections);
}

void ToProto(NProto::TOrderClause* proto, TConstOrderClausePtr original)
{
    ToProto(proto->mutable_order_columns(), original->OrderColumns);
    proto->set_is_descending(original->IsDescending);
}

void ToProto(NProto::TQuery* proto, TConstQueryPtr original)
{
    proto->set_input_row_limit(original->InputRowLimit);
    proto->set_output_row_limit(original->OutputRowLimit);

    ToProto(proto->mutable_id(), original->Id);

    proto->set_limit(original->Limit);
    ToProto(proto->mutable_table_schema(), original->TableSchema);
    ToProto(proto->mutable_renamed_table_schema(), original->RenamedTableSchema);
    proto->set_key_columns_count(original->KeyColumnsCount);

    ToProto(proto->mutable_join_clauses(), original->JoinClauses);

    if (original->WhereClause) {
        ToProto(proto->mutable_predicate(), original->WhereClause);
    }

    if (original->GroupClause) {
        ToProto(proto->mutable_group_clause(), original->GroupClause);
    }

    if (original->HavingClause) {
        ToProto(proto->mutable_having_clause(), original->HavingClause);
    }

    if (original->OrderClause) {
        ToProto(proto->mutable_order_clause(), original->OrderClause);
    }
    
    if (original->ProjectClause) {
        ToProto(proto->mutable_project_clause(), original->ProjectClause);
    }
}

TNamedItem FromProto(const NProto::TNamedItem& serialized)
{
    return TNamedItem(
        FromProto(serialized.expression()),
        serialized.name());
}

TAggregateItem FromProto(const NProto::TAggregateItem& serialized)
{
    Stroka aggregateFunction;
    EValueType stateType;
    EValueType resultType;
    if (serialized.has_aggregate_function_name()) {
        aggregateFunction = serialized.aggregate_function_name();
        stateType = EValueType(serialized.state_type());
        resultType = EValueType(serialized.result_type());
    } else {
        switch (EAggregateFunction(serialized.aggregate_function())) {
            case EAggregateFunction::Min:
                aggregateFunction = "min";
                break;
            case EAggregateFunction::Max:
                aggregateFunction = "max";
                break;
            case EAggregateFunction::Sum:
                aggregateFunction = "sum";
                break;
        }
        stateType = EValueType(serialized.expression().type());
        resultType = EValueType(serialized.expression().type());
    }

    return TAggregateItem(
        FromProto(serialized.expression()),
        aggregateFunction,
        serialized.name(),
        stateType,
        resultType);
}

TJoinClausePtr FromProto(const NProto::TJoinClause& serialized)
{
    auto result = New<TJoinClause>();

    result->Equations.reserve(serialized.equations_size());
    for (int i = 0; i < serialized.equations_size(); ++i) {
        result->Equations.emplace_back(
            FromProto(serialized.equations(i).left()),
            FromProto(serialized.equations(i).right()));
    }

    FromProto(&result->JoinedTableSchema, serialized.joined_table_schema());
    FromProto(&result->ForeignTableSchema, serialized.foreign_table_schema());
    FromProto(&result->RenamedTableSchema, serialized.renamed_table_schema());
    FromProto(&result->ForeignKeyColumnsCount, serialized.foreign_key_columns_count());
    FromProto(&result->ForeignDataId, serialized.foreign_data_id());
    FromProto(&result->IsLeft, serialized.is_left());

    return result;
}

TGroupClausePtr FromProto(const NProto::TGroupClause& serialized)
{
    auto result = New<TGroupClause>();
    FromProto(&result->GroupedTableSchema, serialized.grouped_table_schema());
    result->IsMerge = serialized.is_merge();
    result->IsFinal = serialized.is_final();

    result->GroupItems.reserve(serialized.group_items_size());
    for (int i = 0; i < serialized.group_items_size(); ++i) {
        result->GroupItems.push_back(FromProto(serialized.group_items(i)));
    }
    result->AggregateItems.reserve(serialized.aggregate_items_size());
    for (int i = 0; i < serialized.aggregate_items_size(); ++i) {
        result->AggregateItems.push_back(FromProto(serialized.aggregate_items(i)));
    }

    return result;
}

TProjectClausePtr FromProto(const NProto::TProjectClause& serialized)
{
    auto result = New<TProjectClause>();

    result->Projections.reserve(serialized.projections_size());
    for (int i = 0; i < serialized.projections_size(); ++i) {
        result->AddProjection(FromProto(serialized.projections(i)));
    }

    return result;
}

TOrderClausePtr FromProto(const NProto::TOrderClause& serialized)
{
    auto result = New<TOrderClause>();

    result->IsDescending = serialized.is_descending();

    result->OrderColumns.reserve(serialized.order_columns_size());
    for (int i = 0; i < serialized.order_columns_size(); ++i) {
        result->OrderColumns.push_back(serialized.order_columns(i));
    }

    return result;
}

TQueryPtr FromProto(const NProto::TQuery& serialized)
{
    auto query = New<TQuery>(
        serialized.input_row_limit(),
        serialized.output_row_limit(),
        NYT::FromProto<TGuid>(serialized.id()));

    query->Limit = serialized.limit();

    FromProto(&query->TableSchema, serialized.table_schema());
    FromProto(&query->RenamedTableSchema, serialized.renamed_table_schema());
    FromProto(&query->KeyColumnsCount, serialized.key_columns_count());

    query->JoinClauses.reserve(serialized.join_clauses_size());
    for (int i = 0; i < serialized.join_clauses_size(); ++i) {
        query->JoinClauses.push_back(FromProto(serialized.join_clauses(i)));
    }

    if (serialized.has_predicate()) {
        query->WhereClause = FromProto(serialized.predicate());
    }

    if (serialized.has_group_clause()) {
        query->GroupClause = FromProto(serialized.group_clause());       
    }

    if (serialized.has_having_clause()) {
        query->HavingClause = FromProto(serialized.having_clause());
    }

    if (serialized.has_order_clause()) {
        query->OrderClause = FromProto(serialized.order_clause());       
    }

    if (serialized.has_project_clause()) {
        query->ProjectClause = FromProto(serialized.project_clause());       
    }

    return query;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TPlanFragment* proto, TConstPlanFragmentPtr fragment)
{
    ToProto(proto->mutable_query(), fragment->Query);

    NTabletClient::TWireProtocolWriter writer;
    for (const auto& dataSource : fragment->DataSources) {
        ToProto(proto->add_data_id(), dataSource.Id);
        const auto& range = dataSource.Range;
        writer.WriteUnversionedRow(range.first);
        writer.WriteUnversionedRow(range.second);
    }

    ToProto(proto->mutable_data_bounds(), ToString(MergeRefs(writer.Flush())));

    proto->set_ordered(fragment->Ordered);
    proto->set_verbose_logging(fragment->VerboseLogging);
    proto->set_max_subqueries(fragment->MaxSubqueries);
    proto->set_enable_code_cache(fragment->EnableCodeCache);
    
    proto->set_source(fragment->Source);
    proto->set_timestamp(fragment->Timestamp);
}

TPlanFragmentPtr FromProto(const NProto::TPlanFragment& serialized)
{
    auto result = New<TPlanFragment>(serialized.source());

    result->Query = FromProto(serialized.query());
    result->Ordered = serialized.ordered();
    result->VerboseLogging = serialized.verbose_logging();
    result->MaxSubqueries = serialized.max_subqueries();
    result->EnableCodeCache = serialized.enable_code_cache();
    result->Timestamp = serialized.timestamp();

    NTabletClient::TWireProtocolReader reader(TSharedRef::FromString(serialized.data_bounds()));

    const auto& rowBuffer = result->KeyRangesRowBuffer;
    for (int i = 0; i < serialized.data_id_size(); ++i) {
        TDataSource dataSource;
        FromProto(&dataSource.Id, serialized.data_id(i));

        auto lowerBound = rowBuffer->Capture(reader.ReadUnversionedRow());
        auto upperBound = rowBuffer->Capture(reader.ReadUnversionedRow());

        dataSource.Range = TRowRange(lowerBound, upperBound);
        result->DataSources.push_back(dataSource);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
