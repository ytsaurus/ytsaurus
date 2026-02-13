#include "read_range_inference.h"

#include "conversion.h"
#include "config.h"
#include "custom_data_types.h"

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/table_client/unversioned_value.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/library/query/base/query.h>

#include <yt/yt/library/query/engine_api/new_range_inferrer.h>

#include <library/cpp/iterator/zip.h>

#include <library/cpp/yt/memory/chunked_memory_pool.h>
#include <library/cpp/yt/memory/shared_range.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>

#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeFactory.h>

#include <Interpreters/convertFieldToType.h>

namespace NYT::NClickHouseServer {

using namespace NYT::NChunkClient;
using namespace NYT::NTableClient;
using namespace NYT::NQueryClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

using NYT::NQueryClient::EBinaryOp;
using NYT::NTableClient::TUnversionedRow;

////////////////////////////////////////////////////////////////////////////////

const std::unordered_map<std::string, EBinaryOp> BinaryOpNameToOpCode
{
    {"equals", EBinaryOp::Equal},
    {"notEquals", EBinaryOp::NotEqual},
    {"less", EBinaryOp::Less},
    {"lessOrEquals", EBinaryOp::LessOrEqual},
    {"greater", EBinaryOp::Greater},
    {"greaterOrEquals", EBinaryOp::GreaterOrEqual},
    {"and", EBinaryOp::And},
    {"or", EBinaryOp::Or},
};

const std::unordered_map<EBinaryOp, EBinaryOp> BinaryOpToConversedOp
{
    {EBinaryOp::Equal, EBinaryOp::Equal},
    {EBinaryOp::NotEqual, EBinaryOp::NotEqual},
    {EBinaryOp::Less, EBinaryOp::Greater},
    {EBinaryOp::LessOrEqual, EBinaryOp::GreaterOrEqual},
    {EBinaryOp::Greater, EBinaryOp::Less},
    {EBinaryOp::GreaterOrEqual, EBinaryOp::LessOrEqual},
    {EBinaryOp::And, EBinaryOp::And},
    {EBinaryOp::Or, EBinaryOp::Or},
};

////////////////////////////////////////////////////////////////////////////////

NYT::TSharedRange<TUnversionedRow> ConvertPreparedSetToSharedRange(const DB::DataTypePtr& targetDataType, const DB::QueryTreeNodePtr& node)
{
    auto constantNode = node->as<DB::ConstantNode>();
    if (!constantNode) {
        return {};
    }

    std::vector<DB::Field> convertedValues;
    if (constantNode->getResultType()->getTypeId() == DB::TypeIndex::Tuple) {
        auto tupleType = dynamic_pointer_cast<const DB::DataTypeTuple>(constantNode->getResultType());
        const auto& types = tupleType->getElements();
        const auto& values = constantNode->getValue().safeGet<DB::Tuple>();

        convertedValues.reserve(values.size());
        for (const auto& [value, type] : Zip(values, types)) {
            auto convertedValue = DB::convertFieldToTypeStrict(value, *type, *targetDataType);
            if (!convertedValue.has_value()) {
                convertedValues.clear();
                break;
            }
            convertedValues.emplace_back(std::move(*convertedValue));
        }

    } else {
        // Assume "value in (42)".
        auto convertedValue = DB::convertFieldToTypeStrict(constantNode->getValue(), *constantNode->getResultType(), *targetDataType);
        if (convertedValue.has_value()) {
            convertedValues.emplace_back(std::move(*convertedValue));
        }
    }

    if (convertedValues.empty()) {
        return {};
    }

    // NB: QL range inferrer expects that values to be sorted.
    std::sort(convertedValues.begin(), convertedValues.end());

    auto column = targetDataType->createColumn();
    for (auto& value : convertedValues) {
        column->insert(std::move(value));
    }

    return NYT::NClickHouseServer::ToRowRange(
        DB::Block(
            {DB::ColumnWithTypeAndName(std::move(column), targetDataType, /*name*/ "")}),
            {targetDataType},
            {0},
            NYT::NClickHouseServer::TCompositeSettings::Create(true));
}

////////////////////////////////////////////////////////////////////////////////

DB::QueryTreeNodePtr AdjustToYTBooleanExpression(DB::QueryTreeNodePtr node)
{
    if (node->getNodeType() != DB::QueryTreeNodeType::COLUMN) {
        return node;
    }

    auto funcNode = std::make_shared<DB::FunctionNode>("notEquals");
    auto& argumets = funcNode->getArguments().getNodes();
    argumets.push_back(node);
    argumets.push_back(std::make_shared<DB::ConstantNode>(DB::Field(0)));

    return funcNode;
}

////////////////////////////////////////////////////////////////////////////////

struct ExpressionConvertionResult
{
    TConstExpressionPtr Expression;
    DB::DataTypePtr DataType;
    EValueType ValueType;
};

// TODO (buyval01) : Support complex tuple expression:
// (k, l) >= (1, 2) AND (k, l) < (1, 4)
// expr: (k > 0#1 OR k = 0#1 AND l >= 0#2) AND (k < 0#1 OR k = 0#1 AND l < 0#4)
// (k, m) IN ((2, 3), (4, 6)) AND l IN (2, 3)
// expr: ((k, m) IN ([0#2, 0#3], [0#4, 0#6])) AND (l IN ([0#2], [0#3]))
std::optional<ExpressionConvertionResult> ConnverterImpl(
    const TCompositeSettingsPtr& settings,
    const TTableSchemaPtr& schema,
    DB::QueryTreeNodePtr node,
    const DB::DataTypePtr& desiredDataType,
    std::optional<EValueType> desiredValueType)
{
    std::optional<ExpressionConvertionResult> result;

    switch (node->getNodeType()) {
        case DB::QueryTreeNodeType::COLUMN: {
            auto columnNode = node->as<DB::ColumnNode&>();
            if (auto columnSchema = schema->FindColumn(columnNode.getColumnName())) {
                result.emplace();
                result->Expression =  New<TReferenceExpression>(
                    SimpleLogicalType(ESimpleLogicalValueType::Null),
                    columnNode.getColumnName());
                // NB: Read range inference depends on the matching of the data types.
                // This is very similar to type conversion for write queries. That's why we need to use the same converters.
                // For example, both YT Timestamp (unsigned int) and Timestamp64 (signed int) types correspond to DateTime64(6),
                // but at this step of the reverse conversion, DateTime64(6) must be dispatched to different ValueType
                // in order for the constant node to be processed correctly.
                result->DataType = ToDataType(*columnSchema, settings, /*isLowCardinality*/ false, /*isReadConversion*/ false);
                result->ValueType = columnSchema->GetWireType();
            }
            break;
        }

        case DB::QueryTreeNodeType::CONSTANT: {
            auto constantNode = node->as<DB::ConstantNode&>();

            auto constantDataType = constantNode.getResultType();
            auto constantValueType = GetWireType(ToLogicalType(constantDataType, settings));

            auto field = constantNode.getValue();
            if (desiredDataType) {
                auto convertedField = DB::convertFieldToTypeStrict(field, *constantDataType, *desiredDataType);
                if (!convertedField) {
                    break;
                }
                field = std::move(*convertedField);
            }

            result.emplace();
            result->DataType = (desiredDataType != nullptr) ? desiredDataType : constantDataType;
            result->ValueType = (desiredValueType.has_value() ? *desiredValueType : constantValueType);

            result->Expression = New<TLiteralExpression>(
                result->ValueType,
                ToUnversionedOwningValue(field,result->DataType, settings));

            break;
        }

        case DB::QueryTreeNodeType::FUNCTION: {
            auto funcNode = node->as<DB::FunctionNode>();
            auto name = funcNode->getFunctionName();
            auto arguments = funcNode->getArguments().getNodes();

            if (name == "not") {
                auto argument = AdjustToYTBooleanExpression(arguments[0]);
                if (auto arg = ConnverterImpl(settings, schema, argument, GetDataTypeBoolean(), EValueType::Boolean)) {
                    result.emplace();
                    result->Expression = New<TUnaryOpExpression>(
                        EValueType::Boolean,
                        EUnaryOp::Not,
                        std::move(arg->Expression));
                }
            } else if (name == "isNull" || name == "isNotNull") {
                if (auto arg = ConnverterImpl(settings, schema, arguments[0], desiredDataType, desiredValueType)) {
                    TConstExpressionPtr expr = New<TFunctionExpression>(
                        EValueType::Boolean,
                        "is_null",
                        std::initializer_list<TConstExpressionPtr>({std::move(arg->Expression)}));
                    if (name == "isNotNull") {
                        expr = New<TUnaryOpExpression>(
                            EValueType::Boolean,
                            EUnaryOp::Not,
                            std::move(expr));
                    }
                    result.emplace();
                    result->Expression = std::move(expr);
                }
            } else if (auto it = BinaryOpNameToOpCode.find(name); it != BinaryOpNameToOpCode.end()) {
                auto opCode = it->second;

                auto lhsNode = arguments[0];
                auto rhsNode = arguments[1];

                if (rhsNode->getNodeType() == DB::QueryTreeNodeType::COLUMN) {
                    lhsNode.swap(rhsNode);
                    opCode = BinaryOpToConversedOp.at(opCode);
                }

                DB::DataTypePtr desiredLhsDataType;
                std::optional<EValueType> desiredLhsValueType;

                // NB: CH uses integer literals and columns with the UInt8 data type as boolean values.
                // To use QL inferrer, we need to adapt such cases to valid logical expressions.
                if (name == "and" || name == "or") {
                    lhsNode = AdjustToYTBooleanExpression(lhsNode);
                    rhsNode = AdjustToYTBooleanExpression(rhsNode);

                    desiredLhsDataType = GetDataTypeBoolean();
                    desiredLhsValueType = EValueType::Boolean;
                }

                auto lhsExpr = ConnverterImpl(settings, schema, lhsNode, desiredLhsDataType, desiredLhsValueType);
                if (!lhsExpr) {
                    break;
                }

                auto rhsExpr = ConnverterImpl(settings, schema, rhsNode, lhsExpr->DataType, lhsExpr->ValueType);
                if (rhsExpr) {
                    result.emplace();
                    result->Expression = New<TBinaryOpExpression>(
                        EValueType::Boolean,
                        opCode,
                        std::move(lhsExpr->Expression),
                        std::move(rhsExpr->Expression));
                }
            } else if (arguments.size() == 2 && name == "in") {
                auto argument = ConnverterImpl(settings, schema, arguments[0], desiredDataType, desiredValueType);
                if (!argument) {
                    break;
                }

                auto values = ConvertPreparedSetToSharedRange(argument->DataType, arguments[1]);
                if (!values.Empty()) {
                    result.emplace();
                    result->Expression = New<TInExpression>(
                        std::initializer_list<TConstExpressionPtr>({
                            std::move(argument->Expression)}),
                        std::move(values));
                }
            }

            if (result) {
                result->DataType = GetDataTypeBoolean();
                result->ValueType = EValueType::Boolean;
            }

            break;
        }

        default:
            break;
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TConstExpressionPtr ConvertToConstExpression(const TTableSchemaPtr& schema, DB::QueryTreeNodePtr node)
{
    node = AdjustToYTBooleanExpression(node);
    auto result = ConnverterImpl(
        TCompositeSettings::Create(/*convertUnsupportedTypesToString*/ true),
        schema,
        node,
        GetDataTypeBoolean(),
        EValueType::Boolean);
    return result ? result->Expression : nullptr;
}

std::vector<TReadRange> InferReadRange(
    DB::QueryTreeNodePtr filterNode,
    const TTableSchemaPtr& schema)
{
    if (!filterNode) {
        return {};
    }

    auto predicateExpr = ConvertToConstExpression(schema, std::move(filterNode));
    if (!predicateExpr) {
        return {};
    }

    auto rowRanges = NQueryClient::CreateNewRangeInferrer(
        predicateExpr,
        schema,
        schema->GetKeyColumns(),
        /*evaluatorCache*/ nullptr,
        NQueryClient::GetBuiltinConstraintExtractors(),
        /*options*/ {.RangeExpansionLimit = 1000},
        GetDefaultMemoryChunkProvider(),
        /*forceLightRangeInference*/ true);

    std::vector<TReadRange> result;
    result.reserve(rowRanges.size());
    for (const auto& rowRange : rowRanges) {
        auto lowerLimit = KeyBoundFromLegacyRow(rowRange.first, /*isUpper*/ false, schema->GetKeyColumnCount());
        auto upperLimit = KeyBoundFromLegacyRow(rowRange.second, /*isUpper*/ true, schema->GetKeyColumnCount());
        if (lowerLimit.IsUniversal() && upperLimit.IsUniversal()) {
            continue;
        }
        result.emplace_back(TReadLimit(lowerLimit), TReadLimit(upperLimit));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
