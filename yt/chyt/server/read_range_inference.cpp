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
#include <Analyzer/ColumnNode.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/SetUtils.h>

#include <Core/Settings.h>

#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeFactory.h>

#include <Interpreters/convertFieldToType.h>

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

NYT::TSharedRange<TUnversionedRow> ConvertConstantSetToSharedRange(
    const DB::DataTypePtr& targetDataType,
    const DB::QueryTreeNodePtr& node,
    const NYT::NClickHouseServer::TCompositeSettingsPtr& settings,
    bool transformNullIn)
{
    auto constantNode = node->as<DB::ConstantNode>();
    if (!constantNode) {
        return {};
    }

    auto set = DB::getSetElementsForConstantValue(
        targetDataType,
        constantNode->getValue(),
        constantNode->getResultType(),
        transformNullIn);
    if (set.columns() != 1) {
        return {};
    }

    auto& column = set.getByPosition(0).column;
    auto columnSize = column->size();

    // NB: QL range inferrer expects that values to be sorted.
    DB::IColumn::Permutation permutation(columnSize);
    using TPermutationIndex = DB::IColumn::Permutation::value_type;
    DB::iota(permutation.data(), columnSize, TPermutationIndex(0));
    std::sort(permutation.begin(), permutation.end(), [&column](TPermutationIndex lhs, TPermutationIndex rhs) {
        // In YT, NULL values compare less than any other values.
        return column->compareAt(lhs, rhs, *column, /*nan_direction_hint*/ -1) < 0;
    });
    column = column->permute(permutation, /*limit*/ 0);

    return NYT::NClickHouseServer::ToRowRange(
        std::move(set),
        {targetDataType},
        {0},
        settings);
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

} // namespace

namespace NYT::NClickHouseServer {

using namespace NYT::NTableClient;
using namespace NYT::NQueryClient;

////////////////////////////////////////////////////////////////////////////////

struct TConversionContext
{
    const TTableSchemaPtr& Schema;
    const TCompositeSettingsPtr& ConversionSettings;
    bool TranformNullIn;
};

struct TExpressionConvertionResult
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
std::optional<TExpressionConvertionResult> ConnverterImpl(
    DB::QueryTreeNodePtr node,
    const DB::DataTypePtr& desiredDataType,
    std::optional<EValueType> desiredValueType,
    const TConversionContext& context
)
{
    std::optional<TExpressionConvertionResult> result;

    switch (node->getNodeType())
    {
        case DB::QueryTreeNodeType::COLUMN: {
            auto columnNode = node->as<DB::ColumnNode&>();
            if (auto columnSchema = context.Schema->FindColumn(columnNode.getColumnName())) {
                result.emplace();
                result->Expression =  New<NYT::NQueryClient::TReferenceExpression>(
                    SimpleLogicalType(ESimpleLogicalValueType::Null),
                    columnNode.getColumnName());
                // NB: Read range inference depends on the matching of the data types.
                // This is very similar to type conversion for write queries. That's why we need to use the same converters.
                // For example, both YT Timestamp (unsigned int) and Timestamp64 (signed int) types correspond to DateTime64(6),
                // but at this step of the reverse conversion, DateTime64(6) must be dispatched to different ValueType
                // in order for the constant node to be processed correctly.
                result->DataType = ToDataType(*columnSchema, context.ConversionSettings, /*isReadConversion*/ false);
                result->ValueType = columnSchema->GetWireType();
            }
            break;
        }

        case DB::QueryTreeNodeType::CONSTANT: {
            auto constantNode = node->as<DB::ConstantNode&>();

            auto constantDataType = constantNode.getResultType();
            auto constantValueType = GetWireType(ToLogicalType(constantDataType, context.ConversionSettings));

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

            result->Expression = New<NYT::NQueryClient::TLiteralExpression>(
                result->ValueType,
                ToUnversionedOwningValue(field,result->DataType, context.ConversionSettings));

            break;
        }

        case DB::QueryTreeNodeType::FUNCTION: {
            auto funcNode = node->as<DB::FunctionNode>();
            auto name = funcNode->getFunctionName();
            auto arguments = funcNode->getArguments().getNodes();

            if (name == "not") {
                auto argument = AdjustToYTBooleanExpression(arguments[0]);
                if (auto arg = ConnverterImpl(argument, GetDataTypeBoolean(), EValueType::Boolean, context)) {
                    result.emplace();
                    result->Expression = New<NYT::NQueryClient::TUnaryOpExpression>(
                        EValueType::Boolean,
                        NQueryClient::EUnaryOp::Not,
                        std::move(arg->Expression));
                }
            } else if (name == "isNull" || name == "isNotNull") {
                if (auto arg = ConnverterImpl(arguments[0], desiredDataType, desiredValueType, context)) {
                    TConstExpressionPtr expr = New<TFunctionExpression>(
                        EValueType::Boolean,
                        "is_null",
                        std::initializer_list<TConstExpressionPtr>({std::move(arg->Expression)}));
                    if (name == "isNotNull") {
                        expr = New<NYT::NQueryClient::TUnaryOpExpression>(
                            EValueType::Boolean,
                            NQueryClient::EUnaryOp::Not,
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

                auto lhsExpr = ConnverterImpl(lhsNode, desiredLhsDataType, desiredLhsValueType, context);
                if (!lhsExpr) {
                    break;
                }

                auto rhsExpr = ConnverterImpl(rhsNode, lhsExpr->DataType, lhsExpr->ValueType, context);
                if (rhsExpr) {
                    result.emplace();
                    result->Expression = New<NYT::NQueryClient::TBinaryOpExpression>(
                        EValueType::Boolean,
                        opCode,
                        std::move(lhsExpr->Expression),
                        std::move(rhsExpr->Expression));
                }
            } else if (arguments.size() == 2 && name == "in") {
                auto argument = ConnverterImpl(arguments[0], desiredDataType, desiredValueType, context);
                if (!argument) {
                    break;
                }

                auto values = ConvertConstantSetToSharedRange(
                    argument->DataType,
                    arguments[1],
                    context.ConversionSettings,
                    context.TranformNullIn);
                if (!values.Empty()) {
                    result.emplace();
                    result->Expression = New<NYT::NQueryClient::TInExpression>(
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

TConstExpressionPtr ConvertToConstExpression(
    DB::QueryTreeNodePtr node,
    const TTableSchemaPtr& schema,
    const TCompositeSettingsPtr& settings,
    bool transformNullIn)
{
    node = AdjustToYTBooleanExpression(node);

    auto result = ConnverterImpl(
        node,
        GetDataTypeBoolean(),
        EValueType::Boolean,
        TConversionContext{
            .Schema = schema,
            .ConversionSettings = settings,
            .TranformNullIn = transformNullIn,
        });
    return result ? result->Expression : nullptr;
}

std::vector<NChunkClient::TReadRange> InferReadRange(
    DB::QueryTreeNodePtr filterNode,
    const TTableSchemaPtr& schema,
    const DB::Settings& settings)
{
    if (!filterNode) {
        return {};
    }

    auto predicateExpr = ConvertToConstExpression(
        std::move(filterNode),
        schema,
        TCompositeSettings::Create(/*convertUnsupportedTypesToString*/ true),
        settings.transform_null_in);
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

    std::vector<NChunkClient::TReadRange> result;
    result.reserve(rowRanges.size());
    for (const auto& rowRange : rowRanges) {
        auto lowerLimit = KeyBoundFromLegacyRow(rowRange.first, /*isUpper*/ false, schema->GetKeyColumnCount());
        auto upperLimit = KeyBoundFromLegacyRow(rowRange.second, /*isUpper*/ true, schema->GetKeyColumnCount());
        if (lowerLimit.IsUniversal() && upperLimit.IsUniversal()) {
            continue;
        }
        result.emplace_back(NChunkClient::TReadLimit(lowerLimit), NChunkClient::TReadLimit(upperLimit));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
