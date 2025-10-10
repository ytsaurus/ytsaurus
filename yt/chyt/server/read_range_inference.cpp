#include "read_range_inference.h"

#include "conversion.h"
#include "config.h"

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

namespace {

using NYT::NQueryClient::EBinaryOp;
using NYT::NTableClient::TUnversionedRow;

////////////////////////////////////////////////////////////////////////////////

static const std::unordered_map<std::string, EBinaryOp> binaryOpNameToOpCode
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

////////////////////////////////////////////////////////////////////////////////

NYT::TSharedRange<TUnversionedRow> ConvertPreparedSetToSharedRange(DB::DataTypePtr targetDataType, DB::QueryTreeNodePtr node)
{
    auto constantNode = node->as<DB::ConstantNode>();
    if (!constantNode) {
        return {};
    }

    if (constantNode->getResultType()->getTypeId() != DB::TypeIndex::Tuple) {
        return {};
    }
    auto tupleType = dynamic_pointer_cast<const DB::DataTypeTuple>(constantNode->getResultType());
    auto tupleElementTypes = tupleType->getElements();

    const auto& tupleValues = constantNode->getValue().safeGet<const DB::Tuple&>();
    std::vector<DB::Field> convertedValues;
    convertedValues.reserve(tupleValues.size());
    for (const auto& [value, data] : Zip(tupleValues, tupleElementTypes)) {
        auto convertedValue = DB::convertFieldToTypeStrict(value, *data, *targetDataType);
        if (!convertedValue.has_value()) {
            return {};
        }
        convertedValues.emplace_back(std::move(*convertedValue));
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


} // namespace

namespace NYT::NClickHouseServer {

using namespace NYT::NTableClient;
using namespace NYT::NQueryClient;

////////////////////////////////////////////////////////////////////////////////


// TODO (buyval01) : Support complex tuple expression:
// (k, l) >= (1, 2) AND (k, l) < (1, 4)
// expr: (k > 0#1 OR k = 0#1 AND l >= 0#2) AND (k < 0#1 OR k = 0#1 AND l < 0#4)
// (k, m) IN ((2, 3), (4, 6)) AND l IN (2, 3)
// expr: ((k, m) IN ([0#2, 0#3], [0#4, 0#6])) AND (l IN ([0#2], [0#3]))
TConstExpressionPtr ConnverterImpl(
    const TCompositeSettingsPtr& settings,
    const TTableSchemaPtr& schema,
    DB::QueryTreeNodePtr node,
    DB::DataTypePtr& desiredDataType,
    std::optional<EValueType>& desiredValueType)
{
    TConstExpressionPtr result;

    switch (node->getNodeType())
    {
        case DB::QueryTreeNodeType::COLUMN: {
            auto columnNode = node->as<DB::ColumnNode&>();
            if (auto columnSchema = schema->FindColumn(columnNode.getColumnName())) {
                result =  New<NYT::NQueryClient::TReferenceExpression>(
                    SimpleLogicalType(ESimpleLogicalValueType::Null),
                    columnNode.getColumnName());
                desiredDataType = ToDataType(*columnSchema, settings);
                desiredValueType = columnSchema->GetWireType();
            }
            break;
        }

        case DB::QueryTreeNodeType::CONSTANT: {
            auto constantNode = node->as<DB::ConstantNode&>();

            auto constantDataType = constantNode.getResultType();
            auto constantValueType = NTableClient::GetWireType(ToLogicalType(constantDataType, settings));

            auto field = constantNode.getValue();
            if (desiredDataType) {
                field = DB::convertFieldToType(field, *desiredDataType);
            }

            TUnversionedValue value;
            ToUnversionedValue(
                field,
                (desiredDataType != nullptr) ? desiredDataType : constantDataType,
                settings,
                &value);

            result = New<NYT::NQueryClient::TLiteralExpression>(
                (desiredValueType.has_value() ? *desiredValueType : constantValueType),
                value);

            desiredDataType = constantDataType;
            desiredValueType = constantValueType;

            break;
        }

        case DB::QueryTreeNodeType::FUNCTION: {
            auto funcNode = node->as<DB::FunctionNode>();
            auto name = funcNode->getFunctionName();
            auto arguments = funcNode->getArguments().getNodes();

            if (name == "not") {
                auto argument = AdjustToYTBooleanExpression(arguments[0]);
                if (auto arg = ConnverterImpl(settings, schema, argument, desiredDataType, desiredValueType)) {
                    result = New<NYT::NQueryClient::TUnaryOpExpression>(
                        EValueType::Boolean,
                        NQueryClient::EUnaryOp::Not,
                        std::move(arg));
                }
            } else if (name == "isNull" || name == "isNotNull") {
                if (auto arg = ConnverterImpl(settings, schema, arguments[0], desiredDataType, desiredValueType)) {
                    result = New<TFunctionExpression>(
                        EValueType::Boolean,
                        "is_null",
                        std::initializer_list<TConstExpressionPtr>({std::move(arg)}));
                    if (name == "isNotNull") {
                        result = New<NYT::NQueryClient::TUnaryOpExpression>(
                            EValueType::Boolean,
                            NQueryClient::EUnaryOp::Not,
                            std::move(result));
                    }
                }
            } else if (binaryOpNameToOpCode.contains(name)) {
                auto lhsNode = arguments[0];
                auto rhsNode = arguments[1];

                if (rhsNode->getNodeType() == DB::QueryTreeNodeType::COLUMN) {
                    lhsNode.swap(rhsNode);
                }

                DB::DataTypePtr lhsDataType;
                std::optional<EValueType> lhsValueType;

                // NB: CH uses integer literals and columns with the UInt8 data type as boolean values.
                // To use QL inferrer, we need to adapt such cases to valid logical expressions.
                if (name == "and" || name == "or") {
                    lhsNode = AdjustToYTBooleanExpression(lhsNode);
                    rhsNode = AdjustToYTBooleanExpression(rhsNode);

                    lhsDataType = desiredDataType;
                    lhsValueType = desiredValueType;
                }

                auto lhsExpr = ConnverterImpl(settings, schema, lhsNode, lhsDataType, lhsValueType);
                if (!lhsExpr) {
                    break;
                }

                auto rhsExpr = ConnverterImpl(settings, schema, rhsNode, lhsDataType, lhsValueType);
                if (lhsExpr && rhsExpr) {
                    result = New<NYT::NQueryClient::TBinaryOpExpression>(
                        EValueType::Boolean,
                        binaryOpNameToOpCode.at(name),
                        std::move(lhsExpr),
                        std::move(rhsExpr));
                }
            } else if (arguments.size() == 2 && name == "in") {
                DB::DataTypePtr argumentsDataType;
                std::optional<EValueType> argumentsValueType;
                auto argExpr = ConnverterImpl(settings, schema, arguments[0], argumentsDataType, argumentsValueType);
                if (!argExpr) {
                    break;
                }

                auto values = ConvertPreparedSetToSharedRange(argumentsDataType, arguments[1]);

                if (argExpr && !values.Empty()) {
                    result = New<NYT::NQueryClient::TInExpression>(
                        std::initializer_list<TConstExpressionPtr>({
                            std::move(argExpr)}),
                        std::move(values));
                }
            }
            break;
        }

        default:
            break;
    }

    return result;
}

TConstExpressionPtr ConvertToConstExpression(const TTableSchemaPtr& schema, DB::QueryTreeNodePtr node)
{
    DB::DataTypePtr desiredDataType = DB::DataTypeFactory::instance().get("bool");
    std::optional<EValueType> desiredValueType = EValueType::Boolean;

    node = AdjustToYTBooleanExpression(node);

    return ConnverterImpl(
        TCompositeSettings::Create(/*convertUnsupportedTypesToString*/ true),
        schema,
        node,
        desiredDataType,
        desiredValueType);
}

std::vector<NChunkClient::TReadRange> InferReadRange(
    DB::QueryTreeNodePtr filterNode,
    const NTableClient::TTableSchemaPtr& schema)
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

    std::vector<NChunkClient::TReadRange> result;
    result.reserve(rowRanges.size());
    for (const auto& rowRange : rowRanges) {
        result.emplace_back(
            NChunkClient::TReadLimit(KeyBoundFromLegacyRow(rowRange.first, /*isUpper*/ false, schema->GetKeyColumnCount())),
            NChunkClient::TReadLimit(KeyBoundFromLegacyRow(rowRange.second, /*isUpper*/ true, schema->GetKeyColumnCount())));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
