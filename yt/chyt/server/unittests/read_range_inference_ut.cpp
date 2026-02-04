#include "helpers.h"

#include <yt/chyt/server/config.h>
#include <yt/chyt/server/read_range_inference.h>
#include <yt/chyt/server/storage_distributor.h>
#include <yt/chyt/server/table.h>

#include <library/cpp/yt/logging/logger.h>
#include <library/cpp/yt/yson_string/string.h>

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>

#include <yt/yt/library/query/base/public.h>
#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_helpers.h>

#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Passes/QueryAnalysisPass.h>

#include <Interpreters/Context.h>

#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>

#include <Storages/StorageDummy.h>

namespace NYT::NClickHouseServer {
namespace {

using namespace NYT::NQueryClient;
using namespace NYT::NTableClient;

////////////////////////////////////////////////////////////////////////////////

NYT::TSharedRange<TRow> MakeRows(TStringBuf yson)
{
    using NYT::NYTree::INodePtr;
    TUnversionedRowBuilder keyBuilder;
    auto keyParts = ConvertTo<std::vector<INodePtr>>(
        NYT::NYson::TYsonString(yson, NYT::NYson::EYsonType::ListFragment));

    auto buffer = New<TRowBuffer>();
    std::vector<TRow> rows;

    for (int id = 0; id < std::ssize(keyParts); ++id) {
        keyBuilder.Reset();

        const auto& keyPart = keyParts[id];
        switch (keyPart->GetType()) {
            #define XX(type, cppType) \
            case NYTree::ENodeType::type: \
                keyBuilder.AddValue(Make ## type ## Value<TUnversionedValue>( \
                    keyPart->As ## type()->GetValue(), \
                    id)); \
                break;
            ITERATE_SCALAR_YTREE_NODE_TYPES(XX)
            #undef XX
            case NYTree::ENodeType::Entity:
                keyBuilder.AddValue(MakeSentinelValue<TUnversionedValue>(
                    keyPart->Attributes().Get<EValueType>("type"),
                    id));
                break;
            default:
                keyBuilder.AddValue(MakeAnyValue<TUnversionedValue>(
                    NYson::ConvertToYsonString(keyPart).AsStringBuf(),
                    id));
                break;
        }

        rows.push_back(buffer->CaptureRow(keyBuilder.GetRow()));
    }

    return MakeSharedRange(std::move(rows), buffer);
}

////////////////////////////////////////////////////////////////////////////////

TConstExpressionPtr MakeReferenceExpression(const std::string& columnName)
{
    return New<TReferenceExpression>(
        SimpleLogicalType(ESimpleLogicalValueType::Null),
        columnName);
}

TConstExpressionPtr MakeLiteralExpression(TUnversionedValue value)
{
    return New<TLiteralExpression>(EValueType::Null, value);
}

TConstExpressionPtr MakeUnaryExpression(EUnaryOp opcode, TConstExpressionPtr operand)
{
    return New<TUnaryOpExpression>(EValueType::Null, opcode, operand);
}

TConstExpressionPtr MakeBinaryExpression(EBinaryOp opcode, TConstExpressionPtr lhs, TConstExpressionPtr rhs)
{
    return New<TBinaryOpExpression>(EValueType::Null, opcode, lhs, rhs);
}

TConstExpressionPtr MakeFunctionExpression(const std::string& functionName, const std::vector<TConstExpressionPtr>& arguments)
{
    return New<TFunctionExpression>(EValueType::Null, functionName, arguments);
}

////////////////////////////////////////////////////////////////////////////////

bool Equal(TConstExpressionPtr lhs, TConstExpressionPtr rhs)
{
    if (auto literalLhs = lhs->As<TLiteralExpression>()) {
        auto literalRhs = rhs->As<TLiteralExpression>();
        if (literalRhs == nullptr || literalLhs->Value != literalRhs->Value) {
            return false;
        }
    } else if (auto referenceLhs = lhs->As<TReferenceExpression>()) {
        auto referenceRhs = rhs->As<TReferenceExpression>();
        if (referenceRhs == nullptr
            || referenceLhs->ColumnName != referenceRhs->ColumnName) {
            return false;
        }
    } else if (auto functionLhs = lhs->As<TFunctionExpression>()) {
        auto functionRhs = rhs->As<TFunctionExpression>();
        if (functionRhs == nullptr
            || functionLhs->FunctionName != functionRhs->FunctionName
            || functionLhs->Arguments.size() != functionRhs->Arguments.size()) {
            return false;
        }
        for (int index = 0; index < std::ssize(functionLhs->Arguments); ++index) {
            if (!Equal(functionLhs->Arguments[index], functionRhs->Arguments[index])) {
                return false;
            }
        }
    } else if (auto unaryLhs = lhs->As<TUnaryOpExpression>()) {
        auto unaryRhs = rhs->As<TUnaryOpExpression>();
        if (unaryRhs == nullptr
            || unaryLhs->Opcode != unaryRhs->Opcode
            || !Equal(unaryLhs->Operand, unaryRhs->Operand)) {
            return false;
        }
    } else if (auto binaryLhs = lhs->As<TBinaryOpExpression>()) {
        auto binaryRhs = rhs->As<TBinaryOpExpression>();
        if (binaryRhs == nullptr
            || binaryLhs->Opcode != binaryRhs->Opcode
            || !Equal(binaryLhs->Lhs, binaryRhs->Lhs)
            || !Equal(binaryLhs->Rhs, binaryRhs->Rhs)) {
            return false;
        }
    } else if (auto inLhs = lhs->As<TInExpression>()) {
        auto inRhs = rhs->As<TInExpression>();
        if (inRhs == nullptr
            || inLhs->Values.Size() != inRhs->Values.Size()
            || inLhs->Arguments.size() != inRhs->Arguments.size()) {
            return false;
        }
        for (int index = 0; index < std::ssize(inLhs->Values); ++index) {
            if (inLhs->Values[index] != inRhs->Values[index]) {
                return false;
            }
        }
        for (int index = 0; index < std::ssize(inLhs->Arguments); ++index) {
            if (!Equal(inLhs->Arguments[index], inRhs->Arguments[index])) {
                return false;
            }
        }
    } else {
        YT_ABORT();
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

class TQueryTreeConverterTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<TConstExpressionPtr, const char*>>
{
protected:
    void SetUp() override
    { }

    DB::ContextPtr CreateQueryContext() const
    {
        auto context = DB::Context::createCopy(GetGlobalContext());
        context->makeQueryContext();
        return context;
    }

    TTableSchemaPtr GetSampleTableSchema() const
    {
        return New<TTableSchema>(std::vector{
            TColumnSchema("key", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("key_i64", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("key_ui64", EValueType::Uint64).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("key_dbl", EValueType::Double).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("key_str", EValueType::String).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("key_ts", ESimpleLogicalValueType::Timestamp).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("key_ts64", ESimpleLogicalValueType::Timestamp64).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("value1", EValueType::Int64),
            TColumnSchema("value2", EValueType::Int64)});
    }
};

TEST_P(TQueryTreeConverterTest, Simple)
{
    auto schema = GetSampleTableSchema();

    auto context = CreateQueryContext();

    auto compositeSettings = TCompositeSettings::Create(true);
    auto columns = DB::ColumnsDescription(ToNamesAndTypesList(*schema, /*columnAttributes*/ {}, compositeSettings));
    auto storage = std::make_shared<DB::StorageDummy>(DB::StorageID{"YT", "test"}, columns);

    auto tableNode = std::make_shared<DB::TableNode>(storage, context);

    auto& param = GetParam();

    auto expected = std::get<0>(param);

    DB::ParserExpressionWithOptionalAlias parser(false);
    auto predicateAst = DB::parseQuery(parser, std::get<1>(param), 0 /*maxQuerySize*/, 0 /*maxQueryDepth*/, 0 /*max_parser_backtracks*/);
    auto queryTree = DB::buildQueryTree(predicateAst, context);

    DB::QueryAnalysisPass queryAnalysisPass(tableNode);
    queryAnalysisPass.run(queryTree, context);

    auto expr = ConvertToConstExpression(schema, std::move(queryTree));
    ASSERT_TRUE(expr != nullptr);

    EXPECT_TRUE(Equal(expr, expected))
        << "got: " << InferName(expr) << std::endl
        << "expected: " <<  InferName(expected) << std::endl;
}

// TODO (buyval01) : Add test with CH implicit cast of uint8 values to bool

INSTANTIATE_TEST_SUITE_P(
    CheckValueTypeMatch,
    TQueryTreeConverterTest,
    ::testing::Values(
        std::tuple<TConstExpressionPtr, const char*>(
            MakeBinaryExpression(EBinaryOp::Equal,
                MakeReferenceExpression("key_i64"),
                MakeLiteralExpression(MakeUnversionedInt64Value(90))),
            "key_i64 = 90"),
        std::tuple<TConstExpressionPtr, const char*>(
            MakeBinaryExpression(EBinaryOp::Equal,
                MakeReferenceExpression("key_ui64"),
                MakeLiteralExpression(MakeUnversionedUint64Value(90))),
            "key_ui64 = 90"),
        std::tuple<TConstExpressionPtr, const char*>(
            MakeBinaryExpression(EBinaryOp::Equal,
                MakeReferenceExpression("key_dbl"),
                MakeLiteralExpression(MakeUnversionedDoubleValue(90))),
            "key_dbl = 90"),
        std::tuple<TConstExpressionPtr, const char*>(
            MakeBinaryExpression(EBinaryOp::Equal,
                MakeReferenceExpression("key_str"),
                MakeLiteralExpression(MakeUnversionedStringValue("90"))),
            "key_str = \'90\'"),
        std::tuple<TConstExpressionPtr, const char*>(
            MakeBinaryExpression(EBinaryOp::Equal,
                MakeReferenceExpression("key_ts"),
                MakeLiteralExpression(MakeUnversionedUint64Value(90000000))),
            "key_ts = 90"),
        std::tuple<TConstExpressionPtr, const char*>(
            MakeBinaryExpression(EBinaryOp::Equal,
                MakeReferenceExpression("key_ts64"),
                MakeLiteralExpression(MakeUnversionedInt64Value(90000000))),
            "key_ts64 = 90")
    )
);

INSTANTIATE_TEST_SUITE_P(
    CheckExpressions,
    TQueryTreeConverterTest,
    ::testing::Values(
        std::tuple<TConstExpressionPtr, const char*>(
            MakeBinaryExpression(EBinaryOp::GreaterOrEqual,
                MakeReferenceExpression("key"),
                MakeLiteralExpression(MakeUnversionedInt64Value(90))),
            "key >= 90"),
        std::tuple<TConstExpressionPtr, const char*>(
            MakeBinaryExpression(EBinaryOp::Greater,
                MakeReferenceExpression("key"),
                MakeLiteralExpression(MakeUnversionedInt64Value(-90))),
            "key > -90"),
        std::tuple<TConstExpressionPtr, const char*>(
            MakeBinaryExpression(EBinaryOp::Less,
                MakeReferenceExpression("key"),
                MakeLiteralExpression(MakeUnversionedInt64Value(0))),
            "key < 0"),
        std::tuple<TConstExpressionPtr, const char*>(
            MakeUnaryExpression(EUnaryOp::Not,
                MakeBinaryExpression(EBinaryOp::Equal,
                    MakeReferenceExpression("key"),
                    MakeLiteralExpression(MakeUnversionedInt64Value(2)))),
            "not key = 2"),
        std::tuple<TConstExpressionPtr, const char*>(
            MakeBinaryExpression(EBinaryOp::Or,
                MakeBinaryExpression(EBinaryOp::Less,
                    MakeReferenceExpression("key"),
                    MakeLiteralExpression(MakeUnversionedInt64Value(3))),
                MakeBinaryExpression(EBinaryOp::GreaterOrEqual,
                    MakeReferenceExpression("key"),
                    MakeLiteralExpression(MakeUnversionedInt64Value(2)))),
            "(key < 3) or (key >= 2)"),
        std::tuple<TConstExpressionPtr, const char*>(
            MakeUnaryExpression(EUnaryOp::Not,
                MakeBinaryExpression(EBinaryOp::And,
                    MakeBinaryExpression(EBinaryOp::Less,
                        MakeReferenceExpression("key"),
                        MakeLiteralExpression(MakeUnversionedInt64Value(3))),
                    MakeBinaryExpression(EBinaryOp::GreaterOrEqual,
                        MakeReferenceExpression("key"),
                        MakeLiteralExpression(MakeUnversionedInt64Value(2))))),
            "not ((key < 3) and (key >= 2))"),
        std::tuple<TConstExpressionPtr, const char*>(
            New<TInExpression>(
                std::initializer_list<TConstExpressionPtr>({
                    MakeReferenceExpression("key")}),
                MakeRows("1; 2; 3")),
            "key in (1, 2, 3)"),
        std::tuple<TConstExpressionPtr, const char*>(
            New<TInExpression>(
                std::initializer_list<TConstExpressionPtr>({
                    MakeBinaryExpression(EBinaryOp::NotEqual,
                        MakeReferenceExpression("key"),
                        MakeLiteralExpression(MakeUnversionedInt64Value(0)))}),
                MakeRows("\%false; \%true")),
            "(key != 0) in (0, 1)"),
        std::tuple<TConstExpressionPtr, const char*>(
            MakeBinaryExpression(EBinaryOp::And,
                MakeBinaryExpression(EBinaryOp::GreaterOrEqual,
                    MakeReferenceExpression("key"),
                    MakeLiteralExpression( MakeUnversionedInt64Value(-3))),
                MakeBinaryExpression(EBinaryOp::LessOrEqual,
                    MakeReferenceExpression("key"),
                    MakeLiteralExpression(MakeUnversionedInt64Value(3) ))),
            "key between -3 and 3"),
        std::tuple<TConstExpressionPtr, const char*>(
            MakeFunctionExpression("is_null",
                std::initializer_list<TConstExpressionPtr>({
                        MakeReferenceExpression("key")})),
            "key is null"),
        std::tuple<TConstExpressionPtr, const char*>(
            MakeUnaryExpression(EUnaryOp::Not,
                MakeFunctionExpression("is_null",
                    std::initializer_list<TConstExpressionPtr>({
                            MakeReferenceExpression("key")}))),
            "key is not null")
));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NClickHouseServer
