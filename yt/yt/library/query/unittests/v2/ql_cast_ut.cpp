#include <yt/yt/library/query/unittests/ql_helpers.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/query_client/query_statistics.h>

#include <yt/yt/library/query/base/query_helpers.h>
#include <yt/yt/library/query/base/query_preparer.h>
#include <yt/yt/library/query/base/functions.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>
#include <yt/yt/library/query/engine_api/config.h>
#include <yt/yt/library/query/engine_api/coordinator.h>

#include <yt/yt/library/query/engine/builtin_function_profiler.h>
#include <yt/yt/library/query/engine/folding_profiler.h>
#include <yt/yt/library/query/engine/functions_cg.h>

#include <yt/yt/core/test_framework/fixed_growth_string_output.h>

#include <library/cpp/resource/resource.h>

// Tests:
// TCompareExpressionTest
// TEliminateLookupPredicateTest
// TEliminatePredicateTest
// TParseAndPrepareExpressionTest
// TArithmeticTest
// TCompareWithNullTest
// TEvaluateExpressionTest
// TEvaluateAggregationTest
// TEvaluateAggregationWithStringStateTest
// TExpressionStrConvTest

namespace NYT::NQueryClient {
namespace {

using namespace NTableClient;

using NCodegen::EExecutionBackend;

////////////////////////////////////////////////////////////////////////////////

void EvaluateExpression(
    TConstExpressionPtr expr,
    TStringBuf rowString,
    const TTableSchemaPtr& schema,
    TUnversionedValue* result,
    TRowBufferPtr buffer,
    bool enableWebAssembly = true)
{
    TCGVariables variables;

    auto image = Profile(
        expr,
        schema,
        /*id*/ nullptr,
        &variables,
        /*useCanonicalNullRelations*/ false,
        EnableWebAssemblyInUnitTests() && enableWebAssembly ? EExecutionBackend::WebAssembly : EExecutionBackend::Native)();
    auto instance = image.Instantiate();

    auto row = YsonToSchemafulRow(rowString, *schema, true);

    instance.Run(
        variables.GetLiteralValues(),
        variables.GetOpaqueData(),
        variables.GetOpaqueDataSizes(),
        result,
        row.Elements(),
        buffer);

    buffer->CaptureValue(result);
}

class TCastExpressionTest
    : public ::testing::Test
{
protected:
    TTableSchemaPtr Schema_;
    TRowBufferPtr Buffer_;

    void SetUp() override
    {
        Schema_ = New<TTableSchema>(std::vector{
            TColumnSchema("i1", EValueType::Int64),
            TColumnSchema("i2", SimpleLogicalType(ESimpleLogicalValueType::Int32)),
            TColumnSchema("u1", EValueType::Uint64),
            TColumnSchema("u2", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Uint16))),
            TColumnSchema("d1", EValueType::Double),
            TColumnSchema("s1", EValueType::String),
            TColumnSchema("any", EValueType::Any),
            TColumnSchema("b", EValueType::Boolean),
            TColumnSchema("l", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int32))),
        });

        Buffer_ = New<TRowBuffer>();
    }

    void Check(TStringBuf exprString, TStringBuf rowString, TUnversionedValue expected)
    {
        auto expr = ParseAndPrepareExpression(exprString, *Schema_);

        TUnversionedValue result{};

        EvaluateExpression(expr, rowString, Schema_, &result, Buffer_, /*enableWebAssembly*/ false);
        EXPECT_EQ(result, expected);

        result = {};

        EvaluateExpression(expr, rowString, Schema_, &result, Buffer_, /*enableWebAssembly*/ true);
        EXPECT_EQ(result, expected);
    }

    void CheckThrows(TStringBuf exprString, TStringBuf rowString, const char* errorSubstring)
    {
        EXPECT_THROW_WITH_SUBSTRING(Check(exprString, rowString, MakeNull()), errorSubstring);
    }
};

TEST_F(TCastExpressionTest, Basic)
{
    Check("CAST(1 AS Int64)", "", MakeInt64(1));
    Check("CAST(1 AS `Int64?`)", "", MakeInt64(1));
    Check("CAST(i1 AS `Int64?`)", "", MakeNull());
    Check("CAST(i1 AS `Int64?`)", "i1=1", MakeInt64(1));
    Check("CAST(i2 AS `Int64?`)", "i2=1", MakeInt64(1));
    Check("CAST(# AS `Int64?`)", "", MakeNull());

    Check("CAST(1 AS Uint64)", "", MakeUint64(1));
    Check("CAST(1 AS `Uint64?`)", "", MakeUint64(1));
    Check("CAST(i1 AS `Uint64?`)", "i1=1", MakeUint64(1));
    Check("CAST(i2 AS `Uint64?`)", "i2=1", MakeUint64(1));
    Check("CAST(u2 AS `Uint64?`)", "u2=1u", MakeUint64(1));
    Check("CAST(1u AS `Int64`)", "", MakeInt64(1));

    Check("CAST(s1 AS String)", "s1=\"alpha\"", MakeString("alpha"));

    Check("CAST(1 AS String)", "", MakeString("1"));
    Check("CAST(\"1\" AS `Optional<Double>`)", "", MakeDouble(1.0));
}

TEST_F(TCastExpressionTest, AnyToBasic)
{
    Check("CAST(yson_string_to_any(\"123\") AS Int64)", "", MakeInt64(123));
    Check("CAST(yson_string_to_any(\"123.0\") AS Double)", "", MakeDouble(123.0));
    Check("CAST(yson_string_to_any(\"123.0\") AS Int64)", "", MakeInt64(123));
    Check("CAST(yson_string_to_any(\"abacaba\") AS `String?`)", "", MakeString("abacaba"));
    Check("CAST(yson_string_to_any(\"%true\") AS `Bool`)", "", MakeBoolean(true));
}

TEST_F(TCastExpressionTest, ToComposite)
{
    Check("CAST(yson_string_to_any(\"[1;2;3]\") AS `List<Int32>`)", "", MakeComposite("[1;2;3;]"));
    Check("CAST(yson_string_to_any(\"[1;2;3]\") AS `Optional<List<Int32>>`)", "", MakeComposite("[1;2;3;]"));
    Check("CAST(yson_string_to_any(\"[1;2;3]\") AS `List<Optional<Int32>>`)", "", MakeComposite("[1;2;3;]"));
    Check("CAST(yson_string_to_any(\"#\") AS `Optional<Struct<a:String, b: Null>>`)", "", MakeComposite("#"));
    Check("CAST(yson_string_to_any(\"[alyx;#;13.2]\") AS `Struct<a:String, b: Int32?, c: Double?>`)", "",
        MakeComposite("[alyx;#;13.2]"));
}

TEST_F(TCastExpressionTest, Malformed)
{
    CheckThrows("CAST(i1 AS Int64)", "i1=#", "Encountered a null value during cast to a non-nullable type");
    CheckThrows("CAST(u1 AS `Uint64`)", "", "Encountered a null value during cast to a non-nullable type");
    CheckThrows("CAST(\"1\" AS `List<Float>`)", "", "is not supported");
    CheckThrows("CAST(any AS Int64)", "any={a=b;c=2;}", "Cannot convert value");
    CheckThrows("CAST(l AS String)", "l=[1;2;3;]", "Cannot convert value");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
