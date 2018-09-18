#include <yt/core/test_framework/framework.h>

#include "table_value_consumer_mock.h"
#include "ql_helpers.h"

#include <yt/ytlib/table_client/helpers.h>
#include <yt/client/table_client/value_consumer.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NTableClient {

using ::testing::StrictMock;

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

// To make sure different options work together as intended, we will
// test all possible combinations of them.
// Order of options: <EnableStringToAllConversion,
//                    EnableAllToStringConversion,
//                    EnableIntegralTypeConversion,
//                    EnableIntegralToDoubleConversion>
class TValueConsumerTypeConversionTest
    : public ::testing::TestWithParam<std::tuple<bool, bool, bool, bool>>
{ };

// A matcher that compares unversioned values. We can't use an operator == for them
// because it does not enable comparing two values that have type Any.
MATCHER_P(SameAs, expectedUnversionedValue, "is same as " + ToString(expectedUnversionedValue))
{
    // This is a dirty hack, though it should work.
    return ToString(arg) == ToString(expectedUnversionedValue);
}

TEST_P(TValueConsumerTypeConversionTest, TestBehaviour)
{
    auto typeConversionConfig = New<TTypeConversionConfig>();
    std::tie(
        typeConversionConfig->EnableStringToAllConversion,
        typeConversionConfig->EnableAllToStringConversion,
        typeConversionConfig->EnableIntegralTypeConversion,
        typeConversionConfig->EnableIntegralToDoubleConversion) = GetParam();

    TTableSchema schema({
        TColumnSchema("any", EValueType::Any),
        TColumnSchema("int64", EValueType::Int64),
        TColumnSchema("uint64", EValueType::Uint64),
        TColumnSchema("string", EValueType::String),
        TColumnSchema("boolean", EValueType::Boolean),
        TColumnSchema("double", EValueType::Double),
    });

    auto nameTable = New<TNameTable>();
    nameTable->GetIdOrRegisterName("extra"); // A column that is missing in the table schema.

    StrictMock<TMockValueConsumer> mock(
        New<TNameTable>(),
        true /* allowUnknownColumns */,
        schema,
        typeConversionConfig);

    // For convenience, we will specify values in yson. To use MakeUnversionedValue, we need a stateless lexer.
    TStatelessLexer lexer;

    // Helper functions for testing the behaviour of a value consumer type conversion.
    // Both |source| and |result| are yson-encoded unversioned values.
    // ExpectConversion(column, source, condition, result) <=>
    //     <=> check that if |condition| is held, |source| transforms to
    //         |result| in column |column|, otherwise |source| remains as is.
    auto ExpectConversion = [&mock, &lexer] (TString column, TString source, bool condition, TString result = "") {
        int id = mock.GetNameTable()->GetId(column);
        if (condition) {
            EXPECT_CALL(mock, OnMyValue(SameAs(MakeUnversionedValue(result, id, lexer))));
        } else {
            EXPECT_CALL(mock, OnMyValue(SameAs(MakeUnversionedValue(source, id, lexer))));
        }
        mock.OnValue(MakeUnversionedValue(source, id, lexer));
    };

    auto ExpectError = [&mock, &lexer] (TString column, TString source, bool condition) {
        int id = mock.GetNameTable()->GetId(column);
        if (condition) {
            EXPECT_THROW(mock.OnValue(MakeUnversionedValue(source, id, lexer)), std::exception);
        } else {
            EXPECT_CALL(mock, OnMyValue(SameAs(MakeUnversionedValue(source, id, lexer))));
            mock.OnValue(MakeUnversionedValue(source, id, lexer));
        }
    };

    static const bool Never = false;

    // Arbitrary type can be used in Any column without conversion under any circumstances.
    for (TString value : {"42", "18u", "abc", "%true", "3.14", "#", "{}"}) {
        ExpectConversion("any", value, Never);
    }

    ExpectConversion("int64", "42u", typeConversionConfig->EnableIntegralTypeConversion, "42");
    ExpectError("int64", "9223372036854775808u", typeConversionConfig->EnableIntegralTypeConversion); // 2^63 leads to an integer overflow.
    ExpectConversion("int64", "\"-42\"", typeConversionConfig->EnableStringToAllConversion, "-42");
    ExpectError("int64", "abc", typeConversionConfig->EnableStringToAllConversion);
    for (TString value : {"42", "%true", "3.14", "#", "{}"}) {
        ExpectConversion("int64", value, Never);
    }

    ExpectConversion("uint64", "42", typeConversionConfig->EnableIntegralTypeConversion, "42u");
    ExpectError("uint64", "-42", typeConversionConfig->EnableIntegralTypeConversion);
    ExpectConversion("uint64", "\"234\"", typeConversionConfig->EnableStringToAllConversion, "234u");
    ExpectError("uint64", "abc", typeConversionConfig->EnableStringToAllConversion);
    for (TString value : {"42u", "%true", "3.14", "#", "{}"}) {
        ExpectConversion("uint64", value, Never);
    }

    ExpectConversion("string", "42u", typeConversionConfig->EnableAllToStringConversion, "\"42\"");
    ExpectConversion("string", "42", typeConversionConfig->EnableAllToStringConversion, "\"42\"");
    ExpectConversion("string", "3.14", typeConversionConfig->EnableAllToStringConversion, "\"3.14\"");
    ExpectConversion("string", "%true", typeConversionConfig->EnableAllToStringConversion, "\"true\"");
    ExpectConversion("string", "%false", typeConversionConfig->EnableAllToStringConversion, "\"false\"");
    for (TString value : {"abc", "#", "{}"}) {
        ExpectConversion("string", value, Never);
    }

    ExpectConversion("boolean", "true", typeConversionConfig->EnableStringToAllConversion, "%true");
    ExpectConversion("boolean", "false", typeConversionConfig->EnableStringToAllConversion, "%false");
    ExpectError("boolean", "abc", typeConversionConfig->EnableStringToAllConversion);
    for (TString value : {"42", "18u", "%true", "3.14", "#", "{}"}) {
        ExpectConversion("boolean", value, Never);
    }

    ExpectConversion("double", "-42", typeConversionConfig->EnableIntegralToDoubleConversion, "-42.0");
    ExpectConversion("double", "42u", typeConversionConfig->EnableIntegralToDoubleConversion, "42.0");
    ExpectConversion("double", "\"1.23\"", typeConversionConfig->EnableStringToAllConversion, "1.23");
    ExpectError("double", "abc", typeConversionConfig->EnableStringToAllConversion);
    for (TString value : {"3.14", "%true", "#", "{}"}) {
        ExpectConversion("double", value, Never);
    }
}

INSTANTIATE_TEST_CASE_P(AllCombinations,
    TValueConsumerTypeConversionTest,
    ::testing::Combine(::testing::Bool(), ::testing::Bool(), ::testing::Bool(), ::testing::Bool()));

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT

