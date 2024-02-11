#include <yt/yt/core/test_framework/framework.h>

#include "yt/yt/client/unittests/mock/table_value_consumer.h"

#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/client/table_client/value_consumer.h>
#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTableClient {
namespace {

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

    auto schema = New<TTableSchema>(std::vector{
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
        true /*allowUnknownColumns*/,
        schema,
        typeConversionConfig);

    // For convenience, we will specify values in YSON.
    auto rowBuffer = New<TRowBuffer>();
    auto makeValue = [=] (TStringBuf yson, int id) {
        return TryDecodeUnversionedAnyValue(MakeUnversionedAnyValue(yson, id), rowBuffer);
    };

    // Helper functions for testing the behaviour of a value consumer type conversion.
    // Both |source| and |result| are yson-encoded unversioned values.
    // ExpectConversion(column, source, condition, result) <=>
    //     <=> check that if |condition| is held, |source| transforms to
    //         |result| in column |column|, otherwise |source| remains as is.
    auto expectConversion = [&] (TString column, TString source, bool condition, TString result = "") {
        int id = mock.GetNameTable()->GetId(column);
        if (condition) {
            EXPECT_CALL(mock, OnMyValue(SameAs(makeValue(result, id))));
        } else {
            EXPECT_CALL(mock, OnMyValue(SameAs(makeValue(source, id))));
        }
        mock.OnValue(makeValue(source, id));
    };

    auto expectError = [&] (TString column, TString source, bool condition) {
        int id = mock.GetNameTable()->GetId(column);
        if (condition) {
            EXPECT_THROW(mock.OnValue(makeValue(source, id)), std::exception);
        } else {
            EXPECT_CALL(mock, OnMyValue(SameAs(makeValue(source, id))));
            mock.OnValue(makeValue(source, id));
        }
    };

    constexpr bool Never = false;

    // Arbitrary type can be used in Any column without conversion under any circumstances.
    for (TString value : {"42", "18u", "abc", "%true", "3.14", "#", "{}"}) {
        expectConversion("any", value, Never);
    }

    expectConversion("int64", "42u", typeConversionConfig->EnableIntegralTypeConversion, "42");
    expectError("int64", "9223372036854775808u", typeConversionConfig->EnableIntegralTypeConversion); // 2^63 leads to an integer overflow.
    expectConversion("int64", "\"-42\"", typeConversionConfig->EnableStringToAllConversion, "-42");
    expectError("int64", "abc", typeConversionConfig->EnableStringToAllConversion);
    for (TString value : {"42", "%true", "3.14", "#", "{}"}) {
        expectConversion("int64", value, Never);
    }

    expectConversion("uint64", "42", typeConversionConfig->EnableIntegralTypeConversion, "42u");
    expectError("uint64", "-42", typeConversionConfig->EnableIntegralTypeConversion);
    expectConversion("uint64", "\"234\"", typeConversionConfig->EnableStringToAllConversion, "234u");
    expectConversion("uint64", "\"234u\"", typeConversionConfig->EnableStringToAllConversion, "234u");
    expectError("uint64", "abc", typeConversionConfig->EnableStringToAllConversion);
    for (TString value : {"42u", "%true", "3.14", "#", "{}"}) {
        expectConversion("uint64", value, Never);
    }

    expectConversion("string", "42u", typeConversionConfig->EnableAllToStringConversion, "\"42\"");
    expectConversion("string", "42", typeConversionConfig->EnableAllToStringConversion, "\"42\"");
    expectConversion("string", "3.14", typeConversionConfig->EnableAllToStringConversion, "\"3.14\"");
    expectConversion("string", "%true", typeConversionConfig->EnableAllToStringConversion, "\"true\"");
    expectConversion("string", "%false", typeConversionConfig->EnableAllToStringConversion, "\"false\"");
    for (TString value : {"abc", "#", "{}"}) {
        expectConversion("string", value, Never);
    }

    expectConversion("boolean", "true", typeConversionConfig->EnableStringToAllConversion, "%true");
    expectConversion("boolean", "false", typeConversionConfig->EnableStringToAllConversion, "%false");
    expectError("boolean", "abc", typeConversionConfig->EnableStringToAllConversion);
    for (TString value : {"42", "18u", "%true", "3.14", "#", "{}"}) {
        expectConversion("boolean", value, Never);
    }

    expectConversion("double", "-42", typeConversionConfig->EnableIntegralToDoubleConversion, "-42.0");
    expectConversion("double", "42u", typeConversionConfig->EnableIntegralToDoubleConversion, "42.0");
    expectConversion("double", "\"1.23\"", typeConversionConfig->EnableStringToAllConversion, "1.23");
    expectError("double", "abc", typeConversionConfig->EnableStringToAllConversion);
    for (TString value : {"3.14", "%true", "#", "{}"}) {
        expectConversion("double", value, Never);
    }
}

INSTANTIATE_TEST_SUITE_P(AllCombinations,
    TValueConsumerTypeConversionTest,
    ::testing::Combine(::testing::Bool(), ::testing::Bool(), ::testing::Bool(), ::testing::Bool()));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NVersionedTableClient

