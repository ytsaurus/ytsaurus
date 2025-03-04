#include <yt/yt/core/test_framework/framework.h>

#include <yt/chyt/server/ch_to_yt_converter.h>
#include <yt/chyt/server/custom_data_types.h>
#include <yt/chyt/server/config.h>

#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/library/decimal/decimal.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/IDataType.h>

#include <library/cpp/iterator/functools.h>

#include <cmath>

namespace NYT::NClickHouseServer {
namespace {

using namespace NDecimal;
using namespace NLogging;
using namespace NTableChunkFormat;
using namespace NTableClient;
using namespace NYson;
using namespace NYTree;

TLogger Logger("Test");

////////////////////////////////////////////////////////////////////////////////

DB::ColumnPtr MakeColumn(DB::DataTypePtr dataType, std::vector<DB::Field> fields)
{
    auto column = dataType->createColumn();
    for (auto& field : fields) {
        column->insert(std::move(field));
    }
    return column;
}

std::vector<TStringBuf> ToStringBufs(std::vector<TString>& ysonStrings)
{
    std::vector<TStringBuf> result;
    result.reserve(ysonStrings.size());
    for (const auto& ysonString : ysonStrings) {
        result.emplace_back(ysonString);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TCHToYTConversionTest
    : public ::testing::Test
{
public:
    void SetUp() override
    {
        Settings_ = New<TCompositeSettings>();
    }

    std::vector<TUnversionedValue> ExpectConversion(
        DB::ColumnPtr column,
        TLogicalTypePtr expectedLogicalType,
        std::vector<TStringBuf> expectedValueYsons)
    {
        EXPECT_EQ(*expectedLogicalType, *Converter_->GetLogicalType());
        std::vector<TUnversionedValue> expectedValues;
        for (const auto& yson : expectedValueYsons) {
            expectedValues.push_back(MakeUnversionedValue(yson));
        }
        auto actualValueRange = Converter_->ConvertColumnToUnversionedValues(column);
        std::vector<TUnversionedValue> actualValues(actualValueRange.begin(), actualValueRange.end());
        EXPECT_THAT(actualValues, ::testing::ElementsAreArray(expectedValues));
        return actualValues;
    }

    void ExpectYsonConversion(
        DB::ColumnPtr column,
        TLogicalTypePtr expectedLogicalType,
        std::vector<TStringBuf> expectedValueYsons)
    {
        EXPECT_EQ(*expectedLogicalType, *Converter_->GetLogicalType());
        std::vector<INodePtr> expectedNodes;
        for (const auto& yson : expectedValueYsons) {
            expectedNodes.emplace_back(ConvertToNode(TYsonStringBuf(TStringBuf(yson.data(), yson.size()))));
        }
        auto actualValueRange = Converter_->ConvertColumnToUnversionedValues(column);
        std::vector<INodePtr> actualNodes;
        for (const auto& actualValue : actualValueRange) {
            ASSERT_EQ(EValueType::Composite, actualValue.Type);
            actualNodes.emplace_back(ConvertToNode(TYsonStringBuf(actualValue.AsStringBuf())));
        }
        ASSERT_EQ(expectedNodes.size(), actualNodes.size());
        int index = 0;
        for (const auto& [expectedNode, actualNode] : Zip(expectedNodes, actualNodes)) {
            EXPECT_TRUE(AreNodesEqual(expectedNode, actualNode))
                << "Yson strings define different nodes at index " << index << ":" << std::endl
                << "  expected: " << ConvertToYsonString(expectedNode, EYsonFormat::Text).AsStringBuf() << std::endl
                << "  actual: " << ConvertToYsonString(actualNode, EYsonFormat::Text).AsStringBuf() << std::endl;
            index++;
        }
    }

protected:
    TCompositeSettingsPtr Settings_;
    std::optional<TCHToYTConverter> Converter_;

private:
    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>();

    TUnversionedValue MakeUnversionedValue(TStringBuf yson)
    {
        return TryDecodeUnversionedAnyValue(MakeUnversionedAnyValue(yson), RowBuffer_);
    }
};

TEST_F(TCHToYTConversionTest, Int16)
{
    auto dataType = std::make_shared<DB::DataTypeInt16>();

    auto column = MakeColumn(dataType, {
        DB::Int16(42),
        DB::Int16(-17),
        std::numeric_limits<DB::Int16>::max(),
        std::numeric_limits<DB::Int16>::min()
    });

    std::vector<TStringBuf> expectedValueYsons = {
        "42",
        "-17",
        "32767",
        "-32768",
    };

    Converter_.emplace(dataType, Settings_);

    ExpectConversion(column, SimpleLogicalType(ESimpleLogicalValueType::Int16), expectedValueYsons);
}

TEST_F(TCHToYTConversionTest, Boolean)
{
    auto dataType = GetDataTypeBoolean();

    auto validColumn = MakeColumn(dataType, {
        DB::UInt8(0),
        DB::UInt8(1),
    });

    auto invalidColumn = MakeColumn(dataType, {
        DB::UInt8(2),
    });

    std::vector<TStringBuf> expectedValueYsons = {
        "%false",
        "%true",
    };

    Converter_.emplace(dataType, Settings_);

    ExpectConversion(validColumn, SimpleLogicalType(ESimpleLogicalValueType::Boolean), expectedValueYsons);
    EXPECT_THROW(Converter_->ConvertColumnToUnversionedValues(invalidColumn), std::exception);
}

TEST_F(TCHToYTConversionTest, Float32)
{
    auto dataType = std::make_shared<DB::DataTypeFloat32>();

    auto column = MakeColumn(dataType, {
        DB::Float32(1.25),
        DB::Float32(-32),
    });

    std::vector<TStringBuf> expectedValueYsons = {
        "1.25",
        "-32.0",
    };

    Converter_.emplace(dataType, Settings_);

    ExpectConversion(column, SimpleLogicalType(ESimpleLogicalValueType::Float), expectedValueYsons);
}

TEST_F(TCHToYTConversionTest, String)
{
    auto dataType = std::make_shared<DB::DataTypeString>();

    auto column = MakeColumn(dataType, {
        DB::String("YT"),
        DB::String("rules"),
    });

    std::vector<TStringBuf> expectedValueYsons = {
        "\"YT\"",
        "\"rules\"",
    };

    Converter_.emplace(dataType, Settings_);

    auto actualValues = ExpectConversion(column, SimpleLogicalType(ESimpleLogicalValueType::String), expectedValueYsons);
    EXPECT_EQ(actualValues[0].Data.String, column->getDataAt(0).data);
    EXPECT_EQ(actualValues[1].Data.String, column->getDataAt(1).data);
}

TEST_F(TCHToYTConversionTest, Interval)
{
    auto dataType = std::make_shared<DB::DataTypeInterval>(DB::IntervalKind::Kind::Microsecond);

    auto column = MakeColumn(dataType, {
        DB::Int64(42),
        DB::Int64(-17),
        DB::Int64(123456789),
        DB::Int64(-987654321),
    });

    std::vector<TStringBuf> expectedValueYsons = {
        "42",
        "-17",
        "123456789",
        "-987654321",
    };

    Converter_.emplace(dataType, Settings_);

    ExpectConversion(column, SimpleLogicalType(ESimpleLogicalValueType::Interval64), expectedValueYsons);
}

TEST_F(TCHToYTConversionTest, Date)
{
    auto dataTypeDate = std::make_shared<DB::DataTypeDate>();
    auto columnDate = MakeColumn(dataTypeDate, {
        DB::UInt16(0),
        DB::UInt16(42),
        DB::UInt16(100),
        DB::UInt16(12345),
    });

    std::vector<TStringBuf> expectedValuesDate = {
        "0u",
        "42u",
        "100u",
        "12345u",
    };

    Converter_.emplace(dataTypeDate, Settings_);
    ExpectConversion(columnDate, SimpleLogicalType(ESimpleLogicalValueType::Date), expectedValuesDate);

    auto dataTypeDate32 = std::make_shared<DB::DataTypeDate32>();
    auto columnDate32 = MakeColumn(dataTypeDate32, {
        DB::Int32(0),
        DB::Int32(42),
        DB::Int32(100),
        DB::Int32(-12345),
        DB::Int32(12345),
    });
    std::vector<TStringBuf> expectedValuesDate32 = {
        "0",
        "42",
        "100",
        "-12345",
        "12345",
    };

    Converter_.emplace(dataTypeDate32, Settings_);
    ExpectConversion(columnDate32, SimpleLogicalType(ESimpleLogicalValueType::Date32), expectedValuesDate32);
}

TEST_F(TCHToYTConversionTest, DateTime64)
{
    std::vector<ui64> expectedUnsignedValues = {
        0,
        42,
        100,
        12345,
        123456,
    };
    std::vector<i64> expectedSignedValues = {
        -42,
        -100,
        -12345,
        -123456,
    };
    expectedSignedValues.insert(
        expectedSignedValues.end(),
        expectedUnsignedValues.begin(),
        expectedUnsignedValues.end()
    );

    std::vector<TString> ysonUnsignedStrings;
    for (const auto& value : expectedUnsignedValues) {
        ysonUnsignedStrings.emplace_back(ConvertToYsonString(value).ToString());
    }
    std::vector<TString> ysonSignedStrings;
    for (const auto& value : expectedSignedValues) {
        ysonSignedStrings.emplace_back(ConvertToYsonString(value).ToString());
    }

    auto expectedUnsignedYsons = ToStringBufs(ysonUnsignedStrings);
    auto expectedSignedYsons = ToStringBufs(ysonSignedStrings);

    auto testAsType = [&] (
        int scale,
        ESimpleLogicalValueType logicalValueType,
        DB::DataTypePtr dataType,
        auto expectedValues,
        std::vector<TStringBuf> expectedYsons)
    {
        std::vector<DB::Field> fields;
        for (const auto& value : expectedValues) {
            DB::DateTime64 chValue = value;
            fields.emplace_back(DB::DecimalField(chValue, scale));
        }

        auto column = MakeColumn(dataType, fields);
        auto expectedLogicalType = SimpleLogicalType(logicalValueType);

        Converter_.emplace(dataType, Settings_);
        ExpectConversion(column, expectedLogicalType, expectedYsons);
    };

    testAsType(0, ESimpleLogicalValueType::Datetime64, std::make_shared<DB::DataTypeDateTime64>(0), expectedSignedValues, expectedSignedYsons);
    testAsType(6, ESimpleLogicalValueType::Timestamp64, std::make_shared<DB::DataTypeDateTime64>(6), expectedSignedValues, expectedSignedYsons);
    testAsType(6, ESimpleLogicalValueType::Timestamp, GetDataTypeTimestamp(), expectedUnsignedValues, expectedUnsignedYsons);

    auto invalidDateTimeDataType = std::make_shared<DB::DataTypeDateTime64>(3);
    EXPECT_THROW(Converter_.emplace(invalidDateTimeDataType, Settings_), std::exception);

    {
        auto dataType = GetDataTypeTimestamp();
        auto column = MakeColumn(dataType, {DB::DecimalField(DB::DateTime64(-1), 6)});
        Converter_.emplace(dataType, Settings_);
        EXPECT_THROW(Converter_->ConvertColumnToUnversionedValues(column), std::exception);
    }
}

TEST_F(TCHToYTConversionTest, Decimal)
{
    std::vector<TString> expectedValues = {
        "0",
        "1.23",
        "-1",
        "0.001",
        "123456.123",
        "999999.999",
        "-999999.999",
    };

    for (int precision : {9, 15, 18, 19, 35}) {
        int scale = 3;
        auto dataType = DB::createDecimal<DB::DataTypeDecimal>(precision, scale);

        std::vector<TString> ysonStrings;
        std::vector<DB::Field> fields;

        for (const auto& value : expectedValues) {
            TString binary = TDecimal::TextToBinary(value, precision, scale);
            ysonStrings.emplace_back(ConvertToYsonString(binary).ToString());

            auto addDecimalField = [&]<typename DecimalType>(TString value) {
                auto chValue = std::make_shared<DB::DataTypeDecimal<DecimalType>>(precision, scale)->parseFromString(value);
                fields.emplace_back(DB::DecimalField(chValue, scale));
            };

            if (precision <= static_cast<int>(DB::DecimalUtils::max_precision<DB::Decimal32>)) {
                addDecimalField.operator()<DB::Decimal32>(value);
            } else if (precision <= static_cast<int>(DB::DecimalUtils::max_precision<DB::Decimal64>)) {
                addDecimalField.operator()<DB::Decimal64>(value);
            } else if (precision <= static_cast<int>(DB::DecimalUtils::max_precision<DB::Decimal128>)) {
                addDecimalField.operator()<DB::Decimal128>(value);
            } else if (precision <= static_cast<int>(DB::DecimalUtils::max_precision<DB::Decimal256>)) {
                addDecimalField.operator()<DB::Decimal256>(value);
            } else {
                YT_ABORT();
            }
        }

        auto column = MakeColumn(dataType, fields);
        auto expectedLogicalType = DecimalLogicalType(precision, scale);
        auto expectedYsons = ToStringBufs(ysonStrings);

        Converter_.emplace(dataType, Settings_);

        ExpectConversion(column, expectedLogicalType, expectedYsons);
    }
}

TEST_F(TCHToYTConversionTest, NullableDecimal)
{
    std::vector<TString> expectedValues = {
        "0",
        "1.23",
        "#",
        "-1",
        "0.001",
        "123456.123",
        "#",
        "999999.999",
        "-999999.999",
        "#",
    };

    for (int precision : {9, 15, 18, 19, 35}) {
        int scale = 3;
        auto dataType = DB::makeNullable(DB::createDecimal<DB::DataTypeDecimal>(precision, scale));

        std::vector<TString> ysonStrings;
        std::vector<DB::Field> fields;

        for (const auto& value : expectedValues) {
            if (value == "#") {
                ysonStrings.emplace_back(value);
                fields.emplace_back(DB::Null());
            } else {
                TString binary = TDecimal::TextToBinary(value, precision, scale);
                ysonStrings.emplace_back(ConvertToYsonString(binary).ToString());

                if (precision <= 9) {
                    auto parsedValue = TDecimal::ParseBinary32(precision, binary);
                    fields.emplace_back(DB::DecimalField(DB::Decimal32(parsedValue), scale));
                } else if (precision <= 18) {
                    auto parsedValue = TDecimal::ParseBinary64(precision, binary);
                    fields.emplace_back(DB::DecimalField(DB::Decimal64(parsedValue), scale));
                } else {
                    auto ytValue = TDecimal::ParseBinary128(precision, binary);
                    DB::Decimal128 chValue;
                    std::memcpy(&chValue, &ytValue, sizeof(ytValue));
                    fields.emplace_back(DB::DecimalField(chValue, scale));
                }
            }
        }

        auto column = MakeColumn(dataType, fields);
        auto expectedLogicalType = OptionalLogicalType(DecimalLogicalType(precision, scale));
        auto expectedYsons = ToStringBufs(ysonStrings);

        Converter_.emplace(dataType, Settings_);

        ExpectConversion(column, expectedLogicalType, expectedYsons);
    }
}

TEST_F(TCHToYTConversionTest, NullableInt64)
{
    auto dataType = DB::makeNullable(std::make_shared<DB::DataTypeInt64>());

    auto column = MakeColumn(dataType, {
        DB::Int64(42),
        DB::Field(),
        DB::Int64(-11),
        DB::Field(),
        DB::Int64(0),
    });

    std::vector<TStringBuf> expectedValueYsons = {
        "42",
        "#",
        "-11",
        "#",
        "0",
    };

    auto expectedLogicalType = OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64));

    Converter_.emplace(dataType, Settings_);

    ExpectConversion(column, expectedLogicalType, expectedValueYsons);
}

TEST_F(TCHToYTConversionTest, ArrayInt32)
{
    auto dataType = std::make_shared<DB::DataTypeArray>(std::make_shared<DB::DataTypeInt32>());

    auto column = MakeColumn(dataType, {
        DB::Array{DB::Int32(42), DB::Int32(10)},
        DB::Array(),
        DB::Array{DB::Int32(-17)},
    });

    std::vector<TStringBuf> expectedValueYsons = {
        "[42;10]",
        "[]",
        "[-17]",
    };

    auto expectedLogicalType = ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int32));

    Converter_.emplace(dataType, Settings_);

    ExpectYsonConversion(column, expectedLogicalType, expectedValueYsons);
}

TEST_F(TCHToYTConversionTest, ArrayNullableString)
{
    auto dataType = std::make_shared<DB::DataTypeArray>(DB::makeNullable(std::make_shared<DB::DataTypeString>()));

    auto column = MakeColumn(dataType, {
        DB::Array{DB::String("foo"), DB::Field(), DB::String("bar")},
        DB::Array(),
        DB::Array{DB::String("baz")},
        DB::Array{DB::Field(), DB::Field()},
        DB::Array(),
        DB::Array{DB::String("qux")},
    });

    std::vector<TStringBuf> expectedValueYsons = {
        "[foo;#;bar]",
        "[]",
        "[baz]",
        "[#;#]",
        "[]",
        "[qux]",
    };

    auto expectedLogicalType = ListLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String)));

    Converter_.emplace(dataType, Settings_);

    ExpectYsonConversion(column, expectedLogicalType, expectedValueYsons);
}

TEST_F(TCHToYTConversionTest, TupleUInt32StringBoolean)
{
    auto dataType = std::make_shared<DB::DataTypeTuple>(std::vector<DB::DataTypePtr>{
        std::make_shared<DB::DataTypeUInt32>(),
        std::make_shared<DB::DataTypeString>(),
        GetDataTypeBoolean(),
    });

    auto column = MakeColumn(dataType, {
        DB::Tuple{DB::UInt32(42), DB::String("foo"), DB::UInt8(0)},
        DB::Tuple{DB::UInt32(123), DB::String("bar"), DB::UInt8(1)},
    });

    std::vector<TStringBuf> expectedValueYsons = {
        "[42u;foo;%false]",
        "[123u;bar;%true]",
    };

    auto expectedLogicalType = TupleLogicalType({
        SimpleLogicalType(ESimpleLogicalValueType::Uint32),
        SimpleLogicalType(ESimpleLogicalValueType::String),
        SimpleLogicalType(ESimpleLogicalValueType::Boolean),
    });

    Converter_.emplace(dataType, Settings_);

    ExpectYsonConversion(column, expectedLogicalType, expectedValueYsons);
}

TEST_F(TCHToYTConversionTest, NamedTupleInt8Double)
{
    auto dataType = std::make_shared<DB::DataTypeTuple>(std::vector<DB::DataTypePtr>{
        std::make_shared<DB::DataTypeInt8>(),
        std::make_shared<DB::DataTypeFloat64>(),
    }, std::vector<std::string>{
        "my_precious_int8",
        "their_ugly_float64",
    });

    auto column = MakeColumn(dataType, {
        DB::Tuple{DB::Int8(42), DB::Float64(6.25)},
        DB::Tuple{DB::Int8(-17), DB::Float64(-1.25)},
    });

    std::vector<TStringBuf> expectedValueYsons = {
        "[42;6.25]",
        "[-17;-1.25]",
    };

    auto expectedLogicalType = StructLogicalType({
        {"my_precious_int8", SimpleLogicalType(ESimpleLogicalValueType::Int8)},
        {"their_ugly_float64", SimpleLogicalType(ESimpleLogicalValueType::Double)},
    });

    Converter_.emplace(dataType, Settings_);

    ExpectYsonConversion(column, expectedLogicalType, expectedValueYsons);
}

TEST_F(TCHToYTConversionTest, LowCadrinalityString)
{
    auto dataType = std::make_shared<DB::DataTypeLowCardinality>(std::make_shared<DB::DataTypeString>());

    auto column = MakeColumn(dataType, {
        DB::Field("a"),
        DB::Field("abcd"),
        DB::Field("a"),
        DB::Field("b"),
    });

    std::vector<TStringBuf> expectedValueYsons = {
        "a",
        "abcd",
        "a",
        "b",
    };

    auto expectedLogicalType = SimpleLogicalType(ESimpleLogicalValueType::String);

    Converter_.emplace(dataType, Settings_);

    ExpectConversion(column, expectedLogicalType, expectedValueYsons);
}

TEST_F(TCHToYTConversionTest, LowCadrinalityNullable)
{
    auto dataType = std::make_shared<DB::DataTypeLowCardinality>(
            std::make_shared<DB::DataTypeNullable>(
                std::make_shared<DB::DataTypeString>()));

    auto column = MakeColumn(dataType, {
        DB::Field("a"),
        DB::Field("abcd"),
        DB::Field(),
        DB::Field("b"),
    });

    std::vector<TStringBuf> expectedValueYsons = {
        "a",
        "abcd",
        "#",
        "b",
    };

    auto expectedLogicalType = OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String));

    Converter_.emplace(dataType, Settings_);

    ExpectConversion(column, expectedLogicalType, expectedValueYsons);
}

TEST_F(TCHToYTConversionTest, MapInt32String)
{
    auto dataType = std::make_shared<DB::DataTypeMap>(
        std::make_shared<DB::DataTypeInt32>(),
        std::make_shared<DB::DataTypeString>()
    );

    auto column = MakeColumn(dataType, {
        DB::Map{DB::Tuple{DB::Int32(42), DB::String("foo")}, DB::Tuple{DB::Int32(27), DB::String("bar")}},
        DB::Map{},
        DB::Map{DB::Tuple{DB::Int32(-1), DB::String("")}},
    });

    std::vector<TStringBuf> expectedValueYsons = {
        "[[42; foo]; [27; bar]]",
        "[]",
        "[[-1; \"\"]]",
    };

    auto expectedLogicalType = DictLogicalType(
        SimpleLogicalType(ESimpleLogicalValueType::Int32),
        SimpleLogicalType(ESimpleLogicalValueType::String)
    );

    Converter_.emplace(dataType, Settings_);

    ExpectYsonConversion(column, expectedLogicalType, expectedValueYsons);
}

TEST_F(TCHToYTConversionTest, LowCadrinalityComposite)
{
    auto dataType = std::make_shared<DB::DataTypeArray>(
            std::make_shared<DB::DataTypeLowCardinality>(
                std::make_shared<DB::DataTypeString>()));

    auto column = MakeColumn(dataType, {
        DB::Array{DB::Field("a"), DB::Field("b")},
        DB::Array{DB::Field("abcd")},
    });

    std::vector<TStringBuf> expectedValueYsons = {
        "[a;b]",
        "[abcd]",
    };

    auto expectedLogicalType = ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String));

    Converter_.emplace(dataType, Settings_);

    ExpectYsonConversion(column, expectedLogicalType, expectedValueYsons);
}

TEST_F(TCHToYTConversionTest, UnsupportedTypesToString)
{
    auto dataTypeUUID = std::make_shared<DB::DataTypeUUID>();
    auto columnUUID = MakeColumn(dataTypeUUID, {
        DB::UUID(123454321),
    });

    std::vector<TStringBuf> expectedUUIDValueYsons = {
        R"("00000000-075b-c371-0000-000000000000")",
    };

    auto expectedUUIDLogicalType = SimpleLogicalType(ESimpleLogicalValueType::String);

    EXPECT_THROW(Converter_.emplace(dataTypeUUID, Settings_), std::exception);

    auto dataTypeArrayUUID = std::make_shared<DB::DataTypeArray>(dataTypeUUID);

    auto columnArrayUUID = MakeColumn(dataTypeArrayUUID, {
        DB::Array{
            DB::UUID{},
            DB::UUID{123454321},
        },
    });

    std::vector<TStringBuf> expectedArrayUUIDValueYsons = {
        R"(["00000000-0000-0000-0000-000000000000";"00000000-075b-c371-0000-000000000000";])",
    };

    auto expectedArrayUUIDLogicalType = ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String));

    EXPECT_THROW(Converter_.emplace(dataTypeArrayUUID, Settings_), std::exception);

    auto settings = New<TCompositeSettings>();
    settings->ConvertUnsupportedTypesToString = true;

    Converter_.emplace(dataTypeUUID, settings);
    ExpectConversion(columnUUID, expectedUUIDLogicalType, expectedUUIDValueYsons);

    Converter_.emplace(dataTypeArrayUUID, settings);
    ExpectYsonConversion(columnArrayUUID, expectedArrayUUIDLogicalType, expectedArrayUUIDValueYsons);
}


////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NClickHouseServer
