#include <yt/yt/core/test_framework/framework.h>

#include <yt/chyt/server/config.h>
#include <yt/chyt/server/conversion.h>
#include <yt/chyt/server/custom_data_types.h>
#include <yt/chyt/server/helpers.h>
#include <yt/chyt/server/yt_to_ch_column_converter.h>

#include <yt/yt/ytlib/table_chunk_format/column_reader.h>
#include <yt/yt/ytlib/table_chunk_format/column_writer.h>
#include <yt/yt/ytlib/table_chunk_format/data_block_writer.h>

#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/library/clickhouse_functions/unescaped_yson.h>

#include <yt/yt/library/decimal/decimal.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/iterator/functools.h>

#include <Core/Types.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeMap.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>

#include <util/system/env.h>

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

void AppendVector(std::vector<TString>& lhs, std::vector<TString>& rhs)
{
    lhs.insert(lhs.end(), rhs.begin(), rhs.end());
}

void ValidateTypeEquality(const DB::DataTypePtr& actualType, const DB::DataTypePtr& expectedType)
{
    ASSERT_NE(actualType, nullptr);
    ASSERT_NE(expectedType, nullptr);
    ASSERT_EQ(actualType->getName(), expectedType->getName());
    ASSERT_TRUE(actualType->equals(*expectedType));
}

////////////////////////////////////////////////////////////////////////////////

//! Empty string stands for ConsumeNulls(1) call.
using TYsonStringBufs = std::vector<TYsonStringBuf>;
using TUnversionedValues = std::vector<TUnversionedValue>;
using TYTColumn = IUnversionedColumnarRowBatch::TColumn*;

void DoConsume(TYTToCHColumnConverter& converter, TYsonStringBufs ysons)
{
    for (const auto& yson : ysons) {
        if (!yson) {
            converter.ConsumeNulls(1);
        } else {
            converter.ConsumeYson(yson);
        }
    }
}

void DoConsume(TYTToCHColumnConverter& converter, TUnversionedValues values)
{
    converter.ConsumeUnversionedValues(values);
}

void DoConsume(TYTToCHColumnConverter& converter, TYTColumn ytColumn)
{
    converter.ConsumeYtColumn(*ytColumn);
}

void ExpectFields(const DB::IColumn& column, const std::vector<DB::Field>& expectedFields)
{
    ASSERT_EQ(expectedFields.size(), column.size());

    for (int index = 0; index < std::ssize(column); ++index) {
        EXPECT_EQ(expectedFields[index], column[index]) << "Mismatch at position " << index;
        if (!(expectedFields[index] == column[index])) {
            YT_ABORT();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TYTToCHConversionTest
    : public ::testing::Test
{
public:
    void SetUp() override
    {
        Settings_ = New<TCompositeSettings>();
    }

protected:
    TCompositeSettingsPtr Settings_;
    TRowBufferPtr RowBuffer_ = New<TRowBuffer>();

    template <class TExpectedColumnType = void>
    void ExpectDataConversion(TComplexTypeFieldDescriptor descriptor, const auto& input, const std::vector<DB::Field>& expectedFields) const
    {
        TYTToCHColumnConverter converter(descriptor, Settings_);
        converter.InitColumn();
        DoConsume(converter, input);
        auto column = converter.FlushColumn();

        if constexpr (!std::is_void_v<TExpectedColumnType>) {
            EXPECT_NE(dynamic_cast<const TExpectedColumnType*>(column.get()), nullptr);
        }

        ExpectFields(*column, expectedFields);
    }

    void ExpectDataConversion(
        TComplexTypeFieldDescriptor descriptor,
        const TYTColumn& ytColumn,
        TRange<DB::UInt8> filterHint,
        const std::vector<DB::Field>& expectedFields) const
    {
        TYTToCHColumnConverter converter(descriptor, Settings_);
        converter.InitColumn();
        converter.ConsumeYtColumn(*ytColumn, filterHint);
        auto chColumn = converter.FlushColumn();

        ExpectFields(*chColumn, expectedFields);
    }

    void ExpectTypeConversion(TComplexTypeFieldDescriptor descriptor, const DB::DataTypePtr& expectedDataType) const
    {
        TYTToCHColumnConverter converter(descriptor, Settings_);
        ValidateTypeEquality(converter.GetDataType(), expectedDataType);
    }
};

////////////////////////////////////////////////////////////////////////////////

TYsonStringBufs ToYsonStringBufs(std::vector<TString>& ysonStrings)
{
    TYsonStringBufs result;
    result.reserve(ysonStrings.size());
    for (const auto& ysonString : ysonStrings) {
        if (ysonString.empty()) {
            result.emplace_back();
        } else {
            result.emplace_back(ysonString);
        }
    }
    return result;
}

std::pair<TUnversionedValues, std::any> YsonStringBufsToVariadicUnversionedValues(TYsonStringBufs ysons)
{
    auto rowBuffer = New<TRowBuffer>();
    TUnversionedValues result;
    for (const auto& yson : ysons) {
        if (yson) {
            result.push_back(TryDecodeUnversionedAnyValue(MakeUnversionedAnyValue(yson.AsStringBuf()), rowBuffer));
        } else {
            result.push_back(MakeUnversionedNullValue());
        }
        result.back() = rowBuffer->CaptureValue(result.back());
    }
    return {result, rowBuffer};
}

std::pair<TUnversionedValues, std::any> YsonStringBufsToAnyUnversionedValues(TYsonStringBufs ysons)
{
    auto rowBuffer = New<TRowBuffer>();
    TUnversionedValues result;
    for (const auto& yson : ysons) {
        if (yson) {
            result.emplace_back(rowBuffer->CaptureValue(MakeUnversionedAnyValue(yson.AsStringBuf())));
        } else {
            result.emplace_back(MakeUnversionedNullValue());
        }
    }
    return {result, rowBuffer};
}

std::pair<TYTColumn, std::any> UnversionedValuesToYtColumn(TUnversionedValues values, TColumnSchema columnSchema)
{
    TDataBlockWriter blockWriter;
    auto writer = CreateUnversionedColumnWriter(
        /*columnIndex*/ 0,
        columnSchema,
        &blockWriter,
        GetNullMemoryUsageTracker());

    for (const auto& value : values) {
        TUnversionedRowBuilder builder;
        builder.AddValue(value);
        TUnversionedRow row = builder.GetRow();
        writer->WriteUnversionedValues(TRange(&row, 1));
    }

    writer->FinishBlock(/*blockIndex*/ 0);
    auto block = blockWriter.DumpBlock(/*blockIndex*/ 0, values.size());
    auto meta = writer->ColumnMeta();

    auto reader = CreateUnversionedColumnReader(columnSchema, meta, /*columnIndex*/ 0, /*columnId*/ 0, /*sortOrder*/ std::nullopt);

    struct TTag
    { };

    reader->SetCurrentBlock(MergeRefsToRef<TTag>(block.Data), /*blockIndex*/ 0);

    auto columns = std::make_unique<IUnversionedColumnarRowBatch::TColumn[]>(reader->GetBatchColumnCount());
    reader->ReadColumnarBatch(TMutableRange(columns.get(), reader->GetBatchColumnCount()), values.size());

    struct TOwner
    {
        decltype(columns) Columns;
        decltype(reader) Reader;
        decltype(block) Block;
    };

    auto ytColumn = &columns[0];
    ytColumn->Type = columnSchema.LogicalType();

    auto owner = std::make_shared<TOwner>();
    owner->Columns = std::move(columns);
    owner->Block = std::move(block);
    owner->Reader = std::move(reader);

    return {ytColumn, owner};
}

////////////////////////////////////////////////////////////////////////////////

bool IsDecimalRepresentable(const TString& decimal, int precision, int scale)
{
    auto decimalPoint = std::find(decimal.begin(), decimal.end(), '.');
    auto isDigit = [] (unsigned char c) { return std::isdigit(c); };

    auto digitsBeforePoint = std::count_if(decimal.begin(), decimalPoint, isDigit);
    auto digitsAfterPoint = std::count_if(decimalPoint, decimal.end(), isDigit);

    if (digitsAfterPoint > scale) {
        return false;
    }
    if (scale + digitsBeforePoint > precision) {
        return false;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TYTToCHConversionTest, AnyPassthrough)
{
    std::vector<TString> ysons = {
        "42",
        "xyz",
        "[42; xyz]",
        "{foo={bar={baz=42}}}",
    };

    for (auto& yson : ysons) {
        yson = ToString(ConvertToYsonString(TYsonString(yson), EYsonFormat::Binary));
    }

    auto logicalType = SimpleLogicalType(ESimpleLogicalValueType::Any);
    TComplexTypeFieldDescriptor descriptor(logicalType);

    auto anyYsons = ToYsonStringBufs(ysons);
    auto [anyUnversionedValues, anyUnversionedValuesOwner] = YsonStringBufsToAnyUnversionedValues(anyYsons);
    auto [ytColumn, ytColumnOwner] = UnversionedValuesToYtColumn(anyUnversionedValues, TColumnSchema(/*name*/ "", logicalType));


    for (const auto& ysonFormat : TEnumTraits<EExtendedYsonFormat>::GetDomainValues()) {
        Settings_->DefaultYsonFormat = ysonFormat;

        ExpectTypeConversion(descriptor, std::make_shared<DB::DataTypeString>());

        std::vector<DB::Field> expectedFields;
        for (const auto& yson : ysons) {
            expectedFields.emplace_back(std::string(ConvertToYsonStringExtendedFormat(TYsonString(yson), ysonFormat).AsStringBuf()));
        }

        ExpectDataConversion(descriptor, anyYsons, expectedFields);
        ExpectDataConversion(descriptor, anyUnversionedValues, expectedFields);
        ExpectDataConversion(descriptor, ytColumn, expectedFields);
    }
}

TEST_F(TYTToCHConversionTest, SimpleTypes)
{
    // Prepare test values as YSON.
    std::vector<TString> ysonStringsInt8 = {
        "42",
        "-1",
        "-128",
        "127",
    };
    std::vector<TString> ysonStringsInt16 = {
        ToString(std::numeric_limits<i16>::min()),
        ToString(std::numeric_limits<i16>::max()),
    };
    AppendVector(ysonStringsInt16, ysonStringsInt8);
    std::vector<TString> ysonStringsInt32 = {
        ToString(std::numeric_limits<i32>::min()),
        ToString(std::numeric_limits<i32>::max()),
    };
    AppendVector(ysonStringsInt32, ysonStringsInt16);
    std::vector<TString> ysonStringsInt64 = {
        ToString(std::numeric_limits<i64>::min()),
        ToString(std::numeric_limits<i64>::max()),
    };
    AppendVector(ysonStringsInt64, ysonStringsInt32);

    std::vector<TString> ysonStringsUint8 = {
        "42u",
        "255u",
        "0u",
    };
    std::vector<TString> ysonStringsUint16 = {
        ToString(std::numeric_limits<ui16>::max()) + "u",
    };
    AppendVector(ysonStringsUint16, ysonStringsUint8);
    std::vector<TString> ysonStringsUint32 = {
        ToString(std::numeric_limits<ui32>::max()) + "u",
    };
    AppendVector(ysonStringsUint32, ysonStringsUint16);
    std::vector<TString> ysonStringsUint64 = {
        ToString(std::numeric_limits<ui64>::max()) + "u",
    };
    AppendVector(ysonStringsUint64, ysonStringsUint32);

    std::vector<TString> ysonStringsBool = {
        "%false",
        "%true",
    };

    std::vector<TString> ysonStringsString = {
        "\"foo\"",
        "\"\"",
    };

    std::vector<TString> ysonStringsFloat = {
        "3.14",
        "-2.718",
        "%nan ",
        "%inf ",
        "%-inf ",
        "0.0",
    };

    std::vector<TString> ysonStringsJson = {
        R"("{\"foo\":\"bar\", \"bar\":[\"baz\"]}")",
        R"("{\"id\":1}")",
        R"("{}")",
    };

    auto ysonsInt8 = ToYsonStringBufs(ysonStringsInt8);
    auto ysonsInt16 = ToYsonStringBufs(ysonStringsInt16);
    auto ysonsInt32 = ToYsonStringBufs(ysonStringsInt32);
    auto ysonsInt64 = ToYsonStringBufs(ysonStringsInt64);
    auto ysonsUint8 = ToYsonStringBufs(ysonStringsUint8);
    auto ysonsUint16 = ToYsonStringBufs(ysonStringsUint16);
    auto ysonsUint32 = ToYsonStringBufs(ysonStringsUint32);
    auto ysonsUint64 = ToYsonStringBufs(ysonStringsUint64);
    auto ysonsBool = ToYsonStringBufs(ysonStringsBool);
    auto ysonsString = ToYsonStringBufs(ysonStringsString);
    auto ysonsFloat = ToYsonStringBufs(ysonStringsFloat);
    auto ysonsJson = ToYsonStringBufs(ysonStringsJson);

    // Dummy variables are replacing template class parameters which are currently not available for lambdas.
    auto testAsType = [&] (
        const TYsonStringBufs ysons,
        ESimpleLogicalValueType simpleLogicalValueType,
        DB::DataTypePtr expectedDataType,
        auto ytTypeDummy,
        auto chTypeDummy)
    {
        YT_LOG_TRACE("Running tests (Type: %v)", simpleLogicalValueType);

        using TYtType = decltype(ytTypeDummy);
        using TChType = decltype(chTypeDummy);

        std::vector<DB::Field> expectedFields;
        for (const auto& yson : ysons) {
            expectedFields.emplace_back(TChType(ConvertTo<TYtType>(yson)));
        }

        auto logicalType = SimpleLogicalType(simpleLogicalValueType);

        auto [unversionedValues, unversionedValuesOwner] = YsonStringBufsToVariadicUnversionedValues(ysons);
        TColumnSchema columnSchemaRequired(/*name*/ "", logicalType);
        TColumnSchema columnSchemaOptional(/*name*/ "", OptionalLogicalType(logicalType));
        auto [ytColumnOptional, ytColumnOptionalOwner] = UnversionedValuesToYtColumn(unversionedValues, columnSchemaOptional);
        auto [ytColumnRequired, ytColumnRequiredOwner] = UnversionedValuesToYtColumn(unversionedValues, columnSchemaRequired);

        TComplexTypeFieldDescriptor descriptor(logicalType);
        ExpectTypeConversion(descriptor, expectedDataType);
        ExpectDataConversion(descriptor, ysons, expectedFields);
        ExpectDataConversion(descriptor, unversionedValues, expectedFields);
        ExpectDataConversion(descriptor, ytColumnOptional, expectedFields);
        ExpectDataConversion(descriptor, ytColumnRequired, expectedFields);
    };

    testAsType(ysonsInt8, ESimpleLogicalValueType::Int8, std::make_shared<DB::DataTypeNumber<DB::Int8>>(), i8(), i8());
    testAsType(ysonsInt16, ESimpleLogicalValueType::Int16, std::make_shared<DB::DataTypeNumber<i16>>(), i16(), i16());
    testAsType(ysonsInt32, ESimpleLogicalValueType::Int32, std::make_shared<DB::DataTypeNumber<i32>>(), i32(), i32());
    testAsType(ysonsInt64, ESimpleLogicalValueType::Int64, std::make_shared<DB::DataTypeNumber<i64>>(), i64(), i64());
    testAsType(ysonsInt64, ESimpleLogicalValueType::Interval, std::make_shared<DB::DataTypeInterval>(DB::IntervalKind::Kind::Microsecond), i64(), i64());
    testAsType(ysonsInt64, ESimpleLogicalValueType::Interval64, std::make_shared<DB::DataTypeInterval>(DB::IntervalKind::Kind::Microsecond), i64(), i64());
    testAsType(ysonsUint8, ESimpleLogicalValueType::Uint8, std::make_shared<DB::DataTypeNumber<DB::UInt8>>(), DB::UInt8(), DB::UInt8());
    testAsType(ysonsBool, ESimpleLogicalValueType::Boolean, GetDataTypeBoolean(), bool(), DB::UInt8());
    testAsType(ysonsUint16, ESimpleLogicalValueType::Uint16, std::make_shared<DB::DataTypeNumber<ui16>>(), ui16(), ui16());
    testAsType(ysonsUint32, ESimpleLogicalValueType::Uint32, std::make_shared<DB::DataTypeNumber<ui32>>(), ui32(), ui32());
    testAsType(ysonsUint64, ESimpleLogicalValueType::Uint64, std::make_shared<DB::DataTypeNumber<ui64>>(), ui64(), ui64());
    testAsType(ysonsUint16, ESimpleLogicalValueType::Date, std::make_shared<DB::DataTypeDate>(), ui16(), ui16());
    testAsType(ysonsInt32, ESimpleLogicalValueType::Date32, std::make_shared<DB::DataTypeDate32>(), i32(), i32());
    testAsType(ysonsUint32, ESimpleLogicalValueType::Datetime, std::make_shared<DB::DataTypeDateTime>(), ui32(), ui32());
    testAsType(ysonsString, ESimpleLogicalValueType::String, std::make_shared<DB::DataTypeString>(), TString(), std::string());
    testAsType(ysonsJson, ESimpleLogicalValueType::Json, std::make_shared<DB::DataTypeString>(), TString(), std::string());
    testAsType(ysonsFloat, ESimpleLogicalValueType::Float, std::make_shared<DB::DataTypeNumber<float>>(), double(), float());
    testAsType(ysonsFloat, ESimpleLogicalValueType::Double, std::make_shared<DB::DataTypeNumber<double>>(), double(), double());
}

TEST_F(TYTToCHConversionTest, DateTime64Types)
{
    std::vector<ui64> ysonUnsignedValues = {
        0,
        42,
        1234,
        12345,
        654321,
        7777777,
    };
    std::vector<i64> ysonSignedValues = {
        -7777777,
        -654321,
        -12345,
        -1234,
        -42,
        -1,
        0,
        42,
        1234,
        12345,
        654321,
        7777777,
    };

    std::vector<TString> ysonUnsignedStrings;
    ysonUnsignedStrings.reserve(ysonUnsignedValues.size());
    for (const auto& val : ysonUnsignedValues) {
        ysonUnsignedStrings.emplace_back(ConvertToYsonString(val).ToString());
    }

    std::vector<TString> ysonSignedStrings;
    ysonSignedStrings.reserve(ysonSignedValues.size());
    for (const auto& val : ysonSignedValues) {
        ysonSignedStrings.emplace_back(ToString(val));
    }

    auto ysonsUnsigned = ToYsonStringBufs(ysonUnsignedStrings);
    auto ysonsSigned = ToYsonStringBufs(ysonSignedStrings);


    auto testAsType = [&] (
        ESimpleLogicalValueType simpleLogicalValueType,
        int scale,
        auto ysonValues,
        TYsonStringBufs ysons)
    {
        DB::DataTypePtr expectedDataType = std::make_shared<DB::DataTypeDateTime64>(scale);
        std::vector<DB::Field> expectedFields;
        for (const auto& ytValue : ysonValues) {
            expectedFields.emplace_back(DB::DecimalField(DB::DateTime64(ytValue), scale));
        }

        auto logicalType = SimpleLogicalType(simpleLogicalValueType);

        auto [unversionedValues, _] = YsonStringBufsToVariadicUnversionedValues(ysons);
        TColumnSchema columnSchemaRequired(/*name*/ "", logicalType);
        TColumnSchema columnSchemaOptional(/*name*/ "", OptionalLogicalType(logicalType));
        auto [ytColumnOptional, ytColumnOptionalOwner] = UnversionedValuesToYtColumn(unversionedValues, columnSchemaOptional);
        auto [ytColumnRequired, ytColumnRequiredOwner] = UnversionedValuesToYtColumn(unversionedValues, columnSchemaRequired);

        TComplexTypeFieldDescriptor descriptor(logicalType);
        ExpectTypeConversion(descriptor, expectedDataType);
        ExpectDataConversion(descriptor, ysons, expectedFields);
        ExpectDataConversion(descriptor, unversionedValues, expectedFields);
        ExpectDataConversion(descriptor, ytColumnOptional, expectedFields);
        ExpectDataConversion(descriptor, ytColumnRequired, expectedFields);
    };

    testAsType(ESimpleLogicalValueType::Datetime64, 0, ysonSignedValues, ysonsSigned);
    testAsType(ESimpleLogicalValueType::Timestamp, 6, ysonUnsignedValues, ysonsUnsigned);
    testAsType(ESimpleLogicalValueType::Timestamp64, 6, ysonSignedValues, ysonsSigned);

    {
        auto columnSchema = TColumnSchema(
            /*name*/ "",
            SimpleLogicalType(ESimpleLogicalValueType::Timestamp));
        TComplexTypeFieldDescriptor descriptor(columnSchema);
        auto converter = TYTToCHColumnConverter(descriptor, Settings_, /*isReadConversions*/ false);
        auto dataType = converter.GetDataType();
        ASSERT_EQ("YtTimestamp", dataType->getName());
    }
}

TEST_F(TYTToCHConversionTest, OptionalSimpleTypeAsUnversionedValue)
{
    auto intValue = MakeUnversionedInt64Value(42);
    auto nullValue = MakeUnversionedNullValue();

    auto logicalType = OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64));
    TComplexTypeFieldDescriptor descriptor(logicalType);
    TYTToCHColumnConverter converter(descriptor, Settings_);
    converter.InitColumn();

    std::vector<TUnversionedValue> values = {intValue, nullValue};

    converter.ConsumeUnversionedValues(values);

    auto column = converter.FlushColumn();
    ExpectFields(*column, std::vector<DB::Field>{
        DB::Field(42),
        DB::Field(),
    });
}

TEST_F(TYTToCHConversionTest, OptionalSimpleType)
{
    // Nesting level is the number of optional<> wrappers around int.

    std::vector<std::vector<TString>> ysonStringsByNestingLevel = {
        {}, // unused
        {"", "#", "42"},
        {"", "#", "[#]", "[42]"},
        {"", "#", "[#]", "[[#]]", "[[42]]"},
    };

    auto logicalType = SimpleLogicalType(ESimpleLogicalValueType::Int64);
    auto expectedDataType = DB::makeNullable(std::make_shared<DB::DataTypeNumber<i64>>());

    for (int nestingLevel = 1; nestingLevel <= 3; ++nestingLevel) {
        logicalType = OptionalLogicalType(std::move(logicalType));

        YT_LOG_TRACE(
            "Running tests (NestingLevel: %v)",
            nestingLevel);
        TComplexTypeFieldDescriptor descriptor(logicalType);
        ExpectTypeConversion(descriptor, expectedDataType);

        auto ysons = ToYsonStringBufs(ysonStringsByNestingLevel[nestingLevel]);
        // <Null>s or <Int64>s.
        auto [variadicUnversionedValues, variadicUnversionedValuesOwner] = YsonStringBufsToVariadicUnversionedValues(ysons);
        // <Any>s.
        auto [anyUnversionedValues, anyUnversionedValuesOwner] = YsonStringBufsToAnyUnversionedValues(ysons);
        std::vector<DB::Field> expectedFields;
        for (int index = 0; index <= nestingLevel; ++index) {
            expectedFields.emplace_back();
        }
        expectedFields.emplace_back(42);

        ExpectDataConversion(descriptor, ysons, expectedFields);
        ExpectDataConversion(descriptor, variadicUnversionedValues, expectedFields);

        TColumnSchema columnSchema(/*name*/ "", logicalType);

        if (nestingLevel == 1) {
            // We may interpret values as unversioned values <Null>, <Null> and 42.
            auto [ytColumn, ytColumnOwner] = UnversionedValuesToYtColumn(variadicUnversionedValues, columnSchema);
            ExpectDataConversion(descriptor, ytColumn, expectedFields);
        } else {
            // Physical type for optional<optional<int>> (and more nested types) is any,
            // so in this case we deal with string yt columns. They should be dispatched to YSON-based consumers.
            auto [ytColumn, ytColumnOwner] = UnversionedValuesToYtColumn(anyUnversionedValues, columnSchema);
            ExpectDataConversion(descriptor, ytColumn, expectedFields);
        }
    }
}

TEST_F(TYTToCHConversionTest, NullAndVoid)
{
    // Nesting level is the number of optional<> wrappers around null or void.

    std::vector<std::vector<TString>> ysonStringsByNestingLevel = {
        {"", "#"},
        {"", "#", "[#]"},
        {"", "#", "[#]", "[[#]]"},
    };

    auto expectedDataType = DB::makeNullable(std::make_shared<DB::DataTypeNothing>());

    for (auto simpleLogicalValueType : {ESimpleLogicalValueType::Null, ESimpleLogicalValueType::Void}) {
        auto logicalType = SimpleLogicalType(simpleLogicalValueType);

        for (int nestingLevel = 0; nestingLevel <= 2; ++nestingLevel) {
            if (nestingLevel != 0) {
                logicalType = OptionalLogicalType(std::move(logicalType));
            }

            YT_LOG_TRACE(
                "Running tests (SimpleLogicalValueType: %v, NestingLevel: %v)",
                simpleLogicalValueType,
                nestingLevel);

            TComplexTypeFieldDescriptor descriptor(logicalType);
            ExpectTypeConversion(descriptor, expectedDataType);

            auto ysons = ToYsonStringBufs(ysonStringsByNestingLevel[nestingLevel]);
            // <Null>s or <Any>s.
            auto [variadicUnversionedValues, variadicUnversionedValuesOwner] = YsonStringBufsToVariadicUnversionedValues(ysons);
            // <Any>s.
            auto [anyUnversionedValues, anyUnversionedValuesOwner] = YsonStringBufsToAnyUnversionedValues(ysons);
            std::vector<DB::Field> expectedFields(nestingLevel + 2);

            ExpectDataConversion(descriptor, ysons, expectedFields);
            ExpectDataConversion(descriptor, variadicUnversionedValues, expectedFields);

            TColumnSchema columnSchema(/*name*/ "", logicalType);

            if (nestingLevel == 0) {
                // We may interpret values as unversioned values <Null> and <Null>.
                auto [ytColumn, ytColumnOwner] = UnversionedValuesToYtColumn(variadicUnversionedValues, columnSchema);
                ExpectDataConversion(descriptor, ytColumn, expectedFields);
            } else {
                // Physical type for optional<null/void> (and more nested types) is any,
                // so in this case we deal with string yt columns. They should be dispatched to YSON-based consumers.
                auto [ytColumn, ytColumnOwner] = UnversionedValuesToYtColumn(anyUnversionedValues, columnSchema);
                ExpectDataConversion(descriptor, ytColumn, expectedFields);
            }
        }
    }
}

TEST_F(TYTToCHConversionTest, ListInt32)
{
    std::vector<TString> ysonStringsListInt32 = {
        "[42;57]",
        "[]",
        "[58;124;99;]",
        "",
        "[-1]",
    };
    auto ysonsListInt32 = ToYsonStringBufs(ysonStringsListInt32);

    auto logicalType = ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int32));
    auto expectedDataType = std::make_shared<DB::DataTypeArray>(std::make_shared<DB::DataTypeNumber<i32>>());

    TComplexTypeFieldDescriptor descriptor(logicalType);
    ExpectTypeConversion(descriptor, expectedDataType);

    std::vector<DB::Field> expectedFields{
        DB::Array{DB::Field(42), DB::Field(57)},
        DB::Array{},
        DB::Array{DB::Field(58), DB::Field(124), DB::Field(99)},
        DB::Array{},
        DB::Array{DB::Field(-1)},
    };

    auto [anyUnversionedValues, anyUnversionedValuesOwner] = YsonStringBufsToAnyUnversionedValues(ysonsListInt32);
    auto [ytColumn, ytColumnOwner] = UnversionedValuesToYtColumn(anyUnversionedValues, TColumnSchema(/*name*/ "", logicalType));

    ExpectDataConversion(descriptor, ysonsListInt32, expectedFields);
    ExpectDataConversion(descriptor, anyUnversionedValues, expectedFields);
    ExpectDataConversion(descriptor, ytColumn, expectedFields);
}

TEST_F(TYTToCHConversionTest, ListListInt32)
{
    std::vector<TString> ysonStringsListListInt32 = {
        "[[42;57];[-1]]",
        "[]",
        "[[];]",
        "",
        "[[];[];[1;2;]]",
    };
    auto ysonsListListInt32 = ToYsonStringBufs(ysonStringsListListInt32);

    auto logicalType = ListLogicalType(ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int32)));
    auto expectedDataType = std::make_shared<DB::DataTypeArray>(std::make_shared<DB::DataTypeArray>(std::make_shared<DB::DataTypeNumber<i32>>()));

    TComplexTypeFieldDescriptor descriptor(logicalType);
    ExpectTypeConversion(descriptor, expectedDataType);

    std::vector<DB::Field> expectedFields{
        DB::Array{DB::Array{DB::Field(42), DB::Field(57)}, DB::Array{-1}},
        DB::Array{},
        DB::Array{DB::Field(DB::Array{})}, // Without explicit DB::Field, compiler thinks that it is a copy-constructor.
        DB::Array{},
        DB::Array{DB::Array{}, DB::Array{}, DB::Array{DB::Field(1), DB::Field(2)}},
    };

    auto [anyUnversionedValues, anyUnversionedValuesOwner] = YsonStringBufsToAnyUnversionedValues(ysonsListListInt32);
    auto [ytColumn, ytColumnOwner] = UnversionedValuesToYtColumn(anyUnversionedValues, TColumnSchema(/*name*/ "", logicalType));

    ExpectDataConversion(descriptor, ysonsListListInt32, expectedFields);
    ExpectDataConversion(descriptor, anyUnversionedValues, expectedFields);
    ExpectDataConversion(descriptor, ytColumn, expectedFields);
}

TEST_F(TYTToCHConversionTest, ListAny)
{
    std::vector<TString> ysonStringsListAny = {
        "[#; 42; []; [[];[]]; x]",
        "[foo; 23; bar; {foo=bar}]",
        "[]",
    };
    auto ysonsListAny = ToYsonStringBufs(ysonStringsListAny);

    auto logicalType = ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Any));
    auto expectedDataType = std::make_shared<DB::DataTypeArray>(std::make_shared<DB::DataTypeString>());

    Settings_->DefaultYsonFormat = EExtendedYsonFormat::Text;

    TComplexTypeFieldDescriptor descriptor(logicalType);
    ExpectTypeConversion(descriptor, expectedDataType);

    std::vector<DB::Field> expectedFields{
        DB::Array{DB::Field("#"), DB::Field("42"), DB::Field("[]"), DB::Field("[[];[];]"), DB::Field("\"x\"")},
        DB::Array{DB::Field("\"foo\""), DB::Field("23"), DB::Field("\"bar\""), DB::Field("{\"foo\"=\"bar\";}")},
        DB::Array{},
    };

    auto [anyUnversionedValues, anyUnversionedValuesOwner] = YsonStringBufsToAnyUnversionedValues(ysonsListAny);
    auto [ytColumn, ytColumnOwner] = UnversionedValuesToYtColumn(anyUnversionedValues, TColumnSchema(/*name*/ "", logicalType));

    ExpectDataConversion(descriptor, ysonsListAny, expectedFields);
    ExpectDataConversion(descriptor, anyUnversionedValues, expectedFields);
    ExpectDataConversion(descriptor, ytColumn, expectedFields);
}

TEST_F(TYTToCHConversionTest, OptionalListOptionalInt32)
{
    std::vector<TString> ysonStringsOptionalListOptionalInt32 = {
        "[#]",
        "",
        "[]",
        "[42]",
        "[#;58;12;#]",
    };
    auto ysonsOptionalListOptionalInt32 = ToYsonStringBufs(ysonStringsOptionalListOptionalInt32);

    auto logicalType = OptionalLogicalType(ListLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int32))));
    auto expectedDataType = std::make_shared<DB::DataTypeArray>(std::make_shared<DB::DataTypeNullable>(std::make_shared<DB::DataTypeNumber<i32>>()));

    TComplexTypeFieldDescriptor descriptor(logicalType);
    ExpectTypeConversion(descriptor, expectedDataType);

    std::vector<DB::Field> expectedFields{
        DB::Array{DB::Field()},
        DB::Array{},
        DB::Array{},
        DB::Array{DB::Field(42)},
        DB::Array{DB::Field(), DB::Field(58), DB::Field(12), DB::Field()},
    };

    auto [anyUnversionedValues, anyUnversionedValuesOwner] = YsonStringBufsToAnyUnversionedValues(ysonsOptionalListOptionalInt32);
    auto [ytColumn, ytColumnOwner] = UnversionedValuesToYtColumn(anyUnversionedValues, TColumnSchema(/*name*/ "", logicalType));

    ExpectDataConversion(descriptor, ysonsOptionalListOptionalInt32, expectedFields);
    ExpectDataConversion(descriptor, anyUnversionedValues, expectedFields);
    ExpectDataConversion(descriptor, ytColumn, expectedFields);
}

TEST_F(TYTToCHConversionTest, DictIntString)
{
    std::vector<TString> ysonStringsDictIntString = {
        "[[42; foo]; [27; bar]]",
        "[]",
        "[[-1; \"\"]]",
    };
    auto ysonsDictIntString = ToYsonStringBufs(ysonStringsDictIntString);

    auto logicalType = DictLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int32), SimpleLogicalType(ESimpleLogicalValueType::String));
    auto expectedDataType = std::make_shared<DB::DataTypeMap>(
        std::make_shared<DB::DataTypeInt32>(), std::make_shared<DB::DataTypeString>()
    );

    TComplexTypeFieldDescriptor descriptor(logicalType);
    ExpectTypeConversion(descriptor, expectedDataType);

    std::vector<DB::Field> expectedFields{
        DB::Map{DB::Tuple{DB::Field(42), DB::Field("foo")}, DB::Tuple{DB::Field(27), DB::Field("bar")}},
        DB::Map{},
        DB::Map{DB::Tuple{DB::Field(-1), DB::Field("")}},
    };

    auto [anyUnversionedValues, anyUnversionedValuesOwner] = YsonStringBufsToAnyUnversionedValues(ysonsDictIntString);
    auto [ytColumn, ytColumnOwner] = UnversionedValuesToYtColumn(anyUnversionedValues, TColumnSchema(/*name*/ "", logicalType));

    ExpectDataConversion(descriptor, ysonsDictIntString, expectedFields);
    ExpectDataConversion(descriptor, anyUnversionedValues, expectedFields);
    ExpectDataConversion(descriptor, ytColumn, expectedFields);
}

TEST_F(TYTToCHConversionTest, DictWithUnsupportedKeyType)
{
    std::vector<TString> ysonStrings = {
        "[[42; foo]; [27; bar]]",
        "[[#; empty]]",
        "[[-1; \"\"]]",
    };
    auto ysons = ToYsonStringBufs(ysonStrings);

    auto logicalType = DictLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int32)), SimpleLogicalType(ESimpleLogicalValueType::String));
    auto expectedDataType = std::make_shared<DB::DataTypeArray>(
        std::make_shared<DB::DataTypeTuple>(
            std::vector<DB::DataTypePtr>{DB::makeNullable(std::make_shared<DB::DataTypeInt32>()), std::make_shared<DB::DataTypeString>()},
            std::vector<std::string>{"keys", "values"}
        )
    );

    TComplexTypeFieldDescriptor desctiptor(logicalType);
    ExpectTypeConversion(desctiptor, expectedDataType);

    std::vector<DB::Field> expectedFields{
        DB::Array{DB::Tuple{DB::Field(42), DB::Field("foo")}, DB::Tuple{DB::Field(27), DB::Field("bar")}},
        DB::Array{DB::Tuple{DB::Field(), DB::Field("empty")}},
        DB::Array{DB::Tuple{DB::Field(-1), DB::Field("")}},
    };

    auto [anyUnversionedValues, anyUnversionedValuesOwner] = YsonStringBufsToAnyUnversionedValues(ysons);
    auto [ytColumn, ytColumnOwner] = UnversionedValuesToYtColumn(anyUnversionedValues, TColumnSchema(/*name*/ "", logicalType));

    ExpectDataConversion(desctiptor, ysons, expectedFields);
    ExpectDataConversion(desctiptor, anyUnversionedValues, expectedFields);
    ExpectDataConversion(desctiptor, ytColumn, expectedFields);
}

TEST_F(TYTToCHConversionTest, OptionalTupleInt32String)
{
    std::vector<TString> ysonStringsOptionalTupleInt32String = {
        "[42; xyz]",
        "#",
        "",
        "[-1; abc]",
    };
    auto ysonsOptionalTupleInt32String = ToYsonStringBufs(ysonStringsOptionalTupleInt32String);

    auto logicalType = OptionalLogicalType(TupleLogicalType({SimpleLogicalType(ESimpleLogicalValueType::Int32), SimpleLogicalType(ESimpleLogicalValueType::String)}));
    auto expectedDataType = std::make_shared<DB::DataTypeTuple>(std::vector<DB::DataTypePtr>{std::make_shared<DB::DataTypeNumber<i32>>(), std::make_shared<DB::DataTypeString>()});

    TComplexTypeFieldDescriptor descriptor(logicalType);
    ExpectTypeConversion(descriptor, expectedDataType);

    std::vector<DB::Field> expectedFields{
        DB::Tuple{DB::Field(42), DB::Field("xyz")},
        DB::Tuple{DB::Field(0), DB::Field("")},
        DB::Tuple{DB::Field(0), DB::Field("")},
        DB::Tuple{DB::Field(-1), DB::Field("abc")},
    };

    auto [anyUnversionedValues, anyUnversionedValuesOwner] = YsonStringBufsToAnyUnversionedValues(ysonsOptionalTupleInt32String);
    auto [ytColumn, ytColumnOwner] = UnversionedValuesToYtColumn(anyUnversionedValues, TColumnSchema(/*name*/ "", logicalType));

    ExpectDataConversion(descriptor, ysonsOptionalTupleInt32String, expectedFields);
    ExpectDataConversion(descriptor, anyUnversionedValues, expectedFields);
    ExpectDataConversion(descriptor, ytColumn, expectedFields);
}

TEST_F(TYTToCHConversionTest, OptionalStructInt32String)
{
    std::vector<TString> ysonStringsOptionalStructInt32String = {
        "{key=42;value=xyz}",
        "{key=42}",
        "{value=xyz}",
        "{}",
        "",
        "[27; asd]",
        "[27]",
        "[]",
    };
    auto ysonsOptionalStructInt32String = ToYsonStringBufs(ysonStringsOptionalStructInt32String);

    auto logicalType = OptionalLogicalType(StructLogicalType({
        TStructField{.Name = "key", .Type = SimpleLogicalType(ESimpleLogicalValueType::Int32)},
        TStructField{.Name = "value", .Type = SimpleLogicalType(ESimpleLogicalValueType::String)},
    }));

    auto expectedDataType = std::make_shared<DB::DataTypeTuple>(
        std::vector<DB::DataTypePtr>{std::make_shared<DB::DataTypeNumber<i32>>(), std::make_shared<DB::DataTypeString>()},
        std::vector<std::string>{"key", "value"});

    TComplexTypeFieldDescriptor descriptor(logicalType);
    ExpectTypeConversion(descriptor, expectedDataType);

    std::vector<DB::Field> expectedFields{
        DB::Tuple{DB::Field(42), DB::Field("xyz")},
        DB::Tuple{DB::Field(42), DB::Field("")},
        DB::Tuple{DB::Field(0), DB::Field("xyz")},
        DB::Tuple{DB::Field(0), DB::Field("")},
        DB::Tuple{DB::Field(0), DB::Field("")},
        DB::Tuple{DB::Field(27), DB::Field("asd")},
        DB::Tuple{DB::Field(27), DB::Field("")},
        DB::Tuple{DB::Field(0), DB::Field("")},
    };

    auto [anyUnversionedValues, anyUnversionedValuesOwner] = YsonStringBufsToAnyUnversionedValues(ysonsOptionalStructInt32String);
    auto [ytColumn, ytColumnOwner] = UnversionedValuesToYtColumn(anyUnversionedValues, TColumnSchema(/*name*/ "", logicalType));

    ExpectDataConversion(descriptor, ysonsOptionalStructInt32String, expectedFields);
    ExpectDataConversion(descriptor, anyUnversionedValues, expectedFields);
    ExpectDataConversion(descriptor, ytColumn, expectedFields);
}

TEST_F(TYTToCHConversionTest, Decimal)
{
    std::vector<TString> values = {
        "1.2",
        "123456.123",
        "999999.999",
        "-999999.999",
        "0.001",
        "0",
        "-1",
        "1234567.1234",
        "1234567890.12345",
        "123456789012345.12345",
        "12345678901234567890.12345",
        "1234567890123456789012345.12345",
        "123456789012345678901234567890.12345",
        "1234567890123456789012345.1234567890",
        "-123456789012345678901234567890.12345", // 35, 5
        "1234567890123456789012345678901.12345", // 36, 5,
        "-1234567890123456789012345678901234.123", // 37, 3
        "123456789012345678901234567890123.12345", // 38, 5
        "-123456789012345678901234567890123456.123", // 39, 3
        "-1234567890123456789012345678901234567890.321", // 43, 3
        "1234567890123456789012345678901234567890123456789012345678901234567890.321", // 73, 3
        "1234567890123456789012345678901234567890123456789012345678901234567890.12345", // 75, 5
        "1234567890123456789012345678901234567890123456789012345678901234567890123.321", // 76, 3
    };

    for (int precision : {5, 9, 10, 15, 18, 19, 30, 35, 36, 37, 38, 39, 55, 76}) {
        for (int scale : {3, 5, 10}) {
            if (scale >= precision) {
                continue;
            }

            auto logicalType = DecimalLogicalType(precision, scale);
            TComplexTypeFieldDescriptor descriptor(logicalType);

            std::vector<TString> ysonStrings;
            std::vector<DB::Field> expectedFields;

            for (const auto& value : values) {
                if (!IsDecimalRepresentable(value, precision, scale)) {
                    continue;
                }

                TString binary = TDecimal::TextToBinary(value, precision, scale);
                ysonStrings.emplace_back(ConvertToYsonString(binary).ToString());

                auto addDecimalField = [&]<typename DecimalType>(TString value) {
                    auto chValue = std::make_shared<DB::DataTypeDecimal<DecimalType>>(precision, scale)->parseFromString(value);
                    expectedFields.emplace_back(DB::DecimalField(chValue, scale));
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
            YT_VERIFY(ysonStrings.size() > 0);

            auto ysons = ToYsonStringBufs(ysonStrings);
            auto [unversionedValues, unversionedValuesOwner] = YsonStringBufsToVariadicUnversionedValues(ysons);
            auto [ytColumn, ytColumnOwner] = UnversionedValuesToYtColumn(unversionedValues, TColumnSchema(/*name*/ "", logicalType));

            for (const auto& value : unversionedValues) {
                if (value.Type != EValueType::Null) {
                    EXPECT_EQ(value.Type, EValueType::String);
                }
            }

            ExpectDataConversion(descriptor, ysons, expectedFields);
            ExpectDataConversion(descriptor, unversionedValues, expectedFields);
            ExpectDataConversion(descriptor, ytColumn, expectedFields);
        }
    }
}

// TODO(achulkov2, dakovalkov): I do not like the tests below. We are essentially reimplementing the conversion
// with TDecimal::ParseBinaryXX, so what is the point? They should be reimplemented using the same
// native CH-parsing approach as above. There is also a ton of copy and paste that should be
// eliminated.

// Mostly copy-pasted from TestDecimal.
TEST_F(TYTToCHConversionTest, OptionalDecimal)
{
    std::vector<TString> values = {
        "1.2",
        "123456.123",
        "999999.999",
        "-999999.999",
        "0.001",
        "0",
        "#",
        "-1",
        "1234567.1234",
        "1234567890.12345",
        "123456789012345.12345",
        "12345678901234567890.12345",
        "1234567890123456789012345.12345",
        "123456789012345678901234567890.12345",
        "1234567890123456789012345.1234567890",
        "#",
    };

    for (int precision : {5, 9, 10, 15, 18, 19, 30, 35}) {
        for (int scale : {3, 5, 10}) {
            if (scale >= precision) {
                continue;
            }

            auto logicalType = OptionalLogicalType(DecimalLogicalType(precision, scale));
            TComplexTypeFieldDescriptor descriptor(logicalType);

            std::vector<TString> ysonStrings;
            std::vector<DB::Field> expectedFields;

            for (const auto& value : values) {
                if (value == "#") {
                    ysonStrings.emplace_back(value);
                    expectedFields.emplace_back(DB::Null());
                } else {
                    if (!IsDecimalRepresentable(value, precision, scale)) {
                        continue;
                    }

                    TString binary = TDecimal::TextToBinary(value, precision, scale);
                    ysonStrings.emplace_back(ConvertToYsonString(binary).ToString());

                    if (precision <= 9) {
                        auto parsedValue = TDecimal::ParseBinary32(precision, binary);
                        expectedFields.emplace_back(DB::DecimalField(DB::Decimal32(parsedValue), scale));
                    } else if (precision <= 18) {
                        auto parsedValue = TDecimal::ParseBinary64(precision, binary);
                        expectedFields.emplace_back(DB::DecimalField(DB::Decimal64(parsedValue), scale));
                    } else {
                        auto ytValue = TDecimal::ParseBinary128(precision, binary);
                        DB::Decimal128 chValue;
                        std::memcpy(&chValue, &ytValue, sizeof(ytValue));
                        expectedFields.emplace_back(DB::DecimalField(chValue, scale));
                    }
                }
            }
            YT_VERIFY(ysonStrings.size() > 0);

            auto ysons = ToYsonStringBufs(ysonStrings);
            auto [unversionedValues, unversionedValuesOwner] = YsonStringBufsToVariadicUnversionedValues(ysons);
            auto [ytColumn, ytColumnOwner] = UnversionedValuesToYtColumn(unversionedValues, TColumnSchema(/*name*/ "", logicalType));

            ExpectDataConversion(descriptor, ysons, expectedFields);
            ExpectDataConversion(descriptor, unversionedValues, expectedFields);
            ExpectDataConversion(descriptor, ytColumn, expectedFields);
        }
    }
}

// Mostly copy-pasted from TestDecimal.
TEST_F(TYTToCHConversionTest, ListDecimal)
{
    std::vector<TString> values = {
        "1.2",
        "123456.123",
        "999999.999",
        "-999999.999",
        "0.001",
        "0",
        "-1",
        "1234567.1234",
        "1234567890.12345",
        "123456789012345.12345",
        "12345678901234567890.12345",
        "1234567890123456789012345.12345",
        "123456789012345678901234567890.12345",
        "1234567890123456789012345.1234567890",
    };

    for (int precision : {5, 9, 10, 15, 18, 19, 30, 35}) {
        for (int scale : {3, 5, 10}) {
            if (scale >= precision) {
                continue;
            }

            auto logicalType = ListLogicalType(DecimalLogicalType(precision, scale));
            TComplexTypeFieldDescriptor descriptor(logicalType);

            std::vector<TString> ysonStrings;
            std::vector<DB::Field> expectedFields;

            for (const auto& value : values) {
                if (!IsDecimalRepresentable(value, precision, scale)) {
                    continue;
                }

                TString binary = TDecimal::TextToBinary(value, precision, scale);
                std::vector<TString> binaryArray = {binary,};
                ysonStrings.emplace_back(ConvertToYsonString(binaryArray).ToString());

                DB::Array fieldArray;

                if (precision <= 9) {
                    auto parsedValue = TDecimal::ParseBinary32(precision, binary);
                    fieldArray.emplace_back(DB::DecimalField(DB::Decimal32(parsedValue), scale));
                } else if (precision <= 18) {
                    auto parsedValue = TDecimal::ParseBinary64(precision, binary);
                    fieldArray.emplace_back(DB::DecimalField(DB::Decimal64(parsedValue), scale));
                } else {
                    auto ytValue = TDecimal::ParseBinary128(precision, binary);
                    DB::Decimal128 chValue;
                    std::memcpy(&chValue, &ytValue, sizeof(ytValue));
                    fieldArray.emplace_back(DB::DecimalField(chValue, scale));
                }

                expectedFields.emplace_back(std::move(fieldArray));
            }
            YT_VERIFY(ysonStrings.size() > 0);

            auto ysons = ToYsonStringBufs(ysonStrings);
            auto [unversionedValues, unversionedValuesOwner] = YsonStringBufsToVariadicUnversionedValues(ysons);
            auto [ytColumn, ytColumnOwner] = UnversionedValuesToYtColumn(unversionedValues, TColumnSchema(/*name*/ "", logicalType));

            ExpectDataConversion(descriptor, ysons, expectedFields);
            ExpectDataConversion(descriptor, unversionedValues, expectedFields);
            ExpectDataConversion(descriptor, ytColumn, expectedFields);
        }
    }
}

TEST_F(TYTToCHConversionTest, AnyUpcast)
{
    // This is a pretty tricky scenario. Any column may be read from chunks originally
    // from the table having concrete type for this column. In this case we interpret
    // such column as Any, so we must transform concrete values into YSON strings.
    std::vector<TUnversionedValue> intValues = {MakeUnversionedInt64Value(42), MakeUnversionedInt64Value(-17)};
    std::vector<TUnversionedValue> stringValues = {MakeUnversionedStringValue("foo"), MakeUnversionedStringValue("bar")};
    std::vector<TUnversionedValue> doubleValues = {MakeUnversionedDoubleValue(3.14), MakeUnversionedDoubleValue(-2.71)};
    std::vector<TUnversionedValue> booleanValues = {MakeUnversionedBooleanValue(false), MakeUnversionedBooleanValue(true)};
    std::vector<TUnversionedValue> anyValues = {MakeUnversionedAnyValue("{a=1}"), MakeUnversionedAnyValue("[]")};

    Settings_->DefaultYsonFormat = EExtendedYsonFormat::Text;
    TColumnSchema int64ColumnSchema(/*name*/ "", SimpleLogicalType(ESimpleLogicalValueType::Int64));
    TColumnSchema int16ColumnSchema(/*name*/ "", SimpleLogicalType(ESimpleLogicalValueType::Int16));
    TColumnSchema stringColumnSchema(/*name*/ "", SimpleLogicalType(ESimpleLogicalValueType::String));
    TColumnSchema doubleColumnSchema(/*name*/ "", SimpleLogicalType(ESimpleLogicalValueType::Double));
    TColumnSchema booleanColumnSchema(/*name*/ "", SimpleLogicalType(ESimpleLogicalValueType::Boolean));
    TColumnSchema anyColumnSchema(/*name*/ "", SimpleLogicalType(ESimpleLogicalValueType::Any));

    auto [ytInt64Column, ytInt64ColumnOwner] = UnversionedValuesToYtColumn(intValues, int64ColumnSchema);
    auto [ytInt16Column, ytInt16ColumnOwner] = UnversionedValuesToYtColumn(intValues, int16ColumnSchema);
    auto [ytStringColumn, ytStringColumnOwner] = UnversionedValuesToYtColumn(stringValues, stringColumnSchema);
    auto [ytDoubleColumn, ytDoubleColumnOwner] = UnversionedValuesToYtColumn(doubleValues, doubleColumnSchema);
    auto [ytBooleanColumn, ytBooleanColumnOwner] = UnversionedValuesToYtColumn(booleanValues, booleanColumnSchema);
    auto [ytAnyColumn, ytAnyColumnOwner] = UnversionedValuesToYtColumn(anyValues, anyColumnSchema);

    TComplexTypeFieldDescriptor anyDescriptor(anyColumnSchema);

    ExpectTypeConversion(anyDescriptor, std::make_shared<DB::DataTypeString>());

    ExpectDataConversion(anyDescriptor, ytInt64Column, {DB::Field("42"), DB::Field("-17")});
    ExpectDataConversion(anyDescriptor, ytInt16Column, {DB::Field("42"), DB::Field("-17")});
    ExpectDataConversion(anyDescriptor, ytStringColumn, {DB::Field("\"foo\""), DB::Field("\"bar\"")});
    ExpectDataConversion(anyDescriptor, ytDoubleColumn, {DB::Field("3.14"), DB::Field("-2.71")});
    ExpectDataConversion(anyDescriptor, ytBooleanColumn, {DB::Field("%false"), DB::Field("%true")});
    ExpectDataConversion(anyDescriptor, ytAnyColumn, {DB::Field("{\"a\"=1;}"), DB::Field("[]")});
}

TEST_F(TYTToCHConversionTest, IntegerUpcast)
{
    // Similar as previous for integers, e.g. int16 -> int32.
    std::vector<TUnversionedValue> intValues = {MakeUnversionedInt64Value(42), MakeUnversionedInt64Value(-17)};

    Settings_->DefaultYsonFormat = EExtendedYsonFormat::Text;
    TColumnSchema int32ColumnSchema(/*name*/ "", SimpleLogicalType(ESimpleLogicalValueType::Int32));
    TColumnSchema int16ColumnSchema(/*name*/ "", SimpleLogicalType(ESimpleLogicalValueType::Int16));

    auto [ytInt16Column, ytInt16ColumnOwner] = UnversionedValuesToYtColumn(intValues, int16ColumnSchema);

    TComplexTypeFieldDescriptor int32Descriptor(int32ColumnSchema);

    ExpectTypeConversion(int32Descriptor, std::make_shared<DB::DataTypeInt32>());

    ExpectDataConversion<DB::ColumnInt32>(int32Descriptor, ytInt16Column, {DB::Field(42), DB::Field(-17)});
}

TEST_F(TYTToCHConversionTest, ReadOnlyConversions)
{
    std::vector<TColumnSchema> readOnlyColumnSchemas{
        TColumnSchema(
            /*name*/ "",
            OptionalLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Double)))),
        TColumnSchema(
            /*name*/ "",
            OptionalLogicalType(ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Uint8)))),
        TColumnSchema(
            /*name*/ "",
            SimpleLogicalType(ESimpleLogicalValueType::Json)),
        TColumnSchema(
            /*name*/ "",
            DictLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int32)), SimpleLogicalType(ESimpleLogicalValueType::String))),
    };
    for (const auto& columnSchema : readOnlyColumnSchemas) {
        TComplexTypeFieldDescriptor descriptor(columnSchema);
        EXPECT_THROW(TYTToCHColumnConverter(descriptor, Settings_, /*isReadConversions*/ false), std::exception)
            << Format("Conversion of %v did not throw", *columnSchema.LogicalType());
    }
}

TEST_F(TYTToCHConversionTest, FilterHintString)
{
    std::vector<TUnversionedValue> unversionedValues = {
        MakeUnversionedStringValue("foo"),
        MakeUnversionedStringValue("bar"),
        MakeUnversionedStringValue("baaaaaaaz"),
    };

    auto logicalType = SimpleLogicalType(ESimpleLogicalValueType::String);
    TColumnSchema columnSchemaRequired(/*name*/ "", logicalType);
    TColumnSchema columnSchemaOptional(/*name*/ "", OptionalLogicalType(logicalType));
    auto [ytColumnRequired, ytColumnRequiredOwner] = UnversionedValuesToYtColumn(unversionedValues, columnSchemaRequired);
    auto [ytColumnOptional, ytColumnOptionalOwner] = UnversionedValuesToYtColumn(unversionedValues, columnSchemaOptional);

    std::vector<DB::UInt8> filterHint = {1, 0, 1};
    std::vector<DB::Field> expectedFields = {"foo", "", "baaaaaaaz"};

    TComplexTypeFieldDescriptor requiredDescriptor(logicalType);
    TComplexTypeFieldDescriptor optionalDescriptor(OptionalLogicalType(logicalType));
    ExpectDataConversion(requiredDescriptor, ytColumnOptional, filterHint, expectedFields);
    ExpectDataConversion(optionalDescriptor, ytColumnRequired, filterHint, expectedFields);
}

TEST_F(TYTToCHConversionTest, FilterHintList)
{
    std::vector<TUnversionedValue> unversionedValues = {
        MakeUnversionedAnyValue("[1;2;3]"),
        MakeUnversionedAnyValue("[58;124;99]"),
        MakeUnversionedAnyValue("[-1]"),
    };

    auto logicalType = ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int32));
    TColumnSchema columnSchema(/*name*/ "", logicalType);
    auto [ytColumn, ytColumnOwner] = UnversionedValuesToYtColumn(unversionedValues, columnSchema);

    std::vector<DB::UInt8> filterHint = {1, 0, 1};
    std::vector<DB::Field> expectedFields{
        DB::Array{DB::Field(1), DB::Field(2), DB::Field(3)},
        DB::Array{},
        DB::Array{DB::Field(-1)},
    };

    TComplexTypeFieldDescriptor descriptor(logicalType);
    ExpectDataConversion(descriptor, ytColumn, filterHint, expectedFields);
}

TEST_F(TYTToCHConversionTest, FilterHintTuple)
{
    std::vector<TUnversionedValue> unversionedValues = {
        MakeUnversionedAnyValue("[42; xyz]"),
        MakeUnversionedAnyValue("[123; baaaaz]"),
        MakeUnversionedAnyValue("[-1; abc]"),
    };

    auto logicalType = TupleLogicalType({SimpleLogicalType(ESimpleLogicalValueType::Int32), SimpleLogicalType(ESimpleLogicalValueType::String)});
    TColumnSchema columnSchema(/*name*/ "", logicalType);
    auto [ytColumn, ytColumnOwner] = UnversionedValuesToYtColumn(unversionedValues, columnSchema);

    std::vector<DB::UInt8> filterHint = {1, 0, 1};
    std::vector<DB::Field> expectedFields{
        DB::Tuple{DB::Field(42), DB::Field("xyz")},
        DB::Tuple{DB::Field(0), DB::Field("")},
        DB::Tuple{DB::Field(-1), DB::Field("abc")},
    };

    TComplexTypeFieldDescriptor descriptor(logicalType);
    ExpectDataConversion(descriptor, ytColumn, filterHint, expectedFields);
}

////////////////////////////////////////////////////////////////////////////////

class TBenchmarkYTToCHConversion
    : public ::testing::Test
{
public:
    void SetUp() override
    {
        if (GetEnv("SKIP_CHYT_CONVERSION_BENCHMARK_TESTS") != "") {
            GTEST_SKIP();
        }
        Settings_ = New<TCompositeSettings>();
    }
protected:
    TCompositeSettingsPtr Settings_;
};

TEST_F(TBenchmarkYTToCHConversion, TestStringConversionSpeedSmall)
{
    std::vector<TString> ysons;
    for (int i = 0; i < 10000; i++) {
        ysons.push_back("E" + ToString(i));
    }

    auto columnSchema = TColumnSchema(/*name*/ "", SimpleLogicalType(ESimpleLogicalValueType::String));
    TComplexTypeFieldDescriptor descriptor(columnSchema.LogicalType());

    auto anyYsons = ToYsonStringBufs(ysons);
    auto [anyUnversionedValues, anyUnversionedValuesOwner] = YsonStringBufsToAnyUnversionedValues(anyYsons);
    auto [ytColumn, ytColumnOwner] = UnversionedValuesToYtColumn(anyUnversionedValues, columnSchema);

    TYTToCHColumnConverter converter(descriptor, Settings_);
    for (int i = 0; i < 10000; i++) {
        converter.InitColumn();
        converter.ConsumeYtColumn(*ytColumn);
        converter.FlushColumn();
    }
}

TEST_F(TBenchmarkYTToCHConversion, TestStringConversionSpeedMedium)
{
    std::vector<TString> ysons;
    for (int i = 0; i < 10000; i++) {
        ysons.push_back(TString{"E", 14} + ToString(i));
    }

    auto columnSchema = TColumnSchema(/*name*/ "", SimpleLogicalType(ESimpleLogicalValueType::String));
    TComplexTypeFieldDescriptor descriptor(columnSchema.LogicalType());

    auto anyYsons = ToYsonStringBufs(ysons);
    auto [anyUnversionedValues, anyUnversionedValuesOwner] = YsonStringBufsToAnyUnversionedValues(anyYsons);
    auto [ytColumn, ytColumnOwner] = UnversionedValuesToYtColumn(anyUnversionedValues, columnSchema);

    TYTToCHColumnConverter converter(descriptor, Settings_);
    for (int i = 0; i < 10000; i++) {
        converter.InitColumn();
        converter.ConsumeYtColumn(*ytColumn);
        converter.FlushColumn();
    }
}

TEST_F(TBenchmarkYTToCHConversion, TestStringConversionSpeedBig)
{
    std::vector<TString> ysons;
    for (int i = 0; i < 10000; i++) {
        ysons.push_back("E" + ToString(i) + TString(256, 'f'));
    }

    auto columnSchema = TColumnSchema(/*name*/ "", SimpleLogicalType(ESimpleLogicalValueType::String));
    TComplexTypeFieldDescriptor descriptor(columnSchema.LogicalType());

    auto anyYsons = ToYsonStringBufs(ysons);
    auto [anyUnversionedValues, anyUnversionedValuesOwner] = YsonStringBufsToAnyUnversionedValues(anyYsons);
    auto [ytColumn, ytColumnOwner] = UnversionedValuesToYtColumn(anyUnversionedValues, columnSchema);

    TYTToCHColumnConverter converter(descriptor, Settings_);
    for (int i = 0; i < 10000; i++) {
        converter.InitColumn();
        converter.ConsumeYtColumn(*ytColumn);
        converter.FlushColumn();
    }
}

////////////////////////////////////////////////////////////////////////////////

TOwningKeyBound MakeBound(const TUnversionedValues& values, bool isInclusive, bool isUpper)
{
    TUnversionedOwningRowBuilder builder;
    for (const auto& value : values) {
        builder.AddValue(value);
    }

    return TOwningKeyBound::FromRow(builder.FinishRow(), isInclusive, isUpper);
}

TOwningKeyBound MakeLowerBound(const TUnversionedValues& values)
{
    return MakeBound(values, /*isInclusive*/ true, /*isUpper*/ false);
}

TOwningKeyBound MakeUpperBound(const TUnversionedValues& values)
{
    return MakeBound(values, /*isInclusive*/ false, /*isupper*/ true);
}

TTableSchema MakeDummySchema(const DB::DataTypes& types)
{
    std::vector<TColumnSchema> columnSchemas;
    columnSchemas.reserve(types.size());

    for (auto chType : types) {
        bool required = true;
        ESimpleLogicalValueType ytType;

        if (chType->getTypeId() == DB::TypeIndex::Nullable) {
            required = false;
            chType = DB::removeNullable(chType);
        }

        switch (chType->getTypeId()) {
            case DB::TypeIndex::Int64:
                ytType = ESimpleLogicalValueType::Int64;
                break;
            case DB::TypeIndex::UInt64:
                ytType = ESimpleLogicalValueType::Uint64;
                break;
            case DB::TypeIndex::String:
                ytType = ESimpleLogicalValueType::String;
                break;
            default:
                YT_ABORT();
        }

        auto& columnSchema = columnSchemas.emplace_back("dummy_name", ytType);
        columnSchema.SetRequired(required);
    }

    return TTableSchema(std::move(columnSchemas));
}

TEST(TTestKeyConversion, TestBasic)
{
    DB::DataTypes dataTypes = {
        std::make_shared<DB::DataTypeInt64>(),
        std::make_shared<DB::DataTypeString>(),
        std::make_shared<DB::DataTypeUInt64>(),
        std::make_shared<DB::DataTypeInt64>(),
    };
    auto schema = MakeDummySchema(dataTypes);

    auto lowerBound = MakeLowerBound({
        MakeUnversionedInt64Value(1),
        MakeUnversionedStringValue("test"),
        MakeUnversionedUint64Value(2)});
    auto upperBound = MakeUpperBound({
        MakeUnversionedInt64Value(10),
        MakeUnversionedStringValue("test2"),
        MakeUnversionedUint64Value(1)});

    auto chKeys = ToClickHouseKeys(lowerBound, upperBound, schema, dataTypes, 3, false);
    EXPECT_EQ(chKeys.MinKey, std::vector<DB::FieldRef>({1, "test", 2u}));
    EXPECT_EQ(chKeys.MaxKey, std::vector<DB::FieldRef>({10, "test2", 1u}));

    // Convert exclusive to inclusive.
    chKeys = ToClickHouseKeys(lowerBound, upperBound, schema, dataTypes, 3, true);
    EXPECT_EQ(chKeys.MinKey, std::vector<DB::FieldRef>({1, "test", 2u}));
    EXPECT_EQ(chKeys.MaxKey, std::vector<DB::FieldRef>({10, "test2", 0u}));

    // Upper bound is not always exclusive.
    chKeys = ToClickHouseKeys(lowerBound, upperBound, schema, dataTypes, 2, true);
    EXPECT_EQ(chKeys.MinKey, std::vector<DB::FieldRef>({1, "test"}));
    EXPECT_EQ(chKeys.MaxKey, std::vector<DB::FieldRef>({10, "test2"}));

    chKeys = ToClickHouseKeys(lowerBound, upperBound, schema, dataTypes, 1, true);
    EXPECT_EQ(chKeys.MinKey, std::vector<DB::FieldRef>({DB::FieldRef(1)}));
    EXPECT_EQ(chKeys.MaxKey, std::vector<DB::FieldRef>({DB::FieldRef(10)}));

    // Extending key.
    chKeys = ToClickHouseKeys(lowerBound, upperBound, schema, dataTypes, 4, true);
    EXPECT_EQ(chKeys.MinKey, std::vector<DB::FieldRef>({1, "test", 2u, std::numeric_limits<DB::Int64>::min()}));
    EXPECT_EQ(chKeys.MaxKey, std::vector<DB::FieldRef>({10, "test2", 0u, std::numeric_limits<DB::Int64>::max()}));

    chKeys = ToClickHouseKeys(lowerBound, upperBound, schema, dataTypes, 4, false);
    EXPECT_EQ(chKeys.MinKey, std::vector<DB::FieldRef>({1, "test", 2u, std::numeric_limits<DB::Int64>::min()}));
    EXPECT_EQ(chKeys.MaxKey, std::vector<DB::FieldRef>({10, "test2", 1u, std::numeric_limits<DB::Int64>::min()}));
}

TEST(TTestKeyConversion, TestNulls)
{
    DB::DataTypes dataTypes = {
        DB::makeNullable(std::make_shared<DB::DataTypeInt64>()),
        DB::makeNullable(std::make_shared<DB::DataTypeInt64>()),
        DB::makeNullable(std::make_shared<DB::DataTypeUInt64>()),
    };
    auto schema = MakeDummySchema(dataTypes);

    // Different prefix
    {
        auto lowerBound = MakeLowerBound({
            MakeUnversionedInt64Value(1),
            MakeUnversionedNullValue(),
            MakeUnversionedUint64Value(10)});
        auto upperBound = MakeUpperBound({
            MakeUnversionedInt64Value(10),
            MakeUnversionedNullValue(),
            MakeUnversionedUint64Value(22)});

        auto chKeys = ToClickHouseKeys(lowerBound, upperBound, schema, dataTypes, 3, true);
        EXPECT_EQ(chKeys.MinKey, std::vector<DB::FieldRef>({
            1,
            DB::NEGATIVE_INFINITY,
            10u}));
        EXPECT_EQ(chKeys.MaxKey, std::vector<DB::FieldRef>({
            10,
            DB::NEGATIVE_INFINITY,
            21u}));
    }
    // Same prefix
    {
        auto lowerBound = MakeLowerBound({
            MakeUnversionedInt64Value(1),
            MakeUnversionedNullValue(),
            MakeUnversionedUint64Value(10)});
        auto upperBound = MakeUpperBound({
            MakeUnversionedInt64Value(1),
            MakeUnversionedNullValue(),
            MakeUnversionedUint64Value(22)});

        auto chKeys = ToClickHouseKeys(lowerBound, upperBound, schema, dataTypes, 3, true);
        EXPECT_EQ(chKeys.MinKey, std::vector<DB::FieldRef>({
            1,
            DB::NEGATIVE_INFINITY,
            10u}));
        EXPECT_EQ(chKeys.MaxKey, std::vector<DB::FieldRef>({
            1,
            DB::NEGATIVE_INFINITY,
            21u}));
    }
    // Minimum value
    {
        auto lowerBound = MakeLowerBound({
            MakeUnversionedInt64Value(1),
            MakeUnversionedInt64Value(std::numeric_limits<DB::Int64>::min()),
            MakeUnversionedUint64Value(10)});
        auto upperBound = MakeUpperBound({
            MakeUnversionedInt64Value(10),
            MakeUnversionedInt64Value(std::numeric_limits<DB::Int64>::min()),
            MakeUnversionedUint64Value(22)});

        auto chKeys = ToClickHouseKeys(lowerBound, upperBound, schema, dataTypes, 3, true);
        EXPECT_EQ(chKeys.MinKey, std::vector<DB::FieldRef>({
            1,
            std::numeric_limits<DB::Int64>::min(),
            10u}));
        EXPECT_EQ(chKeys.MaxKey, std::vector<DB::FieldRef>({
            10,
            std::numeric_limits<DB::Int64>::min(),
            21u}));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NClickHouseServer
