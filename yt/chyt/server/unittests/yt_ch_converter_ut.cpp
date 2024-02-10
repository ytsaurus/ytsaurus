#include <yt/yt/core/test_framework/framework.h>

#include <yt/chyt/server/config.h>
#include <yt/chyt/server/conversion.h>
#include <yt/chyt/server/data_type_boolean.h>
#include <yt/chyt/server/helpers.h>
#include <yt/chyt/server/yt_ch_converter.h>

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
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
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

///////////////////////////////////////////////////////////////////////////////

//! Empty string stands for ConsumeNulls(1) call.
using TYsonStringBufs = std::vector<TYsonStringBuf>;
using TUnversionedValues = std::vector<TUnversionedValue>;
using TYTColumn = IUnversionedColumnarRowBatch::TColumn*;

void DoConsume(TYTCHConverter& converter, TYsonStringBufs ysons)
{
    for (const auto& yson : ysons) {
        if (!yson) {
            converter.ConsumeNulls(1);
        } else {
            converter.ConsumeYson(yson);
        }
    }
}

void DoConsume(TYTCHConverter& converter, TUnversionedValues values)
{
    converter.ConsumeUnversionedValues(values);
}

void DoConsume(TYTCHConverter& converter, TYTColumn ytColumn)
{
    converter.ConsumeYtColumn(*ytColumn);
}

void ExpectFields(const DB::IColumn& column, std::vector<DB::Field> expectedFields)
{
    ASSERT_EQ(expectedFields.size(), column.size());

    for (int index = 0; index < std::ssize(column); ++index) {
        EXPECT_EQ(expectedFields[index], column[index]) << "Mismatch at position " << index;
        if (!(expectedFields[index] == column[index])) {
            YT_ABORT();
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

class TTestYTCHConversion
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
    void ExpectDataConversion(TComplexTypeFieldDescriptor descriptor, const auto& input, std::vector<DB::Field> expectedFields) const
    {
        TYTCHConverter converter(descriptor, Settings_);
        DoConsume(converter, input);
        auto column = converter.FlushColumn();

        if constexpr (!std::is_void_v<TExpectedColumnType>) {
            EXPECT_NE(dynamic_cast<const TExpectedColumnType*>(column.get()), nullptr);
        }

        ExpectFields(*column, expectedFields);
    }

    void ExpectTypeConversion(TComplexTypeFieldDescriptor descriptor, const DB::DataTypePtr& expectedDataType) const
    {
        TYTCHConverter converter(descriptor, Settings_);
        ValidateTypeEquality(converter.GetDataType(), expectedDataType);
    }
};

///////////////////////////////////////////////////////////////////////////////

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
        &blockWriter);

    for (const auto& value : values) {
        TUnversionedRowBuilder builder;
        builder.AddValue(value);
        TUnversionedRow row = builder.GetRow();
        writer->WriteUnversionedValues(MakeRange(&row, 1));
    }

    writer->FinishBlock(/*blockIndex*/ 0);
    auto block = blockWriter.DumpBlock(/*blockIndex*/ 0, values.size());
    auto meta = writer->ColumnMeta();

    auto reader = CreateUnversionedColumnReader(columnSchema, meta, /*columnIndex*/ 0, /*columnId*/ 0, /*sortOrder*/ std::nullopt);

    struct TTag
    { };

    reader->SetCurrentBlock(MergeRefsToRef<TTag>(block.Data), /*blockIndex*/ 0);

    auto columns = std::make_unique<IUnversionedColumnarRowBatch::TColumn[]>(reader->GetBatchColumnCount());
    reader->ReadColumnarBatch(MakeMutableRange(columns.get(), reader->GetBatchColumnCount()), values.size());

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

///////////////////////////////////////////////////////////////////////////////

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

///////////////////////////////////////////////////////////////////////////////

TEST_F(TTestYTCHConversion, TestAnyPassthrough)
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

TEST_F(TTestYTCHConversion, TestSimpleTypes)
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

    testAsType(ysonsInt8, ESimpleLogicalValueType::Int8, std::make_shared<DB::DataTypeNumber<i8>>(), i8(), i8());
    testAsType(ysonsInt16, ESimpleLogicalValueType::Int16, std::make_shared<DB::DataTypeNumber<i16>>(), i16(), i16());
    testAsType(ysonsInt32, ESimpleLogicalValueType::Int32, std::make_shared<DB::DataTypeNumber<i32>>(), i32(), i32());
    testAsType(ysonsInt64, ESimpleLogicalValueType::Int64, std::make_shared<DB::DataTypeNumber<i64>>(), i64(), i64());
    testAsType(ysonsInt64, ESimpleLogicalValueType::Interval, std::make_shared<DB::DataTypeNumber<i64>>(), i64(), i64());
    testAsType(ysonsUint8, ESimpleLogicalValueType::Uint8, std::make_shared<DB::DataTypeNumber<DB::UInt8>>(), DB::UInt8(), DB::UInt8());
    testAsType(ysonsBool, ESimpleLogicalValueType::Boolean, GetDataTypeBoolean(), bool(), DB::UInt8());
    testAsType(ysonsUint16, ESimpleLogicalValueType::Uint16, std::make_shared<DB::DataTypeNumber<ui16>>(), ui16(), ui16());
    testAsType(ysonsUint32, ESimpleLogicalValueType::Uint32, std::make_shared<DB::DataTypeNumber<ui32>>(), ui32(), ui32());
    testAsType(ysonsUint64, ESimpleLogicalValueType::Uint64, std::make_shared<DB::DataTypeNumber<ui64>>(), ui64(), ui64());
    testAsType(ysonsUint16, ESimpleLogicalValueType::Date, std::make_shared<DB::DataTypeDate>(), ui16(), ui16());
    testAsType(ysonsUint32, ESimpleLogicalValueType::Datetime, std::make_shared<DB::DataTypeDateTime>(), ui32(), ui32());
    testAsType(ysonsUint64, ESimpleLogicalValueType::Timestamp, std::make_shared<DB::DataTypeNumber<ui64>>(), ui64(), ui64());
    testAsType(ysonsString, ESimpleLogicalValueType::String, std::make_shared<DB::DataTypeString>(), TString(), std::string());
    testAsType(ysonsFloat, ESimpleLogicalValueType::Float, std::make_shared<DB::DataTypeNumber<float>>(), double(), float());
    testAsType(ysonsFloat, ESimpleLogicalValueType::Double, std::make_shared<DB::DataTypeNumber<double>>(), double(), double());
}

TEST_F(TTestYTCHConversion, TestOptionalSimpleTypeAsUnversionedValue)
{
    auto intValue = MakeUnversionedInt64Value(42);
    auto nullValue = MakeUnversionedNullValue();

    auto logicalType = OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64));
    TComplexTypeFieldDescriptor descriptor(logicalType);
    TYTCHConverter converter(descriptor, Settings_);

    std::vector<TUnversionedValue> values = {intValue, nullValue};

    converter.ConsumeUnversionedValues(values);

    auto column = converter.FlushColumn();
    ExpectFields(*column, std::vector<DB::Field>{
        DB::Field(42),
        DB::Field(),
    });
}

TEST_F(TTestYTCHConversion, TestOptionalSimpleType)
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

TEST_F(TTestYTCHConversion, TestNullAndVoid)
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

TEST_F(TTestYTCHConversion, TestListInt32)
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

TEST_F(TTestYTCHConversion, TestListListInt32)
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

TEST_F(TTestYTCHConversion, TestListAny)
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

TEST_F(TTestYTCHConversion, TestOptionalListOptionalInt32)
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

TEST_F(TTestYTCHConversion, TestDictIntString)
{
    std::vector<TString> ysonStringsDictIntString = {
        "[[42; foo]; [27; bar]]",
        "[]",
        "[[-1; \"\"]]",
    };
    auto ysonsDictIntString = ToYsonStringBufs(ysonStringsDictIntString);

    auto logicalType = DictLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int32), SimpleLogicalType(ESimpleLogicalValueType::String));
    auto expectedDataType = std::make_shared<DB::DataTypeArray>(std::make_shared<DB::DataTypeTuple>(
        std::vector<DB::DataTypePtr>{std::make_shared<DB::DataTypeNumber<i32>>(), std::make_shared<DB::DataTypeString>()},
        std::vector<std::string>{"key", "value"}
    ));

    TComplexTypeFieldDescriptor descriptor(logicalType);
    ExpectTypeConversion(descriptor, expectedDataType);

    std::vector<DB::Field> expectedFields{
        DB::Array{DB::Tuple{DB::Field(42), DB::Field("foo")}, DB::Tuple{DB::Field(27), DB::Field("bar")}},
        DB::Array{},
        DB::Array{DB::Tuple{DB::Field(-1), DB::Field("")}},
    };

    auto [anyUnversionedValues, anyUnversionedValuesOwner] = YsonStringBufsToAnyUnversionedValues(ysonsDictIntString);
    auto [ytColumn, ytColumnOwner] = UnversionedValuesToYtColumn(anyUnversionedValues, TColumnSchema(/*name*/ "", logicalType));

    ExpectDataConversion(descriptor, ysonsDictIntString, expectedFields);
    ExpectDataConversion(descriptor, anyUnversionedValues, expectedFields);
    ExpectDataConversion(descriptor, ytColumn, expectedFields);
}

TEST_F(TTestYTCHConversion, TestOptionalTupleInt32String)
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

TEST_F(TTestYTCHConversion, TestOptionalStructInt32String)
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

TEST_F(TTestYTCHConversion, TestDecimal)
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

// Mostly copy-pasted from TestDecimal.
TEST_F(TTestYTCHConversion, TestOptionalDecimal)
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
TEST_F(TTestYTCHConversion, TestListDecimal)
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

TEST_F(TTestYTCHConversion, TestAnyUpcast)
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

TEST_F(TTestYTCHConversion, TestIntegerUpcast)
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

TEST_F(TTestYTCHConversion, TestReadOnlyConversions)
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
            DictLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int32), SimpleLogicalType(ESimpleLogicalValueType::String))),
    };
    for (const auto& columnSchema : readOnlyColumnSchemas) {
        TComplexTypeFieldDescriptor descriptor(columnSchema);
        EXPECT_THROW(TYTCHConverter(descriptor, Settings_, /*enableReadOnlyConversions*/ false), std::exception)
            << Format("Conversion of %v did not throw", *columnSchema.LogicalType());
    }
}

////////////////////////////////////////////////////////////////////////////////

class TBenchmarkYTCHConversion
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

TEST_F(TBenchmarkYTCHConversion, TestStringConversionSpeedSmall)
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

    for (int i = 0; i < 10000; i++) {
        TYTCHConverter converter(descriptor, Settings_);
        converter.ConsumeYtColumn(*ytColumn);
        converter.FlushColumn();
    }
}

TEST_F(TBenchmarkYTCHConversion, TestStringConversionSpeedMedium)
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

    for (int i = 0; i < 10000; i++) {
        TYTCHConverter converter(descriptor, Settings_);
        converter.ConsumeYtColumn(*ytColumn);
        converter.FlushColumn();
    }
}

TEST_F(TBenchmarkYTCHConversion, TestStringConversionSpeedBig)
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

    for (int i = 0; i < 10000; i++) {
        TYTCHConverter converter(descriptor, Settings_);
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
