#include "helpers.h"

#include <yt/core/test_framework/framework.h>

#include <yt/server/clickhouse_server/yt_ch_converter.h>
#include <yt/server/clickhouse_server/config.h>
#include <yt/server/clickhouse_server/helpers.h>

#include <yt/ytlib/table_chunk_format/column_reader.h>
#include <yt/ytlib/table_chunk_format/column_writer.h>
#include <yt/ytlib/table_chunk_format/data_block_writer.h>

#include <yt/ytlib/table_client/helpers.h>

#include <yt/client/table_client/logical_type.h>
#include <yt/client/table_client/helpers.h>

#include <yt/core/yson/string.h>
#include <yt/core/ytree/convert.h>

#include <library/cpp/iterator/functools.h>

#include <Core/Types.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>

namespace NYT::NClickHouseServer {

using namespace NYson;
using namespace NTableClient;
using namespace NYTree;
using namespace NLogging;
using namespace NTableChunkFormat;

static TLogger Logger("Test");

////////////////////////////////////////////////////////////////////////////////

void AppendVector(std::vector<TString>& lhs, std::vector<TString>& rhs)
{
    lhs.insert(lhs.end(), rhs.begin(), rhs.end());
};

void ValidateTypeEquality(const DB::DataTypePtr& lhs, const DB::DataTypePtr& rhs)
{
    ASSERT_NE(lhs, nullptr);
    ASSERT_NE(rhs, nullptr);
    EXPECT_TRUE(lhs->equals(*rhs));
    EXPECT_EQ(lhs->getName(), rhs->getName());
}

///////////////////////////////////////////////////////////////////////////////

//! Empty string stands for ConsumeNulls(1) call.
using TYsonStringBufs = std::vector<TYsonStringBuf>;
using TUnversionedValues = std::vector<TUnversionedValue>;
using TYtColumn = IUnversionedColumnarRowBatch::TColumn*;

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

void DoConsume(TYTCHConverter& converter, TYtColumn ytColumn)
{
    converter.ConsumeYtColumn(*ytColumn);
}

void ExpectFields(const DB::IColumn& column, std::vector<DB::Field> expectedFields)
{
    ASSERT_EQ(expectedFields.size(), column.size());

    for (int index = 0; index < column.size(); ++index) {
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
    virtual void SetUp() override
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
    TStatelessLexer lexer;
    for (const auto& yson : ysons) {
        if (yson) {
            result.emplace_back(MakeUnversionedValue(yson.AsStringBuf(), /* id */ 0, lexer));
        } else {
            result.emplace_back(MakeUnversionedNullValue());
        }
        result.back() = rowBuffer->Capture(result.back());
    }
    return {result, rowBuffer};
}

std::pair<TUnversionedValues, std::any> YsonStringBufsToAnyUnversionedValues(TYsonStringBufs ysons)
{
    auto rowBuffer = New<TRowBuffer>();
    TUnversionedValues result;
    for (const auto& yson : ysons) {
        if (yson) {
            result.emplace_back(rowBuffer->Capture(MakeUnversionedAnyValue(yson.AsStringBuf())));
        } else {
            result.emplace_back(MakeUnversionedNullValue());
        }
    }
    return {result, rowBuffer};
}

std::pair<TYtColumn, std::any> UnversionedValuesToYtColumn(TUnversionedValues values, TColumnSchema columnSchema)
{
    TDataBlockWriter blockWriter;
    auto writer = CreateUnversionedColumnWriter(
        columnSchema,
        /* columnIndex */ 0,
        &blockWriter);

    for (const auto& value : values) {
        TUnversionedRowBuilder builder;
        builder.AddValue(value);
        TUnversionedRow row = builder.GetRow();
        writer->WriteUnversionedValues(MakeRange(&row, 1));
    }

    writer->FinishBlock(/* blockIndex */ 0);
    auto block = blockWriter.DumpBlock(/* blockIndex */ 0, values.size());
    auto meta = writer->ColumnMeta();

    auto reader = CreateUnversionedColumnReader(columnSchema, meta, /* columnIndex */ 0, /* columnId */ 0, /* sortOrder */ std::nullopt);

    struct TTag
    { };

    reader->SetCurrentBlock(MergeRefsToRef<TTag>(block.Data), /* blockIndex */ 0);

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

TEST_F(TTestYTCHConversion, TestAnyPassthrough)
{
    std::vector<TString> ysons = {
        "42",
        "xyz",
        "[42; xyz]",
        "{foo={bar={baz=42}}}",
    };

    auto logicalType = SimpleLogicalType(ESimpleLogicalValueType::Any);
    TComplexTypeFieldDescriptor descriptor(logicalType);

    auto anyYsons = ToYsonStringBufs(ysons);
    auto [anyUnversionedValues, anyUnversionedValuesOwner] = YsonStringBufsToAnyUnversionedValues(anyYsons);
    auto [ytColumn, ytColumnOwner] = UnversionedValuesToYtColumn(anyUnversionedValues, TColumnSchema(/* name */ "", logicalType));


    for (const auto& ysonFormat : TEnumTraits<EYsonFormat>::GetDomainValues()) {
        Settings_->DefaultYsonFormat = ysonFormat;

        ExpectTypeConversion(descriptor, std::make_shared<DB::DataTypeString>());

        std::vector<DB::Field> expectedFields;
        for (const auto& yson : ysons) {
            expectedFields.emplace_back(std::string(ConvertToYsonString(TYsonString(yson), ysonFormat).AsStringBuf()));
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
        TColumnSchema columnSchemaRequired(/* name */ "", logicalType);
        TColumnSchema columnSchemaOptional(/* name */ "", OptionalLogicalType(logicalType));
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
    testAsType(ysonsBool, ESimpleLogicalValueType::Boolean, std::make_shared<DB::DataTypeNumber<DB::UInt8>>(), bool(), DB::UInt8());
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
    std::vector<std::vector<TString>> ysonStringsByNestingLevel = {
        {},
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

        TColumnSchema columnSchema(/* name */ "", logicalType);

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
    auto [ytColumn, ytColumnOwner] = UnversionedValuesToYtColumn(anyUnversionedValues, TColumnSchema(/* name */ "", logicalType));

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
    auto [ytColumn, ytColumnOwner] = UnversionedValuesToYtColumn(anyUnversionedValues, TColumnSchema(/* name */ "", logicalType));

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

    Settings_->DefaultYsonFormat = EYsonFormat::Text;

    TComplexTypeFieldDescriptor descriptor(logicalType);
    ExpectTypeConversion(descriptor, expectedDataType);

    std::vector<DB::Field> expectedFields{
        DB::Array{DB::Field("#"), DB::Field("42"), DB::Field("[]"), DB::Field("[[];[];]"), DB::Field("\"x\"")},
        DB::Array{DB::Field("\"foo\""), DB::Field("23"), DB::Field("\"bar\""), DB::Field("{\"foo\"=\"bar\";}")},
        DB::Array{},
    };

    auto [anyUnversionedValues, anyUnversionedValuesOwner] = YsonStringBufsToAnyUnversionedValues(ysonsListAny);
    auto [ytColumn, ytColumnOwner] = UnversionedValuesToYtColumn(anyUnversionedValues, TColumnSchema(/* name */ "", logicalType));

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
    auto [ytColumn, ytColumnOwner] = UnversionedValuesToYtColumn(anyUnversionedValues, TColumnSchema(/* name */ "", logicalType));

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
    auto [ytColumn, ytColumnOwner] = UnversionedValuesToYtColumn(anyUnversionedValues, TColumnSchema(/* name */ "", logicalType));

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
    auto [ytColumn, ytColumnOwner] = UnversionedValuesToYtColumn(anyUnversionedValues, TColumnSchema(/* name */ "", logicalType));

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
        TStructField{.Type = SimpleLogicalType(ESimpleLogicalValueType::Int32), .Name = "key"},
        TStructField{.Type = SimpleLogicalType(ESimpleLogicalValueType::String), .Name = "value"},
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
    auto [ytColumn, ytColumnOwner] = UnversionedValuesToYtColumn(anyUnversionedValues, TColumnSchema(/* name */ "", logicalType));

    ExpectDataConversion(descriptor, ysonsOptionalStructInt32String, expectedFields);
    ExpectDataConversion(descriptor, anyUnversionedValues, expectedFields);
    ExpectDataConversion(descriptor, ytColumn, expectedFields);
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

    Settings_->DefaultYsonFormat = EYsonFormat::Text;
    TColumnSchema int64ColumnSchema(/* name */ "", SimpleLogicalType(ESimpleLogicalValueType::Int64));
    TColumnSchema int16ColumnSchema(/* name */ "", SimpleLogicalType(ESimpleLogicalValueType::Int16));
    TColumnSchema stringColumnSchema(/* name */ "", SimpleLogicalType(ESimpleLogicalValueType::String));
    TColumnSchema doubleColumnSchema(/* name */ "", SimpleLogicalType(ESimpleLogicalValueType::Double));
    TColumnSchema booleanColumnSchema(/* name */ "", SimpleLogicalType(ESimpleLogicalValueType::Boolean));
    TColumnSchema anyColumnSchema(/* name */ "", SimpleLogicalType(ESimpleLogicalValueType::Any));

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

    Settings_->DefaultYsonFormat = EYsonFormat::Text;
    TColumnSchema int32ColumnSchema(/* name */ "", SimpleLogicalType(ESimpleLogicalValueType::Int32));
    TColumnSchema int16ColumnSchema(/* name */ "", SimpleLogicalType(ESimpleLogicalValueType::Int16));

    auto [ytInt16Column, ytInt16ColumnOwner] = UnversionedValuesToYtColumn(intValues, int16ColumnSchema);

    TComplexTypeFieldDescriptor int32Descriptor(int32ColumnSchema);

    ExpectTypeConversion(int32Descriptor, std::make_shared<DB::DataTypeInt32>());

    ExpectDataConversion<DB::ColumnInt32>(int32Descriptor, ytInt16Column, {DB::Field(42), DB::Field(-17)});
}

TEST_F(TTestYTCHConversion, TestReadOnlyConversions)
{
    std::vector<TColumnSchema> readOnlyColumnSchemas{
        TColumnSchema(
            /* name */ "",
            OptionalLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Double)))),
        TColumnSchema(
            /* name */ "",
            OptionalLogicalType(ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Uint8)))),
        TColumnSchema(
            /* name */ "",
            DictLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int32), SimpleLogicalType(ESimpleLogicalValueType::String))),
    };
    for (const auto& columnSchema : readOnlyColumnSchemas) {
        TComplexTypeFieldDescriptor descriptor(columnSchema);
        EXPECT_THROW(TYTCHConverter(descriptor, Settings_, /* enableReadOnlyConversions */ false), std::exception)
            << Format("Conversion of %v did not throw", *columnSchema.LogicalType());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
