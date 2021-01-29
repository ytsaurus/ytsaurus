#include "helpers.h"

#include <yt/core/test_framework/framework.h>

#include <yt/server/clickhouse_server/ch_yt_converter.h>
#include <yt/server/clickhouse_server/data_type_boolean.h>
#include <yt/server/clickhouse_server/config.h>

#include <yt/ytlib/table_client/helpers.h>

#include <yt/client/table_client/logical_type.h>
#include <yt/client/table_client/helpers.h>

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

#include <library/cpp/iterator/functools.h>

#include <cmath>

namespace NYT::NClickHouseServer {

using namespace NYson;
using namespace NTableClient;
using namespace NYTree;
using namespace NLogging;
using namespace NTableChunkFormat;

static TLogger Logger("Test");

////////////////////////////////////////////////////////////////////////////////

DB::ColumnPtr MakeColumn(DB::DataTypePtr dataType, std::vector<DB::Field> fields)
{
    auto column = dataType->createColumn();
    for (auto& field : fields) {
        column->insert(std::move(field));
    }
    return column;
}

TUnversionedValue MakeUnversionedValue(TStringBuf yson)
{
    // Workaround for use-after-free in MakeUnversionedValue for string values.
    // Works only if there is no escaping in string literal.
    if (yson.starts_with('"') && yson.ends_with('"') && yson.size() >= 2) {
        return MakeUnversionedStringValue(yson.substr(1, yson.size() - 2));
    } else {
        TStatelessLexer lexer;
        return NTableClient::MakeUnversionedValue(yson, /* id */ 0, lexer);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TTestCHYTConversion
    : public ::testing::Test
{
public:
    virtual void SetUp() override
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
        TStatelessLexer lexer;
        for (const auto& yson : expectedValueYsons) {
            expectedValues.emplace_back(MakeUnversionedValue(yson));
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
            actualNodes.emplace_back(ConvertToNode(TYsonStringBuf(TStringBuf(actualValue.Data.String, actualValue.Length))));
        }
        ASSERT_EQ(expectedNodes.size(), actualNodes.size());
        int index = 0;
        for (const auto& [expectedNode, actualNode] : Zip(expectedNodes, actualNodes))
        {
            EXPECT_TRUE(AreNodesEqual(expectedNode, actualNode))
                << "Yson strings define different nodes at index " << index << ":" << std::endl
                << "  expected: " << ConvertToYsonString(expectedNode, EYsonFormat::Text).AsStringBuf() << std::endl
                << "  actual: " << ConvertToYsonString(actualNode, EYsonFormat::Text).AsStringBuf() << std::endl;
            index++;
        }
    }

protected:
    TCompositeSettingsPtr Settings_;
    std::optional<TCHYTConverter> Converter_;
};

TEST_F(TTestCHYTConversion, TestInt16)
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

TEST_F(TTestCHYTConversion, TestBoolean)
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

TEST_F(TTestCHYTConversion, TestFloat32)
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

TEST_F(TTestCHYTConversion, TestString)
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

TEST_F(TTestCHYTConversion, TestNullableInt64)
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

TEST_F(TTestCHYTConversion, TestArrayInt32)
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

TEST_F(TTestCHYTConversion, TestArrayNullableString)
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

TEST_F(TTestCHYTConversion, TestTupleUInt32StringBoolean)
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

TEST_F(TTestCHYTConversion, TestNamedTupleInt8Double)
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
