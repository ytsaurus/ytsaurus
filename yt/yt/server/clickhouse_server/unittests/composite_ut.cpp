#include <yt/core/test_framework/framework.h>

#include <yt/server/clickhouse_server/composite.h>
#include <yt/server/clickhouse_server/config.h>
#include <yt/server/clickhouse_server/helpers.h>

#include <yt/core/yson/string.h>
#include <yt/core/ytree/convert.h>

#include <yt/client/table_client/logical_type.h>

#include <library/cpp/iterator/functools.h>

#include <Core/Types.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Columns/IColumn.h>
#include <Core/Field.h>

namespace NYT::NClickHouseServer {

using namespace NYson;
using namespace NTableClient;
using namespace NYTree;
using namespace NLogging;

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

////////////////////////////////////////////////////////////////////////////////

class TTestComposites
    : public ::testing::Test
{
public:
    virtual void SetUp() override
    {
        Settings_ = New<TCompositeSettings>();
        Settings_->EnableConversion = true;
    }

protected:
    TCompositeSettingsPtr Settings_;
};

void ExpectFields(const DB::IColumn& column, std::vector<DB::Field> expectedFields)
{
    ASSERT_EQ(expectedFields.size(), column.size());

    for (int index = 0; index < column.size(); ++index) {
        EXPECT_EQ(expectedFields[index], column[index]) << "Mismatch at position " << index;
    }
}

TEST_F(TTestComposites, TestAnyPassthrough)
{
    std::vector<TString> ysons = {
        "42",
        "xyz",
        "[42; xyz]",
        "{foo={bar={baz=42}}}",
    };

    auto settings = CloneYsonSerializable(Settings_);

    for (const auto& ysonFormat : TEnumTraits<EYsonFormat>::GetDomainValues()) {
        settings->DefaultYsonFormat = ysonFormat;
        TComplexTypeFieldDescriptor descriptor(SimpleLogicalType(ESimpleLogicalValueType::Any));
        TCompositeValueToClickHouseColumnConverter converter(descriptor, settings);
        ValidateTypeEquality(converter.GetDataType(), std::make_shared<DB::DataTypeString>());

        for (const auto& yson : ysons) {
            converter.ConsumeYson(yson);
        }

        auto column = converter.FlushColumn();

        std::vector<DB::Field> expectedFields;
        for (const auto& yson : ysons) {
            expectedFields.emplace_back(std::string(ConvertToYsonString(TYsonString(yson), ysonFormat).GetData()));
        }

        ExpectFields(*column, expectedFields);
    }
}

TEST_F(TTestComposites, TestSimpleTypes)
{
    std::vector<TString> ysonsInt8 = {
        "42",
        "-1",
        "-128",
        "127",
    };
    std::vector<TString> ysonsInt16 = {
        ToString(std::numeric_limits<i16>::min()),
        ToString(std::numeric_limits<i16>::max()),
    };
    AppendVector(ysonsInt16, ysonsInt8);
    std::vector<TString> ysonsInt32 = {
        ToString(std::numeric_limits<i32>::min()),
        ToString(std::numeric_limits<i32>::max()),
    };
    AppendVector(ysonsInt32, ysonsInt16);
    std::vector<TString> ysonsInt64 = {
        ToString(std::numeric_limits<i64>::min()),
        ToString(std::numeric_limits<i64>::max()),
    };
    AppendVector(ysonsInt64, ysonsInt32);

    std::vector<TString> ysonsUint8 = {
        "42u",
        "255u",
        "0u",
    };
    std::vector<TString> ysonsUint16 = {
        ToString(std::numeric_limits<ui16>::max()) + "u",
    };
    AppendVector(ysonsUint16, ysonsUint8);
    std::vector<TString> ysonsUint32 = {
        ToString(std::numeric_limits<ui32>::max()) + "u",
    };
    AppendVector(ysonsUint32, ysonsUint16);
    std::vector<TString> ysonsUint64 = {
        ToString(std::numeric_limits<ui64>::max()) + "u",
    };
    AppendVector(ysonsUint64, ysonsUint32);

    std::vector<TString> ysonsBool = {
        "%false",
        "%true",
    };

    std::vector<TString> ysonsString = {
        "\"foo\"",
        "\"\"",
    };

    std::vector<TString> ysonsFloat = {
        "3.14",
        "-2.718",
        "%nan ",
        "%inf ",
        "%-inf ",
        "0.0",
    };

    std::vector<TString> ysonsNothing = {
        "#",
    };

    // Dummy variables are replacing template class parameters which are currently not available for lambdas.
    auto testAsType = [&] (const std::vector<TString> ysons, ESimpleLogicalValueType simpleLogicalValueType, DB::DataTypePtr expectedDataType, auto ytTypeDummy, auto chTypeDummy)
    {
        YT_LOG_TRACE("Running tests (Type: %v)", simpleLogicalValueType);

        using TYtType = decltype(ytTypeDummy);
        using TChType = decltype(chTypeDummy);

        std::vector<DB::Field> expectedFields;
        if (simpleLogicalValueType != ESimpleLogicalValueType::Void && simpleLogicalValueType != ESimpleLogicalValueType::Null) {
            for (const auto& yson : ysons) {
                expectedFields.emplace_back(TChType(ConvertTo<TYtType>(TYsonString(yson))));
            }
        }

        TComplexTypeFieldDescriptor descriptor(SimpleLogicalType(simpleLogicalValueType));
        TCompositeValueToClickHouseColumnConverter converter(descriptor, Settings_);
        ValidateTypeEquality(converter.GetDataType(), expectedDataType);
        for (const auto& yson : ysons) {
            converter.ConsumeYson(yson);
        }
        auto column = converter.FlushColumn();
        if (simpleLogicalValueType != ESimpleLogicalValueType::Void && simpleLogicalValueType != ESimpleLogicalValueType::Null) {
            ExpectFields(*column, expectedFields);
        } else {
            EXPECT_EQ(1, column->size());
        }
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
    testAsType(ysonsNothing, ESimpleLogicalValueType::Void, std::make_shared<DB::DataTypeString>(), /* unused */ int(), /* unused */ int());
    testAsType(ysonsNothing, ESimpleLogicalValueType::Null, std::make_shared<DB::DataTypeString>(), /* unused */ int(), /* unused */ int());
}

TEST_F(TTestComposites, TestOptionalSimpleType)
{
    std::vector<std::vector<TString>> ysonsByNestingLevel = {
        {},
        {/* #, */ "#", "42"},
        {/* #, */ "#", "[#]", "[42]"},
        {/* #, */ "#", "[#]", "[[#]]", "[[42]]"},
    };

    auto logicalType = SimpleLogicalType(ESimpleLogicalValueType::Int64);
    auto expectedDataType = DB::makeNullable(std::make_shared<DB::DataTypeNumber<i64>>());

    for (int nestingLevel = 1; nestingLevel <= 3; ++nestingLevel) {
        logicalType = OptionalLogicalType(std::move(logicalType));

        YT_LOG_TRACE(
            "Running tests (NestingLevel: %v)",
            nestingLevel);
        TComplexTypeFieldDescriptor descriptor(logicalType);
        TCompositeValueToClickHouseColumnConverter converter(descriptor, Settings_);
        ValidateTypeEquality(converter.GetDataType(), expectedDataType);

        if (nestingLevel > 0) {
            converter.ConsumeNull();
        }
        for (const auto& yson : ysonsByNestingLevel[nestingLevel]) {
            converter.ConsumeYson(yson);
        }

        auto column = converter.FlushColumn();
        for (int index = 0; index <= nestingLevel; ++index) {
            EXPECT_EQ(DB::Field(), (*column)[index]);
        }
        EXPECT_EQ(DB::Field(42), (*column)[nestingLevel + 1]);
    }
}

TEST_F(TTestComposites, TestListInt32)
{
    std::vector<TString> ysonsListInt32 = {
        "[42;57]",
        "[]",
        "[58;124;99;]",
        // #
        "[-1]",
    };

    auto logicalType = ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int32));
    auto expectedDataType = std::make_shared<DB::DataTypeArray>(std::make_shared<DB::DataTypeNumber<i32>>());

    TComplexTypeFieldDescriptor descriptor(logicalType);
    TCompositeValueToClickHouseColumnConverter converter(descriptor, Settings_);
    ValidateTypeEquality(converter.GetDataType(), expectedDataType);

    for (const auto& [index, yson] : Enumerate(ysonsListInt32)) {
        if (index == 3) {
            converter.ConsumeNull();
        }
        converter.ConsumeYson(yson);
    }
    auto column = converter.FlushColumn();
    ExpectFields(*column, std::vector<DB::Field>{
        DB::Array{DB::Field(42), DB::Field(57)},
        DB::Array{},
        DB::Array{DB::Field(58), DB::Field(124), DB::Field(99)},
        DB::Array{},
        DB::Array{DB::Field(-1)},
    });
}

TEST_F(TTestComposites, TestListListInt32)
{
    std::vector<TString> ysonsListListInt32 = {
        "[[42;57];[-1]]",
        "[]",
        "[[];]",
        // #
        "[[];[];[1;2;]]",
    };

    auto logicalType = ListLogicalType(ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int32)));
    auto expectedDataType = std::make_shared<DB::DataTypeArray>(std::make_shared<DB::DataTypeArray>(std::make_shared<DB::DataTypeNumber<i32>>()));

    TComplexTypeFieldDescriptor descriptor(logicalType);
    TCompositeValueToClickHouseColumnConverter converter(descriptor, Settings_);
    ValidateTypeEquality(converter.GetDataType(), expectedDataType);

    for (const auto& [index, yson] : Enumerate(ysonsListListInt32)) {
        if (index == 3) {
            converter.ConsumeNull();
        }
        converter.ConsumeYson(yson);
    }
    auto column = converter.FlushColumn();
    ExpectFields(*column, std::vector<DB::Field>{
        DB::Array{DB::Array{DB::Field(42), DB::Field(57)}, DB::Array{-1}},
        DB::Array{},
        DB::Array{DB::Field(DB::Array{})}, // Without explicit DB::Field, compiler thinks that it is a copy-constructor.
        DB::Array{},
        DB::Array{DB::Array{}, DB::Array{}, DB::Array{DB::Field(1), DB::Field(2)}},
    });
}

TEST_F(TTestComposites, TestListAny)
{
    std::vector<TString> ysonsListAny = {
        "[#; 42; []; [[];[]]; x]",
        "[foo; 23; bar; {foo=bar}]",
        "[]",
    };

    auto logicalType = ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Any));
    auto expectedDataType = std::make_shared<DB::DataTypeArray>(std::make_shared<DB::DataTypeString>());

    auto settings = CloneYsonSerializable(Settings_);
    settings->DefaultYsonFormat = EYsonFormat::Text;

    TComplexTypeFieldDescriptor descriptor(logicalType);
    TCompositeValueToClickHouseColumnConverter converter(descriptor, settings);
    ValidateTypeEquality(converter.GetDataType(), expectedDataType);

    for (const auto& [index, yson] : Enumerate(ysonsListAny)) {
        converter.ConsumeYson(yson);
    }
    auto column = converter.FlushColumn();
    ExpectFields(*column, std::vector<DB::Field>{
        DB::Array{DB::Field("#"), DB::Field("42"), DB::Field("[]"), DB::Field("[[];[];]"), DB::Field("\"x\"")},
        DB::Array{DB::Field("\"foo\""), DB::Field("23"), DB::Field("\"bar\""), DB::Field("{\"foo\"=\"bar\";}")},
        DB::Array{},
    });
}

TEST_F(TTestComposites, TestOptionalListOptionalInt32)
{
    std::vector<TString> ysonsOptionalListOptionalInt32 = {
        "[#]",
        // #
        "[]",
        "[42]",
        "[#;58;12;#]",
    };

    auto logicalType = OptionalLogicalType(ListLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int32))));
    auto expectedDataType = std::make_shared<DB::DataTypeArray>(std::make_shared<DB::DataTypeNullable>(std::make_shared<DB::DataTypeNumber<i32>>()));

    TComplexTypeFieldDescriptor descriptor(logicalType);
    TCompositeValueToClickHouseColumnConverter converter(descriptor, Settings_);
    ValidateTypeEquality(converter.GetDataType(), expectedDataType);

    for (const auto& [index, yson] : Enumerate(ysonsOptionalListOptionalInt32)) {
        if (index == 1) {
            converter.ConsumeNull();
        }
        converter.ConsumeYson(yson);
    }
    auto column = converter.FlushColumn();
    ExpectFields(*column, std::vector<DB::Field>{
        DB::Array{DB::Field()},
        DB::Array{},
        DB::Array{},
        DB::Array{DB::Field(42)},
        DB::Array{DB::Field(), DB::Field(58), DB::Field(12), DB::Field()},
    });
}

TEST_F(TTestComposites, TestDictIntString)
{
    std::vector<TString> ysonDicts = {
        "[[42; foo]; [27; bar]]",
        "[]",
        "[[-1; \"\"]]",
    };

    auto logicalType = DictLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int32), SimpleLogicalType(ESimpleLogicalValueType::String));
    auto expectedDataType = std::make_shared<DB::DataTypeArray>(std::make_shared<DB::DataTypeTuple>(
        std::vector<DB::DataTypePtr>{std::make_shared<DB::DataTypeNumber<i32>>(), std::make_shared<DB::DataTypeString>()},
        std::vector<std::string>{"key", "value"}
    ));

    TComplexTypeFieldDescriptor descriptor(logicalType);
    TCompositeValueToClickHouseColumnConverter converter(descriptor, Settings_);
    ValidateTypeEquality(converter.GetDataType(), expectedDataType);

    for (const auto& yson : ysonDicts) {
        converter.ConsumeYson(yson);
    }
    auto column = converter.FlushColumn();
    ExpectFields(*column, std::vector<DB::Field>{
        DB::Array{DB::Tuple{DB::Field(42), DB::Field("foo")}, DB::Tuple{DB::Field(27), DB::Field("bar")}},
        DB::Array{},
        DB::Array{DB::Tuple{DB::Field(-1), DB::Field("")}},
    });
}

TEST_F(TTestComposites, TestOptionalTupleInt32String)
{
    std::vector<TString> ysonsOptionalTupleInt32String = {
        "[42; xyz]",
        "#",
        // #
        "[-1; abc]",
    };

    auto logicalType = OptionalLogicalType(TupleLogicalType({SimpleLogicalType(ESimpleLogicalValueType::Int32), SimpleLogicalType(ESimpleLogicalValueType::String)}));
    auto expectedDataType = std::make_shared<DB::DataTypeTuple>(std::vector<DB::DataTypePtr>{std::make_shared<DB::DataTypeNumber<i32>>(), std::make_shared<DB::DataTypeString>()});

    TComplexTypeFieldDescriptor descriptor(logicalType);
    TCompositeValueToClickHouseColumnConverter converter(descriptor, Settings_);
    ValidateTypeEquality(converter.GetDataType(), expectedDataType);

    for (const auto& [index, yson] : Enumerate(ysonsOptionalTupleInt32String)) {
        if (index == 2) {
            converter.ConsumeNull();
        }
        converter.ConsumeYson(yson);
    }
    auto column = converter.FlushColumn();
    ExpectFields(*column, std::vector<DB::Field>{
        DB::Tuple{DB::Field(42), DB::Field("xyz")},
        DB::Tuple{DB::Field(0), DB::Field("")},
        DB::Tuple{DB::Field(0), DB::Field("")},
        DB::Tuple{DB::Field(-1), DB::Field("abc")},
    });
}

TEST_F(TTestComposites, TestOptionalStructInt32String)
{
    std::vector<TString> ysonsOptionalStructInt32String = {
        "{key=42;value=xyz}",
        "{key=42}",
        "{value=xyz}",
        "{}",
        // #
        "[27; asd]",
        "[27]",
        "[]",
    };

    auto logicalType = OptionalLogicalType(StructLogicalType({
        TStructField{.Type = SimpleLogicalType(ESimpleLogicalValueType::Int32), .Name = "key"},
        TStructField{.Type = SimpleLogicalType(ESimpleLogicalValueType::String), .Name = "value"},
    }));

    auto expectedDataType = std::make_shared<DB::DataTypeTuple>(
        std::vector<DB::DataTypePtr>{std::make_shared<DB::DataTypeNumber<i32>>(), std::make_shared<DB::DataTypeString>()},
        std::vector<std::string>{"key", "value"});

    TComplexTypeFieldDescriptor descriptor(logicalType);
    TCompositeValueToClickHouseColumnConverter converter(descriptor, Settings_);
    ValidateTypeEquality(converter.GetDataType(), expectedDataType);

    for (const auto& [index, yson] : Enumerate(ysonsOptionalStructInt32String)) {
        if (index == 4) {
            converter.ConsumeNull();
        }
        converter.ConsumeYson(yson);
    }
    auto column = converter.FlushColumn();
    ExpectFields(*column, std::vector<DB::Field>{
        DB::Tuple{DB::Field(42), DB::Field("xyz")},
        DB::Tuple{DB::Field(42), DB::Field("")},
        DB::Tuple{DB::Field(0), DB::Field("xyz")},
        DB::Tuple{DB::Field(0), DB::Field("")},
        DB::Tuple{DB::Field(0), DB::Field("")},
        DB::Tuple{DB::Field(27), DB::Field("asd")},
        DB::Tuple{DB::Field(27), DB::Field("")},
        DB::Tuple{DB::Field(0), DB::Field("")},
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
