
#include <yt/core/test_framework/framework.h>

#include <yt/client/table_client/logical_type.h>
#include <yt/client/formats/skiff_yson_converter.h>

#include <yt/core/yson/pull_parser.h>
#include <yt/core/skiff/skiff.h>
#include <yt/core/skiff/skiff_schema.h>

#include <util/string/hex.h>

namespace NYT::NFormats {
namespace {

using namespace NTableClient;
using namespace NSkiff;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TSkiffSchemaPtr SkiffOptional(TSkiffSchemaPtr skiffSchema)
{
    return CreateVariant8Schema({
        CreateSimpleTypeSchema(EWireType::Nothing),
        std::move(skiffSchema)
    });
}

TString ConvertYsonHex(
    const TLogicalTypePtr& logicalType,
    const TSkiffSchemaPtr& skiffSchema,
    TStringBuf ysonString,
    const TYsonToSkiffConverterConfig& config = {})
{
    auto converter = CreateYsonToSkiffConverter(
        TComplexTypeFieldDescriptor("test-field", logicalType),
        skiffSchema,
        config);

    // Yson parsers have a bug when they can't parse some values that end unexpectedly.
    TString spacedYsonInput = ysonString.ToString() + " ";

    TStringStream out;
    {
        TCheckedInDebugSkiffWriter writer(skiffSchema, &out);

        TMemoryInput in(spacedYsonInput);
        TYsonPullParser pullParser(&in, EYsonType::Node);
        TYsonPullParserCursor cursor(&pullParser);

        converter(&cursor, &writer);

        EXPECT_EQ(cursor.GetCurrent().GetType(), EYsonItemType::EndOfStream);
        writer.Finish();
    }

    auto result = HexEncode(out.Str());
    result.to_lower();
    return result;
}

TString ConvertHexToTextYson(
    const TLogicalTypePtr& logicalType,
    const TSkiffSchemaPtr& skiffSchema,
    TStringBuf hexString,
    const TSkiffToYsonConverterConfig& config = {})
{
    auto converter = CreateSkiffToYsonConverter(TComplexTypeFieldDescriptor("test-field", logicalType), skiffSchema, config);

    TStringStream out;
    {
        TString binaryString = HexDecode(hexString);
        TMemoryInput in(binaryString);
        TCheckedInDebugSkiffParser parser(skiffSchema, &in);

        auto writer = TYsonWriter(&out, EYsonFormat::Text);
        converter(&parser, &writer);
        EXPECT_EQ(parser.GetReadBytesCount(), binaryString.size());
    }

    return out.Str();
}


#define CHECK_BIDIRECTIONAL_CONVERSION(logicalType, skiffSchema, ysonString, skiffString, ...) \
    do { \
        std::tuple<TYsonToSkiffConverterConfig,TSkiffToYsonConverterConfig> cfg = {__VA_ARGS__}; \
        auto actualSkiffString = ConvertYsonHex(logicalType, skiffSchema, ysonString, std::get<0>(cfg)); \
        EXPECT_EQ(actualSkiffString, skiffString) << "Yson -> Skiff conversion error"; \
        auto actualYsonString = ConvertHexToTextYson(logicalType, skiffSchema, skiffString, std::get<1>(cfg)); \
        EXPECT_EQ(actualYsonString, ysonString) << "Skiff -> Yson conversion error"; \
    } while (0)

#define CHECK_EXCEPTION_SUBSTR(expr, pattern) \
do { \
    try { \
        expr; \
        ADD_FAILURE() << "Expected to throw"; \
    } catch (const std::exception& ex) { \
        EXPECT_THAT(ex.what(), testing::HasSubstr(pattern)); \
    } \
} while (0)


TEST(TYsonSkiffConverterTest, TestSimpleTypes)
{
    CHECK_BIDIRECTIONAL_CONVERSION(
        SimpleLogicalType(ESimpleLogicalValueType::Int64),
        CreateSimpleTypeSchema(EWireType::Int64),
        "-42",
        "d6ffffff" "ffffffff");

    CHECK_BIDIRECTIONAL_CONVERSION(
        SimpleLogicalType(ESimpleLogicalValueType::Uint64),
        CreateSimpleTypeSchema(EWireType::Uint64),
        "42u",
        "2a000000" "00000000");

    CHECK_BIDIRECTIONAL_CONVERSION(
        SimpleLogicalType(ESimpleLogicalValueType::Uint64),
        CreateSimpleTypeSchema(EWireType::Uint64),
        "8u",
        "08000000" "00000000");

    CHECK_BIDIRECTIONAL_CONVERSION(
        SimpleLogicalType(ESimpleLogicalValueType::Boolean),
        CreateSimpleTypeSchema(EWireType::Boolean),
        "%true",
        "01");

    CHECK_BIDIRECTIONAL_CONVERSION(
        SimpleLogicalType(ESimpleLogicalValueType::Double),
        CreateSimpleTypeSchema(EWireType::Double),
        "0.",
        "00000000" "00000000");

    CHECK_BIDIRECTIONAL_CONVERSION(
        SimpleLogicalType(ESimpleLogicalValueType::String),
        CreateSimpleTypeSchema(EWireType::String32),
        "\"foo\"",
        "03000000" "666f6f");
}

TEST(TYsonSkiffConverterTest, TestYson32)
{
    CHECK_BIDIRECTIONAL_CONVERSION(
        SimpleLogicalType(ESimpleLogicalValueType::Any),
        CreateSimpleTypeSchema(EWireType::Yson32),
        "-42",
        "02000000" "0253");

    CHECK_BIDIRECTIONAL_CONVERSION(
        SimpleLogicalType(ESimpleLogicalValueType::Any),
        CreateSimpleTypeSchema(EWireType::Yson32),
        "#",
        "01000000" "23");

    CHECK_BIDIRECTIONAL_CONVERSION(
        SimpleLogicalType(ESimpleLogicalValueType::Any),
        CreateSimpleTypeSchema(EWireType::Yson32),
        "[1;2;[3;];]",
        "0e000000" "5b02023b02043b5b02063b5d3b5d");
}

TEST(TYsonSkiffConverterTest, TestOptionalTypes)
{
    CHECK_BIDIRECTIONAL_CONVERSION(
        SimpleLogicalType(ESimpleLogicalValueType::Int64, /*required*/ false),
        SkiffOptional(CreateSimpleTypeSchema(EWireType::Int64)),
        "-42",
        "01" "d6ffffff" "ffffffff");

    CHECK_BIDIRECTIONAL_CONVERSION(
        SimpleLogicalType(ESimpleLogicalValueType::Int64, /*required*/ false),
        SkiffOptional(CreateSimpleTypeSchema(EWireType::Int64)),
        "#",
        "00");

    CHECK_BIDIRECTIONAL_CONVERSION(
        OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Boolean, /*required*/ false)),
        SkiffOptional(SkiffOptional(CreateSimpleTypeSchema(EWireType::Boolean))),
        "[%true;]",
        "01" "01" "01");

    CHECK_BIDIRECTIONAL_CONVERSION(
        OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Boolean, /*required*/ false)),
        SkiffOptional(SkiffOptional(CreateSimpleTypeSchema(EWireType::Boolean))),
        "[#;]",
        "01" "00");

    CHECK_BIDIRECTIONAL_CONVERSION(
        OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Boolean, /*required*/ false)),
        SkiffOptional(SkiffOptional(CreateSimpleTypeSchema(EWireType::Boolean))),
        "#",
        "00");

    CHECK_BIDIRECTIONAL_CONVERSION(
        OptionalLogicalType(ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Boolean))),
        SkiffOptional(CreateRepeatedVariant8Schema({CreateSimpleTypeSchema(EWireType::Boolean)})),
        "#",
        "00");

    CHECK_BIDIRECTIONAL_CONVERSION(
        OptionalLogicalType(
            OptionalLogicalType(
                ListLogicalType(
                    SimpleLogicalType(ESimpleLogicalValueType::Boolean)
                )
            )
        ),
        SkiffOptional(
            SkiffOptional(
                CreateRepeatedVariant8Schema({
                    CreateSimpleTypeSchema(EWireType::Boolean)
                })
            )
        ),
        "[[%true;%false;%true;];]",
        "01" "01" "0001" "0000" "0001" "ff");

    CHECK_BIDIRECTIONAL_CONVERSION(
        OptionalLogicalType(
            OptionalLogicalType(
                ListLogicalType(
                    SimpleLogicalType(ESimpleLogicalValueType::Boolean)
                )
            )
        ),
        SkiffOptional(
            SkiffOptional(
                CreateRepeatedVariant8Schema({
                    CreateSimpleTypeSchema(EWireType::Boolean)
                })
            )
        ),
        "[#;]",
        "0100");

    CHECK_EXCEPTION_SUBSTR(
        ConvertYsonHex(
            OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Boolean, /*required*/ false)),
            SkiffOptional(CreateSimpleTypeSchema(EWireType::Boolean)),
            " [ %true ] "),
        "Optional nesting mismatch");

    CHECK_EXCEPTION_SUBSTR(
        ConvertHexToTextYson(
            SimpleLogicalType(ESimpleLogicalValueType::Boolean, /*required*/ false),
            CreateSimpleTypeSchema(EWireType::Boolean),
            "00"),
        "Optional nesting mismatch");

    TYsonToSkiffConverterConfig ysonToSkiffConfig;
    ysonToSkiffConfig.AllowOmitTopLevelOptional = true;

    TSkiffToYsonConverterConfig skiffToYsonConfig;
    skiffToYsonConfig.AllowOmitTopLevelOptional = true;

    CHECK_BIDIRECTIONAL_CONVERSION(
        OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Boolean, /*required*/ false)),
        SkiffOptional(CreateSimpleTypeSchema(EWireType::Boolean)),
        "[%true;]",
        "01" "01",
        ysonToSkiffConfig,
        skiffToYsonConfig);

    CHECK_BIDIRECTIONAL_CONVERSION(
        OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Boolean, /*required*/ false)),
        SkiffOptional(CreateSimpleTypeSchema(EWireType::Boolean)),
        "[#;]",
        "00",
        ysonToSkiffConfig,
        skiffToYsonConfig);

    CHECK_EXCEPTION_SUBSTR(
        ConvertYsonHex(
            OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Boolean, /*required*/ false)),
            SkiffOptional(CreateSimpleTypeSchema(EWireType::Boolean)),
            " # ",
            ysonToSkiffConfig),
        "value expected to be nonempty");
}

TEST(TYsonSkiffConverterTest, TestListTypes)
{
    CHECK_BIDIRECTIONAL_CONVERSION(
        ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Boolean)),
        CreateRepeatedVariant8Schema({CreateSimpleTypeSchema(EWireType::Boolean)}),
        "[]",
        "ff");

    CHECK_BIDIRECTIONAL_CONVERSION(
        ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Boolean)),
        CreateRepeatedVariant8Schema({CreateSimpleTypeSchema(EWireType::Boolean)}),
        "[%true;%true;%true;]",
        "00" "01" "00" "01" "00" "01" "ff");

    CHECK_BIDIRECTIONAL_CONVERSION(
        ListLogicalType(ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Boolean))),
        CreateRepeatedVariant8Schema({CreateRepeatedVariant8Schema({CreateSimpleTypeSchema(EWireType::Boolean)})}),
        "[[];[%true;];[%true;%true;];]",
        "00" "ff" "00" "0001ff" "00" "00010001ff" "ff");
}

TEST(TYsonSkiffConverterTest, TestStruct)
{
    CHECK_BIDIRECTIONAL_CONVERSION(
        StructLogicalType({
            {"key", SimpleLogicalType(ESimpleLogicalValueType::String)},
            {"value", SimpleLogicalType(ESimpleLogicalValueType::Boolean)},
        }),
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::String32)->SetName("key"),
            CreateSimpleTypeSchema(EWireType::Boolean)->SetName("value"),
        }),
        "[\"true\";%true;]",
        "04000000" "74727565" "01");
}

TEST(TYsonSkiffConverterTest, TestSkippedFields)
{
    TString skiffString;
    skiffString = ConvertYsonHex(
        StructLogicalType({
            {"key",    SimpleLogicalType(ESimpleLogicalValueType::String)},
            {"subkey", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            {"value",  SimpleLogicalType(ESimpleLogicalValueType::Boolean)},
        }),
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::String32)->SetName("key"),
            CreateSimpleTypeSchema(EWireType::Boolean)->SetName("value"),
        }),
        " [ true ; 1; %true ] ");
    EXPECT_EQ(skiffString, AsStringBuf("04000000" "74727565" "01"));

    skiffString = ConvertYsonHex(
        StructLogicalType({
            {"key",    SimpleLogicalType(ESimpleLogicalValueType::String)},
            {"subkey", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            {"value",  SimpleLogicalType(ESimpleLogicalValueType::Boolean)},
        }),
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::Int64)->SetName("subkey"),
        }),
        " [ true ; 1; %true ] ");
    EXPECT_EQ(skiffString, AsStringBuf("01000000" "00000000"));

    try {
        ConvertHexToTextYson(
            StructLogicalType({
                {"key",    SimpleLogicalType(ESimpleLogicalValueType::String)},
                {"subkey", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
                {"value",  SimpleLogicalType(ESimpleLogicalValueType::Boolean)},
            }),
            CreateTupleSchema({
                CreateSimpleTypeSchema(EWireType::Int64)->SetName("subkey"),
            }),
            "01000000" "00000000");
    } catch (const std::exception& e) {
        EXPECT_THAT(e.what(), testing::ContainsRegex("Non optional struct field .* is missing"));
    }

    CHECK_BIDIRECTIONAL_CONVERSION(
        StructLogicalType({
            {"key",    SimpleLogicalType(ESimpleLogicalValueType::String, /*required*/ false)},
            {"subkey", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            {"value",  SimpleLogicalType(ESimpleLogicalValueType::Boolean, /*required*/ false)},
        }),
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::Int64)->SetName("subkey"),
        }),
        "[#;15;#;]",
        "0f000000" "00000000");
}

TEST(TYsonSkiffConverterTest, TestTuple)
{
    CHECK_BIDIRECTIONAL_CONVERSION(
        TupleLogicalType({
            SimpleLogicalType(ESimpleLogicalValueType::String),
            SimpleLogicalType(ESimpleLogicalValueType::Boolean),
        }),
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::String32),
            CreateSimpleTypeSchema(EWireType::Boolean),
        }),
        "[\"true\";%true;]",
        "04000000" "74727565" "01");

    CHECK_BIDIRECTIONAL_CONVERSION(
        TupleLogicalType({
            SimpleLogicalType(ESimpleLogicalValueType::Int64),
            SimpleLogicalType(ESimpleLogicalValueType::Int64, /*required*/ false),
        }),
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::Int64),
            SkiffOptional(CreateSimpleTypeSchema(EWireType::Int64)),
        }),
        "[2;42;]",
        "02000000" "00000000" "01" "2a000000" "00000000");
}

class TYsonSkiffConverterTestVariant
    : public ::testing::TestWithParam<std::tuple<ELogicalMetatype, EWireType>>
{
public:
    TLogicalTypePtr VariantLogicalType(const std::vector<TLogicalTypePtr>& elements)
    {
        auto [metatype, wireType] = GetParam();
        if (metatype == ELogicalMetatype::VariantTuple) {
            return VariantTupleLogicalType(elements);
        } else {
            std::vector<TStructField> fields;
            for (size_t i = 0; i < elements.size(); ++i) {
                fields.push_back({Format("field%v", i), elements[i]});
            }
            return VariantStructLogicalType(fields);
        }
    }

    TSkiffSchemaPtr VariantSkiffSchema(const std::vector<TSkiffSchemaPtr> elements)
    {
        for (size_t i = 0; i < elements.size(); ++i) {
            elements[i]->SetName(Format("field%v", i));
        }
        auto [metatype, wireType] = GetParam();
        if (wireType == EWireType::Variant8) {
            return CreateVariant8Schema(std::move(elements));
        } else if (wireType == EWireType::Variant16) {
            return CreateVariant16Schema(std::move(elements));
        }
        Y_UNREACHABLE();
    }

    TString VariantTagInfix() const
    {
        auto [metatype, wireType] = GetParam();
        if (wireType == EWireType::Variant16) {
            return "00";
        }
        return {};
    }
};

TEST_P(TYsonSkiffConverterTestVariant, TestVariant)
{
    CHECK_BIDIRECTIONAL_CONVERSION(
        VariantLogicalType({
            SimpleLogicalType(ESimpleLogicalValueType::Int64),
            SimpleLogicalType(ESimpleLogicalValueType::Boolean)
        }),
        VariantSkiffSchema({
            CreateSimpleTypeSchema(EWireType::Int64),
            CreateSimpleTypeSchema(EWireType::Boolean),
        }),
        "[0;42;]",
        "00" + VariantTagInfix() + "2a000000" "00000000");

    CHECK_BIDIRECTIONAL_CONVERSION(
        VariantLogicalType({
            SimpleLogicalType(ESimpleLogicalValueType::Int64),
            SimpleLogicalType(ESimpleLogicalValueType::Boolean)
        }),
        VariantSkiffSchema({
            CreateSimpleTypeSchema(EWireType::Int64),
            CreateSimpleTypeSchema(EWireType::Boolean),
        }),
        "[1;%true;]",
        "01" + VariantTagInfix() + "01");
}

TEST_P(TYsonSkiffConverterTestVariant, TestMalformedVariants)
{
    auto logicalType = VariantLogicalType({
        SimpleLogicalType(ESimpleLogicalValueType::Boolean),
        SimpleLogicalType(ESimpleLogicalValueType::Int64),
    });
    auto skiffSchema = VariantSkiffSchema({
        CreateSimpleTypeSchema(EWireType::Boolean),
        CreateSimpleTypeSchema(EWireType::Int64),
    });

    CHECK_EXCEPTION_SUBSTR(ConvertYsonHex(logicalType, skiffSchema, "[2; 42]"), "Yson to Skiff conversion error");
    CHECK_EXCEPTION_SUBSTR(ConvertYsonHex(logicalType, skiffSchema, "[]"), "Yson to Skiff conversion error");
    CHECK_EXCEPTION_SUBSTR(ConvertYsonHex(logicalType, skiffSchema, "[0]"), "Yson to Skiff conversion error");

    CHECK_EXCEPTION_SUBSTR(ConvertHexToTextYson(logicalType, skiffSchema, "02" + VariantTagInfix() + "00"),
        "Skiff to Yson conversion error");
}

INSTANTIATE_TEST_CASE_P(
    Variants,
    TYsonSkiffConverterTestVariant,
    ::testing::Combine(
        ::testing::ValuesIn({ELogicalMetatype::VariantStruct, ELogicalMetatype::VariantTuple}),
        ::testing::ValuesIn({EWireType::Variant8, EWireType::Variant16})
    )
);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFormats
