#include <yt/core/test_framework/framework.h>

#include <yt/client/complex_types/named_structures_yson.h>
#include <yt/client/table_client/logical_type.h>

namespace NYT::NComplexTypes {

using namespace NYson;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

const auto KeyValueStruct = StructLogicalType({
    {"key", SimpleLogicalType(ESimpleLogicalValueType::String)},
    {"value", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))},
});

const auto IntStringVariant = VariantStructLogicalType({
    {"int", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
    {"string", SimpleLogicalType(ESimpleLogicalValueType::String)},
});

thread_local TPositionalToNamedConfig PositionalToNamedConfigInstance;

class TWithConfig
{
public:
    TWithConfig(const TPositionalToNamedConfig& config)
    {
        PositionalToNamedConfigInstance = config;
    }

    ~TWithConfig()
    {
        PositionalToNamedConfigInstance = {};
    }
};

TString CanonizeYson(TStringBuf yson)
{
    TString result;
    {
        TStringOutput out(result);
        TYsonWriter writer(&out, EYsonFormat::Pretty, EYsonType::Node);
        ParseYsonStringBuffer(yson, EYsonType::Node, &writer);
    }
    return result;
}

void ConvertYson(
    bool namedToPositional,
    const TLogicalTypePtr& type,
    TStringBuf sourceYson,
    TString* convertedYson)
{
    TComplexTypeFieldDescriptor descriptor("<test-field>", type);
    TYsonConverter converter;
    try {
        converter = namedToPositional
            ? CreateNamedToPositionalYsonConverter(descriptor)
            : CreatePositionalToNamedYsonConverter(descriptor, PositionalToNamedConfigInstance);
    } catch (const std::exception& ex) {
        ADD_FAILURE() << "cannot create converter: " << ex.what();
        return;
    }

    TMemoryInput in(sourceYson);
    TYsonPullParser parser(&in, EYsonType::Node);
    TYsonPullParserCursor cursor(&parser);

    TStringOutput out(*convertedYson);
    {
        TYsonWriter writer(&out, EYsonFormat::Pretty);
        converter(&cursor, &writer);
    }
    EXPECT_EQ(cursor->GetType(), EYsonItemType::EndOfStream);
}

void CheckYsonConvertion(
    bool namedToPositional,
    const TLogicalTypePtr& type,
    TStringBuf sourceYson,
    TStringBuf expectedConvertedYson)
{
    TString convertedYson;
    try {
        ConvertYson(namedToPositional, type, sourceYson, &convertedYson);
    } catch (const std::exception& ex) {
        ADD_FAILURE() << "convertion error: " << ex.what();
        return;
    }

    EXPECT_EQ(convertedYson, CanonizeYson(expectedConvertedYson));
}

#define CHECK_POSITIONAL_TO_NAMED(type, positionalYson, namedYson) \
    do { \
        SCOPED_TRACE("positional -> named error"); \
        CheckYsonConvertion(false, type, positionalYson, namedYson); \
    } while (0)

#define CHECK_NAMED_TO_POSITIONAL(type, namedYson, positionalYson) \
    do { \
        SCOPED_TRACE("named -> positional error"); \
        CheckYsonConvertion(true, type, namedYson, positionalYson); \
    } while (0)

#define CHECK_NAMED_TO_POSITIONAL_THROWS(type, namedYson, exceptionSubstring) \
    do { \
        TString tmp; \
        EXPECT_THROW_WITH_SUBSTRING(ConvertYson(true, type, namedYson, &tmp), exceptionSubstring); \
    } while (0)

#define CHECK_POSITIONAL_TO_NAMED_THROWS(type, namedYson, exceptionSubstring) \
    do { \
        TString tmp; \
        EXPECT_THROW_WITH_SUBSTRING(ConvertYson(false, type, namedYson, &tmp), exceptionSubstring); \
    } while (0)

#define CHECK_BIDIRECTIONAL(type, positionalYson, namedYson) \
    do { \
        CHECK_POSITIONAL_TO_NAMED(type, positionalYson, namedYson); \
        CHECK_NAMED_TO_POSITIONAL(type, namedYson, positionalYson); \
    } while (0)

TEST(TNamedPositionalYsonConverter, TestSimpleTypes)
{
    CHECK_BIDIRECTIONAL(
        SimpleLogicalType(ESimpleLogicalValueType::Int64),
        "-42",
        "-42");

    CHECK_BIDIRECTIONAL(
        SimpleLogicalType(ESimpleLogicalValueType::String),
        "foo",
        "foo");
}

TEST(TNamedPositionalYsonConverter, TestStruct)
{
    CHECK_BIDIRECTIONAL(KeyValueStruct, "[foo; bar]", "{key=foo; value=bar}");
    CHECK_BIDIRECTIONAL(KeyValueStruct, "[qux; #]", "{key=qux; value=#}");

    CHECK_POSITIONAL_TO_NAMED(KeyValueStruct, "[baz]", "{key=baz; value=#}");
    CHECK_NAMED_TO_POSITIONAL(KeyValueStruct, "{key=baz}", "[baz; #]");

    CHECK_NAMED_TO_POSITIONAL_THROWS(KeyValueStruct, "{}", "is missing while parsing");
    CHECK_NAMED_TO_POSITIONAL_THROWS(KeyValueStruct, "{value=baz}", "is missing while parsing");
}

TEST(TNamedPositionalYsonConverter, TestStructSkipNullValues)
{
    TPositionalToNamedConfig config;
    config.SkipNullValues = true;
    TWithConfig g(config);

    CHECK_POSITIONAL_TO_NAMED(KeyValueStruct, "[foo; bar]", "{key=foo; value=bar}");
    CHECK_POSITIONAL_TO_NAMED(KeyValueStruct, "[foo; #]", "{key=foo}");
    CHECK_POSITIONAL_TO_NAMED(KeyValueStruct, "[foo]", "{key=foo}");

    auto type2 = StructLogicalType({
        {"opt_int", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        {"opt_opt_int", OptionalLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64)))},
        {"list_null", ListLogicalType(NullLogicalType)},
        {"null", NullLogicalType},
    });
    CHECK_POSITIONAL_TO_NAMED(type2, "[42; [#]; []; #]", "{opt_int=42; opt_opt_int=[#]; list_null=[]}");
    CHECK_POSITIONAL_TO_NAMED(type2, "[#; #; [#; #;]]", "{list_null=[#; #;]}");
}

TEST(TNamedPositionalYsonConverter, TestVariantStruct)
{
    CHECK_BIDIRECTIONAL(IntStringVariant, "[0; 42]", "[int; 42]");
    CHECK_BIDIRECTIONAL(IntStringVariant, "[1; foo]", "[string; foo]");

    CHECK_NAMED_TO_POSITIONAL_THROWS(IntStringVariant, "[str; foo]", "Unknown variant field");
}

TEST(TNamedPositionalYsonConverter, TestOptional)
{
    CHECK_BIDIRECTIONAL(
        OptionalLogicalType(KeyValueStruct),
        "[foo; bar]",
        "{key=foo; value=bar}");

    CHECK_BIDIRECTIONAL(
        OptionalLogicalType(KeyValueStruct),
        "#",
        "#");

    CHECK_BIDIRECTIONAL(
        OptionalLogicalType(OptionalLogicalType(KeyValueStruct)),
        "[[foo; bar]]",
        "[{key=foo; value=bar}]");

    CHECK_BIDIRECTIONAL(
        OptionalLogicalType(OptionalLogicalType(KeyValueStruct)),
        "#",
        "#");

    CHECK_BIDIRECTIONAL(
        OptionalLogicalType(OptionalLogicalType(KeyValueStruct)),
        "[#]",
        "[#]");
}

TEST(TNamedPositionalYsonConverter, TestList)
{
    CHECK_BIDIRECTIONAL(
        ListLogicalType(KeyValueStruct),
        "[[foo; bar]; [qux; #]]",
        "[{key=foo; value=bar}; {key=qux; value=#};]");
}

TEST(TNamedPositionalYsonConverter, TestTuple)
{
    CHECK_BIDIRECTIONAL(
        TupleLogicalType({KeyValueStruct, IntStringVariant, SimpleLogicalType(ESimpleLogicalValueType::Utf8)}),
        "[[foo; bar]; [0; 5]; foo]",
        "[{key=foo; value=bar}; [int; 5]; foo;]");
}

TEST(TNamedPositionalYsonConverter, TestVariantTuple)
{
    auto type = VariantTupleLogicalType({KeyValueStruct, IntStringVariant, SimpleLogicalType(ESimpleLogicalValueType::Utf8)});
    CHECK_BIDIRECTIONAL(
        type,
        "[0; [foo; #]]",
        "[0; {key=foo; value=#}]");

    CHECK_BIDIRECTIONAL(
        type,
        "[1; [1; bar]]",
        "[1; [string; bar]]");

    CHECK_BIDIRECTIONAL(
        type,
        "[2; qux]",
        "[2; qux]");
}

TEST(TNamedPositionalYsonConverter, TestDict)
{
    CHECK_BIDIRECTIONAL(
        DictLogicalType(KeyValueStruct, IntStringVariant),
        "[ [[foo; #]; [0; 0]] ; [[bar; qux;]; [1; baz;]]; ]",
        "[ [{key=foo; value=#}; [int; 0]] ; [{key=bar; value=qux;}; [string; baz;]] ]");
}

TEST(TNamedPositionalYsonConverter, TestTagged)
{
    CHECK_BIDIRECTIONAL(
        TaggedLogicalType("foo", KeyValueStruct),
        "[foo; bar]",
        "{key=foo; value=bar}");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComplexTypes
