#include "stdafx.h"
#include "framework.h"

#include <ytlib/formats/json_parser.h>

#include <core/yson/consumer-mock.h>

#include <util/string/base64.h>

namespace NYT {
namespace NFormats {
namespace {

using namespace NYson;

using ::testing::InSequence;
using ::testing::StrictMock;
using ::testing::NiceMock;

////////////////////////////////////////////////////////////////////////////////

inline Stroka SurroundWithQuotes(const Stroka& s)
{
    Stroka quote = "\"";
    return quote + s + quote;
}

// Basic types:
TEST(TJsonParserTest, List)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnBeginList());
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnInt64Scalar(1));
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnStringScalar("aaa"));
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnDoubleScalar(::testing::DoubleEq(3.5)));
    EXPECT_CALL(Mock, OnEndList());

    Stroka input = "[1,\"aaa\",3.5]";

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, Map)
{
    StrictMock<TMockYsonConsumer> Mock;
    //InSequence dummy; // order in map is not specified

    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("hello"));
        EXPECT_CALL(Mock, OnStringScalar("world"));
        EXPECT_CALL(Mock, OnKeyedItem("foo"));
        EXPECT_CALL(Mock, OnStringScalar("bar"));
    EXPECT_CALL(Mock, OnEndMap());

    Stroka input = "{\"hello\":\"world\",\"foo\":\"bar\"}";

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, Entity)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnEntity());

    Stroka input = "null";

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, EmptyString)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnStringScalar(""));

    Stroka input = SurroundWithQuotes("");

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}


TEST(TJsonParserTest, OutOfRangeUnicodeSymbols)
{
    StrictMock<TMockYsonConsumer> Mock;

    Stroka input = SurroundWithQuotes("\\u0100");
    TStringInput stream(input);

    EXPECT_ANY_THROW(
        ParseJson(&stream, &Mock)
    );
}

TEST(TJsonParserTest, EscapedUnicodeSymbols)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    Stroka s = Stroka("\x80\n\xFF", 3);
    EXPECT_CALL(Mock, OnStringScalar(s));

    Stroka input = SurroundWithQuotes("\\u0080\\u000A\\u00FF");

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, Boolean)
{
    StrictMock<TMockYsonConsumer> Mock;
    Stroka input = "true";

    EXPECT_CALL(Mock, OnBooleanScalar(true));

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, InvalidJson)
{
    StrictMock<TMockYsonConsumer> Mock;
    Stroka input = "{\"hello\" = \"world\"}"; // YSon style instead of json

    TStringInput stream(input);
    EXPECT_ANY_THROW(
        ParseJson(&stream, &Mock)
    );
}

////////////////////////////////////////////////////////////////////////////////

// Values with attributes:
TEST(TJsonParserTest, ListWithAttributes)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnBeginAttributes());
        EXPECT_CALL(Mock, OnKeyedItem("foo"));
        EXPECT_CALL(Mock, OnStringScalar("bar"));
    EXPECT_CALL(Mock, OnEndAttributes());

    EXPECT_CALL(Mock, OnBeginList());
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnInt64Scalar(1));
    EXPECT_CALL(Mock, OnEndList());

    Stroka input =
        "{"
            "\"$attributes\":{\"foo\":\"bar\"}"
            ","
            "\"$value\":[1]"
        "}";

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, MapWithAttributes)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnBeginAttributes());
        EXPECT_CALL(Mock, OnKeyedItem("foo"));
        EXPECT_CALL(Mock, OnStringScalar("bar"));
    EXPECT_CALL(Mock, OnEndAttributes());

    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("spam"));
        EXPECT_CALL(Mock, OnStringScalar("bad"));
    EXPECT_CALL(Mock, OnEndMap());

    Stroka input =
        "{"
            "\"$attributes\":{\"foo\":\"bar\"}"
            ","
            "\"$value\":{\"spam\":\"bad\"}"
        "}";

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, Int64WithAttributes)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnBeginAttributes());
        EXPECT_CALL(Mock, OnKeyedItem("foo"));
        EXPECT_CALL(Mock, OnStringScalar("bar"));
    EXPECT_CALL(Mock, OnEndAttributes());

    EXPECT_CALL(Mock, OnInt64Scalar(42));

    Stroka input =
        "{"
            "\"$attributes\":{\"foo\":\"bar\"}"
            ","
            "\"$value\":42"
        "}";

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, EntityWithAttributes)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnBeginAttributes());
        EXPECT_CALL(Mock, OnKeyedItem("foo"));
        EXPECT_CALL(Mock, OnStringScalar("bar"));
    EXPECT_CALL(Mock, OnEndAttributes());

    EXPECT_CALL(Mock, OnEntity());

    Stroka input =
        "{"
            "\"$attributes\":{\"foo\":\"bar\"}"
            ","
            "\"$value\":null"
        "}";

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, StringWithAttributes)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnBeginAttributes());
        EXPECT_CALL(Mock, OnKeyedItem("foo"));
        EXPECT_CALL(Mock, OnStringScalar("bar"));
    EXPECT_CALL(Mock, OnEndAttributes());

    EXPECT_CALL(Mock, OnStringScalar("some_string"));

    Stroka input =
        "{"
            "\"$attributes\":{\"foo\":\"bar\"}"
            ","
            "\"$value\":\"some_string\""
        "}";

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, DoubleAttributes)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnBeginAttributes());
        EXPECT_CALL(Mock, OnKeyedItem("foo"));
        EXPECT_CALL(Mock, OnBeginAttributes());
            EXPECT_CALL(Mock, OnKeyedItem("another_foo"));
            EXPECT_CALL(Mock, OnStringScalar("another_bar"));
        EXPECT_CALL(Mock, OnEndAttributes());
        EXPECT_CALL(Mock, OnStringScalar("bar"));
    EXPECT_CALL(Mock, OnEndAttributes());

    EXPECT_CALL(Mock, OnStringScalar("some_string"));

    Stroka input =
        "{"
            "\"$attributes\":{\"foo\":"
                "{"
                    "\"$attributes\":{\"another_foo\":\"another_bar\"}"
                    ","
                    "\"$value\":\"bar\"}"
                "}"
            ","
            "\"$value\":\"some_string\""
        "}";

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, SomeHackyTest)
{
    Stroka input = "{\"$value\": \"yamr\", \"$attributes\": {\"lenval\": \"false\", \"has_subkey\": \"false\"}}";

    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnBeginAttributes());
        EXPECT_CALL(Mock, OnKeyedItem("lenval"));
        EXPECT_CALL(Mock, OnStringScalar("false"));
        EXPECT_CALL(Mock, OnKeyedItem("has_subkey"));
        EXPECT_CALL(Mock, OnStringScalar("false"));
    EXPECT_CALL(Mock, OnEndAttributes());

    EXPECT_CALL(Mock, OnStringScalar("yamr"));

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, EmptyListFragment)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    Stroka empty;
    TStringInput stream(empty);
    ParseJson(&stream, &Mock, nullptr, NYson::EYsonType::ListFragment);
}

TEST(TJsonParserTest, ListFragment)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("hello"));
        EXPECT_CALL(Mock, OnStringScalar("world"));
    EXPECT_CALL(Mock, OnEndMap());
    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("foo"));
        EXPECT_CALL(Mock, OnStringScalar("bar"));
    EXPECT_CALL(Mock, OnEndMap());

    Stroka input = "{\"hello\":\"world\"}\n{\"foo\":\"bar\"}\n";

    TStringInput stream(input);
    ParseJson(&stream, &Mock, nullptr, NYson::EYsonType::ListFragment);
}

TEST(TJsonParserTest, SpecialKeys)
{
    StrictMock<TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnListItem());
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("$$value"));
        EXPECT_CALL(Mock, OnStringScalar("10"));
        EXPECT_CALL(Mock, OnKeyedItem("$attributes"));
        EXPECT_CALL(Mock, OnStringScalar("20"));
    EXPECT_CALL(Mock, OnEndMap());

    Stroka input = "{\"$$$value\":\"10\",\"$$attributes\":\"20\"}\n";

    TStringInput stream(input);
    ParseJson(&stream, &Mock, nullptr, NYson::EYsonType::ListFragment);
}

TEST(TJsonParserTest, AttributesWithoutValue)
{
    StrictMock<TMockYsonConsumer> Mock;

    Stroka input = "{\"$attributes\":\"20\"}";

    TStringInput stream(input);
    EXPECT_ANY_THROW(
        ParseJson(&stream, &Mock)
    );
}

TEST(TJsonParserTest, Trash)
{
    StrictMock<TMockYsonConsumer> Mock;

    Stroka input = "fdslfsdhfkajsdhf";

    TStringInput stream(input);
    EXPECT_ANY_THROW(
        ParseJson(&stream, &Mock)
    );
}

TEST(TJsonParserTest, TrailingTrash)
{
    StrictMock<TMockYsonConsumer> Mock;

    Stroka input = "{\"a\":\"b\"} fdslfsdhfkajsdhf";

    TStringInput stream(input);
    EXPECT_ANY_THROW(
        ParseJson(&stream, &Mock)
    );
}

TEST(TJsonParserTest, MultipleValues)
{
    StrictMock<TMockYsonConsumer> Mock;

    Stroka input = "{\"a\":\"b\"}{\"a\":\"b\"}";

    TStringInput stream(input);
    EXPECT_ANY_THROW(
        ParseJson(&stream, &Mock)
    );
}

TEST(TJsonParserTest, ReservedKeyName)
{
    StrictMock<TMockYsonConsumer> Mock;

    EXPECT_CALL(Mock, OnBeginMap());

    Stroka input = "{\"$other\":\"20\"}";

    TStringInput stream(input);
    EXPECT_ANY_THROW(
        ParseJson(&stream, &Mock)
    );
}

TEST(TJsonParserTest, MemoryLimit1)
{
    StrictMock<TMockYsonConsumer> Mock;

    auto config = New<TJsonFormatConfig>();
    config->MemoryLimit = 10;

    Stroka input = "{\"my_string\":\"" + Stroka(100000, 'X') + "\"}";

    TStringInput stream(input);
    EXPECT_ANY_THROW(
        ParseJson(&stream, &Mock, config)
    );
}

TEST(TJsonParserTest, MemoryLimit2)
{
    StrictMock<TMockYsonConsumer> Mock;

    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("my_string"));
        Stroka expectedString(100000, 'X');
        EXPECT_CALL(Mock, OnStringScalar(expectedString));
    EXPECT_CALL(Mock, OnEndMap());

    Stroka input = "{\"my_string\":\"" + Stroka(100000, 'X') + "\"}";

    auto config = New<TJsonFormatConfig>();
    config->MemoryLimit = 500000;

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, MemoryLimit3)
{
    StrictMock<TMockYsonConsumer> Mock;

    auto config = New<TJsonFormatConfig>();
    config->MemoryLimit = 1000;

    int keyCount = 100;
    TStringStream stream;
    stream << "{";
    for (int i = 0; i < keyCount; ++i) {
        stream << "\"key" << ToString(i) << "\": \"value\"";
        if (i + 1 < keyCount) {
            stream << ",";
        }
    }
    stream << "}";

    EXPECT_ANY_THROW(
        ParseJson(&stream, &Mock, config)
    );
}

TEST(TJsonParserTest, MemoryLimit4)
{
    NiceMock<TMockYsonConsumer> Mock;

    auto config = New<TJsonFormatConfig>();
    config->MemoryLimit = 200000;

    int rowCount = 1000;
    int keyCount = 100;

    TStringStream stream;
    for (int j = 0; j < rowCount; ++j) {
        stream << "{";
        for (int i = 0; i < keyCount; ++i) {
            stream << "\"key" << ToString(i) << "\": \"value\"";
            if (i + 1 < keyCount) {
                stream << ",";
            }
        }
        stream << "}\n";
    }

    // Not throw, because of total memory occupied by all rows is greater than MemoryLimit,
    // but memory occuied by individual row is much lower than MemoryLimit.
    ParseJson(&stream, &Mock, config, NYson::EYsonType::ListFragment);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NFormats
} // namespace NYT
