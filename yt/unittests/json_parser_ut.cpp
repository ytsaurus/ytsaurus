#include "stdafx.h"
#include "framework.h"

#include <core/formats/json_parser.h>
#include <core/ytree/yson_consumer-mock.h>

#include <util/string/base64.h>

namespace NYT {
namespace NFormats {
namespace {

using ::testing::InSequence;
using ::testing::StrictMock;

////////////////////////////////////////////////////////////////////////////////

inline Stroka SurroundWithQuotes(const Stroka& s)
{
    Stroka quote = "\"";
    return quote + s + quote;
}

// Basic types:
TEST(TJsonParserTest, List)
{
    StrictMock<NYTree::TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnBeginList());
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnIntegerScalar(1));
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
    StrictMock<NYTree::TMockYsonConsumer> Mock;
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
    StrictMock<NYTree::TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnEntity());

    Stroka input = "null";

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, EmptyString)
{
    StrictMock<NYTree::TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnStringScalar(""));

    Stroka input = SurroundWithQuotes("");

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}


TEST(TJsonParserTest, OutOfRangeUnicodeSymbols)
{
    StrictMock<NYTree::TMockYsonConsumer> Mock;

    Stroka input = SurroundWithQuotes("\\u0100");
    TStringInput stream(input);

    EXPECT_ANY_THROW(
        ParseJson(&stream, &Mock)
    );
}

TEST(TJsonParserTest, EscapedUnicodeSymbols)
{
    StrictMock<NYTree::TMockYsonConsumer> Mock;
    InSequence dummy;

    Stroka s = Stroka("\x80\n\xFF", 3);
    EXPECT_CALL(Mock, OnStringScalar(s));

    Stroka input = SurroundWithQuotes("\\u0080\\u000A\\u00FF");

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, UnsupportedValue)
{
    StrictMock<NYTree::TMockYsonConsumer> Mock;
    Stroka input = "true";

    TStringInput stream(input);
    EXPECT_ANY_THROW(
        ParseJson(&stream, &Mock)
    );
}

TEST(TJsonParserTest, InvalidJson)
{
    StrictMock<NYTree::TMockYsonConsumer> Mock;
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
    StrictMock<NYTree::TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnBeginAttributes());
        EXPECT_CALL(Mock, OnKeyedItem("foo"));
        EXPECT_CALL(Mock, OnStringScalar("bar"));
    EXPECT_CALL(Mock, OnEndAttributes());

    EXPECT_CALL(Mock, OnBeginList());
        EXPECT_CALL(Mock, OnListItem());
        EXPECT_CALL(Mock, OnIntegerScalar(1));
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
    StrictMock<NYTree::TMockYsonConsumer> Mock;
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

TEST(TJsonParserTest, IntegerWithAttributes)
{
    StrictMock<NYTree::TMockYsonConsumer> Mock;
    InSequence dummy;

    EXPECT_CALL(Mock, OnBeginAttributes());
        EXPECT_CALL(Mock, OnKeyedItem("foo"));
        EXPECT_CALL(Mock, OnStringScalar("bar"));
    EXPECT_CALL(Mock, OnEndAttributes());

    EXPECT_CALL(Mock, OnIntegerScalar(42));

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
    StrictMock<NYTree::TMockYsonConsumer> Mock;
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
    StrictMock<NYTree::TMockYsonConsumer> Mock;
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
    StrictMock<NYTree::TMockYsonConsumer> Mock;
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

    StrictMock<NYTree::TMockYsonConsumer> Mock;
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

TEST(TJsonParserTest, ListFragment)
{
    StrictMock<NYTree::TMockYsonConsumer> Mock;
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
    StrictMock<NYTree::TMockYsonConsumer> Mock;
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
    StrictMock<NYTree::TMockYsonConsumer> Mock;

    Stroka input = "{\"$attributes\":\"20\"}";

    TStringInput stream(input);
    EXPECT_ANY_THROW(
        ParseJson(&stream, &Mock)
    );
}

TEST(TJsonParserTest, MemoryLimit1)
{
    StrictMock<NYTree::TMockYsonConsumer> Mock;

    auto config = New<TJsonFormatConfig>();
    config->MemoryLimit = 10;

    Stroka input = "{\"my_string\":\"with_long_long_value\"}";

    TStringInput stream(input);
    EXPECT_ANY_THROW(
        ParseJson(&stream, &Mock, config)
    );
}

TEST(TJsonParserTest, MemoryLimit2)
{
    StrictMock<NYTree::TMockYsonConsumer> Mock;
    //InSequence dummy; // order in map is not specified

    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem("my_string"));
        EXPECT_CALL(Mock, OnStringScalar("with_long_long_value"));
    EXPECT_CALL(Mock, OnEndMap());

    Stroka input = "{\"my_string\":\"with_long_long_value\"}";

    auto config = New<TJsonFormatConfig>();
    config->MemoryLimit = 1000;

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NFormats
} // namespace NYT
