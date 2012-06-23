#include "stdafx.h"

#include <ytlib/formats/json_parser.h>
#include <ytlib/ytree/yson_consumer-mock.h>

#include <util/string/base64.h>

#include <contrib/testing/framework.h>

using ::testing::InSequence;
using ::testing::StrictMock;

namespace NYT {
namespace NFormats {

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


TEST(TJsonParserTest, ValidUtf8String)
{
    StrictMock<NYTree::TMockYsonConsumer> Mock;
    InSequence dummy;

    Stroka s = Stroka("\xCF\x8F", 2); // (110)0 1111 (10)00 1111 -- valid code points
    EXPECT_CALL(Mock, OnStringScalar(s));

    Stroka input = SurroundWithQuotes(s);

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, NotValidUtf8String)
{
    StrictMock<NYTree::TMockYsonConsumer> Mock;
    InSequence dummy;

    Stroka s = Stroka("\x80\x01", 2); // second codepoint doesn't start with 10..
    EXPECT_CALL(Mock, OnStringScalar(s));

    Stroka input = SurroundWithQuotes("&" + Base64Encode(s));

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, StringStartingWithSpecailSymbol)
{
    StrictMock<NYTree::TMockYsonConsumer> Mock;
    InSequence dummy;

    Stroka s = "&some_string";
    EXPECT_CALL(Mock, OnStringScalar(s));

    Stroka input = SurroundWithQuotes("&" + Base64Encode(s));

    TStringInput stream(input);
    ParseJson(&stream, &Mock);
}

TEST(TJsonParserTest, StringStartingWithSpecialSymbolAsKeyInMap)
{
    StrictMock<NYTree::TMockYsonConsumer> Mock;
    InSequence dummy;

    Stroka s = "&hello";
    EXPECT_CALL(Mock, OnBeginMap());
        EXPECT_CALL(Mock, OnKeyedItem(s));
        EXPECT_CALL(Mock, OnStringScalar("world"));
    EXPECT_CALL(Mock, OnEndMap());

    Stroka expectedS = SurroundWithQuotes("&" + Base64Encode(s));
    Stroka input = Sprintf("{%s:\"world\"}", ~expectedS);

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

///////////////////////////////////////////////////////////////////////////////
            
} // namespace NFormats
} // namespace NYT
