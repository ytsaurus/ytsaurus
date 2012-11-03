#include "stdafx.h"

#include <ytlib/formats/json_writer.h>

#include <util/string/base64.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

inline Stroka SurroundWithQuotes(const Stroka& s)
{
    Stroka quote = "\"";
    return quote + s + quote;
}

// Basic types:
TEST(TJsonWriterTest, List)
{
    TStringStream outputStream;
    TJsonWriter writer(&outputStream);

    writer.OnBeginList();
        writer.OnListItem();
        writer.OnIntegerScalar(1);
        writer.OnListItem();
        writer.OnStringScalar("aaa");
        writer.OnListItem();
        writer.OnDoubleScalar(3.5);
    writer.OnEndList();
    writer.Flush();

    Stroka output = "[1,\"aaa\",3.5]";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonWriterTest, Map)
{
    TStringStream outputStream;
    TJsonWriter writer(&outputStream);

    writer.OnBeginMap();
        writer.OnKeyedItem("hello");
        writer.OnStringScalar("world");
        writer.OnKeyedItem("foo");
        writer.OnStringScalar("bar");
    writer.OnEndMap();
    writer.Flush();

    Stroka output = "{\"hello\":\"world\",\"foo\":\"bar\"}";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonWriterTest, Entity)
{
    TStringStream outputStream;
    TJsonWriter writer(&outputStream);

    writer.OnEntity();
    writer.Flush();

    Stroka output = "null";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonWriterTest, EmptyString)
{
    TStringStream outputStream;
    TJsonWriter writer(&outputStream);

    writer.OnStringScalar("");
    writer.Flush();

    Stroka output = SurroundWithQuotes("");
    EXPECT_EQ(output, outputStream.Str());
}


TEST(TJsonWriterTest, ValidUtf8String)
{
    TStringStream outputStream;
    TJsonWriter writer(&outputStream);

    Stroka s = Stroka("\xCF\x8F", 2); // (110)0 1111 (10)00 1111 -- valid code points
    writer.OnStringScalar(s);
    writer.Flush();

    Stroka output = SurroundWithQuotes(s);
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonWriterTest, NotValidUtf8String)
{
    TStringStream outputStream;
    TJsonWriter writer(&outputStream);

    Stroka s = Stroka("\x80\x01", 2); // second codepoint doesn't start with 10..
    writer.OnStringScalar(s);
    writer.Flush();

    Stroka output = SurroundWithQuotes("&" + Base64Encode(s));
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonWriterTest, StringStartingWithSpecailSymbol)
{
    TStringStream outputStream;
    TJsonWriter writer(&outputStream);

    Stroka s = "&some_string";
    writer.OnStringScalar(s);
    writer.Flush();

    Stroka output = SurroundWithQuotes("&" + Base64Encode(s));
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonWriterTest, StringStartingWithSpecialSymbolAsKeyInMap)
{
    TStringStream outputStream;
    TJsonWriter writer(&outputStream);

    Stroka s = "&hello";
    writer.OnBeginMap();
        writer.OnKeyedItem(s);
        writer.OnStringScalar("world");
    writer.OnEndMap();
    writer.Flush();

    Stroka expectedS = SurroundWithQuotes("&" + Base64Encode(s));
    Stroka output = Sprintf("{%s:\"world\"}", ~expectedS);
    EXPECT_EQ(output, outputStream.Str());
}

////////////////////////////////////////////////////////////////////////////////

// Values with attributes:
TEST(TJsonWriterTest, ListWithAttributes)
{
    TStringStream outputStream;
    TJsonWriter writer(&outputStream);

    writer.OnBeginAttributes();
        writer.OnKeyedItem("foo");
        writer.OnStringScalar("bar");
    writer.OnEndAttributes();

    writer.OnBeginList();
        writer.OnListItem();
        writer.OnIntegerScalar(1);
    writer.OnEndList();
    writer.Flush();

    Stroka output =
        "{"
            "\"$attributes\":{\"foo\":\"bar\"}"
            ","
            "\"$value\":[1]"
        "}";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonWriterTest, MapWithAttributes)
{
    TStringStream outputStream;
    TJsonWriter writer(&outputStream);

    writer.OnBeginAttributes();
        writer.OnKeyedItem("foo");
        writer.OnStringScalar("bar");
    writer.OnEndAttributes();

    writer.OnBeginMap();
        writer.OnKeyedItem("spam");
        writer.OnStringScalar("bad");
    writer.OnEndMap();
    writer.Flush();

    Stroka output =
        "{"
            "\"$attributes\":{\"foo\":\"bar\"}"
            ","
            "\"$value\":{\"spam\":\"bad\"}"
        "}";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonWriterTest, IntegerWithAttributes)
{
    TStringStream outputStream;
    TJsonWriter writer(&outputStream);

    writer.OnBeginAttributes();
        writer.OnKeyedItem("foo");
        writer.OnStringScalar("bar");
    writer.OnEndAttributes();

    writer.OnIntegerScalar(42);
    writer.Flush();

    Stroka output =
        "{"
            "\"$attributes\":{\"foo\":\"bar\"}"
            ","
            "\"$value\":42"
        "}";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonWriterTest, EntityWithAttributes)
{
    TStringStream outputStream;
    TJsonWriter writer(&outputStream);

    writer.OnBeginAttributes();
        writer.OnKeyedItem("foo");
        writer.OnStringScalar("bar");
    writer.OnEndAttributes();

    writer.OnEntity();
    writer.Flush();

    Stroka output =
        "{"
            "\"$attributes\":{\"foo\":\"bar\"}"
            ","
            "\"$value\":null"
        "}";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonWriterTest, StringWithAttributes)
{
    TStringStream outputStream;
    TJsonWriter writer(&outputStream);

    writer.OnBeginAttributes();
        writer.OnKeyedItem("foo");
        writer.OnStringScalar("bar");
    writer.OnEndAttributes();

    writer.OnStringScalar("some_string");
    writer.Flush();

    Stroka output =
        "{"
            "\"$attributes\":{\"foo\":\"bar\"}"
            ","
            "\"$value\":\"some_string\""
        "}";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonWriterTest, DoubleAttributes)
{
    TStringStream outputStream;
    TJsonWriter writer(&outputStream);

    writer.OnBeginAttributes();
        writer.OnKeyedItem("foo");
        writer.OnBeginAttributes();
            writer.OnKeyedItem("another_foo");
            writer.OnStringScalar("another_bar");
        writer.OnEndAttributes();
        writer.OnStringScalar("bar");
    writer.OnEndAttributes();

    writer.OnStringScalar("some_string");
    writer.Flush();

    Stroka output =
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
    EXPECT_EQ(output, outputStream.Str());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TJsonWriterTest, NeverAttributes)
{
    TStringStream outputStream;
    auto config = New<TJsonFormatConfig>();
    config->AttributesMode = EJsonAttributesMode::Never;
    TJsonWriter writer(&outputStream, config);

    writer.OnBeginAttributes();
        writer.OnKeyedItem("foo");
        writer.OnStringScalar("bar");
    writer.OnEndAttributes();

    writer.OnBeginMap();
        writer.OnKeyedItem("answer");
        writer.OnIntegerScalar(42);

        writer.OnKeyedItem("question");
        writer.OnBeginAttributes();
            writer.OnKeyedItem("foo");
            writer.OnStringScalar("bar");
        writer.OnEndAttributes();
        writer.OnStringScalar("strange question");
    writer.OnEndMap();
    writer.Flush();

    Stroka output =
        "{"
            "\"answer\":42,"
            "\"question\":\"strange question\""
        "}";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonWriterTest, AlwaysAttributes)
{
    TStringStream outputStream;
    auto config = New<TJsonFormatConfig>();
    config->AttributesMode = EJsonAttributesMode::Always;
    TJsonWriter writer(&outputStream, config);

    writer.OnBeginAttributes();
        writer.OnKeyedItem("foo");
        writer.OnStringScalar("bar");
    writer.OnEndAttributes();

    writer.OnBeginMap();
        writer.OnKeyedItem("answer");
        writer.OnIntegerScalar(42);

        writer.OnKeyedItem("question");
        writer.OnBeginAttributes();
            writer.OnKeyedItem("foo");
            writer.OnStringScalar("bar");
        writer.OnEndAttributes();
        writer.OnStringScalar("strange question");
    writer.OnEndMap();
    writer.Flush();

    Stroka output =
        "{"
            "\"$attributes\":{\"foo\":{\"$attributes\":{},\"$value\":\"bar\"}},"
            "\"$value\":"
            "{"
                "\"answer\":{\"$attributes\":{},\"$value\":42},"
                "\"question\":"
                "{"
                    "\"$attributes\":{\"foo\":{\"$attributes\":{},\"$value\":\"bar\"}},"
                    "\"$value\":\"strange question\""
                "}"
            "}"
        "}";
    EXPECT_EQ(output, outputStream.Str());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
