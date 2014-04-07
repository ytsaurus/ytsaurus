#include "stdafx.h"
#include "framework.h"

#include <core/formats/json_writer.h>

#include <util/string/base64.h>

namespace NYT {
namespace NFormats {
namespace {

using namespace NYson;

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
    auto writer = CreateJsonConsumer(&outputStream);

    writer->OnBeginList();
        writer->OnListItem();
        writer->OnIntegerScalar(1);
        writer->OnListItem();
        writer->OnStringScalar("aaa");
        writer->OnListItem();
        writer->OnDoubleScalar(3.5);
    writer->OnEndList();

    Stroka output = "[1,\"aaa\",3.5]";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonWriterTest, Map)
{
    TStringStream outputStream;
    auto writer = CreateJsonConsumer(&outputStream);

    writer->OnBeginMap();
        writer->OnKeyedItem("hello");
        writer->OnStringScalar("world");
        writer->OnKeyedItem("foo");
        writer->OnStringScalar("bar");
    writer->OnEndMap();

    Stroka output = "{\"hello\":\"world\",\"foo\":\"bar\"}";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonWriterTest, DoubleMap)
{
    TStringStream outputStream;
    auto writer = CreateJsonConsumer(&outputStream, NYson::EYsonType::ListFragment);

    writer->OnListItem();
    writer->OnBeginMap();
        writer->OnKeyedItem("hello");
        writer->OnStringScalar("world");
    writer->OnEndMap();
    writer->OnListItem();
    writer->OnBeginMap();
        writer->OnKeyedItem("foo");
        writer->OnStringScalar("bar");
    writer->OnEndMap();

    Stroka output = "{\"hello\":\"world\"}\n{\"foo\":\"bar\"}\n";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonWriterTest, Entity)
{
    TStringStream outputStream;
    auto writer = CreateJsonConsumer(&outputStream);

    writer->OnEntity();

    Stroka output = "null";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonWriterTest, EmptyString)
{
    TStringStream outputStream;
    auto writer = CreateJsonConsumer(&outputStream);

    writer->OnStringScalar("");

    Stroka output = SurroundWithQuotes("");
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonWriterTest, AsciiString)
{
    TStringStream outputStream;
    auto writer = CreateJsonConsumer(&outputStream);

    Stroka s = Stroka("\x7F\x32", 2);
    writer->OnStringScalar(s);

    Stroka output = SurroundWithQuotes(s);
    EXPECT_EQ(output, outputStream.Str());
}


TEST(TJsonWriterTest, NonAsciiString)
{
    TStringStream outputStream;
    auto writer = CreateJsonConsumer(&outputStream);

    Stroka s = Stroka("\xFF\x00\x80", 3);
    writer->OnStringScalar(s);

    Stroka output = SurroundWithQuotes("\xC3\xBF\\u0000\xC2\x80");
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonWriterTest, NonAsciiStringWithoutEscaping)
{
    TStringStream outputStream;
    auto config = New<TJsonFormatConfig>();
    config->EnableEscaping = false;
    auto writer = CreateJsonConsumer(&outputStream, EYsonType::Node, config);

    Stroka s = Stroka("\xC3\xBF", 2);
    writer->OnStringScalar(s);

    Stroka output = SurroundWithQuotes(Stroka("\xC3\xBF", 2));
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonWriterTest, IncorrectUtfWithoutEscaping)
{
    TStringStream outputStream;
    auto config = New<TJsonFormatConfig>();
    config->EnableEscaping = false;
    auto writer = CreateJsonConsumer(&outputStream, EYsonType::Node, config);

    Stroka s = Stroka("\xFF", 1);
    EXPECT_ANY_THROW(
        writer->OnStringScalar(s);
    );
}

TEST(TJsonWriterTest, StringStartingWithSpecailSymbol)
{
    TStringStream outputStream;
    auto writer = CreateJsonConsumer(&outputStream);

    Stroka s = "&some_string";
    writer->OnStringScalar(s);

    Stroka output = SurroundWithQuotes(s);
    EXPECT_EQ(output, outputStream.Str());
}

////////////////////////////////////////////////////////////////////////////////

// Values with attributes:
TEST(TJsonWriterTest, ListWithAttributes)
{
    TStringStream outputStream;
    auto writer = CreateJsonConsumer(&outputStream);

    writer->OnBeginAttributes();
        writer->OnKeyedItem("foo");
        writer->OnStringScalar("bar");
    writer->OnEndAttributes();

    writer->OnBeginList();
        writer->OnListItem();
        writer->OnIntegerScalar(1);
    writer->OnEndList();

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
    auto writer = CreateJsonConsumer(&outputStream);

    writer->OnBeginAttributes();
        writer->OnKeyedItem("foo");
        writer->OnStringScalar("bar");
    writer->OnEndAttributes();

    writer->OnBeginMap();
        writer->OnKeyedItem("spam");
        writer->OnStringScalar("bad");
    writer->OnEndMap();

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
    auto writer = CreateJsonConsumer(&outputStream);

    writer->OnBeginAttributes();
        writer->OnKeyedItem("foo");
        writer->OnStringScalar("bar");
    writer->OnEndAttributes();

    writer->OnIntegerScalar(42);

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
    auto writer = CreateJsonConsumer(&outputStream);

    writer->OnBeginAttributes();
        writer->OnKeyedItem("foo");
        writer->OnStringScalar("bar");
    writer->OnEndAttributes();

    writer->OnEntity();

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
    auto writer = CreateJsonConsumer(&outputStream);

    writer->OnBeginAttributes();
        writer->OnKeyedItem("foo");
        writer->OnStringScalar("bar");
    writer->OnEndAttributes();

    writer->OnStringScalar("some_string");

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
    auto writer = CreateJsonConsumer(&outputStream);

    writer->OnBeginAttributes();
        writer->OnKeyedItem("foo");
        writer->OnBeginAttributes();
            writer->OnKeyedItem("another_foo");
            writer->OnStringScalar("another_bar");
        writer->OnEndAttributes();
        writer->OnStringScalar("bar");
    writer->OnEndAttributes();

    writer->OnStringScalar("some_string");

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
    auto writer = CreateJsonConsumer(&outputStream, EYsonType::Node, config);

    writer->OnBeginAttributes();
        writer->OnKeyedItem("foo");
        writer->OnStringScalar("bar");
    writer->OnEndAttributes();

    writer->OnBeginMap();
        writer->OnKeyedItem("answer");
        writer->OnIntegerScalar(42);

        writer->OnKeyedItem("question");
        writer->OnBeginAttributes();
            writer->OnKeyedItem("foo");
            writer->OnStringScalar("bar");
        writer->OnEndAttributes();
        writer->OnStringScalar("strange question");
    writer->OnEndMap();

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
    auto writer = CreateJsonConsumer(&outputStream, EYsonType::Node, config);

    writer->OnBeginAttributes();
        writer->OnKeyedItem("foo");
        writer->OnStringScalar("bar");
    writer->OnEndAttributes();

    writer->OnBeginMap();
        writer->OnKeyedItem("answer");
        writer->OnIntegerScalar(42);

        writer->OnKeyedItem("question");
        writer->OnBeginAttributes();
            writer->OnKeyedItem("foo");
            writer->OnStringScalar("bar");
        writer->OnEndAttributes();
        writer->OnStringScalar("strange question");
    writer->OnEndMap();

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

TEST(TJsonWriterTest, SpecialKeys)
{
    TStringStream outputStream;
    auto writer = CreateJsonConsumer(&outputStream);

    writer->OnBeginMap();
        writer->OnKeyedItem("$value");
        writer->OnStringScalar("foo");
        writer->OnKeyedItem("$$attributes");
        writer->OnStringScalar("bar");
        writer->OnKeyedItem("$other");
        writer->OnIntegerScalar(42);
    writer->OnEndMap();

    Stroka output = "{\"$$value\":\"foo\",\"$$$attributes\":\"bar\",\"$$other\":42}";
    EXPECT_EQ(output, outputStream.Str());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NFormats
} // namespace NYT
