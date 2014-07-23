#include "stdafx.h"
#include "framework.h"

#include <ytlib/formats/json_writer.h>

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
    auto consumer = CreateJsonConsumer(&outputStream);

    consumer->OnBeginList();
        consumer->OnListItem();
        consumer->OnInt64Scalar(1);
        consumer->OnListItem();
        consumer->OnStringScalar("aaa");
        consumer->OnListItem();
        consumer->OnDoubleScalar(3.5);
    consumer->OnEndList();

    Stroka output = "[1,\"aaa\",3.5]";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonWriterTest, Map)
{
    TStringStream outputStream;
    auto consumer = CreateJsonConsumer(&outputStream);

    consumer->OnBeginMap();
        consumer->OnKeyedItem("hello");
        consumer->OnStringScalar("world");
        consumer->OnKeyedItem("foo");
        consumer->OnStringScalar("bar");
    consumer->OnEndMap();

    Stroka output = "{\"hello\":\"world\",\"foo\":\"bar\"}";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonWriterTest, DoubleMap)
{
    TStringStream outputStream;
    auto consumer = CreateJsonConsumer(&outputStream, NYson::EYsonType::ListFragment);

    consumer->OnListItem();
    consumer->OnBeginMap();
        consumer->OnKeyedItem("hello");
        consumer->OnStringScalar("world");
    consumer->OnEndMap();
    consumer->OnListItem();
    consumer->OnBeginMap();
        consumer->OnKeyedItem("foo");
        consumer->OnStringScalar("bar");
    consumer->OnEndMap();

    Stroka output = "{\"hello\":\"world\"}\n{\"foo\":\"bar\"}\n";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonWriterTest, ListFragmentWithEntity)
{
    TStringStream outputStream;
    auto consumer = CreateJsonConsumer(&outputStream, NYson::EYsonType::ListFragment);

    consumer->OnListItem();
    consumer->OnBeginAttributes();
        consumer->OnKeyedItem("x");
        consumer->OnStringScalar("y");
    consumer->OnEndAttributes();
    consumer->OnEntity();
    consumer->OnListItem();
    consumer->OnBeginMap();
        consumer->OnKeyedItem("hello");
        consumer->OnStringScalar("world");
    consumer->OnEndMap();
    consumer->OnListItem();
    consumer->OnBeginMap();
        consumer->OnKeyedItem("foo");
        consumer->OnStringScalar("bar");
    consumer->OnEndMap();

    Stroka output = "{\"$attributes\":{\"x\":\"y\"},\"$value\":null}\n{\"hello\":\"world\"}\n{\"foo\":\"bar\"}\n";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonWriterTest, Entity)
{
    TStringStream outputStream;
    auto consumer = CreateJsonConsumer(&outputStream);

    consumer->OnEntity();

    Stroka output = "null";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonWriterTest, EmptyString)
{
    TStringStream outputStream;
    auto consumer = CreateJsonConsumer(&outputStream);

    consumer->OnStringScalar("");

    Stroka output = SurroundWithQuotes("");
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonWriterTest, AsciiString)
{
    TStringStream outputStream;
    auto consumer = CreateJsonConsumer(&outputStream);

    Stroka s = Stroka("\x7F\x32", 2);
    consumer->OnStringScalar(s);

    Stroka output = SurroundWithQuotes(s);
    EXPECT_EQ(output, outputStream.Str());
}


TEST(TJsonWriterTest, NonAsciiString)
{
    TStringStream outputStream;
    auto consumer = CreateJsonConsumer(&outputStream);

    Stroka s = Stroka("\xFF\x00\x80", 3);
    consumer->OnStringScalar(s);

    Stroka output = SurroundWithQuotes("\xC3\xBF\\u0000\xC2\x80");
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonWriterTest, NonAsciiStringWithoutEscaping)
{
    TStringStream outputStream;
    auto config = New<TJsonFormatConfig>();
    config->EncodeUtf8 = false;
    auto consumer = CreateJsonConsumer(&outputStream, EYsonType::Node, config);

    Stroka s = Stroka("\xC3\xBF", 2);
    consumer->OnStringScalar(s);

    Stroka output = SurroundWithQuotes(Stroka("\xC3\xBF", 2));
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonWriterTest, IncorrectUtfWithoutEscaping)
{
    TStringStream outputStream;
    auto config = New<TJsonFormatConfig>();
    config->EncodeUtf8 = false;
    auto consumer = CreateJsonConsumer(&outputStream, EYsonType::Node, config);

    Stroka s = Stroka("\xFF", 1);
    EXPECT_ANY_THROW(
        consumer->OnStringScalar(s);
    );
}

TEST(TJsonWriterTest, StringStartingWithSpecailSymbol)
{
    TStringStream outputStream;
    auto consumer = CreateJsonConsumer(&outputStream);

    Stroka s = "&some_string";
    consumer->OnStringScalar(s);

    Stroka output = SurroundWithQuotes(s);
    EXPECT_EQ(output, outputStream.Str());
}

////////////////////////////////////////////////////////////////////////////////

// Values with attributes:
TEST(TJsonWriterTest, ListWithAttributes)
{
    TStringStream outputStream;
    auto consumer = CreateJsonConsumer(&outputStream);

    consumer->OnBeginAttributes();
        consumer->OnKeyedItem("foo");
        consumer->OnStringScalar("bar");
    consumer->OnEndAttributes();

    consumer->OnBeginList();
        consumer->OnListItem();
        consumer->OnInt64Scalar(1);
    consumer->OnEndList();

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
    auto consumer = CreateJsonConsumer(&outputStream);

    consumer->OnBeginAttributes();
        consumer->OnKeyedItem("foo");
        consumer->OnStringScalar("bar");
    consumer->OnEndAttributes();

    consumer->OnBeginMap();
        consumer->OnKeyedItem("spam");
        consumer->OnStringScalar("bad");
    consumer->OnEndMap();

    Stroka output =
        "{"
            "\"$attributes\":{\"foo\":\"bar\"}"
            ","
            "\"$value\":{\"spam\":\"bad\"}"
        "}";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonWriterTest, Int64WithAttributes)
{
    TStringStream outputStream;
    auto consumer = CreateJsonConsumer(&outputStream);

    consumer->OnBeginAttributes();
        consumer->OnKeyedItem("foo");
        consumer->OnStringScalar("bar");
    consumer->OnEndAttributes();

    consumer->OnInt64Scalar(42);

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
    auto consumer = CreateJsonConsumer(&outputStream);

    consumer->OnBeginAttributes();
        consumer->OnKeyedItem("foo");
        consumer->OnStringScalar("bar");
    consumer->OnEndAttributes();

    consumer->OnEntity();

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
    auto consumer = CreateJsonConsumer(&outputStream);

    consumer->OnBeginAttributes();
        consumer->OnKeyedItem("foo");
        consumer->OnStringScalar("bar");
    consumer->OnEndAttributes();

    consumer->OnStringScalar("some_string");

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
    auto consumer = CreateJsonConsumer(&outputStream);

    consumer->OnBeginAttributes();
        consumer->OnKeyedItem("foo");
        consumer->OnBeginAttributes();
            consumer->OnKeyedItem("another_foo");
            consumer->OnStringScalar("another_bar");
        consumer->OnEndAttributes();
        consumer->OnStringScalar("bar");
    consumer->OnEndAttributes();

    consumer->OnStringScalar("some_string");

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
    auto consumer = CreateJsonConsumer(&outputStream, EYsonType::Node, config);

    consumer->OnBeginAttributes();
        consumer->OnKeyedItem("foo");
        consumer->OnStringScalar("bar");
    consumer->OnEndAttributes();

    consumer->OnBeginMap();
        consumer->OnKeyedItem("answer");
        consumer->OnInt64Scalar(42);

        consumer->OnKeyedItem("question");
        consumer->OnBeginAttributes();
            consumer->OnKeyedItem("foo");
            consumer->OnStringScalar("bar");
        consumer->OnEndAttributes();
        consumer->OnStringScalar("strange question");
    consumer->OnEndMap();

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
    auto consumer = CreateJsonConsumer(&outputStream, EYsonType::Node, config);

    consumer->OnBeginAttributes();
        consumer->OnKeyedItem("foo");
        consumer->OnStringScalar("bar");
    consumer->OnEndAttributes();

    consumer->OnBeginMap();
        consumer->OnKeyedItem("answer");
        consumer->OnInt64Scalar(42);

        consumer->OnKeyedItem("question");
        consumer->OnBeginAttributes();
            consumer->OnKeyedItem("foo");
            consumer->OnStringScalar("bar");
        consumer->OnEndAttributes();
        consumer->OnStringScalar("strange question");
    consumer->OnEndMap();

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
        writer->OnInt64Scalar(42);
    writer->OnEndMap();

    Stroka output = "{\"$$value\":\"foo\",\"$$$attributes\":\"bar\",\"$$other\":42}";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TJsonWriterTest, TestStringLengthLimit)
{
    TStringStream outputStream;
    auto config = New<TJsonFormatConfig>();
    config->StringLengthLimit = 2;
    auto writer = CreateJsonConsumer(&outputStream, EYsonType::Node, config);

    writer->OnBeginMap();
        writer->OnKeyedItem("hello");
        writer->OnStringScalar(Stroka(10000, 'A'));
    writer->OnEndMap();

    Stroka output = "{\"hello\":{\"$attributes\":{\"incomplete\":\"true\"},\"$value\":\"AA\"}}";
    EXPECT_EQ(output, outputStream.Str());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NFormats
} // namespace NYT
