#include "stdafx.h"

#include <ytlib/formats/json_writer.h>

#include <util/string/base64.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

Stroka SurroundWithQuotes(const Stroka& s)
{
    Stroka quote = "\"";
    return quote + s + quote;
}

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

TEST(TJsonWriterTest, ValidUtf8String)
{
    TStringStream outputStream;
    TJsonWriter writer(&outputStream);

    Stroka s = Stroka("\xCF\x8F", 2); // (110)0 1111 (10)00 1111 -- valid code points
    writer.OnStringScalar(s);
    writer.Flush();

    Stroka output = SurroundWithQuotes(s);

    Cout << outputStream.Str() << Endl;
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
    Cout << outputStream.Str() << Endl;
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

    Cout << outputStream.Str() << Endl;
    EXPECT_EQ(output, outputStream.Str());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
