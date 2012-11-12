#include "stdafx.h"

#include <ytlib/formats/dsv_writer.h>
#include <ytlib/formats/dsv_parser.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace NFormats {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TDsvWriterTest, SimpleTabular)
{
    TStringStream outputStream;
    TDsvWriter writer(&outputStream);

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("integer");
        writer.OnIntegerScalar(42);
        writer.OnKeyedItem("string");
        writer.OnStringScalar("some");
        writer.OnKeyedItem("double");
        writer.OnDoubleScalar(10.);     // let's hope that 10. will be serialized as 10.
    writer.OnEndMap();
    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("foo");
        writer.OnStringScalar("bar");
        writer.OnKeyedItem("one");
        writer.OnIntegerScalar(1);
    writer.OnEndMap();

    Stroka output =
        "integer=42\tstring=some\tdouble=10.\n"
        "foo=bar\tone=1\n";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TDsvWriterTest, StringScalar)
{
    TStringStream outputStream;
    TDsvWriter writer(&outputStream, EYsonType::Node);

    writer.OnStringScalar("0-2-xb-1234");
    EXPECT_EQ("0-2-xb-1234", outputStream.Str());
}

TEST(TDsvWriterTest, ListContainingDifferentTypes)
{
    TStringStream outputStream;
    TDsvWriter writer(&outputStream, EYsonType::Node);

    writer.OnBeginList();
    writer.OnListItem();
    writer.OnIntegerScalar(100);
    writer.OnListItem();
    writer.OnStringScalar("foo");
    writer.OnListItem();
    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("a");
        writer.OnStringScalar("10");
        writer.OnKeyedItem("b");
        writer.OnStringScalar("c");
    writer.OnEndMap();
    writer.OnEndList();

    Stroka output =
        "100\n"
        "foo\n"
        "\n"
        "a=10\tb=c\n";

    Cout << outputStream.Str();
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TDsvWriterTest, ListInsideList)
{
    TStringStream outputStream;
    TDsvWriter writer(&outputStream, EYsonType::Node);

    writer.OnBeginList();
    writer.OnListItem();
    EXPECT_ANY_THROW(writer.OnBeginList());
}

TEST(TDsvWriterTest, ListInsideMap)
{
    TStringStream outputStream;
    TDsvWriter writer(&outputStream, EYsonType::Node);

    writer.OnBeginMap();
    writer.OnKeyedItem("foo");
    EXPECT_ANY_THROW(writer.OnBeginList());
}

TEST(TDsvWriterTest, MapInsideMap)
{
    TStringStream outputStream;
    TDsvWriter writer(&outputStream, EYsonType::Node);

    writer.OnBeginMap();
    writer.OnKeyedItem("foo");
    EXPECT_ANY_THROW(writer.OnBeginMap());
}

TEST(TDsvWriterTest, WithoutEsacping)
{
    auto config = New<TDsvFormatConfig>();
    config->EnableEscaping = false;

    TStringStream outputStream;
    TDsvWriter writer(&outputStream, EYsonType::Node, config);

    writer.OnStringScalar("string_with_\t_\\_=_and_\n");

    Stroka output = "string_with_\t_\\_=_and_\n";

    EXPECT_EQ(outputStream.Str(), output);
}

////////////////////////////////////////////////////////////////////////////////
// OnRaw tests:

TEST(TDsvWriterTest, TabularUsingOnRaw)
{
    TStringStream outputStream;
    TDsvWriter writer(&outputStream);

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("integer");
        writer.OnRaw("42", EYsonType::Node);
        writer.OnKeyedItem("string");
        writer.OnRaw("some", EYsonType::Node);
        writer.OnKeyedItem("double");
        writer.OnRaw("10.", EYsonType::Node);
    writer.OnEndMap();
    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("foo");
        writer.OnRaw("bar", EYsonType::Node);
        writer.OnKeyedItem("one");
        writer.OnRaw("1", EYsonType::Node);
    writer.OnEndMap();

    Stroka output =
        "integer=42\tstring=some\tdouble=10.\n"
        "foo=bar\tone=1\n";

    EXPECT_EQ(output, outputStream.Str());
}

TEST(TDsvWriterTest, ListUsingOnRaw)
{
    TStringStream outputStream;
    TDsvWriter writer(&outputStream, EYsonType::Node);

    writer.OnRaw("[10; 20; 30]", EYsonType::Node);
    Stroka output =
        "10\n"
        "20\n"
        "30\n";

    EXPECT_EQ(output, outputStream.Str());
}

TEST(TDsvWriterTest, MapUsingOnRaw)
{
    TStringStream outputStream;
    TDsvWriter writer(&outputStream, EYsonType::Node);

    writer.OnRaw("{a=b; c=d}", EYsonType::Node);
    Stroka output = "a=b\tc=d";

    EXPECT_EQ(output, outputStream.Str());
}


TEST(TDsvWriterTest, ListInTable)
{
    TStringStream outputStream;
    TDsvWriter writer(&outputStream);

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("value");

    EXPECT_ANY_THROW(writer.OnRaw("[10; 20; 30]", EYsonType::Node));
}

TEST(TDsvWriterTest, MapInTable)
{
    TStringStream outputStream;
    TDsvWriter writer(&outputStream);

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("value");

    EXPECT_ANY_THROW(writer.OnRaw("{a=10}", EYsonType::Node));
}

TEST(TDsvWriterTest, AttributesInTable)
{
    TStringStream outputStream;
    TDsvWriter writer(&outputStream);

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("value");

    EXPECT_ANY_THROW(writer.OnRaw("<a=10>string", EYsonType::Node));
}

TEST(TDsvWriterTest, EntityInTable)
{
    TStringStream outputStream;
    TDsvWriter writer(&outputStream);

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("value");

    EXPECT_ANY_THROW(writer.OnRaw("#", EYsonType::Node));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TTskvWriterTest, SimpleTabular)
{
    auto config = New<TDsvFormatConfig>();
    config->LinePrefix = "tskv";

    TStringStream outputStream;
    TDsvWriter writer(&outputStream, EYsonType::ListFragment, config);

    writer.OnListItem();
    writer.OnBeginMap();
    writer.OnEndMap();

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("id");
        writer.OnStringScalar("1");
        writer.OnKeyedItem("guid");
        writer.OnIntegerScalar(100500);
    writer.OnEndMap();

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("id");
        writer.OnStringScalar("2");
        writer.OnKeyedItem("guid");
        writer.OnIntegerScalar(20025);
    writer.OnEndMap();

    Stroka output =
        "tskv\n"
        "tskv\tid=1\tguid=100500\n"
        "tskv\tid=2\tguid=20025\n";

    EXPECT_EQ(outputStream.Str(), output);
}

TEST(TTskvWriterTest, Escaping)
{
    auto config = New<TDsvFormatConfig>();
    config->LinePrefix = "tskv";

    TStringStream outputStream;
    TDsvWriter writer(&outputStream, EYsonType::ListFragment, config);

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem(Stroka("\0 is escaped", 12));
        writer.OnStringScalar(Stroka("\0 is escaped", 12));
        writer.OnKeyedItem("Escaping in in key: \t \n \\ =");
        writer.OnStringScalar("Escaping in value: \t \n \\ =");
    writer.OnEndMap();

    Stroka output =
        "tskv"
        "\t"

        "\\0 is escaped"
        "="
        "\\0 is escaped"

        "\t"

        "Escaping in in key: \\t \\n \\\\ \\="
        "="
        "Escaping in value: \\t \\n \\\\ =" // Note: = is not escaped

        "\n";

    Cout << outputStream.Str();
    Cout << output;

    EXPECT_EQ(outputStream.Str(), output);
}

TEST(TTskvWriterTest, EscapingOfCustomSeparator)
{
    auto config = New<TDsvFormatConfig>();
    config->KeyValueSeparator = ':';

    TStringStream outputStreamA;
    TDsvWriter writerA(&outputStreamA, EYsonType::ListFragment, config);

    writerA.OnListItem();
    writerA.OnBeginMap();
        writerA.OnKeyedItem(Stroka("=my\\:key"));
        writerA.OnStringScalar(Stroka("42"));
    writerA.OnEndMap();

    TStringStream outputStreamB;
    TDsvWriter writerB(&outputStreamB, EYsonType::ListFragment, config);
    ParseDsv(outputStreamA.Str(), &writerB);

    EXPECT_EQ(outputStreamA.Str(), outputStreamB.Str());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
