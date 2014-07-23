#include "stdafx.h"
#include "framework.h"

#include <ytlib/formats/dsv_writer.h>
#include <ytlib/formats/dsv_parser.h>

namespace NYT {
namespace NFormats {
namespace {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TEST(TDsvWriterTest, SimpleTabular)
{
    TStringStream outputStream;
    TDsvTabularConsumer consumer(&outputStream);

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("integer");
        consumer.OnInt64Scalar(42);
        consumer.OnKeyedItem("string");
        consumer.OnStringScalar("some");
        consumer.OnKeyedItem("double");
        consumer.OnDoubleScalar(10.);     // let's hope that 10. will be serialized as 10.
    consumer.OnEndMap();
    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("foo");
        consumer.OnStringScalar("bar");
        consumer.OnKeyedItem("one");
        consumer.OnInt64Scalar(1);
    consumer.OnEndMap();

    Stroka output =
        "integer=42\tstring=some\tdouble=10.\n"
        "foo=bar\tone=1\n";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TDsvWriterTest, TabularWithAttributes)
{
    TStringStream outputStream;
    TDsvTabularConsumer consumer(&outputStream);

    consumer.OnListItem();
    consumer.OnBeginAttributes();
    EXPECT_ANY_THROW(consumer.OnKeyedItem("index"));
}

TEST(TDsvWriterTest, StringScalar)
{
    TStringStream outputStream;
    TDsvNodeConsumer consumer(&outputStream);

    consumer.OnStringScalar("0-2-xb-1234");
    EXPECT_EQ("0-2-xb-1234", outputStream.Str());
}

TEST(TDsvWriterTest, ListContainingDifferentTypes)
{
    TStringStream outputStream;
    TDsvNodeConsumer consumer(&outputStream);

    consumer.OnBeginList();
    consumer.OnListItem();
    consumer.OnInt64Scalar(100);
    consumer.OnListItem();
    consumer.OnStringScalar("foo");
    consumer.OnListItem();
    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("a");
        consumer.OnStringScalar("10");
        consumer.OnKeyedItem("b");
        consumer.OnStringScalar("c");
    consumer.OnEndMap();
    consumer.OnEndList();

    Stroka output =
        "100\n"
        "foo\n"
        "\n"
        "a=10\tb=c\n";

    EXPECT_EQ(output, outputStream.Str());
}

TEST(TDsvWriterTest, ListInsideList)
{
    TStringStream outputStream;
    TDsvNodeConsumer consumer(&outputStream);

    consumer.OnBeginList();
    consumer.OnListItem();
    EXPECT_ANY_THROW(consumer.OnBeginList());
}

TEST(TDsvWriterTest, ListInsideMap)
{
    TStringStream outputStream;
    TDsvNodeConsumer consumer(&outputStream);

    consumer.OnBeginMap();
    consumer.OnKeyedItem("foo");
    EXPECT_ANY_THROW(consumer.OnBeginList());
}

TEST(TDsvWriterTest, MapInsideMap)
{
    TStringStream outputStream;
    TDsvNodeConsumer consumer(&outputStream);

    consumer.OnBeginMap();
    consumer.OnKeyedItem("foo");
    EXPECT_ANY_THROW(consumer.OnBeginMap());
}

TEST(TDsvWriterTest, WithoutEsacping)
{
    auto config = New<TDsvFormatConfig>();
    config->EnableEscaping = false;

    TStringStream outputStream;
    TDsvNodeConsumer consumer(&outputStream, config);

    consumer.OnStringScalar("string_with_\t_\\_=_and_\n");

    Stroka output = "string_with_\t_\\_=_and_\n";

    EXPECT_EQ(output, outputStream.Str());
}

////////////////////////////////////////////////////////////////////////////////
// OnRaw tests:

TEST(TDsvWriterTest, TabularUsingOnRaw)
{
    TStringStream outputStream;
    auto config = New<TDsvFormatConfig>();
    config->EnableTableIndex = true;
    TDsvTabularConsumer consumer(&outputStream, config);

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("integer");
        consumer.OnRaw("42", EYsonType::Node);
        consumer.OnKeyedItem("string");
        consumer.OnRaw("some", EYsonType::Node);
        consumer.OnKeyedItem("double");
        consumer.OnRaw("10.", EYsonType::Node);
    consumer.OnEndMap();
    consumer.OnListItem();
    consumer.OnBeginAttributes();
    consumer.OnKeyedItem("table_index");
    consumer.OnInt64Scalar(2);
    consumer.OnEndAttributes();
    consumer.OnEntity();
    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("foo");
        consumer.OnRaw("bar", EYsonType::Node);
        consumer.OnKeyedItem("one");
        consumer.OnRaw("1", EYsonType::Node);
    consumer.OnEndMap();

    Stroka output =
        "integer=42\tstring=some\tdouble=10.\t@table_index=0\n"
        "foo=bar\tone=1\t@table_index=2\n";

    EXPECT_EQ(output, outputStream.Str());
}

TEST(TDsvWriterTest, ListUsingOnRaw)
{
    TStringStream outputStream;
    TDsvNodeConsumer consumer(&outputStream);

    consumer.OnRaw("[10; 20; 30]", EYsonType::Node);
    Stroka output =
        "10\n"
        "20\n"
        "30\n";

    EXPECT_EQ(output, outputStream.Str());
}

TEST(TDsvWriterTest, MapUsingOnRaw)
{
    TStringStream outputStream;
    TDsvNodeConsumer consumer(&outputStream);

    consumer.OnRaw("{a=b; c=d}", EYsonType::Node);
    Stroka output = "a=b\tc=d";

    EXPECT_EQ(output, outputStream.Str());
}


TEST(TDsvWriterTest, ListInTable)
{
    TStringStream outputStream;
    TDsvTabularConsumer consumer(&outputStream);

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("value");

    EXPECT_ANY_THROW(consumer.OnRaw("[10; 20; 30]", EYsonType::Node));
}

TEST(TDsvWriterTest, MapInTable)
{
    TStringStream outputStream;
    TDsvTabularConsumer consumer(&outputStream);

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("value");

    EXPECT_ANY_THROW(consumer.OnRaw("{a=10}", EYsonType::Node));
}

TEST(TDsvWriterTest, AttributesInTable)
{
    TStringStream outputStream;
    TDsvTabularConsumer consumer(&outputStream);

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("value");

    EXPECT_ANY_THROW(consumer.OnRaw("<a=10>string", EYsonType::Node));
}

TEST(TDsvWriterTest, EntityInTable)
{
    TStringStream outputStream;
    TDsvTabularConsumer consumer(&outputStream);

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("value");

    EXPECT_ANY_THROW(consumer.OnRaw("#", EYsonType::Node));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TTskvWriterTest, SimpleTabular)
{
    auto config = New<TDsvFormatConfig>();
    config->LinePrefix = "tskv";

    TStringStream outputStream;
    TDsvTabularConsumer consumer(&outputStream, config);

    consumer.OnListItem();
    consumer.OnBeginMap();
    consumer.OnEndMap();

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("id");
        consumer.OnStringScalar("1");
        consumer.OnKeyedItem("guid");
        consumer.OnInt64Scalar(100500);
    consumer.OnEndMap();

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("id");
        consumer.OnStringScalar("2");
        consumer.OnKeyedItem("guid");
        consumer.OnInt64Scalar(20025);
    consumer.OnEndMap();

    Stroka output =
        "tskv\n"
        "tskv\tid=1\tguid=100500\n"
        "tskv\tid=2\tguid=20025\n";

    EXPECT_EQ(output, outputStream.Str());
}

TEST(TTskvWriterTest, Escaping)
{
    auto config = New<TDsvFormatConfig>();
    config->LinePrefix = "tskv";

    TStringStream outputStream;
    TDsvTabularConsumer consumer(&outputStream, config);

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem(Stroka("\0 is escaped", 12));
        consumer.OnStringScalar(Stroka("\0 is escaped", 12));
        consumer.OnKeyedItem("Escaping in in key: \r \t \n \\ =");
        consumer.OnStringScalar("Escaping in value: \r \t \n \\ =");
    consumer.OnEndMap();

    Stroka output =
        "tskv"
        "\t"

        "\\0 is escaped"
        "="
        "\\0 is escaped"

        "\t"

        "Escaping in in key: \\r \\t \\n \\\\ \\="
        "="
        "Escaping in value: \\r \\t \\n \\\\ =" // Note: = is not escaped

        "\n";

    EXPECT_EQ(output, outputStream.Str());
}

TEST(TTskvWriterTest, EscapingOfCustomSeparator)
{
    auto config = New<TDsvFormatConfig>();
    config->KeyValueSeparator = ':';

    TStringStream outputStreamA;
    TDsvTabularConsumer writerA(&outputStreamA, config);

    writerA.OnListItem();
    writerA.OnBeginMap();
        writerA.OnKeyedItem(Stroka("=my\\:key"));
        writerA.OnStringScalar(Stroka("42"));
    writerA.OnEndMap();

    TStringStream outputStreamB;
    TDsvTabularConsumer writerB(&outputStreamB, config);
    ParseDsv(outputStreamA.Str(), &writerB, config);

    EXPECT_EQ(outputStreamA.Str(), outputStreamB.Str());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NDriver
} // namespace NYT
