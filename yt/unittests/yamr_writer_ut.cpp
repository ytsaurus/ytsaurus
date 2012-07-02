#include "stdafx.h"

#include <ytlib/formats/yamr_writer.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

TEST(TYamrWriterTest, Simple)
{
    TStringStream outputStream;
    TYamrWriter writer(&outputStream);

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("k");
        writer.OnStringScalar("key1");
        writer.OnKeyedItem("v");
        writer.OnStringScalar("value1");
    writer.OnEndMap();

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("k");
        writer.OnStringScalar("key2");
        writer.OnKeyedItem("v");
        writer.OnStringScalar("value2");
    writer.OnEndMap();

    Stroka output =
        "key1\tvalue1\n"
        "key2\tvalue2\n";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TYamrWriterTest, SimpleWithSubkey)
{
    TStringStream outputStream;
    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = true;
    TYamrWriter writer(&outputStream, config);

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("k");
        writer.OnStringScalar("key1");
        writer.OnKeyedItem("sk");
        writer.OnStringScalar("subkey1");
        writer.OnKeyedItem("v");
        writer.OnStringScalar("value1");
    writer.OnEndMap();

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("k");
        writer.OnStringScalar("key2");
        writer.OnKeyedItem("sk");
        writer.OnStringScalar("subkey2");
        writer.OnKeyedItem("v");
        writer.OnStringScalar("value2");
    writer.OnEndMap();

    Stroka output =
        "key1\tsubkey1\tvalue1\n"
        "key2\tsubkey2\tvalue2\n";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TYamrWriterTest, NonStringValues)
{
    TStringStream outputStream;
    TYamrWriter writer(&outputStream);

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("k");
        writer.OnStringScalar("integer");
        writer.OnKeyedItem("v");
        writer.OnIntegerScalar(42);
    writer.OnEndMap();

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("k");
        writer.OnStringScalar("double");
        writer.OnKeyedItem("v");
        writer.OnDoubleScalar(10);
    writer.OnEndMap();

    Stroka output =
        "integer\t42\n"
        "double\t10.\n";

    EXPECT_EQ(output, outputStream.Str());
}


TEST(TYamrWriterTest, SkippedValues)
{
    TStringStream outputStream;
    TYamrWriter writer(&outputStream);

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("k");
        writer.OnStringScalar("foo");
        writer.OnKeyedItem("v");
        writer.OnStringScalar("bar");
    writer.OnEndMap();

    writer.OnListItem();
    writer.OnBeginMap();
    writer.OnEndMap();

    Stroka output =
        "foo\tbar\n"
        "\t\n";

    EXPECT_EQ(output, outputStream.Str());
}

TEST(TYamrWriterTest, Lenval)
{
    TStringStream outputStream;
    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = true;
    config->Lenval = true;
    TYamrWriter writer(&outputStream, config);


    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("k");
        writer.OnStringScalar("key1");
        writer.OnKeyedItem("sk");
        writer.OnStringScalar("subkey1");
        writer.OnKeyedItem("v");
        writer.OnStringScalar("value1");
    writer.OnEndMap();

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("k");
        writer.OnStringScalar("key2");
        writer.OnKeyedItem("sk");
        writer.OnStringScalar("subkey2");
        writer.OnKeyedItem("v");
        writer.OnStringScalar("value2");
    writer.OnEndMap();

    Stroka output = Stroka(
        "\x04\x00\x00\x00" "key1"
        "\x07\x00\x00\x00" "subkey1"
        "\x06\x00\x00\x00" "value1"

        "\x04\x00\x00\x00" "key2"
        "\x07\x00\x00\x00" "subkey2"
        "\x06\x00\x00\x00" "value2"
        , 2 * (3 * 4 + 4 + 6 + 7) // all i32 + lengths of keys
    );

    EXPECT_EQ(output, outputStream.Str());
}

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NFormats
} // namespace NYT
