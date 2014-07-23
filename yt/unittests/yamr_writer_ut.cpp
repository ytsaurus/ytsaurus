#include "stdafx.h"
#include "framework.h"

#include <ytlib/formats/yamr_writer.h>

namespace NYT {
namespace NFormats {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TYamrWriterTest, Simple)
{
    TStringStream outputStream;
    TYamrConsumer consumer(&outputStream);

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("key");
        consumer.OnStringScalar("key1");
        consumer.OnKeyedItem("value");
        consumer.OnStringScalar("value1");
    consumer.OnEndMap();

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("key");
        consumer.OnStringScalar("key2");
        consumer.OnKeyedItem("value");
        consumer.OnStringScalar("value2");
    consumer.OnEndMap();

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
    TYamrConsumer consumer(&outputStream, config);

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("key");
        consumer.OnStringScalar("key1");
        consumer.OnKeyedItem("subkey");
        consumer.OnStringScalar("subkey1");
        consumer.OnKeyedItem("value");
        consumer.OnStringScalar("value1");
    consumer.OnEndMap();

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("key");
        consumer.OnStringScalar("key2");
        consumer.OnKeyedItem("subkey");
        consumer.OnStringScalar("subkey2");
        consumer.OnKeyedItem("value");
        consumer.OnStringScalar("value2");
    consumer.OnEndMap();

    Stroka output =
        "key1\tsubkey1\tvalue1\n"
        "key2\tsubkey2\tvalue2\n";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TYamrWriterTest, WritingWithoutSubkey)
{
    TStringStream outputStream;
    TYamrConsumer consumer(&outputStream);

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("key");
        consumer.OnStringScalar("key1");
        consumer.OnKeyedItem("subkey");
        consumer.OnStringScalar("subkey1");
        consumer.OnKeyedItem("value");
        consumer.OnStringScalar("value1");
    consumer.OnEndMap();

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("key");
        consumer.OnStringScalar("key2");
        consumer.OnKeyedItem("subkey");
        consumer.OnStringScalar("subkey2");
        consumer.OnKeyedItem("value");
        consumer.OnStringScalar("value2");
    consumer.OnEndMap();

    Stroka output =
        "key1\tvalue1\n"
        "key2\tvalue2\n";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TYamrWriterTest, SkippedKey)
{
    TStringStream outputStream;
    TYamrConsumer consumer(&outputStream);

    auto DoWrite = [&]() {
        consumer.OnListItem();
        consumer.OnBeginMap();
            consumer.OnKeyedItem("key");
            consumer.OnStringScalar("foo");
        consumer.OnEndMap();
    };

    EXPECT_THROW(DoWrite(), std::exception);
}

TEST(TYamrWriterTest, SkippedValue)
{
    TStringStream outputStream;
    TYamrConsumer consumer(&outputStream);

    auto DoWrite = [&]() {
        consumer.OnListItem();
        consumer.OnBeginMap();
            consumer.OnKeyedItem("value");
            consumer.OnStringScalar("bar");
        consumer.OnEndMap();
    };

    EXPECT_THROW(DoWrite(), std::exception);
}

TEST(TYamrWriterTest, SubkeyCouldBeSkipped)
{
    TStringStream outputStream;
    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = true;
    TYamrConsumer consumer(&outputStream, config);

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("value");
        consumer.OnStringScalar("bar");
        consumer.OnKeyedItem("key");
        consumer.OnStringScalar("foo");
    consumer.OnEndMap();

    Stroka output = "foo\t\tbar\n";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TYamrWriterTest, Escaping)
{
    TStringStream outputStream;
    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = true;
    config->EnableEscaping = true;
    TYamrConsumer consumer(&outputStream, config);

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("value");
        consumer.OnStringScalar("\n");
        consumer.OnKeyedItem("subkey");
        consumer.OnStringScalar("\t");
        consumer.OnKeyedItem("key");
        consumer.OnStringScalar("\n");
    consumer.OnEndMap();

    Stroka output = "\\n\t\\t\t\\n\n";
    EXPECT_EQ(output, outputStream.Str());
}

TEST(TYamrWriterTest, SimpleWithTableIndex)
{
    TStringStream outputStream;
    auto config = New<TYamrFormatConfig>();
    config->EnableTableIndex = true;
    TYamrConsumer consumer(&outputStream, config);

    consumer.OnListItem();
    consumer.OnBeginAttributes();
        consumer.OnKeyedItem("table_index");
        consumer.OnInt64Scalar(1);
    consumer.OnEndAttributes();
    consumer.OnEntity();

    consumer.OnBeginMap();
        consumer.OnKeyedItem("key");
        consumer.OnStringScalar("key1");
        consumer.OnKeyedItem("value");
        consumer.OnStringScalar("value1");
    consumer.OnEndMap();

    Stroka output = Stroka("1\nkey1\tvalue1\n");

    EXPECT_EQ(output, outputStream.Str());
}

TEST(TYamrWriterTest, Lenval)
{
    TStringStream outputStream;
    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = true;
    config->Lenval = true;
    TYamrConsumer consumer(&outputStream, config);


    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("key");
        consumer.OnStringScalar("key1");
        consumer.OnKeyedItem("subkey");
        consumer.OnStringScalar("subkey1");
        consumer.OnKeyedItem("value");
        consumer.OnStringScalar("value1");
    consumer.OnEndMap();

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("key");
        consumer.OnStringScalar("key2");
        consumer.OnKeyedItem("subkey");
        consumer.OnStringScalar("subkey2");
        consumer.OnKeyedItem("value");
        consumer.OnStringScalar("value2");
    consumer.OnEndMap();

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

TEST(TYamrWriterTest, LenvalWithoutFields)
{
    TStringStream outputStream;
    auto config = New<TYamrFormatConfig>();
    config->HasSubkey = true;
    config->Lenval = true;
    TYamrConsumer consumer(&outputStream, config);

    // Note: order is unusual (value, key)
    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("value");
        consumer.OnStringScalar("value1");
        consumer.OnKeyedItem("subkey");
        consumer.OnStringScalar("");
        consumer.OnKeyedItem("key");
        consumer.OnStringScalar("key1");
    consumer.OnEndMap();

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("subkey");
        consumer.OnStringScalar("subkey2");
        consumer.OnKeyedItem("value");
        consumer.OnStringScalar("");
        consumer.OnKeyedItem("key");
        consumer.OnStringScalar("key2");
    consumer.OnEndMap();

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("value");
        consumer.OnStringScalar("value3");
        consumer.OnKeyedItem("key");
        consumer.OnStringScalar("");
        consumer.OnKeyedItem("subkey");
        consumer.OnStringScalar("subkey3");
    consumer.OnEndMap();

    Stroka output = Stroka(
        "\x04\x00\x00\x00" "key1"
        "\x00\x00\x00\x00" ""
        "\x06\x00\x00\x00" "value1"

        "\x04\x00\x00\x00" "key2"
        "\x07\x00\x00\x00" "subkey2"
        "\x00\x00\x00\x00" ""

        "\x00\x00\x00\x00" ""
        "\x07\x00\x00\x00" "subkey3"
        "\x06\x00\x00\x00" "value3"

        , 9 * 4 + (4 + 6) + (4 + 7) + (7 + 6) // all i32 + lengths of keys
    );

    EXPECT_EQ(output, outputStream.Str());
}

TEST(TYamrWriterTest, LenvalWithTableIndex)
{
    TStringStream outputStream;
    auto config = New<TYamrFormatConfig>();
    config->Lenval = true;
    config->EnableTableIndex = true;
    TYamrConsumer consumer(&outputStream, config);

    consumer.OnListItem();
    consumer.OnBeginAttributes();
        consumer.OnKeyedItem("table_index");
        consumer.OnInt64Scalar(0);
    consumer.OnEndAttributes();
    consumer.OnEntity();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("key");
        consumer.OnStringScalar("key1");
        consumer.OnKeyedItem("value");
        consumer.OnStringScalar("value1");
    consumer.OnEndMap();

    Stroka output = Stroka(
        "\xff\xff\xff\xff" "\x00\x00\x00\x00"
        "\x04\x00\x00\x00" "key1"
        "\x06\x00\x00\x00" "value1"
        , 4 + 4 + 4 + 4 + 4 + 6
    );

    EXPECT_EQ(output, outputStream.Str());
}

TEST(TYamrWriterTest, IntegerAndDoubleValues)
{
    TStringStream outputStream;
    auto config = New<TYamrFormatConfig>();
    TYamrConsumer consumer(&outputStream, config);

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("key");
        consumer.OnInt64Scalar(1);
        consumer.OnKeyedItem("value");
        consumer.OnDoubleScalar(1.5);
    consumer.OnEndMap();

    Stroka output("1\t1.5\n");

    EXPECT_EQ(output, outputStream.Str());
}
////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NFormats
} // namespace NYT
