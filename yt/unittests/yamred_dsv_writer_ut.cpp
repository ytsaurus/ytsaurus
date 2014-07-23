#include "stdafx.h"
#include "framework.h"

#include <ytlib/formats/yamred_dsv_writer.h>

namespace NYT {
namespace NFormats {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TYamredDsvWriterTest, Simple)
{
    TStringStream outputStream;
    auto config = New<TYamredDsvFormatConfig>();
    config->HasSubkey = true;
    config->KeyColumnNames.push_back("key_a");
    config->KeyColumnNames.push_back("key_b");
    TYamredDsvConsumer consumer(&outputStream, config);

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("key_a");
        consumer.OnStringScalar("a");
        consumer.OnKeyedItem("key_b");
        consumer.OnStringScalar("b");
    consumer.OnEndMap();

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("key_b");
        consumer.OnStringScalar("1");
        consumer.OnKeyedItem("column");
        consumer.OnStringScalar("2");
        consumer.OnKeyedItem("subkey");
        consumer.OnStringScalar("3");
        consumer.OnKeyedItem("key_a");
        consumer.OnStringScalar("xxx");
    consumer.OnEndMap();

    Stroka output =
        "a b\t\t\n"
        "xxx 1\t\tcolumn=2\tsubkey=3\n";

    EXPECT_EQ(output, outputStream.Str());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYamredDsvWriterTest, TestLiveConditions)
{
    TStringStream outputStream;
    auto config = New<TYamredDsvFormatConfig>();
    config->KeyColumnNames.push_back("key_a");
    config->KeyColumnNames.push_back("key_b");
    TYamredDsvConsumer consumer(&outputStream, config);

    {
        Stroka keyA = "key_a";
        Stroka a = "a";
        Stroka keyB = "key_b";
        Stroka b = "b";
        consumer.OnListItem();
        consumer.OnBeginMap();
            consumer.OnKeyedItem(keyA);
            consumer.OnStringScalar(a);
            consumer.OnKeyedItem(keyB);
            consumer.OnStringScalar(b);
        consumer.OnEndMap();
    }

    // Make some allocations!
    for (int i = 0; i < 100; ++i) {
        std::vector<int> x(i);
        for (int j = 0; j < x.size(); ++j) {
            x[j] = j;
        }
    }

    {
        Stroka keyA = "key_a";
        Stroka a = "_a_";
        Stroka keyB = "key_b";
        Stroka b = "xbx";
        consumer.OnListItem();
        consumer.OnBeginMap();
            consumer.OnKeyedItem(keyA);
            consumer.OnStringScalar(a);
            consumer.OnKeyedItem(keyB);
            consumer.OnStringScalar(b);
        consumer.OnEndMap();
    }

    Stroka output =
        "a b\t\n"
        "_a_ xbx\t\n";

    EXPECT_EQ(output, outputStream.Str());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYamredDsvWriterTest, WithoutSubkey)
{
    TStringStream outputStream;
    auto config = New<TYamredDsvFormatConfig>();
    config->HasSubkey = false;
    config->KeyColumnNames.push_back("key_a");
    config->KeyColumnNames.push_back("key_b");
    config->SubkeyColumnNames.push_back("subkey");
    TYamredDsvConsumer consumer(&outputStream, config);

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("key_a");
        consumer.OnStringScalar("a");
        consumer.OnKeyedItem("key_b");
        consumer.OnStringScalar("b");
        consumer.OnKeyedItem("column");
        consumer.OnStringScalar("value");
    consumer.OnEndMap();

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("key_b");
        consumer.OnStringScalar("1");
        consumer.OnKeyedItem("column");
        consumer.OnStringScalar("2");
        consumer.OnKeyedItem("subkey");
        consumer.OnStringScalar("3");
        consumer.OnKeyedItem("key_a");
        consumer.OnStringScalar("xxx");
    consumer.OnEndMap();

    Stroka output =
        "a b\tcolumn=value\n"
        "xxx 1\tcolumn=2\n";

    EXPECT_EQ(output, outputStream.Str());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYamredDsvWriterTest, Escaping)
{
    TStringStream outputStream;
    auto config = New<TYamredDsvFormatConfig>();
    config->HasSubkey = false;
    config->KeyColumnNames.push_back("key_a");
    config->KeyColumnNames.push_back("key_b");
    config->SubkeyColumnNames.push_back("subkey");
    TYamredDsvConsumer consumer(&outputStream, config);

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("key_a");
        consumer.OnStringScalar("a\n");
        consumer.OnKeyedItem("key_b");
        consumer.OnStringScalar("\nb\t");
        consumer.OnKeyedItem("column");
        consumer.OnStringScalar("\nva\\lue\t");
    consumer.OnEndMap();

    Stroka output = "a\\n \\nb\\t\tcolumn=\\nva\\\\lue\\t\n";

    EXPECT_EQ(output, outputStream.Str());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYamredDsvWriterTest, Lenval)
{
    TStringStream outputStream;
    auto config = New<TYamredDsvFormatConfig>();
    config->Lenval = true;
    config->HasSubkey = true;
    config->KeyColumnNames.push_back("key_a");
    config->KeyColumnNames.push_back("key_b");
    config->SubkeyColumnNames.push_back("subkey");
    TYamredDsvConsumer consumer(&outputStream, config);

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("subkey");
        consumer.OnStringScalar("xxx");
        consumer.OnKeyedItem("key_a");
        consumer.OnStringScalar("a");
        consumer.OnKeyedItem("column");
        consumer.OnStringScalar("value");
        consumer.OnKeyedItem("key_b");
        consumer.OnStringScalar("b");
    consumer.OnEndMap();

    Stroka output = Stroka(
        "\x03\x00\x00\x00" "a b"
        "\x03\x00\x00\x00" "xxx"
        "\x0C\x00\x00\x00" "column=value"
        , 3 * 4 + 3 + 3 + 12
    );

    EXPECT_EQ(output, outputStream.Str());
}

TEST(TYamredDsvWriterTest, TableIndex)
{
    TStringStream outputStream;
    auto config = New<TYamredDsvFormatConfig>();
    config->Lenval = true;
    config->EnableTableIndex = true;
    config->KeyColumnNames.push_back("key");
    TYamredDsvConsumer consumer(&outputStream, config);

    consumer.OnListItem();
    consumer.OnBeginAttributes();
        consumer.OnKeyedItem("table_index");
        consumer.OnInt64Scalar(0);
    consumer.OnEndAttributes();
    consumer.OnEntity();

    consumer.OnBeginMap();
        consumer.OnKeyedItem("key");
        consumer.OnStringScalar("x");
        consumer.OnKeyedItem("value");
        consumer.OnStringScalar("y");
    consumer.OnEndMap();

    Stroka output = Stroka(
        "\xff\xff\xff\xff" "\x00\x00\x00\x00"
        "\x01\x00\x00\x00" "x"
        "\x07\x00\x00\x00" "value=y"
        , 4 + 4 + 2 * 4 + 1 + 7
    );

    EXPECT_EQ(output, outputStream.Str());
}

TEST(TYamredDsvWriterTest, EscapingInLenval)
{
    TStringStream outputStream;
    auto config = New<TYamredDsvFormatConfig>();
    config->Lenval = true;
    config->KeyColumnNames.push_back("key");
    TYamredDsvConsumer consumer(&outputStream, config);

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("key");
        consumer.OnStringScalar("\tx");
        consumer.OnKeyedItem("value");
        consumer.OnStringScalar("\ty");
    consumer.OnEndMap();

    Stroka output = Stroka(
        "\x03\x00\x00\x00" "\\tx"
        "\x09\x00\x00\x00" "value=\\ty"
        , 2 * 4 + 3 + 9
    );

    EXPECT_EQ(output, outputStream.Str());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NFormats
} // namespace NYT
