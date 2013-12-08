#include "stdafx.h"
#include "framework.h"

#include <core/formats/yamred_dsv_writer.h>

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
    TYamredDsvWriter writer(&outputStream, config);

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("key_a");
        writer.OnStringScalar("a");
        writer.OnKeyedItem("key_b");
        writer.OnStringScalar("b");
    writer.OnEndMap();

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("key_b");
        writer.OnStringScalar("1");
        writer.OnKeyedItem("column");
        writer.OnStringScalar("2");
        writer.OnKeyedItem("subkey");
        writer.OnStringScalar("3");
        writer.OnKeyedItem("key_a");
        writer.OnStringScalar("xxx");
    writer.OnEndMap();

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
    TYamredDsvWriter writer(&outputStream, config);

    {
        Stroka keyA = "key_a";
        Stroka a = "a";
        Stroka keyB = "key_b";
        Stroka b = "b";
        writer.OnListItem();
        writer.OnBeginMap();
            writer.OnKeyedItem(keyA);
            writer.OnStringScalar(a);
            writer.OnKeyedItem(keyB);
            writer.OnStringScalar(b);
        writer.OnEndMap();
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
        writer.OnListItem();
        writer.OnBeginMap();
            writer.OnKeyedItem(keyA);
            writer.OnStringScalar(a);
            writer.OnKeyedItem(keyB);
            writer.OnStringScalar(b);
        writer.OnEndMap();
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
    TYamredDsvWriter writer(&outputStream, config);

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("key_a");
        writer.OnStringScalar("a");
        writer.OnKeyedItem("key_b");
        writer.OnStringScalar("b");
        writer.OnKeyedItem("column");
        writer.OnStringScalar("value");
    writer.OnEndMap();

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("key_b");
        writer.OnStringScalar("1");
        writer.OnKeyedItem("column");
        writer.OnStringScalar("2");
        writer.OnKeyedItem("subkey");
        writer.OnStringScalar("3");
        writer.OnKeyedItem("key_a");
        writer.OnStringScalar("xxx");
    writer.OnEndMap();

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
    TYamredDsvWriter writer(&outputStream, config);

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("key_a");
        writer.OnStringScalar("a\n");
        writer.OnKeyedItem("key_b");
        writer.OnStringScalar("\nb\t");
        writer.OnKeyedItem("column");
        writer.OnStringScalar("\nva\\lue\t");
    writer.OnEndMap();

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
    TYamredDsvWriter writer(&outputStream, config);

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("subkey");
        writer.OnStringScalar("xxx");
        writer.OnKeyedItem("key_a");
        writer.OnStringScalar("a");
        writer.OnKeyedItem("column");
        writer.OnStringScalar("value");
        writer.OnKeyedItem("key_b");
        writer.OnStringScalar("b");
    writer.OnEndMap();

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
    TYamredDsvWriter writer(&outputStream, config);

    writer.OnListItem();
    writer.OnBeginAttributes();
        writer.OnKeyedItem("table_index");
        writer.OnIntegerScalar(0);
    writer.OnEndAttributes();
    writer.OnEntity();

    writer.OnBeginMap();
        writer.OnKeyedItem("key");
        writer.OnStringScalar("x");
        writer.OnKeyedItem("value");
        writer.OnStringScalar("y");
    writer.OnEndMap();

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
    TYamredDsvWriter writer(&outputStream, config);

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("key");
        writer.OnStringScalar("\tx");
        writer.OnKeyedItem("value");
        writer.OnStringScalar("\ty");
    writer.OnEndMap();

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
