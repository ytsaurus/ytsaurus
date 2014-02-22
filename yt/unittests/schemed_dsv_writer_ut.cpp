#include "stdafx.h"
#include "framework.h"

#include <ytlib/formats/schemed_dsv_writer.h>

namespace NYT {
namespace NFormats {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TSchemedDsvWriterTest, Simple)
{
    TStringStream outputStream;
    auto config = New<TSchemedDsvFormatConfig>();
    config->Columns.push_back("a");
    config->Columns.push_back("b");
    TSchemedDsvWriter writer(&outputStream, config);

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("a");
        writer.OnStringScalar("1");
        writer.OnKeyedItem("b");
        writer.OnStringScalar("2");
    writer.OnEndMap();

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("a");
        writer.OnStringScalar("10");
    writer.OnEndMap();

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("a");
        writer.OnStringScalar("x");
        writer.OnKeyedItem("b");
        writer.OnStringScalar("y");
        writer.OnKeyedItem("c");
        writer.OnStringScalar("z");
    writer.OnEndMap();

    Stroka output =
        "1\t2\n"
        "x\ty\n";

    EXPECT_EQ(output, outputStream.Str());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSchemedDsvWriterTest, TableIndex)
{
    TStringStream outputStream;
    auto config = New<TSchemedDsvFormatConfig>();
    config->Columns.push_back("a");
    config->EnableTableIndex = true;
    TSchemedDsvWriter writer(&outputStream, config);

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("a");
        writer.OnStringScalar("1");
    writer.OnEndMap();

    writer.OnListItem();
    writer.OnBeginAttributes();
        writer.OnKeyedItem("table_index");
        writer.OnIntegerScalar(1);
    writer.OnEndAttributes();
    writer.OnEntity();

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("b");
        writer.OnStringScalar("10");
    writer.OnEndMap();

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("a");
        writer.OnStringScalar("x");
        writer.OnKeyedItem("b");
        writer.OnStringScalar("y");
    writer.OnEndMap();

    Stroka output =
        "0\t1\n"
        "1\tx\n";

    EXPECT_EQ(output, outputStream.Str());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSchemedDsvWriterTest, FailMode)
{
    TStringStream outputStream;
    auto config = New<TSchemedDsvFormatConfig>();
    config->Columns.push_back("a");
    config->MissingValueMode = TSchemedDsvFormatConfig::EMissingValueMode::Fail;
    TSchemedDsvWriter writer(&outputStream, config);

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("a");
        writer.OnStringScalar("1");
    writer.OnEndMap();
    EXPECT_EQ("1\n", outputStream.Str());

    EXPECT_ANY_THROW(
        writer.OnListItem();
        writer.OnBeginMap();
            writer.OnKeyedItem("b");
            writer.OnStringScalar("10");
        writer.OnEndMap();
    );
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSchemedDsvWriterTest, PrintSentinelMode)
{
    TStringStream outputStream;
    auto config = New<TSchemedDsvFormatConfig>();
    config->Columns.push_back("a");
    config->MissingValueMode = TSchemedDsvFormatConfig::EMissingValueMode::PrintSentinel;
    config->MissingValueSentinel = "null";
    TSchemedDsvWriter writer(&outputStream, config);

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("a");
        writer.OnStringScalar("1");
    writer.OnEndMap();

    writer.OnListItem();
    writer.OnBeginMap();
        writer.OnKeyedItem("b");
        writer.OnStringScalar("10");
    writer.OnEndMap();

    EXPECT_EQ("1\nnull\n", outputStream.Str());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NFormats
} // namespace NYT
