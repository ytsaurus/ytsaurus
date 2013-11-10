#include "stdafx.h"

#include <core/formats/schemed_dsv_writer.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace NFormats {

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

} // namespace NFormats
} // namespace NYT


