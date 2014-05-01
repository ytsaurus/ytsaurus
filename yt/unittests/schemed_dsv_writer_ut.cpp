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
    TSchemedDsvConsumer consumer(&outputStream, config);

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("a");
        consumer.OnStringScalar("1");
        consumer.OnKeyedItem("b");
        consumer.OnStringScalar("2");
    consumer.OnEndMap();

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("a");
        consumer.OnStringScalar("10");
    consumer.OnEndMap();

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("a");
        consumer.OnStringScalar("x");
        consumer.OnKeyedItem("b");
        consumer.OnStringScalar("y");
        consumer.OnKeyedItem("c");
        consumer.OnStringScalar("z");
    consumer.OnEndMap();

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
    TSchemedDsvConsumer consumer(&outputStream, config);

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("a");
        consumer.OnIntegerScalar(1);
    consumer.OnEndMap();

    consumer.OnListItem();
    consumer.OnBeginAttributes();
        consumer.OnKeyedItem("table_index");
        consumer.OnIntegerScalar(1);
    consumer.OnEndAttributes();
    consumer.OnEntity();

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("b");
        consumer.OnStringScalar("10");
    consumer.OnEndMap();

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("a");
        consumer.OnStringScalar("x");
        consumer.OnKeyedItem("b");
        consumer.OnStringScalar("y");
    consumer.OnEndMap();

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
    TSchemedDsvConsumer consumer(&outputStream, config);

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("a");
        consumer.OnStringScalar("1");
    consumer.OnEndMap();
    EXPECT_EQ("1\n", outputStream.Str());

    EXPECT_ANY_THROW(
        consumer.OnListItem();
        consumer.OnBeginMap();
            consumer.OnKeyedItem("b");
            consumer.OnStringScalar("10");
        consumer.OnEndMap();
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
    TSchemedDsvConsumer consumer(&outputStream, config);

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("a");
        consumer.OnStringScalar("1");
    consumer.OnEndMap();

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("b");
        consumer.OnStringScalar("10");
    consumer.OnEndMap();

    EXPECT_EQ("1\nnull\n", outputStream.Str());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NFormats
} // namespace NYT
