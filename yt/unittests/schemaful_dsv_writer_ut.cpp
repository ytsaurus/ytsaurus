#include "stdafx.h"
#include "framework.h"

#include <core/formats/schemaful_dsv_writer.h>

namespace NYT {
namespace NFormats {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TSchemafulDsvWriterTest, Simple)
{
    TStringStream outputStream;
    auto config = New<TSchemafulDsvFormatConfig>();
    config->Columns.push_back("a");
    config->Columns.push_back("b");
    TSchemafulDsvWriter writer(&outputStream, config);

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

TEST(TSchemafulDsvWriterTest, TableIndex)
{
    TStringStream outputStream;
    auto config = New<TSchemafulDsvFormatConfig>();
    config->Columns.push_back("a");
    config->EnableTableIndex = true;
    TSchemafulDsvWriter writer(&outputStream, config);

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

TEST(TSchemafulDsvWriterTest, FailMode)
{
    TStringStream outputStream;
    auto config = New<TSchemafulDsvFormatConfig>();
    config->Columns.push_back("a");
    config->MissingValueMode = TSchemafulDsvFormatConfig::EMissingValueMode::Fail;
    TSchemafulDsvWriter writer(&outputStream, config);

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

TEST(TSchemafulDsvWriterTest, PrintSentinelMode)
{
    TStringStream outputStream;
    auto config = New<TSchemafulDsvFormatConfig>();
    config->Columns.push_back("a");
    config->MissingValueMode = TSchemafulDsvFormatConfig::EMissingValueMode::PrintSentinel;
    config->MissingValueSentinel = "null";
    TSchemafulDsvWriter writer(&outputStream, config);

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
