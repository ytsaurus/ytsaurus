#include "stdafx.h"
#include "framework.h"

#include <ytlib/formats/schemaful_dsv_writer.h>

namespace NYT {
namespace NFormats {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TSchemafulDsvWriterTest, Simple)
{
    TStringStream outputStream;
    auto config = New<TSchemafulDsvFormatConfig>();
    config->Columns = std::vector<Stroka>();
    config->Columns->push_back("a");
    config->Columns->push_back("b");
    TSchemafulDsvConsumer consumer(&outputStream, config);

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

TEST(TSchemafulDsvWriterTest, TableIndex)
{
    TStringStream outputStream;
    auto config = New<TSchemafulDsvFormatConfig>();
    config->Columns = std::vector<Stroka>();
    config->Columns->push_back("a");
    config->EnableTableIndex = true;
    TSchemafulDsvConsumer consumer(&outputStream, config);

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("a");
        consumer.OnInt64Scalar(1);
    consumer.OnEndMap();

    consumer.OnListItem();
    consumer.OnBeginAttributes();
        consumer.OnKeyedItem("table_index");
        consumer.OnInt64Scalar(1);
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

TEST(TSchemafulDsvWriterTest, FailMode)
{
    TStringStream outputStream;
    auto config = New<TSchemafulDsvFormatConfig>();
    config->Columns = std::vector<Stroka>();
    config->Columns->push_back("a");
    config->MissingValueMode = TSchemafulDsvFormatConfig::EMissingValueMode::Fail;
    TSchemafulDsvConsumer consumer(&outputStream, config);

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

TEST(TSchemafulDsvWriterTest, PrintSentinelMode)
{
    TStringStream outputStream;
    auto config = New<TSchemafulDsvFormatConfig>();
    config->Columns = std::vector<Stroka>();
    config->Columns->push_back("a");
    config->MissingValueMode = TSchemafulDsvFormatConfig::EMissingValueMode::PrintSentinel;
    config->MissingValueSentinel = "null";
    TSchemafulDsvConsumer consumer(&outputStream, config);

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
