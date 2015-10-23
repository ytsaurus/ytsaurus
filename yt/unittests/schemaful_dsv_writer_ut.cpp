#include "stdafx.h"
#include "framework.h"

#include <ytlib/table_client/name_table.h>

#include <ytlib/formats/schemaful_dsv_writer.h>

#include <core/concurrency/async_stream.h>

namespace NYT {
namespace NFormats {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;
using namespace NTableClient;

class TSchemalessWriterForSchemafulDsvTest : 
    public ::testing::Test
{
protected:
    TNameTablePtr NameTable_;
    int KeyAId_;
    int KeyBId_;
    int KeyCId_;
    int KeyDId_;
    TSchemafulDsvFormatConfigPtr Config_;

    TShemalessWriterForSchemafulDsvPtr Writer_;

    TStringStream OutputStream_;

    TSchemalessWriterForSchemafulDsvTest() {
        NameTable_ = New<TNameTable>();
        KeyAId_ = NameTable_->RegisterName("column_a");
        KeyBId_ = NameTable_->RegisterName("column_b");
        KeyCId_ = NameTable_->RegisterName("column_c");
        KeyDId_ = NameTable_->RegisterName("column_d");
        Config_ = New<TSchemafulDsvFormatConfig>();
    }

    void CreateStandardWriter() {
        Writer_ = New<TShemalessWriterForSchemafulDsv>(
            NameTable_, 
            CreateAsyncAdapter(static_cast<TOutputStream*>(&OutputStream_)),
            false, // enableContextSaving  
            Config_);
    }
};

TEST_F(TSchemalessWriterForSchemafulDsvTest, Simple)
{
    Config_->Columns = {"column_b", "column_c", "column_a"};
    CreateStandardWriter();
    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedStringValue("value_a", KeyAId_));
    row1.AddValue(MakeUnversionedInt64Value(-42, KeyBId_));
    row1.AddValue(MakeUnversionedBooleanValue(true, KeyCId_));
    row1.AddValue(MakeUnversionedStringValue("garbage", KeyDId_));

    TUnversionedRowBuilder row2;
    // The order is reversed.
    row2.AddValue(MakeUnversionedStringValue("value_c", KeyCId_));
    row2.AddValue(MakeUnversionedBooleanValue(false, KeyBId_));
    row2.AddValue(MakeUnversionedInt64Value(23, KeyAId_));

    std::vector<TUnversionedRow> rows = {row1.GetRow(), row2.GetRow()};

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close()
        .Get()
        .ThrowOnError();
    
    Stroka expectedOutput =
        "-42\ttrue\tvalue_a\n"
        "false\tvalue_c\t23\n";
    EXPECT_EQ(expectedOutput, OutputStream_.Str()); 
}

TEST_F(TSchemalessWriterForSchemafulDsvTest, ThrowOnMissingColumn)
{
    Config_->Columns = {"column_b", "column_c", "column_a"};
    CreateStandardWriter();
    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedStringValue("value_a", KeyAId_));
    row1.AddValue(MakeUnversionedBooleanValue(true, KeyCId_));
    row1.AddValue(MakeUnversionedStringValue("garbage", KeyDId_));

    std::vector<TUnversionedRow> rows = {row1.GetRow()};

    EXPECT_EQ(false, Writer_->Write(rows));
    EXPECT_THROW(Writer_->Close()
                    .Get()
                    .ThrowOnError(), std::exception);
}

// This test shows actual behavior of writer. It is OK to change it in future.
TEST_F(TSchemalessWriterForSchemafulDsvTest, TrickyDoubleRepresentations)
{
    Config_->Columns = {"column_a", "column_b", "column_c", "column_d"};
    CreateStandardWriter();
    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedDoubleValue(1.234567890123456, KeyAId_));
    row1.AddValue(MakeUnversionedDoubleValue(42, KeyBId_));
    row1.AddValue(MakeUnversionedDoubleValue(1e300, KeyCId_));
    row1.AddValue(MakeUnversionedDoubleValue(-1e-300, KeyDId_));    

    std::vector<TUnversionedRow> rows = {row1.GetRow()};

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close()
        .Get()
        .ThrowOnError();
    Stroka expectedOutput = "1.23457\t42\t1e+300\t-1e-300\n";
    EXPECT_EQ(expectedOutput, OutputStream_.Str());
}

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
        consumer.OnKeyedItem("b");
        consumer.OnEntity();
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
    config->MissingValueMode = EMissingSchemafulDsvValueMode::Fail;
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

    EXPECT_ANY_THROW(
        consumer.OnListItem();
        consumer.OnBeginMap();
            consumer.OnKeyedItem("b");
            consumer.OnEntity();
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
    config->MissingValueMode = EMissingSchemafulDsvValueMode::PrintSentinel;
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
