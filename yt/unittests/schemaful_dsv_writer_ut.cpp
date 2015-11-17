#include "stdafx.h"
#include "framework.h"

#include <ytlib/table_client/name_table.h>

#include <ytlib/formats/schemaful_dsv_writer.h>

#include <core/concurrency/async_stream.h>

#include <limits>

namespace NYT {
namespace NFormats {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;
using namespace NTableClient;

class TSchemalessWriterForSchemafulDsvTest
    : public ::testing::Test
{
protected:
    TNameTablePtr NameTable_;
    int KeyAId_;
    int KeyBId_;
    int KeyCId_;
    int KeyDId_;
    int TableIndexId_;
    int RangeIndexId_;
    int RowIndexId_;
    TSchemafulDsvFormatConfigPtr Config_;

    TSchemalessWriterForSchemafulDsvPtr Writer_;

    TStringStream OutputStream_;

    TSchemalessWriterForSchemafulDsvTest() {
        NameTable_ = New<TNameTable>();
        KeyAId_ = NameTable_->RegisterName("column_a");
        KeyBId_ = NameTable_->RegisterName("column_b");
        KeyCId_ = NameTable_->RegisterName("column_c");
        KeyDId_ = NameTable_->RegisterName("column_d");
        TableIndexId_ = NameTable_->RegisterName(TableIndexColumnName);
        RowIndexId_ = NameTable_->RegisterName(RowIndexColumnName);
        RangeIndexId_ = NameTable_->RegisterName(RangeIndexColumnName);

        Config_ = New<TSchemafulDsvFormatConfig>();
    }

    void CreateStandardWriter() {
        Writer_ = New<TSchemalessWriterForSchemafulDsv>(
            NameTable_, 
            CreateAsyncAdapter(static_cast<TOutputStream*>(&OutputStream_)),
            false, // enableContextSaving  
            New<TControlAttributesConfig>(),
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

    // Ignore system columns.
    row1.AddValue(MakeUnversionedInt64Value(2, TableIndexId_));
    row1.AddValue(MakeUnversionedInt64Value(42, RowIndexId_));
    row1.AddValue(MakeUnversionedInt64Value(1, RangeIndexId_));

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

// This test shows the actual behavior of writer. It is OK to change it in the future. :)
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

TEST_F(TSchemalessWriterForSchemafulDsvTest, IntegralTypeRepresentations)
{
    Config_->Columns = {"column_a", "column_b", "column_c", "column_d"};
    CreateStandardWriter();
    
    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedInt64Value(0LL, KeyAId_));
    row1.AddValue(MakeUnversionedInt64Value(-1LL, KeyBId_));
    row1.AddValue(MakeUnversionedInt64Value(1LL, KeyCId_));
    row1.AddValue(MakeUnversionedInt64Value(99LL, KeyDId_));
    
    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedInt64Value(123LL, KeyAId_));
    row2.AddValue(MakeUnversionedInt64Value(-123LL, KeyBId_));
    row2.AddValue(MakeUnversionedInt64Value(1234LL, KeyCId_));
    row2.AddValue(MakeUnversionedInt64Value(-1234LL, KeyDId_));

    TUnversionedRowBuilder row3;
    row3.AddValue(MakeUnversionedUint64Value(0ULL, KeyAId_));
    row3.AddValue(MakeUnversionedUint64Value(98ULL, KeyBId_));
    row3.AddValue(MakeUnversionedUint64Value(987ULL, KeyCId_));
    row3.AddValue(MakeUnversionedUint64Value(9876ULL, KeyDId_));

    TUnversionedRowBuilder row4;
    row4.AddValue(MakeUnversionedInt64Value(std::numeric_limits<i64>::max(), KeyAId_));
    row4.AddValue(MakeUnversionedInt64Value(std::numeric_limits<i64>::min(), KeyBId_));
    row4.AddValue(MakeUnversionedInt64Value(std::numeric_limits<i64>::min() + 1LL, KeyCId_));
    row4.AddValue(MakeUnversionedUint64Value(std::numeric_limits<ui64>::max(), KeyDId_));

    std::vector<TUnversionedRow> rows = 
        {row1.GetRow(), row2.GetRow(), row3.GetRow(), row4.GetRow()};

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close()
        .Get()
        .ThrowOnError();
    Stroka expectedOutput = 
        "0\t-1\t1\t99\n"
        "123\t-123\t1234\t-1234\n"
        "0\t98\t987\t9876\n"
        "9223372036854775807\t-9223372036854775808\t-9223372036854775807\t18446744073709551615\n";
    EXPECT_EQ(expectedOutput, OutputStream_.Str());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NFormats
} // namespace NYT
