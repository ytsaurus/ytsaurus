#include <yt/core/test_framework/framework.h>

#include <yt/ytlib/formats/schemaless_web_json_writer.h>

#include <yt/ytlib/table_client/public.h>
#include <yt/client/table_client/name_table.h>

#include <yt/core/concurrency/async_stream.h>

#include <limits>

namespace NYT {
namespace NFormats {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;
using namespace NTableClient;

class TSchemalessWriterForWebJson
    : public ::testing::Test
{
protected:
    TNameTablePtr NameTable_;
    TSchemalessWebJsonFormatConfigPtr Config_;

    TStringStream OutputStream_;

    ISchemalessFormatWriterPtr Writer_;

    int KeyAId_ = -1;
    int KeyBId_ = -1;
    int KeyCId_ = -1;
    int KeyDId_ = -1;

    int TableIndexColumnId_ = -1;
    int RowIndexColumnId_ = -1;
    int TabletIndexColumnId_ = -1;

    TSchemalessWriterForWebJson()
    {
        NameTable_ = New<TNameTable>();

        KeyAId_ = NameTable_->RegisterName("column_a");
        KeyBId_ = NameTable_->RegisterName("column_b");
        KeyCId_ = NameTable_->RegisterName("column_c");
        // We do not register KeyD intentionally.

        TableIndexColumnId_ = NameTable_->RegisterName(TableIndexColumnName);
        RowIndexColumnId_ = NameTable_->RegisterName(RowIndexColumnName);
        TabletIndexColumnId_ = NameTable_->RegisterName(TabletIndexColumnName);

        Config_ = New<TSchemalessWebJsonFormatConfig>();
    }

    void CreateStandardWriter()
    {
        Writer_ = CreateSchemalessWriterForWebJson(
            Config_,
            CreateAsyncAdapter(static_cast<IOutputStream*>(&OutputStream_)),
            NameTable_);
    }
};

TEST_F(TSchemalessWriterForWebJson, Simple)
{
    Config_->MaxAllColumnNamesCount = 2;

    CreateStandardWriter();

    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedUint64Value(100500, KeyAId_));
    row1.AddValue(MakeUnversionedBooleanValue(true, KeyBId_));
    row1.AddValue(MakeUnversionedStringValue("row1_c", KeyCId_));
    row1.AddValue(MakeUnversionedInt64Value(0, RowIndexColumnId_));

    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedStringValue("row2_c", KeyCId_));
    row2.AddValue(MakeUnversionedStringValue("row2_b", KeyBId_));
    row2.AddValue(MakeUnversionedInt64Value(1, RowIndexColumnId_));

    std::vector<TUnversionedRow> rows = {row1.GetRow(), row2.GetRow()};

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close();

    TString expectedOutput =
        "{"
            "\"rows\":["
                "{"
                    "\"column_a\":{"
                        "\"$type\":\"uint64\","
                        "\"$value\":\"100500\""
                    "},"
                    "\"column_b\":{"
                        "\"$type\":\"boolean\","
                        "\"$value\":\"true\""
                    "},"
                    "\"column_c\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row1_c\""
                    "}"
                "},"
                "{"
                    "\"column_c\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row2_c\""
                    "},"
                    "\"column_b\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row2_b\""
                    "}"
                "}"
            "],"
            "\"incomplete_columns\":\"false\","
            "\"incomplete_all_column_names\":\"true\","
            "\"all_column_names\":["
                "\"column_a\","
                "\"column_b\""
            "]"
        "}";

    EXPECT_EQ(expectedOutput.length(), Writer_->GetWrittenSize());
    EXPECT_EQ(expectedOutput, OutputStream_.Str());
}

TEST_F(TSchemalessWriterForWebJson, SliceColumnsByMaxCount)
{
    Config_->MaxSelectedColumnCount = 2;

    CreateStandardWriter();

    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedStringValue("row1_a", KeyAId_));
    row1.AddValue(MakeUnversionedStringValue("row1_b", KeyBId_));
    row1.AddValue(MakeUnversionedStringValue("row1_c", KeyCId_));

    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedStringValue("row2_c", KeyCId_));
    row2.AddValue(MakeUnversionedStringValue("row2_b", KeyBId_));

    TUnversionedRowBuilder row3;
    row3.AddValue(MakeUnversionedStringValue("row3_c", KeyCId_));

    std::vector<TUnversionedRow> rows = {row1.GetRow(), row2.GetRow(), row3.GetRow()};

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close();

    TString expectedOutput =
        "{"
            "\"rows\":["
                "{"
                    "\"column_a\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row1_a\""
                    "},"
                    "\"column_b\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row1_b\""
                    "}"
                "},"
                "{"
                    "\"column_b\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row2_b\""
                    "}"
                "},"
                "{"
                "}"
            "],"
            "\"incomplete_columns\":\"true\","
            "\"incomplete_all_column_names\":\"false\","
            "\"all_column_names\":["
                "\"column_a\","
                "\"column_b\","
                "\"column_c\""
            "]"
        "}";

    EXPECT_EQ(expectedOutput.length(), Writer_->GetWrittenSize());
    EXPECT_EQ(expectedOutput, OutputStream_.Str());
}

TEST_F(TSchemalessWriterForWebJson, SliceStrings)
{
    Config_->FieldWeightLimit = 6;

    CreateStandardWriter();

    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedStringValue("row1_b", KeyBId_));
    row1.AddValue(MakeUnversionedStringValue("rooooow1_c", KeyCId_));
    row1.AddValue(MakeUnversionedStringValue("row1_a", KeyAId_));

    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedStringValue("row2_c", KeyCId_));
    row2.AddValue(MakeUnversionedStringValue("rooow2_b", KeyBId_));

    TUnversionedRowBuilder row3;
    row3.AddValue(MakeUnversionedStringValue("row3_c", KeyCId_));

    std::vector<TUnversionedRow> rows = {row1.GetRow(), row2.GetRow(), row3.GetRow()};

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close();

    TString expectedOutput =
        "{"
            "\"rows\":["
                "{"
                    "\"column_b\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row1_b\""
                    "},"
                    "\"column_c\":{"
                        "\"$incomplete\":true,"
                        "\"$type\":\"string\","
                        "\"$value\":\"rooooo\""
                    "},"
                    "\"column_a\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row1_a\""
                    "}"
                "},"
                "{"
                    "\"column_c\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row2_c\""
                    "},"
                    "\"column_b\":{"
                        "\"$incomplete\":true,"
                        "\"$type\":\"string\","
                        "\"$value\":\"rooow2\""
                    "}"
                "},"
                "{"
                    "\"column_c\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row3_c\""
                    "}"
                "}"
            "],"
            "\"incomplete_columns\":\"false\","
            "\"incomplete_all_column_names\":\"false\","
            "\"all_column_names\":["
                "\"column_a\","
                "\"column_b\","
                "\"column_c\""
            "]"
        "}";

    EXPECT_EQ(expectedOutput.length(), Writer_->GetWrittenSize());
    EXPECT_EQ(expectedOutput, OutputStream_.Str());
}

TEST_F(TSchemalessWriterForWebJson, ReplaceAnyWithNull)
{
    Config_->FieldWeightLimit = 8;

    CreateStandardWriter();

    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedAnyValue("{key=a}", KeyBId_));
    row1.AddValue(MakeUnversionedStringValue("row1_c", KeyCId_));
    row1.AddValue(MakeUnversionedStringValue("row1_a", KeyAId_));

    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedAnyValue("{key=aaaaaa}", KeyCId_));
    row2.AddValue(MakeUnversionedStringValue("row2_b", KeyBId_));

    TUnversionedRowBuilder row3;
    row3.AddValue(MakeUnversionedStringValue("row3_c", KeyCId_));

    std::vector<TUnversionedRow> rows = {row1.GetRow(), row2.GetRow(), row3.GetRow()};

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close();

    TString expectedOutput =
        "{"
            "\"rows\":["
                "{"
                    "\"column_b\":{"
                        "\"key\":{"
                            "\"$type\":\"string\","
                            "\"$value\":\"a\""
                        "}"
                    "},"
                    "\"column_c\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row1_c\""
                    "},"
                    "\"column_a\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row1_a\""
                    "}"
                "},"
                "{"
                    "\"column_c\":{"
                        "\"$incomplete\":true,"
                        "\"$type\":\"any\","
                        "\"$value\":\"\""
                    "},"
                    "\"column_b\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row2_b\""
                    "}"
                "},"
                "{"
                    "\"column_c\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row3_c\""
                    "}"
                "}"
            "],"
            "\"incomplete_columns\":\"false\","
            "\"incomplete_all_column_names\":\"false\","
            "\"all_column_names\":["
                "\"column_a\","
                "\"column_b\","
                "\"column_c\""
            "]"
        "}";

    EXPECT_EQ(expectedOutput.length(), Writer_->GetWrittenSize());
    EXPECT_EQ(expectedOutput, OutputStream_.Str());
}

TEST_F(TSchemalessWriterForWebJson, SkipSystemColumns)
{
    Config_->SkipSystemColumns = false;

    CreateStandardWriter();

    TUnversionedRowBuilder row;
    row.AddValue(MakeUnversionedInt64Value(0, TableIndexColumnId_));
    row.AddValue(MakeUnversionedInt64Value(1, RowIndexColumnId_));
    row.AddValue(MakeUnversionedInt64Value(2, TabletIndexColumnId_));

    std::vector<TUnversionedRow> rows = {row.GetRow()};

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close();

    TString expectedOutput =
        "{"
            "\"rows\":["
                "{"
                    "\"$$table_index\":{"
                        "\"$type\":\"int64\","
                        "\"$value\":\"0\""
                    "},"
                    "\"$$row_index\":{"
                        "\"$type\":\"int64\","
                        "\"$value\":\"1\""
                    "},"
                    "\"$$tablet_index\":{"
                        "\"$type\":\"int64\","
                        "\"$value\":\"2\""
                    "}"
                "}"
            "],"
            "\"incomplete_columns\":\"false\","
            "\"incomplete_all_column_names\":\"false\","
            "\"all_column_names\":["
                "\"$row_index\","
                "\"$table_index\","
                "\"$tablet_index\""
            "]"
        "}";

    EXPECT_EQ(expectedOutput.length(), Writer_->GetWrittenSize());
    EXPECT_EQ(expectedOutput, OutputStream_.Str());
}

TEST_F(TSchemalessWriterForWebJson, SkipUnregisteredColumns)
{
    CreateStandardWriter();

    TUnversionedRowBuilder row;
    row.AddValue(MakeUnversionedBooleanValue(true, KeyDId_));
    std::vector<TUnversionedRow> rows = {row.GetRow()};

    EXPECT_EQ(true, Writer_->Write(rows));

    KeyDId_ = Writer_->GetNameTable()->RegisterName("column_d");

    rows.clear();
    row.Reset();
    row.AddValue(MakeUnversionedBooleanValue(true, KeyDId_));
    rows.push_back(row.GetRow());

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close();

    TString expectedOutput =
        "{"
            "\"rows\":["
                "{"
                "},"
                "{"
                    "\"column_d\":{"
                        "\"$type\":\"boolean\","
                        "\"$value\":\"true\""
                    "}"
                "}"
            "],"
            "\"incomplete_columns\":\"false\","
            "\"incomplete_all_column_names\":\"false\","
            "\"all_column_names\":["
                "\"column_d\""
            "]"
        "}";

    EXPECT_EQ(expectedOutput.length(), Writer_->GetWrittenSize());
    EXPECT_EQ(expectedOutput, OutputStream_.Str());
}

TEST_F(TSchemalessWriterForWebJson, SliceColumnsByName)
{
    Config_->ColumnNames = {
        "column_b",
        "column_c",
        "$tablet_index"};
    Config_->MaxSelectedColumnCount = 2;
    Config_->SkipSystemColumns = false;

    CreateStandardWriter();

    TUnversionedRowBuilder row;
    row.AddValue(MakeUnversionedUint64Value(100500, KeyAId_));
    row.AddValue(MakeUnversionedDoubleValue(0.42, KeyBId_));
    row.AddValue(MakeUnversionedStringValue("abracadabra", KeyCId_));
    row.AddValue(MakeUnversionedInt64Value(10, TabletIndexColumnId_));

    std::vector<TUnversionedRow> rows = {row.GetRow()};

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close();

    TString expectedOutput =
        "{"
            "\"rows\":["
                "{"
                    "\"column_b\":{"
                        "\"$type\":\"double\","
                        "\"$value\":\"0.42\""
                    "},"
                    "\"column_c\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"abracadabra\""
                    "},"
                    "\"$$tablet_index\":{"
                        "\"$type\":\"int64\","
                        "\"$value\":\"10\""
                    "}"
                "}"
            "],"
            "\"incomplete_columns\":\"true\","
            "\"incomplete_all_column_names\":\"false\","
            "\"all_column_names\":["
                "\"$tablet_index\","
                "\"column_a\","
                "\"column_b\","
                "\"column_c\""
            "]"
        "}";

    EXPECT_EQ(expectedOutput.length(), Writer_->GetWrittenSize());
    EXPECT_EQ(expectedOutput, OutputStream_.Str());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NFormats
} // namespace NYT
