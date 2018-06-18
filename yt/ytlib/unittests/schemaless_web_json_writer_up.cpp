#include <yt/core/test_framework/framework.h>

#include <yt/ytlib/formats/schemaless_web_json_writer.h>

#include <yt/ytlib/table_client/name_table.h>

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

    int KeyAId_;
    int KeyBId_;
    int KeyCId_;

    TSchemalessWriterForWebJson()
    {
        NameTable_ = New<TNameTable>();
        KeyAId_ = NameTable_->RegisterName("column_a");
        KeyBId_ = NameTable_->RegisterName("column_b");
        KeyCId_ = NameTable_->RegisterName("column_c");

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
    CreateStandardWriter();

    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedUint64Value(100500, KeyAId_));
    row1.AddValue(MakeUnversionedBooleanValue(true, KeyBId_));
    row1.AddValue(MakeUnversionedStringValue("row1_c", KeyCId_));

    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedStringValue("row2_c", KeyCId_));
    row2.AddValue(MakeUnversionedStringValue("row2_b", KeyBId_));

    std::vector<TUnversionedRow> rows = {row1.GetRow(), row2.GetRow()};

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
            "\"incomplete_columns\":{"
                "\"$type\":\"boolean\","
                "\"$value\":\"false\""
            "}"
        "}";

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close();

    EXPECT_EQ(expectedOutput.length(), Writer_->GetWrittenSize());
    EXPECT_EQ(expectedOutput, OutputStream_.Str());
}

TEST_F(TSchemalessWriterForWebJson, SliceColumns)
{
    Config_->ColumnLimit = 2;

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
            "\"incomplete_columns\":{"
                "\"$type\":\"boolean\","
                "\"$value\":\"true\""
            "}"
        "}";

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close();

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
            "\"incomplete_columns\":{"
                "\"$type\":\"boolean\","
                "\"$value\":\"false\""
            "}"
        "}";

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close();

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
            "\"incomplete_columns\":{"
                "\"$type\":\"boolean\","
                "\"$value\":\"false\""
            "}"
        "}";

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close();

    EXPECT_EQ(expectedOutput.length(), Writer_->GetWrittenSize());
    EXPECT_EQ(expectedOutput, OutputStream_.Str());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NFormats
} // namespace NYT
