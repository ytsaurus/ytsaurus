#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/formats/blob_writer.h>
#include <yt/yt/library/named_value/named_value.h>

#include <yt/yt/client/table_client/blob_reader.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/concurrency/async_stream.h>

namespace NYT::NFormats {
namespace {

using namespace NConcurrency;
using namespace NNamedValue;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TSchemalesssBlobWriterTest
    : public ::testing::Test
{
protected:
    TNameTablePtr NameTable_ = New<TNameTable>();
    TBlobFormatConfigPtr Config_ = New<TBlobFormatConfig>();;
    TControlAttributesConfigPtr ControlAttributesConfig_ = New<TControlAttributesConfig>();
    ISchemalessFormatWriterPtr Writer_;
    TStringStream OutputStream_;

    void CreateWriter()
    {
        Writer_ = CreateSchemalessWriterForBlob(
            Config_,
            NameTable_,
            CreateAsyncAdapter(static_cast<IOutputStream*>(&OutputStream_)),
            /*enableContextSaving*/ false,
            ControlAttributesConfig_,
            /*keyColumnCount*/ 0);
    }
};

TEST_F(TSchemalesssBlobWriterTest, Simple)
{
    CreateWriter();

    auto row1 = MakeRow(NameTable_, {
        {TString(TBlobTableSchema::PartIndexColumn), 0},
        {TString(TBlobTableSchema::DataColumn), "hello"}
    });

    auto row2 = MakeRow(NameTable_, {
        {TString(TBlobTableSchema::PartIndexColumn), 1},
        {TString(TBlobTableSchema::DataColumn), "world"}
    });

    std::vector<TUnversionedRow> rows = {row1.Get(), row2.Get()};

    EXPECT_TRUE(Writer_->Write(rows));
    Writer_->Close()
        .Get()
        .ThrowOnError();

    TString expectedOutput = "helloworld";
    EXPECT_EQ(expectedOutput, OutputStream_.Str());
}

TEST_F(TSchemalesssBlobWriterTest, ConfiguredColumnNames)
{
    std::string dataColumnName = "my_data";
    std::string partIndexColumnName = "my_part";

    Config_->DataColumnName = dataColumnName;
    Config_->PartIndexColumnName = partIndexColumnName;
    CreateWriter();

    auto row1 = MakeRow(NameTable_, {
        {TString(partIndexColumnName), 0},
        {TString(dataColumnName), "hello"}
    });

    EXPECT_TRUE(Writer_->Write({row1.Get()}));
    Writer_->Close()
        .Get()
        .ThrowOnError();

    TString expectedOutput = "hello";
    EXPECT_EQ(expectedOutput, OutputStream_.Str());
}

TEST_F(TSchemalesssBlobWriterTest, PartIndexStartNonZero)
{
    CreateWriter();

    auto row1 = MakeRow(NameTable_, {
        {TString(TBlobTableSchema::PartIndexColumn), 5},
        {TString(TBlobTableSchema::DataColumn), "hello"}
    });

    auto row2 = MakeRow(NameTable_, {
        {TString(TBlobTableSchema::PartIndexColumn), 6},
        {TString(TBlobTableSchema::DataColumn), "world"}
    });

    std::vector<TUnversionedRow> rows = {row1.Get(), row2.Get()};

    EXPECT_TRUE(Writer_->Write(rows));
    Writer_->Close()
        .Get()
        .ThrowOnError();

    TString expectedOutput = "helloworld";
    EXPECT_EQ(expectedOutput, OutputStream_.Str());
}

TEST_F(TSchemalesssBlobWriterTest, PartIndexWrongOrder)
{
    CreateWriter();

    auto row1 = MakeRow(NameTable_, {
        {TString(TBlobTableSchema::PartIndexColumn), 1},
        {TString(TBlobTableSchema::DataColumn), "world"}
    });

    auto row2 = MakeRow(NameTable_, {
        {TString(TBlobTableSchema::PartIndexColumn), 0},
        {TString(TBlobTableSchema::DataColumn), "hello"}
    });

    std::vector<TUnversionedRow> rows = {row1.Get(), row2.Get()};

    EXPECT_FALSE(Writer_->Write(rows));
    EXPECT_THROW(Writer_->Close().Get().ThrowOnError(), TErrorException);
}

TEST_F(TSchemalesssBlobWriterTest, MissingValue)
{
    CreateWriter();

    auto row1 = MakeRow(NameTable_, {
        {TString(TBlobTableSchema::PartIndexColumn), 0},
        {TString(TBlobTableSchema::DataColumn), "hello"}
    });

    auto row2 = MakeRow(NameTable_, {
        {TString(TBlobTableSchema::PartIndexColumn), 1},
    });

    std::vector<TUnversionedRow> rows = {row1.Get(), row2.Get()};

    EXPECT_FALSE(Writer_->Write(rows));
    EXPECT_THROW(Writer_->Close().Get().ThrowOnError(), TErrorException);
}

TEST_F(TSchemalesssBlobWriterTest, InvalidColumnType)
{
    CreateWriter();

    auto row1 = MakeRow(NameTable_, {
        {TString(TBlobTableSchema::PartIndexColumn), false},
        {TString(TBlobTableSchema::DataColumn), "hello"}
    });

    EXPECT_FALSE(Writer_->Write({row1.Get()}));
    EXPECT_THROW(Writer_->Close().Get().ThrowOnError(), TErrorException);
}

TEST_F(TSchemalesssBlobWriterTest, UnsupportedControlAttribute)
{
    ControlAttributesConfig_->EnableTableIndex = true;
    EXPECT_THROW(CreateWriter(), TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFormats
