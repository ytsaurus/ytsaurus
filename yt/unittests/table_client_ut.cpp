#include "stdafx.h"

#include <ytlib/chunk_client/file_reader.h>
#include <ytlib/chunk_client/file_writer.h>

#include <ytlib/table_client/validating_writer.h>
#include <ytlib/table_client/sorted_validating_writer.h>

#include <ytlib/table_client/chunk_reader.h>
#include <ytlib/table_client/chunk_writer.h>

#include <util/system/fs.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

TRow FilterRow(const TRow& row, const TChannel& channel)
{
    TRow result;
    FOREACH(auto& pair, row) {
        if (channel.Contains(pair.first))
            result.push_back(pair);
    }

    std::sort(result.begin(), result.end());
    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TReadWriteTest
{
public:
    TReadWriteTest(std::vector<TRow>&& rows)
        : Rows(rows)
    { }

    void Write(TValidatingWriter* writer)
    {
        EXPECT_IS_TRUE(writer->AsyncOpen()->Get().IsOK());

        FOREACH(auto& row, Rows) {
            FOREACH(auto& pair, row) {
                writer->Write(pair.first, pair.second);
            }
            EXPECT_IS_TRUE(writer->AsyncEndRow()->Get().IsOK());
        }

        EXPECT_IS_TRUE(writer->AsyncClose()->Get().IsOK());
    }

    void Read(
        TChunkReader::TPtr chunkReader, 
        const TChannel& channel, 
        int startRow = 0, 
        int endRow = std::numeric_limits<int>::max())
    {
        int rowsSize = static_cast<int>(Rows.size());
        startRow = std::min(startRow, rowsSize);
        endRow = std::min(endRow, rowsSize);

        EXPECT_IS_TRUE(chunkReader->AsyncOpen()->Get().IsOK());

        for (int i = startRow; i < endRow; ++i) {
            TRow ethalon = FilterRow(Rows[i], channel);

            EXPECT_IS_TRUE(chunkReader->IsValid());
            auto row = chunkReader->GetCurrentRow();
            std::sort(row.begin(), row.end());

            EXPECT_EQ(ethalon, row);
            EXPECT_IS_TRUE(chunkReader->AsyncNextRow()->Get().IsOK());
        }

        EXPECT_IS_FALSE(chunkReader->IsValid());
    }

private:
    std::vector<TRow> Rows;
};

////////////////////////////////////////////////////////////////////////////////

class TTableClientTest
    : public ::testing::Test
{
public:
    TTableClientTest()
        : FileName("test.chunk")
    { }

    void TearDown()
    {
        NFs::Remove(~FileName);
    }

    IAsyncBlockWriter::TPtr CreateAsyncWriter()
    {
        auto fileWriter = New<NChunkClient::TChunkFileWriter>(
            NChunkClient::TChunkId::Create(), FileName);
        fileWriter->Open();

        auto chunkWriter = New<TChunkWriter>(
            ~New<TChunkWriter::TConfig>(), 
            ~fileWriter);

        return chunkWriter;
    }

    NChunkClient::IAsyncReader::TPtr CreateFileReader()
    {
        auto reader = New<NChunkClient::TChunkFileReader>(FileName);
        reader->Open();
        return reader;
    }

    void ReadNone(TChunkReader::TPtr chunkReader)
    {
        EXPECT_IS_TRUE(chunkReader->AsyncOpen()->Get().IsOK());
        EXPECT_IS_FALSE(chunkReader->IsValid());
    }

    void OpenFail(TChunkReader::TPtr chunkReader) 
    {
        EXPECT_IS_FALSE(chunkReader->AsyncOpen()->Get().IsOK());
    }

    Stroka FileName;
};

////////////////////////////////////////////////////////////////////////////////

std::vector<TRow> CreateTwoRows()
{
    static Stroka a("a");
    static Stroka b("b");
    static Stroka c("c");

    std::vector<TRow> rows;
    {
        TRow row;
        row.push_back(std::make_pair(a, TValue(a)));
        rows.push_back(row);
    } {
        TRow row;
        row.push_back(std::make_pair(b, TValue(b)));
        row.push_back(std::make_pair(c, TValue(c)));
        rows.push_back(row);
    }
    return rows;
}

////////////////////////////////////////////////////////////////////////////////

class TEmptyKeysSortedRows2
    : public TTableClientTest
{
public:
    TEmptyKeysSortedRows2()
        : Test(CreateTwoRows())
    { }

    void SetUp()
    {
        TSortedValidatingWriter sortedWriter(
            TSchema::CreateDefault(),
            ~CreateAsyncWriter());

        Test.Write(&sortedWriter);
    }

    TReadWriteTest Test;
};

TEST_F(TEmptyKeysSortedRows2, ReadWholeChunk)
{
    // Read whole chunk.
    auto chunkReader = New<TChunkReader>(
        ~New<NChunkClient::TSequentialReader::TConfig>(),
        TChannel::CreateUniversal(),
        ~CreateFileReader(),
        NProto::TReadLimit(),
        NProto::TReadLimit(),
        "");

    Test.Read(chunkReader, TChannel::CreateUniversal());
}

TEST_F(TEmptyKeysSortedRows2, ReadSecondRow) 
{
    NProto::TReadLimit startLimit;
    startLimit.set_row_index(1);

    TChannel channel = TChannel::CreateEmpty(); 
    channel.AddColumn("b");

    auto chunkReader = New<TChunkReader>(
        ~New<NChunkClient::TSequentialReader::TConfig>(),
        channel,
        ~CreateFileReader(),
        startLimit,
        NProto::TReadLimit(),
        "");

    Test.Read(chunkReader, channel, 1);
} 

TEST_F(TEmptyKeysSortedRows2, ReadFirstRow) 
{
    // Read only first row
    NProto::TReadLimit endLimit;
    endLimit.set_row_index(1);

    auto chunkReader = New<TChunkReader>(
        ~New<NChunkClient::TSequentialReader::TConfig>(),
        TChannel::CreateEmpty(),
        ~CreateFileReader(),
        NProto::TReadLimit(),
        endLimit,
        "");

    Test.Read(chunkReader, TChannel::CreateEmpty(), 0, 1);
} 

TEST_F(TEmptyKeysSortedRows2, ReadChannel) 
{
    // Unlimited key range, limited number of columns.
    NProto::TReadLimit startLimit; startLimit.mutable_key();
    NProto::TReadLimit endLimit; endLimit.mutable_key();

    TChannel channel = TChannel::CreateEmpty(); 
    channel.AddColumn("a");

    auto chunkReader = New<TChunkReader>(
        ~New<NChunkClient::TSequentialReader::TConfig>(),
        channel,
        ~CreateFileReader(),
        startLimit,
        endLimit,
        "");

    Test.Read(chunkReader, channel);
}

TEST_F(TEmptyKeysSortedRows2, ReadNothing) {
    // Read nothing - semifinite key range, empty key in table.
    NProto::TReadLimit startLimit;
    startLimit.mutable_key();
    NProto::TReadLimit endLimit;
    endLimit.mutable_key()->add_values("a");
    endLimit.mutable_key()->add_values("b");

    auto chunkReader = New<TChunkReader>(
        ~New<NChunkClient::TSequentialReader::TConfig>(),
        TChannel::CreateUniversal(),
        ~CreateFileReader(),
        startLimit,
        endLimit,
        "");

    ReadNone(chunkReader);
}

////////////////////////////////////////////////////////////////////////////////

class TEmptyKeysUnsortedRows2
    : public TTableClientTest
{
public:
    TEmptyKeysUnsortedRows2()
        : Test(CreateTwoRows())
    { }

    void SetUp()
    {
        auto schema = TSchema::CreateDefault();
        {
            auto channel = TChannel::CreateEmpty();
            channel.AddColumn("a");
            schema.AddChannel(channel);
        } {
            auto channel = TChannel::CreateEmpty();
            channel.AddColumn("b");
            schema.AddChannel(channel);
        } {
            auto channel = TChannel::CreateEmpty();
            channel.AddRange(TRange("ba"));
            schema.AddChannel(channel);
        }

        TValidatingWriter unsortedWriter(
            schema,
            ~CreateAsyncWriter());

        Test.Write(&unsortedWriter);
    }

    TReadWriteTest Test;
};

TEST_F(TEmptyKeysUnsortedRows2, ReadWhole)
{
    auto chunkReader = New<TChunkReader>(
        ~New<NChunkClient::TSequentialReader::TConfig>(),
        TChannel::CreateUniversal(),
        ~CreateFileReader(),
        NProto::TReadLimit(),
        NProto::TReadLimit(),
        "");

    Test.Read(chunkReader, TChannel::CreateUniversal());
} 

TEST_F(TEmptyKeysUnsortedRows2, ReadSecond) 
{
    NProto::TReadLimit startLimit;
    startLimit.set_row_index(1);

    TChannel channel = TChannel::CreateEmpty(); 
    channel.AddColumn("b");

    auto chunkReader = New<TChunkReader>(
        ~New<NChunkClient::TSequentialReader::TConfig>(),
        channel,
        ~CreateFileReader(),
        startLimit,
        NProto::TReadLimit(),
        "");

    Test.Read(chunkReader, channel, 1);
}

TEST_F(TEmptyKeysUnsortedRows2, ReadKeyRangeFail) 
{
    NProto::TReadLimit startLimit; startLimit.mutable_key();
    NProto::TReadLimit endLimit; endLimit.mutable_key();

    auto chunkReader = New<TChunkReader>(
        ~New<NChunkClient::TSequentialReader::TConfig>(),
        TChannel::CreateEmpty(),
        ~CreateFileReader(),
        startLimit,
        endLimit,
        "");

    OpenFail(chunkReader);
}

////////////////////////////////////////////////////////////////////////////////
std::vector<TRow> CreateManyRows(int blockSize)
{
    static Stroka b("bing");
    static Stroka g("google");
    static Stroka y("yandex");

    std::vector<TRow> rows;

    for (int i = 0; i < blockSize; ++i)
    {
        TRow row;
        row.push_back(std::make_pair(b, TValue(b)));
        row.push_back(std::make_pair(g, TValue(g)));
        row.push_back(std::make_pair(y, TValue(y)));
        rows.push_back(row);
    }

    for (int i = 0; i < blockSize; ++i)
    {
        TRow row;
        row.push_back(std::make_pair(b, TValue(g)));
        row.push_back(std::make_pair(g, TValue(y)));
        row.push_back(std::make_pair(y, TValue(b)));
        rows.push_back(row);
    } 

    for (int i = 0; i < blockSize; ++i)
    {
        TRow row;
        row.push_back(std::make_pair(b, TValue(y)));
        row.push_back(std::make_pair(g, TValue(b)));
        row.push_back(std::make_pair(y, TValue(g)));
        rows.push_back(row);
    }
    return rows;
}

////////////////////////////////////////////////////////////////////////////////

class TManyRows
    : public TTableClientTest
{
public:
    TManyRows()
        : BlockSize(20000)
        , Test(CreateManyRows(BlockSize))
    { }

    void SetUp()
    {
        auto schema = TSchema::CreateDefault();
        {
            auto channel = TChannel::CreateEmpty();
            channel.AddColumn("bing");
            channel.AddRange(TRange("google "));
            schema.AddChannel(channel);
        } {
            auto channel = TChannel::CreateEmpty();
            channel.AddColumn("google");
            channel.AddRange(TRange("", "bing"));
            schema.AddChannel(channel);
        } {
            auto channel = TChannel::CreateEmpty();
            channel.AddRange(TRange("yandex"));
            schema.AddChannel(channel);
        }

        schema.KeyColumns().push_back("bing");
        schema.KeyColumns().push_back("yandex");

        TSortedValidatingWriter sortedWriter(
            schema,
            ~CreateAsyncWriter());

        Test.Write(&sortedWriter);
    }

    const int BlockSize;
    TReadWriteTest Test;
};

TEST_F(TManyRows, ReadWhole)
{
    auto chunkReader = New<TChunkReader>(
        ~New<NChunkClient::TSequentialReader::TConfig>(),
        TChannel::CreateUniversal(),
        ~CreateFileReader(),
        NProto::TReadLimit(),
        NProto::TReadLimit(),
        "");

    Test.Read(chunkReader, TChannel::CreateUniversal());
}

TEST_F(TManyRows, ReadKeyRange)
{
    NProto::TReadLimit startLimit;
    startLimit.mutable_key()->add_values("c");
    startLimit.mutable_key()->add_values("d");

    NProto::TReadLimit endLimit;
    endLimit.mutable_key()->add_values("google+");

    auto channel = TChannel::CreateEmpty();
    channel.AddRange(TRange("google"));

    auto chunkReader = New<TChunkReader>(
        ~New<NChunkClient::TSequentialReader::TConfig>(),
        channel,
        ~CreateFileReader(),
        startLimit,
        endLimit,
        "");

    Test.Read(chunkReader, channel, BlockSize, 2 * BlockSize);
} 

////////////////////////////////////////////////////////////////////////////////

std::vector<TRow> CreateLongRows(int blockSize)
{
    static Stroka b("bing");
    static Stroka g("google");
    static Stroka y("yandex");

    const int N = 30000;
    std::vector<Stroka> bvector(N, b);
    static Stroka bvalue = JoinToString(bvector);

    std::vector<Stroka> gvector(N, g);
    static Stroka gvalue = JoinToString(gvector);

    std::vector<Stroka> yavector(N, y);
    static Stroka yvalue = JoinToString(yavector);

    std::vector<TRow> rows;

    for (int i = 0; i < blockSize; ++i) {
        TRow row;
        row.push_back(std::make_pair(b, TValue(bvalue)));
        row.push_back(std::make_pair(g, TValue(gvalue)));
        row.push_back(std::make_pair(y, TValue(yvalue)));
        rows.push_back(row);
    } 
    
    for (int i = 0; i < blockSize; ++i) {
        TRow row;
        row.push_back(std::make_pair(b, TValue(gvalue)));
        row.push_back(std::make_pair(g, TValue(yvalue)));
        row.push_back(std::make_pair(y, TValue(bvalue)));
        rows.push_back(row);
    } 
    
    for (int i = 0; i < blockSize; ++i) {
        TRow row;
        row.push_back(std::make_pair(b, TValue(yvalue)));
        row.push_back(std::make_pair(g, TValue(bvalue)));
        row.push_back(std::make_pair(y, TValue(gvalue)));
        rows.push_back(row);
    }

    return rows;
}

////////////////////////////////////////////////////////////////////////////////

class TVeryLongRows
    : public TTableClientTest
{
public:
    TVeryLongRows()
        : BlockSize(20)
        , Test(CreateLongRows(BlockSize))
    { }

    void SetUp()
    {
        auto schema = TSchema::CreateDefault();
        {
            auto channel = TChannel::CreateEmpty();
            channel.AddColumn("bing");
            channel.AddRange(TRange("google "));
            schema.AddChannel(channel);
        } {
            auto channel = TChannel::CreateEmpty();
            channel.AddColumn("google");
            channel.AddRange(TRange("", "bing"));
            schema.AddChannel(channel);
        } {
            auto channel = TChannel::CreateEmpty();
            channel.AddRange(TRange("yandex"));
            schema.AddChannel(channel);
        }

        schema.KeyColumns().push_back("bing");
        schema.KeyColumns().push_back("yandex");

        TSortedValidatingWriter sortedWriter(
            schema,
            ~CreateAsyncWriter());

        Test.Write(&sortedWriter);
    }

    const int BlockSize;
    TReadWriteTest Test;
};

TEST_F(TVeryLongRows, ReadKeyRange)
{
    NProto::TReadLimit startLimit;
    startLimit.mutable_key()->add_values("c");
    startLimit.mutable_key()->add_values("d");

    NProto::TReadLimit endLimit;
    endLimit.mutable_key()->add_values("yahoo");

    auto channel = TChannel::CreateEmpty();
    channel.AddRange(TRange("google"));

    auto chunkReader = New<TChunkReader>(
        ~New<NChunkClient::TSequentialReader::TConfig>(),
        channel,
        ~CreateFileReader(),
        startLimit,
        endLimit,
        "");

    Test.Read(chunkReader, channel, BlockSize, 2 * BlockSize);
} 


////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
