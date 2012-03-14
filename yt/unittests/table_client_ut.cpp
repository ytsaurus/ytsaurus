#include "stdafx.h"

//#include "table_reader.pb.h"

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
        EXPECT_TRUE(writer->AsyncOpen()->Get().IsOK());

        FOREACH(auto& row, Rows) {
            FOREACH(auto& pair, row) {
                writer->Write(pair.first, pair.second);
            }
            EXPECT_TRUE(writer->AsyncEndRow()->Get().IsOK());
        }

        EXPECT_TRUE(writer->AsyncClose()->Get().IsOK());
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

        EXPECT_TRUE(chunkReader->AsyncOpen()->Get().IsOK());

        for (int i = startRow; i < endRow; ++i) {
            TRow ethalon = FilterRow(Rows[i], channel);

            EXPECT_TRUE(chunkReader->IsValid());
            auto row = chunkReader->GetCurrentRow();
            std::sort(row.begin(), row.end());

            EXPECT_EQ(ethalon, row);
            EXPECT_TRUE(chunkReader->AsyncNextRow()->Get().IsOK());
        }

        EXPECT_FALSE(chunkReader->IsValid());
    }

private:
    std::vector<TRow> Rows;
};

////////////////////////////////////////////////////////////////////////////////

std::vector<TRow> CreateTwoRows()
{
    std::vector<TRow> rows;
    {
        TRow row;
        row.push_back(std::make_pair("a", TValue("a")));
        rows.push_back(row);
    } {
        TRow row;
        row.push_back(std::make_pair("b", TValue("b")));
        row.push_back(std::make_pair("c", TValue("c")));
        rows.push_back(row);
    }
    return rows;
}

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

    IAsyncWriter::TPtr CreateAsyncWriter()
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
        EXPECT_TRUE(chunkReader->AsyncOpen()->Get().IsOK());
        EXPECT_FALSE(chunkReader->IsValid());
    }

    Stroka FileName;
};

TEST_F(TTableClientTest, EmptyKeysSorted)
{
    TReadWriteTest test(CreateTwoRows());

    {
        TSortedValidatingWriter sortedWriter(
            TSchema::Default(),
            ~CreateAsyncWriter());

        test.Write(&sortedWriter);
    }

    {
        // Read whole chunk.
        auto chunkReader = New<TChunkReader>(
            ~New<NChunkClient::TSequentialReader::TConfig>(),
            TChannel::Universal(),
            ~CreateFileReader(),
            NProto::TReadLimit(),
            NProto::TReadLimit());

        test.Read(chunkReader, TChannel::Universal());
    } {
        // Read only second row
        NProto::TReadLimit startLimit;
        startLimit.set_row_index(1);

        TChannel channel = TChannel::Empty(); 
        channel.AddColumn("b");

        auto chunkReader = New<TChunkReader>(
            ~New<NChunkClient::TSequentialReader::TConfig>(),
            channel,
            ~CreateFileReader(),
            startLimit,
            NProto::TReadLimit());

        test.Read(chunkReader, channel, 1);
    } {
        // Read only first row
        NProto::TReadLimit endLimit;
        endLimit.set_row_index(1);

        auto chunkReader = New<TChunkReader>(
            ~New<NChunkClient::TSequentialReader::TConfig>(),
            TChannel::Empty(),
            ~CreateFileReader(),
            NProto::TReadLimit(),
            endLimit);

        test.Read(chunkReader, TChannel::Empty(), 0, 1);
    } {
        // Unlimited key range, limited number of columns.
        NProto::TReadLimit startLimit; startLimit.mutable_key();
        NProto::TReadLimit endLimit; endLimit.mutable_key();

        TChannel channel = TChannel::Universal(); 
        channel.AddColumn("a");

        auto chunkReader = New<TChunkReader>(
            ~New<NChunkClient::TSequentialReader::TConfig>(),
            channel,
            ~CreateFileReader(),
            startLimit,
            endLimit);

        test.Read(chunkReader, channel);
    } {
        // Read nothing - semifinite key range, empty key in table.
        NProto::TReadLimit startLimit;
        startLimit.mutable_key();
        NProto::TReadLimit endLimit;
        endLimit.mutable_key()->add_values("a");
        endLimit.mutable_key()->add_values("b");

        auto chunkReader = New<TChunkReader>(
            ~New<NChunkClient::TSequentialReader::TConfig>(),
            TChannel::Universal(),
            ~CreateFileReader(),
            startLimit,
            endLimit);

        ReadNone(chunkReader);
    }
}

TEST_F(TTableClientTest, EmptyKeysUnsorted)
{
    TReadWriteTest test(CreateTwoRows());

    {
        auto schema = TSchema::Default();
        {
            channel = TChannel(
        }

        TValidatingWriter sortedWriter(
            TSchema::Default(),
            ~CreateAsyncWriter());

        test.Write(&sortedWriter);
    }

    {
        // Read whole chunk.
        auto chunkReader = New<TChunkReader>(
            ~New<NChunkClient::TSequentialReader::TConfig>(),
            TChannel::Universal(),
            ~CreateFileReader(),
            NProto::TReadLimit(),
            NProto::TReadLimit());

        test.Read(chunkReader, TChannel::Universal());
    } {
        // Read only second row
        NProto::TReadLimit startLimit;
        startLimit.set_row_index(1);

        TChannel channel = TChannel::Universal(); 
        channel.AddColumn("b");

        auto chunkReader = New<TChunkReader>(
            ~New<NChunkClient::TSequentialReader::TConfig>(),
            channel,
            ~CreateFileReader(),
            startLimit,
            NProto::TReadLimit());

        test.Read(chunkReader, channel, 1);
    } {
        // Read only first row
        NProto::TReadLimit endLimit;
        endLimit.set_row_index(1);

        auto chunkReader = New<TChunkReader>(
            ~New<NChunkClient::TSequentialReader::TConfig>(),
            TChannel::Universal(),
            ~CreateFileReader(),
            NProto::TReadLimit(),
            endLimit);

        test.Read(chunkReader, TChannel::Universal(), 0, 1);
    } {
        // Unlimited key range, limited number of columns.
        NProto::TReadLimit startLimit; startLimit.mutable_key();
        NProto::TReadLimit endLimit; endLimit.mutable_key();

        TChannel channel = TChannel::Universal(); 
        channel.AddColumn("a");

        auto chunkReader = New<TChunkReader>(
            ~New<NChunkClient::TSequentialReader::TConfig>(),
            channel,
            ~CreateFileReader(),
            startLimit,
            endLimit);

        test.Read(chunkReader, channel);
    } {
        // Read nothing - semifinite key range, empty key in table.
        NProto::TReadLimit startLimit;
        startLimit.mutable_key();
        NProto::TReadLimit endLimit;
        endLimit.mutable_key()->add_values("a");
        endLimit.mutable_key()->add_values("b");

        auto chunkReader = New<TChunkReader>(
            ~New<NChunkClient::TSequentialReader::TConfig>(),
            TChannel::Universal(),
            ~CreateFileReader(),
            startLimit,
            endLimit);

        ReadNone(chunkReader);
    }
}



////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
