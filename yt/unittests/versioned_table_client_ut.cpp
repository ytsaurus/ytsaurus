#include "stdafx.h"

#include <ytlib/new_table_client/chunk_reader.h>
#include <ytlib/new_table_client/chunk_writer.h>
#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/reader.h>
#include <ytlib/new_table_client/row.h>

#include <ytlib/chunk_client/memory_reader.h>
#include <ytlib/chunk_client/memory_writer.h>

#include <core/compression/public.h>

#include <contrib/testing/framework.h>

namespace NYT {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

const Stroka TheAnswer = "the answer to life the universe and everything";
const Stroka Advertisment = "YT - the best distributed storage and computation ever! No SMS.";
const Stroka Empty = "";

class TVersionedTableClientTest
    : public ::testing::Test
{
private:
    virtual void SetUp() override
    {
        auto options = New<TEncodingWriterOptions>();
        options->CompressionCodec = NCompression::ECodec::Snappy;

        MemoryWriter = New<TMemoryWriter>();
        ChunkWriter = New<TChunkWriter>(
            New<TChunkWriterConfig>(),
            options,
            MemoryWriter);
    }

protected:
    IReaderPtr ChunkReader;
    TChunkWriterPtr ChunkWriter;

    TMemoryReaderPtr MemoryReader;
    TMemoryWriterPtr MemoryWriter;

    void WriteInteger(i64 value, int id)
    {
        ChunkWriter->WriteValue(TRowValue::MakeInteger(value, id));
    }

    void WriteDouble(double value, int id)
    {
        ChunkWriter->WriteValue(TRowValue::MakeDouble(value, id));
    }

    void WriteString(const Stroka& value, int id)
    {
        ChunkWriter->WriteValue(TRowValue::MakeString(value, id));
    }

    void WriteNull(int id)
    {
        ChunkWriter->WriteValue(TRowValue::MakeSentinel(EColumnType::Null, id));
    }

    Stroka ToStroka(const TRowValue& value)
    {
        YCHECK(value.Type == EColumnType::String);
        return Stroka(value.Data.String, value.Length);
    }

    void PrepareSimple()
    {
        TTableSchema schema;
        schema.Columns().push_back({ "banner", EColumnType::Integer });
        schema.Columns().push_back({ "bid",    EColumnType::Double  });
        schema.Columns().push_back({ "body",   EColumnType::String  });

        auto nameTable = New<TNameTable>();
        EXPECT_EQ(0, nameTable->RegisterName("banner"));
        EXPECT_EQ(1, nameTable->RegisterName("bid"));
        EXPECT_EQ(2, nameTable->RegisterName("body"));
        EXPECT_EQ(3, nameTable->RegisterName("beer"));
        EXPECT_EQ(4, nameTable->RegisterName("beef"));
        EXPECT_EQ(5, nameTable->RegisterName("beet"));

        ChunkWriter->Open(nameTable, schema);

        WriteInteger(42, 0);
        WriteDouble(42.0, 1);
        WriteString(TheAnswer, 2);
        ChunkWriter->EndRow();

        WriteInteger(0, 0);
        WriteDouble(0.0, 1);
        WriteNull(2);
        WriteString(Empty, 4);
        ChunkWriter->EndRow();

        WriteInteger(1, 0);
        WriteDouble(100.0, 1);
        WriteString(Advertisment, 2);
        WriteNull(4);
        WriteInteger(100500, 5);
        ChunkWriter->EndRow();

        auto error = ChunkWriter->AsyncClose().Get();
        EXPECT_TRUE(error.IsOK());

        // Initialize reader.
        MemoryReader = New<TMemoryReader>(
            std::move(MemoryWriter->GetChunkMeta()),
            std::move(MemoryWriter->GetBlocks()));

        ChunkReader = CreateChunkReader(
            New<TChunkReaderConfig>(),
            MemoryReader);
    }

};

TEST_F(TVersionedTableClientTest, SimpleReadSchemed)
{
    PrepareSimple();

    TTableSchema schema;
    schema.Columns().push_back({ "body", EColumnType::String });

    auto nameTable = New<TNameTable>();
    EXPECT_TRUE(ChunkReader->Open(nameTable, schema).Get().IsOK());

    std::vector<TRow> rows;
    rows.reserve(10);

    EXPECT_TRUE(ChunkReader->Read(&rows));
    EXPECT_EQ(3, rows.size());

    EXPECT_EQ(0, nameTable->GetId("body"));

    EXPECT_EQ(1, rows[0].GetValueCount());
    EXPECT_EQ(0, rows[0][0].Id);
    EXPECT_EQ(EColumnType::String, rows[0][0].Type);
    EXPECT_STREQ(~TheAnswer, ~ToStroka(rows[0][0]));

    EXPECT_EQ(1, rows[1].GetValueCount());
    EXPECT_EQ(0, rows[1][0].Id);
    EXPECT_EQ(EColumnType::Null, rows[1][0].Type);

    EXPECT_EQ(1, rows[2].GetValueCount());
    EXPECT_EQ(0, rows[2][0].Id);
    EXPECT_EQ(EColumnType::String, rows[2][0].Type);
    EXPECT_STREQ(~Advertisment, ~ToStroka(rows[2][0]));

    rows.clear();

    EXPECT_FALSE(ChunkReader->Read(&rows));
    EXPECT_EQ(0, rows.size());
}

TEST_F(TVersionedTableClientTest, SimpleReadAll)
{
    PrepareSimple();

    TTableSchema schema;
    schema.Columns().push_back({ "body", EColumnType::String });
    schema.Columns().push_back({ "bid",  EColumnType::Double });

    auto nameTable = New<TNameTable>();
    EXPECT_TRUE(ChunkReader->Open(nameTable, schema, true).Get().IsOK());

    std::vector<TRow> rows;
    rows.reserve(10);

    EXPECT_TRUE(ChunkReader->Read(&rows));
    EXPECT_EQ(3, rows.size());

    EXPECT_EQ(nameTable->GetId("body"), 0);
    EXPECT_EQ(nameTable->GetId("bid"), 1);

    EXPECT_EQ(3, rows[0].GetValueCount());
    EXPECT_EQ(4, rows[1].GetValueCount());
    EXPECT_EQ(5, rows[2].GetValueCount());

    rows.clear();

    EXPECT_FALSE(ChunkReader->Read(&rows));
    EXPECT_EQ(0, rows.size());
}

TEST_F(TVersionedTableClientTest, SimpleReadBadSchema)
{
    PrepareSimple();

    TTableSchema schema;
    schema.Columns().push_back({ "body", EColumnType::Double });

    auto nameTable = New<TNameTable>();
    EXPECT_FALSE(ChunkReader->Open(nameTable, schema).Get().IsOK());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
