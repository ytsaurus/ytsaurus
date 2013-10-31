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
const Stroka AdYT = "YT - the best distributed storage and computation ever! No SMS.";
const Stroka Zero = "";

class TVersionedTableClientTest : public ::testing::Test {
public:
    TVersionedTableClientTest()
    { }

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

    void WriteInteger(i64 data, int index)
    {
        TRowValue value;
        value.Data.Integer = data;
        value.Type = EColumnType::Integer;
        value.Index = index;
        ChunkWriter->WriteValue(value);
    }

    void WriteDouble(double data, int index)
    {
        TRowValue value;
        value.Data.Double = data;
        value.Type = EColumnType::Double;
        value.Index = index;
        ChunkWriter->WriteValue(value);
    }

    void WriteString(Stroka data, int index)
    {
        TRowValue value;
        value.Data.String = ~data;
        value.Length = data.length();
        value.Type = EColumnType::String;
        value.Index = index;
        ChunkWriter->WriteValue(value);
    }

    void WriteNull(int index)
    {
        TRowValue value;
        value.Type = EColumnType::Null;
        value.Index = index;
        ChunkWriter->WriteValue(value);
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
        schema.Columns().push_back({ "bid", EColumnType::Double });
        schema.Columns().push_back({ "body", EColumnType::String });

        auto nameTable = New<TNameTable>();
        EXPECT_EQ(nameTable->RegisterName("banner"), 0);
        EXPECT_EQ(nameTable->RegisterName("bid"), 1);
        EXPECT_EQ(nameTable->RegisterName("body"), 2);

        EXPECT_EQ(nameTable->RegisterName("beer"), 3);
        EXPECT_EQ(nameTable->RegisterName("beef"), 4);
        EXPECT_EQ(nameTable->RegisterName("beet"), 5);

        ChunkWriter->Open(nameTable, schema);

        WriteInteger(42, 0);
        WriteDouble(42., 1);
        WriteString(TheAnswer, 2);
        ChunkWriter->EndRow();

        WriteInteger(0, 0);
        WriteDouble(0.0, 1);
        WriteNull(2);
        WriteString(Zero, 4);
        ChunkWriter->EndRow();

        WriteInteger(1, 0);
        WriteDouble(100., 1);
        WriteString(AdYT, 2);
        WriteNull(4);
        WriteInteger(100500, 5);
        ChunkWriter->EndRow();

        auto error = ChunkWriter->AsyncClose().Get();
        EXPECT_TRUE(error.IsOK());

        // Initialize reader.
        MemoryReader = New<TMemoryReader>(
            std::move(MemoryWriter->GetBlocks()),
            std::move(MemoryWriter->GetChunkMeta()));

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

    EXPECT_EQ(rows.size(), 3);

    EXPECT_EQ(*nameTable->FindIndex("body"), 0);

    EXPECT_EQ(rows[0].GetValueCount(), 1);
    EXPECT_EQ(rows[0][0].Index, 0);
    EXPECT_STREQ(~ToStroka(rows[0][0]), ~TheAnswer);

    EXPECT_EQ(rows[1].GetValueCount(), 1);
    EXPECT_EQ(rows[1][0].Index, 0);
    EXPECT_EQ(rows[1][0].Type, EColumnType::Null);

    EXPECT_EQ(rows[2].GetValueCount(), 1);
    EXPECT_EQ(rows[2][0].Index, 0);
    EXPECT_STREQ(~ToStroka(rows[2][0]), ~AdYT);

    rows.clear();
    EXPECT_FALSE(ChunkReader->Read(&rows));
}

TEST_F(TVersionedTableClientTest, SimpleReadAll)
{
    PrepareSimple();

    TTableSchema schema;
    schema.Columns().push_back({ "body", EColumnType::String });
    schema.Columns().push_back({ "bid", EColumnType::Double });

    auto nameTable = New<TNameTable>();
    EXPECT_TRUE(ChunkReader->Open(nameTable, schema, true).Get().IsOK());

    std::vector<TRow> rows;
    rows.reserve(10);
    EXPECT_TRUE(ChunkReader->Read(&rows));

    EXPECT_EQ(rows.size(), 3);

    EXPECT_EQ(*nameTable->FindIndex("body"), 0);
    EXPECT_EQ(*nameTable->FindIndex("bid"), 1);

    EXPECT_EQ(rows[0].GetValueCount(), 3);
    EXPECT_EQ(rows[1].GetValueCount(), 4);
    EXPECT_EQ(rows[2].GetValueCount(), 5);

    rows.clear();
    EXPECT_FALSE(ChunkReader->Read(&rows));
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
