#include "stdafx.h"
#include "framework.h"

#include <ytlib/new_table_client/schemaful_chunk_reader.h>
#include <ytlib/new_table_client/schemaful_chunk_writer.h>
#include <ytlib/new_table_client/config.h>
#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/writer.h>

#include <ytlib/chunk_client/memory_reader.h>
#include <ytlib/chunk_client/memory_writer.h>

#include <core/compression/public.h>

namespace NYT {
namespace NVersionedTableClient {
namespace {

using namespace NChunkClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

const Stroka TheAnswer = "the answer to life the universe and everything";
const Stroka Advertisment = "YT - the best distributed storage and computation ever! No SMS.";
const Stroka Empty = "";

class TVersionedTableClientTest
    : public ::testing::Test
{
protected:
    virtual void SetUp() override
    {
        MemoryWriter = New<TMemoryWriter>();
        ChunkWriter = CreateChunkWriter(
            New<TChunkWriterConfig>(),
            New<TEncodingWriterOptions>(),
            MemoryWriter);
    }

    IReaderPtr ChunkReader;
    IWriterPtr ChunkWriter;

    TMemoryReaderPtr MemoryReader;
    TMemoryWriterPtr MemoryWriter;

    void WriteInteger(i64 value, int id)
    {
        ChunkWriter->WriteValue(MakeIntegerValue<TUnversionedValue>(value, id));
    }

    void WriteDouble(double value, int id)
    {
        ChunkWriter->WriteValue(MakeDoubleValue<TUnversionedValue>(value, id));
    }

    void WriteBoolean(bool value, int id)
    {
        ChunkWriter->WriteValue(MakeBooleanValue<TUnversionedValue>(value, id));
    }

    void WriteString(const Stroka& value, int id)
    {
        ChunkWriter->WriteValue(MakeStringValue<TUnversionedValue>(value, id));
    }

    void WriteNull(int id)
    {
        ChunkWriter->WriteValue(MakeSentinelValue<TUnversionedValue>(EValueType::Null, id));
    }

    Stroka ToStroka(const TUnversionedValue& value)
    {
        YCHECK(value.Type == EValueType::String);
        return Stroka(value.Data.String, value.Length);
    }

    void PrepareSimple()
    {
        TTableSchema schema;
        schema.Columns().push_back({ "banner", EValueType::Int64 });
        schema.Columns().push_back({ "bid",    EValueType::Double  });
        schema.Columns().push_back({ "body",   EValueType::String  });

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

        auto error = ChunkWriter->Close().Get();
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

TEST_F(TVersionedTableClientTest, Empty)
{
    ChunkWriter->Open(New<TNameTable>(), TTableSchema());
    EXPECT_TRUE(ChunkWriter->Close().Get().IsOK());

    MemoryReader = New<TMemoryReader>(
        std::move(MemoryWriter->GetChunkMeta()),
        std::move(MemoryWriter->GetBlocks()));

    ChunkReader = CreateChunkReader(
        New<TChunkReaderConfig>(),
        MemoryReader);

    EXPECT_TRUE(ChunkReader->Open(New<TNameTable>(), TTableSchema()).Get().IsOK());

    std::vector<TUnversionedRow> rows;
    rows.reserve(10);

    EXPECT_FALSE(ChunkReader->Read(&rows));
}

TEST_F(TVersionedTableClientTest, SimpleReadSchemaful)
{
    PrepareSimple();

    TTableSchema schema;
    schema.Columns().push_back({ "body", EValueType::String });

    auto nameTable = New<TNameTable>();
    EXPECT_TRUE(ChunkReader->Open(nameTable, schema).Get().IsOK());

    std::vector<TUnversionedRow> rows;
    rows.reserve(10);

    EXPECT_TRUE(ChunkReader->Read(&rows));
    EXPECT_EQ(3, rows.size());

    EXPECT_EQ(0, nameTable->GetId("body"));

    EXPECT_EQ(1, rows[0].GetCount());
    EXPECT_EQ(0, rows[0][0].Id);
    EXPECT_EQ(EValueType::String, rows[0][0].Type);
    EXPECT_STREQ(~TheAnswer, ~ToStroka(rows[0][0]));

    EXPECT_EQ(1, rows[1].GetCount());
    EXPECT_EQ(0, rows[1][0].Id);
    EXPECT_EQ(EValueType::Null, rows[1][0].Type);

    EXPECT_EQ(1, rows[2].GetCount());
    EXPECT_EQ(0, rows[2][0].Id);
    EXPECT_EQ(EValueType::String, rows[2][0].Type);
    EXPECT_STREQ(~Advertisment, ~ToStroka(rows[2][0]));

    rows.clear();

    EXPECT_FALSE(ChunkReader->Read(&rows));
    EXPECT_EQ(0, rows.size());
}

TEST_F(TVersionedTableClientTest, SimpleReadAll)
{
    PrepareSimple();

    TTableSchema schema;
    schema.Columns().push_back({ "body", EValueType::String });
    schema.Columns().push_back({ "bid",  EValueType::Double });

    auto nameTable = New<TNameTable>();
    EXPECT_TRUE(ChunkReader->Open(nameTable, schema, true).Get().IsOK());

    std::vector<TUnversionedRow> rows;
    rows.reserve(10);

    EXPECT_TRUE(ChunkReader->Read(&rows));
    EXPECT_EQ(3, rows.size());

    EXPECT_EQ(nameTable->GetId("body"), 0);
    EXPECT_EQ(nameTable->GetId("bid"), 1);

    EXPECT_EQ(3, rows[0].GetCount());
    EXPECT_EQ(4, rows[1].GetCount());
    EXPECT_EQ(5, rows[2].GetCount());

    rows.clear();

    EXPECT_FALSE(ChunkReader->Read(&rows));
    EXPECT_EQ(0, rows.size());
}

TEST_F(TVersionedTableClientTest, SimpleReadBadSchema)
{
    PrepareSimple();

    TTableSchema schema;
    schema.Columns().push_back({ "body", EValueType::Double });

    auto nameTable = New<TNameTable>();
    EXPECT_FALSE(ChunkReader->Open(nameTable, schema).Get().IsOK());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NVersionedTableClient
} // namespace NYT
