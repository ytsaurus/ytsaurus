#include "stdafx.h"
#include "framework.h"

#include "versioned_table_client_ut.h"

#include <ytlib/chunk_client/data_statistics.h>

#include <ytlib/new_table_client/config.h>
#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/schemaless_chunk_reader.h>
#include <ytlib/new_table_client/schemaless_chunk_writer.h>
#include <ytlib/new_table_client/unversioned_row.h>

#include <ytlib/chunk_client/memory_reader.h>
#include <ytlib/chunk_client/memory_writer.h>

#include <ytlib/transaction_client/public.h>

#include <core/compression/public.h>

namespace NYT {
namespace NVersionedTableClient {
namespace {

using namespace NChunkClient;
using namespace NTransactionClient;

using NChunkClient::NProto::TChunkMeta;
using NChunkClient::NProto::ZeroDataStatistics;

////////////////////////////////////////////////////////////////////////////////

Stroka A("a");
const int HugeRowCount = 1000000;
const int SmallRowCount = 100;

class TSchemalessChunksTest
    : public TVersionedTableClientTestBase
{
protected:
    virtual void SetUp() override
    {

        NameTable = New<TNameTable>();
        MemoryWriter = New<TMemoryWriter>();

        KeyColumns = {"k1", "k2", "k3"};
        EXPECT_EQ(0, NameTable->RegisterName("k1"));
        EXPECT_EQ(1, NameTable->RegisterName("k2"));
        EXPECT_EQ(2, NameTable->RegisterName("k3"));

        EXPECT_EQ(3, NameTable->RegisterName("v1"));
        EXPECT_EQ(4, NameTable->RegisterName("v2"));

        auto config = New<TChunkWriterConfig>();
        config->BlockSize = 2 * 1024 * 1024;

        ChunkWriter = CreateSchemalessChunkWriter(
            config,
            New<TChunkWriterOptions>(),
            NameTable,
            KeyColumns,
            MemoryWriter);

        EXPECT_TRUE(ChunkWriter->Open().Get().IsOK());
    }

    TNameTablePtr NameTable;
    TKeyColumns KeyColumns;

    ISchemalessReaderPtr ChunkReader;
    ISchemalessChunkWriterPtr ChunkWriter;

    IReaderPtr MemoryReader;
    TMemoryWriterPtr MemoryWriter;

    TChunkMeta MasterMeta;

    TChunkedMemoryPool MemoryPool;

    void CheckResult(const std::vector<TUnversionedRow>& expected, const std::vector<TUnversionedRow>& actual)
    {
        EXPECT_EQ(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); ++i) {
            ExpectRowsEqual(expected[i], actual[i]);
        }
    }

    void FinishWriter()
    {
        EXPECT_TRUE(ChunkWriter->Close().Get().IsOK());

        MasterMeta = ChunkWriter->GetMasterMeta();

        // Initialize reader.
        MemoryReader = New<TMemoryReader>(
            std::move(MemoryWriter->GetChunkMeta()),
            std::move(MemoryWriter->GetBlocks()));
    }

    std::vector<TUnversionedRow> CreateManyRows(int startIndex = 0, int endIndex = HugeRowCount)
    {
        std::vector<TUnversionedRow> rows;
        for (int i = startIndex; i < endIndex; ++i) {
            TUnversionedRow row = TUnversionedRow::Allocate(&MemoryPool, 4);
            row[0] = MakeUnversionedStringValue(A, 0);
            row[1] = MakeUnversionedIntegerValue(i, 1);
            row[2] = MakeUnversionedSentinelValue(EValueType::Null, 2);

            row[3] = MakeUnversionedDoubleValue(3.1415, 3 + (i % 2));

            rows.push_back(row);
        }
        return rows;
    }

    void WriteManyRows()
    {
        ChunkWriter->Write(CreateManyRows());
        FinishWriter();
    }

    void WriteFewRows()
    {
        ChunkWriter->Write(CreateManyRows(0, SmallRowCount));
        FinishWriter();
    }

};

TEST_F(TSchemalessChunksTest, ReadAllUnsorted)
{
    WriteManyRows();
    std::vector<TUnversionedRow> expected = CreateManyRows();

    TColumnFilter columnFilter;
    columnFilter.All = true;

    auto chunkReader = CreateSchemalessChunkReader(
        New<TChunkReaderConfig>(),
        MemoryReader,
        NameTable,
        TKeyColumns(),
        MasterMeta,
        TReadLimit(),
        TReadLimit(),
        columnFilter);

    EXPECT_TRUE(chunkReader->Open().Get().IsOK());

    auto it = expected.begin();

    std::vector<TUnversionedRow> actual;
    actual.reserve(997);

    while (chunkReader->Read(&actual)) {
        if (actual.empty()) {
            EXPECT_TRUE(chunkReader->GetReadyEvent().Get().IsOK());
            continue;
        }
        std::vector<TUnversionedRow> ex(it, it + actual.size());
        CheckResult(ex, actual);
        it += actual.size();
    }
}

TEST_F(TSchemalessChunksTest, EmptyRead)
{
    WriteFewRows();
    std::vector<TUnversionedRow> expected;

    TColumnFilter columnFilter;
    columnFilter.All = true;

    TReadLimit lowerLimit;
    lowerLimit.SetRowIndex(SmallRowCount);

    auto chunkReader = CreateSchemalessChunkReader(
        New<TChunkReaderConfig>(),
        MemoryReader,
        NameTable,
        TKeyColumns(),
        MasterMeta,
        lowerLimit,
        TReadLimit(),
        columnFilter);

    EXPECT_TRUE(chunkReader->Open().Get().IsOK());

    EXPECT_TRUE(chunkReader->GetFetchingCompletedEvent().IsSet());
    EXPECT_EQ(ZeroDataStatistics(), chunkReader->GetDataStatistics());

    auto it = expected.begin();

    std::vector<TUnversionedRow> actual;
    actual.reserve(997);

    while (chunkReader->Read(&actual)) {
        if (actual.empty()) {
            EXPECT_TRUE(chunkReader->GetReadyEvent().Get().IsOK());
            continue;
        }
        std::vector<TUnversionedRow> ex(it, it + actual.size());
        CheckResult(ex, actual);
        it += actual.size();
    }
}

TEST_F(TSchemalessChunksTest, ReadSortedRange)
{
    WriteManyRows();
    std::vector<TUnversionedRow> expected = CreateManyRows(100000, 800000);

    TColumnFilter columnFilter;
    columnFilter.All = true;

    TUnversionedOwningRowBuilder lowerBuilder;
    lowerBuilder.AddValue(MakeUnversionedStringValue(A, 0));
    lowerBuilder.AddValue(MakeUnversionedIntegerValue(100000, 1));

    TReadLimit lowerLimit;
    lowerLimit.SetKey(lowerBuilder.GetRowAndReset());

    TUnversionedOwningRowBuilder upperBuilder;
    upperBuilder.AddValue(MakeUnversionedStringValue(A, 0));
    upperBuilder.AddValue(MakeUnversionedIntegerValue(900000, 1));

    TReadLimit upperLimit;
    upperLimit.SetRowIndex(800000);
    upperLimit.SetKey(upperBuilder.GetRowAndReset());

    auto chunkReader = CreateSchemalessChunkReader(
        New<TChunkReaderConfig>(),
        MemoryReader,
        NameTable,
        TKeyColumns(),
        MasterMeta,
        lowerLimit,
        upperLimit,
        columnFilter);

    EXPECT_TRUE(chunkReader->Open().Get().IsOK());

    auto it = expected.begin();

    std::vector<TUnversionedRow> actual;
    actual.reserve(997);

    while (chunkReader->Read(&actual)) {
        if (actual.empty()) {
            EXPECT_TRUE(chunkReader->GetReadyEvent().Get().IsOK());
            continue;
        }
        std::vector<TUnversionedRow> ex(it, it + actual.size());
        CheckResult(ex, actual);
        it += actual.size();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NVersionedTableClient
} // namespace NYT
