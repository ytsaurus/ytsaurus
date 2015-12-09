#include "framework.h"
#include "versioned_table_client_ut.h"

#include <yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/ytlib/chunk_client/data_statistics.h>
#include <yt/ytlib/chunk_client/memory_reader.h>
#include <yt/ytlib/chunk_client/memory_writer.h>

#include <yt/ytlib/table_client/config.h>
#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/ytlib/table_client/schemaless_chunk_writer.h>
#include <yt/ytlib/table_client/unversioned_row.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/compression/public.h>

#include <yt/core/misc/common.h>

namespace NYT {
namespace NTableClient {
namespace {

using namespace NChunkClient;
using namespace NTransactionClient;

using NChunkClient::NProto::TChunkMeta;
using NChunkClient::NProto::ZeroDataStatistics;

////////////////////////////////////////////////////////////////////////////////

Stroka A("a");
const int SmallRowCount  = 100;
const int MediumRowCount = 10000;
const int HugeRowCount   = 1000000;

class TSchemalessChunksTest
    : public TVersionedTableClientTestBase
{
protected:
    virtual void SetUp() override
    {
        NameTable = New<TNameTable>();

        KeyColumns = {"k1", "k2", "k3"};
        EXPECT_EQ(0, NameTable->RegisterName("k1"));
        EXPECT_EQ(1, NameTable->RegisterName("k2"));
        EXPECT_EQ(2, NameTable->RegisterName("k3"));

        EXPECT_EQ(3, NameTable->RegisterName("v1"));
        EXPECT_EQ(4, NameTable->RegisterName("v2"));
    }

    TNameTablePtr NameTable;
    TKeyColumns KeyColumns;

    ISchemalessReaderPtr ChunkReader;
    ISchemalessChunkWriterPtr ChunkWriter;

    IChunkReaderPtr MemoryReader;
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

    void SetUpWriter()
    {
        MemoryWriter = New<TMemoryWriter>();

        auto config = New<TChunkWriterConfig>();
        config->BlockSize = 2 * 1024;

        ChunkWriter = CreateSchemalessChunkWriter(
            config,
            New<TChunkWriterOptions>(),
            NameTable,
            KeyColumns,
            MemoryWriter);

        EXPECT_TRUE(ChunkWriter->Open().Get().IsOK());
    }

    void FinishWriter()
    {
        EXPECT_TRUE(ChunkWriter->Close().Get().IsOK());

        MasterMeta = ChunkWriter->GetMasterMeta();

        // Initialize reader.
        MemoryReader = CreateMemoryReader(
            std::move(MemoryWriter->GetChunkMeta()),
            std::move(MemoryWriter->GetBlocks()));
    }

    std::vector<TUnversionedRow> CreateManyRows(int startIndex = 0, int endIndex = HugeRowCount)
    {
        std::vector<TUnversionedRow> rows;
        for (int i = startIndex; i < endIndex; ++i) {
            TUnversionedRow row = TUnversionedRow::Allocate(&MemoryPool, 4);
            row[0] = MakeUnversionedStringValue(A, 0);
            row[1] = MakeUnversionedInt64Value(i, 1);
            row[2] = MakeUnversionedSentinelValue(EValueType::Null, 2);

            row[3] = MakeUnversionedDoubleValue(3.1415, 3 + (i % 2));

            rows.push_back(row);
        }
        return rows;
    }

    void WriteRows(int startIndex, int endIndex)
    {
        SetUpWriter();
        ChunkWriter->Write(CreateManyRows(startIndex, endIndex));
        FinishWriter();
    }

    void WriteManyRows()
    {
        WriteRows(0, HugeRowCount);
    }

    void WriteFewRows()
    {
        WriteRows(0, SmallRowCount);
    }

    std::vector<TUnversionedOwningRow> ReadRows(
        i64 tableRowIndex = 0,
        TNullable<double> samplingRate = Null)
    {
        std::vector<TUnversionedOwningRow> rows;

        TColumnFilter columnFilter;

        auto config = New<TChunkReaderConfig>();
        config->SamplingSeed = 42;
        config->SamplingRate = samplingRate;

        auto chunkReader = CreateSchemalessChunkReader(
            config,
            MemoryReader,
            NameTable,
            GetNullBlockCache(),
            TKeyColumns(),
            MasterMeta,
            TReadLimit(),
            TReadLimit(),
            columnFilter,
            tableRowIndex);

        EXPECT_TRUE(chunkReader->Open().Get().IsOK());

        std::vector<TUnversionedRow> actual;
        actual.reserve(997);

        while (chunkReader->Read(&actual)) {
            if (actual.empty()) {
                EXPECT_TRUE(chunkReader->GetReadyEvent().Get().IsOK());
                continue;
            }
            for (auto row : actual) {
                rows.push_back(TUnversionedOwningRow(row));
            }
        }

        return rows;
    }

};

TEST_F(TSchemalessChunksTest, ReadAllUnsorted)
{
    WriteManyRows();
    std::vector<TUnversionedRow> expected = CreateManyRows();

    TColumnFilter columnFilter;

    auto chunkReader = CreateSchemalessChunkReader(
        New<TChunkReaderConfig>(),
        MemoryReader,
        NameTable,
        GetNullBlockCache(),
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

    TReadLimit lowerLimit;
    lowerLimit.SetRowIndex(SmallRowCount);

    auto chunkReader = CreateSchemalessChunkReader(
        New<TChunkReaderConfig>(),
        MemoryReader,
        NameTable,
        GetNullBlockCache(),
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

    TUnversionedOwningRowBuilder lowerBuilder;
    lowerBuilder.AddValue(MakeUnversionedStringValue(A, 0));
    lowerBuilder.AddValue(MakeUnversionedInt64Value(100000, 1));

    TReadLimit lowerLimit;
    lowerLimit.SetKey(lowerBuilder.FinishRow());
    // Test initialization, when key limit is upper than row limit.
    lowerLimit.SetRowIndex(10);

    TUnversionedOwningRowBuilder upperBuilder;
    upperBuilder.AddValue(MakeUnversionedStringValue(A, 0));
    upperBuilder.AddValue(MakeUnversionedInt64Value(900000, 1));

    TReadLimit upperLimit;
    upperLimit.SetRowIndex(800000);
    upperLimit.SetKey(upperBuilder.FinishRow());

    auto chunkReader = CreateSchemalessChunkReader(
        New<TChunkReaderConfig>(),
        MemoryReader,
        NameTable,
        GetNullBlockCache(),
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

TEST_F(TSchemalessChunksTest, SampledRead)
{
    WriteManyRows();
    std::vector<TUnversionedRow> expected = CreateManyRows();

    TColumnFilter columnFilter;

    auto config = New<TChunkReaderConfig>();
    config->SamplingSeed = 42;

    for (double samplingRate = 0.0; samplingRate <= 1.0; samplingRate += 0.25) {
        config->SamplingRate = samplingRate;
        double variation = samplingRate * (1 - samplingRate);

        auto chunkReader = CreateSchemalessChunkReader(
            config,
            MemoryReader,
            NameTable,
            GetNullBlockCache(),
            TKeyColumns(),
            MasterMeta,
            TReadLimit(),
            TReadLimit(),
            columnFilter);

        EXPECT_TRUE(chunkReader->Open().Get().IsOK());

        std::vector<TUnversionedRow> actual;
        actual.reserve(997);

        int readRowCount = 0;
        while (chunkReader->Read(&actual)) {
            if (actual.empty()) {
                EXPECT_TRUE(chunkReader->GetReadyEvent().Get().IsOK());
                continue;
            }
            readRowCount += actual.size();
        }
        double rate = readRowCount * 1.0 / expected.size();
        EXPECT_GE(rate, samplingRate - variation);
        EXPECT_LE(rate, samplingRate + variation);
    }
}

TEST_F(TSchemalessChunksTest, MultiPartSampledRead)
{
    for (double samplingRate = 0.0; samplingRate <= 1.0; samplingRate += 0.25) {
        i64 rowCount = MediumRowCount;

        WriteRows(0, rowCount);
        auto expected = ReadRows(0, samplingRate);

        auto expectedIt = expected.begin();

        int partCount = 10;
        i64 partSize = (rowCount + partCount - 1) / partCount;
        for (int i = 0; i < partCount; ++i) {
            int lowerBound = i * partSize;
            int upperBound = std::min((i + 1) * partSize, rowCount);

            WriteRows(lowerBound, upperBound);
            auto actual = ReadRows(lowerBound, samplingRate);

            for (auto actualRow : actual) {
                ExpectRowsEqual(expectedIt->Get(), actualRow.Get());
                ++expectedIt;
            }
        }
        YCHECK(expectedIt == expected.end());
    }
}

TEST_F(TSchemalessChunksTest, ReadMultipleIndexRanges)
{
    WriteManyRows();

    TColumnFilter columnFilter;

    std::vector<TReadRange> readRanges;
    std::vector<TUnversionedRow> expected;

    for (int index = 0; index < 10; ++index) {
        int startIndex = 100000 + 1000 * index;
        int endIndex = 100000 + 1000 * index + 100;

        std::vector<TUnversionedRow> expectedInRange = CreateManyRows(startIndex, endIndex);
        expected.insert(expected.end(), expectedInRange.begin(), expectedInRange.end());

        TReadLimit lower;
        TReadLimit upper;
        lower.SetRowIndex(startIndex);
        upper.SetRowIndex(endIndex);
        readRanges.emplace_back(std::move(lower), std::move(upper));
    }

    {
        TReadLimit lower;
        TReadLimit upper;
        lower.SetRowIndex(HugeRowCount);
        upper.SetRowIndex(HugeRowCount + 100);
        readRanges.emplace_back(std::move(lower), std::move(upper));
    }

    auto chunkReader = CreateSchemalessChunkReader(
        New<TChunkReaderConfig>(),
        MemoryReader,
        NameTable,
        GetNullBlockCache(),
        TKeyColumns(),
        MasterMeta,
        std::move(readRanges),
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
        EXPECT_TRUE(actual.size() <= std::distance(it, expected.end()));
        std::vector<TUnversionedRow> ex(it, it + actual.size());
        CheckResult(ex, actual);
        it += actual.size();
    }
}

TEST_F(TSchemalessChunksTest, ReadMultipleKeyRanges)
{
    WriteManyRows();

    TColumnFilter columnFilter;

    std::vector<TReadRange> readRanges;
    std::vector<TUnversionedRow> expected;

    for (int index = 0; index < 10; ++index) {
        int startIndex = 100000 + 1000 * index;
        int endIndex = 100000 + 1000 * index + 100;

        std::vector<TUnversionedRow> expectedInRange = CreateManyRows(startIndex, endIndex);
        expected.insert(expected.end(), expectedInRange.begin(), expectedInRange.end());

        TReadLimit lower;
        TReadLimit upper;
        TUnversionedOwningRowBuilder builder;

        builder.AddValue(MakeUnversionedStringValue(A, 0));
        builder.AddValue(MakeUnversionedInt64Value(startIndex, 1));
        lower.SetKey(builder.FinishRow());

        builder.AddValue(MakeUnversionedStringValue(A, 0));
        builder.AddValue(MakeUnversionedInt64Value(endIndex, 1));
        upper.SetKey(builder.FinishRow());

        readRanges.emplace_back(std::move(lower), std::move(upper));
    }

    {
        TReadLimit lower;
        TReadLimit upper;
        TUnversionedOwningRowBuilder builder;

        builder.AddValue(MakeUnversionedStringValue(A, 0));
        builder.AddValue(MakeUnversionedInt64Value(HugeRowCount, 1));
        lower.SetKey(builder.FinishRow());

        builder.AddValue(MakeUnversionedStringValue(A, 0));
        builder.AddValue(MakeUnversionedInt64Value(HugeRowCount + 100, 1));
        upper.SetKey(builder.FinishRow());


        readRanges.emplace_back(std::move(lower), std::move(upper));
    }

    auto chunkReader = CreateSchemalessChunkReader(
        New<TChunkReaderConfig>(),
        MemoryReader,
        NameTable,
        GetNullBlockCache(),
        TKeyColumns(),
        MasterMeta,
        std::move(readRanges),
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
        EXPECT_TRUE(actual.size() <= std::distance(it, expected.end()));
        std::vector<TUnversionedRow> ex(it, it + actual.size());
        CheckResult(ex, actual);
        it += actual.size();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NTableClient
} // namespace NYT
