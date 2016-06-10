#include "framework.h"
#include "versioned_table_client_ut.h"

#include <yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/ytlib/chunk_client/data_statistics.h>
#include <yt/ytlib/chunk_client/memory_reader.h>
#include <yt/ytlib/chunk_client/memory_writer.h>
#include <yt/ytlib/chunk_client/chunk_spec.pb.h>

#include <yt/ytlib/table_client/config.h>
#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/ytlib/table_client/schemaless_chunk_writer.h>
#include <yt/ytlib/table_client/unversioned_row.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/compression/public.h>

namespace NYT {
namespace NTableClient {
namespace {

using namespace NChunkClient;
using namespace NTransactionClient;

using NChunkClient::NProto::TChunkSpec;
using NChunkClient::NProto::TChunkMeta;
using NChunkClient::NProto::TDataStatistics;

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

        EXPECT_EQ(0, NameTable->RegisterName("k1"));
        EXPECT_EQ(1, NameTable->RegisterName("k2"));
        EXPECT_EQ(2, NameTable->RegisterName("k3"));

        EXPECT_EQ(3, NameTable->RegisterName("v1"));
        EXPECT_EQ(4, NameTable->RegisterName("v2"));

        Schema_ = TTableSchema({
            TColumnSchema("k1", EValueType::String).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("k2", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("k3", EValueType::Boolean).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("v1", EValueType::Double),
            TColumnSchema("v2", EValueType::Double),
        });

    }

    TTableSchema Schema_;

    TNameTablePtr NameTable;

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

    void SetUpWriter(TChunkWriterOptionsPtr options)
    {
        MemoryWriter = New<TMemoryWriter>();

        auto config = New<TChunkWriterConfig>();
        config->BlockSize = 2 * 1024;

        ChunkWriter = CreateSchemalessChunkWriter(
            config,
            options,
            Schema_,
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
            auto row = TMutableUnversionedRow::Allocate(&MemoryPool, 5);
            row[0] = MakeUnversionedStringValue(A, 0);
            row[1] = MakeUnversionedInt64Value(i, 1);
            row[2] = MakeUnversionedSentinelValue(EValueType::Null, 2);

            if (i % 2 == 0) {
                row[3] = MakeUnversionedDoubleValue(3.1415, 3);
                row[4] = MakeUnversionedSentinelValue(EValueType::Null, 4);
            } else {
                row[3] = MakeUnversionedSentinelValue(EValueType::Null, 3);
                row[4] = MakeUnversionedDoubleValue(3.1415, 4);
            }

            rows.push_back(row);
        }
        return rows;
    }

    void WriteRows(int startIndex, int endIndex, EOptimizeFor optimizeFor)
    {
        auto options = New<TChunkWriterOptions>();
        options->OptimizeFor = optimizeFor;
        WriteRows(startIndex, endIndex, options);
    }

    void WriteRows(int startIndex, int endIndex, TChunkWriterOptionsPtr options)
    {
        SetUpWriter(options);
        ChunkWriter->Write(CreateManyRows(startIndex, endIndex));
        FinishWriter();
    }

    void WriteManyRows(EOptimizeFor optimizeFor = EOptimizeFor::Lookup)
    {
        WriteRows(0, HugeRowCount, optimizeFor);
    }

    void WriteFewRows(EOptimizeFor optimizeFor = EOptimizeFor::Lookup)
    {
        WriteRows(0, SmallRowCount, optimizeFor);
    }

    std::vector<TUnversionedOwningRow> LookupRows(
        TSharedRange<TKey> keys,
        const TKeyColumns& keyColumns)
    {
        std::vector<TUnversionedOwningRow> rows;

        TChunkSpec chunkSpec;
        ToProto(chunkSpec.mutable_chunk_id(), NullChunkId);
        chunkSpec.mutable_chunk_meta()->MergeFrom(MasterMeta);

        auto options = New<TChunkReaderOptions>();
        options->DynamicTable = true;

        auto chunkReader = CreateSchemalessChunkReader(
            chunkSpec,
            New<TChunkReaderConfig>(),
            options,
            MemoryReader,
            NameTable,
            GetNullBlockCache(),
            keyColumns,
            TColumnFilter(),
            keys);

        std::vector<TUnversionedRow> actual;
        actual.reserve(997);

        while (chunkReader->Read(&actual)) {
            if (actual.empty()) {
                if (!chunkReader->GetReadyEvent().Get().IsOK()) {
                    std::cout << ToString(chunkReader->GetReadyEvent().Get()) << std::endl;
                }
                EXPECT_TRUE(chunkReader->GetReadyEvent().Get().IsOK());
                continue;
            }
            for (auto row : actual) {
                rows.push_back(TUnversionedOwningRow(row));
            }
        }

        return rows;
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

        TChunkSpec chunkSpec;
        ToProto(chunkSpec.mutable_chunk_id(), NullChunkId);
        chunkSpec.mutable_chunk_meta()->MergeFrom(MasterMeta);
        chunkSpec.set_table_row_index(tableRowIndex);

        auto chunkReader = CreateSchemalessChunkReader(
            chunkSpec,
            config,
            New<TChunkReaderOptions>(),
            MemoryReader,
            NameTable,
            GetNullBlockCache(),
            TKeyColumns(),
            columnFilter,
            TReadRange());

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

    void TestReadAllUnsorted(EOptimizeFor optimizeFor)
    {
        WriteManyRows(optimizeFor);
        std::vector<TUnversionedRow> expected = CreateManyRows();

        TColumnFilter columnFilter;

        TChunkSpec chunkSpec;
        ToProto(chunkSpec.mutable_chunk_id(), NullChunkId);
        chunkSpec.mutable_chunk_meta()->MergeFrom(MasterMeta);
        chunkSpec.set_table_row_index(42);

        auto chunkReader = CreateSchemalessChunkReader(
            chunkSpec,
            New<TChunkReaderConfig>(),
            New<TChunkReaderOptions>(),
            MemoryReader,
            NameTable,
            GetNullBlockCache(),
            TKeyColumns(),
            columnFilter,
            TReadRange());

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

    void TestEmptyRead(EOptimizeFor optimizeFor)
    {
        WriteFewRows(optimizeFor);
        std::vector<TUnversionedRow> expected;

        TColumnFilter columnFilter;

        TReadLimit lowerLimit;
        lowerLimit.SetRowIndex(SmallRowCount);

        TChunkSpec chunkSpec;
        ToProto(chunkSpec.mutable_chunk_id(), NullChunkId);
        chunkSpec.mutable_chunk_meta()->MergeFrom(MasterMeta);
        chunkSpec.set_table_row_index(42);

        auto chunkReader = CreateSchemalessChunkReader(
            chunkSpec,
            New<TChunkReaderConfig>(),
            New<TChunkReaderOptions>(),
            MemoryReader,
            NameTable,
            GetNullBlockCache(),
            TKeyColumns(),
            columnFilter,
            TReadRange{lowerLimit, TReadLimit()});

        EXPECT_TRUE(chunkReader->IsFetchingCompleted());
        EXPECT_EQ(TDataStatistics(), chunkReader->GetDataStatistics());

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

    void TestReadSortedRange(EOptimizeFor optimizeFor)
    {
        WriteManyRows(optimizeFor);
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

        TChunkSpec chunkSpec;
        ToProto(chunkSpec.mutable_chunk_id(), NullChunkId);
        chunkSpec.mutable_chunk_meta()->MergeFrom(MasterMeta);
        chunkSpec.set_table_row_index(42);

        auto chunkReader = CreateSchemalessChunkReader(
            chunkSpec,
            New<TChunkReaderConfig>(),
            New<TChunkReaderOptions>(),
            MemoryReader,
            NameTable,
            GetNullBlockCache(),
            TKeyColumns(),
            columnFilter,
            TReadRange{lowerLimit, upperLimit});

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

    void TestSampledRead(EOptimizeFor optimizeFor)
    {
        WriteManyRows(optimizeFor);
        TColumnFilter columnFilter;

        auto config = New<TChunkReaderConfig>();
        config->SamplingSeed = 42;

        for (double samplingRate = 0.0; samplingRate <= 1.0; samplingRate += 0.25) {
            config->SamplingRate = samplingRate;
            double variation = samplingRate * (1 - samplingRate);

            TChunkSpec chunkSpec;
            ToProto(chunkSpec.mutable_chunk_id(), NullChunkId);
            chunkSpec.mutable_chunk_meta()->MergeFrom(MasterMeta);
            chunkSpec.set_table_row_index(42);

            auto chunkReader = CreateSchemalessChunkReader(
                chunkSpec,
                config,
                New<TChunkReaderOptions>(),
                MemoryReader,
                NameTable,
                GetNullBlockCache(),
                TKeyColumns(),
                columnFilter,
                TReadRange());

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
            double rate = readRowCount * 1.0 / HugeRowCount;
            EXPECT_GE(rate, samplingRate - variation);
            EXPECT_LE(rate, samplingRate + variation);
        }
    }

    void TestReadSystemColumns(EOptimizeFor optimizeFor)
    {
        WriteRows(0, 2, optimizeFor);

        int tableIndexId = NameTable->GetIdOrRegisterName(TableIndexColumnName);
        int rangeIndexId = NameTable->GetIdOrRegisterName(RangeIndexColumnName);
        int rowIndexId = NameTable->GetIdOrRegisterName(RowIndexColumnName);

        TUnversionedRowBuilder builder1;
        builder1.AddValue(MakeUnversionedInt64Value(1, rangeIndexId));
        builder1.AddValue(MakeUnversionedInt64Value(10, tableIndexId));
        builder1.AddValue(MakeUnversionedInt64Value(42, rowIndexId));

        TUnversionedRowBuilder builder2;
        builder2.AddValue(MakeUnversionedInt64Value(1, rangeIndexId));
        builder2.AddValue(MakeUnversionedInt64Value(10, tableIndexId));
        builder2.AddValue(MakeUnversionedInt64Value(43, rowIndexId));

        std::vector<TUnversionedRow> expected = {
            builder1.GetRow(),
            builder2.GetRow()};

        TColumnFilter columnFilter;
        columnFilter.All = false;

        TChunkSpec chunkSpec;
        ToProto(chunkSpec.mutable_chunk_id(), NullChunkId);
        chunkSpec.mutable_chunk_meta()->MergeFrom(MasterMeta);
        chunkSpec.set_table_row_index(42);
        chunkSpec.set_range_index(1);
        chunkSpec.set_table_index(10);

        auto options = New<TChunkReaderOptions>();
        options->EnableTableIndex = true;
        options->EnableRangeIndex = true;
        options->EnableRowIndex = true;

        auto chunkReader = CreateSchemalessChunkReader(
            chunkSpec,
            New<TChunkReaderConfig>(),
            options,
            MemoryReader,
            NameTable,
            GetNullBlockCache(),
            TKeyColumns(),
            columnFilter,
            TReadRange());

        EXPECT_TRUE(chunkReader->IsFetchingCompleted());

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

    void TestMultiPartSampledRead(EOptimizeFor optimizeFor)
    {
        for (double samplingRate = 0.0; samplingRate <= 1.0; samplingRate += 0.25) {
            i64 rowCount = MediumRowCount;

            WriteRows(0, rowCount, optimizeFor);
            auto expected = ReadRows(0, samplingRate);

            auto expectedIt = expected.begin();

            int partCount = 10;
            i64 partSize = (rowCount + partCount - 1) / partCount;
            for (int i = 0; i < partCount; ++i) {
                int lowerBound = i * partSize;
                int upperBound = std::min((i + 1) * partSize, rowCount);

                WriteRows(lowerBound, upperBound, optimizeFor);
                auto actual = ReadRows(lowerBound, samplingRate);

                for (auto actualRow : actual) {
                    ExpectRowsEqual(*expectedIt, actualRow);
                    ++expectedIt;
                }
            }
            YCHECK(expectedIt == expected.end());
        }
    }

    void TestLookup(EOptimizeFor optimizeFor)
    {
        auto options = New<TChunkWriterOptions>();
        options->OptimizeFor = optimizeFor;
        options->ValidateSorted = true;
        options->ValidateUniqueKeys = true;
        WriteRows(0, 50, options);
        auto expected = ReadRows();

        TUnversionedRowBuilder builder1;
        builder1.AddValue(MakeUnversionedStringValue(A, 0));
        builder1.AddValue(MakeUnversionedInt64Value(10, 1));
        builder1.AddValue(MakeUnversionedSentinelValue(EValueType::Null, 2));

        TUnversionedRowBuilder builder2;
        builder2.AddValue(MakeUnversionedStringValue(A, 0));
        builder2.AddValue(MakeUnversionedInt64Value(20, 1));
        builder2.AddValue(MakeUnversionedSentinelValue(EValueType::Null, 2));

        std::vector<TUnversionedRow> keys = {
            builder1.GetRow(),
            builder2.GetRow()};

        auto actual = LookupRows(MakeSharedRange(keys), TKeyColumns());

        ExpectRowsEqual(expected[10], actual[0]);
        ExpectRowsEqual(expected[20], actual[1]);
    }

    void TestLookupWiderKey(EOptimizeFor optimizeFor)
    {
        auto options = New<TChunkWriterOptions>();
        options->OptimizeFor = optimizeFor;
        options->ValidateSorted = true;
        options->ValidateUniqueKeys = true;
        WriteRows(0, 50, options);
        auto expected = ReadRows();

        auto keyColumns = TKeyColumns{"k1", "k2", "k3", "k4"};

        TUnversionedRowBuilder builder1;
        builder1.AddValue(MakeUnversionedStringValue(A, 0));
        builder1.AddValue(MakeUnversionedInt64Value(10, 1));
        builder1.AddValue(MakeUnversionedSentinelValue(EValueType::Null, 2));
        builder1.AddValue(MakeUnversionedSentinelValue(EValueType::Null, 3));

        TUnversionedRowBuilder builder2;
        builder2.AddValue(MakeUnversionedStringValue(A, 0));
        builder2.AddValue(MakeUnversionedInt64Value(20, 1));
        builder2.AddValue(MakeUnversionedSentinelValue(EValueType::Null, 2));
        builder2.AddValue(MakeUnversionedSentinelValue(EValueType::Null, 3));

        std::vector<TUnversionedRow> keys = {
            builder1.GetRow(),
            builder2.GetRow()};

        auto actual = LookupRows(MakeSharedRange(keys), keyColumns);

        ExpectRowsEqual(expected[10], actual[0]);
        ExpectRowsEqual(expected[20], actual[1]);
    }
};

TEST_F(TSchemalessChunksTest, ReadAllUnsortedScan)
{
    TestReadAllUnsorted(EOptimizeFor::Scan);
}

TEST_F(TSchemalessChunksTest, ReadAllUnsortedLookup)
{
    TestReadAllUnsorted(EOptimizeFor::Lookup);
}

TEST_F(TSchemalessChunksTest, EmptyReadScan)
{
    TestEmptyRead(EOptimizeFor::Scan);
}

TEST_F(TSchemalessChunksTest, EmptyReadLookup)
{
    TestEmptyRead(EOptimizeFor::Lookup);
}

TEST_F(TSchemalessChunksTest, ReadSortedRangeLookup)
{
    TestReadSortedRange(EOptimizeFor::Lookup);
}

TEST_F(TSchemalessChunksTest, ReadSortedRangeScan)
{
    TestReadSortedRange(EOptimizeFor::Scan);
}

TEST_F(TSchemalessChunksTest, SampleReadLookup)
{
    TestSampledRead(EOptimizeFor::Lookup);
}

TEST_F(TSchemalessChunksTest, SampleReadScan)
{
    TestSampledRead(EOptimizeFor::Scan);
}

TEST_F(TSchemalessChunksTest, ReadSystemColumnsScan)
{
    TestReadSystemColumns(EOptimizeFor::Scan);
}

TEST_F(TSchemalessChunksTest, ReadSystemColumnsLookup)
{
    TestReadSystemColumns(EOptimizeFor::Lookup);
}

TEST_F(TSchemalessChunksTest, MultiPartSampleReadScan)
{
    TestMultiPartSampledRead(EOptimizeFor::Scan);
}

TEST_F(TSchemalessChunksTest, MultiPartSampleReadLookup)
{
    TestMultiPartSampledRead(EOptimizeFor::Lookup);
}
TEST_F(TSchemalessChunksTest, LookupSimpleKeyLookup)
{
    TestLookup(EOptimizeFor::Lookup);
}

TEST_F(TSchemalessChunksTest, LookupWiderKeyLookup)
{
    TestLookupWiderKey(EOptimizeFor::Lookup);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NTableClient
} // namespace NYT
