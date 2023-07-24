#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/yt/ytlib/chunk_client/memory_reader.h>
#include <yt/yt/ytlib/chunk_client/memory_writer.h>
#include <yt/yt/ytlib/chunk_client/meta_aggregating_writer.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>

#include <yt/yt/ytlib/table_client/cached_versioned_chunk_meta.h>
#include <yt/yt/ytlib/table_client/chunk_state.h>
#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/helpers.h>
#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/yt/client/table_client/column_sort_schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/client/table_client/unittests/helpers/helpers.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/yson/string.h>

namespace NYT::NTableClient {
namespace {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;

using NChunkClient::TChunkReaderStatistics;
using NChunkClient::NProto::TChunkSpec;

////////////////////////////////////////////////////////////////////////////////

constexpr int RowCount = 1000;

////////////////////////////////////////////////////////////////////////////////

class TMetaAggregatingWriterTest
    : public ::testing::TestWithParam<std::tuple<EOptimizeFor, TTableSchemaPtr, TLegacyReadRange, TColumnFilter>>
{
protected:
    struct TChunkInfo
    {
        TChunkSpec ChunkSpec;
        TRefCountedChunkMetaPtr ChunkMeta;
        TChunkStatePtr ChunkState;
        TMemoryWriterPtr MemoryWriter;
    };

    TRowBufferPtr RowBuffer_ = New<TRowBuffer>();
    std::vector<TUnversionedRow> Rows_;
    TMetaAggregatingWriterOptionsPtr Options_;
    TChunkWriterConfigPtr ChunkWriterConfig_;
    TMemoryWriterPtr MemoryWriter_;
    TChunkedMemoryPool Pool_;

    static constexpr int InputChunkCount = 3;

    TTableSchemaPtr Schema_;

    void SetUp() override
    {
        RowBuffer_ = New<TRowBuffer>();
        Schema_ = std::get<1>(GetParam());

        Options_ = New<TMetaAggregatingWriterOptions>();
        Options_->TableSchema = Schema_;

        ChunkWriterConfig_ = New<TChunkWriterConfig>();
        ChunkWriterConfig_->BlockSize = 256;

        PrepareChunks();
    }

    void PrepareChunks()
    {
        auto options = New<TChunkWriterOptions>();
        options->OptimizeFor = std::get<0>(GetParam());
        options->Postprocess();

        std::vector<TChunkInfo> inputChunks;
        i64 key = 0;
        auto descending = Schema_->IsSorted() && Schema_->GetSortColumns()[0].SortOrder == ESortOrder::Descending;

        for (int chunkIndex = 0; chunkIndex < InputChunkCount; ++chunkIndex) {
            auto memoryWriter = New<TMemoryWriter>();

            auto chunkWriter = CreateSchemalessChunkWriter(
                ChunkWriterConfig_,
                options,
                Schema_,
                /*nameTable*/ nullptr,
                memoryWriter,
                /*dataSink*/ std::nullopt);

            TUnversionedRowsBuilder builder;
            for (int i = 0; i < RowCount; ++i) {
                builder.AddRow(
                    key,
                    Format("%v", key),
                    TYsonString(Format("{key=%v;value=%v}", key, key + 10)));
                descending ? --key : ++key;
            }
            auto rows = builder.Build();
            chunkWriter->Write(rows);
            WaitFor(chunkWriter->Close())
                .ThrowOnError();
            for (auto row : rows) {
                Rows_.push_back(RowBuffer_->CaptureRow(row));
            }

            TChunkSpec chunkSpec;
            ToProto(chunkSpec.mutable_chunk_id(), NullChunkId);
            inputChunks.push_back({
                .ChunkSpec = chunkSpec,
                .ChunkMeta = memoryWriter->GetChunkMeta(),
                .ChunkState = New<TChunkState>(
                    GetNullBlockCache(),
                    chunkSpec),
                .MemoryWriter=memoryWriter
            });
        }

        MemoryWriter_ = New<TMemoryWriter>();
        auto writer = CreateMetaAggregatingWriter(
            MemoryWriter_,
            Options_);

        WaitFor(writer->Open())
            .ThrowOnError();

        for (const auto& chunkInfo : inputChunks) {
            auto deferredChunkMeta = New<TDeferredChunkMeta>();
            deferredChunkMeta->CopyFrom(*chunkInfo.ChunkMeta);
            writer->AbsorbMeta(deferredChunkMeta, NullChunkId);
            writer->WriteBlocks(TWorkloadDescriptor(), chunkInfo.MemoryWriter->GetBlocks());
        }

        WaitFor(writer->Close())
            .ThrowOnError();
    }

    ISchemalessChunkReaderPtr CreateReader(const TColumnFilter& columnFilter, const TReadRange& readRange)
    {
        TChunkSpec chunkSpec;
        ToProto(chunkSpec.mutable_chunk_id(), NullChunkId);
        auto chunkState = New<TChunkState>(GetNullBlockCache(), chunkSpec);
        chunkState->TableSchema = Schema_;

        auto memoryReader = CreateMemoryReader(
            MemoryWriter_->GetChunkMeta(),
            MemoryWriter_->GetBlocks());

        return CreateSchemalessRangeChunkReader(
            chunkState,
            New<TColumnarChunkMeta>(*MemoryWriter_->GetChunkMeta()),
            TChunkReaderConfig::GetDefault(),
            TChunkReaderOptions::GetDefault(),
            memoryReader,
            TNameTable::FromSchema(*Schema_),
            /*chunkReadOptions*/ {},
            /*sortColumns*/ {},
            /*omittedInaccessibleColumns*/ {},
            columnFilter,
            readRange);
    }

    void CheckColumnarStatistics()
    {
        TColumnarStatistics expectedStatistics;
        expectedStatistics.Update(Rows_);

        auto writenStatistics = NYT::FromProto<TColumnarStatistics>(
            GetProtoExtension<NProto::TColumnarStatisticsExt>(MemoryWriter_->GetChunkMeta()->Getextensions()),
            GetProtoExtension<NYT::NChunkClient::NProto::TMiscExt>(MemoryWriter_->GetChunkMeta()->Getextensions()).row_count());

        EXPECT_EQ(writenStatistics, expectedStatistics);
    }
};

TEST_P(TMetaAggregatingWriterTest, TestReadAll)
{
    auto reader = CreateReader(/*columnFilter*/ {}, /*readRange*/ {});
    CheckSchemalessResult(Rows_, reader, Schema_->GetKeyColumnCount());
    CheckColumnarStatistics();
}

TEST_P(TMetaAggregatingWriterTest, TestRange)
{
    const auto& readRange = std::get<2>(GetParam());
    const auto& columnFilter = std::get<3>(GetParam());
    auto keyColumnCount = Schema_->GetKeyColumnCount();
    auto nameTable = TNameTable::FromSchema(*Schema_);
    auto expected = CreateFilteredRangedRows(
        Rows_,
        nameTable,
        nameTable,
        columnFilter,
        readRange,
        &Pool_,
        keyColumnCount);

    auto lowerReadLimit = ReadLimitFromLegacyReadLimit(readRange.LowerLimit(), /*isUpper*/ false, keyColumnCount);
    auto upperReadLimit = ReadLimitFromLegacyReadLimit(readRange.UpperLimit(), /*isUpper*/ true, keyColumnCount);
    auto reader = CreateReader(columnFilter, TReadRange(lowerReadLimit, upperReadLimit));
    CheckSchemalessResult(expected, reader, keyColumnCount);
    CheckColumnarStatistics();
}

INSTANTIATE_TEST_SUITE_P(Test,
    TMetaAggregatingWriterTest,
    ::testing::Combine(
        ::testing::Values(
            EOptimizeFor::Scan,
            EOptimizeFor::Lookup),
        ::testing::Values(
            ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
                "<strict=%true>["
                "{name = c0; type = int64};"
                "{name = c1; type = string};"
                "{name = c2; type = any};"
            "]"))),
            ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
                "<strict=%true>["
                "{name = c0; type = int64; sort_order = ascending};"
                "{name = c1; type = string};"
                "{name = c2; type = any};"
            "]"))),
            ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
                "<strict=%true>["
                "{name = c0; type = int64; sort_order = descending};"
                "{name = c1; type = string};"
                "{name = c2; type = any};"
            "]")))),
        ::testing::Values(
            TLegacyReadRange(TLegacyReadLimit().SetRowIndex(0), TLegacyReadLimit().SetRowIndex(1)),
            TLegacyReadRange(TLegacyReadLimit().SetRowIndex(17), TLegacyReadLimit().SetRowIndex(30)),
            TLegacyReadRange(TLegacyReadLimit().SetRowIndex(RowCount - 9), TLegacyReadLimit().SetRowIndex(RowCount))),
        ::testing::Values(TColumnFilter(), TColumnFilter({0}), TColumnFilter({0, 2}))
));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkClient
