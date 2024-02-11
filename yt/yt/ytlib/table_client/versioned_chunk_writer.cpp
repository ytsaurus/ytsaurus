#include "versioned_chunk_writer.h"

#include "chunk_index_builder.h"
#include "chunk_meta_extensions.h"
#include "config.h"
#include "helpers.h"
#include "private.h"
#include "row_merger.h"
#include "versioned_block_writer.h"
#include "versioned_row_digest.h"
#include "key_filter.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/table_chunk_format/column_writer.h>
#include <yt/yt/ytlib/table_chunk_format/data_block_writer.h>
#include <yt/yt/ytlib/table_chunk_format/timestamp_writer.h>
#include <yt/yt/ytlib/table_chunk_format/slim_versioned_block_writer.h>

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/data_sink.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/encoding_chunk_writer.h>
#include <yt/yt/ytlib/chunk_client/encoding_writer.h>
#include <yt/yt/ytlib/chunk_client/multi_chunk_writer_base.h>

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/versioned_writer.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/misc/range.h>
#include <yt/yt/core/misc/random.h>

#include <util/generic/ylimits.h>

namespace NYT::NTableClient {

using namespace NTableChunkFormat;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NRpc;
using namespace NTransactionClient;
using namespace NObjectClient;
using namespace NApi;
using namespace NTableClient::NProto;
using namespace NTracing;

using NYT::TRange;
using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static constexpr i64 MinRowRangeDataWeight = 64_KB;

////////////////////////////////////////////////////////////////////////////////

class TVersionedChunkWriterBase
    : public IVersionedChunkWriter
{
public:
    TVersionedChunkWriterBase(
        TChunkWriterConfigPtr config,
        TChunkWriterOptionsPtr options,
        TTableSchemaPtr schema,
        IChunkWriterPtr chunkWriter,
        IBlockCachePtr blockCache,
        const std::optional<NChunkClient::TDataSink>& dataSink)
        : Logger(TableClientLogger.WithTag("ChunkWriterId: %v", TGuid::Create()))
        , Options_(std::move(options))
        , Config_(std::move(config))
        , Schema_(std::move(schema))
        , SamplesMemoryUsageGuard_(
            TMemoryUsageTrackerGuard::Acquire(
                Options_->MemoryTracker,
                /*size*/ 0))
        , EncodingChunkWriter_(New<TEncodingChunkWriter>(
            Config_,
            Options_,
            chunkWriter,
            std::move(blockCache),
            Logger))
        , LastKey_(TUnversionedValueRange(nullptr, nullptr))
        , MinTimestamp_(MaxTimestamp)
        , MaxTimestamp_(MinTimestamp)
        , RandomGenerator_(RandomNumber<ui64>())
        , SamplingThreshold_(static_cast<ui64>(MaxFloor<ui64>() * Config_->SampleRate))
        , SamplingRowMerger_(New<TRowBuffer>(TVersionedChunkWriterBaseTag()), Schema_)
        , ColumnarStatistics_(TColumnarStatistics::MakeEmpty(Schema_->GetColumnCount(), Options_->EnableColumnarValueStatistics))
        , RowDigestBuilder_(CreateVersionedRowDigestBuilder(Config_->VersionedRowDigest))
        , KeyFilterBuilder_(CreateXorFilterBuilder(Config_, Schema_->GetKeyColumnCount()))
        , TraceContext_(CreateTraceContextFromCurrent("ChunkWriter"))
        , FinishGuard_(TraceContext_)
    {
        if (dataSink) {
            PackBaggageForChunkWriter(
                TraceContext_,
                *dataSink,
                TExtraChunkTags{
                    .CompressionCodec = Options_->CompressionCodec,
                    .ErasureCodec = chunkWriter->GetErasureCodecId(),
                });
        }
    }

    TFuture<void> GetReadyEvent() override
    {
        return EncodingChunkWriter_->GetReadyEvent();
    }

    i64 GetRowCount() const override
    {
        return RowCount_;
    }

    bool Write(TRange<TVersionedRow> rows) override
    {
        TCurrentTraceContextGuard traceGuard(TraceContext_);

        if (rows.Empty()) {
            return EncodingChunkWriter_->IsReady();
        }

        SamplingRowMerger_.Reset();

        if (RowCount_ == 0) {
            auto firstRow = rows.Front();
            ToProto(
                BoundaryKeysExt_.mutable_min(),
                TLegacyOwningKey(firstRow.Keys()));
            EmitSample(firstRow);
        }

        if (RowDigestBuilder_) {
            for (auto row : rows) {
                RowDigestBuilder_->OnRow(row);
            }
        }

        if (KeyFilterBuilder_) {
            for (auto row : rows) {
                KeyFilterBuilder_->AddKey(row);
            }
        }

        DoWriteRows(rows);

        LastKey_ = TLegacyOwningKey(rows.Back().Keys());

        if (KeyFilterBuilder_) {
            KeyFilterBuilder_->FlushBlock(LastKey_, /*force*/ false);
        }

        return EncodingChunkWriter_->IsReady();
    }

    TFuture<void> Close() override
    {
        TCurrentTraceContextGuard traceGuard(TraceContext_);

        // psushin@ forbids empty chunks :)
        YT_VERIFY(RowCount_ > 0);

        return BIND(&TVersionedChunkWriterBase::DoClose, MakeStrong(this))
            .AsyncVia(NChunkClient::TDispatcher::Get()->GetWriterInvoker())
            .Run();
    }

    i64 GetMetaSize() const override
    {
        // Other meta parts are negligible.
        return BlockMetaExtSize_ + SamplesExtSize_;
    }

    bool IsCloseDemanded() const override
    {
        return EncodingChunkWriter_->IsCloseDemanded();
    }

    TDeferredChunkMetaPtr GetMeta() const override
    {
        return EncodingChunkWriter_->GetMeta();
    }

    TChunkId GetChunkId() const override
    {
        return EncodingChunkWriter_->GetChunkId();
    }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        return EncodingChunkWriter_->GetDataStatistics();
    }

    TCodecStatistics GetCompressionStatistics() const override
    {
        return EncodingChunkWriter_->GetCompressionStatistics();
    }

    i64 GetDataWeight() const override
    {
        return DataWeight_;
    }

protected:
    const NLogging::TLogger Logger;

    const TChunkWriterOptionsPtr Options_;
    const TChunkWriterConfigPtr Config_;
    const TTableSchemaPtr Schema_;

    TMemoryUsageTrackerGuard SamplesMemoryUsageGuard_;

    TEncodingChunkWriterPtr EncodingChunkWriter_;

    TLegacyOwningKey LastKey_;

    TDataBlockMetaExt BlockMetaExt_;
    i64 BlockMetaExtSize_ = 0;

    TColumnGroupInfosExt ColumnGroupInfosExt_;

    TSystemBlockMetaExt SystemBlockMetaExt_;

    TSamplesExt SamplesExt_;
    i64 SamplesExtSize_ = 0;

    i64 DataWeight_ = 0;

    TBoundaryKeysExt BoundaryKeysExt_;

    i64 RowCount_ = 0;

    TTimestamp MinTimestamp_;
    TTimestamp MaxTimestamp_;

    TRandomGenerator RandomGenerator_;
    const ui64 SamplingThreshold_;

    struct TVersionedChunkWriterBaseTag { };
    TSamplingRowMerger SamplingRowMerger_;

    TColumnarStatistics ColumnarStatistics_;

    IVersionedRowDigestBuilderPtr RowDigestBuilder_;
    IKeyFilterBuilderPtr KeyFilterBuilder_;

    const TTraceContextPtr TraceContext_;
    const TTraceContextFinishGuard FinishGuard_;

    virtual void DoClose() = 0;
    virtual void DoWriteRows(TRange<TVersionedRow> rows) = 0;
    virtual EChunkFormat GetChunkFormat() const = 0;

    void FillCommonMeta(TChunkMeta* meta) const
    {
        meta->set_type(ToProto<int>(EChunkType::Table));
        meta->set_format(ToProto<int>(GetChunkFormat()));

        SetProtoExtension(meta->mutable_extensions(), BoundaryKeysExt_);
    }

    virtual void PrepareChunkMeta()
    {
        ToProto(BoundaryKeysExt_.mutable_max(), LastKey_);

        const auto& meta = EncodingChunkWriter_->GetMeta();

        FillCommonMeta(meta.Get());
        SetProtoExtension(meta->mutable_extensions(), ToProto<TTableSchemaExt>(Schema_));
        SetProtoExtension(meta->mutable_extensions(), BlockMetaExt_);
        SetProtoExtension(meta->mutable_extensions(), SamplesExt_);

        if (!Options_->EnableColumnarValueStatistics) {
            // Just in case...
            ColumnarStatistics_.ClearValueStatistics();
        }
        if (!Options_->EnableRowCountInColumnarStatistics) {
            ColumnarStatistics_.ChunkRowCount.reset();
        }
        SetProtoExtension(meta->mutable_extensions(), ToProto<TColumnarStatisticsExt>(ColumnarStatistics_));
        SetProtoExtension(meta->mutable_extensions(), SystemBlockMetaExt_);
        if (RowDigestBuilder_) {
            TVersionedRowDigestExt rowDigestExt;
            ToProto(&rowDigestExt, RowDigestBuilder_->FlushDigest());
            SetProtoExtension(meta->mutable_extensions(), rowDigestExt);
        }

        meta->UpdateMemoryUsage();

        auto& miscExt = EncodingChunkWriter_->MiscExt();
        miscExt.set_sorted(true);
        miscExt.set_row_count(RowCount_);
        miscExt.set_data_weight(DataWeight_);
    }

    void EmitSampleRandomly(TVersionedRow row)
    {
        if (RandomGenerator_.Generate<ui64>() < SamplingThreshold_) {
            EmitSample(row);
        }
    }

    void EmitSample(TVersionedRow row)
    {
        auto mergedRow = SamplingRowMerger_.MergeRow(row);
        ToProto(SamplesExt_.add_entries(), mergedRow);
        i64 rowSize = SamplesExt_.entries(SamplesExt_.entries_size() - 1).length();
        SamplesExtSize_ += rowSize;
        SamplesMemoryUsageGuard_.IncrementSize(rowSize);
    }

    void MaybeWriteKeyFilterBlocks()
    {
        if (KeyFilterBuilder_) {
            KeyFilterBuilder_->FlushBlock(LastKey_, /*force*/ true);

            auto blocks = KeyFilterBuilder_->SerializeBlocks(&SystemBlockMetaExt_);
            for (auto& block : blocks) {
                EncodingChunkWriter_->WriteBlock(std::move(block), KeyFilterBuilder_->GetBlockType());
            }
        }
    }

    static void ValidateRowsOrder(TVersionedRow row, TUnversionedValueRange prevKey)
    {
        YT_VERIFY(
            !prevKey ||
            CompareValueRanges(prevKey, row.Keys()) < 0);
    }

    static void ValidateRowDataWeight(TVersionedRow row, i64 dataWeight)
    {
        if (dataWeight > MaxServerVersionedRowDataWeight) {
            THROW_ERROR_EXCEPTION("Versioned row data weight is too large")
                << TErrorAttribute("key", ToOwningKey(row))
                << TErrorAttribute("actual_data_weight", dataWeight)
                << TErrorAttribute("max_data_weight", MaxServerVersionedRowDataWeight);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSimpleBlockFormatAdapter
{
protected:
    std::unique_ptr<TSimpleVersionedBlockWriter> BlockWriter_;

    TSimpleBlockFormatAdapter(
        const TChunkWriterConfigPtr& /*config*/,
        TTableSchemaPtr schema,
        const NLogging::TLogger& /*logger*/)
        : Schema_(std::move(schema))
    { }

    void ResetBlockWriter(IMemoryUsageTrackerPtr memoryTracker)
    {
        BlockWriter_ = std::make_unique<TSimpleVersionedBlockWriter>(
            Schema_,
            TMemoryUsageTrackerGuard::Acquire(
                std::move(memoryTracker),
                /*size*/ 0));
    }

    void OnDataBlocksWritten(
        TUnversionedValueRange /*lastKey*/,
        TSystemBlockMetaExt* /*systemBlockMetaExt*/,
        const TEncodingChunkWriterPtr& /*encodingChunkWriter*/)
    { }

    EChunkFormat GetChunkFormat() const
    {
        return EChunkFormat::TableVersionedSimple;
    }

private:
    const TTableSchemaPtr Schema_;
};

class TSlimBlockFormatAdapter
{
protected:
    std::unique_ptr<TSlimVersionedBlockWriter> BlockWriter_;

    TSlimBlockFormatAdapter(
        TChunkWriterConfigPtr config,
        TTableSchemaPtr schema,
        const NLogging::TLogger& /*logger*/)
        : Config_(std::move(config))
        , Schema_(std::move(schema))
    { }

    void ResetBlockWriter(IMemoryUsageTrackerPtr memoryTracker)
    {
        BlockWriter_ = std::make_unique<TSlimVersionedBlockWriter>(
            Config_->Slim,
            Schema_,
            TMemoryUsageTrackerGuard::Acquire(
                std::move(memoryTracker),
                /*size*/ 0));
    }

    void OnDataBlocksWritten(
        TUnversionedValueRange /*lastKey*/,
        TSystemBlockMetaExt* /*systemBlockMetaExt*/,
        const TEncodingChunkWriterPtr& encodingChunkWriter)
    {
        const auto& meta = encodingChunkWriter->GetMeta();
        auto chunkFeatures = FromProto<EChunkFeatures>(meta->features());
        chunkFeatures |= EChunkFeatures::SlimBlockFormat;
        meta->set_features(ToProto<ui64>(chunkFeatures));
    }

    EChunkFormat GetChunkFormat() const
    {
        return EChunkFormat::TableVersionedSlim;
    }

private:
    const TChunkWriterConfigPtr Config_;
    const TTableSchemaPtr Schema_;
};

class TIndexedBlockFormatAdapter
{
protected:
    TIndexedBlockFormatAdapter(
        const TChunkWriterConfigPtr& config,
        TTableSchemaPtr schema,
        const NLogging::TLogger& logger)
        : Schema_(std::move(schema))
        , BlockFormatDetail_(Schema_)
        , ChunkIndexBuilder_(CreateChunkIndexBuilder(
            config->ChunkIndexes,
            BlockFormatDetail_,
            logger))
    { }

    std::unique_ptr<TIndexedVersionedBlockWriter> BlockWriter_;


    void ResetBlockWriter(IMemoryUsageTrackerPtr memoryTracker)
    {
        BlockWriter_ = std::make_unique<TIndexedVersionedBlockWriter>(
            Schema_,
            BlockCount_++,
            BlockFormatDetail_,
            ChunkIndexBuilder_,
            TMemoryUsageTrackerGuard::Acquire(
                std::move(memoryTracker),
                /*size*/ 0));
    }

    void OnDataBlocksWritten(
        TUnversionedValueRange lastKey,
        TSystemBlockMetaExt* systemBlockMetaExt,
        const TEncodingChunkWriterPtr& encodingChunkWriter)
    {
        const auto& meta = encodingChunkWriter->GetMeta();

        auto blocks = ChunkIndexBuilder_->BuildIndex(lastKey, systemBlockMetaExt);
        for (auto& block : blocks) {
            encodingChunkWriter->WriteBlock(std::move(block), ChunkIndexBuilder_->GetBlockType());
        }

        auto chunkFeatures = FromProto<EChunkFeatures>(meta->features());
        chunkFeatures |= EChunkFeatures::IndexedBlockFormat;
        meta->set_features(ToProto<ui64>(chunkFeatures));

        auto& miscExt = encodingChunkWriter->MiscExt();
        miscExt.set_block_format_version(TIndexedVersionedBlockWriter::GetBlockFormatVersion());
        miscExt.set_system_block_count(blocks.size());
    }

    EChunkFormat GetChunkFormat() const
    {
        return EChunkFormat::TableVersionedIndexed;
    }

private:
    const TTableSchemaPtr Schema_;
    const TIndexedVersionedBlockFormatDetail BlockFormatDetail_;
    const IChunkIndexBuilderPtr ChunkIndexBuilder_;

    int BlockCount_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TBlockFormatAdapter>
class TSimpleVersionedChunkWriter
    : public TVersionedChunkWriterBase
    , protected TBlockFormatAdapter
{
public:
    TSimpleVersionedChunkWriter(
        TChunkWriterConfigPtr config,
        TChunkWriterOptionsPtr options,
        TTableSchemaPtr schema,
        IChunkWriterPtr chunkWriter,
        IBlockCachePtr blockCache,
        const std::optional<NChunkClient::TDataSink>& dataSink)
        : TVersionedChunkWriterBase(
            std::move(config),
            std::move(options),
            std::move(schema),
            std::move(chunkWriter),
            std::move(blockCache),
            dataSink)
        , TBlockFormatAdapter(
            Config_,
            Schema_,
            Logger)
    {
        ResetBlockWriter(Options_->MemoryTracker);
    }

    i64 GetCompressedDataSize() const override
    {
        return
            EncodingChunkWriter_->GetDataStatistics().compressed_data_size() +
            BlockWriter_->GetBlockSize();
    }

private:
    using TBlockFormatAdapter::BlockWriter_;

    using TBlockFormatAdapter::ResetBlockWriter;
    using TBlockFormatAdapter::OnDataBlocksWritten;


    void DoWriteRows(TRange<TVersionedRow> rows) override
    {
        if (rows.Empty()) {
            return;
        }

        auto firstRow = rows.Front();

        WriteRow(firstRow, LastKey_.Elements());
        FinishBlockIfLarge(firstRow);

        int rowCount = static_cast<int>(rows.Size());
        for (int index = 1; index < rowCount; ++index) {
            WriteRow(rows[index], rows[index - 1].Keys());
            FinishBlockIfLarge(rows[index]);
        }

        ColumnarStatistics_.Update(rows);
    }

    static void ValidateRow(
        TVersionedRow row,
        i64 dataWeight,
        TUnversionedValueRange prevKey)
    {
        ValidateRowsOrder(row, prevKey);
        ValidateRowDataWeight(row, dataWeight);

        if (row.GetWriteTimestampCount() > MaxTimestampCountPerRow) {
            THROW_ERROR_EXCEPTION("Too many write timestamps in a versioned row")
                << TErrorAttribute("key", ToOwningKey(row));
        }
        if (row.GetDeleteTimestampCount() > MaxTimestampCountPerRow) {
            THROW_ERROR_EXCEPTION("Too many delete timestamps in a versioned row")
                << TErrorAttribute("key", ToOwningKey(row));
        }
    }

    void WriteRow(
        TVersionedRow row,
        TUnversionedValueRange prevKey)
    {
        EmitSampleRandomly(row);
        auto rowWeight = NTableClient::GetDataWeight(row);

        ValidateRow(row, rowWeight, prevKey);

        ++RowCount_;
        DataWeight_ += rowWeight;

        BlockWriter_->WriteRow(row);
    }

    void FinishBlockIfLarge(TVersionedRow row)
    {
        if (BlockWriter_->GetBlockSize() < Config_->BlockSize) {
            return;
        }

        FinishBlock(row.Keys());
        ResetBlockWriter(Options_->MemoryTracker);
    }

    void FinishBlock(TUnversionedValueRange keyRange)
    {
        auto block = BlockWriter_->FlushBlock();
        block.Meta.set_chunk_row_count(RowCount_);
        block.Meta.set_block_index(BlockMetaExt_.data_blocks_size());
        ToProto(block.Meta.mutable_last_key(), keyRange);

        YT_VERIFY(block.Meta.uncompressed_size() > 0);

        BlockMetaExtSize_ += block.Meta.ByteSizeLong();

        BlockMetaExt_.add_data_blocks()->Swap(&block.Meta);
        EncodingChunkWriter_->WriteBlock(std::move(block.Data), EBlockType::UncompressedData);

        MaxTimestamp_ = std::max(MaxTimestamp_, BlockWriter_->GetMaxTimestamp());
        MinTimestamp_ = std::min(MinTimestamp_, BlockWriter_->GetMinTimestamp());
    }

    void PrepareChunkMeta() override
    {
        TVersionedChunkWriterBase::PrepareChunkMeta();

        auto& miscExt = EncodingChunkWriter_->MiscExt();
        miscExt.set_min_timestamp(MinTimestamp_);
        miscExt.set_max_timestamp(MaxTimestamp_);
    }

    void DoClose() override
    {
        if (BlockWriter_->GetRowCount() > 0) {
            FinishBlock(LastKey_.Elements());
        }

        OnDataBlocksWritten(LastKey_.Elements(), &SystemBlockMetaExt_, EncodingChunkWriter_);

        MaybeWriteKeyFilterBlocks();

        PrepareChunkMeta();

        EncodingChunkWriter_->Close();
    }

    EChunkFormat GetChunkFormat() const override
    {
        return TBlockFormatAdapter::GetChunkFormat();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TColumnarVersionedChunkWriter
    : public TVersionedChunkWriterBase
{
public:
    TColumnarVersionedChunkWriter(
        TChunkWriterConfigPtr config,
        TChunkWriterOptionsPtr options,
        TTableSchemaPtr schema,
        IChunkWriterPtr chunkWriter,
        IBlockCachePtr blockCache,
        const std::optional<NChunkClient::TDataSink>& dataSink)
        : TVersionedChunkWriterBase(
            std::move(config),
            std::move(options),
            std::move(schema),
            std::move(chunkWriter),
            std::move(blockCache),
            dataSink)
        , DataToBlockFlush_(Config_->BlockSize)
    {
        auto createBlockWriter = [&] {
            int blockWriterIndex = std::ssize(BlockWriters_);
            BlockWriters_.emplace_back(std::make_unique<TDataBlockWriter>(Options_->EnableSegmentMetaInBlocks));
            return blockWriterIndex;
        };

        // 1. Timestamp and key columns are always stored in one group block.
        // 2. Store all columns (including timestamp) in one group block if
        //    all columns (key and value) have the same group in schema.
        auto mainBlockWriter = createBlockWriter();

        THashMap<TString, int> groupBlockWriters;
        auto getBlockWriterIndex = [&] (const std::optional<TString>& group) {
            if (group) {
                auto [it, inserted] = groupBlockWriters.emplace(*group, 0);
                if (inserted) {
                    it->second = createBlockWriter();
                }

                return it->second;
            } else if (Options_->SingleColumnGroupByDefault) {
                return mainBlockWriter;
            } else {
                return createBlockWriter();
            }
        };

        // Key columns.
        for (int keyColumnIndex = 0; keyColumnIndex < Schema_->GetKeyColumnCount(); ++keyColumnIndex) {
            const auto& columnSchema = Schema_->Columns()[keyColumnIndex];

            auto blockWriterIndex = getBlockWriterIndex(columnSchema.Group());

            ColumnToGroupIndex_.push_back(blockWriterIndex);
            ValueColumnWriters_.emplace_back(CreateUnversionedColumnWriter(
                keyColumnIndex,
                columnSchema,
                BlockWriters_[blockWriterIndex].get(),
                Config_->MaxSegmentValueCount));
        }

        // Value columns.
        for (
            int valueColumnIndex = Schema_->GetKeyColumnCount();
            valueColumnIndex < std::ssize(Schema_->Columns());
            ++valueColumnIndex)
        {
            const auto& columnSchema = Schema_->Columns()[valueColumnIndex];

            auto blockWriterIndex = getBlockWriterIndex(columnSchema.Group());

            ColumnToGroupIndex_.push_back(blockWriterIndex);
            ValueColumnWriters_.emplace_back(CreateVersionedColumnWriter(
                valueColumnIndex,
                columnSchema,
                BlockWriters_[blockWriterIndex].get(),
                Config_->MaxSegmentValueCount));
        }

        // Timestamp column.
        ColumnToGroupIndex_.push_back(mainBlockWriter);
        TimestampWriter_ = CreateTimestampWriter(BlockWriters_[mainBlockWriter].get());

        YT_VERIFY(BlockWriters_.size() > 0);
    }

    i64 GetCompressedDataSize() const override
    {
        i64 result = EncodingChunkWriter_->GetDataStatistics().compressed_data_size();
        for (const auto& blockWriter : BlockWriters_) {
            result += blockWriter->GetCurrentSize();
        }
        return result;
    }

    i64 GetMetaSize() const override
    {
        i64 metaSize = 0;
        for (const auto& valueColumnWriter : ValueColumnWriters_) {
            metaSize += valueColumnWriter->GetMetaSize();
        }
        metaSize += TimestampWriter_->GetMetaSize();

        return metaSize + TVersionedChunkWriterBase::GetMetaSize();
    }

private:
    std::vector<std::unique_ptr<TDataBlockWriter>> BlockWriters_;
    std::vector<std::unique_ptr<IValueColumnWriter>> ValueColumnWriters_;
    std::unique_ptr<ITimestampWriter> TimestampWriter_;
    std::vector<ui16> ColumnToGroupIndex_;

    i64 DataToBlockFlush_;

    void DoWriteRows(TRange<TVersionedRow> rows) override
    {
        int startRowIndex = 0;
        while (startRowIndex < std::ssize(rows)) {
            i64 weight = 0;
            int rowIndex = startRowIndex;
            for (; rowIndex < std::ssize(rows) && weight < DataToBlockFlush_; ++rowIndex) {
                auto row = rows[rowIndex];
                auto rowWeight = NTableClient::GetDataWeight(row);
                if (rowIndex == 0) {
                    ValidateRow(row, rowWeight, LastKey_.Elements());
                } else {
                    ValidateRow(row, rowWeight, rows[rowIndex - 1].Keys());
                }

                weight += rowWeight;
            }

            auto range = MakeRange(rows.Begin() + startRowIndex, rows.Begin() + rowIndex);
            for (const auto& columnWriter : ValueColumnWriters_) {
                columnWriter->WriteVersionedValues(range);
            }
            TimestampWriter_->WriteTimestamps(range);

            RowCount_ += range.Size();
            DataWeight_ += weight;

            startRowIndex = rowIndex;

            TryFlushBlock(rows[rowIndex - 1]);
        }

        ColumnarStatistics_.Update(rows);

        for (auto row : rows) {
            EmitSampleRandomly(row);
        }
    }

    static void ValidateRow(
        TVersionedRow row,
        i64 dataWeight,
        TUnversionedValueRange prevKey)
    {
        ValidateRowsOrder(row, prevKey);
        ValidateRowDataWeight(row, dataWeight);
    }

    void TryFlushBlock(TVersionedRow lastRow)
    {
        while (true) {
            i64 totalSize = 0;
            i64 maxWriterSize = -1;
            int maxWriterIndex = -1;

            for (int i = 0; i < std::ssize(BlockWriters_); ++i) {
                auto size = BlockWriters_[i]->GetCurrentSize();
                totalSize += size;
                if (size > maxWriterSize) {
                    maxWriterIndex = i;
                    maxWriterSize = size;
                }
            }

            YT_VERIFY(maxWriterIndex >= 0);

            if (totalSize > Config_->MaxBufferSize || maxWriterSize > Config_->BlockSize) {
                FinishBlock(maxWriterIndex, lastRow.Keys());
            } else {
                DataToBlockFlush_ = std::min(Config_->MaxBufferSize - totalSize, Config_->BlockSize - maxWriterSize);
                DataToBlockFlush_ = std::max(MinRowRangeDataWeight, DataToBlockFlush_);
                break;
            }
        }
    }

    void FinishBlock(int blockWriterIndex, TUnversionedValueRange keyRange)
    {
        auto block = BlockWriters_[blockWriterIndex]->DumpBlock(BlockMetaExt_.data_blocks_size(), RowCount_);
        YT_VERIFY(block.Meta.uncompressed_size() > 0);

        block.Meta.set_block_index(BlockMetaExt_.data_blocks_size());
        ToProto(block.Meta.mutable_last_key(), keyRange);

        BlockMetaExtSize_ += block.Meta.ByteSizeLong();

        BlockMetaExt_.add_data_blocks()->Swap(&block.Meta);

        if (Options_->EnableSegmentMetaInBlocks) {
            ColumnGroupInfosExt_.add_block_group_indexes(blockWriterIndex);

            YT_VERIFY(block.SegmentMetaOffset);
            ColumnGroupInfosExt_.add_segment_meta_offsets(*block.SegmentMetaOffset);
        }

        EncodingChunkWriter_->WriteBlock(std::move(block.Data), EBlockType::UncompressedData);
    }

    void PrepareChunkMeta() override
    {
        TVersionedChunkWriterBase::PrepareChunkMeta();

        auto& miscExt = EncodingChunkWriter_->MiscExt();
        miscExt.set_min_timestamp(TimestampWriter_->GetMinTimestamp());
        miscExt.set_max_timestamp(TimestampWriter_->GetMaxTimestamp());

        auto meta = EncodingChunkWriter_->GetMeta();

        if (Options_->EnableSegmentMetaInBlocks) {
            ToProto(ColumnGroupInfosExt_.mutable_column_to_group(), ColumnToGroupIndex_);
            SetProtoExtension(meta->mutable_extensions(), ColumnGroupInfosExt_);
        }

        NProto::TColumnMetaExt columnMetaExt;
        for (const auto& valueColumnWriter : ValueColumnWriters_) {
            *columnMetaExt.add_columns() = valueColumnWriter->ColumnMeta();
        }
        *columnMetaExt.add_columns() = TimestampWriter_->ColumnMeta();
        SetProtoExtension(meta->mutable_extensions(), columnMetaExt);
        meta->UpdateMemoryUsage();
    }

    void DoClose() override
    {
        for (int i = 0; i < std::ssize(BlockWriters_); ++i) {
            if (BlockWriters_[i]->GetCurrentSize() > 0) {
                FinishBlock(i, LastKey_.Elements());
            }
        }

        MaybeWriteKeyFilterBlocks();

        PrepareChunkMeta();

        EncodingChunkWriter_->Close();
    }

    EChunkFormat GetChunkFormat() const override
    {
        return EChunkFormat::TableVersionedColumnar;
    }
};

////////////////////////////////////////////////////////////////////////////////

IVersionedChunkWriterPtr CreateVersionedChunkWriter(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    TTableSchemaPtr schema,
    IChunkWriterPtr chunkWriter,
    const std::optional<NChunkClient::TDataSink>& dataSink,
    IBlockCachePtr blockCache)
{
    if (blockCache->GetSupportedBlockTypes() != EBlockType::None) {
        // It is hard to support both reordering and uncompressed block caching
        // since get cached significantly before we know the final permutation.
        // Supporting reordering for compressed block cache is not hard
        // to implement, but is not done for now.
        config->EnableBlockReordering = false;
    }

    auto createWriter = [&] <class TWriter> {
        return New<TWriter>(
            std::move(config),
            std::move(options),
            std::move(schema),
            std::move(chunkWriter),
            std::move(blockCache),
            dataSink);
    };

    auto chunkFormat = options->GetEffectiveChunkFormat(/*versioned*/ true);
    switch (chunkFormat) {
        case EChunkFormat::TableVersionedColumnar:
            return createWriter.operator()<TColumnarVersionedChunkWriter>();
        case EChunkFormat::TableVersionedSimple:
            return createWriter.operator()<TSimpleVersionedChunkWriter<TSimpleBlockFormatAdapter>>();
        case EChunkFormat::TableVersionedIndexed:
            if (options->CompressionCodec != NCompression::ECodec::None) {
                THROW_ERROR_EXCEPTION("Chunk index cannot be used with compression codec %Qlv",
                    options->CompressionCodec);
            }
            return createWriter.operator()<TSimpleVersionedChunkWriter<TIndexedBlockFormatAdapter>>();
        case EChunkFormat::TableVersionedSlim:
            return createWriter.operator()<TSimpleVersionedChunkWriter<TSlimBlockFormatAdapter>>();
        default:
            THROW_ERROR_EXCEPTION("Unsupported chunk format %Qlv",
                chunkFormat);
    }
}

////////////////////////////////////////////////////////////////////////////////

IVersionedMultiChunkWriterPtr CreateVersionedMultiChunkWriter(
    std::function<IVersionedChunkWriterPtr(IChunkWriterPtr)> chunkWriterFactory,
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    NNative::IClientPtr client,
    TString localHostName,
    TCellTag cellTag,
    TTransactionId transactionId,
    TMasterTableSchemaId schemaId,
    TChunkListId parentChunkListId,
    IThroughputThrottlerPtr throttler,
    IBlockCachePtr blockCache)
{
    using TVersionedMultiChunkWriter = TMultiChunkWriterBase<
        IVersionedMultiChunkWriter,
        IVersionedChunkWriter,
        TRange<TVersionedRow>
    >;

    auto writer = New<TVersionedMultiChunkWriter>(
        std::move(config),
        std::move(options),
        std::move(client),
        std::move(localHostName),
        cellTag,
        transactionId,
        schemaId,
        parentChunkListId,
        std::move(chunkWriterFactory),
        /*trafficMeter*/ nullptr,
        std::move(throttler),
        std::move(blockCache));
    writer->Init();
    return writer;
}

IVersionedMultiChunkWriterPtr CreateVersionedMultiChunkWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    TTableSchemaPtr schema,
    NNative::IClientPtr client,
    TString localHostName,
    TCellTag cellTag,
    TTransactionId transactionId,
    TMasterTableSchemaId schemaId,
    const std::optional<NChunkClient::TDataSink>& dataSink,
    TChunkListId parentChunkListId,
    IThroughputThrottlerPtr throttler,
    IBlockCachePtr blockCache)
{
    auto chunkWriterFactory = [=] (IChunkWriterPtr underlyingWriter) {
        return CreateVersionedChunkWriter(
            config,
            options,
            schema,
            std::move(underlyingWriter),
            dataSink,
            blockCache);
    };

    return CreateVersionedMultiChunkWriter(
        std::move(chunkWriterFactory),
        std::move(config),
        std::move(options),
        std::move(client),
        std::move(localHostName),
        cellTag,
        transactionId,
        schemaId,
        parentChunkListId,
        std::move(throttler),
        std::move(blockCache));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
