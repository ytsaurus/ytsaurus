#include "schemaless_chunk_writer.h"

#include "chunk_meta_extensions.h"
#include "config.h"
#include "partitioner.h"
#include "schema.h"
#include "schemaless_block_writer.h"
#include "table_ypath_proxy.h"
#include "helpers.h"
#include "skynet_column_evaluator.h"
#include "versioned_chunk_writer.h"

#include <yt/yt/ytlib/table_chunk_format/column_writer.h>
#include <yt/yt/ytlib/table_chunk_format/schemaless_column_writer.h>
#include <yt/yt/ytlib/table_chunk_format/data_block_writer.h>

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/encoding_chunk_writer.h>
#include <yt/yt/ytlib/chunk_client/multi_chunk_writer_base.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>
#include <yt/yt/ytlib/transaction_client/transaction_listener.h>
#include <yt/yt/ytlib/transaction_client/config.h>

#include <yt/yt/ytlib/table_client/table_upload_options.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/ytlib/object_client/helpers.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/misc/io_tags.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schemaless_row_reorderer.h>
#include <yt/yt/client/table_client/check_schema_compatibility.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/api/config.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/random.h>

#include <yt/yt/core/ytree/helpers.h>

#include <util/generic/cast.h>
#include <util/generic/ylimits.h>
#include <util/generic/buffer.h>

namespace NYT::NTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NTableClient::NProto;
using namespace NTableChunkFormat;
using namespace NTabletClient;
using namespace NRpc;
using namespace NApi;
using namespace NTransactionClient;
using namespace NNodeTrackerClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NApi;
using namespace NQueryClient;
using namespace NTracing;

using NYT::ToProto;
using NYT::FromProto;
using NYT::TRange;

static const i64 PartitionRowCountThreshold = 1000 * 1000;
static const i64 PartitionRowCountLimit = std::numeric_limits<i32>::max() - PartitionRowCountThreshold;
static const i64 MinRowRangeDataWeight = 64_KB;

////////////////////////////////////////////////////////////////////////////////

TChunkTimestamps::TChunkTimestamps(TTimestamp minTimestamp, TTimestamp maxTimestamp)
    : MinTimestamp(minTimestamp)
    , MaxTimestamp(maxTimestamp)
{ }

////////////////////////////////////////////////////////////////////////////////

void ValidateRowWeight(i64 weight, const TChunkWriterConfigPtr& config, const TChunkWriterOptionsPtr& options)
{
    if (!options->ValidateRowWeight || weight < config->MaxRowWeight) {
        return;
    }

    THROW_ERROR_EXCEPTION(EErrorCode::RowWeightLimitExceeded, "Row weight is too large")
        << TErrorAttribute("row_weight", weight)
        << TErrorAttribute("row_weight_limit", config->MaxRowWeight);
}

void ValidateKeyWeight(i64 weight, const TChunkWriterConfigPtr& config, const TChunkWriterOptionsPtr& options)
{
    if (!options->ValidateKeyWeight || weight < config->MaxKeyWeight) {
        return;
    }

    THROW_ERROR_EXCEPTION(EErrorCode::RowWeightLimitExceeded, "Key weight is too large")
        << TErrorAttribute("key_weight", weight)
        << TErrorAttribute("key_weight_limit", config->MaxKeyWeight);
}

////////////////////////////////////////////////////////////////////////////////

class TUnversionedChunkWriterBase
    : public ISchemalessChunkWriter
{
public:
    TUnversionedChunkWriterBase(
        TChunkWriterConfigPtr config,
        TChunkWriterOptionsPtr options,
        IChunkWriterPtr chunkWriter,
        IBlockCachePtr blockCache,
        TTableSchemaPtr schema,
        TNameTablePtr nameTable,
        const TChunkTimestamps& chunkTimestamps,
        const std::optional<NChunkClient::TDataSink>& dataSink)
        : Logger(TableClientLogger.WithTag("ChunkWriterId: %v", TGuid::Create()))
        , Schema_(std::move(schema))
        , ChunkTimestamps_(chunkTimestamps)
        , ChunkNameTable_(nameTable ? std::move(nameTable) : TNameTable::FromSchemaStable(*Schema_))
        , Config_(std::move(config))
        , Options_(std::move(options))
        , TraceContext_(CreateTraceContextFromCurrent("ChunkWriter"))
        , FinishGuard_(TraceContext_)
        , RandomGenerator_(RandomNumber<ui64>())
        , SamplingThreshold_(static_cast<ui64>(MaxFloor<ui64>() * Config_->SampleRate))
        , ColumnarStatistics_(TColumnarStatistics::MakeEmpty(ChunkNameTable_->GetSize(), Options_->EnableColumnarValueStatistics))
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

        // NB(gepardo). TEncodingChunkWriter writes the blocks into the underlying writer in a callback.
        // We need the callback to capture the baggage, so we create EncodingChunkWriter_ here under trace
        // context, not in the initialization list above.
        TCurrentTraceContextGuard traceGuard(TraceContext_);
        EncodingChunkWriter_ = New<TEncodingChunkWriter>(
            Config_,
            Options_,
            std::move(chunkWriter),
            std::move(blockCache),
            Logger);
    }

    TFuture<void> Close() override
    {
        TCurrentTraceContextGuard traceGuard(TraceContext_);

        if (RowCount_ == 0) {
            // Empty chunk.
            return VoidFuture;
        }

        return BIND(&TUnversionedChunkWriterBase::DoClose, MakeStrong(this))
            .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
            .Run();
    }

    TFuture<void> GetReadyEvent() override
    {
        return EncodingChunkWriter_->GetReadyEvent();
    }

    i64 GetMetaSize() const override
    {
        // Other meta parts are negligible.
        return BlockMetaExtSize_ + SamplesExtSize_ + ChunkNameTable_->GetByteSize();
    }

    i64 GetCompressedDataSize() const override
    {
        return EncodingChunkWriter_->GetDataStatistics().compressed_data_size();
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

    TDataStatistics GetDataStatistics() const override
    {
        auto dataStatistics = EncodingChunkWriter_->GetDataStatistics();
        dataStatistics.set_row_count(RowCount_);
        return dataStatistics;
    }

    TCodecStatistics GetCompressionStatistics() const override
    {
        return EncodingChunkWriter_->GetCompressionStatistics();
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return ChunkNameTable_;
    }

    const TTableSchemaPtr& GetSchema() const override
    {
        return Schema_;
    }

    i64 GetDataWeight() const override
    {
        return DataWeight_;
    }

protected:
    const NLogging::TLogger Logger;

    const TTableSchemaPtr Schema_;
    const TChunkTimestamps ChunkTimestamps_;

    TNameTablePtr ChunkNameTable_;

    const TChunkWriterConfigPtr Config_;
    const TChunkWriterOptionsPtr Options_;

    i64 RowCount_ = 0;
    i64 DataWeight_ = 0;
    i64 DataWeightSinceLastBlockFlush_ = 0;

    TEncodingChunkWriterPtr EncodingChunkWriter_;
    TLegacyOwningKey LastKey_;

    NProto::TDataBlockMetaExt BlockMetaExt_;

    const TTraceContextPtr TraceContext_;
    const TTraceContextFinishGuard FinishGuard_;

    virtual EChunkFormat GetChunkFormat() const = 0;
    virtual bool SupportBoundaryKeys() const = 0;

    bool IsSorted() const
    {
        return Schema_->IsSorted() && SupportBoundaryKeys();
    }

    void RegisterBlock(TBlock& block, TUnversionedRow lastRow)
    {
        if (IsSorted()) {
            ToProto(
                block.Meta.mutable_last_key(),
                lastRow.FirstNElements(Schema_->GetKeyColumnCount()));
        }

        YT_VERIFY(block.Meta.uncompressed_size() > 0);

        block.Meta.set_block_index(BlockMetaExt_.data_blocks_size());

        BlockMetaExtSize_ += block.Meta.ByteSizeLong();
        BlockMetaExt_.add_data_blocks()->Swap(&block.Meta);

        // NB: Currently schemaless writer does not support system blocks.
        EncodingChunkWriter_->WriteBlock(
            std::move(block.Data),
            EBlockType::UncompressedData,
            block.GroupIndex);
    }

    void ProcessRowset(TRange<TUnversionedRow> rows)
    {
        if (rows.Empty()) {
            return;
        }

        EmitRandomSamples(rows);

        if (IsSorted()) {
            CaptureBoundaryKeys(rows);
        }

        ColumnarStatistics_.Update(rows);
    }

    virtual void PrepareChunkMeta()
    {
        auto& miscExt = EncodingChunkWriter_->MiscExt();
        miscExt.set_sorted(IsSorted());
        miscExt.set_unique_keys(Schema_->GetUniqueKeys());
        miscExt.set_row_count(RowCount_);
        miscExt.set_data_weight(DataWeight_);

        if (ChunkTimestamps_.MinTimestamp != NullTimestamp) {
            miscExt.set_min_timestamp(ChunkTimestamps_.MinTimestamp);
        }
        if (ChunkTimestamps_.MaxTimestamp != NullTimestamp) {
            miscExt.set_max_timestamp(ChunkTimestamps_.MaxTimestamp);
        }

        if (Options_->EnableSkynetSharing) {
            miscExt.set_shared_to_skynet(true);
        }

        auto meta = EncodingChunkWriter_->GetMeta();
        FillCommonMeta(meta.Get());

        auto nameTableExt = ToProto<TNameTableExt>(ChunkNameTable_);
        SetProtoExtension(meta->mutable_extensions(), nameTableExt);

        auto schemaExt = ToProto<TTableSchemaExt>(*Schema_);
        SetProtoExtension(meta->mutable_extensions(), schemaExt);

        meta->RegisterFinalizer([blockMetaExt = std::move(BlockMetaExt_)] (TDeferredChunkMeta* meta) mutable {
            YT_VERIFY(meta->BlockIndexMapping());
            const auto& mapping = *meta->BlockIndexMapping();
            // Note that simply mapping each block's block_index is not enough.
            // Currently, our code assumes that blocks follow in ascending order
            // of block indexes (which is quite natural assumption).
            NProto::TDataBlockMetaExt reorderedBlockMetaExt;
            reorderedBlockMetaExt.mutable_data_blocks()->Reserve(blockMetaExt.data_blocks().size());
            for (ssize_t index = 0; index < blockMetaExt.data_blocks_size(); ++index) {
                reorderedBlockMetaExt.add_data_blocks();
            }
            for (auto& block : *blockMetaExt.mutable_data_blocks()) {
                auto index = block.block_index();
                YT_VERIFY(index < std::ssize(mapping));
                auto mappedIndex = mapping[index];
                reorderedBlockMetaExt.mutable_data_blocks(mappedIndex)->Swap(&block);
                reorderedBlockMetaExt.mutable_data_blocks(mappedIndex)->set_block_index(mappedIndex);
            }
            SetProtoExtension(meta->mutable_extensions(), reorderedBlockMetaExt);
        });

        if (SamplesExtSize_ == 0 && Sample_) {
            EmitSample(Sample_);
        }
        SetProtoExtension(meta->mutable_extensions(), SamplesExt_);

        if (!Options_->EnableColumnarValueStatistics) {
            // Just in case...
            ColumnarStatistics_.ClearValueStatistics();
        }
        if (!Options_->EnableRowCountInColumnarStatistics) {
            ColumnarStatistics_.ChunkRowCount.reset();
        }
        SetProtoExtension(meta->mutable_extensions(), ToProto<TColumnarStatisticsExt>(ColumnarStatistics_));

        if (IsSorted()) {
            ToProto(BoundaryKeysExt_.mutable_max(), LastKey_);
            SetProtoExtension(meta->mutable_extensions(), BoundaryKeysExt_);
        }

        if (Options_->MaxHeavyColumns > 0) {
            auto columnCount = GetNameTable()->GetSize();
            auto heavyColumnStatisticsExt = GetHeavyColumnStatisticsExt(
                ColumnarStatistics_,
                [&] (int columnIndex) {
                    return TColumnStableName(TString{GetNameTable()->GetName(columnIndex)});
                },
                columnCount,
                Options_->MaxHeavyColumns);
            SetProtoExtension(meta->mutable_extensions(), std::move(heavyColumnStatisticsExt));
        }

        if (Schema_->IsSorted()) {
            // Sorted or partition chunks.
            TKeyColumnsExt keyColumnsExt;
            ToProto(keyColumnsExt.mutable_names(), Schema_->GetKeyColumns());
            SetProtoExtension(meta->mutable_extensions(), keyColumnsExt);
        }
    }

    virtual void DoClose()
    {
        PrepareChunkMeta();

        EncodingChunkWriter_->Close();
    }

    i64 UpdateDataWeight(TUnversionedRow row)
    {
        i64 weight = 1;
        int keyColumnCount = IsSorted() ? Schema_->GetKeyColumnCount() : 0;

        for (int index = 0; index < keyColumnCount; ++index) {
            weight += NTableClient::GetDataWeight(row[index]);
        }
        ValidateKeyWeight(weight, Config_, Options_);

        for (int index = keyColumnCount; index < static_cast<int>(row.GetCount()); ++index) {
            weight += NTableClient::GetDataWeight(row[index]);
        }
        ValidateRowWeight(weight, Config_, Options_);
        DataWeight_ += weight;
        DataWeightSinceLastBlockFlush_ += weight;

        return weight;
    }

private:
    i64 BlockMetaExtSize_ = 0;

    NProto::TBoundaryKeysExt BoundaryKeysExt_;

    TRandomGenerator RandomGenerator_;
    const ui64 SamplingThreshold_;

    TUnversionedOwningRow Sample_;
    NProto::TSamplesExt SamplesExt_;
    i64 SamplesExtSize_ = 0;

    TColumnarStatistics ColumnarStatistics_;

    void FillCommonMeta(TChunkMeta* meta) const
    {
        meta->set_type(ToProto<int>(EChunkType::Table));
        meta->set_format(ToProto<int>(GetChunkFormat()));

        {
            auto chunkFeatures = FromProto<EChunkFeatures>(meta->features());

            bool hasDescendingColumns = false;
            for (const auto& column : Schema_->Columns()) {
                if (column.SortOrder() == ESortOrder::Descending) {
                    hasDescendingColumns = true;
                }
            }

            if (hasDescendingColumns) {
                chunkFeatures |= EChunkFeatures::DescendingSortOrder;
            }

            if (Schema_->HasHunkColumns()) {
                chunkFeatures |= EChunkFeatures::UnversionedHunks;
            }

            meta->set_features(static_cast<ui64>(chunkFeatures));
        }

        if (Config_->TestingOptions->AddUnsupportedFeature) {
            meta->set_features(0xFFFF);
        }
    }

    void EmitRandomSamples(TRange<TUnversionedRow> rows)
    {
        for (auto row : rows) {
            if (RandomGenerator_.Generate<ui64>() < SamplingThreshold_) {
                EmitSample(row);
            }
        }

        if (SamplesExtSize_ == 0 && !Sample_) {
            Sample_ = TUnversionedOwningRow(rows.Front());
        }
        if (SamplesExtSize_ > 0 && Sample_) {
            Sample_ = TUnversionedOwningRow();
        }
    }

    void CaptureBoundaryKeys(TRange<TUnversionedRow> rows)
    {
        if (!BoundaryKeysExt_.has_min()) {
            auto firstRow = rows.Front();
            ToProto(
                BoundaryKeysExt_.mutable_min(),
                firstRow.FirstNElements(Schema_->GetKeyColumnCount()));
        }

        auto lastRow = rows.Back();
        LastKey_ = TLegacyOwningKey(lastRow.FirstNElements(Schema_->GetKeyColumnCount()));
    }

    void EmitSample(TUnversionedRow row)
    {
        TCompactVector<TUnversionedValue, TypicalColumnCount> sampleValues;
        int weight = 0;
        for (auto it = row.Begin(); it != row.End(); ++it) {
            sampleValues.push_back(*it);
            auto& value = sampleValues.back();
            weight += NTableClient::GetDataWeight(value);

            if (value.Type == EValueType::Any) {
                // Composite types are non-comparable, so we don't store it inside samples.
                value.Length = 0;
            } else if (value.Type == EValueType::String) {
                value.Length = std::min(static_cast<int>(value.Length), MaxSampleSize);
            }
        }

        auto entry = SerializeToString(MakeRange(sampleValues));
        SamplesExt_.add_entries(entry);
        SamplesExt_.add_weights(weight);
        SamplesExtSize_ += entry.length();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSchemalessChunkWriter
    : public TUnversionedChunkWriterBase
{
public:
    TSchemalessChunkWriter(
        TChunkWriterConfigPtr config,
        TChunkWriterOptionsPtr options,
        IChunkWriterPtr chunkWriter,
        IBlockCachePtr blockCache,
        TTableSchemaPtr schema,
        TNameTablePtr nameTable,
        const TChunkTimestamps& chunkTimestamps,
        const std::optional<NChunkClient::TDataSink>& dataSink)
        : TUnversionedChunkWriterBase(
            std::move(config),
            std::move(options),
            std::move(chunkWriter),
            std::move(blockCache),
            std::move(schema),
            std::move(nameTable),
            chunkTimestamps,
            dataSink)
        , BlockWriter_(std::make_unique<THorizontalBlockWriter>(Schema_))
    { }

    i64 GetCompressedDataSize() const override
    {
        return TUnversionedChunkWriterBase::GetCompressedDataSize() +
           (BlockWriter_ ? BlockWriter_->GetBlockSize() : 0);
    }

    bool Write(TRange<TUnversionedRow> rows) override
    {
        TCurrentTraceContextGuard traceGuard(TraceContext_);

        for (auto row : rows) {
            UpdateDataWeight(row);
            ++RowCount_;
            BlockWriter_->WriteRow(row);

            if (BlockWriter_->GetBlockSize() >= Config_->BlockSize ||
                DataWeightSinceLastBlockFlush_ > Config_->MaxDataWeightBetweenBlocks)
            {
                DataWeightSinceLastBlockFlush_ = 0;
                auto block = BlockWriter_->FlushBlock();
                block.Meta.set_chunk_row_count(RowCount_);
                RegisterBlock(block, row);
                BlockWriter_ = std::make_unique<THorizontalBlockWriter>(Schema_);
            }
        }

        ProcessRowset(rows);
        return EncodingChunkWriter_->IsReady();
    }

private:
    std::unique_ptr<THorizontalBlockWriter> BlockWriter_;

    EChunkFormat GetChunkFormat() const override
    {
        return EChunkFormat::TableUnversionedSchemalessHorizontal;
    }

    bool SupportBoundaryKeys() const override
    {
        return true;
    }

    void DoClose() override
    {
        if (BlockWriter_->GetRowCount() > 0) {
            auto block = BlockWriter_->FlushBlock();
            block.Meta.set_chunk_row_count(RowCount_);
            RegisterBlock(block, LastKey_.Get());
        }

        TUnversionedChunkWriterBase::DoClose();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TColumnUnversionedChunkWriter
    : public TUnversionedChunkWriterBase
{
public:
    TColumnUnversionedChunkWriter(
        TChunkWriterConfigPtr config,
        TChunkWriterOptionsPtr options,
        IChunkWriterPtr chunkWriter,
        IBlockCachePtr blockCache,
        TTableSchemaPtr schema,
        TNameTablePtr nameTable,
        const TChunkTimestamps& chunkTimestamps,
        const std::optional<NChunkClient::TDataSink>& dataSink)
        : TUnversionedChunkWriterBase(
            std::move(config),
            std::move(options),
            std::move(chunkWriter),
            std::move(blockCache),
            std::move(schema),
            std::move(nameTable),
            chunkTimestamps,
            dataSink)
        , DataToBlockFlush_(Config_->BlockSize)
    {
        TCurrentTraceContextGuard traceGuard(TraceContext_);

        // Only scan-optimized version for now.
        THashMap<TString, TDataBlockWriter*> groupBlockWriters;
        for (const auto& column : Schema_->Columns()) {
            if (column.Group() && groupBlockWriters.find(*column.Group()) == groupBlockWriters.end()) {
                auto blockWriter = std::make_unique<TDataBlockWriter>();
                groupBlockWriters[*column.Group()] = blockWriter.get();
                auto groupIndex = BlockWriters_.size();
                blockWriter->SetGroupIndex(groupIndex);
                BlockWriters_.emplace_back(std::move(blockWriter));
            }
        }

        auto getBlockWriter = [&] (const NTableClient::TColumnSchema& columnSchema) -> TDataBlockWriter* {
            if (columnSchema.Group()) {
                return groupBlockWriters[*columnSchema.Group()];
            } else {
                auto blockWriter = std::make_unique<TDataBlockWriter>();
                auto groupIndex = BlockWriters_.size();
                blockWriter->SetGroupIndex(groupIndex);
                BlockWriters_.emplace_back(std::move(blockWriter));
                return BlockWriters_.back().get();
            }
        };

        for (int columnIndex = 0; columnIndex < Schema_->GetColumnCount(); ++columnIndex) {
            const auto& columnSchema = Schema_->Columns()[columnIndex];
            ValueColumnWriters_.emplace_back(CreateUnversionedColumnWriter(
                columnIndex,
                columnSchema,
                getBlockWriter(columnSchema)));
        }

        if (!Schema_->GetStrict() || BlockWriters_.empty()) {
            // When we have empty strict schema, we create schemaless writer (trash writer) to fullfill the invariant
            // that at least one writer should be present.
            auto blockWriter = std::make_unique<TDataBlockWriter>();
            ValueColumnWriters_.emplace_back(CreateSchemalessColumnWriter(
                Schema_->GetColumnCount(),
                blockWriter.get()));
            BlockWriters_.emplace_back(std::move(blockWriter));
        }

        YT_VERIFY(BlockWriters_.size() > 0);
    }

    bool Write(TRange<TUnversionedRow> rows) override
    {
        TCurrentTraceContextGuard traceGuard(TraceContext_);

        int startRowIndex = 0;
        while (startRowIndex < std::ssize(rows)) {
            i64 weight = 0;
            int rowIndex = startRowIndex;
            for (; rowIndex < std::ssize(rows) && weight < DataToBlockFlush_; ++rowIndex) {
                weight += UpdateDataWeight(rows[rowIndex]);
            }

            auto range = MakeRange(rows.Begin() + startRowIndex, rows.Begin() + rowIndex);
            for (const auto& columnWriter : ValueColumnWriters_) {
                columnWriter->WriteUnversionedValues(range);
            }

            RowCount_ += range.Size();

            startRowIndex = rowIndex;

            TryFlushBlock(rows[rowIndex - 1]);
        }

        ProcessRowset(rows);

        return EncodingChunkWriter_->IsReady();
    }

    i64 GetCompressedDataSize() const override
    {
        i64 result = TUnversionedChunkWriterBase::GetCompressedDataSize();
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

        return metaSize + TUnversionedChunkWriterBase::GetMetaSize();
    }

private:
    std::vector<std::unique_ptr<TDataBlockWriter>> BlockWriters_;
    std::vector<std::unique_ptr<IValueColumnWriter>> ValueColumnWriters_;

    i64 DataToBlockFlush_;

    EChunkFormat GetChunkFormat() const override
    {
        return EChunkFormat::TableUnversionedColumnar;
    }

    bool SupportBoundaryKeys() const override
    {
        return true;
    }

    void TryFlushBlock(TUnversionedRow lastRow)
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

            if (totalSize > Config_->MaxBufferSize ||
                maxWriterSize > Config_->BlockSize ||
                DataWeightSinceLastBlockFlush_ > Config_->MaxDataWeightBetweenBlocks)
            {
                FinishBlock(maxWriterIndex, lastRow);
            } else {
                DataToBlockFlush_ = std::min(Config_->MaxBufferSize - totalSize, Config_->BlockSize - maxWriterSize);
                DataToBlockFlush_ = std::max(MinRowRangeDataWeight, DataToBlockFlush_);

                break;
            }
        }
    }

    void FinishBlock(int blockWriterIndex, TUnversionedRow lastRow)
    {
        DataWeightSinceLastBlockFlush_ = 0;
        auto block = BlockWriters_[blockWriterIndex]->DumpBlock(BlockMetaExt_.data_blocks_size(), RowCount_);
        block.Meta.set_chunk_row_count(RowCount_);
        RegisterBlock(block, lastRow);
    }

    void DoClose() override
    {
        for (int i = 0; i < std::ssize(BlockWriters_); ++i) {
            if (BlockWriters_[i]->GetCurrentSize() > 0) {
                FinishBlock(i, LastKey_.Get());
            }
        }

        TUnversionedChunkWriterBase::DoClose();
    }

    void PrepareChunkMeta() override
    {
        TUnversionedChunkWriterBase::PrepareChunkMeta();

        NProto::TColumnMetaExt columnMetaExt;
        for (const auto& valueColumnWriter : ValueColumnWriters_) {
            *columnMetaExt.add_columns() = valueColumnWriter->ColumnMeta();
        }

        auto meta = EncodingChunkWriter_->GetMeta();
        meta->RegisterFinalizer([columnMetaExt = std::move(columnMetaExt)] (NChunkClient::TDeferredChunkMeta* meta) mutable {
            YT_VERIFY(meta->BlockIndexMapping());
            const auto& mapping = *meta->BlockIndexMapping();
            for (auto& column : *columnMetaExt.mutable_columns()) {
                for (auto& segment : *column.mutable_segments()) {
                    auto blockIndex = segment.block_index();
                    YT_VERIFY(blockIndex < std::ssize(mapping));
                    segment.set_block_index(mapping[blockIndex]);
                }
            }
            SetProtoExtension(meta->mutable_extensions(), columnMetaExt);
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

ISchemalessChunkWriterPtr CreateSchemalessChunkWriter(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    TTableSchemaPtr schema,
    TNameTablePtr nameTable,
    IChunkWriterPtr chunkWriter,
    const std::optional<NChunkClient::TDataSink>& dataSink,
    const TChunkTimestamps& chunkTimestamps,
    IBlockCachePtr blockCache)
{
    auto chunkFormat = options->GetEffectiveChunkFormat(/*versioned*/ false);
    switch (chunkFormat) {
        case EChunkFormat::TableUnversionedSchemalessHorizontal:
            return New<TSchemalessChunkWriter>(
                std::move(config),
                std::move(options),
                std::move(chunkWriter),
                std::move(blockCache),
                std::move(schema),
                std::move(nameTable),
                chunkTimestamps,
                dataSink);
        case EChunkFormat::TableUnversionedColumnar:
            return New<TColumnUnversionedChunkWriter>(
                std::move(config),
                std::move(options),
                std::move(chunkWriter),
                std::move(blockCache),
                std::move(schema),
                std::move(nameTable),
                chunkTimestamps,
                dataSink);
        default:
            THROW_ERROR_EXCEPTION("Unsupported chunk format %Qlv",
                chunkFormat);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TPartitionChunkWriter
    : public TUnversionedChunkWriterBase
{
public:
    TPartitionChunkWriter(
        TChunkWriterConfigPtr config,
        TChunkWriterOptionsPtr options,
        IChunkWriterPtr chunkWriter,
        IBlockCachePtr blockCache,
        TTableSchemaPtr schema,
        TNameTablePtr nameTable,
        int partitionCount,
        const std::optional<NChunkClient::TDataSink>& dataSink)
        : TUnversionedChunkWriterBase(
            std::move(config),
            std::move(options),
            std::move(chunkWriter),
            std::move(blockCache),
            std::move(schema),
            std::move(nameTable),
            TChunkTimestamps(),
            dataSink)
    {
        PartitionsExt_.mutable_row_counts()->Resize(partitionCount, 0);
        PartitionsExt_.mutable_uncompressed_data_sizes()->Resize(partitionCount, 0);
    }

    bool WriteBlock(TBlock block)
    {
        TCurrentTraceContextGuard traceGuard(TraceContext_);

        RowCount_ += block.Meta.row_count();
        block.Meta.set_chunk_row_count(RowCount_);

        // For partition chunks we may assume that data weight is equal to uncompressed data size.
        DataWeight_ += block.Meta.uncompressed_size();

        PartitionsExt_.set_row_counts(
            block.Meta.partition_index(),
            PartitionsExt_.row_counts(block.Meta.partition_index()) + block.Meta.row_count());

        PartitionsExt_.set_uncompressed_data_sizes(
            block.Meta.partition_index(),
            PartitionsExt_.uncompressed_data_sizes(block.Meta.partition_index()) + block.Meta.uncompressed_size());

        LargestPartitionRowCount_ = std::max(PartitionsExt_.row_counts(block.Meta.partition_index()), LargestPartitionRowCount_);

        // Don't store last block keys in partition chunks.
        RegisterBlock(block, TUnversionedRow());
        return EncodingChunkWriter_->IsReady();
    }

    bool SupportBoundaryKeys() const override
    {
        return false;
    }

    bool Write(TRange<TUnversionedRow> /*rows*/) override
    {
        // This method is never called for partition chunks.
        // Blocks are formed in the multi chunk writer.
        YT_ABORT();
    }

    i64 GetCompressedDataSize() const override
    {
        // Return uncompressed data size to make smaller chunks and better balance partition data
        // between HDDs. Also returning uncompressed data makes chunk switch deterministic,
        // since compression is asynchronous.
        return EncodingChunkWriter_->GetDataStatistics().uncompressed_data_size();
    }

    bool IsCloseDemanded() const override
    {
        return LargestPartitionRowCount_ > PartitionRowCountLimit;
    }

    i64 GetMetaSize() const override
    {
        return TUnversionedChunkWriterBase::GetMetaSize() +
            // PartitionsExt.
            2 * sizeof(i64) * PartitionsExt_.row_counts_size();
    }

private:
    TPartitionsExt PartitionsExt_;
    i64 LargestPartitionRowCount_ = 0;

    EChunkFormat GetChunkFormat() const override
    {
        return EChunkFormat::TableUnversionedSchemalessHorizontal;
    }

    void PrepareChunkMeta() override
    {
        TUnversionedChunkWriterBase::PrepareChunkMeta();

        YT_LOG_DEBUG("Partition totals: %v", PartitionsExt_.ShortDebugString());

        auto meta = EncodingChunkWriter_->GetMeta();
        SetProtoExtension(meta->mutable_extensions(), PartitionsExt_);
    }
};

DECLARE_REFCOUNTED_CLASS(TPartitionChunkWriter)
DEFINE_REFCOUNTED_TYPE(TPartitionChunkWriter)

////////////////////////////////////////////////////////////////////////////////

struct TSchemalessChunkWriterTag {};

class TSchemalessMultiChunkWriterBase
    : public TNontemplateMultiChunkWriterBase
    , public ISchemalessMultiChunkWriter
{
public:
    TSchemalessMultiChunkWriterBase(
        TTableWriterConfigPtr config,
        TTableWriterOptionsPtr options,
        NNative::IClientPtr client,
        TString localHostName,
        TCellTag cellTag,
        TTransactionId transactionId,
        TMasterTableSchemaId schemaId,
        TChunkListId parentChunkListId,
        TNameTablePtr nameTable,
        TTableSchemaPtr schema,
        TLegacyOwningKey lastKey,
        TTrafficMeterPtr trafficMeter,
        IThroughputThrottlerPtr throttler,
        IBlockCachePtr blockCache)
        : TNontemplateMultiChunkWriterBase(
            config,
            options,
            client,
            std::move(localHostName),
            cellTag,
            transactionId,
            schemaId,
            parentChunkListId,
            trafficMeter,
            throttler,
            blockCache)
        , Config_(std::move(config))
        , Options_(std::move(options))
        , NameTable_(std::move(nameTable))
        , Schema_(std::move(schema))
        , LastKeyHolder_(std::move(lastKey))
    {
        if (Options_->EvaluateComputedColumns) {
            ColumnEvaluator_ = Client_->GetNativeConnection()->GetColumnEvaluatorCache()->Find(Schema_);
        }

        if (Options_->EnableSkynetSharing) {
            SkynetColumnEvaluator_ = New<TSkynetColumnEvaluator>(*Schema_);
        }

        if (LastKeyHolder_) {
            auto lastKeyHolderFixed = LegacyKeyToKeyFriendlyOwningRow(LastKeyHolder_, Options_->TableSchema->GetKeyColumnCount());
            if (LastKeyHolder_ != lastKeyHolderFixed) {
                YT_LOG_DEBUG("Table last key fixed (LastKey: %v -> %v)", LastKeyHolder_, lastKeyHolderFixed);
                LastKeyHolder_ = lastKeyHolderFixed;
            }
            LastKey_ = TKey::FromRow(LastKeyHolder_);
            YT_LOG_DEBUG("Writer is in sorted append mode (LastKey: %v)", LastKey_);
        }

        if (Options_->TableSchema) {
            ValidateKeyColumnCount(
                Options_->TableSchema->GetKeyColumnCount(),
                Schema_->GetKeyColumnCount(),
                Options_->TableSchema->IsUniqueKeys());
        }
    }

    TFuture<void> GetReadyEvent() override
    {
        if (Error_.IsOK()) {
            return TNontemplateMultiChunkWriterBase::GetReadyEvent();
        } else {
            return MakeFuture(Error_);
        }
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    const TTableSchemaPtr& GetSchema() const override
    {
        return Schema_;
    }

protected:
    const TTableWriterConfigPtr Config_;
    const TTableWriterOptionsPtr Options_;
    const TNameTablePtr NameTable_;
    const TTableSchemaPtr Schema_;

    TError Error_;

    virtual TNameTablePtr GetChunkNameTable() = 0;

    void ResetIdMapping()
    {
        IdMapping_.clear();
    }

    std::vector<TUnversionedRow> ReorderAndValidateRows(TRange<TUnversionedRow> rows)
    {
        RowBuffer_->Clear();

        std::vector<TUnversionedRow> result;
        result.reserve(rows.Size());

        for (size_t rowIndex = 0; rowIndex < rows.Size(); ++rowIndex) {
            auto row = rows[rowIndex];

            if (!row) {
                THROW_ERROR_EXCEPTION("Unexpected null row")
                    << TErrorAttribute("row_index", rowIndex);
            }

            ValidateDuplicateIds(row);

            int columnCount = Schema_->GetColumnCount();
            int maxColumnCount = columnCount + (Schema_->GetStrict() ? 0 : row.GetCount());
            auto mutableRow = TMutableUnversionedRow::Allocate(RowBuffer_->GetPool(), maxColumnCount);

            for (int i = 0; i < columnCount; ++i) {
                // Id for schema columns in chunk name table always coincide with column index in schema.
                mutableRow[i] = MakeUnversionedSentinelValue(EValueType::Null, i);
            }

            for (const auto* valueIt = row.Begin(); valueIt != row.End(); ++valueIt) {
                if (IdMapping_.size() <= valueIt->Id) {
                    IdMapping_.resize(valueIt->Id + 1, -1);
                }

                if (IdMapping_[valueIt->Id] == -1) {
                    const auto& name = NameTable_->GetNameOrThrow(valueIt->Id);
                    auto stableName = Schema_->GetNameMapping().NameToStableName(name);
                    IdMapping_[valueIt->Id] = GetChunkNameTable()->GetIdOrRegisterName(stableName.Underlying());
                }

                int id = IdMapping_[valueIt->Id];
                if (id < std::ssize(Schema_->Columns())) {
                    // Validate schema column types.
                    mutableRow[id] = *valueIt;
                    mutableRow[id].Id = id;
                } else {
                    // Validate non-schema columns for
                    if (Schema_->GetStrict()) {
                        THROW_ERROR_EXCEPTION(
                            EErrorCode::SchemaViolation,
                            "Unknown column %Qv in strict schema",
                            NameTable_->GetName(valueIt->Id));
                    }

                    mutableRow[columnCount] = *valueIt;
                    mutableRow[columnCount].Id = id;
                    ++columnCount;
                }
            }

            if (Options_->CastAnyToComposite) {
                for (int i = 0; i < std::ssize(Schema_->Columns()); ++i) {
                    const auto& column = Schema_->Columns()[i];
                    if (IsV3Composite(column.LogicalType()) && mutableRow[i].Type == EValueType::Any) {
                        mutableRow[i].Type = EValueType::Composite;
                    }
                }
            }

            // Now mutableRow contains all values that schema knows about.
            // And we can check types and check that all required fields are set.
            for (int i = 0; i < std::ssize(Schema_->Columns()); ++i) {
                const auto& column = Schema_->Columns()[i];
                ValidateValueType(
                    mutableRow[i],
                    column,
                    /*typeAnyAcceptsAllValues*/ true,
                    /*isRequired*/ false,
                    Options_->ValidateAnyIsValidYson);
            }

            ValidateColumnCount(columnCount);
            mutableRow.SetCount(columnCount);

            EvaluateComputedColumns(mutableRow);
            EvaluateSkynetColumns(mutableRow, rowIndex + 1 == rows.Size());

            result.push_back(mutableRow);
        }

        ValidateSortOrderAndUniqueness(result);
        return result;
    }

private:
    TUnversionedOwningRowBuilder KeyBuilder_;
    TUnversionedOwningRow LastKeyHolder_;
    std::optional<TKey> LastKey_;

    bool IsFirstRow_ = true;

    TRowBufferPtr RowBuffer_ = New<TRowBuffer>(TSchemalessChunkWriterTag());

    TColumnEvaluatorPtr ColumnEvaluator_;
    TSkynetColumnEvaluatorPtr SkynetColumnEvaluator_;

    // Maps global name table indexes into chunk name table indexes.
    std::vector<int> IdMapping_;

    // For duplicate id validation.
    TCompactVector<i64, TypicalColumnCount> IdValidationMarks_;
    i64 CurrentIdValidationMark_ = 1;

    void EvaluateComputedColumns(TMutableUnversionedRow row)
    {
        if (ColumnEvaluator_) {
            ColumnEvaluator_->EvaluateKeys(row, RowBuffer_);
        }
    }

    void EvaluateSkynetColumns(TMutableUnversionedRow row, bool isLastRow)
    {
        if (SkynetColumnEvaluator_) {
            SkynetColumnEvaluator_->ValidateAndComputeHashes(row, RowBuffer_, isLastRow);
        }
    }

    void ValidateColumnCount(int columnCount)
    {
        if (!Options_->ValidateColumnCount || columnCount < MaxColumnId) {
            return;
        }

        THROW_ERROR_EXCEPTION("Too many columns in row")
            << TErrorAttribute("column_count", columnCount)
            << TErrorAttribute("max_column_count", MaxColumnId);
    }

    void ValidateDuplicateIds(TUnversionedRow row)
    {
        if (!Options_->ValidateDuplicateIds) {
            return;
        }

        auto mark = CurrentIdValidationMark_++;
        for (const auto* value = row.Begin(); value != row.End(); ++value) {
            auto id = value->Id;
            if (id >= IdValidationMarks_.size()) {
                IdValidationMarks_.resize(std::max(IdValidationMarks_.size() * 2, static_cast<size_t>(id) + 1));
            }
            auto& idMark = IdValidationMarks_[id];
            if (idMark == mark) {
                auto name = NameTable_->GetNameOrThrow(id);
                THROW_ERROR_EXCEPTION("Duplicate %Qv column in unversioned row", name)
                    << TErrorAttribute("id", id);
            }
            idMark = mark;
        }
    }

    void ValidateSortOrderAndUniqueness(const std::vector<TUnversionedRow>& rows)
    {
        if (!Options_->ValidateSorted || rows.empty()) {
            return;
        }

        // Table schema and chunk schema might differ, but table schema is never stricter
        // than chunk schema. Rows order and uniqueness inside chunk is validated according to
        // chunk schema however last row of some chunk and first row of next chunk are validated
        // according to table schema.
        if (LastKey_ && IsFirstRow_) {
            auto tableSchema = Options_->TableSchema;
            // If table schema is not defined explicitly, chunk schema is used instead.
            if (!tableSchema) {
                tableSchema = Schema_;
            }

            if (tableSchema->IsSorted()) {
                auto firstKey = TKey::FromRow(rows.front(), tableSchema->GetKeyColumnCount());
                ValidateSortOrderAndUniqueness(
                    *LastKey_,
                    firstKey,
                    tableSchema->ToComparator(),
                    tableSchema->IsUniqueKeys());
            }
        } else if (LastKey_) {
            YT_VERIFY(!IsFirstRow_);
            if (Schema_->IsSorted()) {
                auto firstKey = TKey::FromRow(rows.front(), Schema_->GetKeyColumnCount());
                ValidateSortOrderAndUniqueness(
                    *LastKey_,
                    firstKey,
                    Schema_->ToComparator(),
                    Schema_->IsUniqueKeys());
            }
        }

        if (Schema_->IsSorted()) {
            for (int rowIndex = 0; rowIndex + 1 < std::ssize(rows); ++rowIndex) {
                auto currentKey = TKey::FromRow(rows[rowIndex], Schema_->GetKeyColumnCount());
                auto nextKey = TKey::FromRow(rows[rowIndex + 1], Schema_->GetKeyColumnCount());
                ValidateSortOrderAndUniqueness(
                    currentKey,
                    nextKey,
                    Schema_->ToComparator(),
                    Schema_->IsUniqueKeys());
            }

            const auto& lastKey = rows.back();
            for (int keyColumnIndex = 0; keyColumnIndex < Schema_->GetKeyColumnCount(); ++keyColumnIndex) {
                KeyBuilder_.AddValue(lastKey[keyColumnIndex]);
            }
            LastKeyHolder_ = KeyBuilder_.FinishRow();
            LastKey_ = TKey::FromRow(LastKeyHolder_);
        }

        IsFirstRow_ = false;
    }

    void ValidateSortOrderAndUniqueness(
        TKey currentKey,
        TKey nextKey,
        const TComparator& comparator,
        bool checkKeysUniqueness)
    {
        int comparisonResult = comparator.CompareKeys(currentKey, nextKey);

        if (comparisonResult < 0) {
            return;
        }

        checkKeysUniqueness &= Options_->ValidateUniqueKeys;
        if (comparisonResult == 0 && !checkKeysUniqueness) {
            return;
        }

        TError error;
        if (comparisonResult == 0) {
            YT_VERIFY(checkKeysUniqueness);
            error = TError(
                EErrorCode::UniqueKeyViolation,
                "Duplicate key %v",
                currentKey);
        } else {
            error = TError(
                EErrorCode::SortOrderViolation,
                "Sort order violation: %v > %v",
                currentKey,
                nextKey)
                << TErrorAttribute("comparator", comparator);
            if (Options_->ExplodeOnValidationError) {
                YT_LOG_FATAL(error);
            }
        }

        THROW_ERROR_EXCEPTION(error);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TPartitionMultiChunkWriter
    : public TSchemalessMultiChunkWriterBase
{
public:
    TPartitionMultiChunkWriter(
        TTableWriterConfigPtr config,
        TTableWriterOptionsPtr options,
        NNative::IClientPtr client,
        TString localHostName,
        TCellTag cellTag,
        TTransactionId transactionId,
        TMasterTableSchemaId schemaId,
        TChunkListId parentChunkListId,
        TNameTablePtr nameTable,
        TTableSchemaPtr schema,
        IPartitionerPtr partitioner,
        TTrafficMeterPtr trafficMeter,
        IThroughputThrottlerPtr throttler,
        IBlockCachePtr blockCache,
        const std::optional<NChunkClient::TDataSink>& dataSink)
        : TSchemalessMultiChunkWriterBase(
            config,
            options,
            client,
            std::move(localHostName),
            cellTag,
            transactionId,
            schemaId,
            parentChunkListId,
            nameTable,
            schema,
            TLegacyOwningKey(),
            trafficMeter,
            throttler,
            blockCache)
        , Partitioner_(partitioner)
        , BlockReserveSize_(std::max(Config_->MaxBufferSize / Partitioner_->GetPartitionCount() / 2, i64(1)))
    {
        Logger.AddTag("PartitionMultiChunkWriterId: %v", TGuid::Create());

        int partitionCount = Partitioner_->GetPartitionCount();
        BlockWriters_.reserve(partitionCount);

        for (int partitionIndex = 0; partitionIndex < partitionCount; ++partitionIndex) {
            BlockWriters_.emplace_back(new THorizontalBlockWriter(Schema_, BlockReserveSize_));
            CurrentBufferCapacity_ += BlockWriters_.back()->GetCapacity();
        }

        ChunkWriterFactory_ = [=, this] (IChunkWriterPtr underlyingWriter) {
            return New<TPartitionChunkWriter>(
                config,
                options,
                std::move(underlyingWriter),
                blockCache,
                Schema_,
                /*nameTable*/ nullptr,
                partitionCount,
                dataSink);
        };
    }

    bool Write(TRange<TUnversionedRow> rows) override
    {
        YT_VERIFY(!SwitchingSession_);

        if (!Error_.IsOK()) {
            return false;
        }

        try {
            auto reorderedRows = ReorderAndValidateRows(rows);

            for (auto row : reorderedRows) {
                WriteRow(row);
            }

            // Return true if current writer is ready for more data and
            // we didn't switch to the next chunk.
            bool readyForMore = DumpLargeBlocks();
            bool switched = TrySwitchSession();
            return readyForMore && !switched;
        } catch (const std::exception& ex) {
            Error_ = TError(ex);
            YT_LOG_WARNING(Error_, "Partition multi chunk writer failed");
            return false;
        }
    }

    TFuture<void> Close() override
    {
        return BIND(&TPartitionMultiChunkWriter::DoClose, MakeStrong(this))
            .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
            .Run();
    }

private:
    const IPartitionerPtr Partitioner_;
    const i64 BlockReserveSize_;

    std::function<TPartitionChunkWriterPtr(IChunkWriterPtr)> ChunkWriterFactory_;

    THashSet<int> LargePartitions_;
    std::vector<std::unique_ptr<THorizontalBlockWriter>> BlockWriters_;

    TNameTablePtr ChunkNameTable_;

    i64 CurrentBufferCapacity_ = 0;

    TPartitionChunkWriterPtr CurrentWriter_;

    TError Error_;

    IChunkWriterBasePtr CreateTemplateWriter(IChunkWriterPtr underlyingWriter) override
    {
        CurrentWriter_ = ChunkWriterFactory_(std::move(underlyingWriter));
        // Since we form blocks outside chunk writer, we must synchronize name tables between different chunks.
        if (ChunkNameTable_) {
            for (int id = 0; id < ChunkNameTable_->GetSize(); ++id) {
                YT_VERIFY(CurrentWriter_->GetNameTable()->GetIdOrRegisterName(ChunkNameTable_->GetName(id)) == id);
            }
        }
        ChunkNameTable_ = CurrentWriter_->GetNameTable();
        return CurrentWriter_;
    }

    TNameTablePtr GetChunkNameTable() override
    {
        return ChunkNameTable_;
    }

    void DoClose()
    {
        if (!Error_.IsOK()) {
            THROW_ERROR Error_;
        }

        for (int partitionIndex = 0; partitionIndex < Partitioner_->GetPartitionCount(); ++partitionIndex) {
            auto& blockWriter = BlockWriters_[partitionIndex];
            if (blockWriter->GetRowCount() > 0) {
                bool readyForMore = FlushBlock(partitionIndex);
                bool switched = TrySwitchSession();

                if (!readyForMore || switched) {
                    WaitFor(GetReadyEvent())
                        .ThrowOnError();
                }
            }
        }

        WaitFor(TNontemplateMultiChunkWriterBase::Close())
            .ThrowOnError();
    }

    void WriteRow(TUnversionedRow row)
    {
        i64 weight = NTableClient::GetDataWeight(row);
        ValidateRowWeight(weight, Config_, Options_);

        auto partitionIndex = Partitioner_->GetPartitionIndex(row);
        auto& blockWriter = BlockWriters_[partitionIndex];

        CurrentBufferCapacity_ -= blockWriter->GetCapacity();

        blockWriter->WriteRow(row);

        CurrentBufferCapacity_ += blockWriter->GetCapacity();

        if (blockWriter->GetRowCount() >= PartitionRowCountThreshold ||
            blockWriter->GetBlockSize() > Config_->BlockSize)
        {
            LargePartitions_.insert(partitionIndex);
        }
    }

    bool DumpLargeBlocks()
    {
        bool readyForMore = true;
        for (auto partitionIndex : LargePartitions_) {
            readyForMore = FlushBlock(partitionIndex);
        }
        LargePartitions_.clear();

        while (CurrentBufferCapacity_ > Config_->MaxBufferSize) {
            i64 largestPartitionSize = -1;
            int largestPartitionIndex = -1;
            for (int partitionIndex = 0; partitionIndex < std::ssize(BlockWriters_); ++partitionIndex) {
                auto& blockWriter = BlockWriters_[partitionIndex];
                if (blockWriter->GetBlockSize() > largestPartitionSize) {
                    largestPartitionSize = blockWriter->GetBlockSize();
                    largestPartitionIndex = partitionIndex;
                }
            }

            readyForMore = FlushBlock(largestPartitionIndex);
        }

        return readyForMore;
    }

    bool FlushBlock(int partitionIndex)
    {
        auto& blockWriter = BlockWriters_[partitionIndex];
        CurrentBufferCapacity_ -= blockWriter->GetCapacity();

        auto block = blockWriter->FlushBlock();
        block.Meta.set_partition_index(partitionIndex);
        blockWriter.reset(new THorizontalBlockWriter(Schema_, BlockReserveSize_));
        CurrentBufferCapacity_ += blockWriter->GetCapacity();

        YT_LOG_DEBUG("Flushing partition block (PartitionIndex: %v, BlockSize: %v, BlockRowCount: %v, CurrentBufferCapacity: %v)",
            partitionIndex,
            block.Meta.uncompressed_size(),
            block.Meta.row_count(),
            CurrentBufferCapacity_);

        return CurrentWriter_->WriteBlock(std::move(block));
    }
};

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkWriterPtr CreatePartitionMultiChunkWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    TNameTablePtr nameTable,
    TTableSchemaPtr schema,
    NNative::IClientPtr client,
    TString localHostName,
    TCellTag cellTag,
    TTransactionId transactionId,
    TMasterTableSchemaId schemaId,
    TChunkListId parentChunkListId,
    IPartitionerPtr partitioner,
    const std::optional<NChunkClient::TDataSink>& dataSink,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr throttler,
    IBlockCachePtr blockCache)
{
    auto writer = New<TPartitionMultiChunkWriter>(
        std::move(config),
        std::move(options),
        std::move(client),
        std::move(localHostName),
        cellTag,
        transactionId,
        schemaId,
        parentChunkListId,
        std::move(nameTable),
        std::move(schema),
        std::move(partitioner),
        std::move(trafficMeter),
        std::move(throttler),
        std::move(blockCache),
        dataSink);

    writer->Init();

    return writer;
}

////////////////////////////////////////////////////////////////////////////////

class TSchemalessMultiChunkWriter
    : public TSchemalessMultiChunkWriterBase
{
public:
    TSchemalessMultiChunkWriter(
        TTableWriterConfigPtr config,
        TTableWriterOptionsPtr options,
        NNative::IClientPtr client,
        TString localHostName,
        TCellTag cellTag,
        TTransactionId transactionId,
        TMasterTableSchemaId schemaId,
        TChunkListId parentChunkListId,
        std::function<ISchemalessChunkWriterPtr(IChunkWriterPtr)> createChunkWriter,
        TNameTablePtr nameTable,
        TTableSchemaPtr schema,
        TLegacyOwningKey lastKey,
        TTrafficMeterPtr trafficMeter,
        IThroughputThrottlerPtr throttler,
        IBlockCachePtr blockCache)
        : TSchemalessMultiChunkWriterBase(
            config,
            options,
            client,
            std::move(localHostName),
            cellTag,
            transactionId,
            schemaId,
            parentChunkListId,
            nameTable,
            schema,
            lastKey,
            trafficMeter,
            throttler,
            blockCache)
        , CreateChunkWriter_(createChunkWriter)
    { }

    bool Write(TRange<TUnversionedRow> rows) override
    {
        YT_VERIFY(!SwitchingSession_);

        try {
            auto reorderedRows = ReorderAndValidateRows(rows);

            // Return true if current writer is ready for more data and
            // we didn't switch to the next chunk.
            bool readyForMore = CurrentWriter_->Write(reorderedRows);
            bool switched = TrySwitchSession();
            return readyForMore && !switched;
        } catch (const std::exception& ex) {
            Error_ = TError(ex);
            return false;
        }
    }

private:
    const std::function<ISchemalessChunkWriterPtr(IChunkWriterPtr)> CreateChunkWriter_;

    ISchemalessChunkWriterPtr CurrentWriter_;

    TNameTablePtr GetChunkNameTable() override
    {
        return CurrentWriter_->GetNameTable();
    }

    IChunkWriterBasePtr CreateTemplateWriter(IChunkWriterPtr underlyingWriter) override
    {
        CurrentWriter_ = CreateChunkWriter_(underlyingWriter);
        ResetIdMapping();
        return CurrentWriter_;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TUnversionedUpdateMultiChunkWriterTag {};

class TUnversionedUpdateMultiChunkWriter
    : public TSchemalessMultiChunkWriterBase
{
public:
    TUnversionedUpdateMultiChunkWriter(
        TTableWriterConfigPtr config,
        TTableWriterOptionsPtr options,
        NNative::IClientPtr client,
        TString localHostName,
        TCellTag cellTag,
        TTransactionId transactionId,
        TMasterTableSchemaId schemaId,
        TChunkListId parentChunkListId,
        std::function<IVersionedChunkWriterPtr(IChunkWriterPtr)> createChunkWriter,
        TNameTablePtr nameTable,
        TTableSchemaPtr schema,
        TLegacyOwningKey lastKey,
        TTrafficMeterPtr trafficMeter,
        IThroughputThrottlerPtr throttler,
        IBlockCachePtr blockCache)
        : TSchemalessMultiChunkWriterBase(
            config,
            options,
            client,
            std::move(localHostName),
            cellTag,
            transactionId,
            schemaId,
            parentChunkListId,
            nameTable,
            schema->ToUnversionedUpdate(),
            lastKey,
            trafficMeter,
            throttler,
            blockCache)
        , CreateChunkWriter_(std::move(createChunkWriter))
        , OriginalSchema_(std::move(schema))
        , ChunkNameTable_(TNameTable::FromSchemaStable(*Schema_))
    { }

    bool Write(TRange<TUnversionedRow> rows) override
    {
        YT_VERIFY(!SwitchingSession_);

        try {
            auto reorderedRows = ReorderAndValidateRows(rows);

            RowBuffer_->Clear();
            std::vector<TVersionedRow> versionedRows;
            versionedRows.reserve(reorderedRows.size());
            for (const auto& row : reorderedRows) {
                versionedRows.push_back(MakeVersionedRow(row));
            }

            // Return true if current writer is ready for more data and
            // we didn't switch to the next chunk.
            bool readyForMore = CurrentWriter_->Write(versionedRows);
            bool switched = TrySwitchSession();
            return readyForMore && !switched;
        } catch (const std::exception& ex) {
            Error_ = TError(ex);
            return false;
        }
    }

private:
    const std::function<IVersionedChunkWriterPtr(IChunkWriterPtr)> CreateChunkWriter_;
    TTableSchemaPtr OriginalSchema_;
    TRowBufferPtr RowBuffer_ = New<TRowBuffer>(TUnversionedUpdateMultiChunkWriterTag());
    TNameTablePtr ChunkNameTable_;

    IVersionedChunkWriterPtr CurrentWriter_;

    TNameTablePtr GetChunkNameTable() override
    {
        return ChunkNameTable_;
    }

    IChunkWriterBasePtr CreateTemplateWriter(IChunkWriterPtr underlyingWriter) override
    {
        CurrentWriter_ = CreateChunkWriter_(underlyingWriter);
        ResetIdMapping();
        return CurrentWriter_;
    }

    EUnversionedUpdateDataFlags FlagsFromValue(TUnversionedValue value) const
    {
        return value.Type == EValueType::Null
            ? EUnversionedUpdateDataFlags{}
            : FromUnversionedValue<EUnversionedUpdateDataFlags>(value);
    }

    int ToOriginalId(int id, int keyColumnCount) const
    {
        return (id - keyColumnCount - 1) / 2 + keyColumnCount;
    }

    void ValidateModificationType(ERowModificationType modificationType) const
    {
        if (modificationType != ERowModificationType::Write && modificationType != ERowModificationType::Delete) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::SchemaViolation,
                "Unknown modification type with raw value %v",
                ToUnderlying(modificationType));
        }
    }

    void ValidateFlags(TUnversionedValue flags, const NTableClient::TColumnSchema& columnSchema) const
    {
        if (flags.Type == EValueType::Null) {
            return;
        }
        YT_ASSERT(flags.Type == EValueType::Uint64);
        if (flags.Data.Uint64 > ToUnderlying(MaxValidUnversionedUpdateDataFlags)) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::SchemaViolation,
                "Flags column %v has value %v which exceeds its maximum value %v",
                columnSchema.GetDiagnosticNameString(),
                flags.Data.Uint64,
                ToUnderlying(MaxValidUnversionedUpdateDataFlags));
        }
    }

    void ValidateValueColumns(TUnversionedRow row, int keyColumnCount, bool isKey) const
    {
        if (isKey) {
            for (int index = keyColumnCount + 1; index < static_cast<int>(row.GetCount()); ++index) {
                if (row[index].Type != EValueType::Null) {
                    THROW_ERROR_EXCEPTION(
                        EErrorCode::SchemaViolation,
                        "Column %v must be %Qlv when modification type is \"delete\"",
                        Schema_->Columns()[index].GetDiagnosticNameString(),
                        EValueType::Null);
                }
            }
        } else {
            for (int index = keyColumnCount + 1; index < static_cast<int>(row.GetCount()); index += 2) {
                // NB. All validation is done in ReorderAndValidateRows so here we safely
                // assume these conditions to be true.
                YT_ASSERT(row[index].Id == index);
                YT_ASSERT(index + 1 < static_cast<int>(row.GetCount()));
                YT_ASSERT(row[index + 1].Id == row[index].Id + 1);

                const auto& value = row[index];
                const auto& flags = row[index + 1];
                int originalId = ToOriginalId(value.Id, keyColumnCount);

                ValidateFlags(flags, Schema_->Columns()[index + 1]);

                bool isMissing = Any(FlagsFromValue(flags) & EUnversionedUpdateDataFlags::Missing);

                const auto& columnSchema = OriginalSchema_->Columns()[originalId];
                if (columnSchema.Required()) {
                    if (isMissing) {
                        THROW_ERROR_EXCEPTION(
                            EErrorCode::SchemaViolation,
                            "Flags for required column %v cannot have %Qlv bit set",
                            columnSchema.GetDiagnosticNameString(),
                            EUnversionedUpdateDataFlags::Missing);
                    }
                    if (value.Type == EValueType::Null) {
                        THROW_ERROR_EXCEPTION(
                            EErrorCode::SchemaViolation,
                            "Required column %v cannot have %Qlv value",
                            columnSchema.GetDiagnosticNameString(),
                            value.Type);
                    }
                }
            }
        }
    }

    TVersionedRow MakeVersionedRow(const TUnversionedRow row)
    {
        if (!row) {
            return TVersionedRow();
        }

        int keyColumnCount = OriginalSchema_->GetKeyColumnCount();

        for (int index = 0; index < keyColumnCount; ++index) {
            YT_ASSERT(row[index].Id == index);
        }

        YT_ASSERT(row[keyColumnCount].Id == keyColumnCount);

        auto modificationType = ERowModificationType(row[keyColumnCount].Data.Int64);
        ValidateModificationType(modificationType);
        ValidateValueColumns(row, keyColumnCount, modificationType == ERowModificationType::Delete);

        switch (modificationType) {
            case ERowModificationType::Write: {
                int valueColumnCount = 0;

                for (int index = keyColumnCount + 1; index < static_cast<int>(row.GetCount()); index += 2) {
                    auto flags = FlagsFromValue(row[index + 1]);
                    if (None(flags & EUnversionedUpdateDataFlags::Missing)) {
                        ++valueColumnCount;
                    }
                }

                auto versionedRow = TMutableVersionedRow::Allocate(
                    RowBuffer_->GetPool(),
                    keyColumnCount,
                    valueColumnCount,
                    1,
                    0);

                ::memcpy(versionedRow.BeginKeys(), row.Begin(), sizeof(TUnversionedValue) * keyColumnCount);

                auto* currentValue = versionedRow.BeginValues();
                for (int index = keyColumnCount + 1; index < static_cast<int>(row.GetCount()); index += 2) {
                    auto flags = FlagsFromValue(row[index + 1]);

                    if (Any(flags & EUnversionedUpdateDataFlags::Missing)) {
                        continue;
                    }

                    // NB: Any timestamp works here. The reader will replace it with the correct one.
                    *currentValue = MakeVersionedValue(row[index], MinTimestamp);
                    currentValue->Id = ToOriginalId(currentValue->Id, keyColumnCount);
                    if (Any(flags & EUnversionedUpdateDataFlags::Aggregate)) {
                        currentValue->Flags |= EValueFlags::Aggregate;
                    }
                    ++currentValue;
                }

                versionedRow.WriteTimestamps()[0] = MinTimestamp;
                return versionedRow;
            }

            case ERowModificationType::Delete: {
                auto versionedRow = TMutableVersionedRow::Allocate(
                    RowBuffer_->GetPool(),
                    keyColumnCount,
                    0,
                    0,
                    1);

                ::memcpy(versionedRow.BeginKeys(), row.Begin(), sizeof(TUnversionedValue) * keyColumnCount);

                versionedRow.DeleteTimestamps()[0] = MinTimestamp;

                return versionedRow;
            }

            default:
                YT_ABORT();
        }
    }
};
////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkWriterPtr CreateSchemalessMultiChunkWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    TNameTablePtr nameTable,
    TTableSchemaPtr schema,
    TLegacyOwningKey lastKey,
    NNative::IClientPtr client,
    TString localHostName,
    TCellTag cellTag,
    TTransactionId transactionId,
    TMasterTableSchemaId schemaId,
    const std::optional<NChunkClient::TDataSink>& dataSink,
    TChunkListId parentChunkListId,
    const TChunkTimestamps& chunkTimestamps,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr throttler,
    IBlockCachePtr blockCache)
{
    switch (options->SchemaModification) {
        case ETableSchemaModification::None: {
            auto createChunkWriter = [=] (IChunkWriterPtr underlyingWriter) {
                return CreateSchemalessChunkWriter(
                    config,
                    options,
                    schema,
                    /*nameTable*/ nullptr,
                    underlyingWriter,
                    dataSink,
                    chunkTimestamps,
                    blockCache);
            };

            auto writer = New<TSchemalessMultiChunkWriter>(
                config,
                options,
                client,
                localHostName,
                cellTag,
                transactionId,
                schemaId,
                parentChunkListId,
                createChunkWriter,
                nameTable,
                schema,
                lastKey,
                trafficMeter,
                throttler,
                blockCache);

            writer->Init();

            return writer;
        }

        case ETableSchemaModification::UnversionedUpdate: {
            auto createChunkWriter = [=] (IChunkWriterPtr underlyingWriter) {
                return CreateVersionedChunkWriter(
                    config,
                    options,
                    schema,
                    std::move(underlyingWriter),
                    dataSink,
                    blockCache);
            };

            auto writer = New<TUnversionedUpdateMultiChunkWriter>(
                config,
                options,
                client,
                localHostName,
                cellTag,
                transactionId,
                schemaId,
                parentChunkListId,
                createChunkWriter,
                nameTable,
                schema,
                lastKey,
                trafficMeter,
                throttler,
                blockCache);

            writer->Init();

            return writer;
        }

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

class TSchemalessTableWriter
    : public IUnversionedWriter
    , public TTransactionListener
{
public:
    TSchemalessTableWriter(
        TTableWriterConfigPtr config,
        TTableWriterOptionsPtr options,
        const TRichYPath& richPath,
        TNameTablePtr nameTable,
        NNative::IClientPtr client,
        TString localHostName,
        ITransactionPtr transaction,
        IThroughputThrottlerPtr throttler,
        IBlockCachePtr blockCache)
        : Config_(std::move(config))
        , Options_(std::move(options))
        , RichPath_(richPath)
        , NameTable_(std::move(nameTable))
        , LocalHostName_(std::move(localHostName))
        , Client_(std::move(client))
        , Transaction_(std::move(transaction))
        , TransactionId_(Transaction_ ? Transaction_->GetId() : NullTransactionId)
        , Throttler_(std::move(throttler))
        , BlockCache_(std::move(blockCache))
        , Logger(TableClientLogger.WithTag("Path: %v, TransactionId: %v",
            richPath.GetPath(),
            TransactionId_))
    {
        if (Transaction_) {
            StartListenTransaction(Transaction_);
        }
    }

    TFuture<void> Open()
    {
        return BIND(&TSchemalessTableWriter::DoOpen, MakeStrong(this))
            .AsyncVia(NChunkClient::TDispatcher::Get()->GetWriterInvoker())
            .Run();
    }

    bool Write(TRange<TUnversionedRow> rows) override
    {
        if (IsAborted()) {
            return false;
        }
        return UnderlyingWriter_->Write(rows);
    }

    TFuture<void> GetReadyEvent() override
    {
        if (IsAborted()) {
            return MakeFuture(GetAbortError());
        }
        return UnderlyingWriter_->GetReadyEvent();
    }

    TFuture<void> Close() override
    {
        return BIND(&TSchemalessTableWriter::DoClose, MakeStrong(this))
            .AsyncVia(NChunkClient::TDispatcher::Get()->GetWriterInvoker())
            .Run();
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    const TTableSchemaPtr& GetSchema() const override
    {
        return TableUploadOptions_.TableSchema.Get();
    }

private:
    const TTableWriterConfigPtr Config_;
    const TTableWriterOptionsPtr Options_;
    const TRichYPath RichPath_;
    const TNameTablePtr NameTable_;
    const TString LocalHostName_;
    const NNative::IClientPtr Client_;
    const ITransactionPtr Transaction_;
    const TTransactionId TransactionId_;
    const IThroughputThrottlerPtr Throttler_;
    const IBlockCachePtr BlockCache_;

    const NLogging::TLogger Logger;

    TObjectId ObjectId_;
    TTableUploadOptions TableUploadOptions_;
    ITransactionPtr UploadTransaction_;
    ISchemalessMultiChunkWriterPtr UnderlyingWriter_;

    TTableSchemaPtr GetChunkSchema() const
    {
        auto chunkSchema = GetSchema();

        bool tableUniqueKeys = chunkSchema->IsUniqueKeys();
        auto chunkUniqueKeys = RichPath_.GetChunkUniqueKeys();
        if (chunkUniqueKeys) {
            if (!*chunkUniqueKeys && tableUniqueKeys) {
                THROW_ERROR_EXCEPTION(
                    EErrorCode::SchemaViolation,
                    "Table schema forces keys to be unique while chunk schema does not");
            }

            chunkSchema = chunkSchema->SetUniqueKeys(*chunkUniqueKeys);
        }

        auto chunkSortColumns = RichPath_.GetChunkSortColumns();
        if (chunkSortColumns) {
            auto tableSchemaSortColumns = GetSchema()->GetSortColumns();
            if (chunkSortColumns->size() < tableSchemaSortColumns.size()) {
                THROW_ERROR_EXCEPTION(
                    EErrorCode::SchemaViolation,
                    "Chunk sort columns list is shorter than table schema sort columns")
                    << TErrorAttribute("chunk_sort_columns_count", chunkSortColumns->size())
                    << TErrorAttribute("table_sort_column_count", tableSchemaSortColumns.size());
            }

            if (tableUniqueKeys && !tableSchemaSortColumns.empty()) {
                THROW_ERROR_EXCEPTION(
                    EErrorCode::SchemaViolation,
                    "Chunk sort columns cannot be set when table is sorted with unique keys");
            }

            for (int columnIndex = 0; columnIndex < std::ssize(tableSchemaSortColumns); ++columnIndex) {
                if ((*chunkSortColumns)[columnIndex] != tableSchemaSortColumns[columnIndex]) {
                    THROW_ERROR_EXCEPTION(EErrorCode::IncompatibleKeyColumns,
                        "Incompatible sort columns: chunk sort columns %v, table sort columns %v",
                        chunkSortColumns,
                        tableSchemaSortColumns);
                }
            }

            chunkSchema = chunkSchema->ToSorted(*chunkSortColumns);
        }

        if (chunkSchema->IsUniqueKeys() && !chunkSchema->IsSorted()) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::InvalidSchemaValue,
                "Non-sorted schema can't have unique keys requirement");
        }

        return chunkSchema;
    }

    void DoOpen()
    {
        auto writerConfig = CloneYsonStruct(Config_);
        writerConfig->WorkloadDescriptor.Annotations.push_back(Format("TablePath: %v", RichPath_.GetPath()));

        const auto& path = RichPath_.GetPath();

        TUserObject userObject(path);

        GetUserObjectBasicAttributes(
            Client_,
            {&userObject},
            TransactionId_,
            Logger,
            EPermission::Write);

        if (userObject.Type != EObjectType::Table) {
            THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, actual %Qlv",
                path,
                EObjectType::Table,
                userObject.Type);
        }

        ObjectId_ = userObject.ObjectId;
        auto nativeCellTag = CellTagFromId(ObjectId_);
        auto externalCellTag = userObject.ExternalCellTag;
        auto objectIdPath = FromObjectId(ObjectId_);

        TTableSchemaPtr chunkSchema;

        {
            YT_LOG_DEBUG("Requesting extended table attributes");

            auto proxy = CreateObjectServiceReadProxy(
                Client_,
                EMasterChannelKind::Follower,
                externalCellTag);

            static const auto AttributeKeys = [] {
                return ConcatVectors(
                    GetTableUploadOptionsAttributeKeys(),
                    std::vector<TString>{
                        "account",
                        "chunk_writer",
                        "primary_medium",
                        "replication_factor",
                        "row_count",
                        "schema",
                        "vital",
                        "enable_skynet_sharing"
                    });
            }();

            auto req = TCypressYPathProxy::Get(objectIdPath);
            AddCellTagToSyncWith(req, userObject.ObjectId);
            NCypressClient::SetTransactionId(req, userObject.ExternalTransactionId);
            ToProto(req->mutable_attributes()->mutable_keys(), AttributeKeys);

            auto rspOrError = WaitFor(proxy.Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(
                rspOrError,
                "Error requesting extended attributes of table %v",
                path);

            const auto& rsp = rspOrError.Value();
            auto node = ConvertToNode(TYsonString(rsp->value()));
            const auto& attributes = node->Attributes();

            if (attributes.Get<bool>("dynamic")) {
                THROW_ERROR_EXCEPTION("\"write_table\" API is not supported for dynamic tables; use \"insert_rows\" instead");
            }

            TableUploadOptions_ = GetTableUploadOptions(
                RichPath_,
                attributes,
                attributes.Get<TTableSchemaPtr>("schema"),
                attributes.Get<i64>("row_count"));

            chunkSchema = GetChunkSchema();

            Options_->ReplicationFactor = attributes.Get<int>("replication_factor");
            Options_->MediumName = attributes.Get<TString>("primary_medium");
            Options_->CompressionCodec = TableUploadOptions_.CompressionCodec;
            Options_->ErasureCodec = TableUploadOptions_.ErasureCodec;
            Options_->EnableStripedErasure = TableUploadOptions_.EnableStripedErasure;
            Options_->Account = attributes.Get<TString>("account");
            Options_->ChunksVital = attributes.Get<bool>("vital");
            Options_->EnableSkynetSharing = attributes.Get<bool>("enable_skynet_sharing", false);

            // Table's schema is never stricter than chunk's schema.
            Options_->ValidateSorted = chunkSchema->IsSorted();
            Options_->ValidateUniqueKeys = chunkSchema->IsUniqueKeys();

            Options_->OptimizeFor = TableUploadOptions_.OptimizeFor;
            Options_->ChunkFormat = TableUploadOptions_.ChunkFormat;
            Options_->EvaluateComputedColumns = TableUploadOptions_.TableSchema->HasComputedColumns();
            Options_->TableSchema = GetSchema();

            auto chunkWriterConfig = attributes.FindYson("chunk_writer");
            if (chunkWriterConfig) {
                ReconfigureYsonStruct(writerConfig, chunkWriterConfig);
            }

            YT_LOG_DEBUG("Extended attributes received "
                "(Account: %v, CompressionCodec: %v, ErasureCodec: %v, EnableStripedErasure: %v, EnableSkynetSharing: %v)",
                Options_->Account,
                Options_->CompressionCodec,
                Options_->ErasureCodec,
                Options_->EnableStripedErasure,
                Options_->EnableSkynetSharing);
        }

        TMasterTableSchemaId chunkSchemaId;

        {
            YT_LOG_DEBUG("Starting table upload");

            auto proxy = CreateObjectServiceWriteProxy(Client_, nativeCellTag);
            auto batchReq = proxy.ExecuteBatch();

            {
                auto req = TTableYPathProxy::BeginUpload(objectIdPath);
                ToProto(req->mutable_table_schema(), TableUploadOptions_.TableSchema.Get());
                // Only time this can be true is when RichPath_ has extra chunk sort columns.
                if (chunkSchema != TableUploadOptions_.TableSchema.Get()) {
                    auto checkResult = CheckTableSchemaCompatibility(*chunkSchema, *TableUploadOptions_.TableSchema.Get(), false);
                    if (!checkResult.second.IsOK()) {
                        YT_LOG_FATAL(
                            checkResult.second,
                            "Chunk schema is incompatible with a table schema (ChunkSchema: %v, TableSchema: %v)",
                            *chunkSchema,
                            *TableUploadOptions_.TableSchema.Get());
                    }
                    ToProto(req->mutable_chunk_schema(), chunkSchema);
                }
                req->set_schema_mode(static_cast<int>(TableUploadOptions_.SchemaMode));
                req->set_optimize_for(static_cast<int>(TableUploadOptions_.OptimizeFor));
                req->set_update_mode(static_cast<int>(TableUploadOptions_.UpdateMode));
                req->set_lock_mode(static_cast<int>(TableUploadOptions_.LockMode));
                req->set_upload_transaction_title(Format("Upload to %v", path));
                req->set_upload_transaction_timeout(ToProto<i64>(Client_->GetNativeConnection()->GetConfig()->UploadTransactionTimeout));
                SetTransactionId(req, Transaction_);
                GenerateMutationId(req);
                batchReq->AddRequest(req, "begin_upload");
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(
                GetCumulativeError(batchRspOrError),
                "Error starting upload to table %v",
                path);
            const auto& batchRsp = batchRspOrError.Value();
            {
                auto rsp = batchRsp->GetResponse<TTableYPathProxy::TRspBeginUpload>("begin_upload").Value();
                auto uploadTransactionId = FromProto<TTransactionId>(rsp->upload_transaction_id());
                chunkSchemaId = FromProto<TMasterTableSchemaId>(rsp->upload_chunk_schema_id());
                UploadTransaction_ = Client_->AttachTransaction(uploadTransactionId, TTransactionAttachOptions{
                    .AutoAbort = true
                });

                YT_LOG_DEBUG("Table upload started (UploadTransactionId: %v)",
                    uploadTransactionId);
            }
        }

        StartListenTransaction(UploadTransaction_);

        TLegacyOwningKey writerLastKey;
        TChunkListId chunkListId;

        {
            YT_LOG_DEBUG("Requesting table upload parameters");

            auto proxy = CreateObjectServiceReadProxy(
                Client_,
                EMasterChannelKind::Follower,
                externalCellTag);

            auto req =  TTableYPathProxy::GetUploadParams(objectIdPath);
            req->set_fetch_last_key(
                TableUploadOptions_.UpdateMode == EUpdateMode::Append &&
                TableUploadOptions_.TableSchema->IsSorted());
            SetTransactionId(req, UploadTransaction_);

            auto rspOrError = WaitFor(proxy.Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(
                rspOrError,
                "Error requesting upload parameters for table %v",
                path);

            const auto& rsp = rspOrError.Value();
            chunkListId = FromProto<TChunkListId>(rsp->chunk_list_id());
            if (auto lastKey = FromProto<TLegacyOwningKey>(rsp->last_key())) {
                writerLastKey = TLegacyOwningKey(lastKey.FirstNElements(TableUploadOptions_.TableSchema->GetKeyColumnCount()));
            }

            Options_->MaxHeavyColumns = rsp->max_heavy_columns();

            YT_LOG_DEBUG("Table upload parameters received (ChunkListId: %v, HasLastKey: %v, MaxHeavyColumns: %v)",
                chunkListId,
                static_cast<bool>(writerLastKey),
                Options_->MaxHeavyColumns);
        }

        auto timestamp = WaitFor(Client_->GetNativeConnection()->GetTimestampProvider()->GenerateTimestamps())
            .ValueOrThrow();

        NChunkClient::TDataSink dataSink;
        dataSink.SetPath(userObject.GetPath());
        dataSink.SetObjectId(userObject.ObjectId);
        dataSink.SetAccount(Options_->Account);

        UnderlyingWriter_ = CreateSchemalessMultiChunkWriter(
            writerConfig,
            Options_,
            NameTable_,
            chunkSchema,
            writerLastKey,
            Client_,
            LocalHostName_,
            externalCellTag,
            UploadTransaction_->GetId(),
            chunkSchemaId,
            dataSink,
            chunkListId,
            TChunkTimestamps{timestamp, timestamp},
            /*trafficMeter*/ nullptr,
            Throttler_,
            BlockCache_);

        YT_LOG_DEBUG("Table opened");
    }

    void DoClose()
    {
        const auto& path = RichPath_.GetPath();
        auto nativeCellTag = CellTagFromId(ObjectId_);
        auto objectIdPath = FromObjectId(ObjectId_);

        YT_LOG_DEBUG("Closing table");

        auto underlyingWriterCloseError = WaitFor(UnderlyingWriter_->Close());

        StopListenTransaction(UploadTransaction_);

        if (!underlyingWriterCloseError.IsOK()) {
            YT_VERIFY(UploadTransaction_); // Shouldn't be closing an unopened writer.
            Y_UNUSED(WaitFor(UploadTransaction_->Abort()));
            THROW_ERROR_EXCEPTION("Error closing chunk writer")
                << underlyingWriterCloseError;
        }

        auto proxy = CreateObjectServiceWriteProxy(
            Client_,
            nativeCellTag);
        auto batchReq = proxy.ExecuteBatch();

        {
            auto req = TTableYPathProxy::EndUpload(objectIdPath);
            *req->mutable_statistics() = UnderlyingWriter_->GetDataStatistics();
            if (TableUploadOptions_.ChunkFormat) {
                req->set_chunk_format(ToProto<int>(*TableUploadOptions_.ChunkFormat));
            }
            req->set_compression_codec(ToProto<int>(TableUploadOptions_.CompressionCodec));
            req->set_erasure_codec(ToProto<int>(TableUploadOptions_.ErasureCodec));

            // COMPAT(h0pless): remove this when clients will send table schema options during begin upload.
            ToProto(req->mutable_table_schema(), TableUploadOptions_.TableSchema.Get());
            req->set_schema_mode(ToProto<int>(TableUploadOptions_.SchemaMode));
            req->set_optimize_for(ToProto<int>(TableUploadOptions_.OptimizeFor));

            if (TableUploadOptions_.SecurityTags) {
                ToProto(req->mutable_security_tags()->mutable_items(), *TableUploadOptions_.SecurityTags);
            }

            SetTransactionId(req, UploadTransaction_);
            GenerateMutationId(req);
            batchReq->AddRequest(req, "end_upload");
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            GetCumulativeError(batchRspOrError),
            "Error finishing upload to table %v",
            path);

        UploadTransaction_->Detach();

        // Log all statistics.
        YT_LOG_DEBUG("Writer data statistics (DataStatistics: %v)", UnderlyingWriter_->GetDataStatistics());
        YT_LOG_DEBUG("Writer compression codec statistics (CodecStatistics: %v)", UnderlyingWriter_->GetCompressionStatistics());

        YT_LOG_DEBUG("Table closed");
    }
};

////////////////////////////////////////////////////////////////////////////////

TFuture<IUnversionedWriterPtr> CreateSchemalessTableWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    const TRichYPath& richPath,
    TNameTablePtr nameTable,
    NNative::IClientPtr client,
    TString localHostName,
    ITransactionPtr transaction,
    IThroughputThrottlerPtr throttler,
    IBlockCachePtr blockCache)
{
    if (blockCache->GetSupportedBlockTypes() != EBlockType::None) {
        // It is hard to support both reordering and uncompressed block caching
        // since block becomes cached significantly before we know the final permutation.
        // Supporting reordering for compressed block cache is not hard
        // to implement, but is not done for now.
        config->EnableBlockReordering = false;
    }

    auto writer = New<TSchemalessTableWriter>(
        std::move(config),
        std::move(options),
        richPath,
        std::move(nameTable),
        std::move(client),
        std::move(localHostName),
        std::move(transaction),
        std::move(throttler),
        std::move(blockCache));
    return writer->Open()
        .Apply(BIND([=] () -> IUnversionedWriterPtr { return writer; }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
