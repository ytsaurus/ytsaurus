#include "schemaless_chunk_writer.h"
#include "chunk_meta_extensions.h"
#include "config.h"
#include "name_table.h"
#include "partitioner.h"
#include "schemaless_block_writer.h"
#include "schemaless_row_reorderer.h"
#include "table_ypath_proxy.h"
#include "helpers.h"
#include "row_buffer.h"
#include "skynet_column_evaluator.h"

#include <yt/ytlib/api/client.h>
#include <yt/ytlib/api/transaction.h>

#include <yt/ytlib/table_chunk_format/column_writer.h>
#include <yt/ytlib/table_chunk_format/schemaless_column_writer.h>
#include <yt/ytlib/table_chunk_format/data_block_writer.h>

#include <yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/ytlib/chunk_client/encoding_chunk_writer.h>
#include <yt/ytlib/chunk_client/multi_chunk_writer_base.h>
#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/transaction_client/helpers.h>
#include <yt/ytlib/transaction_client/transaction_listener.h>
#include <yt/ytlib/transaction_client/config.h>
#include <yt/ytlib/transaction_client/timestamp_provider.h>

#include <yt/ytlib/query_client/column_evaluator.h>

#include <yt/ytlib/api/transaction.h>
#include <yt/ytlib/api/config.h>
#include <yt/ytlib/api/native_connection.h>

#include <yt/ytlib/ypath/rich.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/random.h>

#include <yt/core/ytree/helpers.h>

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NTableClient::NProto;
using namespace NTableChunkFormat;
using namespace NRpc;
using namespace NApi;
using namespace NTransactionClient;
using namespace NNodeTrackerClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NApi;
using namespace NQueryClient;

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
        const TTableSchema& schema,
        const TChunkTimestamps& chunkTimestamps)
        : Logger(NLogging::TLogger(TableClientLogger)
            .AddTag("ChunkWriterId: %v", TGuid::Create()))
        , Schema_(schema)
        , ChunkTimestamps_(chunkTimestamps)
        , ChunkNameTable_(TNameTable::FromSchema(schema))
        , Config_(config)
        , Options_(options)
        , EncodingChunkWriter_(New<TEncodingChunkWriter>(
            config,
            options,
            chunkWriter,
            blockCache,
            Logger))
        , RandomGenerator_(RandomNumber<ui64>())
        , SamplingThreshold_(static_cast<ui64>(std::numeric_limits<ui64>::max() * Config_->SampleRate))
    { }

    virtual TFuture<void> Close() override
    {
        if (RowCount_ == 0) {
            // Empty chunk.
            return VoidFuture;
        }

        return BIND(&TUnversionedChunkWriterBase::DoClose, MakeStrong(this))
            .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
            .Run();
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return EncodingChunkWriter_->GetReadyEvent();
    }

    virtual i64 GetMetaSize() const override
    {
        // Other meta parts are negligible.
        return BlockMetaExtSize_ + SamplesExtSize_ + ChunkNameTable_->GetByteSize();
    }

    virtual i64 GetCompressedDataSize() const override
    {
        return EncodingChunkWriter_->GetDataStatistics().compressed_data_size();
    }

    virtual bool IsCloseDemanded() const override
    {
        return false;
    }

    virtual NChunkClient::NProto::TChunkMeta GetMasterMeta() const override
    {
        TChunkMeta meta;
        SetProtoExtension(meta.mutable_extensions(), EncodingChunkWriter_->MiscExt());
        FillCommonMeta(&meta);

        if (IsSorted()) {
            SetProtoExtension(meta.mutable_extensions(), BoundaryKeysExt_);
        }

        return meta;
    }

    virtual NChunkClient::NProto::TChunkMeta GetSchedulerMeta() const override
    {
        return GetMasterMeta();
    }

    virtual NChunkClient::NProto::TChunkMeta GetNodeMeta() const override
    {
        return EncodingChunkWriter_->Meta();
    }

    virtual TChunkId GetChunkId() const override
    {
        return EncodingChunkWriter_->GetChunkId();
    }

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        auto dataStatistics = EncodingChunkWriter_->GetDataStatistics();
        dataStatistics.set_row_count(RowCount_);
        return dataStatistics;
    }

    virtual TCodecStatistics GetCompressionStatistics() const override
    {
        return EncodingChunkWriter_->GetCompressionStatistics();
    }

    virtual const TNameTablePtr& GetNameTable() const override
    {
        return ChunkNameTable_;
    }

    virtual const TTableSchema& GetSchema() const override
    {
        return Schema_;
    }

    virtual i64 GetDataWeight() const override
    {
        return DataWeight_;
    }

protected:
    const NLogging::TLogger Logger;

    const TTableSchema Schema_;
    const TChunkTimestamps ChunkTimestamps_;

    TNameTablePtr ChunkNameTable_;

    const TChunkWriterConfigPtr Config_;
    const TChunkWriterOptionsPtr Options_;

    i64 RowCount_ = 0;
    i64 DataWeight_ = 0;
    i64 DataWeightSinceLastBlockFlush_ = 0;

    const TEncodingChunkWriterPtr EncodingChunkWriter_;
    TOwningKey LastKey_;

    NProto::TBlockMetaExt BlockMetaExt_;

    virtual ETableChunkFormat GetTableChunkFormat() const = 0;
    virtual bool SupportBoundaryKeys() const = 0;

    bool IsSorted() const
    {
        return Schema_.IsSorted() && SupportBoundaryKeys();
    }

    void RegisterBlock(TBlock& block, TUnversionedRow lastRow)
    {
        if (IsSorted()) {
            ToProto(
                block.Meta.mutable_last_key(),
                lastRow.Begin(),
                lastRow.Begin() + Schema_.GetKeyColumnCount());
        }

        YCHECK(block.Meta.uncompressed_size() > 0);

        block.Meta.set_block_index(BlockMetaExt_.blocks_size());

        BlockMetaExtSize_ += block.Meta.ByteSize();
        BlockMetaExt_.add_blocks()->Swap(&block.Meta);

        EncodingChunkWriter_->WriteBlock(std::move(block.Data));
    }

    void ProcessRowset(const TRange<TUnversionedRow>& rows)
    {
        if (rows.Empty()) {
            return;
        }

        EmitRandomSamples(rows);

        if (IsSorted()) {
            CaptureBoundaryKeys(rows);
        }
    }

    virtual void PrepareChunkMeta()
    {
        auto& miscExt = EncodingChunkWriter_->MiscExt();
        miscExt.set_sorted(IsSorted());
        miscExt.set_unique_keys(Schema_.GetUniqueKeys());
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

        auto& meta = EncodingChunkWriter_->Meta();
        FillCommonMeta(&meta);

        auto nameTableExt = ToProto<TNameTableExt>(ChunkNameTable_);
        SetProtoExtension(meta.mutable_extensions(), nameTableExt);

        auto schemaExt = ToProto<TTableSchemaExt>(Schema_);
        SetProtoExtension(meta.mutable_extensions(), schemaExt);

        SetProtoExtension(meta.mutable_extensions(), BlockMetaExt_);

        SetProtoExtension(meta.mutable_extensions(), SamplesExt_);

        SetProtoExtension(meta.mutable_extensions(), ColumnarStatisticsExt_);

        if (IsSorted()) {
            ToProto(BoundaryKeysExt_.mutable_max(), LastKey_);
            SetProtoExtension(meta.mutable_extensions(), BoundaryKeysExt_);
        }

        if (Schema_.IsSorted()) {
            // Sorted or partition chunks.
            TKeyColumnsExt keyColumnsExt;
            NYT::ToProto(keyColumnsExt.mutable_names(), Schema_.GetKeyColumns());
            SetProtoExtension(meta.mutable_extensions(), keyColumnsExt);
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
        int keyColumnCount = IsSorted() ? Schema_.GetKeyColumnCount() : 0;

        for (int index = 0; index < keyColumnCount; ++index) {
            weight += NTableClient::GetDataWeight(row[index]);
        }
        ValidateKeyWeight(weight, Config_, Options_);

        for (int index = keyColumnCount; index < row.GetCount(); ++index) {
            weight += NTableClient::GetDataWeight(row[index]);
        }
        ValidateRowWeight(weight, Config_, Options_);
        DataWeight_ += weight;
        DataWeightSinceLastBlockFlush_ += weight;

        UpdateColumnarStatistics(ColumnarStatisticsExt_, MakeRange(row.Begin(), row.End()));

        return weight;
    }

private:
    i64 BlockMetaExtSize_ = 0;

    NProto::TBoundaryKeysExt BoundaryKeysExt_;

    TRandomGenerator RandomGenerator_;
    const ui64 SamplingThreshold_;
    NProto::TSamplesExt SamplesExt_;
    NProto::TColumnarStatisticsExt ColumnarStatisticsExt_;
    i64 SamplesExtSize_ = 0;

    void FillCommonMeta(NChunkClient::NProto::TChunkMeta* meta) const
    {
        meta->set_type(static_cast<int>(EChunkType::Table));
        meta->set_version(static_cast<int>(GetTableChunkFormat()));
    }

    void EmitRandomSamples(const TRange<TUnversionedRow>& rows)
    {
        for (auto row : rows) {
            if (RandomGenerator_.Generate<ui64>() < SamplingThreshold_) {
                EmitSample(row);
            }
        }

        if (SamplesExtSize_ == 0) {
            EmitSample(rows.Front());
        }
    }

    void CaptureBoundaryKeys(const TRange<TUnversionedRow>& rows)
    {
        if (!BoundaryKeysExt_.has_min()) {
            auto firstRow = rows.Front();
            ToProto(
                BoundaryKeysExt_.mutable_min(),
                firstRow.Begin(),
                firstRow.Begin() + Schema_.GetKeyColumnCount());
        }

        auto lastRow = rows.Back();
        LastKey_ = TOwningKey(lastRow.Begin(), lastRow.Begin() + Schema_.GetKeyColumnCount());
    }

    void EmitSample(TUnversionedRow row)
    {
        SmallVector<TUnversionedValue, TypicalColumnCount> sampleValues;
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

        auto entry = SerializeToString(sampleValues.begin(), sampleValues.end());
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
        const TTableSchema& schema,
        const TChunkTimestamps& chunkTimestamps)
        : TUnversionedChunkWriterBase(
            config,
            options,
            chunkWriter,
            blockCache,
            schema,
            chunkTimestamps)
    {
        BlockWriter_.reset(new THorizontalSchemalessBlockWriter);
    }

    virtual i64 GetCompressedDataSize() const override
    {
        return TUnversionedChunkWriterBase::GetCompressedDataSize() +
           (BlockWriter_ ? BlockWriter_->GetBlockSize() : 0);
    }

    virtual bool Write(const TRange<TUnversionedRow>& rows) override
    {
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
                BlockWriter_.reset(new THorizontalSchemalessBlockWriter);
            }
        }

        ProcessRowset(rows);
        return EncodingChunkWriter_->IsReady();
    }

private:
    std::unique_ptr<THorizontalSchemalessBlockWriter> BlockWriter_;

    virtual ETableChunkFormat GetTableChunkFormat() const override
    {
        return ETableChunkFormat::SchemalessHorizontal;
    }

    virtual bool SupportBoundaryKeys() const
    {
        return true;
    }

    void DoClose()
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
        const TTableSchema& schema,
        const TChunkTimestamps& chunkTimestamps)
        : TUnversionedChunkWriterBase(
            config,
            options,
            chunkWriter,
            blockCache,
            schema,
            chunkTimestamps)
        , DataToBlockFlush_(Config_->BlockSize)
    {
        // Only scan-optimized version for now.
        THashMap<TString, TDataBlockWriter*> groupBlockWriters;
        for (const auto& column : Schema_.Columns()) {
            if (column.Group() && groupBlockWriters.find(*column.Group()) == groupBlockWriters.end()) {
                auto blockWriter = std::make_unique<TDataBlockWriter>();
                groupBlockWriters[*column.Group()] = blockWriter.get();
                BlockWriters_.emplace_back(std::move(blockWriter));
            }
        }

        auto getBlockWriter = [&] (const NTableClient::TColumnSchema& columnSchema) -> TDataBlockWriter* {
            if (columnSchema.Group()) {
                return groupBlockWriters[*columnSchema.Group()];
            } else {
                BlockWriters_.emplace_back(std::make_unique<TDataBlockWriter>());
                return BlockWriters_.back().get();
            }
        };

        for (int columnIndex = 0; columnIndex < Schema_.Columns().size(); ++columnIndex) {
            const auto& column = Schema_.Columns()[columnIndex];
            ValueColumnWriters_.emplace_back(CreateUnversionedColumnWriter(
                column,
                columnIndex,
                getBlockWriter(column)));
        }

        if (!Schema_.GetStrict()) {
             auto blockWriter = std::make_unique<TDataBlockWriter>();
             ValueColumnWriters_.emplace_back(CreateSchemalessColumnWriter(
                Schema_.Columns().size(),
                blockWriter.get()));
             BlockWriters_.emplace_back(std::move(blockWriter));
        }

        YCHECK(BlockWriters_.size() > 0);
    }

    virtual bool Write(const TRange<TUnversionedRow>& rows) override
    {
        int startRowIndex = 0;
        while (startRowIndex < rows.Size()) {
            i64 weight = 0;
            int rowIndex = startRowIndex;
            for (; rowIndex < rows.Size() && weight < DataToBlockFlush_; ++rowIndex) {
                weight += UpdateDataWeight(rows[rowIndex]);
            }

            auto range = MakeRange(rows.Begin() + startRowIndex, rows.Begin() + rowIndex);
            for (auto& columnWriter : ValueColumnWriters_) {
                columnWriter->WriteUnversionedValues(range);
            }

            RowCount_ += range.Size();

            startRowIndex = rowIndex;

            TryFlushBlock(rows[rowIndex - 1]);
        }

        ProcessRowset(rows);

        return EncodingChunkWriter_->IsReady();
    }

    virtual i64 GetCompressedDataSize() const override
    {
        i64 result = TUnversionedChunkWriterBase::GetCompressedDataSize();
        for (const auto& blockWriter : BlockWriters_) {
            result += blockWriter->GetCurrentSize();
        }
        return result;
    }

    virtual i64 GetMetaSize() const override
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

    virtual ETableChunkFormat GetTableChunkFormat() const override
    {
        return ETableChunkFormat::UnversionedColumnar;
    }

    virtual bool SupportBoundaryKeys() const
    {
        return true;
    }

    void TryFlushBlock(TUnversionedRow lastRow)
    {
        while (true) {
            i64 totalSize = 0;
            i64 maxWriterSize = -1;
            int maxWriterIndex = -1;

            for (int i = 0; i < BlockWriters_.size(); ++i) {
                auto size = BlockWriters_[i]->GetCurrentSize();
                totalSize += size;
                if (size > maxWriterSize) {
                    maxWriterIndex = i;
                    maxWriterSize = size;
                }
            }

            YCHECK(maxWriterIndex >= 0);

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
        auto block = BlockWriters_[blockWriterIndex]->DumpBlock(BlockMetaExt_.blocks_size(), RowCount_);
        block.Meta.set_chunk_row_count(RowCount_);
        RegisterBlock(block, lastRow);
    }

    virtual void DoClose() override
    {
        using NYT::ToProto;

        for (int i = 0; i < BlockWriters_.size(); ++i) {
            if (BlockWriters_[i]->GetCurrentSize() > 0) {
                FinishBlock(i, LastKey_.Get());
            }
        }

        TUnversionedChunkWriterBase::DoClose();
    }

    virtual void PrepareChunkMeta() override
    {
        TUnversionedChunkWriterBase::PrepareChunkMeta();

        auto& meta = EncodingChunkWriter_->Meta();

        NProto::TColumnMetaExt columnMetaExt;
        for (const auto& valueColumnWriter : ValueColumnWriters_) {
            *columnMetaExt.add_columns() = valueColumnWriter->ColumnMeta();
        }

        SetProtoExtension(meta.mutable_extensions(), columnMetaExt);
    }
};

////////////////////////////////////////////////////////////////////////////////

ISchemalessChunkWriterPtr CreateSchemalessChunkWriter(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    const TTableSchema& schema,
    IChunkWriterPtr chunkWriter,
    const TChunkTimestamps& chunkTimestamps,
    IBlockCachePtr blockCache)
{
    if (options->OptimizeFor == EOptimizeFor::Lookup) {
        return New<TSchemalessChunkWriter>(
            config,
            options,
            chunkWriter,
            blockCache,
            schema,
            chunkTimestamps);
    } else {
        return New<TColumnUnversionedChunkWriter>(
            config,
            options,
            chunkWriter,
            blockCache,
            schema,
            chunkTimestamps);
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
        const TTableSchema& schema,
        int partitionCount)
        : TUnversionedChunkWriterBase(
            std::move(config),
            std::move(options),
            std::move(chunkWriter),
            std::move(blockCache),
            schema,
            TChunkTimestamps())
    {
        PartitionsExt_.mutable_row_counts()->Resize(partitionCount, 0);
        PartitionsExt_.mutable_uncompressed_data_sizes()->Resize(partitionCount, 0);
    }

    bool WriteBlock(TBlock block)
    {
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

    virtual bool SupportBoundaryKeys() const override
    {
        return false;
    }

    virtual bool Write(const TRange<TUnversionedRow>& rows) override
    {
        // This method is never called for partition chunks.
        // Blocks are formed in the multi chunk writer.
        Y_UNREACHABLE();
    }

    virtual i64 GetCompressedDataSize() const override
    {
        // Retrun uncompressed data size to make smaller chunks and better balance partition data
        // between HDDs. Also returning uncompressed data makes chunk switch deterministic,
        // since compression is asynchronous.
        return EncodingChunkWriter_->GetDataStatistics().uncompressed_data_size();
    }

    virtual TChunkMeta GetSchedulerMeta() const override
    {
        auto meta = TUnversionedChunkWriterBase::GetSchedulerMeta();
        SetProtoExtension(meta.mutable_extensions(), PartitionsExt_);
        return meta;
    }

    virtual bool IsCloseDemanded() const override
    {
        return LargestPartitionRowCount_ > PartitionRowCountLimit;
    }

    virtual i64 GetMetaSize() const override
    {
        return TUnversionedChunkWriterBase::GetMetaSize() +
            // PartitionsExt.
            2 * sizeof(i64) * PartitionsExt_.row_counts_size();
    }

private:
    TPartitionsExt PartitionsExt_;
    i64 LargestPartitionRowCount_ = 0;

    virtual ETableChunkFormat GetTableChunkFormat() const override
    {
        return ETableChunkFormat::SchemalessHorizontal;
    }

    virtual void PrepareChunkMeta() override
    {
        TUnversionedChunkWriterBase::PrepareChunkMeta();

        LOG_DEBUG("Partition totals: %v", PartitionsExt_.DebugString());

        auto& meta = EncodingChunkWriter_->Meta();

        SetProtoExtension(meta.mutable_extensions(), PartitionsExt_);
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
        INativeClientPtr client,
        TCellTag cellTag,
        const TTransactionId& transactionId,
        const TChunkListId& parentChunkListId,
        TNameTablePtr nameTable,
        const TTableSchema& schema,
        TOwningKey lastKey,
        TTrafficMeterPtr trafficMeter,
        IThroughputThrottlerPtr throttler,
        IBlockCachePtr blockCache)
        : TNontemplateMultiChunkWriterBase(
            config,
            options,
            client,
            cellTag,
            transactionId,
            parentChunkListId,
            trafficMeter,
            throttler,
            blockCache)
        , Config_(config)
        , Options_(options)
        , NameTable_(nameTable)
        , Schema_(schema)
        , LastKey_(lastKey)
    {
        if (Options_->EvaluateComputedColumns) {
            ColumnEvaluator_ = Client_->GetNativeConnection()->GetColumnEvaluatorCache()->Find(Schema_);
        }

        if (Options_->EnableSkynetSharing) {
            SkynetColumnEvaluator_ = New<TSkynetColumnEvaluator>(schema);
        }
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        if (Error_.IsOK()) {
            return TNontemplateMultiChunkWriterBase::GetReadyEvent();
        } else {
            return MakeFuture(Error_);
        }
    }

    virtual const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    virtual const TTableSchema& GetSchema() const override
    {
        return Schema_;
    }

protected:
    const TTableWriterConfigPtr Config_;
    const TTableWriterOptionsPtr Options_;
    const TNameTablePtr NameTable_;
    const TTableSchema Schema_;

    TError Error_;


    virtual TNameTablePtr GetChunkNameTable() = 0;

    void ResetIdMapping()
    {
        IdMapping_.clear();
    }

    std::vector<TUnversionedRow> ReorderAndValidateRows(const TRange<TUnversionedRow>& rows)
    {
        RowBuffer_->Clear();

        std::vector<TUnversionedRow> result;
        result.reserve(rows.Size());

        for (size_t rowIndex = 0; rowIndex < rows.Size(); ++rowIndex) {
            auto row = rows[rowIndex];
            ValidateDuplicateIds(row);

            int maxColumnCount = Schema_.Columns().size() + (Schema_.GetStrict() ? 0 : row.GetCount());
            auto mutableRow = TMutableUnversionedRow::Allocate(RowBuffer_->GetPool(), maxColumnCount);
            int columnCount = Schema_.Columns().size();

            for (int i = 0; i < Schema_.Columns().size(); ++i) {
                // Id for schema columns in chunk name table always coincide with column index in schema.
                mutableRow[i] = MakeUnversionedSentinelValue(EValueType::Null, i);
            }

            for (const auto* valueIt = row.Begin(); valueIt != row.End(); ++valueIt) {
                if (IdMapping_.size() <= valueIt->Id) {
                    IdMapping_.resize(valueIt->Id + 1, -1);
                }

                if (IdMapping_[valueIt->Id] == -1) {
                    IdMapping_[valueIt->Id] = GetChunkNameTable()->GetIdOrRegisterName(NameTable_->GetName(valueIt->Id));
                }

                int id = IdMapping_[valueIt->Id];
                if (id < Schema_.Columns().size()) {
                    // Validate schema column types.
                    mutableRow[id] = *valueIt;
                    mutableRow[id].Id = id;
                } else {
                    // Validate non-schema columns for
                    if (Schema_.GetStrict()) {
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

            // Now mutableRow contains all values that schema knows about.
            // And we can check types and check that all required fields are set.
            for (int i = 0; i < Schema_.Columns().size(); ++i) {
                const auto& column = Schema_.Columns()[i];
                ValidateValueType(mutableRow[i], column, /*typeAnyAcceptsAllValues*/ true);
            }

            ValidateColumnCount(columnCount);
            mutableRow.SetCount(columnCount);

            EvaluateComputedColumns(mutableRow);
            EvaluateSkynetColumns(mutableRow, rowIndex + 1 == rows.Size());

            result.push_back(mutableRow);
        }

        ValidateSortAndUnique(result);
        return result;
    }

private:
    TUnversionedOwningRowBuilder KeyBuilder_;
    TOwningKey LastKey_;

    TRowBufferPtr RowBuffer_ = New<TRowBuffer>(TSchemalessChunkWriterTag());

    TColumnEvaluatorPtr ColumnEvaluator_;
    TSkynetColumnEvaluatorPtr SkynetColumnEvaluator_;

    // Maps global name table indexes into chunk name table indexes.
    std::vector<int> IdMapping_;

    // For duplicate id validation.
    SmallVector<i64, TypicalColumnCount> IdValidationMarks_;
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
                THROW_ERROR_EXCEPTION("Duplicate %Qv column in unversioned row",
                    NameTable_->GetName(id))
                     << TErrorAttribute("id", id);
            }
            idMark = mark;
        }
    }

    void ValidateSortAndUnique(const std::vector<TUnversionedRow>& rows)
    {
        if (!Schema_.IsSorted() || !Options_->ValidateSorted || rows.empty()) {
            return;
        }

        ValidateSortOrder(LastKey_.Get(), rows.front());

        for (int i = 1; i < rows.size(); ++i) {
            ValidateSortOrder(rows[i-1], rows[i]);
        }

        const auto& lastKey = rows.back();
        for (int i = 0; i < Schema_.GetKeyColumnCount(); ++i) {
            KeyBuilder_.AddValue(lastKey[i]);
        }
        LastKey_ = KeyBuilder_.FinishRow();
    }

    void ValidateSortOrder(TUnversionedRow lhs, TUnversionedRow rhs)
    {
        int cmp = CompareRows(lhs, rhs, Schema_.GetKeyColumnCount());
        if (cmp < 0) {
            return;
        }

        if (cmp == 0 && !Options_->ValidateUniqueKeys) {
            return;
        }

        // Output error.
        TUnversionedOwningRowBuilder leftBuilder, rightBuilder;
        for (int i = 0; i < Schema_.GetKeyColumnCount(); ++i) {
            leftBuilder.AddValue(lhs[i]);
            rightBuilder.AddValue(rhs[i]);
        }

        if (cmp == 0) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::UniqueKeyViolation,
                "Duplicate key %v",
                leftBuilder.FinishRow().Get());
        } else {
            if (Options_->ExplodeOnValidationError) {
                Y_UNREACHABLE();
            }

            THROW_ERROR_EXCEPTION(
                EErrorCode::SortOrderViolation,
                "Sort order violation: %v > %v",
                leftBuilder.FinishRow().Get(),
                rightBuilder.FinishRow().Get());
        }
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
        INativeClientPtr client,
        TCellTag cellTag,
        const TTransactionId& transactionId,
        const TChunkListId& parentChunkListId,
        TNameTablePtr nameTable,
        const TTableSchema& schema,
        IPartitionerPtr partitioner,
        TTrafficMeterPtr trafficMeter,
        IThroughputThrottlerPtr throttler,
        IBlockCachePtr blockCache)
        : TSchemalessMultiChunkWriterBase(
            config,
            options,
            client,
            cellTag,
            transactionId,
            parentChunkListId,
            nameTable,
            schema,
            TOwningKey(),
            trafficMeter,
            throttler,
            blockCache)
        , Partitioner_(partitioner)
        , BlockReserveSize_(std::max(config->MaxBufferSize / partitioner->GetPartitionCount() / 2, i64(1)))
    {
        Logger.AddTag("PartitionMultiChunkWriter: %s", TGuid::Create());

        int partitionCount = Partitioner_->GetPartitionCount();
        BlockWriters_.reserve(partitionCount);

        for (int partitionIndex = 0; partitionIndex < partitionCount; ++partitionIndex) {
            BlockWriters_.emplace_back(new THorizontalSchemalessBlockWriter(BlockReserveSize_));
            CurrentBufferCapacity_ += BlockWriters_.back()->GetCapacity();
        }

        ChunkWriterFactory_ = [=] (IChunkWriterPtr underlyingWriter) {
            return New<TPartitionChunkWriter>(
                config,
                options,
                std::move(underlyingWriter),
                blockCache,
                Schema_,
                partitionCount);
        };
    }

    virtual bool Write(const TRange<TUnversionedRow>& rows) override
    {
        YCHECK(!SwitchingSession_);

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
            LOG_WARNING(Error_, "Partition multi chunk writer failed");
            return false;
        }
    }

    virtual TFuture<void> Close() override
    {
        return BIND(&TPartitionMultiChunkWriter::DoClose, MakeStrong(this))
            .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
            .Run();
    }

private:
    const IPartitionerPtr Partitioner_;
    const i64 BlockReserveSize_;

    std::function<TPartitionChunkWriterPtr(IChunkWriterPtr)> ChunkWriterFactory_;

    THashSet<int> LargePartitons_;
    std::vector<std::unique_ptr<THorizontalSchemalessBlockWriter>> BlockWriters_;

    TNameTablePtr ChunkNameTable_;

    i64 CurrentBufferCapacity_ = 0;

    TPartitionChunkWriterPtr CurrentWriter_;

    TError Error_;

    virtual IChunkWriterBasePtr CreateTemplateWriter(IChunkWriterPtr underlyingWriter) override
    {
        CurrentWriter_ = ChunkWriterFactory_(std::move(underlyingWriter));
        // Since we form blocks outside chunk writer, we must synchronize name tables between different chunks.
        if (ChunkNameTable_) {
            for (int id = 0; id < ChunkNameTable_->GetSize(); ++id) {
                YCHECK(CurrentWriter_->GetNameTable()->GetIdOrRegisterName(ChunkNameTable_->GetName(id)) == id);
            }
        }
        ChunkNameTable_ = CurrentWriter_->GetNameTable();
        return CurrentWriter_;
    }

    virtual TNameTablePtr GetChunkNameTable() override
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
            LargePartitons_.insert(partitionIndex);
        }
    }

    bool DumpLargeBlocks()
    {
        bool readyForMore = true;
        for (auto partitionIndex : LargePartitons_) {
            readyForMore = FlushBlock(partitionIndex);
        }
        LargePartitons_.clear();

        while (CurrentBufferCapacity_ > Config_->MaxBufferSize) {
            i64 largestPartitonSize = -1;
            int largestPartitionIndex = -1;
            for (int partitionIndex = 0; partitionIndex < BlockWriters_.size(); ++partitionIndex) {
                auto& blockWriter = BlockWriters_[partitionIndex];
                if (blockWriter->GetBlockSize() > largestPartitonSize) {
                    largestPartitonSize = blockWriter->GetBlockSize();
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
        blockWriter.reset(new THorizontalSchemalessBlockWriter(BlockReserveSize_));
        CurrentBufferCapacity_ += blockWriter->GetCapacity();

        LOG_DEBUG("Flushing partition block (PartitonIndex: %v, BlockSize: %v, BlockRowCount: %v, CurrentBufferCapacity: %v)",
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
    const TTableSchema& schema,
    INativeClientPtr client,
    TCellTag cellTag,
    const TTransactionId& transactionId,
    const TChunkListId& parentChunkListId,
    IPartitionerPtr partitioner,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr throttler,
    IBlockCachePtr blockCache)
{
    auto writer = New<TPartitionMultiChunkWriter>(
        std::move(config),
        std::move(options),
        std::move(client),
        cellTag,
        transactionId,
        parentChunkListId,
        std::move(nameTable),
        schema,
        std::move(partitioner),
        std::move(trafficMeter),
        std::move(throttler),
        std::move(blockCache));

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
        INativeClientPtr client,
        TCellTag cellTag,
        const TTransactionId& transactionId,
        const TChunkListId& parentChunkListId,
        std::function<ISchemalessChunkWriterPtr(IChunkWriterPtr)> createChunkWriter,
        TNameTablePtr nameTable,
        const TTableSchema& schema,
        TOwningKey lastKey,
        TTrafficMeterPtr trafficMeter,
        IThroughputThrottlerPtr throttler,
        IBlockCachePtr blockCache)
        : TSchemalessMultiChunkWriterBase(
            config,
            options,
            client,
            cellTag,
            transactionId,
            parentChunkListId,
            nameTable,
            schema,
            lastKey,
            trafficMeter,
            throttler,
            blockCache)
        , CreateChunkWriter_(createChunkWriter)
    { }

    virtual bool Write(const TRange<TUnversionedRow>& rows) override
    {
        YCHECK(!SwitchingSession_);

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

    virtual TNameTablePtr GetChunkNameTable() override
    {
        return CurrentWriter_->GetNameTable();
    }

    virtual IChunkWriterBasePtr CreateTemplateWriter(IChunkWriterPtr underlyingWriter) override
    {
        CurrentWriter_ = CreateChunkWriter_(underlyingWriter);
        ResetIdMapping();
        return CurrentWriter_;
    }
};

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkWriterPtr CreateSchemalessMultiChunkWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    TNameTablePtr nameTable,
    const TTableSchema& schema,
    TOwningKey lastKey,
    INativeClientPtr client,
    TCellTag cellTag,
    const TTransactionId& transactionId,
    const TChunkListId& parentChunkListId,
    const TChunkTimestamps& chunkTimestamps,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr throttler,
    IBlockCachePtr blockCache)
{
    auto createChunkWriter = [=] (IChunkWriterPtr underlyingWriter) {
        return CreateSchemalessChunkWriter(
            config,
            options,
            schema,
            underlyingWriter,
            chunkTimestamps,
            blockCache);
    };

    auto writer = New<TSchemalessMultiChunkWriter>(
        config,
        options,
        client,
        cellTag,
        transactionId,
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

////////////////////////////////////////////////////////////////////////////////

class TSchemalessTableWriter
    : public ISchemalessWriter
    , public TTransactionListener
{
public:
    TSchemalessTableWriter(
        NLogging::TLogger logger,
        TTableWriterConfigPtr config,
        const TRichYPath& richPath,
        TNameTablePtr nameTable,
        INativeClientPtr client,
        ITransactionPtr uploadTransaction,
        ITransactionPtr transaction,
        ISchemalessMultiChunkWriterPtr underlyingWriter,
        TTableUploadOptions tableUploadOptions,
        TObjectId objectId)
        : Logger(std::move(logger))
        , RichPath_(richPath)
        , NameTable_(nameTable)
        , Client_(client)
        , UploadTransaction_(uploadTransaction)
        , Transaction_(transaction)
        , UnderlyingWriter_(underlyingWriter)
        , TransactionId_(transaction ? transaction->GetId() : NullTransactionId)
        , TableUploadOptions_(tableUploadOptions)
        , ObjectId_(objectId)
    {
        ListenTransaction(UploadTransaction_);

        if (Transaction_) {
            ListenTransaction(Transaction_);
        }
    }

    virtual bool Write(const TRange<TUnversionedRow>& rows) override
    {
        YCHECK(UnderlyingWriter_);
        if (IsAborted()) {
            return false;
        }

        return UnderlyingWriter_->Write(rows);
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        if (IsAborted()) {
            return MakeFuture(GetAbortError());
        }
        return UnderlyingWriter_->GetReadyEvent();
    }

    virtual TFuture<void> Close() override
    {
        return BIND(&TSchemalessTableWriter::DoClose, MakeStrong(this))
            .AsyncVia(NChunkClient::TDispatcher::Get()->GetWriterInvoker())
            .Run();
    }

    virtual const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    virtual const TTableSchema& GetSchema() const override
    {
        return TableUploadOptions_.TableSchema;
    }

private:
    NLogging::TLogger Logger;

    const TRichYPath RichPath_;
    const TNameTablePtr NameTable_;
    const INativeClientPtr Client_;
    const ITransactionPtr UploadTransaction_;
    const ITransactionPtr Transaction_;
    const ISchemalessMultiChunkWriterPtr UnderlyingWriter_;
    const TTransactionId TransactionId_;
    const TTableUploadOptions TableUploadOptions_;
    const TObjectId ObjectId_;

    void DoClose()
    {
        const auto& path = RichPath_.GetPath();
        auto objectIdPath = FromObjectId(ObjectId_);

        LOG_INFO("Closing table");
        {
            auto error = WaitFor(UnderlyingWriter_->Close());
            THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error closing chunk writer");
        }

        UploadTransaction_->Ping();
        UploadTransaction_->Detach();

        auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader);
        TObjectServiceProxy proxy(channel);

        auto batchReq = proxy.ExecuteBatch();

        {
            auto req = TTableYPathProxy::EndUpload(objectIdPath);
            *req->mutable_statistics() = UnderlyingWriter_->GetDataStatistics();
            ToProto(req->mutable_table_schema(), TableUploadOptions_.TableSchema);
            req->set_schema_mode(static_cast<int>(TableUploadOptions_.SchemaMode));
            req->set_optimize_for(static_cast<int>(TableUploadOptions_.OptimizeFor));
            req->set_compression_codec(static_cast<int>(TableUploadOptions_.CompressionCodec));
            req->set_erasure_codec(static_cast<int>(TableUploadOptions_.ErasureCodec));

            SetTransactionId(req, UploadTransaction_);
            GenerateMutationId(req);
            batchReq->AddRequest(req, "end_upload");
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            GetCumulativeError(batchRspOrError),
            "Error finishing upload to table %v",
            path);

        LOG_INFO("Table closed");
    }
};

////////////////////////////////////////////////////////////////////////////////

ISchemalessWriterPtr DoCreateSchemalessTableWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    const TRichYPath& richPath,
    TNameTablePtr nameTable,
    INativeClientPtr client,
    ITransactionPtr transaction,
    IThroughputThrottlerPtr throttler,
    IBlockCachePtr blockCache)
{
    auto Logger = TableClientLogger;

    TTransactionId transactionId = transaction ? transaction->GetId() : NullTransactionId;

    Logger.AddTag("Path: %v, TransactionId: %v",
        richPath.GetPath(),
        transactionId);

    TTableWriterConfigPtr writerConfig = CloneYsonSerializable(config);

    writerConfig->WorkloadDescriptor.Annotations.push_back(Format("TablePath: %v", richPath.GetPath()));

    const auto& path = richPath.GetPath();
        TUserObject userObject;
        userObject.Path = path;

    GetUserObjectBasicAttributes(
        client,
        TMutableRange<TUserObject>(&userObject, 1),
        transaction ? transaction->GetId() : NullTransactionId,
        Logger,
        EPermission::Write);

    if (userObject.Type != EObjectType::Table) {
        THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, actual %Qlv",
            path,
            EObjectType::Table,
            userObject.Type);
    }

    TObjectId objectId = userObject.ObjectId;
    TCellTag cellTag = userObject.CellTag;

    auto uploadMasterChannel = client->GetMasterChannelOrThrow(EMasterChannelKind::Leader, cellTag);
    auto objectIdPath = FromObjectId(objectId);

    TTableWriterOptionsPtr writerOptions = options;
    TTableUploadOptions tableUploadOptions;

    {
        LOG_INFO("Requesting extended table attributes");

        auto channel = client->GetMasterChannelOrThrow(EMasterChannelKind::Follower);
        TObjectServiceProxy proxy(channel);

        auto req = TCypressYPathProxy::Get(objectIdPath);
        SetTransactionId(req, transaction);
        std::vector<TString> attributeKeys{
            "account",
            "chunk_writer",
            "compression_codec",
            "dynamic",
            "erasure_codec",
            "optimize_for",
            "primary_medium",
            "replication_factor",
            "row_count",
            "schema",
            "schema_mode",
            "vital",
            "enable_skynet_sharing"
        };
        ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError,
            "Error requesting extended attributes of table %v",
            path);

        const auto& rsp = rspOrError.Value();
        auto node = ConvertToNode(TYsonString(rsp->value()));
        const auto& attributes = node->Attributes();

        if (attributes.Get<bool>("dynamic")) {
            THROW_ERROR_EXCEPTION("Write to dynamic table is not supported");
        }

        tableUploadOptions = GetTableUploadOptions(
            richPath,
            attributes,
            attributes.Get<i64>("row_count")
        );

        writerOptions->ReplicationFactor = attributes.Get<int>("replication_factor");
        writerOptions->MediumName = attributes.Get<TString>("primary_medium");
        writerOptions->CompressionCodec = tableUploadOptions.CompressionCodec;
        writerOptions->ErasureCodec = tableUploadOptions.ErasureCodec;
        writerOptions->Account = attributes.Get<TString>("account");
        writerOptions->ChunksVital = attributes.Get<bool>("vital");
        writerOptions->EnableSkynetSharing = attributes.Get<bool>("enable_skynet_sharing", false);
        writerOptions->ValidateSorted = tableUploadOptions.TableSchema.IsSorted();
        writerOptions->ValidateUniqueKeys = tableUploadOptions.TableSchema.GetUniqueKeys();
        writerOptions->OptimizeFor = tableUploadOptions.OptimizeFor;
        writerOptions->EvaluateComputedColumns = tableUploadOptions.TableSchema.HasComputedColumns();

        auto chunkWriterConfig = attributes.FindYson("chunk_writer");
        if (chunkWriterConfig) {
            ReconfigureYsonSerializable(writerConfig, chunkWriterConfig);
        }

        LOG_INFO("Extended attributes received (Account: %v, CompressionCodec: %v, ErasureCodec: %v)",
            writerOptions->Account,
            writerOptions->CompressionCodec,
            writerOptions->ErasureCodec);
    }

    ITransactionPtr uploadTransaction;
    {
        LOG_INFO("Starting table upload");

        auto channel = client->GetMasterChannelOrThrow(EMasterChannelKind::Leader);
        TObjectServiceProxy proxy(channel);

        auto batchReq = proxy.ExecuteBatch();

        {
            auto req = TTableYPathProxy::BeginUpload(objectIdPath);
            req->set_update_mode(static_cast<int>(tableUploadOptions.UpdateMode));
            req->set_lock_mode(static_cast<int>(tableUploadOptions.LockMode));
            req->set_upload_transaction_title(Format("Upload to %v", path));
            req->set_upload_transaction_timeout(ToProto<i64>(writerConfig->UploadTransactionTimeout));
            SetTransactionId(req, transaction);
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

            TTransactionAttachOptions options;
            options.AutoAbort = true;

            uploadTransaction = client->AttachTransaction(uploadTransactionId, options);

            LOG_INFO("Table upload started (UploadTransactionId: %v)",
                uploadTransactionId);
        }
    }

    TOwningKey writerLastKey;
    TChunkListId chunkListId;

    {
        LOG_INFO("Requesting table upload parameters");

        auto channel = client->GetMasterChannelOrThrow(EMasterChannelKind::Follower, cellTag);
        TObjectServiceProxy proxy(channel);

        auto req =  TTableYPathProxy::GetUploadParams(objectIdPath);
        req->set_fetch_last_key(tableUploadOptions.UpdateMode == EUpdateMode::Append &&
            tableUploadOptions.TableSchema.IsSorted());

        SetTransactionId(req, uploadTransaction);

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError,
            "Error requesting upload parameters for table %v",
            path);

        const auto& rsp = rspOrError.Value();
        chunkListId = FromProto<TChunkListId>(rsp->chunk_list_id());
        auto lastKey = FromProto<TOwningKey>(rsp->last_key());
        if (lastKey) {
            YCHECK(lastKey.GetCount() >= tableUploadOptions.TableSchema.GetKeyColumnCount());
            writerLastKey = TOwningKey(
                lastKey.Begin(),
                lastKey.Begin() + tableUploadOptions.TableSchema.GetKeyColumnCount());
        }

        LOG_INFO("Table upload parameters received (ChunkListId: %v, HasLastKey: %v)",
             chunkListId,
             static_cast<bool>(writerLastKey));
    }

    auto timestamp = WaitFor(client->GetNativeConnection()->GetTimestampProvider()->GenerateTimestamps())
        .ValueOrThrow();

    auto underlyingWriter = CreateSchemalessMultiChunkWriter(
        writerConfig,
        writerOptions,
        nameTable,
        tableUploadOptions.TableSchema,
        writerLastKey,
        client,
        cellTag,
        uploadTransaction->GetId(),
        chunkListId,
        TChunkTimestamps{timestamp, timestamp},
        /* trafficMeter */ nullptr,
        throttler,
        blockCache);

    LOG_INFO("Table opened");

    return New<TSchemalessTableWriter>(
        Logger,
        writerConfig,
        richPath,
        nameTable,
        std::move(client),
        std::move(uploadTransaction),
        std::move(transaction),
        std::move(underlyingWriter),
        tableUploadOptions,
        objectId);
}

TFuture<ISchemalessWriterPtr> CreateSchemalessTableWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    const TRichYPath& richPath,
    TNameTablePtr nameTable,
    INativeClientPtr client,
    ITransactionPtr transaction,
    IThroughputThrottlerPtr throttler,
    IBlockCachePtr blockCache)
{
    return BIND(&DoCreateSchemalessTableWriter,
        config,
        options,
        richPath,
        nameTable,
        client,
        transaction,
        throttler,
        blockCache)
        .AsyncVia(NChunkClient::TDispatcher::Get()->GetWriterInvoker())
        .Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
