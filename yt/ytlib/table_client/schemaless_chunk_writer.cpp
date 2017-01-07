#include "schemaless_chunk_writer.h"
#include "chunk_meta_extensions.h"
#include "config.h"
#include "name_table.h"
#include "partitioner.h"
#include "schemaless_block_writer.h"
#include "schemaless_row_reorderer.h"
#include "table_ypath_proxy.h"
#include "helpers.h"

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

#include <yt/ytlib/api/transaction.h>
#include <yt/ytlib/api/config.h>

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

using NYT::ToProto;
using NYT::FromProto;

static const i64 PartitionRowCountThreshold = (i64)100 * 1000;
static const i64 PartitionRowCountLimit = std::numeric_limits<i32>::max() - PartitionRowCountThreshold;

////////////////////////////////////////////////////////////////////////////////

const i64 MinRowRangeDataWeight = (i64) 64 * 1024;

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
        const TTableSchema& schema)
        : Logger(TableClientLogger)
          , Schema_(schema)
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

    virtual TFuture<void> Open() override
    {
        return VoidFuture;
    }

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

    virtual i64 GetDataSize() const override
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
        return GetMasterMeta();
    }

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        auto dataStatistics = EncodingChunkWriter_->GetDataStatistics();
        dataStatistics.set_row_count(RowCount_);
        return dataStatistics;
    }

    virtual TNameTablePtr GetNameTable() const override
    {
        return ChunkNameTable_;
    }

    virtual const TTableSchema& GetSchema() const override
    {
        return Schema_;
    }

protected:
    NLogging::TLogger Logger;
    const TTableSchema Schema_;

    TNameTablePtr ChunkNameTable_;

    const TChunkWriterConfigPtr Config_;
    const TChunkWriterOptionsPtr Options_;

    i64 RowCount_ = 0;
    i64 DataWeight_ = 0;

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

        block.Meta.set_block_index(BlockMetaExt_.blocks_size());

        BlockMetaExtSize_ += block.Meta.ByteSize();
        BlockMetaExt_.add_blocks()->Swap(&block.Meta);

        EncodingChunkWriter_->WriteBlock(std::move(block.Data));
    }

    void ProcessRowset(const std::vector<TUnversionedRow>& rows)
    {
        if (rows.empty()) {
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

        auto& meta = EncodingChunkWriter_->Meta();
        FillCommonMeta(&meta);

        auto nameTableExt = ToProto<TNameTableExt>(ChunkNameTable_);
        SetProtoExtension(meta.mutable_extensions(), nameTableExt);

        auto schemaExt = ToProto<TTableSchemaExt>(Schema_);
        SetProtoExtension(meta.mutable_extensions(), schemaExt);

        SetProtoExtension(meta.mutable_extensions(), BlockMetaExt_);

        SetProtoExtension(meta.mutable_extensions(), SamplesExt_);

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

    void ValidateRowWeight(i64 weight)
    {
        if (!Options_->ValidateRowWeight || weight < Config_->MaxRowWeight) {
            return;
        }

        THROW_ERROR_EXCEPTION(EErrorCode::RowWeightLimitExceeded, "Row weight is too large")
            << TErrorAttribute("row_weight", weight)
            << TErrorAttribute("row_weight_limit", Config_->MaxRowWeight);
    }

private:
    i64 BlockMetaExtSize_ = 0;

    NProto::TBoundaryKeysExt BoundaryKeysExt_;

    TRandomGenerator RandomGenerator_;
    const ui64 SamplingThreshold_;
    NProto::TSamplesExt SamplesExt_;
    i64 SamplesExtSize_ = 0;


    void FillCommonMeta(NChunkClient::NProto::TChunkMeta* meta) const
    {
        meta->set_type(static_cast<int>(EChunkType::Table));
        meta->set_version(static_cast<int>(GetTableChunkFormat()));
    }

    void EmitRandomSamples(const std::vector<TUnversionedRow>& rows)
    {
        for (auto row : rows) {
            if (RandomGenerator_.Generate<ui64>() < SamplingThreshold_) {
                EmitSample(row);
            }
        }

        if (SamplesExtSize_ == 0) {
            EmitSample(rows.front());
        }
    }

    void CaptureBoundaryKeys(const std::vector<TUnversionedRow>& rows)
    {
        if (!BoundaryKeysExt_.has_min()) {
            ToProto(
                BoundaryKeysExt_.mutable_min(),
                rows.front().Begin(),
                rows.front().Begin() + Schema_.GetKeyColumnCount());
        }

        LastKey_ = TOwningKey(rows.back().Begin(), rows.back().Begin() + Schema_.GetKeyColumnCount());
    }

    void EmitSample(TUnversionedRow row)
    {
        SmallVector<TUnversionedValue, TypicalColumnCount> sampleValues;
        int weight = 0;
        for (auto it = row.Begin(); it != row.End(); ++it) {
            sampleValues.push_back(*it);
            auto& value = sampleValues.back();

            switch (value.Type) {
                case EValueType::Any:
                    weight += value.Length;
                    // Composite types are non-comparable, so we don't store it inside samples.
                    value.Length = 0;
                    break;

                case EValueType::String:
                    weight += value.Length;
                    value.Length = std::min(static_cast<int>(value.Length), MaxSampleSize);
                    break;

                case EValueType::Int64:
                case EValueType::Uint64:
                case EValueType::Double:
                    weight += 8;
                    break;

                case EValueType::Boolean:
                case EValueType::Null:
                    weight += 1;
                    break;

                default:
                    YUNREACHABLE();
            };
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
        const TTableSchema& schema)
        : TUnversionedChunkWriterBase(
            config,
            options,
            chunkWriter,
            blockCache,
            schema)
    {
        Logger.AddTag("HorizontalUnversionedChunkWriter: %p", this);
        BlockWriter_.reset(new THorizontalSchemalessBlockWriter);
    }

    virtual i64 GetDataSize() const override
    {
        return TUnversionedChunkWriterBase::GetDataSize() +
           (BlockWriter_ ? BlockWriter_->GetBlockSize() : 0);
    }

    virtual bool Write(const std::vector<TUnversionedRow>& rows) override
    {
        for (auto row : rows) {
            i64 weight = GetDataWeight(row);
            ValidateRowWeight(weight);
            DataWeight_ += weight;

            ++RowCount_;
            BlockWriter_->WriteRow(row);

            if (BlockWriter_->GetBlockSize() >= Config_->BlockSize) {
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
        const TTableSchema& schema)
        : TUnversionedChunkWriterBase(
            config,
            options,
            chunkWriter,
            blockCache,
            schema)
        , DataToBlockFlush_(Config_->BlockSize)
    {
        Logger.AddTag("ColumnUnversionedChunkWriter: %p", this);

        // Only scan-optimized version for now.
        yhash_map<Stroka, TDataBlockWriter*> groupBlockWriters;
        for (const auto& column : Schema_.Columns()) {
            if (column.Group && groupBlockWriters.find(*column.Group) == groupBlockWriters.end()) {
                auto blockWriter = std::make_unique<TDataBlockWriter>();
                groupBlockWriters[*column.Group] = blockWriter.get();
                BlockWriters_.emplace_back(std::move(blockWriter));
            }
        }

        auto getBlockWriter = [&] (const NTableClient::TColumnSchema& columnSchema) -> TDataBlockWriter* {
            if (columnSchema.Group) {
                return groupBlockWriters[*columnSchema.Group];
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

    virtual bool Write(const std::vector<TUnversionedRow>& rows) override
    {
        auto* data = const_cast<TUnversionedRow*>(rows.data());

        int startRowIndex = 0;
        while (startRowIndex < rows.size()) {
            i64 weight = 0;
            int rowIndex = startRowIndex;
            for (; rowIndex < rows.size() && weight < DataToBlockFlush_; ++rowIndex) {
                auto rowWeight = GetDataWeight(rows[rowIndex]);
                ValidateRowWeight(rowWeight);
                DataWeight_ += rowWeight;
                weight += rowWeight;
            }

            auto range = MakeRange(data + startRowIndex, data + rowIndex);
            for (auto& columnWriter : ValueColumnWriters_) {
                columnWriter->WriteUnversionedValues(range);
            }

            RowCount_ += range.Size();
            DataWeight_ += weight;

            startRowIndex = rowIndex;

            TryFlushBlock(rows[rowIndex - 1]);
        }

        ProcessRowset(rows);

        return EncodingChunkWriter_->IsReady();
    }

    virtual i64 GetDataSize() const override
    {
        i64 result = TUnversionedChunkWriterBase::GetDataSize();
        for (const auto& blockWriter : BlockWriters_) {
            result += blockWriter->GetCurrentSize();
        }
        return result;
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

            if (totalSize > Config_->MaxBufferSize || maxWriterSize > Config_->BlockSize) {
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
    IBlockCachePtr blockCache)
{
    if (options->OptimizeFor == EOptimizeFor::Lookup) {
        return New<TSchemalessChunkWriter>(
            config,
            options,
            chunkWriter,
            blockCache,
            schema);
    } else {
        return New<TColumnUnversionedChunkWriter>(
            config,
            options,
            chunkWriter,
            blockCache,
            schema);
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
        IPartitionerPtr partitioner)
        : TUnversionedChunkWriterBase(
            config,
            options,
            chunkWriter,
            blockCache,
            schema)
        , Partitioner_(partitioner)
    {
        Logger.AddTag("PartitionChunkWriter: %p", this);

        int partitionCount = Partitioner_->GetPartitionCount();
        BlockWriters_.reserve(partitionCount);

        BlockReserveSize_ = Config_->MaxBufferSize / partitionCount;

        for (int partitionIndex = 0; partitionIndex < partitionCount; ++partitionIndex) {
            BlockWriters_.emplace_back(new THorizontalSchemalessBlockWriter(BlockReserveSize_));
            CurrentBufferCapacity_ += BlockWriters_.back()->GetCapacity();
        }

        PartitionsExt_.mutable_row_counts()->Resize(partitionCount, 0);
        PartitionsExt_.mutable_uncompressed_data_sizes()->Resize(partitionCount, 0);
    }

    virtual bool Write(const std::vector<TUnversionedRow>& rows) override
    {
        for (auto row : rows) {
            WriteRow(row);
        }

        return EncodingChunkWriter_->IsReady();
    }

    virtual i64 GetDataSize() const override
    {
        return TUnversionedChunkWriterBase::GetDataSize() + CurrentBufferCapacity_;
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
            2 * sizeof(i64) * BlockWriters_.size();
    }

private:
    const IPartitionerPtr Partitioner_;

    TPartitionsExt PartitionsExt_;

    std::vector<std::unique_ptr<THorizontalSchemalessBlockWriter>> BlockWriters_;

    i64 CurrentBufferCapacity_ = 0;

    int LargestPartitionIndex_ = 0;
    i64 LargestPartitionSize_ = 0;

    i64 LargestPartitionRowCount_ = 0;

    i64 BlockReserveSize_;
    i64 FlushedRowCount_ = 0;


    virtual ETableChunkFormat GetTableChunkFormat() const override
    {
        return ETableChunkFormat::SchemalessHorizontal;
    }

    virtual bool SupportBoundaryKeys() const
    {
        return false;
    }

    void WriteRow(TUnversionedRow row)
    {
        ++RowCount_;

        i64 weight = GetDataWeight(row);
        ValidateRowWeight(weight);
        DataWeight_ += weight;

        auto partitionIndex = Partitioner_->GetPartitionIndex(row);
        auto& blockWriter = BlockWriters_[partitionIndex];

        CurrentBufferCapacity_ -= blockWriter->GetCapacity();
        i64 oldSize = blockWriter->GetBlockSize();

        blockWriter->WriteRow(row);

        CurrentBufferCapacity_ += blockWriter->GetCapacity();
        i64 newSize = blockWriter->GetBlockSize();

        auto newRowCount = PartitionsExt_.row_counts(partitionIndex) + 1;
        auto newUncompressedDataSize = PartitionsExt_.uncompressed_data_sizes(partitionIndex) + newSize - oldSize;

        PartitionsExt_.set_row_counts(partitionIndex, newRowCount);
        PartitionsExt_.set_uncompressed_data_sizes(partitionIndex, newUncompressedDataSize);

        LargestPartitionRowCount_ = std::max(newRowCount, LargestPartitionRowCount_);

        if (newSize > LargestPartitionSize_) {
            LargestPartitionIndex_ = partitionIndex;
            LargestPartitionSize_ = newSize;
        }

        if (LargestPartitionSize_ >= Config_->BlockSize || CurrentBufferCapacity_ >= Config_->MaxBufferSize) {
            CurrentBufferCapacity_ -= BlockWriters_[LargestPartitionIndex_]->GetCapacity();

            FlushBlock(LargestPartitionIndex_);
            BlockWriters_[LargestPartitionIndex_].reset(new THorizontalSchemalessBlockWriter(BlockReserveSize_));
            CurrentBufferCapacity_ += BlockWriters_[LargestPartitionIndex_]->GetCapacity();

            InitLargestPartition();
        }
    }

    void InitLargestPartition()
    {
        LargestPartitionIndex_ = 0;
        LargestPartitionSize_ = BlockWriters_.front()->GetBlockSize();
        for (int partitionIndex = 1; partitionIndex < BlockWriters_.size(); ++partitionIndex) {
            auto& blockWriter = BlockWriters_[partitionIndex];
            if (blockWriter->GetBlockSize() > LargestPartitionSize_) {
                LargestPartitionSize_ = blockWriter->GetBlockSize();
                LargestPartitionIndex_ = partitionIndex;
            }
        }
    }

    void FlushBlock(int partitionIndex)
    {
        auto& blockWriter = BlockWriters_[partitionIndex];
        auto block = blockWriter->FlushBlock();
        block.Meta.set_partition_index(partitionIndex);
        FlushedRowCount_ += block.Meta.row_count();
        block.Meta.set_chunk_row_count(FlushedRowCount_);

        // Don't store last block keys in partition chunks.
        RegisterBlock(block, TUnversionedRow());
    }

    virtual void DoClose() override
    {
        for (int partitionIndex = 0; partitionIndex < BlockWriters_.size(); ++partitionIndex) {
            if (BlockWriters_[partitionIndex]->GetRowCount() > 0) {
                FlushBlock(partitionIndex);
            }
        }

        TUnversionedChunkWriterBase::DoClose();
    }

    virtual void PrepareChunkMeta() override
    {
        TUnversionedChunkWriterBase::PrepareChunkMeta();

        LOG_DEBUG("Partition totals: %v", PartitionsExt_.DebugString());

        auto& meta = EncodingChunkWriter_->Meta();

        SetProtoExtension(meta.mutable_extensions(), PartitionsExt_);
    }
};

////////////////////////////////////////////////////////////////////////////////

ISchemalessChunkWriterPtr CreatePartitionChunkWriter(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    const TTableSchema& schema,
    IChunkWriterPtr chunkWriter,
    IPartitionerPtr partitioner,
    IBlockCachePtr blockCache)
{
    return New<TPartitionChunkWriter>(
        config,
        options,
        chunkWriter,
        blockCache,
        schema,
        partitioner);
}

////////////////////////////////////////////////////////////////////////////////

class TSchemalessMultiChunkWriter
    : public TNontemplateMultiChunkWriterBase
    , public ISchemalessMultiChunkWriter
{
public:
    TSchemalessMultiChunkWriter(
        TMultiChunkWriterConfigPtr config,
        TTableWriterOptionsPtr options,
        IClientPtr client,
        TCellTag cellTag,
        const TTransactionId& transactionId,
        const TChunkListId& parentChunkListId,
        std::function<ISchemalessChunkWriterPtr(IChunkWriterPtr)> createChunkWriter,
        TNameTablePtr nameTable,
        const TTableSchema& schema,
        TOwningKey lastKey,
        IThroughputThrottlerPtr throttler,
        IBlockCachePtr blockCache)
        : TNontemplateMultiChunkWriterBase(
            config,
            options,
            client,
            cellTag,
            transactionId,
            parentChunkListId,
            throttler,
            blockCache)
        , CreateChunkWriter_(createChunkWriter)
        , Options_(options)
        , NameTable_(nameTable)
        , Schema_(schema)
        , LastKey_(lastKey)
    { }

    bool Write(const std::vector<TUnversionedRow>& rows) override
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

    TFuture<void> GetReadyEvent() override
    {
        if (Error_.IsOK()) {
            return TNontemplateMultiChunkWriterBase::GetReadyEvent();
        } else {
            return MakeFuture(Error_);
        }
    }

    virtual TNameTablePtr GetNameTable() const override
    {
        return NameTable_;
    }

    virtual const TTableSchema& GetSchema() const override
    {
        return Schema_;
    }

private:
    const std::function<ISchemalessChunkWriterPtr(IChunkWriterPtr)> CreateChunkWriter_;

    const TTableWriterOptionsPtr Options_;
    const TNameTablePtr NameTable_;
    const TTableSchema Schema_;

    ISchemalessChunkWriterPtr CurrentWriter_;

    TUnversionedOwningRowBuilder KeyBuilder_;
    TOwningKey LastKey_;
    TError Error_;

    TChunkedMemoryPool Pool_;

    // Maps global name table indexes into chunk name table indexes.
    std::vector<int> IdMapping_;

    // For duplicate id validation.
    SmallVector<i64, TypicalColumnCount> IdValidationMarks_;
    i64 CurrentIdValidationMark_ = 1;

    virtual IChunkWriterBasePtr CreateTemplateWriter(IChunkWriterPtr underlyingWriter) override
    {
        CurrentWriter_ = CreateChunkWriter_(underlyingWriter);
        IdMapping_.clear();
        return CurrentWriter_;
    }

    std::vector<TUnversionedRow> ReorderAndValidateRows(const std::vector<TUnversionedRow>& rows)
    {
        Pool_.Clear();

        std::vector<TUnversionedRow> result;
        result.reserve(rows.size());

        for (auto row : rows) {
            ValidateDuplicateIds(row);

            int maxColumnCount = Schema_.Columns().size() + (Schema_.GetStrict() ? 0 : row.GetCount());
            auto mutableRow = TMutableUnversionedRow::Allocate(&Pool_, maxColumnCount);
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
                    IdMapping_[valueIt->Id] = CurrentWriter_->GetNameTable()->GetIdOrRegisterName(NameTable_->GetName(valueIt->Id));
                }

                int id = IdMapping_[valueIt->Id];
                if (id < Schema_.Columns().size()) {
                    // Validate schema column types.
                    const auto& column = Schema_.Columns()[id];
                    if (valueIt->Type != column.Type &&
                        valueIt->Type != EValueType::Null &&
                        column.Type != EValueType::Any)
                    {
                        THROW_ERROR_EXCEPTION(
                            EErrorCode::SchemaViolation,
                            "Invalid type of column %Qv: expected %Qlv or %Qlv but got %Qlv",
                            column.Name,
                            column.Type,
                            EValueType::Null,
                            valueIt->Type);
                    }

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

            ValidateColumnCount(columnCount);
            mutableRow.SetCount(columnCount);

            result.push_back(mutableRow);
        }

        ValidateSortAndUnique(result);
        return result;
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
                THROW_ERROR_EXCEPTION("Duplicate column in unversioned row")
                     << TErrorAttribute("id", id)
                     << TErrorAttribute("column_name", NameTable_->GetName(id));
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
                "Duplicated key: %v",
                leftBuilder.FinishRow().Get());
        } else {
            if (Options_->ExplodeOnValidationError) {
                YUNREACHABLE();
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

ISchemalessMultiChunkWriterPtr CreateSchemalessMultiChunkWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    TNameTablePtr nameTable,
    const TTableSchema& schema,
    TOwningKey lastKey,
    IClientPtr client,
    TCellTag cellTag,
    const TTransactionId& transactionId,
    const TChunkListId& parentChunkListId,
    IThroughputThrottlerPtr throttler,
    IBlockCachePtr blockCache)
{
    auto createChunkWriter = [=] (IChunkWriterPtr underlyingWriter) {
        return CreateSchemalessChunkWriter(
            config,
            options,
            schema,
            underlyingWriter,
            blockCache);
    };

    return New<TSchemalessMultiChunkWriter>(
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
        throttler,
        blockCache);
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkWriterPtr CreatePartitionMultiChunkWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    TNameTablePtr nameTable,
    const TTableSchema& schema,
    IClientPtr client,
    TCellTag cellTag,
    const TTransactionId& transactionId,
    const TChunkListId& parentChunkListId,
    IPartitionerPtr partitioner,
    IThroughputThrottlerPtr throttler,
    IBlockCachePtr blockCache)
{
    auto createChunkWriter = [=] (IChunkWriterPtr underlyingWriter) {
        return CreatePartitionChunkWriter(
            config,
            options,
            schema,
            underlyingWriter,
            partitioner,
            blockCache);
    };

    return New<TSchemalessMultiChunkWriter>(
        config,
        options,
        client,
        cellTag,
        transactionId,
        parentChunkListId,
        createChunkWriter,
        nameTable,
        schema,
        TOwningKey(),
        throttler,
        blockCache);
}

////////////////////////////////////////////////////////////////////////////////

class TSchemalessTableWriter
    : public ISchemalessWriter
    , public TTransactionListener
{
public:
    TSchemalessTableWriter(
        TTableWriterConfigPtr config,
        TTableWriterOptionsPtr options,
        const TRichYPath& richPath,
        TNameTablePtr nameTable,
        IClientPtr client,
        ITransactionPtr transaction,
        IThroughputThrottlerPtr throttler,
        IBlockCachePtr blockCache)
        : Logger(TableClientLogger)
        , Config_(CloneYsonSerializable(config))
        , Options_(options)
        , RichPath_(richPath)
        , NameTable_(nameTable)
        , Client_(client)
        , Transaction_(transaction)
        , Throttler_(throttler)
        , BlockCache_(blockCache)
        , TransactionId_(transaction ? transaction->GetId() : NullTransactionId)
    {
        if (Transaction_) {
            ListenTransaction(Transaction_);
        }

        Config_->WorkloadDescriptor.Annotations.push_back(Format("TablePath: %v", RichPath_.GetPath()));

        Logger.AddTag("Path: %v, TransactionId: %v",
            RichPath_.GetPath(),
            TransactionId_);
    }

    virtual TFuture<void> Open() override
    {
        return BIND(&TSchemalessTableWriter::DoOpen, MakeStrong(this))
            .AsyncVia(NChunkClient::TDispatcher::Get()->GetWriterInvoker())
            .Run();
    }

    virtual bool Write(const std::vector<TUnversionedRow>& rows) override
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
            return MakeFuture(TError("Transaction %v aborted",
                 TransactionId_));
        }
        return UnderlyingWriter_->GetReadyEvent();
    }

    virtual TFuture<void> Close() override
    {
        return BIND(&TSchemalessTableWriter::DoClose, MakeStrong(this))
            .AsyncVia(NChunkClient::TDispatcher::Get()->GetWriterInvoker())
            .Run();
    }

    virtual TNameTablePtr GetNameTable() const override
    {
        return NameTable_;
    }

    virtual const TTableSchema& GetSchema() const override
    {
        return TableUploadOptions_.TableSchema;
    }

private:
    NLogging::TLogger Logger;

    const TTableWriterConfigPtr Config_;
    const TTableWriterOptionsPtr Options_;
    const TRichYPath RichPath_;
    const TNameTablePtr NameTable_;
    const IClientPtr Client_;
    const ITransactionPtr Transaction_;
    const IThroughputThrottlerPtr Throttler_;
    const IBlockCachePtr BlockCache_;

    TTableUploadOptions TableUploadOptions_;

    TTransactionId TransactionId_;

    TCellTag CellTag_ = InvalidCellTag;
    TObjectId ObjectId_;

    ITransactionPtr UploadTransaction_;
    TChunkListId ChunkListId_;

    TOwningKey LastKey_;

    ISchemalessMultiChunkWriterPtr UnderlyingWriter_;

    void DoOpen()
    {
        const auto& path = RichPath_.GetPath();
        TUserObject userObject;
        userObject.Path = path;

        GetUserObjectBasicAttributes(
            Client_,
            TMutableRange<TUserObject>(&userObject, 1),
            Transaction_ ? Transaction_->GetId() : NullTransactionId,
            Logger,
            EPermission::Write);

        ObjectId_ = userObject.ObjectId;
        CellTag_ = userObject.CellTag;

        if (userObject.Type != EObjectType::Table) {
            THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, actual %Qlv",
                path,
                EObjectType::Table,
                userObject.Type);
        }

        auto uploadMasterChannel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader, CellTag_);
        auto objectIdPath = FromObjectId(ObjectId_);

        {
            LOG_INFO("Requesting extended table attributes");

            auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Follower);
            TObjectServiceProxy proxy(channel);

            auto req = TCypressYPathProxy::Get(objectIdPath);
            SetTransactionId(req, Transaction_);
            std::vector<Stroka> attributeKeys{
                "account",
                "compression_codec",
                "erasure_codec",
                "schema_mode",
                "replication_factor",
                "row_count",
                "schema",
                "vital",
                "optimize_for"
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

            TableUploadOptions_ = GetTableUploadOptions(
                RichPath_,
                attributes.Get<TTableSchema>("schema"),
                attributes.Get<ETableSchemaMode>("schema_mode"),
                attributes.Get<i64>("row_count"));

            Options_->ReplicationFactor = attributes.Get<int>("replication_factor");
            Options_->CompressionCodec = attributes.Get<NCompression::ECodec>("compression_codec");
            Options_->ErasureCodec = attributes.Get<NErasure::ECodec>("erasure_codec");
            Options_->Account = attributes.Get<Stroka>("account");
            Options_->ChunksVital = attributes.Get<bool>("vital");
            Options_->ValidateSorted = TableUploadOptions_.TableSchema.IsSorted();
            Options_->ValidateUniqueKeys = TableUploadOptions_.TableSchema.GetUniqueKeys();
            Options_->OptimizeFor = attributes.Get<EOptimizeFor>("optimize_for", EOptimizeFor::Lookup);

            LOG_INFO("Extended attributes received (Account: %v, CompressionCodec: %v, ErasureCodec: %v)",
                Options_->Account,
                Options_->CompressionCodec,
                Options_->ErasureCodec);
        }

        {
            LOG_INFO("Starting table upload");

            auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader);
            TObjectServiceProxy proxy(channel);

            auto batchReq = proxy.ExecuteBatch();

            {
                auto req = TTableYPathProxy::BeginUpload(objectIdPath);
                req->set_update_mode(static_cast<int>(TableUploadOptions_.UpdateMode));
                req->set_lock_mode(static_cast<int>(TableUploadOptions_.LockMode));
                req->set_upload_transaction_title(Format("Upload to %v", path));
                req->set_upload_transaction_timeout(ToProto(Client_->GetConnection()->GetConfig()->TransactionManager->DefaultTransactionTimeout));
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

                TTransactionAttachOptions options;
                options.AutoAbort = true;

                UploadTransaction_ = Client_->AttachTransaction(uploadTransactionId, options);
                ListenTransaction(UploadTransaction_);

                LOG_INFO("Table upload started (UploadTransactionId: %v)",
                    uploadTransactionId);
            }
        }

        {
            LOG_INFO("Requesting table upload parameters");

            auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Follower, CellTag_);
            TObjectServiceProxy proxy(channel);

            auto req =  TTableYPathProxy::GetUploadParams(objectIdPath);
            req->set_fetch_last_key(TableUploadOptions_.UpdateMode == EUpdateMode::Append &&
                TableUploadOptions_.TableSchema.IsSorted());

            SetTransactionId(req, UploadTransaction_);

            auto rspOrError = WaitFor(proxy.Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(
                rspOrError,
                "Error requesting upload parameters for table %v",
                path);

            const auto& rsp = rspOrError.Value();
            ChunkListId_ = FromProto<TChunkListId>(rsp->chunk_list_id());
            auto lastKey = FromProto<TOwningKey>(rsp->last_key());
            if (lastKey) {
                YCHECK(lastKey.GetCount() >= TableUploadOptions_.TableSchema.GetKeyColumnCount());
                LastKey_ = TOwningKey(
                    lastKey.Begin(),
                    lastKey.Begin() + TableUploadOptions_.TableSchema.GetKeyColumnCount());
            }

            LOG_INFO("Table upload parameters received (ChunkListId: %v, HasLastKey: %v)",
                     ChunkListId_,
                     static_cast<bool>(LastKey_));
        }

        UnderlyingWriter_ = CreateSchemalessMultiChunkWriter(
            Config_,
            Options_,
            NameTable_,
            TableUploadOptions_.TableSchema,
            LastKey_,
            Client_,
            CellTag_,
            UploadTransaction_->GetId(),
            ChunkListId_,
            Throttler_,
            BlockCache_);

        WaitFor(UnderlyingWriter_->Open())
            .ThrowOnError();

        LOG_INFO("Table opened");
    }

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

ISchemalessWriterPtr CreateSchemalessTableWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    const TRichYPath& richPath,
    TNameTablePtr nameTable,
    IClientPtr client,
    ITransactionPtr transaction,
    IThroughputThrottlerPtr throttler,
    IBlockCachePtr blockCache)
{
    return New<TSchemalessTableWriter>(
        config,
        options,
        richPath,
        nameTable,
        client,
        transaction,
        throttler,
        blockCache);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
