#include "versioned_chunk_writer.h"
#include "private.h"
#include "chunk_meta_extensions.h"
#include "config.h"
#include "unversioned_row.h"
#include "versioned_block_writer.h"
#include "versioned_writer.h"
#include "row_merger.h"
#include "row_buffer.h"

#include <yt/ytlib/api/native_client.h>
#include <yt/ytlib/api/native_connection.h>

#include <yt/ytlib/table_chunk_format/column_writer.h>
#include <yt/ytlib/table_chunk_format/data_block_writer.h>
#include <yt/ytlib/table_chunk_format/timestamp_writer.h>

#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/ytlib/chunk_client/encoding_chunk_writer.h>
#include <yt/ytlib/chunk_client/encoding_writer.h>
#include <yt/ytlib/chunk_client/multi_chunk_writer_base.h>

#include <yt/core/misc/range.h>
#include <yt/core/misc/random.h>

namespace NYT {
namespace NTableClient {

using namespace NTableChunkFormat;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NRpc;
using namespace NTransactionClient;
using namespace NObjectClient;
using namespace NApi;
using namespace NTableClient::NProto;

using NYT::TRange;

////////////////////////////////////////////////////////////////////////////////

static const i64 MinRowRangeDataWeight = 64_KB;

////////////////////////////////////////////////////////////////////////////////

class TVersionedChunkWriterBase
    : public IVersionedChunkWriter
{
public:
    TVersionedChunkWriterBase(
        TChunkWriterConfigPtr config,
        TChunkWriterOptionsPtr options,
        const TTableSchema& schema,
        IChunkWriterPtr chunkWriter,
        IBlockCachePtr blockCache)
        : Logger(NLogging::TLogger(TableClientLogger)
            .AddTag("ChunkWriterId: %v", TGuid::Create()))
        , Config_(config)
        , Schema_(schema)
        , EncodingChunkWriter_(New<TEncodingChunkWriter>(
            std::move(config),
            std::move(options),
            std::move(chunkWriter),
            std::move(blockCache),
            Logger))
        , LastKey_(static_cast<TUnversionedValue*>(nullptr), static_cast<TUnversionedValue*>(nullptr))
        , MinTimestamp_(MaxTimestamp)
        , MaxTimestamp_(MinTimestamp)
        , RandomGenerator_(RandomNumber<ui64>())
        , SamplingThreshold_(static_cast<ui64>(std::numeric_limits<ui64>::max() * Config_->SampleRate))
        , SamplingRowMerger_(New<TRowBuffer>(TVersionedChunkWriterBaseTag()), Schema_)
#if 0
        , KeyFilter_(Config_->MaxKeyFilterSize, Config_->KeyFilterFalsePositiveRate)
#endif
    { }

    virtual TFuture<void> GetReadyEvent() override
    {
        return EncodingChunkWriter_->GetReadyEvent();
    }

    virtual i64 GetRowCount() const override
    {
        return RowCount_;
    }

    virtual bool Write(const TRange<TVersionedRow>& rows) override
    {
        if (rows.Empty()) {
            return EncodingChunkWriter_->IsReady();
        }

        SamplingRowMerger_.Reset();

        if (RowCount_ == 0) {
            auto firstRow = rows.Front();
            ToProto(
                BoundaryKeysExt_.mutable_min(),
                TOwningKey(firstRow.BeginKeys(), firstRow.EndKeys()));
            EmitSample(firstRow);
        }

        DoWriteRows(rows);

        auto lastRow = rows.Back();
        LastKey_ = TOwningKey(lastRow.BeginKeys(), lastRow.EndKeys());

        return EncodingChunkWriter_->IsReady();
    }

    virtual TFuture<void> Close() override
    {
        // psushin@ forbids empty chunks :)
        YCHECK(RowCount_ > 0);

        return BIND(&TVersionedChunkWriterBase::DoClose, MakeStrong(this))
            .AsyncVia(NChunkClient::TDispatcher::Get()->GetWriterInvoker())
            .Run();
    }

    virtual i64 GetMetaSize() const override
    {
        // Other meta parts are negligible.
        return BlockMetaExtSize_ + SamplesExtSize_;
    }

    virtual bool IsCloseDemanded() const override
    {
        return false;
    }

    virtual TChunkMeta GetSchedulerMeta() const override
    {
        return GetMasterMeta();
    }

    virtual TChunkMeta GetNodeMeta() const override
    {
        return EncodingChunkWriter_->Meta();
    }

    virtual TChunkId GetChunkId() const override
    {
        return EncodingChunkWriter_->GetChunkId();
    }

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        return EncodingChunkWriter_->GetDataStatistics();
    }

    virtual TCodecStatistics GetCompressionStatistics() const override
    {
        return EncodingChunkWriter_->GetCompressionStatistics();
    }

    virtual i64 GetDataWeight() const override
    {
        return DataWeight_;
    }

protected:
    const NLogging::TLogger Logger;

    const TChunkWriterConfigPtr Config_;
    const TTableSchema Schema_;

    TEncodingChunkWriterPtr EncodingChunkWriter_;

    TOwningKey LastKey_;

    TBlockMetaExt BlockMetaExt_;
    i64 BlockMetaExtSize_ = 0;

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

#if 0
    TBloomFilterBuilder KeyFilter_;
#endif

    virtual void DoClose() = 0;
    virtual void DoWriteRows(const TRange<TVersionedRow>& rows) = 0;
    virtual ETableChunkFormat GetTableChunkFormat() const = 0;

    void FillCommonMeta(TChunkMeta* meta) const
    {
        meta->set_type(static_cast<int>(EChunkType::Table));
        meta->set_version(static_cast<int>(GetTableChunkFormat()));

        SetProtoExtension(meta->mutable_extensions(), BoundaryKeysExt_);
    }

    virtual void PrepareChunkMeta()
    {
        using NYT::ToProto;

        ToProto(BoundaryKeysExt_.mutable_max(), LastKey_);

        auto& meta = EncodingChunkWriter_->Meta();
        FillCommonMeta(&meta);

        SetProtoExtension(meta.mutable_extensions(), ToProto<TTableSchemaExt>(Schema_));
        SetProtoExtension(meta.mutable_extensions(), BlockMetaExt_);
        SetProtoExtension(meta.mutable_extensions(), SamplesExt_);

#if 0
        if (KeyFilter_.IsValid()) {
            KeyFilter_.Shrink();
            //FIXME: write bloom filter to chunk.
        }
#endif

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
        SamplesExtSize_ += SamplesExt_.entries(SamplesExt_.entries_size() - 1).length();
    }

    static void ValidateRowsOrder(
        TVersionedRow row,
        const TUnversionedValue* beginPrevKey,
        const TUnversionedValue* endPrevKey)
    {
        YCHECK(
            !beginPrevKey && !endPrevKey ||
            CompareRows(beginPrevKey, endPrevKey, row.BeginKeys(), row.EndKeys()) < 0);
    }

    static void ValidateRowDataWeight(TVersionedRow row, i64 dataWeight)
    {
        if (dataWeight > MaxServerVersionedRowDataWeight) {
            THROW_ERROR_EXCEPTION("Versioned row data weight is too large")
                << TErrorAttribute("key", RowToKey(row))
                << TErrorAttribute("actual_data_weight", dataWeight)
                << TErrorAttribute("max_data_weight", MaxServerVersionedRowDataWeight);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSimpleVersionedChunkWriter
    : public TVersionedChunkWriterBase
{
public:
    TSimpleVersionedChunkWriter(
        TChunkWriterConfigPtr config,
        TChunkWriterOptionsPtr options,
        const TTableSchema& schema,
        IChunkWriterPtr chunkWriter,
        IBlockCachePtr blockCache)
        : TVersionedChunkWriterBase(
            std::move(config),
            std::move(options),
            schema,
            std::move(chunkWriter),
            std::move(blockCache))
        , BlockWriter_(new TSimpleVersionedBlockWriter(Schema_))
    { }

    virtual i64 GetCompressedDataSize() const override
    {
        return EncodingChunkWriter_->GetDataStatistics().compressed_data_size() +
               (BlockWriter_ ? BlockWriter_->GetBlockSize() : 0);
    }

    virtual TChunkMeta GetMasterMeta() const override
    {
        TChunkMeta meta;
        FillCommonMeta(&meta);
        SetProtoExtension(meta.mutable_extensions(), EncodingChunkWriter_->MiscExt());
        return meta;
    }

private:
    std::unique_ptr<TSimpleVersionedBlockWriter> BlockWriter_;

    virtual void DoWriteRows(const TRange<TVersionedRow>& rows) override
    {
        if (rows.Empty()) {
            return;
        }

        //FIXME: insert key into bloom filter.
        //KeyFilter_.Insert(GetFarmFingerprint(rows.front().BeginKeys(), rows.front().EndKeys()));
        auto firstRow = rows.Front();

        WriteRow(firstRow, LastKey_.Begin(), LastKey_.End());
        FinishBlockIfLarge(firstRow);

        int rowCount = static_cast<int>(rows.Size());
        for (int index = 1; index < rowCount; ++index) {
            //KeyFilter_.Insert(GetFarmFingerprint(rows[i].BeginKeys(), rows[i].EndKeys()));
            WriteRow(rows[index], rows[index - 1].BeginKeys(), rows[index - 1].EndKeys());
            FinishBlockIfLarge(rows[index]);
        }
    }

    static void ValidateRow(
        TVersionedRow row,
        i64 dataWeight,
        const TUnversionedValue* beginPrevKey,
        const TUnversionedValue* endPrevKey)
    {
        ValidateRowsOrder(row, beginPrevKey, endPrevKey);
        ValidateRowDataWeight(row, dataWeight);

        if (row.GetWriteTimestampCount() > std::numeric_limits<ui16>::max()) {
            THROW_ERROR_EXCEPTION("Too many write timestamps in a versioned row")
                << TErrorAttribute("key", RowToKey(row));
        }
        if (row.GetDeleteTimestampCount() > std::numeric_limits<ui16>::max()) {
            THROW_ERROR_EXCEPTION("Too many delete timestamps in a versioned row")
                << TErrorAttribute("key", RowToKey(row));
        }
    }

    void WriteRow(
        TVersionedRow row,
        const TUnversionedValue* beginPreviousKey,
        const TUnversionedValue* endPreviousKey)
    {
        EmitSampleRandomly(row);
        auto rowWeight = NTableClient::GetDataWeight(row);

        ValidateRow(row, rowWeight, beginPreviousKey, endPreviousKey);

        ++RowCount_;
        DataWeight_ += rowWeight;
        BlockWriter_->WriteRow(row, beginPreviousKey, endPreviousKey);
    }

    void FinishBlockIfLarge(TVersionedRow row)
    {
        if (BlockWriter_->GetBlockSize() < Config_->BlockSize) {
            return;
        }

        FinishBlock(row.BeginKeys(), row.EndKeys());
        BlockWriter_.reset(new TSimpleVersionedBlockWriter(Schema_));
    }

    void FinishBlock(const TUnversionedValue* beginKey, const TUnversionedValue* endKey)
    {
        auto block = BlockWriter_->FlushBlock();
        block.Meta.set_chunk_row_count(RowCount_);
        block.Meta.set_block_index(BlockMetaExt_.blocks_size());
        ToProto(block.Meta.mutable_last_key(), beginKey, endKey);

        YCHECK(block.Meta.uncompressed_size() > 0);

        BlockMetaExtSize_ += block.Meta.ByteSize();

        BlockMetaExt_.add_blocks()->Swap(&block.Meta);
        EncodingChunkWriter_->WriteBlock(std::move(block.Data));

        MaxTimestamp_ = std::max(MaxTimestamp_, BlockWriter_->GetMaxTimestamp());
        MinTimestamp_ = std::min(MinTimestamp_, BlockWriter_->GetMinTimestamp());
    }

    virtual void DoClose() override
    {
        if (BlockWriter_->GetRowCount() > 0) {
            FinishBlock(LastKey_.Begin(), LastKey_.End());
        }

        PrepareChunkMeta();

        auto& miscExt = EncodingChunkWriter_->MiscExt();
        miscExt.set_min_timestamp(MinTimestamp_);
        miscExt.set_max_timestamp(MaxTimestamp_);

        EncodingChunkWriter_->Close();
    }

    virtual ETableChunkFormat GetTableChunkFormat() const override
    {
        return TSimpleVersionedBlockWriter::FormatVersion;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TColumnVersionedChunkWriter
    : public TVersionedChunkWriterBase
{
public:
    TColumnVersionedChunkWriter(
        TChunkWriterConfigPtr config,
        TChunkWriterOptionsPtr options,
        const TTableSchema& schema,
        IChunkWriterPtr chunkWriter,
        IBlockCachePtr blockCache)
        : TVersionedChunkWriterBase(
            std::move(config),
            std::move(options),
            schema,
            std::move(chunkWriter),
            std::move(blockCache))
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

        // Key columns.
        for (int keyColumnIndex = 0; keyColumnIndex < Schema_.GetKeyColumnCount(); ++keyColumnIndex) {
            const auto& column = Schema_.Columns()[keyColumnIndex];
            ValueColumnWriters_.emplace_back(CreateUnversionedColumnWriter(
                column,
                keyColumnIndex,
                getBlockWriter(column)));
        }

        // Non-key columns.
        for (
            int valueColumnIndex = Schema_.GetKeyColumnCount();
            valueColumnIndex < Schema_.Columns().size();
            ++valueColumnIndex)
        {
            const auto& column = Schema_.Columns()[valueColumnIndex];
            ValueColumnWriters_.emplace_back(CreateVersionedColumnWriter(
                column,
                valueColumnIndex,
                getBlockWriter(column)));
        }

        auto blockWriter = std::make_unique<TDataBlockWriter>();
        TimestampWriter_ = CreateTimestampWriter(blockWriter.get());
        BlockWriters_.emplace_back(std::move(blockWriter));

        YCHECK(BlockWriters_.size() > 1);
    }

    virtual i64 GetCompressedDataSize() const override
    {
        i64 result = EncodingChunkWriter_->GetDataStatistics().compressed_data_size();
        for (const auto& blockWriter : BlockWriters_) {
            result += blockWriter->GetCurrentSize();
        }
        return result;
    }

    virtual TChunkMeta GetMasterMeta() const override
    {
        TChunkMeta meta;
        FillCommonMeta(&meta);
        SetProtoExtension(meta.mutable_extensions(), EncodingChunkWriter_->MiscExt());
        return meta;
    }

    virtual i64 GetMetaSize() const override
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

    i64 DataToBlockFlush_;

    virtual void DoWriteRows(const TRange<TVersionedRow>& rows) override
    {
        int startRowIndex = 0;
        while (startRowIndex < rows.Size()) {
            i64 weight = 0;
            int rowIndex = startRowIndex;
            for (; rowIndex < rows.Size() && weight < DataToBlockFlush_; ++rowIndex) {
                auto rowWeight = NTableClient::GetDataWeight(rows[rowIndex]);
                if (rowIndex == 0) {
                    ValidateRow(rows[rowIndex], rowWeight, LastKey_.Begin(), LastKey_.End());
                } else {
                    ValidateRow(
                        rows[rowIndex],
                        rowWeight,
                        rows[rowIndex - 1].BeginKeys(),
                        rows[rowIndex - 1].EndKeys());
                }

                weight += rowWeight;
            }

            auto range = MakeRange(rows.Begin() + startRowIndex, rows.Begin() + rowIndex);
            for (const auto& columnWriter : ValueColumnWriters_) {
                columnWriter->WriteValues(range);
            }
            TimestampWriter_->WriteTimestamps(range);

            RowCount_ += range.Size();
            DataWeight_ += weight;

            startRowIndex = rowIndex;

            TryFlushBlock(rows[rowIndex - 1]);
        }

        for (auto row : rows) {
            EmitSampleRandomly(row);
        }
    }

    static void ValidateRow(
        TVersionedRow row,
        i64 dataWeight,
        const TUnversionedValue* beginPrevKey,
        const TUnversionedValue* endPrevKey)
    {
        ValidateRowsOrder(row, beginPrevKey, endPrevKey);
        ValidateRowDataWeight(row, dataWeight);
    }

    void TryFlushBlock(TVersionedRow lastRow)
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
                FinishBlock(maxWriterIndex, lastRow.BeginKeys(), lastRow.EndKeys());
            } else {
                DataToBlockFlush_ = std::min(Config_->MaxBufferSize - totalSize, Config_->BlockSize - maxWriterSize);
                DataToBlockFlush_ = std::max(MinRowRangeDataWeight, DataToBlockFlush_);

                break;
            }
        }
    }

    void FinishBlock(int blockWriterIndex, const TUnversionedValue* beginKey, const TUnversionedValue* endKey)
    {
        auto block = BlockWriters_[blockWriterIndex]->DumpBlock(BlockMetaExt_.blocks_size(), RowCount_);
        YCHECK(block.Meta.uncompressed_size() > 0);

        block.Meta.set_block_index(BlockMetaExt_.blocks_size());
        ToProto(block.Meta.mutable_last_key(), beginKey, endKey);

        BlockMetaExtSize_ += block.Meta.ByteSize();

        BlockMetaExt_.add_blocks()->Swap(&block.Meta);
        EncodingChunkWriter_->WriteBlock(std::move(block.Data));
    }

    virtual void DoClose() override
    {
        for (int i = 0; i < BlockWriters_.size(); ++i) {
            if (BlockWriters_[i]->GetCurrentSize() > 0) {
                FinishBlock(i, LastKey_.Begin(), LastKey_.End());
            }
        }

        PrepareChunkMeta();

        auto& miscExt = EncodingChunkWriter_->MiscExt();
        miscExt.set_min_timestamp(static_cast<i64>(TimestampWriter_->GetMinTimestamp()));
        miscExt.set_max_timestamp(static_cast<i64>(TimestampWriter_->GetMaxTimestamp()));

        auto& meta = EncodingChunkWriter_->Meta();

        NProto::TColumnMetaExt columnMetaExt;
        for (const auto& valueColumnWriter : ValueColumnWriters_) {
            *columnMetaExt.add_columns() = valueColumnWriter->ColumnMeta();
        }
        *columnMetaExt.add_columns() = TimestampWriter_->ColumnMeta();

        SetProtoExtension(meta.mutable_extensions(), columnMetaExt);

        EncodingChunkWriter_->Close();
    }

    virtual ETableChunkFormat GetTableChunkFormat() const override
    {
        return ETableChunkFormat::VersionedColumnar;
    }
};

////////////////////////////////////////////////////////////////////////////////

IVersionedChunkWriterPtr CreateVersionedChunkWriter(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    const TTableSchema& schema,
    IChunkWriterPtr chunkWriter,
    IBlockCachePtr blockCache)
{
    if (options->OptimizeFor == EOptimizeFor::Scan) {
        return New<TColumnVersionedChunkWriter>(
            std::move(config),
            std::move(options),
            schema,
            std::move(chunkWriter),
            std::move(blockCache));
    } else {
        return New<TSimpleVersionedChunkWriter>(
            std::move(config),
            std::move(options),
            schema,
            std::move(chunkWriter),
            std::move(blockCache));
    }
}

////////////////////////////////////////////////////////////////////////////////

IVersionedMultiChunkWriterPtr CreateVersionedMultiChunkWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    const TTableSchema& schema,
    INativeClientPtr client,
    TCellTag cellTag,
    const NTransactionClient::TTransactionId& transactionId,
    const TChunkListId& parentChunkListId,
    IThroughputThrottlerPtr throttler,
    IBlockCachePtr blockCache)
{
    typedef TMultiChunkWriterBase<
        IVersionedMultiChunkWriter,
        IVersionedChunkWriter,
        const TRange<TVersionedRow>&> TVersionedMultiChunkWriter;

    auto createChunkWriter = [=] (IChunkWriterPtr underlyingWriter) {
        return CreateVersionedChunkWriter(
            config,
            options,
            schema,
            underlyingWriter,
            blockCache);
    };

    auto writer = New<TVersionedMultiChunkWriter>(
        config,
        options,
        client,
        cellTag,
        transactionId,
        parentChunkListId,
        createChunkWriter,
        /* trafficMeter */ nullptr,
        throttler,
        blockCache);

    writer->Init();

    return writer;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
