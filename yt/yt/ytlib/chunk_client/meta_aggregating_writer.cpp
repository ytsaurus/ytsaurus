#include "meta_aggregating_writer.h"

#include "block.h"
#include "config.h"
#include "deferred_chunk_meta.h"
#include "chunk_meta_extensions.h"
#include "helpers.h"
#include "private.h"

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/helpers.h>
#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <google/protobuf/util/message_differencer.h>

namespace NYT::NChunkClient {

using namespace NTableClient;
using namespace NTableClient::NProto;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

const static THashSet<int> KnownExtensionTags = {
    TProtoExtensionTag<NProto::TMiscExt>::Value,
    TProtoExtensionTag<NProto::TBlocksExt>::Value,
    TProtoExtensionTag<NProto::TErasurePlacementExt>::Value,
    TProtoExtensionTag<NProto::TStripedErasurePlacementExt>::Value,
    TProtoExtensionTag<TDataBlockMetaExt>::Value,
    TProtoExtensionTag<TNameTableExt>::Value,
    TProtoExtensionTag<TBoundaryKeysExt>::Value,
    TProtoExtensionTag<TColumnMetaExt>::Value,
    TProtoExtensionTag<TTableSchemaExt>::Value,
    TProtoExtensionTag<TKeyColumnsExt>::Value,
    TProtoExtensionTag<TSamplesExt>::Value,
    TProtoExtensionTag<TColumnarStatisticsExt>::Value,
    TProtoExtensionTag<THeavyColumnStatisticsExt>::Value,
};

class TMetaAggregatingWriter
    : public IMetaAggregatingWriter
{
public:
    TMetaAggregatingWriter(
        IChunkWriterPtr underlyingWriter,
        TMetaAggregatingWriterOptionsPtr options)
        : UnderlyingWriter_(std::move(underlyingWriter))
        , ChunkMeta_(New<TDeferredChunkMeta>())
        , Options_(std::move(options))
        , Logger(ChunkClientLogger.WithTag("ChunkId: %v", UnderlyingWriter_->GetChunkId()))
    {
        MiscExt_.set_compression_codec(ToProto<int>(Options_->CompressionCodec));
        MiscExt_.set_erasure_codec(ToProto<int>(UnderlyingWriter_->GetErasureCodecId()));
        MiscExt_.set_shared_to_skynet(Options_->EnableSkynetSharing);
        if (Options_->TableSchema)  {
            MiscExt_.set_sorted(Options_->TableSchema->IsSorted());
            TableSchemaExt_ = ToProto<TTableSchemaExt>(Options_->TableSchema);
            SchemaComparator_ = Options_->TableSchema->ToComparator();
        }
    }

    TFuture<void> Open() override
    {
        return UnderlyingWriter_->Open();
    }

    bool WriteBlock(const TWorkloadDescriptor& workloadDescriptor, const TBlock& block) override
    {
        LargestBlockSize_ = std::max<i64>(LargestBlockSize_, block.Size());
        return UnderlyingWriter_->WriteBlock(workloadDescriptor, block);
    }

    bool WriteBlocks(
        const TWorkloadDescriptor& workloadDescriptor,
        const std::vector<TBlock>& blocks) override
    {
        for (const auto& block : blocks) {
            LargestBlockSize_ = std::max<i64>(LargestBlockSize_, block.Size());
        }
        return UnderlyingWriter_->WriteBlocks(workloadDescriptor, blocks);
    }

    TFuture<void> GetReadyEvent() override
    {
        return UnderlyingWriter_->GetReadyEvent();
    }

    TFuture<void> Close(
        const TWorkloadDescriptor& workloadDescriptor,
        const TDeferredChunkMetaPtr& /*chunkMeta*/ = nullptr) override
    {
        FinalizeMeta();
        return UnderlyingWriter_->Close(workloadDescriptor, ChunkMeta_);
    }

    const NProto::TChunkInfo& GetChunkInfo() const override
    {
        return UnderlyingWriter_->GetChunkInfo();
    }

    const NProto::TDataStatistics& GetDataStatistics() const override
    {
        return UnderlyingWriter_->GetDataStatistics();
    }

    TChunkReplicaWithLocationList GetWrittenChunkReplicas() const override
    {
        return UnderlyingWriter_->GetWrittenChunkReplicas();
    }

    TChunkId GetChunkId() const override
    {
        return UnderlyingWriter_->GetChunkId();
    }

    NErasure::ECodec GetErasureCodecId() const override
    {
        return UnderlyingWriter_->GetErasureCodecId();
    }

    bool IsCloseDemanded() const override
    {
        return UnderlyingWriter_->IsCloseDemanded();
    }

    void AbsorbMeta(const TDeferredChunkMetaPtr& meta, TChunkId chunkId) override;

    const TDeferredChunkMetaPtr& GetChunkMeta() const override
    {
        YT_VERIFY(MetaFinalized_);
        return ChunkMeta_;
    }

    TFuture<void> Cancel() override
    {
        return UnderlyingWriter_->Cancel();
    }

private:
    const IChunkWriterPtr UnderlyingWriter_;
    const TDeferredChunkMetaPtr ChunkMeta_;
    const TMetaAggregatingWriterOptionsPtr Options_;
    const NLogging::TLogger Logger;

    bool MetaInitialized_ = false;
    bool MetaFinalized_ = false;
    TChunkId FirstChunkId_;

    i64 RowCount_ = 0;
    i64 UncompressedDataSize_ = 0;
    i64 CompressedDataSize_ = 0;
    i64 DataWeight_ = 0;
    i64 LargestBlockSize_ = 0;
    i64 BlockIndex_ = 0;
    i64 ValueCount_ = 0;

    ui64 MinTimestamp_ = NullTimestamp;
    ui64 MaxTimestamp_ = NullTimestamp;

    TComparator SchemaComparator_;

    NProto::TMiscExt MiscExt_;

    TDataBlockMetaExt BlockMetaExt_;
    TNameTableExt NameTableExt_;

    std::optional<TBoundaryKeysExt> BoundaryKeysExt_;
    std::optional<TColumnMetaExt> ColumnMetaExt_;
    std::optional<TTableSchemaExt> TableSchemaExt_;
    std::optional<TKeyColumnsExt> KeyColumnsExt_;

    std::optional<TSamplesExt> SamplesExt_;
    std::optional<TColumnarStatistics> ColumnarStatistics_;

    template <typename T>
    static bool ExtensionEquals(const std::optional<T>& lhs, const std::optional<T>& rhs);

    void AbsorbFirstMeta(const TDeferredChunkMetaPtr& meta, TChunkId chunkId);
    void AbsorbAnotherMeta(const TDeferredChunkMetaPtr& meta, TChunkId chunkId);
    void FinalizeMeta();
};

DECLARE_REFCOUNTED_CLASS(TMetaAggregatingWriter)
DEFINE_REFCOUNTED_TYPE(TMetaAggregatingWriter)

////////////////////////////////////////////////////////////////////////////////

void TMetaAggregatingWriter::AbsorbMeta(const TDeferredChunkMetaPtr& meta, TChunkId chunkId)
{
    if (!Options_->AllowUnknownExtensions) {
        for (auto tag : GetExtensionTagSet(meta->extensions())) {
            if (!KnownExtensionTags.contains(tag)) {
                THROW_ERROR_EXCEPTION(
                    EErrorCode::IncompatibleChunkMetas,
                    "Chunk %v has unknown extension %v with tag %v",
                    chunkId,
                    FindExtensionName(tag),
                    tag);
            }
        }
    }

    if (!MetaInitialized_) {
        AbsorbFirstMeta(meta, chunkId);
        MetaInitialized_ = true;
        FirstChunkId_ = chunkId;
    } else {
        AbsorbAnotherMeta(meta, chunkId);
    }

    if (FindProtoExtension<TPartitionsExt>(meta->extensions())) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::IncompatibleChunkMetas,
            "Cannot absorb meta of partitioned chunk %v",
            chunkId);
    }

    if (!ExtensionEquals(TableSchemaExt_, FindProtoExtension<TTableSchemaExt>(meta->extensions()))) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::IncompatibleChunkMetas,
            "Chunks %v schema is different from output chunk schema",
            chunkId);
    }

    if (MiscExt_.sorted()) {
        auto boundaryKeysExt = FindProtoExtension<TBoundaryKeysExt>(meta->extensions());
        if (!boundaryKeysExt) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::IncompatibleChunkMetas,
                "Sorted chunk %v must have boundary keys extension",
                chunkId);
        }

        if (!BoundaryKeysExt_) {
            // First meta.
            BoundaryKeysExt_ = boundaryKeysExt;
        } else {
            auto currentMinRow = NYT::FromProto<TLegacyOwningKey>(boundaryKeysExt->min());
            auto previousMaxRow = NYT::FromProto<TLegacyOwningKey>(BoundaryKeysExt_->max());
            YT_VERIFY(SchemaComparator_.CompareKeys(TKey::FromRow(previousMaxRow), TKey::FromRow(currentMinRow)) <= 0);
            BoundaryKeysExt_->set_max(boundaryKeysExt->max());
        }
    }

    if (NYT::FromProto<EChunkType>(meta->type()) == EChunkType::Table) {
        auto samplesExt = FindProtoExtension<TSamplesExt>(meta->extensions());
        if (!samplesExt) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::IncompatibleChunkMetas,
                "Cannot absorb meta of a chunk %v without samples",
                chunkId);
        }
        if (!SamplesExt_) {
            // First meta.
            SamplesExt_ = samplesExt;
        } else {
            for (auto entry : samplesExt->entries()) {
                SamplesExt_->add_entries(entry);
            }
            for (auto weight : samplesExt->weights()) {
                SamplesExt_->add_weights(weight);
            }
        }

        auto columnarStatisticsExt = FindProtoExtension<TColumnarStatisticsExt>(meta->extensions());
        if (!columnarStatisticsExt) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::IncompatibleChunkMetas,
                "Cannot absorb meta of a chunk %v without columnar statistics",
                chunkId);
        }
        i64 chunkRowCount = GetProtoExtension<NProto::TMiscExt>(meta->extensions()).row_count();
        auto chunkColumnarStatistics = NYT::FromProto<TColumnarStatistics>(*columnarStatisticsExt, chunkRowCount);
        if (!ColumnarStatistics_) {
            // First meta.
            ColumnarStatistics_ = std::move(chunkColumnarStatistics);
        } else {
            if (ColumnarStatistics_->GetColumnCount() != chunkColumnarStatistics.GetColumnCount()) {
                THROW_ERROR_EXCEPTION(
                    EErrorCode::IncompatibleChunkMetas,
                    "Sizes of columnar statistics differ in chunks %v and %v",
                    FirstChunkId_,
                    chunkId)
                    << TErrorAttribute("previous", ColumnarStatistics_->GetColumnCount())
                    << TErrorAttribute("current", chunkColumnarStatistics.GetColumnCount());
            }
            *ColumnarStatistics_ += chunkColumnarStatistics;
        }
    }

    auto blockMetaExt = GetProtoExtension<TDataBlockMetaExt>(meta->extensions());
    for (const auto& block : blockMetaExt.data_blocks()) {
        if (MiscExt_.sorted() && !block.has_last_key()) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::IncompatibleChunkMetas,
                "No last key in a block of a sorted chunk %v",
                chunkId);
        }

        if (MiscExt_.sorted() && BlockMetaExt_.data_blocks_size() > 0) {
            const auto& lastBlock = *BlockMetaExt_.data_blocks().rbegin();
            YT_VERIFY(lastBlock.has_last_key() && block.has_last_key());
            auto columnCount = Options_->TableSchema->GetKeyColumnCount();
            auto lastRow = NYT::FromProto<TLegacyOwningKey>(lastBlock.last_key());
            auto row = NYT::FromProto<TLegacyOwningKey>(block.last_key());
            auto lastKey = TKey::FromRow(lastRow, columnCount);
            auto key = TKey::FromRow(row, columnCount);
            YT_VERIFY(Options_->TableSchema->ToComparator().CompareKeys(lastKey, key) <= 0);
        }

        auto* newBlock = BlockMetaExt_.add_data_blocks();
        ToProto(newBlock, block);
        newBlock->set_block_index(BlockIndex_++);
        newBlock->set_chunk_row_count(RowCount_ + newBlock->chunk_row_count());
    }

    auto miscExt = GetProtoExtension<NProto::TMiscExt>(meta->extensions());
    if (MiscExt_.sorted() && !miscExt.sorted()) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::IncompatibleChunkMetas,
            "Input chunk %v is not sorted",
            chunkId);
    }

    if (MiscExt_.compression_codec() != miscExt.compression_codec()) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::IncompatibleChunkMetas,
            "Chunk compression codec %v does not match options compression codec %v for chunk %v",
            MiscExt_.compression_codec(),
            miscExt.compression_codec(),
            chunkId);
    }

    i64 totalBlockCount = std::ssize(BlockMetaExt_.data_blocks());
    if (Options_->MaxBlockCount && totalBlockCount > *Options_->MaxBlockCount) {
        // NB. This error, in fact, doesn't mean that the metas are incompatible. It's used to indicate that
        // we cannot perform shallow merge of the given chunks, as the amount of blocks is too large, preventing
        // shallow merge jobs from running.
        THROW_ERROR_EXCEPTION(
            EErrorCode::IncompatibleChunkMetas,
            "Too many blocks")
            << TErrorAttribute("actual_total_block_count", totalBlockCount)
            << TErrorAttribute("max_allowed_total_block_count", *Options_->MaxBlockCount);
    }

    RowCount_ += miscExt.row_count();
    UncompressedDataSize_ += miscExt.uncompressed_data_size();
    CompressedDataSize_ += miscExt.compressed_data_size();
    DataWeight_ += miscExt.data_weight();
    ValueCount_ += miscExt.value_count();
    if (miscExt.has_min_timestamp()) {
        auto minTs = miscExt.min_timestamp();
        MinTimestamp_ = MinTimestamp_ == NullTimestamp ? minTs : std::min(minTs, MinTimestamp_);
    }
    if (miscExt.has_max_timestamp()) {
        auto maxTs = miscExt.max_timestamp();
        MaxTimestamp_ = MaxTimestamp_ == NullTimestamp ? maxTs : std::max(maxTs, MaxTimestamp_);
    }
}

template <typename T>
bool TMetaAggregatingWriter::ExtensionEquals(const std::optional<T>& lhs, const std::optional<T>& rhs) {
    if (lhs.has_value() != rhs.has_value()) {
        return false;
    }
    if (!lhs.has_value()) {
        return true;
    }
    return google::protobuf::util::MessageDifferencer::Equals(*lhs, *rhs);
}

void TMetaAggregatingWriter::AbsorbFirstMeta(const TDeferredChunkMetaPtr& meta, TChunkId /*chunkId*/)
{
    ChunkMeta_->set_type(meta->type());
    ChunkMeta_->set_format(meta->format());

    NameTableExt_ = GetProtoExtension<TNameTableExt>(meta->extensions());

    ColumnMetaExt_ = FindProtoExtension<TColumnMetaExt>(meta->extensions());
    KeyColumnsExt_ = FindProtoExtension<TKeyColumnsExt>(meta->extensions());
}

void TMetaAggregatingWriter::AbsorbAnotherMeta(const TDeferredChunkMetaPtr& meta, TChunkId chunkId)
{
    if (ChunkMeta_->type() != meta->type()) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::IncompatibleChunkMetas,
            "Meta types differ in chunks %v and %v",
            FirstChunkId_,
            chunkId)
            << TErrorAttribute("previous", NYT::FromProto<EChunkType>(ChunkMeta_->type()))
            << TErrorAttribute("current", NYT::FromProto<EChunkType>(meta->type()));
    }

    if (ChunkMeta_->format() != meta->format()) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::IncompatibleChunkMetas,
            "Meta formats differ in chunks %v and %v",
            FirstChunkId_,
            chunkId)
            << TErrorAttribute("previous", NYT::FromProto<EChunkFormat>(ChunkMeta_->format()))
            << TErrorAttribute("current", NYT::FromProto<EChunkFormat>(meta->format()));
    }

    auto nameTableExt = GetProtoExtension<TNameTableExt>(meta->extensions());
    if (!google::protobuf::util::MessageDifferencer::Equals(NameTableExt_, nameTableExt)) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::IncompatibleChunkMetas,
            "Name tables differ in chunks %v and %v",
            FirstChunkId_,
            chunkId);
    }

    if (!ExtensionEquals(KeyColumnsExt_, FindProtoExtension<TKeyColumnsExt>(meta->extensions()))) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::IncompatibleChunkMetas,
            "Key columns differ in chunks %v and %v",
            FirstChunkId_,
            chunkId);
    }

    auto columnMetaExt = FindProtoExtension<TColumnMetaExt>(meta->extensions());
    if (columnMetaExt.has_value() != ColumnMetaExt_.has_value()) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::IncompatibleChunkMetas,
            "Column metas differ in chunks %v and %v",
            FirstChunkId_,
            chunkId);
    }
    if (columnMetaExt) {
        if (columnMetaExt->columns_size() != ColumnMetaExt_->columns_size()) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::IncompatibleChunkMetas,
                "Columns size differ in chunks %v and %v",
                FirstChunkId_,
                chunkId);
        }

        for (int i = 0; i < columnMetaExt->columns_size(); ++i) {
            const auto& column = columnMetaExt->columns(i);
            auto* resultColumn = ColumnMetaExt_->mutable_columns(i);

            auto getLastSegmentRowCount = [&] () -> i64 {
                auto segmentsSize = resultColumn->segments_size();
                if (segmentsSize == 0) {
                    YT_LOG_ALERT(
                        "Previous chunk has no segment (ColumnIndex: %v, FirstChunkId: %v, CurrentChunkId: %v)",
                        i,
                        FirstChunkId_,
                        chunkId);
                    return 0;
                }
                const auto& lastSegment = resultColumn->segments(segmentsSize - 1);
                return lastSegment.chunk_row_count();
            };
            auto lastSegmentRowCount = getLastSegmentRowCount();

            for (const auto& segment : column.segments()) {
                auto* newSegment = resultColumn->add_segments();
                ToProto(newSegment, segment);
                newSegment->set_chunk_row_count(lastSegmentRowCount + newSegment->chunk_row_count());
                newSegment->set_block_index(BlockIndex_ + newSegment->block_index());
            }
        }
    }
}

void TMetaAggregatingWriter::FinalizeMeta()
{
    YT_VERIFY(MetaInitialized_);
    YT_VERIFY(!MetaFinalized_);

    SetProtoExtension(ChunkMeta_->mutable_extensions(), BlockMetaExt_);
    SetProtoExtension(ChunkMeta_->mutable_extensions(), NameTableExt_);
    if (ColumnMetaExt_) {
        SetProtoExtension(ChunkMeta_->mutable_extensions(), *ColumnMetaExt_);
    }
    if (TableSchemaExt_) {
        SetProtoExtension(ChunkMeta_->mutable_extensions(), *TableSchemaExt_);
    }
    if (KeyColumnsExt_) {
        SetProtoExtension(ChunkMeta_->mutable_extensions(), *KeyColumnsExt_);
    }
    if (BoundaryKeysExt_) {
        SetProtoExtension(ChunkMeta_->mutable_extensions(), *BoundaryKeysExt_);
    }
    if (SamplesExt_) {
        SetProtoExtension(ChunkMeta_->mutable_extensions(), *SamplesExt_);
    }
    if (ColumnarStatistics_) {
        SetProtoExtension(ChunkMeta_->mutable_extensions(), ToProto<TColumnarStatisticsExt>(*ColumnarStatistics_));
    }
    if (Options_->MaxHeavyColumns > 0 && ColumnarStatistics_) {
        auto heavyColumnStatisticsExt = GetHeavyColumnStatisticsExt(
            *ColumnarStatistics_,
            [&] (int columnIndex) {
                return TColumnStableName(TString{NameTableExt_.names(columnIndex)});
            },
            std::ssize(NameTableExt_.names()),
            Options_->MaxHeavyColumns);
        SetProtoExtension(ChunkMeta_->mutable_extensions(), std::move(heavyColumnStatisticsExt));
    }

    MiscExt_.set_row_count(RowCount_);
    MiscExt_.set_uncompressed_data_size(UncompressedDataSize_);
    MiscExt_.set_compressed_data_size(CompressedDataSize_);
    MiscExt_.set_data_weight(DataWeight_);
    MiscExt_.set_max_data_block_size(LargestBlockSize_);
    MiscExt_.set_meta_size(ChunkMeta_->ByteSize());
    MiscExt_.set_value_count(ValueCount_);
    if (MinTimestamp_ != NullTimestamp) {
        MiscExt_.set_min_timestamp(MinTimestamp_);
    }
    if (MaxTimestamp_ != NullTimestamp) {
        MiscExt_.set_max_timestamp(MaxTimestamp_);
    }
    if (TableSchemaExt_) {
        MiscExt_.set_unique_keys(TableSchemaExt_->unique_keys());
    }
    SetProtoExtension(ChunkMeta_->mutable_extensions(), MiscExt_);

    MetaFinalized_ = true;
}

////////////////////////////////////////////////////////////////////////////////

IMetaAggregatingWriterPtr CreateMetaAggregatingWriter(
    IChunkWriterPtr underlyingWriter,
    TMetaAggregatingWriterOptionsPtr options)
{
    return New<TMetaAggregatingWriter>(
        std::move(underlyingWriter),
        std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
