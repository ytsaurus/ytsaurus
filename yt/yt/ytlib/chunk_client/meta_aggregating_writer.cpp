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

#include <yt/yt/ytlib/table_chunk_format/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_chunk_format/column_meta.h>

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
    TProtoExtensionTag<TLargeColumnarStatisticsExt>::Value,
};

// These extensions are set at a different level of writers, so the discrepancies in them should be ignored in chunk meta validation.
const static THashSet<int> ExtensionTagsToIgnoreInValidation = {
    TProtoExtensionTag<NProto::TBlocksExt>::Value,
    TProtoExtensionTag<NProto::TErasurePlacementExt>::Value,
};

////////////////////////////////////////////////////////////////////////////////

template <typename TParsedExtension, typename TProtobufExtension>
static std::optional<TParsedExtension> ParseOptionalProto(const std::optional<TProtobufExtension>& protobufStruct)
{
    std::optional<TParsedExtension> result;
    if (protobufStruct.has_value()) {
        result = NYT::FromProto<TParsedExtension>(*protobufStruct);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

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
        , Logger(ChunkClientLogger().WithTag("ChunkId: %v", UnderlyingWriter_->GetChunkId()))
    {
        MiscExt_.set_compression_codec(ToProto(Options_->CompressionCodec));
        MiscExt_.set_erasure_codec(ToProto(UnderlyingWriter_->GetErasureCodecId()));
        MiscExt_.set_shared_to_skynet(Options_->EnableSkynetSharing);
        if (Options_->TableSchema)  {
            MiscExt_.set_sorted(Options_->TableSchema->IsSorted());
            TableSchema_ = *Options_->TableSchema;
            SchemaComparator_ = Options_->TableSchema->ToComparator();
        }
    }

    TFuture<void> Open() override
    {
        return UnderlyingWriter_->Open();
    }

    bool WriteBlock(
        const IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const TBlock& block) override
    {
        LargestBlockSize_ = std::max<i64>(LargestBlockSize_, block.Size());
        return UnderlyingWriter_->WriteBlock(options, workloadDescriptor, block);
    }

    bool WriteBlocks(
        const IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const std::vector<TBlock>& blocks) override
    {
        for (const auto& block : blocks) {
            LargestBlockSize_ = std::max<i64>(LargestBlockSize_, block.Size());
        }
        return UnderlyingWriter_->WriteBlocks(options, workloadDescriptor, blocks);
    }

    TFuture<void> GetReadyEvent() override
    {
        return UnderlyingWriter_->GetReadyEvent();
    }

    TFuture<void> Close(
        const IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const TDeferredChunkMetaPtr& /*chunkMeta*/ = nullptr) override
    {
        if (!MetaFinalized_) {
            FinalizeMeta();
        }

        return UnderlyingWriter_->Close(options, workloadDescriptor, ChunkMeta_);
    }

    const NProto::TChunkInfo& GetChunkInfo() const override
    {
        return UnderlyingWriter_->GetChunkInfo();
    }

    const NProto::TDataStatistics& GetDataStatistics() const override
    {
        return UnderlyingWriter_->GetDataStatistics();
    }

    TWrittenChunkReplicasInfo GetWrittenChunkReplicasInfo() const override
    {
        return UnderlyingWriter_->GetWrittenChunkReplicasInfo();
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

    void AbsorbMeta(const TDeferredChunkMetaPtr& meta, TChunkId chunkId) override
    {
        InputChunkMetaExtensions_ = GetExtensionTagSet(meta->extensions());

        if (!Options_->AllowUnknownExtensions) {
            for (auto tag : InputChunkMetaExtensions_) {
                if (!KnownExtensionTags.contains(tag)) {
                    THROW_ERROR_EXCEPTION(NChunkClient::EErrorCode::IncompatibleChunkMetas,
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
            THROW_ERROR_EXCEPTION(NChunkClient::EErrorCode::IncompatibleChunkMetas,
                "Cannot absorb meta of partitioned chunk %v",
                chunkId);
        }

        auto tableSchema = ParseOptionalProto<TTableSchema>(FindProtoExtension<TTableSchemaExt>(meta->extensions()));
        if (TableSchema_ != tableSchema) {
            THROW_ERROR_EXCEPTION(NChunkClient::EErrorCode::IncompatibleChunkMetas,
                "Chunks %v schema is different from output chunk schema",
                chunkId);
        }

        if (MiscExt_.sorted()) {
            auto boundaryKeysExt = ParseOptionalProto<TBoundaryKeysExtension>(FindProtoExtension<TBoundaryKeysExt>(meta->extensions()));
            if (!boundaryKeysExt) {
                THROW_ERROR_EXCEPTION(NChunkClient::EErrorCode::IncompatibleChunkMetas,
                    "Sorted chunk %v must have boundary keys extension",
                    chunkId);
            }

            if (!BoundaryKeys_) {
                // First meta.
                BoundaryKeys_ = boundaryKeysExt;
            } else {
                auto currentMinRow = NYT::FromProto<TLegacyOwningKey>(boundaryKeysExt->Min);
                auto previousMaxRow = NYT::FromProto<TLegacyOwningKey>(BoundaryKeys_->Max);
                YT_VERIFY(SchemaComparator_.CompareKeys(TKey::FromRow(previousMaxRow), TKey::FromRow(currentMinRow)) <= 0);
                BoundaryKeys_->Max = boundaryKeysExt->Max;
            }
        }

        if (NYT::FromProto<EChunkType>(meta->type()) == EChunkType::Table) {
            auto samplesExt = ParseOptionalProto<TSamplesExtension>(FindProtoExtension<TSamplesExt>(meta->extensions()));
            if (!samplesExt) {
                THROW_ERROR_EXCEPTION(NChunkClient::EErrorCode::IncompatibleChunkMetas,
                    "Cannot absorb meta of a chunk %v without samples",
                    chunkId);
            }
            if (!SamplesExt_) {
                // First meta.
                SamplesExt_ = samplesExt;
            } else {
                SamplesExt_->Entries.insert(SamplesExt_->Entries.end(), samplesExt->Entries.begin(), samplesExt->Entries.end());
                SamplesExt_->Weights.insert(SamplesExt_->Weights.end(), samplesExt->Weights.begin(), samplesExt->Weights.end());
            }

            auto columnarStatisticsExt = FindProtoExtension<TColumnarStatisticsExt>(meta->extensions());
            if (!columnarStatisticsExt) {
                THROW_ERROR_EXCEPTION(NChunkClient::EErrorCode::IncompatibleChunkMetas,
                    "Cannot absorb meta of a chunk %v without columnar statistics",
                    chunkId);
            }
            auto largeColumnarStatisticsExt = FindProtoExtension<TLargeColumnarStatisticsExt>(meta->extensions());

            i64 chunkRowCount = GetProtoExtension<NProto::TMiscExt>(meta->extensions()).row_count();
            TColumnarStatistics chunkColumnarStatistics;
            FromProto(
                &chunkColumnarStatistics,
                *columnarStatisticsExt,
                largeColumnarStatisticsExt ? &*largeColumnarStatisticsExt : nullptr,
                chunkRowCount);

            if (!ColumnarStatistics_) {
                // First meta.
                ColumnarStatistics_ = std::move(chunkColumnarStatistics);
            } else {
                if (ColumnarStatistics_->GetColumnCount() != chunkColumnarStatistics.GetColumnCount()) {
                    THROW_ERROR_EXCEPTION(NChunkClient::EErrorCode::IncompatibleChunkMetas,
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
                THROW_ERROR_EXCEPTION(NChunkClient::EErrorCode::IncompatibleChunkMetas,
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
            THROW_ERROR_EXCEPTION(NChunkClient::EErrorCode::IncompatibleChunkMetas,
                "Input chunk %v is not sorted",
                chunkId);
        }

        if (MiscExt_.compression_codec() != miscExt.compression_codec()) {
            THROW_ERROR_EXCEPTION(NChunkClient::EErrorCode::IncompatibleChunkMetas,
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
            THROW_ERROR_EXCEPTION(NChunkClient::EErrorCode::IncompatibleChunkMetas,
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

    const TDeferredChunkMetaPtr& GetChunkMeta() const override
    {
        YT_VERIFY(MetaFinalized_);
        return ChunkMeta_;
    }

    TError FinalizeAndValidateChunkMeta() override
    {
        if (!MetaFinalized_) {
            FinalizeMeta();
        }

        for (auto tag : ExtensionTagsToIgnoreInValidation) {
            InputChunkMetaExtensions_.erase(tag);
        }

        auto outputChunkMetaExtensions = GetExtensionTagSet(ChunkMeta_->extensions());
        if (InputChunkMetaExtensions_ == outputChunkMetaExtensions) {
            return TError();
        }

        return TError("Chunk meta extensions in input chunks differs from extensions in output chunks")
            << TErrorAttribute("input_chunks_chunk_meta_extensions", InputChunkMetaExtensions_)
            << TErrorAttribute("output_chunks_chunk_meta_extensions", outputChunkMetaExtensions);
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

    THashSet<int> InputChunkMetaExtensions_;

    std::optional<TBoundaryKeysExtension> BoundaryKeys_;
    std::optional<TColumnMetaExtension> ColumnMeta_;
    std::optional<TTableSchema> TableSchema_;
    std::optional<TKeyColumnsExtension> KeyColumns_;

    std::optional<TSamplesExtension> SamplesExt_;
    std::optional<TColumnarStatistics> ColumnarStatistics_;

    void AbsorbFirstMeta(const TDeferredChunkMetaPtr& meta, TChunkId /* chunkId */)
    {
        ChunkMeta_->set_type(meta->type());
        ChunkMeta_->set_format(meta->format());

        NameTableExt_ = GetProtoExtension<TNameTableExt>(meta->extensions());

        ColumnMeta_ = ParseOptionalProto<TColumnMetaExtension>(FindProtoExtension<TColumnMetaExt>(meta->extensions()));
        KeyColumns_ = ParseOptionalProto<TKeyColumnsExtension>(FindProtoExtension<TKeyColumnsExt>(meta->extensions()));
    }

    void AbsorbAnotherMeta(const TDeferredChunkMetaPtr& meta, TChunkId chunkId)
    {
        if (ChunkMeta_->type() != meta->type()) {
            THROW_ERROR_EXCEPTION(NChunkClient::EErrorCode::IncompatibleChunkMetas,
                "Meta types differ in chunks %v and %v",
                FirstChunkId_,
                chunkId)
                << TErrorAttribute("previous", NYT::FromProto<EChunkType>(ChunkMeta_->type()))
                << TErrorAttribute("current", NYT::FromProto<EChunkType>(meta->type()));
        }

        if (ChunkMeta_->format() != meta->format()) {
            THROW_ERROR_EXCEPTION(NChunkClient::EErrorCode::IncompatibleChunkMetas,
                "Meta formats differ in chunks %v and %v",
                FirstChunkId_,
                chunkId)
                << TErrorAttribute("previous", NYT::FromProto<EChunkFormat>(ChunkMeta_->format()))
                << TErrorAttribute("current", NYT::FromProto<EChunkFormat>(meta->format()));
        }

        auto nameTableExt = GetProtoExtension<TNameTableExt>(meta->extensions());
        if (!google::protobuf::util::MessageDifferencer::Equals(NameTableExt_, nameTableExt)) {
            THROW_ERROR_EXCEPTION(NChunkClient::EErrorCode::IncompatibleChunkMetas,
                "Name tables differ in chunks %v and %v",
                FirstChunkId_,
                chunkId);
        }

        auto keyColumns = ParseOptionalProto<TKeyColumnsExtension>(FindProtoExtension<TKeyColumnsExt>(meta->extensions()));
        if (keyColumns != KeyColumns_) {
            THROW_ERROR_EXCEPTION(NChunkClient::EErrorCode::IncompatibleChunkMetas,
                "Key columns differ in chunks %v and %v",
                FirstChunkId_,
                chunkId);
        }

        auto columnMeta = ParseOptionalProto<TColumnMetaExtension>(FindProtoExtension<TColumnMetaExt>(meta->extensions()));
        if (columnMeta.has_value() != ColumnMeta_.has_value()) {
            THROW_ERROR_EXCEPTION(NChunkClient::EErrorCode::IncompatibleChunkMetas,
                "Column metas differ in chunks %v and %v",
                FirstChunkId_,
                chunkId);
        }
        if (columnMeta) {
            if (ssize(columnMeta->Columns) != ssize(ColumnMeta_->Columns)) {
                THROW_ERROR_EXCEPTION(NChunkClient::EErrorCode::IncompatibleChunkMetas,
                    "Columns size differ in chunks %v and %v",
                    FirstChunkId_,
                    chunkId);
            }

            for (int i = 0; i < ssize(columnMeta->Columns); ++i) {
                const auto& column = columnMeta->Columns[i];
                auto& resultColumn = ColumnMeta_->Columns[i];

                auto getLastSegmentRowCount = [&] () -> i64 {
                    if (resultColumn.Segments.empty()) {
                        YT_LOG_ALERT(
                            "Previous chunk has no segment (ColumnIndex: %v, FirstChunkId: %v, CurrentChunkId: %v)",
                            i,
                            FirstChunkId_,
                            chunkId);
                        return 0;
                    }
                    const auto& lastSegment = resultColumn.Segments.back();
                    return lastSegment.ChunkRowCount;
                };
                auto lastSegmentRowCount = getLastSegmentRowCount();

                for (const auto& segment : column.Segments) {
                    auto newSegment = segment;
                    newSegment.ChunkRowCount += lastSegmentRowCount;
                    newSegment.BlockIndex += BlockIndex_;
                    resultColumn.Segments.emplace_back(std::move(newSegment));
                }
            }
        }
    }

    void FinalizeMeta()
    {
        YT_VERIFY(MetaInitialized_);
        YT_VERIFY(!MetaFinalized_);

        SetProtoExtension(ChunkMeta_->mutable_extensions(), BlockMetaExt_);
        SetProtoExtension(ChunkMeta_->mutable_extensions(), NameTableExt_);
        if (ColumnMeta_) {
            SetProtoExtension(ChunkMeta_->mutable_extensions(), ToProto<TColumnMetaExt>(*ColumnMeta_));
        }
        if (TableSchema_) {
            SetProtoExtension(ChunkMeta_->mutable_extensions(), ToProto<TTableSchemaExt>(*TableSchema_));
        }
        if (KeyColumns_) {
            SetProtoExtension(ChunkMeta_->mutable_extensions(), ToProto<TKeyColumnsExt>(*KeyColumns_));
        }
        if (BoundaryKeys_) {
            SetProtoExtension(ChunkMeta_->mutable_extensions(), ToProto<TBoundaryKeysExt>(*BoundaryKeys_));
        }
        if (SamplesExt_) {
            SetProtoExtension(ChunkMeta_->mutable_extensions(), ToProto<TSamplesExt>(*SamplesExt_));
        }
        if (ColumnarStatistics_) {
            SetProtoExtension(ChunkMeta_->mutable_extensions(), ToProto<TColumnarStatisticsExt>(*ColumnarStatistics_));
            SetProtoExtension(ChunkMeta_->mutable_extensions(), ToProto<TLargeColumnarStatisticsExt>(ColumnarStatistics_->LargeStatistics));
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
        if (TableSchema_) {
            MiscExt_.set_unique_keys(TableSchema_->IsUniqueKeys());
        }
        SetProtoExtension(ChunkMeta_->mutable_extensions(), MiscExt_);

        MetaFinalized_ = true;
    }
};

DECLARE_REFCOUNTED_CLASS(TMetaAggregatingWriter)
DEFINE_REFCOUNTED_TYPE(TMetaAggregatingWriter)

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
