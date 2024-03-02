
#include "schemaless_chunk_reader.h"
#include "helpers.h"
#include "private.h"
#include "hunks.h"
#include "chunk_reader_base.h"
#include "chunk_state.h"
#include "columnar_chunk_meta.h"
#include "columnar_chunk_reader_base.h"
#include "schemaless_block_reader.h"
#include "virtual_value_directory.h"
#include "versioned_chunk_reader.h"
#include "cached_versioned_chunk_meta.h"

#include <yt/yt/ytlib/table_chunk_format/public.h>
#include <yt/yt/ytlib/table_chunk_format/column_reader.h>
#include <yt/yt/ytlib/table_chunk_format/column_reader_detail.h>
#include <yt/yt/ytlib/table_chunk_format/null_column_reader.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/tablet_client/helpers.h>

#include <yt/yt/library/random/bernoulli_sampler.h>

#include <yt/yt/client/table_client/column_sort_schema.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/numeric_helpers.h>

#include <library/cpp/yt/coding/zig_zag.h>

namespace NYT::NTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NTabletClient;
using namespace NTableChunkFormat;
using namespace NTableChunkFormat::NProto;
using namespace NYPath;
using namespace NYTree;
using namespace NRpc;
using namespace NApi;
using namespace NTracing;

using NChunkClient::TDataSliceDescriptor;
using NChunkClient::TReadLimit;
using NChunkClient::TReadRange;

////////////////////////////////////////////////////////////////////////////////

TKeyWideningOptions BuildKeyWideningOptions(
    const std::vector<TString>& sortColumns,
    const TNameTablePtr& nameTable,
    int commonKeyPrefix)
{
    TKeyWideningOptions keyWideningOptions{
        .InsertPosition = commonKeyPrefix,
    };
    keyWideningOptions.InsertedColumnIds.reserve(std::ssize(sortColumns) - commonKeyPrefix);
    for (int i = commonKeyPrefix; i < std::ssize(sortColumns); ++i) {
        keyWideningOptions.InsertedColumnIds.push_back(nameTable->GetIdOrRegisterName(sortColumns[i]));
    }
    return keyWideningOptions;
}

////////////////////////////////////////////////////////////////////////////////

int GetRowIndexId(TNameTablePtr readerNameTable, TChunkReaderOptionsPtr options)
{
    if (options->EnableRowIndex) {
        return readerNameTable->GetIdOrRegisterName(RowIndexColumnName);
    }
    return -1;
}

////////////////////////////////////////////////////////////////////////////////

bool IsInsideRange(TRange<TLegacyKey> range, const TLegacyKey* item)
{
    return item >= range.begin() && item < range.end();
}

// TODO(lukyan): Use raw vector of names for chunk name table instead of TNameTable.
// TNameTable is thread safe and uses spinlock for each operation.
// Column filter universal fills readerNameTable via GetIdOrRegisterName.
std::vector<int> BuildColumnIdMapping(
    const TNameTablePtr& chunkNameTable,
    const TTableSchemaPtr& readerSchema,
    const TNameTablePtr& readerNameTable,
    const TColumnFilter& columnFilter,
    const TRange<TString> omittedInaccessibleColumns)
{
    THashSet<TStringBuf> omittedInaccessibleColumnSet(
        omittedInaccessibleColumns.begin(),
        omittedInaccessibleColumns.end());

    auto chunkNamesCount = chunkNameTable->GetSize();
    std::vector<int> chunkToReaderIdMapping(chunkNamesCount, -1);

    if (columnFilter.IsUniversal()) {
        for (int chunkColumnId = 0; chunkColumnId < chunkNamesCount; ++chunkColumnId) {
            auto stableName = chunkNameTable->GetName(chunkColumnId);
            if (readerSchema->GetNameMapping().IsDeleted(TColumnStableName(TString(stableName)))) {
                continue;
            }
            auto name = readerSchema->GetNameMapping().StableNameToName(TColumnStableName(TString(stableName)));

            if (omittedInaccessibleColumnSet.contains(name)) {
                continue;
            }

            auto readerColumnId = readerNameTable->GetIdOrRegisterName(name);
            chunkToReaderIdMapping[chunkColumnId] = readerColumnId;
        }
    } else {
        for (auto readerColumnId : columnFilter.GetIndexes()) {
            auto name = readerNameTable->GetName(readerColumnId);
            if (omittedInaccessibleColumnSet.contains(name)) {
                continue;
            }

            TStringBuf stableName = name;
            if (readerSchema) {
                if (auto* column = readerSchema->FindColumn(name)) {
                    stableName = column->StableName().Underlying();
                } else if (readerSchema->FindColumnByStableName(TColumnStableName{TString{name}})) {
                    // Column was renamed. We don't want to fetch it by old name.
                    continue;
                }
            }

            auto chunkColumnId = chunkNameTable->FindId(stableName);
            if (!chunkColumnId) {
                continue;
            }
            chunkToReaderIdMapping[*chunkColumnId] = readerColumnId;
        }
    }

    return chunkToReaderIdMapping;
}

TReaderVirtualValues InitializeVirtualColumns(
    TChunkReaderOptionsPtr options,
    TNameTablePtr readerNameTable,
    const TColumnFilter& columnFilter,
    NChunkClient::NProto::TChunkSpec chunkSpec,
    TVirtualValueDirectoryPtr virtualValueDirectory,
    std::optional<i64> virtualRowIndex = std::nullopt)
{
    TReaderVirtualValues virtualValues;

    try {
        if (options->EnableRangeIndex) {
            virtualValues.AddValue(MakeUnversionedInt64Value(
                chunkSpec.range_index(),
                readerNameTable->GetIdOrRegisterName(RangeIndexColumnName)),
                SimpleLogicalType(ESimpleLogicalValueType::Int64));
        }

        // NB: table index should not always be virtual column, sometimes it is stored
        // alongside row in chunk (e.g. for intermediate chunks in schemaful Map-Reduce).
        if (options->EnableTableIndex && chunkSpec.has_table_index()) {
            virtualValues.AddValue(MakeUnversionedInt64Value(
                chunkSpec.table_index(),
                readerNameTable->GetIdOrRegisterName(TableIndexColumnName)),
                SimpleLogicalType(ESimpleLogicalValueType::Int64));
        }

        if (options->EnableTabletIndex) {
            virtualValues.AddValue(MakeUnversionedInt64Value(
                chunkSpec.tablet_index(),
                readerNameTable->GetIdOrRegisterName(TabletIndexColumnName)),
                SimpleLogicalType(ESimpleLogicalValueType::Int64));
        }

        if (virtualValueDirectory) {
            YT_VERIFY(virtualRowIndex);
            YT_VERIFY(*virtualRowIndex < std::ssize(virtualValueDirectory->Rows));
            const auto& row = virtualValueDirectory->Rows[*virtualRowIndex];
            // Copy all values from rows properly mapping virtual ids into actual ids according to #NameTable_.
            for (int virtualColumnIndex = 0; virtualColumnIndex < static_cast<int>(row.GetCount()); ++virtualColumnIndex) {
                auto value = row[virtualColumnIndex];
                auto virtualId = value.Id;
                const auto& columnName = virtualValueDirectory->NameTable->GetName(virtualId);
                // Set proper id for this column.
                std::optional<int> resultId;
                if (columnFilter.IsUniversal()) {
                    resultId = readerNameTable->GetIdOrRegisterName(columnName);
                } else {
                    resultId = readerNameTable->FindId(columnName);
                }
                if (!resultId || !columnFilter.ContainsIndex(*resultId)) {
                    // This value is not requested from reader, ignore it.
                    continue;
                }
                value.Id = *resultId;
                virtualValues.AddValue(value, virtualValueDirectory->Schema->Columns()[virtualColumnIndex].LogicalType());
            }
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to add virtual columns to name table for schemaless chunk reader")
            << ex;
    }

    return virtualValues;
}

bool HasColumnsInMapping(TRange<int> schemalessIdMapping)
{
    for (auto id : schemalessIdMapping) {
        if (id != -1) {
            return true;
        }
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

TBernoulliSampler CreateSmapler(
    TChunkId chunkId,
    std::optional<double> samplingRate,
    std::optional<ui64> samplingSeed)
{
    auto seed = samplingSeed
        ? *samplingSeed ^ FarmFingerprint(chunkId.Parts64[0]) ^ FarmFingerprint(chunkId.Parts64[1])
        : std::random_device()();

    return TBernoulliSampler(samplingRate, seed);
}

class TSchemalessChunkReaderBase
    : public virtual ISchemalessChunkReader
{
public:
    TSchemalessChunkReaderBase(
        const NChunkClient::NProto::TChunkSpec& chunkSpec,
        const TChunkReaderConfigPtr& config,
        const TNameTablePtr& nameTable,
        TChunkId chunkId,
        const std::vector<int>& chunkToReaderIdMapping,
        int rowIndexId,
        TReaderVirtualValues virtualValues,
        const TClientChunkReadOptions& chunkReadOptions,
        std::optional<i64> virtualRowIndex = {})
        : ChunkSpec_(chunkSpec)
        , Config_(std::move(config))
        , NameTable_(nameTable)
        , DataSliceDescriptor_(chunkSpec, virtualRowIndex)
        , ChunkToReaderIdMapping_(chunkToReaderIdMapping)
        , RowIndexId_(rowIndexId)
        , Sampler_(CreateSmapler(chunkId, Config_->SamplingRate, Config_->SamplingSeed))
        , VirtualValues_(virtualValues)
        , Logger(TableClientLogger.WithTag("ChunkReaderId: %v, ChunkId: %v",
            TGuid::Create(),
            chunkId))
    {
        if (chunkReadOptions.ReadSessionId) {
            Logger.AddTag("ReadSessionId: %v", chunkReadOptions.ReadSessionId);
        }
    }

    const TDataSliceDescriptor& GetCurrentReaderDescriptor() const override
    {
        return DataSliceDescriptor_;
    }

    i64 GetTableRowIndex() const override
    {
        return ChunkSpec_.table_row_index() + RowIndex_;
    }

    TInterruptDescriptor GetInterruptDescriptor(
        TRange<TUnversionedRow> /*unreadRows*/) const override
    {
        YT_ABORT();
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

protected:
    const TChunkSpec ChunkSpec_;
    const TChunkReaderConfigPtr Config_;
    // TODO(lukyan): Remove name table. Used only in GetNameTable.
    const TNameTablePtr NameTable_;
    // TODO(lukyan): Move DataSliceDescriptor_ to multichunk reader. Used only in GetCurrentReaderDescriptor.
    const TDataSliceDescriptor DataSliceDescriptor_;

    // Maps chunk name table ids into client id.
    // For filtered out columns maps id to -1.
    const std::vector<int> ChunkToReaderIdMapping_;
    const int RowIndexId_ = -1;

    i64 RowIndex_ = 0;
    i64 RowCount_ = 0;
    i64 DataWeight_ = 0;

    TBernoulliSampler Sampler_;

    //! Values for virtual constant columns like $range_index, $table_index etc.
    TReaderVirtualValues VirtualValues_;

    NLogging::TLogger Logger;

    int GetRootSystemColumnCount() const
    {
        return VirtualValues_.GetTotalColumnCount() / 2 + (RowIndexId_ != -1);
    }

    int GetLeafSystemColumnCount() const
    {
        return VirtualValues_.GetTotalColumnCount() + (RowIndexId_ != -1);
    }

    void AddExtraValues(TMutableUnversionedRow& row, i64 rowIndex)
    {
        if (RowIndexId_ != -1) {
            *row.End() = MakeUnversionedInt64Value(rowIndex, RowIndexId_);
            row.SetCount(row.GetCount() + 1);
        }

        for (const auto& value : VirtualValues_.Values()) {
            *row.End() = value;
            row.SetCount(row.GetCount() + 1);
        }
    }

    TInterruptDescriptor GetInterruptDescriptorImpl(
        TRange<TUnversionedRow> unreadRows,
        const NChunkClient::NProto::TMiscExt& misc,
        const TChunkSpec& chunkSpec,
        const TReadLimit& lowerLimit,
        const TReadLimit& upperLimit,
        i64 rowIndex,
        int interruptDescriptorKeyLength) const
    {
        std::vector<TDataSliceDescriptor> unreadDescriptors;
        std::vector<TDataSliceDescriptor> readDescriptors;

        rowIndex -= unreadRows.Size();
        i64 lowerRowIndex = lowerLimit.GetRowIndex().value_or(0);
        i64 upperRowIndex = upperLimit.GetRowIndex().value_or(misc.row_count());

        // Verify row index is in the chunk range
        if (RowCount_ > 0) {
            // If this is not a trivial case, e.g. lowerLimit > upperLimit,
            // let's do a sanity check.
            YT_VERIFY(lowerRowIndex <= rowIndex);
            YT_VERIFY(rowIndex <= upperRowIndex);
        }

        TKey firstUnreadKey;
        if (!unreadRows.Empty()) {
            firstUnreadKey = TKey::FromRowUnchecked(unreadRows[0], interruptDescriptorKeyLength);
        }

        if (rowIndex < upperRowIndex) {
            unreadDescriptors.emplace_back(chunkSpec);
            auto& chunk = unreadDescriptors[0].ChunkSpecs[0];
            chunk.mutable_lower_limit()->set_row_index(rowIndex);
            if (firstUnreadKey) {
                auto* chunkLowerLimit = chunk.mutable_lower_limit();
                ToProto(chunkLowerLimit->mutable_legacy_key(), firstUnreadKey.AsOwningRow());
                ToProto(chunkLowerLimit->mutable_key_bound_prefix(), firstUnreadKey.AsOwningRow());
                chunkLowerLimit->set_key_bound_is_inclusive(true);
            }
            i64 rowCount = std::max(1l, chunk.row_count_override() - RowCount_ + static_cast<i64>(unreadRows.Size()));
            rowCount = std::min(rowCount, upperRowIndex - rowIndex);
            chunk.set_row_count_override(rowCount);
            i64 chunkDataWeight = misc.has_data_weight() ? misc.data_weight() : misc.uncompressed_data_size();
            // NB(gritukan): Some old chunks might have zero data weight.
            chunkDataWeight = std::max<i64>(chunkDataWeight, 1);
            i64 dataWeight = DivCeil(
                chunkDataWeight,
                misc.row_count()) * rowCount;
            YT_VERIFY(dataWeight > 0);
            chunk.set_data_weight_override(dataWeight);
        }

        // Sometimes scheduler sends us empty slices (when both, row index and key limits are present).
        // Such slices should be considered as read, though actual read row count is equal to zero.
        if (RowCount_ > std::ssize(unreadRows) || rowIndex >= upperRowIndex) {
            readDescriptors.emplace_back(chunkSpec);
            auto& chunk = readDescriptors[0].ChunkSpecs[0];
            chunk.mutable_upper_limit()->set_row_index(rowIndex);
            i64 rowCount = RowCount_ - unreadRows.Size();
            YT_VERIFY(rowCount >= 0);
            chunk.set_row_count_override(rowCount);
            YT_VERIFY(rowIndex >= rowCount);
            chunk.mutable_lower_limit()->set_row_index(rowIndex - rowCount);
            i64 dataWeight = DivCeil(
                misc.has_data_weight() ? misc.data_weight() : misc.uncompressed_data_size(),
                misc.row_count()) * rowCount;
            chunk.set_data_weight_override(dataWeight);
        }
        return {std::move(unreadDescriptors), std::move(readDescriptors)};
    }

    bool SampleRow(int rowIndex)
    {
        if (Config_->SamplingMode != ESamplingMode::Row) {
            return true;
        }

        return Sampler_.Sample(rowIndex);
    }

    bool SampleBlock(int blockIndex)
    {
        if (Config_->SamplingMode != ESamplingMode::Block) {
            return true;
        }

        return Sampler_.Sample(blockIndex);
    }
};

////////////////////////////////////////////////////////////////////////////////

class THorizontalSchemalessChunkReaderBase
    : public TChunkReaderBase
    , public TSchemalessChunkReaderBase
{
public:
    THorizontalSchemalessChunkReaderBase(
        const TChunkStatePtr& chunkState,
        const TColumnarChunkMetaPtr& chunkMeta,
        const TChunkReaderConfigPtr& config,
        const TNameTablePtr& nameTable,
        IChunkReaderPtr underlyingReader,
        const std::vector<int>& chunkToReaderIdMapping,
        int rowIndexId,
        const TReaderVirtualValues& virtualValues,
        const TClientChunkReadOptions& chunkReadOptions,
        TRange<ESortOrder> sortOrders,
        int commonKeyPrefix,
        std::optional<int> partitionTag,
        const TChunkReaderMemoryManagerHolderPtr& memoryManagerHolder,
        std::optional<i64> virtualRowIndex = std::nullopt)
        : TChunkReaderBase(
            config,
            underlyingReader,
            chunkState->BlockCache,
            chunkReadOptions,
            memoryManagerHolder)
        , TSchemalessChunkReaderBase(
            chunkState->ChunkSpec,
            config,
            nameTable,
            underlyingReader->GetChunkId(),
            chunkToReaderIdMapping,
            rowIndexId,
            virtualValues,
            chunkReadOptions,
            virtualRowIndex)
        , ChunkMeta_(chunkMeta)
        , BlockLastKeys_(ChunkMeta_->BlockLastKeys())
        , BlockMetaExt_(ChunkMeta_->DataBlockMeta())
        , PartitionTag_(partitionTag)
        , SortOrders_(sortOrders.begin(), sortOrders.end())
        , CommonKeyPrefix_(commonKeyPrefix)
    {
        YT_VERIFY(CommonKeyPrefix_ <= std::ssize(SortOrders_));

        if (chunkState->DataSource) {
            PackBaggageForChunkReader(TraceContext_, *chunkState->DataSource, MakeExtraChunkTags(ChunkMeta_->Misc()));
        }
    }

    TDataStatistics GetDataStatistics() const override;

protected:
    using TSchemalessChunkReaderBase::Config_;
    using TSchemalessChunkReaderBase::Logger;

    const TColumnarChunkMetaPtr ChunkMeta_;
    const TSharedRange<TUnversionedRow> BlockLastKeys_;

    const TRefCountedDataBlockMetaPtr BlockMetaExt_;

    // TODO(lukyan): Remove
    std::optional<int> PartitionTag_;

    int CurrentBlockIndex_ = 0;

    std::unique_ptr<THorizontalBlockReader> BlockReader_;

    std::vector<int> BlockIndexes_;

    //! During unsorted read of a sorted chunk we consider
    //! chunk as unsorted.
    const std::vector<ESortOrder> SortOrders_;
    const int CommonKeyPrefix_;

    int GetChunkKeyColumnCount() const
    {
        return ChunkMeta_->ChunkSchema()->GetKeyColumnCount();
    }

    TFuture<void> InitBlockFetcher();
};

////////////////////////////////////////////////////////////////////////////////

TFuture<void> THorizontalSchemalessChunkReaderBase::InitBlockFetcher()
{
    YT_LOG_DEBUG("Reading blocks (BlockCount: %v)",
        BlockIndexes_.size());

    std::vector<TBlockFetcher::TBlockInfo> blocks;
    blocks.reserve(BlockIndexes_.size());
    for (int blockIndex : BlockIndexes_) {
        // NB: This reader can only read data blocks, hence in block infos we set block type to UncompressedData.
        YT_VERIFY(blockIndex < BlockMetaExt_->data_blocks_size());
        const auto& blockMeta = BlockMetaExt_->data_blocks(blockIndex);
        int priority = blocks.size();
        blocks.push_back({
            .ReaderIndex = 0,
            .BlockIndex = blockMeta.block_index(),
            .Priority = priority,
            .UncompressedDataSize = blockMeta.uncompressed_size(),
            .BlockType = EBlockType::UncompressedData,
        });
    }

    return DoOpen(std::move(blocks), ChunkMeta_->Misc());
}

TDataStatistics THorizontalSchemalessChunkReaderBase::GetDataStatistics() const
{
    auto dataStatistics = TChunkReaderBase::GetDataStatistics();
    dataStatistics.set_row_count(RowCount_);
    dataStatistics.set_data_weight(DataWeight_);
    return dataStatistics;
}

////////////////////////////////////////////////////////////////////////////////

class THorizontalSchemalessRangeChunkReader
    : public THorizontalSchemalessChunkReaderBase
{
public:
    THorizontalSchemalessRangeChunkReader(
        const TChunkStatePtr& chunkState,
        const TColumnarChunkMetaPtr& chunkMeta,
        const TChunkReaderConfigPtr& config,
        const TNameTablePtr& nameTable,
        IChunkReaderPtr underlyingReader,
        const std::vector<int>& chunkToReaderIdMapping,
        int rowIndexId,
        const TReaderVirtualValues& virtualValues,
        const TClientChunkReadOptions& chunkReadOptions,
        TRange<ESortOrder> sortOrders,
        int commonKeyPrefix,
        const TKeyWideningOptions& keyWideningOptions,
        const TReadRange& readRange,
        std::optional<int> partitionTag,
        const TChunkReaderMemoryManagerHolderPtr& memoryManagerHolder,
        std::optional<i64> virtualRowIndex,
        int interruptDescriptorKeyLength)
        : THorizontalSchemalessChunkReaderBase(
            chunkState,
            chunkMeta,
            config,
            nameTable,
            std::move(underlyingReader),
            chunkToReaderIdMapping,
            rowIndexId,
            virtualValues,
            chunkReadOptions,
            sortOrders,
            commonKeyPrefix,
            partitionTag,
            memoryManagerHolder,
            virtualRowIndex)
        , ReadRange_(readRange)
        , InterruptDescriptorKeyLength_(interruptDescriptorKeyLength)
        , KeyWideningOptions_(keyWideningOptions)
    {
        YT_LOG_DEBUG("Reading range of a chunk (Range: %v)", ReadRange_);

        // Initialize to lowest reasonable value.
        RowIndex_ = ReadRange_.LowerLimit().GetRowIndex().value_or(0);

        if (PartitionTag_) {
            YT_VERIFY(ReadRange_.LowerLimit().IsTrivial());
            YT_VERIFY(ReadRange_.UpperLimit().IsTrivial());

            CreateBlockSequence(0, BlockMetaExt_->data_blocks_size());
        } else {
            int beginIndex = ApplyLowerRowLimit(*BlockMetaExt_, ReadRange_.LowerLimit());
            int endIndex = ApplyUpperRowLimit(*BlockMetaExt_, ReadRange_.UpperLimit());

            bool readSorted = SortOrders_.size() > 0;

            if (readSorted) {
                const auto& misc = ChunkMeta_->Misc();
                if (!misc.sorted()) {
                    THROW_ERROR_EXCEPTION("Requested a sorted read for an unsorted chunk");
                }

                beginIndex = std::max(
                    beginIndex,
                    ApplyLowerKeyLimit(BlockLastKeys_, ReadRange_.LowerLimit(), SortOrders_, CommonKeyPrefix_));

                endIndex = std::min(
                    endIndex,
                    ApplyUpperKeyLimit(BlockLastKeys_, ReadRange_.UpperLimit(), SortOrders_, CommonKeyPrefix_));
            } else {
                YT_VERIFY(!ReadRange_.LowerLimit().KeyBound());
                YT_VERIFY(!ReadRange_.UpperLimit().KeyBound());
            }

            CreateBlockSequence(beginIndex, endIndex);
        }

        // Ready event must be set only when all initialization is finished and
        // RowIndex_ is set into proper value.
        // Must be called after the object is constructed and vtable initialized.
        SetReadyEvent(
            InitBlockFetcher()
            .Apply(BIND([this, this_ = MakeStrong(this)] (const TError& error) {
                if (!error.IsOK()) {
                    THROW_ERROR_EXCEPTION("Failed to initialize chunk reader")
                        << TErrorAttribute("chunk_id", UnderlyingReader_->GetChunkId())
                        << error;
                }

                if (InitFirstBlockNeeded_) {
                    InitFirstBlock();
                    InitFirstBlockNeeded_ = false;
                }
            })));
    }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override;

    TInterruptDescriptor GetInterruptDescriptor(TRange<TUnversionedRow> unreadRows) const override;

private:
    TReadRange ReadRange_;

    const int InterruptDescriptorKeyLength_;
    const TKeyWideningOptions KeyWideningOptions_;

    void InitFirstBlock() override;
    void InitNextBlock() override;

    void CreateBlockSequence(int beginIndex, int endIndex);
};

DEFINE_REFCOUNTED_TYPE(THorizontalSchemalessRangeChunkReader)

////////////////////////////////////////////////////////////////////////////////

void THorizontalSchemalessRangeChunkReader::CreateBlockSequence(int beginIndex, int endIndex)
{
    for (int index = beginIndex; index < endIndex; ++index) {
        if (SampleBlock(index)) {
            BlockIndexes_.push_back(index);
        }
    }
}

void THorizontalSchemalessRangeChunkReader::InitFirstBlock()
{
    int blockIndex = BlockIndexes_[CurrentBlockIndex_];
    const auto& blockMeta = BlockMetaExt_->data_blocks(blockIndex);

    YT_VERIFY(CurrentBlock_ && CurrentBlock_.IsSet());
    BlockReader_.reset(new THorizontalBlockReader(
        CurrentBlock_.Get().ValueOrThrow().Data,
        blockMeta,
        GetCompositeColumnFlags(ChunkMeta_->ChunkSchema()),
        GetHunkColumnFlags(ChunkMeta_->GetChunkFormat(), ChunkMeta_->GetChunkFeatures(), ChunkMeta_->ChunkSchema()),
        ChunkMeta_->HunkChunkRefs(),
        ChunkMeta_->HunkChunkMetas(),
        ChunkToReaderIdMapping_,
        SortOrders_,
        CommonKeyPrefix_,
        KeyWideningOptions_,
        GetRootSystemColumnCount()));

    RowIndex_ = blockMeta.chunk_row_count() - blockMeta.row_count();

    TUnversionedRow blockLastKey;
    if (blockIndex < std::ssize(BlockLastKeys_)) {
        blockLastKey = BlockLastKeys_[blockIndex];
    }
    CheckBlockUpperLimits(
        BlockMetaExt_->data_blocks(blockIndex).chunk_row_count(),
        blockLastKey,
        ReadRange_.UpperLimit(),
        SortOrders_,
        CommonKeyPrefix_);

    const auto& lowerLimit = ReadRange_.LowerLimit();

    if (lowerLimit.GetRowIndex() && RowIndex_ < *lowerLimit.GetRowIndex()) {
        YT_VERIFY(BlockReader_->SkipToRowIndex(*lowerLimit.GetRowIndex() - RowIndex_));
        RowIndex_ = *lowerLimit.GetRowIndex();
    }

    if (lowerLimit.KeyBound()) {
        auto blockRowIndex = BlockReader_->GetRowIndex();
        YT_VERIFY(BlockReader_->SkipToKeyBound(ToKeyBoundRef(lowerLimit.KeyBound())));
        RowIndex_ += BlockReader_->GetRowIndex() - blockRowIndex;
    }
}

void THorizontalSchemalessRangeChunkReader::InitNextBlock()
{
    ++CurrentBlockIndex_;
    InitFirstBlock();
}

IUnversionedRowBatchPtr THorizontalSchemalessRangeChunkReader::Read(const TRowBatchReadOptions& options)
{
    TCurrentTraceContextGuard traceGuard(TraceContext_);
    auto readGuard = AcquireReadGuard();

    MemoryPool_.Clear();

    if (!BeginRead()) {
        // Not ready yet.
        return CreateEmptyUnversionedRowBatch();
    }

    if (!BlockReader_) {
        // Nothing to read from chunk.
        return nullptr;
    }

    if (BlockEnded_) {
        BlockReader_.reset();
        return OnBlockEnded() ? CreateEmptyUnversionedRowBatch() : nullptr;
    }

    std::vector<TUnversionedRow> rows;
    rows.reserve(options.MaxRowsPerRead);
    i64 dataWeight = 0;

    const auto& upperLimit = ReadRange_.UpperLimit();
    while (!BlockEnded_ &&
           std::ssize(rows) < options.MaxRowsPerRead &&
           dataWeight < options.MaxDataWeightPerRead)
    {
        if (CheckRowLimit_ && RowIndex_ >= upperLimit.GetRowIndex()) {
            BlockEnded_ = true;
            break;
        }

        if (CheckKeyLimit_ &&
            !TestKey(
                ToKeyRef(BlockReader_->GetLegacyKey()),
                ToKeyBoundRef(upperLimit.KeyBound()),
                SortOrders_))
        {
            BlockEnded_ = true;
            break;
        }

        if (SampleRow(GetTableRowIndex())) {
            auto row = BlockReader_->GetRow(&MemoryPool_);
            AddExtraValues(row, GetTableRowIndex());
            rows.push_back(row);
            dataWeight += GetDataWeight(row);
        }
        ++RowIndex_;

        if (!BlockReader_->NextRow()) {
            BlockEnded_ = true;
        }
    }

    RowCount_ += rows.size();
    DataWeight_ += dataWeight;

    return CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
}

TInterruptDescriptor THorizontalSchemalessRangeChunkReader::GetInterruptDescriptor(
    TRange<TUnversionedRow> unreadRows) const
{
    if (BlockIndexes_.size() == 0) {
        return {};
    }
    return GetInterruptDescriptorImpl(
        unreadRows,
        ChunkMeta_->Misc(),
        ChunkSpec_,
        ReadRange_.LowerLimit(),
        ReadRange_.UpperLimit(),
        RowIndex_,
        InterruptDescriptorKeyLength_);
}

////////////////////////////////////////////////////////////////////////////////

class THorizontalSchemalessLookupChunkReaderBase
    : public THorizontalSchemalessChunkReaderBase
{
public:
    THorizontalSchemalessLookupChunkReaderBase(
        const TChunkStatePtr& chunkState,
        const TColumnarChunkMetaPtr& chunkMeta,
        const TChunkReaderConfigPtr& config,
        const TNameTablePtr& nameTable,
        NChunkClient::IChunkReaderPtr underlyingReader,
        const std::vector<int>& chunkToReaderIdMapping,
        int rowIndexId,
        const TReaderVirtualValues& virtualValues,
        const TClientChunkReadOptions& chunkReadOptions,
        TRange<ESortOrder> sortOrders,
        int commonKeyPrefix,
        const TKeyWideningOptions& keyWideningOptions,
        const TSharedRange<TLegacyKey>& keys,
        std::optional<int> partitionTag = std::nullopt,
        const TChunkReaderMemoryManagerHolderPtr& memoryManagerHolder = nullptr);

protected:
    TSharedRange<TLegacyKey> Keys_;

    // Subrange of Keys_ that does not fall outside of ChunkSpec read limits.
    TRange<TLegacyKey> PrefixRange_;

    // Subrances of PrefixRange_ that contain key prefixes where each key has to be
    // checked against limits individually.
    TRange<TLegacyKey> LowerBoundCmpRange_;
    TRange<TLegacyKey> UpperBoundCmpRange_;

    TKeyWideningOptions KeyWideningOptions_;
    bool HasMoreBlocks_ = true;

    TReadLimit ChunkSpecLowerLimit_, ChunkSpecUpperLimit_;
    std::optional<TKeyBoundRef> LowerBound_;
    std::optional<TKeyBoundRef> UpperBound_;

    void ApplyLimits();
    void ComputeBlockIndexes(TRange<TLegacyKey> keys);
    void InitBlocks();

    void InitFirstBlock() override;
    void InitNextBlock() override;

private:
    const TChunkStatePtr& ChunkState_;
};

class THorizontalSchemalessLookupChunkReader
    : public THorizontalSchemalessLookupChunkReaderBase
{
public:
    THorizontalSchemalessLookupChunkReader(
        const TChunkStatePtr& chunkState,
        const TColumnarChunkMetaPtr& chunkMeta,
        const TChunkReaderConfigPtr& config,
        const TNameTablePtr& nameTable,
        NChunkClient::IChunkReaderPtr underlyingReader,
        const std::vector<int>& chunkToReaderIdMapping,
        int rowIndexId,
        const TReaderVirtualValues& virtualValues,
        const TClientChunkReadOptions& chunkReadOptions,
        TRange<ESortOrder> sortOrders,
        int commonKeyPrefix,
        const TKeyWideningOptions& keyWideningOptions,
        const TSharedRange<TLegacyKey>& keys,
        std::optional<int> partitionTag = std::nullopt,
        const TChunkReaderMemoryManagerHolderPtr& memoryManagerHolder = nullptr);

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override;
};

class THorizontalSchemalessKeyRangesChunkReader
    : public THorizontalSchemalessLookupChunkReaderBase
{
public:
    THorizontalSchemalessKeyRangesChunkReader(
        const TChunkStatePtr& chunkState,
        const TColumnarChunkMetaPtr& chunkMeta,
        const TChunkReaderConfigPtr& config,
        const TNameTablePtr& nameTable,
        NChunkClient::IChunkReaderPtr underlyingReader,
        const std::vector<int>& chunkToReaderIdMapping,
        int rowIndexId,
        const TReaderVirtualValues& virtualValues,
        const TClientChunkReadOptions& chunkReadOptions,
        TRange<ESortOrder> sortOrders,
        int commonKeyPrefix,
        const TKeyWideningOptions& keyWideningOptions,
        const TSharedRange<TLegacyKey>& keyPrefixes,
        std::optional<int> partitionTag = std::nullopt,
        const TChunkReaderMemoryManagerHolderPtr& memoryManagerHolder = nullptr);

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override;

private:
    int KeyIndex_ = 0;
};

DEFINE_REFCOUNTED_TYPE(THorizontalSchemalessLookupChunkReader)

////////////////////////////////////////////////////////////////////////////////
THorizontalSchemalessLookupChunkReaderBase::THorizontalSchemalessLookupChunkReaderBase(
    const TChunkStatePtr& chunkState,
    const TColumnarChunkMetaPtr& chunkMeta,
    const TChunkReaderConfigPtr& config,
    const TNameTablePtr& nameTable,
    NChunkClient::IChunkReaderPtr underlyingReader,
    const std::vector<int>& chunkToReaderIdMapping,
    int rowIndexId,
    const TReaderVirtualValues& virtualValues,
    const TClientChunkReadOptions& chunkReadOptions,
    TRange<ESortOrder> sortOrders,
    int commonKeyPrefix,
    const TKeyWideningOptions& keyWideningOptions,
    const TSharedRange<TLegacyKey>& keyPrefixes,
    std::optional<int> partitionTag,
    const TChunkReaderMemoryManagerHolderPtr& memoryManagerHolder)
    : THorizontalSchemalessChunkReaderBase(
        chunkState,
        chunkMeta,
        config,
        nameTable,
        std::move(underlyingReader),
        chunkToReaderIdMapping,
        rowIndexId,
        virtualValues,
        chunkReadOptions,
        sortOrders,
        commonKeyPrefix,
        partitionTag,
        memoryManagerHolder)
    , Keys_(keyPrefixes)
    , KeyWideningOptions_(keyWideningOptions)
    , ChunkState_(chunkState)
{
    const auto& misc = ChunkMeta_->Misc();
    if (!misc.sorted()) {
        THROW_ERROR_EXCEPTION("Requested lookup for an unsorted chunk");
    }

    PrefixRange_ = Keys_;
}

void THorizontalSchemalessLookupChunkReaderBase::InitBlocks()
{
    SetReadyEvent(
        InitBlockFetcher()
        .Apply(BIND([this, this_ = MakeStrong(this)] {
            if (InitFirstBlockNeeded_) {
                InitFirstBlock();
                InitFirstBlockNeeded_ = false;
            }
        })));
}

void THorizontalSchemalessLookupChunkReaderBase::ApplyLimits()
{
    // ApplyLimits() extracts lower read limit and upper read limit from the ChunkSpec,
    // and compares these limits to the Keys_ vector.
    //
    // The reader will only receive rows for keys prefixes within the read limit.
    // However, the client of this class may provide key prefixes that fall outside the range.
    // These prefixes have to be filtered out.
    //
    // Every key prefix may fall into three categories:
    //  (1) completely outside the limits.
    //  (2) partially the limits: each key has to be checked against limits individually.
    //  (3) completely within limits.
    //
    // The category (2) includes key prefixes that form either full or a partial prefix
    // match with the boundary key of the limiting ray.
    //
    // ApplyLimits() computes the following subranges of Keys_
    // PrefixRange_, the range of key prefixes partially or completely intersecting with limits.
    // LowerBoundCmpRange_, a prefix of PrefixRange_ where each key has to be individually
    // tested against lower limit.
    // UpperBoundCmpRange_, a suffix of PrefixRange_ where each key has to be tested against
    // the upper limit.
    //
    // This implementation defensively supports the case when read limits inside the ChunkSpec
    // only provide key prefixes, not full keys.

    const TLegacyKey* begin = Keys_.begin();
    const TLegacyKey* end = Keys_.end();

    if (ChunkState_->ChunkSpec.has_lower_limit()) {
        FromProto(&ChunkSpecLowerLimit_, ChunkState_->ChunkSpec.lower_limit(), /*isUpper*/ false, SortOrders_.size());
        if (ChunkSpecLowerLimit_.KeyBound()) {
            LowerBound_ = ToKeyBoundRef(ChunkSpecLowerLimit_.KeyBound());

            begin = std::lower_bound(Keys_.begin(), Keys_.end(), nullptr,
                [&] (TLegacyKey key, nullptr_t) -> bool
                {
                    int prefixLength = std::min<int>(key.GetCount(), LowerBound_->Size());
                    int result = ComparePrefix(key.begin(), LowerBound_->begin(), prefixLength);
                    if (result == 0) {
                        return false;
                    }
                    return !TestComparisonResult(result, SortOrders_, LowerBound_->Inclusive, /*upper*/ false);
                });
        }
    }

    if (ChunkState_->ChunkSpec.has_upper_limit()) {
        FromProto(&ChunkSpecUpperLimit_, ChunkState_->ChunkSpec.upper_limit(), /*isUpper*/ true, SortOrders_.size());
        if (ChunkSpecUpperLimit_.KeyBound()) {
            UpperBound_ = ToKeyBoundRef(ChunkSpecUpperLimit_.KeyBound());

            end = std::lower_bound(Keys_.begin(), Keys_.end(), nullptr,
                [&] (TLegacyKey key, nullptr_t) -> bool
                {
                    int prefixLength = std::min<int>(key.GetCount(), UpperBound_->Size());
                    int result = ComparePrefix(key.begin(), UpperBound_->begin(), prefixLength);
                    if (result == 0) {
                        return true;
                    }
                    return TestComparisonResult(result, SortOrders_, UpperBound_->Inclusive, /*upper*/ true);
                });
        }
    }

    if (end < begin) {
        end = begin;
    }

    LowerBoundCmpRange_ = MakeRange(begin, begin);
    UpperBoundCmpRange_ = MakeRange(end, end);

    if (LowerBound_) {
        auto* lowerBoundCmpEnd = std::lower_bound(begin, end, nullptr,
            [&] (TLegacyKey key, nullptr_t)
            {
                int prefixLength = std::min<int>(key.GetCount(), LowerBound_->size());
                int result = ComparePrefix(key.begin(), LowerBound_->begin(), prefixLength);
                return result == 0;
            });

        LowerBoundCmpRange_ = MakeRange(begin, lowerBoundCmpEnd);
    }
    if (UpperBound_) {
        auto* upperBoundCmpBegin = std::lower_bound(begin, end, nullptr,
            [&] (TLegacyKey key, nullptr_t) {
                int prefixLength = std::min<int>(key.GetCount(), UpperBound_->Size());
                int result = ComparePrefix(key.begin(), UpperBound_->begin(), prefixLength);
                return result != 0;
            });

        UpperBoundCmpRange_ = MakeRange(upperBoundCmpBegin, end);
    }

    PrefixRange_ = MakeRange(begin, end);
}

void THorizontalSchemalessLookupChunkReaderBase::ComputeBlockIndexes(TRange<TLegacyKey> keys)
{
    for (const auto& key : keys) {
        TReadLimit lowerLimit(TKeyBound::FromRowUnchecked() >= key);
        TReadLimit upperLimit(TKeyBound::FromRowUnchecked() <= key);

        int lowIndex = ApplyLowerKeyLimit(BlockLastKeys_, lowerLimit, SortOrders_, CommonKeyPrefix_);
        int highIndex = ApplyUpperKeyLimit(BlockLastKeys_, upperLimit, SortOrders_, CommonKeyPrefix_);

        if (lowIndex == BlockMetaExt_->data_blocks_size()) {
            break;
        }

        if (BlockIndexes_.empty() || highIndex > BlockIndexes_.back() + 1) {
            int startIndex = lowIndex;
            if (!BlockIndexes_.empty() && BlockIndexes_.back() >= startIndex) {
                startIndex = BlockIndexes_.back() + 1;
            }

            for (int index = startIndex; index < highIndex; ++index) {
                BlockIndexes_.push_back(index);
            }
        }
    }
}

void THorizontalSchemalessLookupChunkReaderBase::InitFirstBlock()
{
    int blockIndex = BlockIndexes_[CurrentBlockIndex_];
    const auto& blockMeta = BlockMetaExt_->data_blocks(blockIndex);

    BlockReader_.reset(new THorizontalBlockReader(
        CurrentBlock_.Get().ValueOrThrow().Data,
        blockMeta,
        GetCompositeColumnFlags(ChunkMeta_->ChunkSchema()),
        GetHunkColumnFlags(ChunkMeta_->GetChunkFormat(), ChunkMeta_->GetChunkFeatures(), ChunkMeta_->ChunkSchema()),
        ChunkMeta_->HunkChunkRefs(),
        ChunkMeta_->HunkChunkMetas(),
        ChunkToReaderIdMapping_,
        SortOrders_,
        CommonKeyPrefix_,
        KeyWideningOptions_,
        GetRootSystemColumnCount()));
}

void THorizontalSchemalessLookupChunkReaderBase::InitNextBlock()
{
    ++CurrentBlockIndex_;
    InitFirstBlock();
}

THorizontalSchemalessLookupChunkReader::THorizontalSchemalessLookupChunkReader(
    const TChunkStatePtr& chunkState,
    const TColumnarChunkMetaPtr& chunkMeta,
    const TChunkReaderConfigPtr& config,
    const TNameTablePtr& nameTable,
    NChunkClient::IChunkReaderPtr underlyingReader,
    const std::vector<int>& chunkToReaderIdMapping,
    int rowIndexId,
    const TReaderVirtualValues& virtualValues,
    const TClientChunkReadOptions& chunkReadOptions,
    TRange<ESortOrder> sortOrders,
    int commonKeyPrefix,
    const TKeyWideningOptions& keyWideningOptions,
    const TSharedRange<TLegacyKey>& keys,
    std::optional<int> partitionTag,
    const TChunkReaderMemoryManagerHolderPtr& memoryManagerHolder)
    : THorizontalSchemalessLookupChunkReaderBase(
        chunkState,
        chunkMeta,
        config,
        nameTable,
        underlyingReader,
        chunkToReaderIdMapping,
        rowIndexId,
        virtualValues,
        chunkReadOptions,
        sortOrders,
        commonKeyPrefix,
        keyWideningOptions,
        keys,
        partitionTag,
        memoryManagerHolder)
{
    const auto& misc = ChunkMeta_->Misc();
    if (!misc.unique_keys()) {
        THROW_ERROR_EXCEPTION("Requested lookup for a chunk without \"unique_keys\" restriction");
    }

    for (const auto& key : Keys_) {
        TReadLimit readLimit(TKeyBound::FromRowUnchecked() >= key);

        int index = ApplyLowerKeyLimit(BlockLastKeys_, readLimit, SortOrders_, CommonKeyPrefix_);
        if (index == BlockMetaExt_->data_blocks_size()) {
            break;
        }

        if (BlockIndexes_.empty() || BlockIndexes_.back() != index) {
            BlockIndexes_.push_back(index);
        }
    }

    InitBlocks();
}

THorizontalSchemalessKeyRangesChunkReader::THorizontalSchemalessKeyRangesChunkReader(
    const TChunkStatePtr& chunkState,
    const TColumnarChunkMetaPtr& chunkMeta,
    const TChunkReaderConfigPtr& config,
    const TNameTablePtr& nameTable,
    NChunkClient::IChunkReaderPtr underlyingReader,
    const std::vector<int>& chunkToReaderIdMapping,
    int rowIndexId,
    const TReaderVirtualValues& virtualValues,
    const TClientChunkReadOptions& chunkReadOptions,
    TRange<ESortOrder> sortOrders,
    int commonKeyPrefix,
    const TKeyWideningOptions& keyWideningOptions,
    const TSharedRange<TLegacyKey>& keys,  // must be sorted.
    std::optional<int> partitionTag,
    const TChunkReaderMemoryManagerHolderPtr& memoryManagerHolder)
    : THorizontalSchemalessLookupChunkReaderBase(
        chunkState,
        chunkMeta,
        config,
        nameTable,
        underlyingReader,
        chunkToReaderIdMapping,
        rowIndexId,
        virtualValues,
        chunkReadOptions,
        sortOrders,
        commonKeyPrefix,
        keyWideningOptions,
        keys,
        partitionTag,
        memoryManagerHolder)
{
    YT_LOG_DEBUG("Looking up prefix-ranges (Count: %v)", std::ssize(keys));

    ApplyLimits();
    ComputeBlockIndexes(PrefixRange_);
    InitBlocks();
}

IUnversionedRowBatchPtr THorizontalSchemalessLookupChunkReader::Read(const TRowBatchReadOptions& options)
{
    // Note that Read() implementation does not use PrefixRange_ as computed by ApplyLimits(),
    // i.e. does not respect read limits from the ChunkSpec.
    // TODO YT-18250 fix this.

    TCurrentTraceContextGuard traceGuard(TraceContext_);
    auto readGuard = AcquireReadGuard();

    MemoryPool_.Clear();

    std::vector<TUnversionedRow> rows;
    rows.reserve(
        std::min(
            std::ssize(Keys_) - RowCount_,
            options.MaxRowsPerRead));

    i64 dataWeight = 0;

    auto success = [&] () {
        if (!BeginRead()) {
            // Not ready yet.
            return true;
        }

        // Nothing to read from chunk.
        if (RowCount_ == std::ssize(Keys_)) {
            return false;
        }

        // BlockReader_ is not set when there are no blocks.
        if (!HasMoreBlocks_ || !BlockReader_) {
            while (rows.size() < rows.capacity()) {
                YT_VERIFY(RowCount_ < std::ssize(Keys_));
                rows.push_back(TUnversionedRow());
                ++RowCount_;
            }
            return true;
        }

        while (
            rows.size() < rows.capacity() &&
            dataWeight < options.MaxDataWeightPerRead)
        {
            YT_VERIFY(RowCount_ < std::ssize(Keys_));

            auto key = Keys_[RowCount_];
            if (!BlockReader_->SkipToKey(key)) {
                HasMoreBlocks_ = OnBlockEnded();
                return true;
            }

            if (key == BlockReader_->GetLegacyKey()) {
                auto row = BlockReader_->GetRow(&MemoryPool_);
                rows.push_back(row);
                dataWeight += GetDataWeight(row);

                int blockIndex = BlockIndexes_[CurrentBlockIndex_];
                const auto& blockMeta = BlockMetaExt_->data_blocks(blockIndex);
                RowIndex_ = blockMeta.chunk_row_count() - blockMeta.row_count() + BlockReader_->GetRowIndex();
            } else {
                rows.push_back(TUnversionedRow());
            }

            ++RowCount_;
        }

        return true;
    }();

    DataWeight_ += dataWeight;

    return success
        ? CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)))
        : nullptr;
}

IUnversionedRowBatchPtr THorizontalSchemalessKeyRangesChunkReader::Read(const TRowBatchReadOptions& options) {
    TCurrentTraceContextGuard traceGuard(TraceContext_);
    auto readGuard = AcquireReadGuard();

    MemoryPool_.Clear();

    std::vector<TUnversionedRow> rows;
    i64 dataWeight = 0;

    if (!BeginRead()) {
        // Not ready yet, retry.
        return CreateBatchFromUnversionedRows(MakeSharedRange(
            std::vector<TUnversionedRow>{},
            MakeStrong(this)));

    }

    if (!BlockReader_) {
        return nullptr;
    }

    if (BlockEnded_) {
        BlockReader_.reset();
        OnBlockEnded();
        return CreateBatchFromUnversionedRows(MakeSharedRange(
            std::vector<TUnversionedRow>{},
            MakeStrong(this)));
    }

    i64 initialRowCount = RowCount_;

    while (!BlockEnded_ &&
            KeyIndex_ < std::ssize(PrefixRange_) &&
            dataWeight < options.MaxDataWeightPerRead &&
            RowCount_ - initialRowCount < options.MaxRowsPerRead)
    {
        auto key = PrefixRange_[KeyIndex_];

        if (!BlockReader_->SkipToKey(key)) {
            BlockEnded_ = true;
            break;
        }

        const ui32 keyLength = key.GetCount();
        bool keyMatching = true;

        while (!BlockEnded_ &&
                dataWeight < options.MaxDataWeightPerRead &&
                RowCount_ - initialRowCount < options.MaxRowsPerRead)
        {
            auto fullKey = BlockReader_->GetLegacyKey();
            if (fullKey.GetCount() < keyLength ||
                CompareValueRanges(key.Elements(), fullKey.FirstNElements(keyLength)) != 0)
            {
                keyMatching = false;
                break;
            }

            const TRange<TUnversionedValue> keyValues(fullKey.begin(), fullKey.end());

            int blockIndex = BlockIndexes_[CurrentBlockIndex_];
            const auto& blockMeta = BlockMetaExt_->data_blocks(blockIndex);
            RowIndex_ = blockMeta.chunk_row_count() - blockMeta.row_count() + BlockReader_->GetRowIndex();

            bool skipRow = false;

            if (LowerBound_ && IsInsideRange(LowerBoundCmpRange_, &PrefixRange_[KeyIndex_]) &&
                !TestKeyWithWidening(keyValues, *LowerBound_, SortOrders_))
            {
                skipRow = true;
            }
            if (UpperBound_ && IsInsideRange(UpperBoundCmpRange_, &PrefixRange_[KeyIndex_]) &&
                !TestKeyWithWidening(keyValues, *UpperBound_, SortOrders_))
            {
                skipRow = true;
            }

            if (!skipRow) {
                auto row = BlockReader_->GetRow(&MemoryPool_);
                AddExtraValues(row, GetTableRowIndex());

                rows.push_back(row);
                ++RowCount_;
                dataWeight += GetDataWeight(row);
            }

            if (!BlockReader_->NextRow()) {
                BlockEnded_ = true;
            }
        }

        if (!keyMatching) {
            ++KeyIndex_;
        }
    }

    if (PrefixRange_.begin() + KeyIndex_ == PrefixRange_.end()) {
        BlockEnded_ = true;
    }

    DataWeight_ += dataWeight;

    return CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
}

////////////////////////////////////////////////////////////////////////////////

template <class TBase>
class TColumnarUnversionedRangeChunkReader
    : public TBase
{
public:
    using TBase::TBase;

    using TBase::Columns_;
    using TBase::ChunkMeta_;

    void InitializeColumnReaders(
        int chunkKeyColumnCount,
        const std::vector<int>& columnIdMapping,
        const TKeyWideningOptions& keyWideningOptions,
        TRange<ESortOrder> sortOrders)
    {
        const auto& chunkSchema = *ChunkMeta_->ChunkSchema();
        const auto& columnMeta = ChunkMeta_->ColumnMeta();

        // Cannot read more key columns than stored in chunk, even if range keys are longer.
        chunkKeyColumnCount = std::min(chunkKeyColumnCount, chunkSchema.GetKeyColumnCount());

        auto initKeyColumns = [&] {
            // Initialize key column readers.
            for (int keyColumnIndex = 0; keyColumnIndex < chunkKeyColumnCount; ++keyColumnIndex) {
                auto columnReader = CreateUnversionedColumnReader(
                    chunkSchema.Columns()[keyColumnIndex],
                    columnMeta->columns(keyColumnIndex),
                    keyColumnIndex,
                    keyColumnIndex,
                    sortOrders[keyColumnIndex]);
                KeyColumnReaders_.push_back(columnReader.get());
                Columns_.emplace_back(std::move(columnReader), keyColumnIndex);
            }

            for (int keyColumnIndex = chunkKeyColumnCount; keyColumnIndex < std::ssize(sortOrders); ++keyColumnIndex) {
                auto columnReader = CreateBlocklessUnversionedNullColumnReader(
                    keyColumnIndex,
                    keyColumnIndex,
                    sortOrders[keyColumnIndex]);
                KeyColumnReaders_.push_back(columnReader.get());
                Columns_.emplace_back(std::move(columnReader), -1);
            }
        };

        // Initialize value column readers.
        int schemafulColumnCount = chunkSchema.GetColumnCount();

        int valueIndex = 0;

        auto pushNullValue = [&](int columnId) {
            auto columnReader = CreateBlocklessUnversionedNullColumnReader(
                valueIndex,
                columnId,
                /*sortOrder*/ std::nullopt);
            RowColumnReaders_.push_back(columnReader.get());
            Columns_.emplace_back(std::move(columnReader), -1, columnId);
            ++valueIndex;
        };

        auto pushRegularValue = [&](int columnIndex) {
            auto columnId = columnIdMapping[columnIndex];
            if (columnId == -1) {
                return;
            }

            auto columnReader = CreateUnversionedColumnReader(
                chunkSchema.Columns()[columnIndex],
                columnMeta->columns(columnIndex),
                valueIndex,
                columnId,
                /*sortOrder*/ std::nullopt);
            RowColumnReaders_.push_back(columnReader.get());
            Columns_.emplace_back(std::move(columnReader), columnIndex, columnId);
            ++valueIndex;
        };

        if (keyWideningOptions.InsertPosition < 0) {
            for (int columnIndex = 0; columnIndex < schemafulColumnCount; ++columnIndex) {
                pushRegularValue(columnIndex);
            }
        } else {
            for (int columnIndex = 0; columnIndex < keyWideningOptions.InsertPosition; ++columnIndex) {
                pushRegularValue(columnIndex);
            }
            for (int columnId : keyWideningOptions.InsertedColumnIds) {
                pushNullValue(columnId);
            }
            for (int columnIndex = keyWideningOptions.InsertPosition; columnIndex < schemafulColumnCount; ++columnIndex) {
                pushRegularValue(columnIndex);
            }
        }

        if (HasColumnsInMapping(MakeRange(columnIdMapping).Slice(
            size_t(schemafulColumnCount),
            columnIdMapping.size())))
        {
            auto schemalessChunkColumnIndex = schemafulColumnCount;

            auto columnReader = CreateSchemalessColumnReader(
                columnMeta->columns(schemalessChunkColumnIndex),
                columnIdMapping);
            SchemalessReader_ = columnReader.get();
            Columns_.emplace_back(std::move(columnReader), schemalessChunkColumnIndex);
        }

        initKeyColumns();
    }

protected:
    std::vector<IUnversionedColumnReader*> RowColumnReaders_;
    std::vector<IUnversionedColumnReader*> KeyColumnReaders_;
    ISchemalessColumnReader* SchemalessReader_ = nullptr;

    TChunkedMemoryPool MemoryPool_;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TColumnarSchemalessRangeChunkReader)

class TColumnarSchemalessRangeChunkReader
    : public TSchemalessChunkReaderBase
    , public TColumnarUnversionedRangeChunkReader<TColumnarRangeChunkReaderBase>
{
public:
    TColumnarSchemalessRangeChunkReader(
        const TChunkStatePtr& chunkState,
        const TColumnarChunkMetaPtr& chunkMeta,
        const TChunkReaderConfigPtr& config,
        const TNameTablePtr& nameTable,
        IChunkReaderPtr underlyingReader,
        const std::vector<int>& chunkToReaderIdMapping,
        int rowIndexId,
        const TReaderVirtualValues& virtualValues,
        const TClientChunkReadOptions& chunkReadOptions,
        TRange<ESortOrder> sortOrders,
        int commonKeyPrefix,
        const TKeyWideningOptions& keyWideningOptions,
        const TReadRange& readRange,
        const TChunkReaderMemoryManagerHolderPtr& memoryManagerHolder,
        std::optional<i64> virtualRowIndex,
        int interruptDescriptorKeyLength)
        : TSchemalessChunkReaderBase(
            chunkState->ChunkSpec,
            config,
            nameTable,
            underlyingReader->GetChunkId(),
            chunkToReaderIdMapping,
            rowIndexId,
            virtualValues,
            chunkReadOptions,
            virtualRowIndex)
        , TColumnarUnversionedRangeChunkReader<TColumnarRangeChunkReaderBase>(
            chunkMeta,
            chunkState->DataSource,
            config,
            underlyingReader,
            sortOrders,
            commonKeyPrefix,
            chunkState->BlockCache,
            chunkReadOptions,
            BIND(&TColumnarSchemalessRangeChunkReader::OnRowsSkipped, MakeWeak(this)),
            memoryManagerHolder)
        , InterruptDescriptorKeyLength_(interruptDescriptorKeyLength)
    {
        TCurrentTraceContextGuard traceGuard(TraceContext_);

        YT_LOG_DEBUG("Reading range of a chunk (Range: %v)", readRange);

        LowerLimit_ = readRange.LowerLimit();
        UpperLimit_ = readRange.UpperLimit();

        RowIndex_ = LowerLimit_.GetRowIndex().value_or(0);

        YT_VERIFY(ChunkMeta_->GetChunkFormat() == EChunkFormat::TableUnversionedColumnar);

        bool sortedRead = SortOrders_.size() > 0;

        if (sortedRead && !ChunkMeta_->Misc().sorted()) {
            THROW_ERROR_EXCEPTION("Requested a sorted read for an unsorted chunk");
        }

        InitializeColumnReaders(
            CommonKeyPrefix_,
            ChunkToReaderIdMapping_,
            keyWideningOptions,
            SortOrders_);

        YT_VERIFY(std::ssize(KeyColumnReaders_) == std::ssize(SortOrders_));

        InitLowerRowIndex();
        InitUpperRowIndex();

        YT_LOG_DEBUG("Initialized row index limits (LowerRowIndex: %v, SafeUpperRowIndex: %v, HardUpperRowIndex: %v)",
            LowerRowIndex_,
            SafeUpperRowIndex_,
            HardUpperRowIndex_);

        if (LowerRowIndex_ >= HardUpperRowIndex_) {
            Completed_ = true;
            return;
        }

        // We must continue initialization and set RowIndex_ before
        // ReadyEvent is set for the first time.
        InitBlockFetcher();

        // NB: We must complete initialization before ReadyEvent is set in the constructor.
        SetReadyEvent(
            RequestFirstBlocks()
            .Apply(BIND([this, this_ = MakeStrong(this)] {
                FeedBlocksToReaders();
                Initialize(MakeRange(KeyColumnReaders_));
                RowIndex_ = LowerRowIndex_;
                LowerKeyLimitReached_ = !LowerLimit_.KeyBound();

                YT_LOG_DEBUG("Initialized start row index (LowerKeyLimitReached: %v, RowIndex: %v)",
                    LowerKeyLimitReached_,
                    RowIndex_);

                if (RowIndex_ >= HardUpperRowIndex_) {
                    Completed_ = true;
                }
                if (IsSamplingCompleted()) {
                    Completed_ = true;
                }
            })));
    }

    ~TColumnarSchemalessRangeChunkReader()
    {
        YT_LOG_DEBUG("Columnar reader timing statistics (TimingStatistics: %v)", TTimingReaderBase::GetTimingStatistics());
    }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        TCurrentTraceContextGuard traceGuard(TraceContext_);
        auto readGuard = AcquireReadGuard();

        if (PendingUnmaterializedRowCount_ > 0) {
            // Previous batch was not materialized, readers did not advance.
            // Perform a fake materialization.
            std::vector<IUnversionedColumnarRowBatch::TColumn> allBatchColumns;
            std::vector<const IUnversionedColumnarRowBatch::TColumn*> rootBatchColumns;
            MaterializeColumns(PendingUnmaterializedRowCount_, &allBatchColumns, &rootBatchColumns);
        }
        YT_VERIFY(PendingUnmaterializedRowCount_ == 0);

        Pool_.Clear();

        if (!ReadyEvent().IsSet() || !ReadyEvent().Get().IsOK()) {
            return CreateEmptyUnversionedRowBatch();
        }

        if (Completed_) {
            return nullptr;
        }

        return CanReadColumnarBatch(options)
            ? ReadColumnarBatch(options)
            : ReadNonColumnarBatch(options);
    }

    TInterruptDescriptor GetInterruptDescriptor(
        TRange<TUnversionedRow> unreadRows) const override
    {
        return GetInterruptDescriptorImpl(
            unreadRows,
            ChunkMeta_->Misc(),
            ChunkSpec_,
            LowerLimit_,
            UpperLimit_,
            RowIndex_,
            InterruptDescriptorKeyLength_);
    }

    TDataStatistics GetDataStatistics() const override
    {
        auto dataStatistics = TColumnarRangeChunkReaderBase::GetDataStatistics();
        dataStatistics.set_row_count(RowCount_);
        dataStatistics.set_data_weight(DataWeight_);
        return dataStatistics;
    }

private:
    bool Completed_ = false;
    bool LowerKeyLimitReached_ = false;
    i64 PendingUnmaterializedRowCount_ = 0;

    TChunkedMemoryPool Pool_;

    const int InterruptDescriptorKeyLength_;


    class TColumnarRowBatch
        : public IUnversionedColumnarRowBatch
    {
    public:
        TColumnarRowBatch(
            TColumnarSchemalessRangeChunkReaderPtr reader,
            i64 rowCount)
            : Reader_(std::move(reader))
            , RowCount_(rowCount)
        { }

        int GetRowCount() const override
        {
            return RowCount_;
        }

        TSharedRange<TUnversionedRow> MaterializeRows() override
        {

            if (RootColumns_) {
                THROW_ERROR_EXCEPTION("Cannot materialize batch into rows since it was already materialized into columns");
            }

            if (!Rows_) {
                Rows_ = Reader_->MaterializeRows(RowCount_);
            }

            return Rows_;
        }

        TRange<const TColumn*> MaterializeColumns() override
        {
            if (Rows_) {
                THROW_ERROR_EXCEPTION("Cannot materialize batch into columns since it was already materialized into rows");
            }

            if (!RootColumns_) {
                Reader_->MaterializeColumns(
                    RowCount_,
                    &AllColumns_,
                    &RootColumns_.emplace());
            }

            return MakeRange(*RootColumns_);
        }

        TRange<TDictionaryId> GetRetiringDictionaryIds() const override
        {
            return {};
        }

    private:
        const TColumnarSchemalessRangeChunkReaderPtr Reader_;
        const i64 RowCount_;

        TSharedRange<TUnversionedRow> Rows_;
        std::optional<std::vector<const TColumn*>> RootColumns_;
        std::vector<TColumn> AllColumns_;
    };


    i64 ReadPrologue(i64 maxRowsPerRead)
    {
        FeedBlocksToReaders();
        ArmColumnReaders();

        // Compute the number rows to read.
        i64 rowLimit = maxRowsPerRead;
        // Each read must be fully below or fully above SafeUpperRowLimit,
        // to determine if we should read and validate keys.
        rowLimit = std::min(
            rowLimit,
            RowIndex_ < SafeUpperRowIndex_
                ? SafeUpperRowIndex_ - RowIndex_
                : HardUpperRowIndex_ - RowIndex_);
        // Cap by ready row indexes.
        rowLimit = std::min(rowLimit, GetReadyRowCount());
        YT_VERIFY(rowLimit > 0);

        auto adjustRowLimit = [&] (TRange<TUnversionedRow> keys){
            // Keys are already widened here.
            while (rowLimit > 0 &&
                !TestKey(ToKeyRef(keys[rowLimit - 1]), ToKeyBoundRef(UpperLimit_.KeyBound()), SortOrders_))
            {
                --rowLimit;
                Completed_ = true;
            }
        };

        if (!LowerKeyLimitReached_) {
            auto keys = ReadKeys(rowLimit);

            i64 deltaIndex = 0;
            if (LowerLimit_.KeyBound()) {
                // Keys are already widened here.
                while (deltaIndex < rowLimit &&
                    !TestKey(ToKeyRef(keys[deltaIndex]), ToKeyBoundRef(LowerLimit_.KeyBound()), SortOrders_))
                {
                    ++deltaIndex;
                }
            }

            rowLimit -= deltaIndex;
            RowIndex_ += deltaIndex;

            // Rewind row column readers to the proper row index.
            for (const auto& reader : RowColumnReaders_) {
                reader->SkipToRowIndex(RowIndex_);
            }
            if (SchemalessReader_) {
                SchemalessReader_->SkipToRowIndex(RowIndex_);
            }

            LowerKeyLimitReached_ = (rowLimit > 0);

            // We could have overcome upper limit, we must check it.
            if (RowIndex_ >= SafeUpperRowIndex_ && UpperLimit_.KeyBound()) {
                auto keyRange = MakeRange(keys.data() + deltaIndex, keys.data() + keys.size());

                adjustRowLimit(keyRange);
            }
        } else if (RowIndex_ >= SafeUpperRowIndex_ && UpperLimit_.KeyBound()) {
            auto keys = ReadKeys(rowLimit);

            adjustRowLimit(keys);
        } else {
            // We do not read keys, so we must skip rows for key readers.
            for (const auto& reader : KeyColumnReaders_) {
                reader->SkipToRowIndex(RowIndex_ + rowLimit);
            }
        }

        return rowLimit;
    }

    void ReadEpilogue(std::vector<TUnversionedRow>* rows)
    {
        if (TSchemalessChunkReaderBase::Config_->SamplingRate) {
            i64 insertIndex = 0;
            for (i64 rowIndex = 0; rowIndex < std::ssize(*rows); ++rowIndex) {
                i64 tableRowIndex = ChunkSpec_.table_row_index() + RowIndex_ - rows->size() + rowIndex;
                if (SampleRow(tableRowIndex)) {
                    (*rows)[insertIndex] = (*rows)[rowIndex];
                    ++insertIndex;
                }
            }
            rows->resize(insertIndex);
        }

        i64 dataWeight = 0;
        for (auto row : *rows) {
            dataWeight += GetDataWeight(row);
        }

        RowCount_ += rows->size();
        DataWeight_ += dataWeight;
    }

    void AdvanceRowIndex(i64 rowCount)
    {
        RowIndex_ += rowCount;
        if (RowIndex_ == HardUpperRowIndex_) {
            Completed_ = true;
        }
    }

    bool CanReadColumnarBatch(const TRowBatchReadOptions& options)
    {
        return options.Columnar && !TSchemalessChunkReaderBase::Config_->SamplingRate;
    }

    IUnversionedRowBatchPtr ReadColumnarBatch(const TRowBatchReadOptions& options)
    {
        PendingUnmaterializedRowCount_ = ReadPrologue(options.MaxRowsPerRead);
        return New<TColumnarRowBatch>(this, PendingUnmaterializedRowCount_);
    }

    void MaterializePrologue(i64 rowCount)
    {
        YT_VERIFY(PendingUnmaterializedRowCount_ == rowCount);
        PendingUnmaterializedRowCount_ = 0;
    }

    TSharedRange<TUnversionedRow> MaterializeRows(i64 rowCount)
    {
        MaterializePrologue(rowCount);

        std::vector<TUnversionedRow> rows;
        rows.reserve(rowCount);
        ReadRows(rowCount, &rows);

        if (!Completed_) {
            TryFetchNextRow();
        }

        ReadEpilogue(&rows);

        return MakeSharedRange(std::move(rows), MakeStrong(this));
    }

    void MaterializeColumns(
        i64 rowCount,
        std::vector<IUnversionedColumnarRowBatch::TColumn>* allBatchColumns,
        std::vector<const IUnversionedColumnarRowBatch::TColumn*>* rootBatchColumns)
    {
        MaterializePrologue(rowCount);

        i64 dataWeight = rowCount; // +1 for each row
        for (const auto& reader : RowColumnReaders_) {
            dataWeight += reader->EstimateDataWeight(RowIndex_, RowIndex_ + rowCount);
        }

        int batchColumnCount = 0;
        for (int index = 0; index < std::ssize(RowColumnReaders_); ++index) {
            const auto& reader = RowColumnReaders_[index];
            batchColumnCount += reader->GetBatchColumnCount();
        }

        allBatchColumns->resize(batchColumnCount + GetLeafSystemColumnCount());
        rootBatchColumns->reserve(RowColumnReaders_.size() + GetRootSystemColumnCount());
        int currentBatchColumnIndex = 0;
        for (int index = 0; index < std::ssize(RowColumnReaders_); ++index) {
            const auto& reader = RowColumnReaders_[index];

            // FIXME: Columns_[index]
            const auto& column = Columns_[index];
            YT_VERIFY(column.ColumnReader.get() == reader);

            int columnCount = reader->GetBatchColumnCount();
            auto columnRange = TMutableRange<IUnversionedColumnarRowBatch::TColumn>(
                allBatchColumns->data() + currentBatchColumnIndex,
                columnCount);
            auto* rootColumn = &columnRange.Front();
            rootColumn->Id = column.ColumnId;
            rootColumn->Type = ChunkMeta_->ChunkSchema()->Columns()[column.ColumnMetaIndex].LogicalType();
            rootBatchColumns->push_back(rootColumn);
            reader->ReadColumnarBatch(columnRange, rowCount);
            currentBatchColumnIndex += columnCount;
        }

        for (int index = 0; index < std::ssize(VirtualValues_.Values()); ++index) {
            int columnCount = VirtualValues_.GetBatchColumnCount(index);
            auto columnRange = TMutableRange<IUnversionedColumnarRowBatch::TColumn>(
                allBatchColumns->data() + currentBatchColumnIndex,
                columnCount);
            VirtualValues_.FillColumns(columnRange, index, RowIndex_, rowCount);
            currentBatchColumnIndex += columnCount;
            rootBatchColumns->push_back(&columnRange.Front());
        }

        // Add row_index column.
        if (RowIndexId_ != -1) {
            auto dataValues = TMutableRange<ui64>(Pool_.AllocateUninitialized<ui64>(rowCount), rowCount);
            for (ssize_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                dataValues[rowIndex] = ZigZagEncode64(GetTableRowIndex() + rowIndex);
            }

            ReadColumnarIntegerValues(
                allBatchColumns->data() + currentBatchColumnIndex,
                /*startIndex*/ 0,
                rowCount,
                NTableClient::EValueType::Int64,
                /*baseValue*/ 0,
                dataValues);
            auto* rootColumn = allBatchColumns->data() + currentBatchColumnIndex;
            rootColumn->Id = RowIndexId_;
            rootColumn->Type =  MakeLogicalType(ESimpleLogicalValueType::Int64, /*required*/ false);
            rootBatchColumns->push_back(rootColumn);
            currentBatchColumnIndex += 1;
        }

        AdvanceRowIndex(rowCount);

        RowCount_ += rowCount;
        DataWeight_ += dataWeight;

        if (!Completed_) {
            TryFetchNextRow();
            if (IsSamplingCompleted()) {
                Completed_ = true;
            }
        }
    }

    IUnversionedRowBatchPtr ReadNonColumnarBatch(const TRowBatchReadOptions& options)
    {
        std::vector<TUnversionedRow> rows;
        rows.reserve(options.MaxRowsPerRead);

        i64 rowsDataWeight = 0;
        while (std::ssize(rows) < options.MaxRowsPerRead &&
            rowsDataWeight < options.MaxDataWeightPerRead)
        {
            i64 rowCount = ReadPrologue(rows.capacity() - rows.size());
            ReadRows(rowCount, &rows);
            for (i64 rowIndex = std::ssize(rows) - rowCount; rowIndex < std::ssize(rows); ++rowIndex) {
                rowsDataWeight += GetDataWeight(rows[rowIndex]);
            }
            if (Completed_ || !TryFetchNextRow()) {
                break;
            }
        }

        if (IsSamplingCompleted()) {
            Completed_ = true;
        }

        ReadEpilogue(&rows);

        return CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
    }

    std::vector<TUnversionedRow> ReadKeys(i64 rowCount)
    {
        std::vector<TMutableUnversionedRow> mutableKeys;
        mutableKeys.reserve(rowCount);
        for (i64 index = 0; index < rowCount; ++index) {
            auto key = TLegacyMutableKey::Allocate(
                &Pool_,
                KeyColumnReaders_.size());
            key.SetCount(KeyColumnReaders_.size());
            mutableKeys.push_back(key);
        }

        auto keyRange = TMutableRange<TMutableUnversionedRow>(
            static_cast<TMutableUnversionedRow*>(mutableKeys.data()),
            static_cast<TMutableUnversionedRow*>(mutableKeys.data() + rowCount));

        for (const auto& columnReader : KeyColumnReaders_) {
            columnReader->ReadValues(keyRange);
        }

        std::vector<TUnversionedRow> keys;
        keys.reserve(rowCount);

        for (const auto& key : mutableKeys) {
            keys.push_back(key);
        }
        return keys;
    }

    void ReadRows(i64 rowCount, std::vector<TUnversionedRow>* rows)
    {
        // Compute value counts per row in schemaless case.
        std::vector<ui32> schemalessColumnCounts;
        if (SchemalessReader_) {
            schemalessColumnCounts.resize(rowCount);
            SchemalessReader_->ReadValueCounts(TMutableRange<ui32>(
                schemalessColumnCounts.data(),
                schemalessColumnCounts.size()));
        }

        // Allocate rows.
        auto rowRangeBegin = rows->size();
        for (i64 index = 0; index < rowCount; ++index) {
            auto row = TMutableUnversionedRow::Allocate(
                &Pool_,
                RowColumnReaders_.size() + (SchemalessReader_ ? schemalessColumnCounts[index] : 0) + GetRootSystemColumnCount());
            row.SetCount(RowColumnReaders_.size());
            rows->push_back(row);
        }
        auto rowRange = TMutableRange<TMutableUnversionedRow>(
            static_cast<TMutableUnversionedRow*>(rows->data() + rowRangeBegin),
            rowCount);

        // Read values schemaful values.
        for (const auto& columnReader : RowColumnReaders_) {
            columnReader->ReadValues(rowRange);
        }

        // Read values schemaless values.
        if (SchemalessReader_) {
            SchemalessReader_->ReadValues(rowRange);
        }

        // Fill system columns.
        for (i64 index = 0; index < rowCount; ++index) {
            auto row = rowRange[index];
            AddExtraValues(row, GetTableRowIndex() + index);
        }

        AdvanceRowIndex(rowCount);
    }

    void OnRowsSkipped(int rowCount)
    {
        AdvanceRowIndex(rowCount);
    }
};

DEFINE_REFCOUNTED_TYPE(TColumnarSchemalessRangeChunkReader)

////////////////////////////////////////////////////////////////////////////////

class TColumnarSchemalessLookupChunkReader
    : public TSchemalessChunkReaderBase
    , public TColumnarUnversionedRangeChunkReader<TColumnarLookupChunkReaderBase>
{
public:
    TColumnarSchemalessLookupChunkReader(
        const TChunkStatePtr& chunkState,
        const TColumnarChunkMetaPtr& chunkMeta,
        const TChunkReaderConfigPtr& config,
        const TNameTablePtr& nameTable,
        IChunkReaderPtr underlyingReader,
        const std::vector<int>& chunkToReaderIdMapping,
        int rowIndexId,
        const TReaderVirtualValues& virtualValues,
        const TClientChunkReadOptions& chunkReadOptions,
        TRange<ESortOrder> sortOrders,
        int commonKeyPrefix,
        const TKeyWideningOptions& keyWideningOptions,
        const TSharedRange<TLegacyKey>& keys,
        const TChunkReaderMemoryManagerHolderPtr& memoryManagerHolder)
        : TSchemalessChunkReaderBase(
            chunkState->ChunkSpec,
            config,
            nameTable,
            underlyingReader->GetChunkId(),
            chunkToReaderIdMapping,
            rowIndexId,
            virtualValues,
            chunkReadOptions)
        , TColumnarUnversionedRangeChunkReader<TColumnarLookupChunkReaderBase>(
            chunkMeta,
            chunkState->DataSource,
            config,
            underlyingReader,
            sortOrders,
            commonKeyPrefix,
            chunkState->BlockCache,
            chunkReadOptions,
            [] (int) { YT_ABORT(); }, // Rows should not be skipped in lookup reader.
            memoryManagerHolder)
    {
        TCurrentTraceContextGuard traceGuard(TraceContext_);

        Keys_ = keys;

        YT_VERIFY(ChunkMeta_->GetChunkFormat() == EChunkFormat::TableUnversionedColumnar);

        if (!ChunkMeta_->Misc().sorted()) {
            THROW_ERROR_EXCEPTION("Requested a sorted read for an unsorted chunk");
        }

        InitializeColumnReaders(
            CommonKeyPrefix_,
            ChunkToReaderIdMapping_,
            keyWideningOptions,
            SortOrders_);

        Initialize();

        // NB: We must complete initialization before ReadyEvent is set in the constructor.
        SetReadyEvent(RequestFirstBlocks());
    }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        TCurrentTraceContextGuard traceGuard(TraceContext_);
        auto readGuard = AcquireReadGuard();

        Pool_.Clear();

        if (!ReadyEvent().IsSet() || !ReadyEvent().Get().IsOK()) {
            return CreateEmptyUnversionedRowBatch();
        }

        if (NextKeyIndex_ == std::ssize(Keys_)) {
            return nullptr;
        }

        std::vector<TUnversionedRow> rows;
        rows.reserve(options.MaxRowsPerRead);
        i64 dataWeight = 0;

        while (std::ssize(rows) < options.MaxRowsPerRead &&
               dataWeight < options.MaxDataWeightPerRead)
        {
            FeedBlocksToReaders();

            if (RowIndexes_[NextKeyIndex_] < ChunkMeta_->Misc().row_count()) {
                auto key = Keys_[NextKeyIndex_];
                YT_VERIFY(key.GetCount() == KeyColumnReaders_.size());

                // Reading row.
                i64 lowerRowIndex = KeyColumnReaders_[0]->GetCurrentRowIndex();
                i64 upperRowIndex = KeyColumnReaders_[0]->GetBlockUpperRowIndex();
                for (int i = 0; i < std::ssize(KeyColumnReaders_); ++i) {
                    std::tie(lowerRowIndex, upperRowIndex) = KeyColumnReaders_[i]->GetEqualRange(
                        key[i],
                        lowerRowIndex,
                        upperRowIndex);
                }

                if (upperRowIndex == lowerRowIndex) {
                    // Key does not exist.
                    rows.push_back(TUnversionedRow());
                } else {
                    // Key can be present in exactly one row.
                    YT_VERIFY(upperRowIndex == lowerRowIndex + 1);
                    i64 rowIndex = lowerRowIndex;
                    rows.push_back(ReadRow(rowIndex));
                }
            } else {
                // Key oversteps chunk boundaries.
                rows.push_back(TUnversionedRow());
            }

            dataWeight += GetDataWeight(rows.back());

            if (++NextKeyIndex_ == std::ssize(Keys_) || !TryFetchNextRow()) {
                break;
            }
        }

        i64 rowCount = rows.size();

        RowCount_ += rowCount;
        DataWeight_ += dataWeight;

        return CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
    }

    TDataStatistics GetDataStatistics() const override
    {
        auto dataStatistics = TColumnarChunkReaderBase::GetDataStatistics();
        dataStatistics.set_row_count(RowCount_);
        dataStatistics.set_data_weight(DataWeight_);
        return dataStatistics;
    }

private:
    TChunkedMemoryPool Pool_;

    TMutableUnversionedRow ReadRow(i64 rowIndex)
    {
        ui32 schemalessColumnCount = 0;

        if (SchemalessReader_) {
            SchemalessReader_->SkipToRowIndex(rowIndex);
            SchemalessReader_->ReadValueCounts(TMutableRange<ui32>(&schemalessColumnCount, 1));
        }

        auto row = TMutableUnversionedRow::Allocate(
            &Pool_,
            RowColumnReaders_.size() + schemalessColumnCount + GetRootSystemColumnCount());
        row.SetCount(RowColumnReaders_.size());

        // Read values.
        auto rowRange = TMutableRange<TMutableUnversionedRow>(&row, 1);

        for (const auto& columnReader : RowColumnReaders_) {
            columnReader->SkipToRowIndex(rowIndex);
            columnReader->ReadValues(rowRange);
        }

        if (SchemalessReader_) {
            SchemalessReader_->ReadValues(rowRange);
        }

        return row;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TReaderParams
{
    int RowIndexId;
    TReaderVirtualValues VirtualColumns;
    std::vector<int> ChunkToReaderIdMapping;
    int CommonKeyPrefix;
    TKeyWideningOptions KeyWideningOptions;

    TReaderParams(
        const TChunkStatePtr& chunkState,
        const TColumnarChunkMetaPtr& chunkMeta,
        TChunkId chunkId,
        const TChunkReaderOptionsPtr& options,
        const TNameTablePtr& nameTable,
        const TColumnFilter& columnFilter,
        const std::vector<TString>& omittedInaccessibleColumns,
        const std::vector<TString>& sortColumnNames,
        std::optional<i64> virtualRowIndex = std::nullopt)
    {
        YT_VERIFY(chunkMeta->GetChunkType() == EChunkType::Table);
        YT_VERIFY(chunkState->TableSchema);

        const auto& columns = chunkMeta->ChunkSchema()->Columns();
        for (int index = 0; index < std::ssize(columns); ++index) {
            auto id = chunkMeta->ChunkNameTable()->FindId(columns[index].StableName().Underlying());
            YT_VERIFY(id);
            YT_VERIFY(*id == index);
        }

        RowIndexId = GetRowIndexId(nameTable, options);

        VirtualColumns = InitializeVirtualColumns(
            options,
            nameTable,
            columnFilter,
            chunkState->ChunkSpec,
            chunkState->VirtualValueDirectory,
            virtualRowIndex);

        TNameTablePtr chunkNameTable = options->DynamicTable
            ? TNameTable::FromSchema(*chunkMeta->ChunkSchema())
            : chunkMeta->ChunkNameTable();

        try {
            ChunkToReaderIdMapping = BuildColumnIdMapping(
                chunkNameTable,
                chunkState->TableSchema,
                nameTable,
                columnFilter,
                omittedInaccessibleColumns);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION(EErrorCode::NameTableUpdateFailed, "Failed to update name table for schemaless chunk reader")
                << TErrorAttribute("chunk_id", chunkId)
                << ex;
        }

        auto chunkSortColumnNames = GetColumnNames(
            chunkMeta->ChunkSchema()->GetSortColumns(
                chunkState->TableSchema->GetNameMapping()));

        CommonKeyPrefix = GetCommonKeyPrefix(
            chunkSortColumnNames,
            sortColumnNames);
        if (options->EnableKeyWidening) {
            KeyWideningOptions = BuildKeyWideningOptions(sortColumnNames, nameTable, CommonKeyPrefix);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

ISchemalessChunkReaderPtr CreateSchemalessRangeChunkReader(
    const TChunkStatePtr& chunkState,
    const TColumnarChunkMetaPtr& chunkMeta,
    TChunkReaderConfigPtr config,
    TChunkReaderOptionsPtr options,
    NChunkClient::IChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    const TClientChunkReadOptions& chunkReadOptions,
    TSortColumns sortColumns,
    const std::vector<TString>& omittedInaccessibleColumns,
    const TColumnFilter& columnFilter,
    const TReadRange& readRange,
    std::optional<int> partitionTag,
    const TChunkReaderMemoryManagerHolderPtr& memoryManagerHolder,
    std::optional<i64> virtualRowIndex,
    int interruptDescriptorKeyLength)
{
    YT_VERIFY(chunkState && chunkState->TableSchema);
    auto chunkSortColumns = chunkMeta->ChunkSchema()->GetSortColumns(
        chunkState->TableSchema->GetNameMapping());
    ValidateSortColumns(sortColumns, chunkSortColumns, options->DynamicTable);

    TReaderParams params(
        chunkState,
        chunkMeta,
        underlyingReader->GetChunkId(),
        options,
        nameTable,
        columnFilter,
        omittedInaccessibleColumns,
        GetColumnNames(sortColumns),
        virtualRowIndex);

    // Check that read ranges suit sortColumns if sortColumns are provided.
    if (readRange.LowerLimit().KeyBound()) {
        YT_VERIFY(readRange.LowerLimit().KeyBound().Prefix.GetCount() <= std::ssize(sortColumns));
    }
    if (readRange.UpperLimit().KeyBound()) {
        YT_VERIFY(readRange.UpperLimit().KeyBound().Prefix.GetCount() <= std::ssize(sortColumns));
    }

    auto sortOrders = GetSortOrders(sortColumns);

    switch (chunkMeta->GetChunkFormat()) {
        case EChunkFormat::TableUnversionedSchemalessHorizontal:
            return New<THorizontalSchemalessRangeChunkReader>(
                chunkState,
                chunkMeta,
                config,
                nameTable,
                underlyingReader,
                params.ChunkToReaderIdMapping,
                params.RowIndexId,
                params.VirtualColumns,
                chunkReadOptions,
                sortOrders,
                params.CommonKeyPrefix,
                params.KeyWideningOptions,
                readRange,
                partitionTag,
                memoryManagerHolder,
                virtualRowIndex,
                interruptDescriptorKeyLength);

        case EChunkFormat::TableUnversionedColumnar:
            return New<TColumnarSchemalessRangeChunkReader>(
                chunkState,
                chunkMeta,
                config,
                nameTable,
                underlyingReader,
                params.ChunkToReaderIdMapping,
                params.RowIndexId,
                params.VirtualColumns,
                chunkReadOptions,
                sortOrders,
                params.CommonKeyPrefix,
                params.KeyWideningOptions,
                readRange,
                memoryManagerHolder,
                virtualRowIndex,
                interruptDescriptorKeyLength);

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessChunkReaderPtr CreateSchemalessLookupChunkReader(
    const TChunkStatePtr& chunkState,
    const TColumnarChunkMetaPtr& chunkMeta,
    TChunkReaderConfigPtr config,
    TChunkReaderOptionsPtr options,
    NChunkClient::IChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    const TClientChunkReadOptions& chunkReadOptions,
    const TSortColumns& sortColumns,
    const std::vector<TString>& omittedInaccessibleColumns,
    const TColumnFilter& columnFilter,
    const TSharedRange<TLegacyKey>& keys,
    std::optional<int> partitionTag,
    const TChunkReaderMemoryManagerHolderPtr& memoryManagerHolder)
{
    YT_VERIFY(chunkState && chunkState->TableSchema);
    auto chunkSortColumns = chunkMeta->ChunkSchema()->GetSortColumns(
        chunkState->TableSchema->GetNameMapping());
    ValidateSortColumns(sortColumns, chunkSortColumns, options->DynamicTable);

    TReaderParams params(
        chunkState,
        chunkMeta,
        underlyingReader->GetChunkId(),
        options,
        nameTable,
        columnFilter,
        omittedInaccessibleColumns,
        GetColumnNames(sortColumns));

    for (auto key : keys) {
        YT_VERIFY(key.GetCount() == sortColumns.size());
    }

    switch (chunkMeta->GetChunkFormat()) {
        case EChunkFormat::TableUnversionedSchemalessHorizontal:
            return New<THorizontalSchemalessLookupChunkReader>(
                chunkState,
                chunkMeta,
                std::move(config),
                nameTable,
                std::move(underlyingReader),
                params.ChunkToReaderIdMapping,
                params.RowIndexId,
                params.VirtualColumns,
                chunkReadOptions,
                GetSortOrders(sortColumns),
                params.CommonKeyPrefix,
                params.KeyWideningOptions,
                keys,
                partitionTag,
                memoryManagerHolder);

        case EChunkFormat::TableUnversionedColumnar:
            return New<TColumnarSchemalessLookupChunkReader>(
                chunkState,
                chunkMeta,
                std::move(config),
                nameTable,
                std::move(underlyingReader),
                params.ChunkToReaderIdMapping,
                params.RowIndexId,
                params.VirtualColumns,
                chunkReadOptions,
                GetSortOrders(sortColumns),
                params.CommonKeyPrefix,
                params.KeyWideningOptions,
                keys,
                memoryManagerHolder);

        default:
            THROW_ERROR_EXCEPTION(
                "This operation is not supported for chunks in %Qv format",
                chunkMeta->GetChunkFormat());
    }
}

ISchemalessChunkReaderPtr CreateSchemalessKeyRangesChunkReader(
    const TChunkStatePtr& chunkState,
    const TColumnarChunkMetaPtr& chunkMeta,
    TChunkReaderConfigPtr config,
    TChunkReaderOptionsPtr options,
    NChunkClient::IChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    const TClientChunkReadOptions& chunkReadOptions,
    const TSortColumns& sortColumns,
    const std::vector<TString>& omittedInaccessibleColumns,
    const TColumnFilter& columnFilter,
    const TSharedRange<TLegacyKey>& keyPrefixes,
    std::optional<int> partitionTag,
    const TChunkReaderMemoryManagerHolderPtr& memoryManagerHolder)
{
    YT_VERIFY(chunkState && chunkState->TableSchema);
    auto chunkSortColumns = chunkMeta->ChunkSchema()->GetSortColumns(
        chunkState->TableSchema->GetNameMapping());
    ValidateSortColumns(sortColumns, chunkSortColumns, options->DynamicTable);

    TReaderParams params(
        chunkState,
        chunkMeta,
        underlyingReader->GetChunkId(),
        options,
        nameTable,
        columnFilter,
        omittedInaccessibleColumns,
        GetColumnNames(sortColumns));

    if (chunkMeta->GetChunkFormat() != EChunkFormat::TableUnversionedSchemalessHorizontal) {
        THROW_ERROR_EXCEPTION("Only horizontal tables supported in the key range reader");
    }

    return New<THorizontalSchemalessKeyRangesChunkReader>(
        chunkState,
        chunkMeta,
        std::move(config),
        nameTable,
        std::move(underlyingReader),
        params.ChunkToReaderIdMapping,
        params.RowIndexId,
        params.VirtualColumns,
        chunkReadOptions,
        GetSortOrders(sortColumns),
        params.CommonKeyPrefix,
        params.KeyWideningOptions,
        keyPrefixes,
        partitionTag,
        memoryManagerHolder);
}
////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
