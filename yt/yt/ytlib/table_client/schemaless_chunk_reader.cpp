#include "schemaless_multi_chunk_reader.h"

#include "cached_versioned_chunk_meta.h"
#include "chunk_reader_base.h"
#include "chunk_state.h"
#include "columnar_chunk_reader_base.h"
#include "config.h"
#include "helpers.h"
#include "overlapping_reader.h"
#include "private.h"
#include "row_merger.h"
#include "schemaless_block_reader.h"
#include "versioned_chunk_reader.h"
#include "remote_dynamic_store_reader.h"
#include "virtual_value_directory.h"

#include <yt/ytlib/api/native/connection.h>
#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/table_chunk_format/public.h>
#include <yt/ytlib/table_chunk_format/column_reader.h>
#include <yt/ytlib/table_chunk_format/null_column_reader.h>

#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/data_source.h>
#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/multi_reader_manager.h>
#include <yt/ytlib/chunk_client/parallel_reader_memory_manager.h>
#include <yt/ytlib/chunk_client/reader_factory.h>
#include <yt/ytlib/chunk_client/replication_reader.h>

#include <yt/ytlib/tablet_client/helpers.h>

#include <yt/ytlib/query_client/column_evaluator.h>

#include <yt/library/random/bernoulli_sampler.h>

#include <yt/client/table_client/schema.h>
#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/row_base.h>
#include <yt/client/table_client/row_buffer.h>
#include <yt/client/table_client/unversioned_reader.h>
#include <yt/client/table_client/versioned_reader.h>
#include <yt/client/table_client/unversioned_row_batch.h>
#include <yt/client/table_client/unversioned_row.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/action_queue.h>

#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/numeric_helpers.h>

namespace NYT::NTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NTabletClient;
using namespace NTableChunkFormat;
using namespace NTableChunkFormat::NProto;
using namespace NYPath;
using namespace NYTree;
using namespace NRpc;
using namespace NApi;

using NChunkClient::TDataSliceDescriptor;
using NChunkClient::TLegacyReadLimit;
using NChunkClient::TLegacyReadRange;
using NChunkClient::TReadLimit;
using NChunkClient::TReadRange;
using NChunkClient::NProto::TMiscExt;
using NChunkClient::TChunkReaderStatistics;

using NYT::FromProto;
using NYT::TRange;

////////////////////////////////////////////////////////////////////////////////

class TSchemalessChunkReaderBase
    : public virtual ISchemalessChunkReader
{
public:
    TSchemalessChunkReaderBase(
        const TChunkStatePtr& chunkState,
        TChunkReaderConfigPtr config,
        TChunkReaderOptionsPtr options,
        TChunkId chunkId,
        TNameTablePtr nameTable,
        const TClientBlockReadOptions& blockReadOptions,
        const TColumnFilter& columnFilter,
        const TSortColumns& sortColumns,
        const std::vector<TString>& omittedInaccessibleColumns,
        std::optional<i64> virtualRowIndex = std::nullopt)
        : ChunkState_(chunkState)
        , ChunkSpec_(ChunkState_->ChunkSpec)
        , DataSliceDescriptor_(ChunkState_->ChunkSpec, virtualRowIndex)
        , Config_(config)
        , Options_(options)
        , NameTable_(nameTable)
        , ColumnFilter_(columnFilter)
        , SortColumns_(sortColumns)
        , OmittedInaccessibleColumns_(omittedInaccessibleColumns)
        , OmittedInaccessibleColumnSet_(OmittedInaccessibleColumns_.begin(), OmittedInaccessibleColumns_.end())
        , Sampler_(Config_->SamplingRate, std::random_device()())
        , Logger(TableClientLogger.WithTag("ChunkReaderId: %v, ChunkId: %v",
            TGuid::Create(),
            chunkId))
        , ReaderInvoker_(CreateSerializedInvoker(NChunkClient::TDispatcher::Get()->GetReaderInvoker()))
    {
        if (blockReadOptions.ReadSessionId) {
            Logger.AddTag("ReadSessionId: %v", blockReadOptions.ReadSessionId);
        }

        if (Config_->SamplingSeed) {
            auto seed = *Config_->SamplingSeed;
            seed ^= FarmFingerprint(chunkId.Parts64[0]);
            seed ^= FarmFingerprint(chunkId.Parts64[1]);
            Sampler_ = TBernoulliSampler(Config_->SamplingRate, seed);
        }
    }

    virtual const TDataSliceDescriptor& GetCurrentReaderDescriptor() const override
    {
        return DataSliceDescriptor_;
    }

    virtual const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    virtual i64 GetTableRowIndex() const override
    {
        return ChunkSpec_.table_row_index() + RowIndex_;
    }

    virtual TInterruptDescriptor GetInterruptDescriptor(
        TRange<TUnversionedRow> /*unreadRows*/) const override
    {
        YT_ABORT();
    }

protected:
    const TChunkStatePtr ChunkState_;
    const TChunkSpec ChunkSpec_;
    const TDataSliceDescriptor DataSliceDescriptor_;

    TChunkReaderConfigPtr Config_;
    TChunkReaderOptionsPtr Options_;
    const TNameTablePtr NameTable_;

    const TColumnFilter ColumnFilter_;

    TSortColumns SortColumns_;

    const std::vector<TString> OmittedInaccessibleColumns_;
    const THashSet<TStringBuf> OmittedInaccessibleColumnSet_; // strings are being owned by OmittedInaccessibleColumns_

    i64 RowIndex_ = 0;
    i64 RowCount_ = 0;
    i64 DataWeight_ = 0;

    TBernoulliSampler Sampler_;
    int VirtualColumnCount_ = 0;

    int RowIndexId_ = -1;
    //! Values for virtual constant columns like $range_index, $table_index etc.
    TReaderVirtualValues VirtualValues_;

    NLogging::TLogger Logger;

    IInvokerPtr ReaderInvoker_;

    void InitializeVirtualColumns()
    {
        try {
            if (Options_->EnableRowIndex) {
                RowIndexId_ = NameTable_->GetIdOrRegisterName(RowIndexColumnName);
                // Row index is a non-constant virtual column, so we do not form any virtual value here.
                ++VirtualColumnCount_;
                // TODO(max42): support row index in virtual values for columnar reader.
            }

            if (Options_->EnableRangeIndex) {
                VirtualValues_.AddValue(MakeUnversionedInt64Value(
                    ChunkSpec_.range_index(),
                    NameTable_->GetIdOrRegisterName(RangeIndexColumnName)),
                    SimpleLogicalType(ESimpleLogicalValueType::Int64));
                ++VirtualColumnCount_;
            }

            // NB: table index should not always be virtual column, sometimes it is stored
            // alongside row in chunk (e.g. for intermediate chunks in schemaful Map-Reduce).
            if (Options_->EnableTableIndex && ChunkSpec_.has_table_index()) {
                VirtualValues_.AddValue(MakeUnversionedInt64Value(
                    ChunkSpec_.table_index(),
                    NameTable_->GetIdOrRegisterName(TableIndexColumnName)),
                    SimpleLogicalType(ESimpleLogicalValueType::Int64));
                ++VirtualColumnCount_;
            }

            if (Options_->EnableTabletIndex) {
                VirtualValues_.AddValue(MakeUnversionedInt64Value(
                    ChunkSpec_.tablet_index(),
                    NameTable_->GetIdOrRegisterName(TabletIndexColumnName)),
                    SimpleLogicalType(ESimpleLogicalValueType::Int64));
                ++VirtualColumnCount_;
            }

            if (const auto& virtualValueDirectory = ChunkState_->VirtualValueDirectory) {
                const auto& virtualRowIndex = DataSliceDescriptor_.VirtualRowIndex;
                YT_VERIFY(virtualRowIndex);
                YT_VERIFY(virtualRowIndex < virtualValueDirectory->Rows.size());
                const auto& row = virtualValueDirectory->Rows[*DataSliceDescriptor_.VirtualRowIndex];
                // Copy all values from rows properly mapping virtual ids into actual ids according to #NameTable_.
                for (int virtualColumnIndex = 0; virtualColumnIndex < row.GetCount(); ++virtualColumnIndex) {
                    auto value = row[virtualColumnIndex];
                    auto virtualId = value.Id;
                    const auto& columnName = virtualValueDirectory->NameTable->GetName(virtualId);
                    // Set proper id for this column.
                    std::optional<int> resultId;
                    if (ColumnFilter_.IsUniversal()) {
                        resultId = NameTable_->GetIdOrRegisterName(columnName);
                    } else {
                        resultId = NameTable_->FindId(columnName);
                    }
                    if (!resultId || !ColumnFilter_.ContainsIndex(*resultId)) {
                        // This value is not requested from reader, ignore it.
                        continue;
                    }
                    value.Id = *resultId;
                    VirtualValues_.AddValue(value, virtualValueDirectory->Schema->Columns()[virtualColumnIndex].LogicalType());
                    ++VirtualColumnCount_;
                }
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to add virtual columns to name table for schemaless chunk reader")
                << ex;
        }
    }

    void AddExtraValues(TMutableUnversionedRow& row, i64 rowIndex)
    {
        if (Options_->EnableRowIndex) {
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
        const NProto::TBlockMetaExt& blockMeta,
        const TChunkSpec& chunkSpec,
        const TLegacyReadLimit& lowerLimit,
        const TLegacyReadLimit& upperLimit,
        const TSortColumns& sortColumns,
        i64 rowIndex) const
    {
        std::vector<TDataSliceDescriptor> unreadDescriptors;
        std::vector<TDataSliceDescriptor> readDescriptors;

        rowIndex -= unreadRows.Size();
        i64 lowerRowIndex = lowerLimit.HasRowIndex() ? lowerLimit.GetRowIndex() : 0;
        i64 upperRowIndex = upperLimit.HasRowIndex() ? upperLimit.GetRowIndex() : misc.row_count();

        // Verify row index is in the chunk range
        if (RowCount_ > 0) {
            // If this is not a trivial case, e.g. lowerLimit > upperLimit,
            // let's do a sanity check.
            YT_VERIFY(lowerRowIndex <= rowIndex);
            YT_VERIFY(rowIndex <= upperRowIndex);
        }

        auto lowerKey = lowerLimit.HasLegacyKey() ? lowerLimit.GetLegacyKey() : TLegacyOwningKey();
        auto lastChunkKey = FromProto<TLegacyOwningKey>(blockMeta.blocks().rbegin()->last_key());
        auto upperKey = upperLimit.HasLegacyKey() ? upperLimit.GetLegacyKey() : lastChunkKey;
        TLegacyOwningKey firstUnreadKey;
        if (!unreadRows.Empty()) {
            firstUnreadKey = GetKeyPrefix(unreadRows[0], sortColumns.size());
        }
        // NB: checks after the first one are invalid unless
        // we read some data. Before we read anything, lowerKey may be
        // longer than firstUnreadKey.
        YT_VERIFY(
            RowCount_ == unreadRows.Size() ||
            !firstUnreadKey || (
                (!lowerKey || CompareRows(firstUnreadKey, lowerKey) >= 0) &&
                (!upperKey || CompareRows(firstUnreadKey, upperKey) <= 0)));

        if (rowIndex < upperRowIndex) {
            unreadDescriptors.emplace_back(chunkSpec);
            auto& chunk = unreadDescriptors[0].ChunkSpecs[0];
            chunk.mutable_lower_limit()->set_row_index(rowIndex);
            if (firstUnreadKey) {
                ToProto(chunk.mutable_lower_limit()->mutable_legacy_key(), firstUnreadKey);
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
        if (RowCount_ > unreadRows.Size() || rowIndex >= upperRowIndex) {
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
        TChunkReaderConfigPtr config,
        TChunkReaderOptionsPtr options,
        IChunkReaderPtr underlyingReader,
        TNameTablePtr nameTable,
        const TClientBlockReadOptions& blockReadOptions,
        const TSortColumns& sortColumns,
        const std::vector<TString>& omittedInaccessibleColumns,
        const TColumnFilter& columnFilter,
        std::optional<int> partitionTag,
        const TChunkReaderMemoryManagerPtr& memoryManager,
        std::optional<i64> virtualRowIndex = std::nullopt);

    virtual TDataStatistics GetDataStatistics() const override;

protected:
    using TSchemalessChunkReaderBase::Config_;
    using TSchemalessChunkReaderBase::Logger;

    const TColumnarChunkMetaPtr ChunkMeta_;

    TSortColumns ChunkSortColumns_;

    std::optional<int> PartitionTag_;

    int CurrentBlockIndex_ = 0;

    // Maps chunk name table ids into client id.
    // For filtered out columns maps id to -1.
    std::vector<TColumnIdMapping> IdMapping_;

    std::unique_ptr<THorizontalBlockReader> BlockReader_;

    TRefCountedBlockMetaPtr BlockMetaExt_;

    std::vector<int> BlockIndexes_;

    virtual void DoInitializeBlockSequence() = 0;

    void InitializeIdMapping();

    TFuture<void> InitializeBlockSequence();
};

////////////////////////////////////////////////////////////////////////////////

THorizontalSchemalessChunkReaderBase::THorizontalSchemalessChunkReaderBase(
    const TChunkStatePtr& chunkState,
    const TColumnarChunkMetaPtr& chunkMeta,
    TChunkReaderConfigPtr config,
    TChunkReaderOptionsPtr options,
    IChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    const TClientBlockReadOptions& blockReadOptions,
    const TSortColumns& sortColumns,
    const std::vector<TString>& omittedInaccessibleColumns,
    const TColumnFilter& columnFilter,
    std::optional<int> partitionTag,
    const TChunkReaderMemoryManagerPtr& memoryManager,
    std::optional<i64> virtualRowIndex)
    : TChunkReaderBase(
        config,
        underlyingReader,
        chunkState->BlockCache,
        blockReadOptions,
        memoryManager)
    , TSchemalessChunkReaderBase(
        chunkState,
        config,
        options,
        underlyingReader->GetChunkId(),
        nameTable,
        blockReadOptions,
        columnFilter,
        sortColumns,
        omittedInaccessibleColumns,
        virtualRowIndex)
    , ChunkMeta_(chunkMeta)
    , PartitionTag_(partitionTag)
{ }

TFuture<void> THorizontalSchemalessChunkReaderBase::InitializeBlockSequence()
{
    YT_VERIFY(BlockIndexes_.empty());

    InitializeVirtualColumns();

    DoInitializeBlockSequence();

    YT_LOG_DEBUG("Reading %v blocks", BlockIndexes_.size());

    std::vector<TBlockFetcher::TBlockInfo> blocks;
    for (int blockIndex : BlockIndexes_) {
        YT_VERIFY(blockIndex < BlockMetaExt_->blocks_size());
        auto& blockMeta = BlockMetaExt_->blocks(blockIndex);
        TBlockFetcher::TBlockInfo blockInfo;
        blockInfo.Index = blockMeta.block_index();
        blockInfo.UncompressedDataSize = blockMeta.uncompressed_size();
        blockInfo.Priority = blocks.size();
        blocks.push_back(blockInfo);
    }

    return DoOpen(std::move(blocks), ChunkMeta_->Misc());
}

void THorizontalSchemalessChunkReaderBase::InitializeIdMapping()
{
    YT_VERIFY(ChunkMeta_->GetChunkFormat() == ETableChunkFormat::SchemalessHorizontal);

    BlockMetaExt_ = ChunkMeta_->BlockMeta();

    const auto& chunkNameTable = ChunkMeta_->ChunkNameTable();
    IdMapping_.reserve(chunkNameTable->GetSize());
    for (int chunkSchemaId = 0; chunkSchemaId < chunkNameTable->GetSize(); ++chunkSchemaId) {
        IdMapping_.push_back({chunkSchemaId, -1});
    }

    try {
        if (ColumnFilter_.IsUniversal()) {
            for (int chunkSchemaId = 0; chunkSchemaId < chunkNameTable->GetSize(); ++chunkSchemaId) {
                auto name = chunkNameTable->GetName(chunkSchemaId);
                if (OmittedInaccessibleColumnSet_.contains(name)) {
                    continue;
                }
                auto readerSchemaId = NameTable_->GetIdOrRegisterName(name);
                IdMapping_[chunkSchemaId].ReaderSchemaIndex = readerSchemaId;
            }
        } else {
            for (auto readerSchemaId : ColumnFilter_.GetIndexes()) {
                auto name = NameTable_->GetName(readerSchemaId);
                if (OmittedInaccessibleColumnSet_.contains(name)) {
                    continue;
                }
                auto chunkSchemaId = chunkNameTable->FindId(name);
                if (!chunkSchemaId) {
                    continue;
                }
                IdMapping_[*chunkSchemaId].ReaderSchemaIndex = readerSchemaId;
            }
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to update name table for schemaless chunk reader")
                << TErrorAttribute("chunk_id", UnderlyingReader_->GetChunkId())
                    << ex;
    }
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
        TChunkReaderConfigPtr config,
        TChunkReaderOptionsPtr options,
        IChunkReaderPtr underlyingReader,
        TNameTablePtr nameTable,
        const TClientBlockReadOptions& blockReadOptions,
        const TSortColumns& sortColumns,
        const std::vector<TString>& omittedInaccessibleColumns,
        const TColumnFilter& columnFilter,
        const TReadRange& readRange,
        std::optional<int> partitionTag,
        const TChunkReaderMemoryManagerPtr& memoryManager,
        std::optional<i64> virtualRowIndex = std::nullopt);

    virtual IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override;

    virtual TInterruptDescriptor GetInterruptDescriptor(TRange<TUnversionedRow> unreadRows) const override;

private:
    TLegacyReadRange ReadRange_;

    virtual void DoInitializeBlockSequence() override;

    virtual void InitFirstBlock() override;
    virtual void InitNextBlock() override;

    void CreateBlockSequence(int beginIndex, int endIndex);

    void InitializeBlockSequenceSorted();
};

DEFINE_REFCOUNTED_TYPE(THorizontalSchemalessRangeChunkReader)

////////////////////////////////////////////////////////////////////////////////

THorizontalSchemalessRangeChunkReader::THorizontalSchemalessRangeChunkReader(
    const TChunkStatePtr& chunkState,
    const TColumnarChunkMetaPtr& chunkMeta,
    TChunkReaderConfigPtr config,
    TChunkReaderOptionsPtr options,
    IChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    const TClientBlockReadOptions& blockReadOptions,
    const TSortColumns& sortColumns,
    const std::vector<TString>& omittedInaccessibleColumns,
    const TColumnFilter& columnFilter,
    const TReadRange& readRange,
    std::optional<int> partitionTag,
    const TChunkReaderMemoryManagerPtr& memoryManager,
    std::optional<i64> virtualRowIndex)
    : THorizontalSchemalessChunkReaderBase(
        chunkState,
        chunkMeta,
        std::move(config),
        std::move(options),
        std::move(underlyingReader),
        std::move(nameTable),
        blockReadOptions,
        sortColumns,
        omittedInaccessibleColumns,
        columnFilter,
        partitionTag,
        memoryManager,
        virtualRowIndex)
{
    ReadRange_.LowerLimit() = ReadLimitToLegacyReadLimit(readRange.LowerLimit());
    ReadRange_.UpperLimit() = ReadLimitToLegacyReadLimit(readRange.UpperLimit());

    YT_LOG_DEBUG("Reading range of a chunk (Range: %v)", ReadRange_);

    // Initialize to lowest reasonable value.
    RowIndex_ = ReadRange_.LowerLimit().HasRowIndex()
        ? ReadRange_.LowerLimit().GetRowIndex()
        : 0;

    // Ready event must be set only when all initialization is finished and
    // RowIndex_ is set into proper value.
    // Must be called after the object is constructed and vtable initialized.
    SetReadyEvent(BIND(&THorizontalSchemalessRangeChunkReader::InitializeBlockSequence, MakeStrong(this))
        .AsyncVia(ReaderInvoker_)
        .Run()
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

void THorizontalSchemalessRangeChunkReader::DoInitializeBlockSequence()
{
    InitializeIdMapping();

    if (PartitionTag_) {
        YT_VERIFY(ReadRange_.LowerLimit().IsTrivial());
        YT_VERIFY(ReadRange_.UpperLimit().IsTrivial());

        CreateBlockSequence(0, BlockMetaExt_->blocks_size());
    } else {
        bool readSorted = ReadRange_.LowerLimit().HasLegacyKey() || ReadRange_.UpperLimit().HasLegacyKey() || !SortColumns_.empty();
        if (readSorted) {
            InitializeBlockSequenceSorted();
        } else {
            CreateBlockSequence(
                ApplyLowerRowLimit(*BlockMetaExt_, ReadRange_.LowerLimit()),
                ApplyUpperRowLimit(*BlockMetaExt_, ReadRange_.UpperLimit()));
        }
    }
}

void THorizontalSchemalessRangeChunkReader::InitializeBlockSequenceSorted()
{
    const auto& misc = ChunkMeta_->Misc();
    if (!misc.sorted()) {
        THROW_ERROR_EXCEPTION("Requested a sorted read for an unsorted chunk");
    }

    ChunkSortColumns_ = ChunkMeta_->GetChunkSchema()->GetSortColumns();

    ValidateSortColumns(SortColumns_, ChunkSortColumns_, Options_->DynamicTable);

    if (SortColumns_.empty()) {
        SortColumns_ = ChunkSortColumns_;
    }

    std::optional<int> sortColumnCount;
    if (Options_->DynamicTable) {
        sortColumnCount = SortColumns_.size();
    } else {
        ChunkMeta_->InitBlockLastKeys(GetColumnNames(SortColumns_));
    }

    int beginIndex = std::max(
        ApplyLowerRowLimit(*BlockMetaExt_, ReadRange_.LowerLimit()),
        ApplyLowerKeyLimit(ChunkMeta_->BlockLastKeys(), ReadRange_.LowerLimit(), sortColumnCount));
    int endIndex = std::min(
        ApplyUpperRowLimit(*BlockMetaExt_, ReadRange_.UpperLimit()),
        ApplyUpperKeyLimit(ChunkMeta_->BlockLastKeys(), ReadRange_.UpperLimit(), sortColumnCount));

    CreateBlockSequence(beginIndex, endIndex);
}

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
    const auto& blockMeta = BlockMetaExt_->blocks(blockIndex);

    YT_VERIFY(CurrentBlock_ && CurrentBlock_.IsSet());
    BlockReader_.reset(new THorizontalBlockReader(
        CurrentBlock_.Get().ValueOrThrow().Data,
        blockMeta,
        ChunkMeta_->GetChunkSchema(),
        IdMapping_,
        ChunkSortColumns_.size(),
        SortColumns_.size(),
        VirtualColumnCount_));

    RowIndex_ = blockMeta.chunk_row_count() - blockMeta.row_count();

    int sortColumnCount = std::max(ChunkSortColumns_.size(), SortColumns_.size());
    CheckBlockUpperLimits(
        BlockMetaExt_->blocks(blockIndex).chunk_row_count(),
        ChunkMeta_->BlockLastKeys() ? ChunkMeta_->BlockLastKeys()[blockIndex] : TLegacyKey(),
        ReadRange_.UpperLimit(),
        sortColumnCount);

    const auto& lowerLimit = ReadRange_.LowerLimit();

    if (lowerLimit.HasRowIndex() && RowIndex_ < lowerLimit.GetRowIndex()) {
        YT_VERIFY(BlockReader_->SkipToRowIndex(lowerLimit.GetRowIndex() - RowIndex_));
        RowIndex_ = lowerLimit.GetRowIndex();
    }

    if (lowerLimit.HasLegacyKey()) {
        auto blockRowIndex = BlockReader_->GetRowIndex();

        YT_VERIFY(BlockReader_->SkipToKey(lowerLimit.GetLegacyKey().Get()));
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

    while (!BlockEnded_ &&
           rows.size() < options.MaxRowsPerRead &&
           dataWeight < options.MaxDataWeightPerRead)
    {
        if ((CheckRowLimit_ && RowIndex_ >= ReadRange_.UpperLimit().GetRowIndex()) ||
            (CheckKeyLimit_ && CompareRows(BlockReader_->GetKey(), ReadRange_.UpperLimit().GetLegacyKey()) >= 0))
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
        *BlockMetaExt_,
        ChunkSpec_,
        ReadRange_.LowerLimit(),
        ReadRange_.UpperLimit(),
        SortColumns_,
        RowIndex_);
}

////////////////////////////////////////////////////////////////////////////////

class THorizontalSchemalessLookupChunkReader
    : public THorizontalSchemalessChunkReaderBase
{
public:
    THorizontalSchemalessLookupChunkReader(
        const TChunkStatePtr& chunkState,
        const TColumnarChunkMetaPtr& chunkMeta,
        TChunkReaderConfigPtr config,
        TChunkReaderOptionsPtr options,
        NChunkClient::IChunkReaderPtr underlyingReader,
        TNameTablePtr nameTable,
        const TClientBlockReadOptions& blockReadOptions,
        const TSortColumns& sortColumns,
        const std::vector<TString>& omittedInaccessibleColumns,
        const TColumnFilter& columnFilter,
        const TSharedRange<TLegacyKey>& keys,
        TChunkReaderPerformanceCountersPtr performanceCounters,
        std::optional<int> partitionTag = std::nullopt,
        const TChunkReaderMemoryManagerPtr& memoryManager = nullptr);

    virtual IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override;

private:
    const TSharedRange<TLegacyKey> Keys_;
    const TChunkReaderPerformanceCountersPtr PerformanceCounters_;
    std::vector<bool> KeyFilterTest_;

    virtual void DoInitializeBlockSequence() override;

    virtual void InitFirstBlock() override;
    virtual void InitNextBlock() override;

};

DEFINE_REFCOUNTED_TYPE(THorizontalSchemalessLookupChunkReader)

////////////////////////////////////////////////////////////////////////////////

THorizontalSchemalessLookupChunkReader::THorizontalSchemalessLookupChunkReader(
    const TChunkStatePtr& chunkState,
    const TColumnarChunkMetaPtr& chunkMeta,
    TChunkReaderConfigPtr config,
    TChunkReaderOptionsPtr options,
    NChunkClient::IChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    const TClientBlockReadOptions& blockReadOptions,
    const TSortColumns& sortColumns,
    const std::vector<TString>& omittedInaccessibleColumns,
    const TColumnFilter& columnFilter,
    const TSharedRange<TLegacyKey>& keys,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    std::optional<int> partitionTag,
    const TChunkReaderMemoryManagerPtr& memoryManager)
    : THorizontalSchemalessChunkReaderBase(
        chunkState,
        chunkMeta,
        std::move(config),
        std::move(options),
        std::move(underlyingReader),
        std::move(nameTable),
        blockReadOptions,
        sortColumns,
        omittedInaccessibleColumns,
        columnFilter,
        partitionTag,
        memoryManager)
    , Keys_(keys)
    , PerformanceCounters_(std::move(performanceCounters))
    , KeyFilterTest_(Keys_.Size(), true)
{
    // Ready event must be set only when all initialization is finished and
    // RowIndex_ is set into proper value.
    // Must be called after the object is constructed and vtable initialized.
    SetReadyEvent(BIND(&THorizontalSchemalessLookupChunkReader::InitializeBlockSequence, MakeStrong(this))
        .AsyncVia(ReaderInvoker_)
        .Run()
        .Apply(BIND([this, this_ = MakeStrong(this)] () {
            if (InitFirstBlockNeeded_) {
                InitFirstBlock();
                InitFirstBlockNeeded_ = false;
            }
        })));
}

void THorizontalSchemalessLookupChunkReader::DoInitializeBlockSequence()
{
    InitializeIdMapping();

    const auto& misc = ChunkMeta_->Misc();
    if (!misc.sorted()) {
        THROW_ERROR_EXCEPTION("Requested lookup for an unsorted chunk");
    }
    if (!misc.unique_keys()) {
        THROW_ERROR_EXCEPTION("Requested lookup for a chunk without \"unique_keys\" restriction");
    }

    ChunkSortColumns_ = ChunkMeta_->GetChunkSchema()->GetSortColumns();

    ValidateSortColumns(SortColumns_, ChunkSortColumns_, Options_->DynamicTable);

    // Don't call InitBlockLastKeys because this reader should be used only for dynamic tables.
    YT_VERIFY(Options_->DynamicTable);

    for (const auto& key : Keys_) {
        TLegacyReadLimit readLimit;
        readLimit.SetLegacyKey(TLegacyOwningKey(key));

        int index = ApplyLowerKeyLimit(ChunkMeta_->BlockLastKeys(), readLimit, SortColumns_.size());
        if (index == BlockMetaExt_->blocks_size()) {
            break;
        }

        if (BlockIndexes_.empty() || BlockIndexes_.back() != index) {
            BlockIndexes_.push_back(index);
        }
    }
}

IUnversionedRowBatchPtr THorizontalSchemalessLookupChunkReader::Read(const TRowBatchReadOptions& options)
{
    auto readGuard = AcquireReadGuard();

    MemoryPool_.Clear();

    std::vector<TUnversionedRow> rows;
    rows.reserve(options.MaxRowsPerRead);
    i64 dataWeight = 0;

    auto success = [&] () {
        if (!BeginRead()) {
            // Not ready yet.
            return true;
        }

        if (!BlockReader_) {
            // Nothing to read from chunk.
            if (RowCount_ == Keys_.Size()) {
                return false;
            }

            while (rows.size() < options.MaxRowsPerRead && RowCount_ < Keys_.Size()) {
                rows.push_back(TUnversionedRow());
                ++RowCount_;
            }

            return true;
        }

        if (BlockEnded_) {
            BlockReader_.reset();
            OnBlockEnded();
            return true;
        }

        while (rows.size() < options.MaxRowsPerRead &&
               dataWeight < options.MaxDataWeightPerRead)
        {
            if (RowCount_ == Keys_.Size()) {
                BlockEnded_ = true;
                return true;
            }

            if (!KeyFilterTest_[RowCount_]) {
                rows.push_back(TUnversionedRow());
            } else {
                auto key = Keys_[RowCount_];
                if (!BlockReader_->SkipToKey(key)) {
                    BlockEnded_ = true;
                    return true;
                }

                if (key == BlockReader_->GetKey()) {
                    auto row = BlockReader_->GetRow(&MemoryPool_);
                    rows.push_back(row);
                    dataWeight += GetDataWeight(row);

                    int blockIndex = BlockIndexes_[CurrentBlockIndex_];
                    const auto& blockMeta = BlockMetaExt_->blocks(blockIndex);
                    RowIndex_ = blockMeta.chunk_row_count() - blockMeta.row_count() + BlockReader_->GetRowIndex();
                } else {
                    rows.push_back(TUnversionedRow());
                }
            }

            ++RowCount_;
        }

        return true;
    }();

    if (PerformanceCounters_) {
        PerformanceCounters_->StaticChunkRowLookupCount += rows.size();
        PerformanceCounters_->StaticChunkRowLookupDataWeightCount += dataWeight;
    }

    return success
        ? CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)))
        : nullptr;
}

void THorizontalSchemalessLookupChunkReader::InitFirstBlock()
{
    int blockIndex = BlockIndexes_[CurrentBlockIndex_];
    const auto& blockMeta = BlockMetaExt_->blocks(blockIndex);

    BlockReader_.reset(new THorizontalBlockReader(
        CurrentBlock_.Get().ValueOrThrow().Data,
        blockMeta,
        ChunkMeta_->GetChunkSchema(),
        IdMapping_,
        ChunkSortColumns_.size(),
        SortColumns_.size(),
        VirtualColumnCount_));
}

void THorizontalSchemalessLookupChunkReader::InitNextBlock()
{
    ++CurrentBlockIndex_;
    InitFirstBlock();
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TColumnarSchemalessRangeChunkReader)

class TColumnarSchemalessRangeChunkReader
    : public TSchemalessChunkReaderBase
    , public TColumnarRangeChunkReaderBase
{
public:
    TColumnarSchemalessRangeChunkReader(
        const TChunkStatePtr& chunkState,
        const TColumnarChunkMetaPtr& chunkMeta,
        TChunkReaderConfigPtr config,
        TChunkReaderOptionsPtr options,
        IChunkReaderPtr underlyingReader,
        TNameTablePtr nameTable,
        const TClientBlockReadOptions& blockReadOptions,
        const TSortColumns& sortColumns,
        const std::vector<TString>& omittedInaccessibleColumns,
        const TColumnFilter& columnFilter,
        const TReadRange& readRange,
        const TChunkReaderMemoryManagerPtr& memoryManager,
        std::optional<i64> virtualRowIndex)
        : TSchemalessChunkReaderBase(
            chunkState,
            config,
            options,
            underlyingReader->GetChunkId(),
            nameTable,
            blockReadOptions,
            columnFilter,
            sortColumns,
            omittedInaccessibleColumns,
            virtualRowIndex)
        , TColumnarRangeChunkReaderBase(
            chunkMeta,
            config,
            underlyingReader,
            chunkState->BlockCache,
            blockReadOptions,
            BIND(&TColumnarSchemalessRangeChunkReader::OnRowsSkipped, MakeWeak(this)),
            memoryManager)
    {
        YT_LOG_DEBUG("Reading range of a chunk (Range: %v)", readRange);

        LowerLimit_ = ReadLimitToLegacyReadLimit(readRange.LowerLimit());
        UpperLimit_ = ReadLimitToLegacyReadLimit(readRange.UpperLimit());

        RowIndex_ = LowerLimit_.HasRowIndex() ? LowerLimit_.GetRowIndex() : 0;

        SetReadyEvent(BIND(&TColumnarSchemalessRangeChunkReader::InitializeBlockSequence, MakeStrong(this))
            .AsyncVia(ReaderInvoker_)
            .Run());
    }

    ~TColumnarSchemalessRangeChunkReader()
    {
        YT_LOG_DEBUG("Columnar reader timing statistics (TimingStatistics: %v)", TTimingReaderBase::GetTimingStatistics());
    }

    virtual IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
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

    virtual TInterruptDescriptor GetInterruptDescriptor(
        TRange<TUnversionedRow> unreadRows) const override
    {
        return GetInterruptDescriptorImpl(
            unreadRows,
            ChunkMeta_->Misc(),
            *ChunkMeta_->BlockMeta(),
            ChunkSpec_,
            LowerLimit_,
            UpperLimit_,
            SortColumns_,
            RowIndex_);
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        auto dataStatistics = TColumnarRangeChunkReaderBase::GetDataStatistics();
        dataStatistics.set_row_count(RowCount_);
        dataStatistics.set_data_weight(DataWeight_);
        return dataStatistics;
    }

private:
    std::vector<IUnversionedColumnReader*> RowColumnReaders_;
    std::vector<IUnversionedColumnReader*> KeyColumnReaders_;

    ISchemalessColumnReader* SchemalessReader_ = nullptr;

    bool Completed_ = false;
    bool LowerKeyLimitReached_ = false;
    i64 PendingUnmaterializedRowCount_ = 0;

    TChunkedMemoryPool Pool_;


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

        virtual int GetRowCount() const override
        {
            return RowCount_;
        }

        virtual TSharedRange<TUnversionedRow> MaterializeRows() override
        {
            if (RootColumns_) {
                THROW_ERROR_EXCEPTION("Cannot materialize batch into rows since it was already materialized into columns");
            }

            if (!Rows_) {
                Rows_ = Reader_->MaterializeRows(RowCount_);
            }

            return Rows_;
        }

        virtual TRange<const TColumn*> MaterializeColumns() override
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

        virtual TRange<TDictionaryId> GetRetiringDictionaryIds() const override
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

        if (!LowerKeyLimitReached_) {
            auto keys = ReadKeys(rowLimit);

            i64 deltaIndex = 0;
            for (; deltaIndex < rowLimit; ++deltaIndex) {
                if (keys[deltaIndex] >= LowerLimit_.GetLegacyKey()) {
                    break;
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
            if (RowIndex_ >= SafeUpperRowIndex_ && UpperLimit_.HasLegacyKey()) {
                auto keyRange = MakeRange(keys.data() + deltaIndex, keys.data() + keys.size());
                while (rowLimit > 0 && keyRange[rowLimit - 1] >= UpperLimit_.GetLegacyKey()) {
                    --rowLimit;
                    Completed_ = true;
                }
            }
        } else if (RowIndex_ >= SafeUpperRowIndex_ && UpperLimit_.HasLegacyKey()) {
            auto keys = ReadKeys(rowLimit);
            while (rowLimit > 0 && keys[rowLimit - 1] >= UpperLimit_.GetLegacyKey()) {
                --rowLimit;
                Completed_ = true;
            }
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
            for (i64 rowIndex = 0; rowIndex < rows->size(); ++rowIndex) {
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

        return MakeSharedRange(std::move(rows), this);
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
        for (int index = 0; index < RowColumnReaders_.size(); ++index) {
            const auto& reader = RowColumnReaders_[index];
            batchColumnCount += reader->GetBatchColumnCount();
        }

        allBatchColumns->resize(batchColumnCount + VirtualValues_.GetTotalColumnCount());
        rootBatchColumns->reserve(RowColumnReaders_.size() + VirtualValues_.Values().size());
        int currentBatchColumnIndex = 0;
        for (int index = 0; index < RowColumnReaders_.size(); ++index) {
            const auto& reader = RowColumnReaders_[index];
            const auto& column = Columns_[index];
            int columnCount = reader->GetBatchColumnCount();
            auto columnRange = TMutableRange<IUnversionedColumnarRowBatch::TColumn>(
                allBatchColumns->data() + currentBatchColumnIndex,
                columnCount);
            auto* rootColumn = &columnRange.Front();
            rootColumn->Id = column.ColumnId;
            rootColumn->Type = ChunkMeta_->GetChunkSchema()->Columns()[column.ColumnMetaIndex].LogicalType();
            rootBatchColumns->push_back(rootColumn);
            reader->ReadColumnarBatch(columnRange, rowCount);
            currentBatchColumnIndex += columnCount;
        }

        for (int index = 0; index < VirtualValues_.Values().size(); ++index) {
            int columnCount = VirtualValues_.GetBatchColumnCount(index);
            auto columnRange = TMutableRange<IUnversionedColumnarRowBatch::TColumn>(
                allBatchColumns->data() + currentBatchColumnIndex,
                columnCount);
            VirtualValues_.FillColumns(columnRange, index, RowIndex_, rowCount);
            currentBatchColumnIndex += columnCount;
            rootBatchColumns->push_back(&columnRange.Front());
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
        i64 dataWeightBefore = DataWeight_;

        while (rows.size() < options.MaxRowsPerRead &&
               DataWeight_ - dataWeightBefore < options.MaxDataWeightPerRead)
        {
            i64 rowCount = ReadPrologue(rows.capacity() - rows.size());
            ReadRows(rowCount, &rows);
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

    void InitializeBlockSequence()
    {
        YT_VERIFY(ChunkMeta_->GetChunkFormat() == ETableChunkFormat::UnversionedColumnar);
        InitializeVirtualColumns();

        // Minimum prefix of key columns, that must be included in column filter.
        int minKeyColumnCount = 0;
        if (UpperLimit_.HasLegacyKey()) {
            minKeyColumnCount = std::max(minKeyColumnCount, UpperLimit_.GetLegacyKey().GetCount());
        }
        if (LowerLimit_.HasLegacyKey()) {
            minKeyColumnCount = std::max(minKeyColumnCount, LowerLimit_.GetLegacyKey().GetCount());
        }
        bool sortedRead = minKeyColumnCount > 0 || !SortColumns_.empty();

        if (sortedRead && !ChunkMeta_->Misc().sorted()) {
            THROW_ERROR_EXCEPTION("Requested a sorted read for an unsorted chunk");
        }

        const auto& chunkSchema = *ChunkMeta_->GetChunkSchema();
        const auto& columnMeta = ChunkMeta_->ColumnMeta();

        ValidateSortColumns(SortColumns_, chunkSchema.GetSortColumns(), Options_->DynamicTable);

        // Cannot read more key columns than stored in chunk, even if range keys are longer.
        minKeyColumnCount = std::min(minKeyColumnCount, chunkSchema.GetKeyColumnCount());

        TNameTablePtr chunkNameTable;
        if (Options_->DynamicTable) {
            chunkNameTable = TNameTable::FromSchema(chunkSchema);
        } else {
            chunkNameTable = ChunkMeta_->ChunkNameTable();
            if (UpperLimit_.HasLegacyKey() || LowerLimit_.HasLegacyKey()) {
                ChunkMeta_->InitBlockLastKeys(SortColumns_.empty()
                    ? chunkSchema.GetKeyColumns()
                    : GetColumnNames(SortColumns_));
            }
        }

        // Define columns to read.
        std::vector<TColumnIdMapping> schemalessIdMapping;
        schemalessIdMapping.resize(chunkNameTable->GetSize(), {-1, -1});

        std::vector<int> schemaColumnIndexes;
        bool readSchemalessColumns = false;
        if (ColumnFilter_.IsUniversal()) {
            for (int index = 0; index < chunkSchema.Columns().size(); ++index) {
                const auto& columnSchema = chunkSchema.Columns()[index];
                if (OmittedInaccessibleColumnSet_.contains(columnSchema.Name())) {
                    continue;
                }
                schemaColumnIndexes.push_back(index);
            }
            for (int chunkColumnId = chunkSchema.Columns().size(); chunkColumnId < chunkNameTable->GetSize(); ++chunkColumnId) {
                auto name = chunkNameTable->GetName(chunkColumnId);
                if (OmittedInaccessibleColumnSet_.contains(name)) {
                    continue;
                }
                readSchemalessColumns = true;
                int nameTableIndex = NameTable_->GetIdOrRegisterName(name);
                schemalessIdMapping[chunkColumnId] = {chunkColumnId, nameTableIndex};
            }
        } else {
            THashSet<int> filterIndexes(ColumnFilter_.GetIndexes().begin(), ColumnFilter_.GetIndexes().end());
            for (int chunkColumnId = 0; chunkColumnId < chunkNameTable->GetSize(); ++chunkColumnId) {
                auto name = chunkNameTable->GetName(chunkColumnId);
                if (OmittedInaccessibleColumnSet_.contains(name)) {
                    continue;
                }
                auto nameTableIndex = NameTable_->GetIdOrRegisterName(name);
                if (filterIndexes.contains(nameTableIndex)) {
                    if (chunkColumnId < chunkSchema.Columns().size()) {
                        schemaColumnIndexes.push_back(chunkColumnId);
                    } else {
                        readSchemalessColumns = true;
                        schemalessIdMapping[chunkColumnId] = {chunkColumnId, nameTableIndex};
                    }
                }
            }
        }

        // Create column readers.
        for (int valueIndex = 0; valueIndex < schemaColumnIndexes.size(); ++valueIndex) {
            auto columnIndex = schemaColumnIndexes[valueIndex];
            auto columnId = NameTable_->GetIdOrRegisterName(chunkSchema.Columns()[columnIndex].Name());
            auto columnReader = CreateUnversionedColumnReader(
                chunkSchema.Columns()[columnIndex],
                columnMeta->columns(columnIndex),
                valueIndex,
                columnId);
            RowColumnReaders_.push_back(columnReader.get());
            Columns_.emplace_back(std::move(columnReader), columnIndex, columnId);
        }

        if (readSchemalessColumns) {
            auto columnReader = CreateSchemalessColumnReader(
                columnMeta->columns(chunkSchema.Columns().size()),
                schemalessIdMapping);
            SchemalessReader_ = columnReader.get();
            Columns_.emplace_back(std::move(columnReader), chunkSchema.Columns().size());
        }

        for (int keyIndex = 0; keyIndex < minKeyColumnCount; ++keyIndex) {
            auto columnReader = CreateUnversionedColumnReader(
                chunkSchema.Columns()[keyIndex],
                columnMeta->columns(keyIndex),
                keyIndex,
                -1);
            KeyColumnReaders_.push_back(columnReader.get());
            Columns_.emplace_back(std::move(columnReader), keyIndex);
        }

        for (int keyIndex = minKeyColumnCount; keyIndex < SortColumns_.size(); ++keyIndex) {
            auto columnReader = CreateBlocklessUnversionedNullColumnReader(
                keyIndex,
                keyIndex);
            KeyColumnReaders_.push_back(columnReader.get());
            Columns_.emplace_back(std::move(columnReader), -1);
        }

        InitLowerRowIndex();
        InitUpperRowIndex();

        YT_LOG_DEBUG("Initialized row index limits (LowerRowIndex: %v, SafeUpperRowIndex: %v, HardUpperRowIndex: %v)",
            LowerRowIndex_,
            SafeUpperRowIndex_,
            HardUpperRowIndex_);

        if (LowerRowIndex_ < HardUpperRowIndex_) {
            // We must continue initialization and set RowIndex_ before
            // ReadyEvent is set for the first time.
            InitBlockFetcher();
            WaitFor(RequestFirstBlocks())
                .ThrowOnError();

            FeedBlocksToReaders();
            Initialize(MakeRange(KeyColumnReaders_));
            RowIndex_ = LowerRowIndex_;
            LowerKeyLimitReached_ = !LowerLimit_.HasLegacyKey();

            YT_LOG_DEBUG("Initialized start row index (LowerKeyLimitReached: %v, RowIndex: %v)",
                LowerKeyLimitReached_,
                RowIndex_);

            if (RowIndex_ >= HardUpperRowIndex_) {
                Completed_ = true;
            }
            if (IsSamplingCompleted()) {
                Completed_ = true;
            }
        } else {
            Completed_ = true;
        }
    }

    std::vector<TLegacyKey> ReadKeys(i64 rowCount)
    {
        std::vector<TLegacyKey> keys;
        for (i64 index = 0; index < rowCount; ++index) {
            auto key = TLegacyMutableKey::Allocate(
                &Pool_,
                KeyColumnReaders_.size());
            key.SetCount(KeyColumnReaders_.size());
            keys.push_back(key);
        }

        auto keyRange = TMutableRange<TLegacyMutableKey>(
            static_cast<TLegacyMutableKey*>(keys.data()),
            static_cast<TLegacyMutableKey*>(keys.data() + rowCount));

        for (const auto& columnReader : KeyColumnReaders_) {
            columnReader->ReadValues(keyRange);
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
                RowColumnReaders_.size() + (SchemalessReader_ ? schemalessColumnCounts[index] : 0) + VirtualColumnCount_);
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
    , public TColumnarLookupChunkReaderBase
{
public:
    TColumnarSchemalessLookupChunkReader(
        const TChunkStatePtr& chunkState,
        const TColumnarChunkMetaPtr& chunkMeta,
        TChunkReaderConfigPtr config,
        TChunkReaderOptionsPtr options,
        IChunkReaderPtr underlyingReader,
        TNameTablePtr nameTable,
        const TClientBlockReadOptions& blockReadOptions,
        const TSortColumns& sortColumns,
        const std::vector<TString>& omittedInaccessibleColumns,
        const TColumnFilter& columnFilter,
        const TSharedRange<TLegacyKey>& keys,
        TChunkReaderPerformanceCountersPtr performanceCounters,
        const TChunkReaderMemoryManagerPtr& memoryManager)
        : TSchemalessChunkReaderBase(
            chunkState,
            config,
            options,
            underlyingReader->GetChunkId(),
            nameTable,
            blockReadOptions,
            columnFilter,
            sortColumns,
            omittedInaccessibleColumns)
        , TColumnarLookupChunkReaderBase(
            chunkMeta,
            config,
            underlyingReader,
            chunkState->BlockCache,
            blockReadOptions,
            [] (int) { YT_ABORT(); }, // Rows should not be skipped in lookup reader.
            memoryManager)
        , PerformanceCounters_(std::move(performanceCounters))
    {
        Keys_ = keys;

        SetReadyEvent(BIND(&TColumnarSchemalessLookupChunkReader::InitializeBlockSequence, MakeStrong(this))
            .AsyncVia(ReaderInvoker_)
            .Run());
    }

    virtual IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        auto readGuard = AcquireReadGuard();

        Pool_.Clear();

        if (!ReadyEvent().IsSet() || !ReadyEvent().Get().IsOK()) {
            return CreateEmptyUnversionedRowBatch();
        }

        if (NextKeyIndex_ == Keys_.Size()) {
            return nullptr;
        }

        std::vector<TUnversionedRow> rows;
        rows.reserve(options.MaxRowsPerRead);
        i64 dataWeight = 0;

        while (rows.size() < options.MaxRowsPerRead &&
               dataWeight < options.MaxDataWeightPerRead)
        {
            FeedBlocksToReaders();

            if (RowIndexes_[NextKeyIndex_] < ChunkMeta_->Misc().row_count()) {
                auto key = Keys_[NextKeyIndex_];
                YT_VERIFY(key.GetCount() == KeyColumnReaders_.size());

                // Reading row.
                i64 lowerRowIndex = KeyColumnReaders_[0]->GetCurrentRowIndex();
                i64 upperRowIndex = KeyColumnReaders_[0]->GetBlockUpperRowIndex();
                for (int i = 0; i < KeyColumnReaders_.size(); ++i) {
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

            if (++NextKeyIndex_ == Keys_.Size() || !TryFetchNextRow()) {
                break;
            }
        }

        i64 rowCount = rows.size();

        if (PerformanceCounters_) {
            PerformanceCounters_->StaticChunkRowLookupCount += rowCount;
            PerformanceCounters_->StaticChunkRowLookupDataWeightCount += dataWeight;
        }

        RowCount_ += rowCount;
        DataWeight_ += dataWeight;

        return CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        auto dataStatistics = TColumnarChunkReaderBase::GetDataStatistics();
        dataStatistics.set_row_count(RowCount_);
        dataStatistics.set_data_weight(DataWeight_);
        return dataStatistics;
    }

private:
    const TChunkReaderPerformanceCountersPtr PerformanceCounters_;

    std::vector<IUnversionedColumnReader*> RowColumnReaders_;
    std::vector<IUnversionedColumnReader*> KeyColumnReaders_;

    ISchemalessColumnReader* SchemalessReader_ = nullptr;

    TChunkedMemoryPool Pool_;

    TMutableUnversionedRow ReadRow(i64 rowIndex)
    {
        ui32 schemalessColumnCount = 0;

        if (SchemalessReader_) {
            SchemalessReader_->SkipToRowIndex(rowIndex);
            SchemalessReader_->ReadValueCounts(TMutableRange<ui32>(&schemalessColumnCount, 1));
        }

        auto row = TMutableUnversionedRow::Allocate(&Pool_, RowColumnReaders_.size() + schemalessColumnCount + VirtualColumnCount_);
        row.SetCount(RowColumnReaders_.size());

        // Read values.
        auto range = TMutableRange<TMutableUnversionedRow>(&row, 1);

        for (const auto& columnReader : RowColumnReaders_) {
            columnReader->SkipToRowIndex(rowIndex);
            columnReader->ReadValues(range);
        }

        if (SchemalessReader_) {
            SchemalessReader_->ReadValues(range);
        }

        return row;
    }

    void InitializeBlockSequence()
    {
        YT_VERIFY(ChunkMeta_->GetChunkFormat() == ETableChunkFormat::UnversionedColumnar);
        InitializeVirtualColumns();

        if (!ChunkMeta_->Misc().sorted()) {
            THROW_ERROR_EXCEPTION("Requested a sorted read for an unsorted chunk");
        }

        const auto& chunkSchema = *ChunkMeta_->GetChunkSchema();
        const auto& columnMeta = ChunkMeta_->ColumnMeta();

        ValidateSortColumns(
            SortColumns_,
            chunkSchema.GetSortColumns(),
            Options_->DynamicTable);

        TNameTablePtr chunkNameTable;
        if (Options_->DynamicTable) {
            chunkNameTable = TNameTable::FromSchema(chunkSchema);
        } else {
            chunkNameTable = ChunkMeta_->ChunkNameTable();
            ChunkMeta_->InitBlockLastKeys(GetColumnNames(SortColumns_));
        }

        // Create key column readers.
        KeyColumnReaders_.resize(SortColumns_.size());
        for (int keyColumnIndex = 0; keyColumnIndex < chunkSchema.GetKeyColumnCount(); ++keyColumnIndex) {
            auto columnReader = CreateUnversionedColumnReader(
                chunkSchema.Columns()[keyColumnIndex],
                columnMeta->columns(keyColumnIndex),
                keyColumnIndex,
                keyColumnIndex);

            KeyColumnReaders_[keyColumnIndex] = columnReader.get();
            Columns_.emplace_back(std::move(columnReader), keyColumnIndex);
        }
        for (int keyColumnIndex = chunkSchema.GetKeyColumnCount(); keyColumnIndex < SortColumns_.size(); ++keyColumnIndex) {
            auto columnReader = CreateBlocklessUnversionedNullColumnReader(
                keyColumnIndex,
                keyColumnIndex);

            KeyColumnReaders_[keyColumnIndex] = columnReader.get();
            Columns_.emplace_back(std::move(columnReader), -1);
        }

        // Define columns to read.
        std::vector<TColumnIdMapping> schemalessIdMapping;
        schemalessIdMapping.resize(chunkNameTable->GetSize(), {-1, -1});

        std::vector<int> schemaColumnIndexes;
        bool readSchemalessColumns = false;
        if (ColumnFilter_.IsUniversal()) {
            for (int index = 0; index < chunkSchema.Columns().size(); ++index) {
                const auto& columnSchema = chunkSchema.Columns()[index];
                if (OmittedInaccessibleColumnSet_.contains(columnSchema.Name())) {
                    continue;
                }
                schemaColumnIndexes.push_back(index);
            }

            for (int chunkColumnId = chunkSchema.Columns().size(); chunkColumnId < chunkNameTable->GetSize(); ++chunkColumnId) {
                auto name = chunkNameTable->GetName(chunkColumnId);
                if (OmittedInaccessibleColumnSet_.contains(name)) {
                    continue;
                }
                readSchemalessColumns = true;
                int nameTableIndex = NameTable_->GetIdOrRegisterName(name);
                schemalessIdMapping[chunkColumnId] = {chunkColumnId, nameTableIndex};
            }
        } else {
            auto filterIndexes = THashSet<int>(ColumnFilter_.GetIndexes().begin(), ColumnFilter_.GetIndexes().end());
            for (int chunkColumnId = 0;chunkColumnId < chunkNameTable->GetSize(); ++chunkColumnId) {
                auto name = chunkNameTable->GetName(chunkColumnId);
                if (OmittedInaccessibleColumnSet_.contains(name)) {
                    continue;
                }
                auto nameTableIndex = NameTable_->GetIdOrRegisterName(name);
                if (filterIndexes.contains(nameTableIndex)) {
                    if (chunkColumnId < chunkSchema.Columns().size()) {
                        schemaColumnIndexes.push_back(chunkColumnId);
                    } else {
                        readSchemalessColumns = true;
                        schemalessIdMapping[chunkColumnId] = {chunkColumnId, nameTableIndex};
                    }
                }
            }
        }

        // Create column readers.
        for (int valueIndex = 0; valueIndex < schemaColumnIndexes.size(); ++valueIndex) {
            auto columnIndex = schemaColumnIndexes[valueIndex];
            if (columnIndex < chunkSchema.GetKeyColumnCount()) {
                RowColumnReaders_.push_back(KeyColumnReaders_[columnIndex]);
            } else {
                auto columnReader = CreateUnversionedColumnReader(
                    chunkSchema.Columns()[columnIndex],
                    columnMeta->columns(columnIndex),
                    valueIndex,
                    NameTable_->GetIdOrRegisterName(chunkSchema.Columns()[columnIndex].Name()));

                RowColumnReaders_.emplace_back(columnReader.get());
                Columns_.emplace_back(std::move(columnReader), columnIndex);
            }
        }

        if (readSchemalessColumns) {
            auto columnReader = CreateSchemalessColumnReader(
                columnMeta->columns(chunkSchema.Columns().size()),
                schemalessIdMapping);
            SchemalessReader_ = columnReader.get();

            Columns_.emplace_back(
                std::move(columnReader),
                chunkSchema.Columns().size());
        }

        Initialize();

        // NB: We must complete initialization before ReadyEvent is set in the constructor.
        WaitFor(RequestFirstBlocks())
            .ThrowOnError();
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
    const TClientBlockReadOptions& blockReadOptions,
    const TSortColumns& sortColumns,
    const std::vector<TString>& omittedInaccessibleColumns,
    const TColumnFilter& columnFilter,
    const TReadRange& readRange,
    std::optional<int> partitionTag,
    const TChunkReaderMemoryManagerPtr& memoryManager,
    std::optional<i64> virtualRowIndex)
{
    YT_VERIFY(chunkMeta->GetChunkType() == EChunkType::Table);

    switch (chunkMeta->GetChunkFormat()) {
        case ETableChunkFormat::SchemalessHorizontal:
            return New<THorizontalSchemalessRangeChunkReader>(
                chunkState,
                chunkMeta,
                config,
                options,
                underlyingReader,
                nameTable,
                blockReadOptions,
                sortColumns,
                omittedInaccessibleColumns,
                columnFilter,
                readRange,
                partitionTag,
                memoryManager,
                virtualRowIndex);

        case ETableChunkFormat::UnversionedColumnar:
            return New<TColumnarSchemalessRangeChunkReader>(
                chunkState,
                chunkMeta,
                config,
                options,
                underlyingReader,
                nameTable,
                blockReadOptions,
                sortColumns,
                omittedInaccessibleColumns,
                columnFilter,
                readRange,
                memoryManager,
                virtualRowIndex);

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
    const TClientBlockReadOptions& blockReadOptions,
    const TSortColumns& sortColumns,
    const std::vector<TString>& omittedInaccessibleColumns,
    const TColumnFilter& columnFilter,
    const TSharedRange<TLegacyKey>& keys,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    std::optional<int> partitionTag,
    const TChunkReaderMemoryManagerPtr& memoryManager)
{
    YT_VERIFY(chunkMeta->GetChunkType() == EChunkType::Table);

    auto formatVersion = chunkMeta->GetChunkFormat();
    switch (formatVersion) {
        case ETableChunkFormat::SchemalessHorizontal:
            return New<THorizontalSchemalessLookupChunkReader>(
                chunkState,
                chunkMeta,
                std::move(config),
                std::move(options),
                std::move(underlyingReader),
                std::move(nameTable),
                blockReadOptions,
                sortColumns,
                omittedInaccessibleColumns,
                columnFilter,
                keys,
                std::move(performanceCounters),
                partitionTag,
                memoryManager);

        case ETableChunkFormat::UnversionedColumnar:
            return New<TColumnarSchemalessLookupChunkReader>(
                chunkState,
                chunkMeta,
                std::move(config),
                std::move(options),
                std::move(underlyingReader),
                std::move(nameTable),
                blockReadOptions,
                sortColumns,
                omittedInaccessibleColumns,
                columnFilter,
                keys,
                std::move(performanceCounters),
                memoryManager);

        default:
            THROW_ERROR_EXCEPTION(
                "This operation is not supported for chunks in %Qv format",
                formatVersion);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
