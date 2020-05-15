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
#include "row_sampler.h"
#include "schemaless_block_reader.h"
#include "versioned_chunk_reader.h"
#include "remote_dynamic_store_reader.h"

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

#include <yt/client/table_client/schema.h>
#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/row_base.h>
#include <yt/client/table_client/row_buffer.h>
#include <yt/client/table_client/schemaful_reader.h>
#include <yt/client/table_client/versioned_reader.h>

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
        const TKeyColumns& keyColumns,
        const std::vector<TString>& omittedInaccessibleColumns)
        : ChunkState_(chunkState)
        , ChunkSpec_(ChunkState_->ChunkSpec)
        , DataSliceDescriptor_(ChunkState_->ChunkSpec)
        , Config_(config)
        , Options_(options)
        , NameTable_(nameTable)
        , ColumnFilter_(columnFilter)
        , KeyColumns_(keyColumns)
        , OmittedInaccessibleColumns_(omittedInaccessibleColumns)
        , OmittedInaccessibleColumnSet_(OmittedInaccessibleColumns_.begin(), OmittedInaccessibleColumns_.end())
        , SystemColumnCount_(GetSystemColumnCount(options))
        , Logger(NLogging::TLogger(TableClientLogger)
            .AddTag("ChunkReaderId: %v", TGuid::Create())
            .AddTag("ChunkId: %v", chunkId))
        , ReaderInvoker_(CreateSerializedInvoker(NChunkClient::TDispatcher::Get()->GetReaderInvoker()))
    {
        if (blockReadOptions.ReadSessionId) {
            Logger.AddTag("ReadSessionId: %v", blockReadOptions.ReadSessionId);
        }

        if (Config_->SamplingRate) {
            RowSampler_ = CreateChunkRowSampler(
                chunkId,
                *Config_->SamplingRate,
                Config_->SamplingSeed.value_or(std::random_device()()));
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

    virtual const TKeyColumns& GetKeyColumns() const override
    {
        return KeyColumns_;
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
    TKeyColumns KeyColumns_;
    const std::vector<TString> OmittedInaccessibleColumns_;
    const THashSet<TStringBuf> OmittedInaccessibleColumnSet_; // strings are being owned by OmittedInaccessibleColumns_

    i64 RowIndex_ = 0;
    i64 RowCount_ = 0;
    i64 DataWeight_ = 0;

    std::unique_ptr<IRowSampler> RowSampler_;
    const int SystemColumnCount_;

    int RowIndexId_ = -1;
    int RangeIndexId_ = -1;
    int TableIndexId_ = -1;
    int TabletIndexId_ = -1;

    NLogging::TLogger Logger;

    IInvokerPtr ReaderInvoker_;

    void InitializeSystemColumnIds()
    {
        try {
            if (Options_->EnableRowIndex) {
                RowIndexId_ = NameTable_->GetIdOrRegisterName(RowIndexColumnName);
            }

            if (Options_->EnableRangeIndex) {
                RangeIndexId_ = NameTable_->GetIdOrRegisterName(RangeIndexColumnName);
            }

            if (Options_->EnableTableIndex) {
                TableIndexId_ = NameTable_->GetIdOrRegisterName(TableIndexColumnName);
            }

            if (Options_->EnableTabletIndex) {
                TabletIndexId_ = NameTable_->GetIdOrRegisterName(TabletIndexColumnName);
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to add system columns to name table for schemaless chunk reader")
                << ex;
        }
    }

    TInterruptDescriptor GetInterruptDescriptorImpl(
        TRange<TUnversionedRow> unreadRows,
        const NChunkClient::NProto::TMiscExt& misc,
        const NProto::TBlockMetaExt& blockMeta,
        const TChunkSpec& chunkSpec,
        const TReadLimit& lowerLimit,
        const TReadLimit& upperLimit,
        const TKeyColumns& keyColumns,
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

        auto lowerKey = lowerLimit.HasKey() ? lowerLimit.GetKey() : TOwningKey();
        auto lastChunkKey = FromProto<TOwningKey>(blockMeta.blocks().rbegin()->last_key());
        auto upperKey = upperLimit.HasKey() ? upperLimit.GetKey() : lastChunkKey;
        TOwningKey firstUnreadKey;
        if (!unreadRows.Empty()) {
            firstUnreadKey = GetKeyPrefix(unreadRows[0], keyColumns.size());
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
                ToProto(chunk.mutable_lower_limit()->mutable_key(), firstUnreadKey);
            }
            i64 rowCount = std::max(1l, chunk.row_count_override() - RowCount_ + static_cast<i64>(unreadRows.Size()));
            rowCount = std::min(rowCount, upperRowIndex - rowIndex);
            chunk.set_row_count_override(rowCount);
            i64 dataWeight = DivCeil(
                misc.has_data_weight() ? misc.data_weight() : misc.uncompressed_data_size(),
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
        const TKeyColumns& keyColumns,
        const std::vector<TString>& omittedInaccessibleColumns,
        const TColumnFilter& columnFilter,
        std::optional<int> partitionTag,
        const TChunkReaderMemoryManagerPtr& memoryManager);

    virtual TDataStatistics GetDataStatistics() const override;

protected:
    using TSchemalessChunkReaderBase::Config_;
    using TSchemalessChunkReaderBase::Logger;

    const TColumnarChunkMetaPtr ChunkMeta_;

    int ChunkKeyColumnCount_ = 0;

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
    const TKeyColumns& keyColumns,
    const std::vector<TString>& omittedInaccessibleColumns,
    const TColumnFilter& columnFilter,
    std::optional<int> partitionTag,
    const TChunkReaderMemoryManagerPtr& memoryManager)
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
        keyColumns,
        omittedInaccessibleColumns)
    , ChunkMeta_(chunkMeta)
    , PartitionTag_(partitionTag)
{ }

TFuture<void> THorizontalSchemalessChunkReaderBase::InitializeBlockSequence()
{
    YT_VERIFY(BlockIndexes_.empty());

    InitializeSystemColumnIds();

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
        const TKeyColumns& keyColumns,
        const std::vector<TString>& omittedInaccessibleColumns,
        const TColumnFilter& columnFilter,
        const TReadRange& readRange,
        std::optional<int> partitionTag,
        const TChunkReaderMemoryManagerPtr& memoryManager);

    virtual bool Read(std::vector<TUnversionedRow>* rows) override;
    virtual TInterruptDescriptor GetInterruptDescriptor(TRange<TUnversionedRow> unreadRows) const override;

private:
    TReadRange ReadRange_;

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
    const TKeyColumns& keyColumns,
    const std::vector<TString>& omittedInaccessibleColumns,
    const TColumnFilter& columnFilter,
    const TReadRange& readRange,
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
        keyColumns,
        omittedInaccessibleColumns,
        columnFilter,
        partitionTag,
        memoryManager)
    , ReadRange_(readRange)
{
    YT_LOG_DEBUG("Reading range %v", ReadRange_);

    // Initialize to lowest reasonable value.
    RowIndex_ = ReadRange_.LowerLimit().HasRowIndex()
        ? ReadRange_.LowerLimit().GetRowIndex()
        : 0;

    // Ready event must be set only when all initialization is finished and
    // RowIndex_ is set into proper value.
    // Must be called after the object is constructed and vtable initialized.
    ReadyEvent_ = BIND(&THorizontalSchemalessRangeChunkReader::InitializeBlockSequence, MakeStrong(this))
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
        }));
}

void THorizontalSchemalessRangeChunkReader::DoInitializeBlockSequence()
{
    InitializeIdMapping();

    if (PartitionTag_) {
        YT_VERIFY(ReadRange_.LowerLimit().IsTrivial());
        YT_VERIFY(ReadRange_.UpperLimit().IsTrivial());

        CreateBlockSequence(0, BlockMetaExt_->blocks_size());
    } else {
        bool readSorted = ReadRange_.LowerLimit().HasKey() || ReadRange_.UpperLimit().HasKey() || !KeyColumns_.empty();
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

    auto chunkKeyColumns = ChunkMeta_->ChunkSchema().GetKeyColumns();
    ChunkKeyColumnCount_ = chunkKeyColumns.size();

    ValidateKeyColumns(KeyColumns_, chunkKeyColumns, Options_->DynamicTable);

    if (KeyColumns_.empty()) {
        KeyColumns_ = chunkKeyColumns;
    }

    std::optional<int> keyColumnCount;
    if (Options_->DynamicTable) {
        keyColumnCount = KeyColumns_.size();
    } else {
        ChunkMeta_->InitBlockLastKeys(KeyColumns_);
    }

    int beginIndex = std::max(
        ApplyLowerRowLimit(*BlockMetaExt_, ReadRange_.LowerLimit()),
        ApplyLowerKeyLimit(ChunkMeta_->BlockLastKeys(), ReadRange_.LowerLimit(), keyColumnCount));
    int endIndex = std::min(
        ApplyUpperRowLimit(*BlockMetaExt_, ReadRange_.UpperLimit()),
        ApplyUpperKeyLimit(ChunkMeta_->BlockLastKeys(), ReadRange_.UpperLimit(), keyColumnCount));

    CreateBlockSequence(beginIndex, endIndex);
}

void THorizontalSchemalessRangeChunkReader::CreateBlockSequence(int beginIndex, int endIndex)
{
    for (int index = beginIndex; index < endIndex; ++index) {
        BlockIndexes_.push_back(index);
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
        ChunkMeta_->ChunkSchema(),
        IdMapping_,
        ChunkKeyColumnCount_,
        KeyColumns_.size(),
        SystemColumnCount_));

    RowIndex_ = blockMeta.chunk_row_count() - blockMeta.row_count();

    int keyColumnCount = std::max(ChunkKeyColumnCount_, static_cast<int>(KeyColumns_.size()));
    CheckBlockUpperLimits(
        BlockMetaExt_->blocks(blockIndex).chunk_row_count(),
        ChunkMeta_->BlockLastKeys() ? ChunkMeta_->BlockLastKeys()[blockIndex] : TKey(),
        ReadRange_.UpperLimit(),
        keyColumnCount);

    const auto& lowerLimit = ReadRange_.LowerLimit();

    if (lowerLimit.HasRowIndex() && RowIndex_ < lowerLimit.GetRowIndex()) {
        YT_VERIFY(BlockReader_->SkipToRowIndex(lowerLimit.GetRowIndex() - RowIndex_));
        RowIndex_ = lowerLimit.GetRowIndex();
    }

    if (lowerLimit.HasKey()) {
        auto blockRowIndex = BlockReader_->GetRowIndex();
        YT_VERIFY(BlockReader_->SkipToKey(lowerLimit.GetKey().Get()));
        RowIndex_ += BlockReader_->GetRowIndex() - blockRowIndex;
    }
}

void THorizontalSchemalessRangeChunkReader::InitNextBlock()
{
    ++CurrentBlockIndex_;
    InitFirstBlock();
}

bool THorizontalSchemalessRangeChunkReader::Read(std::vector<TUnversionedRow>* rows)
{
    YT_VERIFY(rows->capacity() > 0);

    MemoryPool_.Clear();
    rows->clear();

    if (!BeginRead()) {
        // Not ready yet.
        return true;
    }

    if (!BlockReader_) {
        // Nothing to read from chunk.
        return false;
    }

    if (BlockEnded_) {
        BlockReader_.reset();
        return OnBlockEnded();
    }

    i64 dataWeight = 0;
    while (rows->size() < rows->capacity() && dataWeight < Config_->MaxDataSizePerRead && !BlockEnded_) {
        if ((CheckRowLimit_ && RowIndex_ >= ReadRange_.UpperLimit().GetRowIndex()) ||
            (CheckKeyLimit_ && CompareRows(BlockReader_->GetKey(), ReadRange_.UpperLimit().GetKey()) >= 0))
        {
            BlockEnded_ = true;
            return true;
        }

        if (!RowSampler_ || RowSampler_->ShouldTakeRow(GetTableRowIndex())) {
            auto row = BlockReader_->GetRow(&MemoryPool_);
            if (Options_->EnableRangeIndex) {
                *row.End() = MakeUnversionedInt64Value(ChunkSpec_.range_index(), RangeIndexId_);
                row.SetCount(row.GetCount() + 1);
            }
            if (Options_->EnableTableIndex) {
                *row.End() = MakeUnversionedInt64Value(ChunkSpec_.table_index(), TableIndexId_);
                row.SetCount(row.GetCount() + 1);
            }
            if (Options_->EnableRowIndex) {
                *row.End() = MakeUnversionedInt64Value(GetTableRowIndex(), RowIndexId_);
                row.SetCount(row.GetCount() + 1);
            }
            if (Options_->EnableTabletIndex) {
                *row.End() = MakeUnversionedInt64Value(ChunkSpec_.tablet_index(), TabletIndexId_);
                row.SetCount(row.GetCount() + 1);
            }

            rows->push_back(row);

            auto rowWeight = GetDataWeight(rows->back());
            dataWeight += rowWeight;
            DataWeight_ += rowWeight;
            ++RowCount_;
        }
        ++RowIndex_;

        if (!BlockReader_->NextRow()) {
            BlockEnded_ = true;
        }
    }

    return true;
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
        KeyColumns_,
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
        const TKeyColumns& keyColumns,
        const std::vector<TString>& omittedInaccessibleColumns,
        const TColumnFilter& columnFilter,
        const TSharedRange<TKey>& keys,
        TChunkReaderPerformanceCountersPtr performanceCounters,
        std::optional<int> partitionTag = std::nullopt,
        const TChunkReaderMemoryManagerPtr& memoryManager = nullptr);

    virtual bool Read(std::vector<TUnversionedRow>* rows) override;

private:
    const TSharedRange<TKey> Keys_;
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
    const TKeyColumns& keyColumns,
    const std::vector<TString>& omittedInaccessibleColumns,
    const TColumnFilter& columnFilter,
    const TSharedRange<TKey>& keys,
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
        keyColumns,
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
    ReadyEvent_ = BIND(&THorizontalSchemalessLookupChunkReader::InitializeBlockSequence, MakeStrong(this))
        .AsyncVia(ReaderInvoker_)
        .Run()
        .Apply(BIND([this, this_ = MakeStrong(this)] () {
            if (InitFirstBlockNeeded_) {
                InitFirstBlock();
                InitFirstBlockNeeded_ = false;
            }
        }));
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

    auto chunkKeyColumns = ChunkMeta_->ChunkSchema().GetKeyColumns();
    ChunkKeyColumnCount_ = chunkKeyColumns.size();

    ValidateKeyColumns(KeyColumns_, chunkKeyColumns, Options_->DynamicTable);

    // Don't call InitBlockLastKeys because this reader should be used only for dynamic tables.
    YT_VERIFY(Options_->DynamicTable);

    for (const auto& key : Keys_) {
        TReadLimit readLimit;
        readLimit.SetKey(TOwningKey(key));

        int index = ApplyLowerKeyLimit(ChunkMeta_->BlockLastKeys(), readLimit, KeyColumns_.size());
        if (index == BlockMetaExt_->blocks_size()) {
            break;
        }

        if (BlockIndexes_.empty() || BlockIndexes_.back() != index) {
            BlockIndexes_.push_back(index);
        }
    }
}

bool THorizontalSchemalessLookupChunkReader::Read(std::vector<TUnversionedRow>* rows)
{
    YT_VERIFY(rows->capacity() > 0);

    MemoryPool_.Clear();
    rows->clear();

    i64 dataWeight = 0;

    auto doRead = [&] {
        if (!BeginRead()) {
            // Not ready yet.
            return true;
        }

        if (!BlockReader_) {
            // Nothing to read from chunk.
            if (RowCount_ == Keys_.Size()) {
                return false;
            }

            while (rows->size() < rows->capacity() && RowCount_ < Keys_.Size()) {
                rows->push_back(TUnversionedRow());
                ++RowCount_;
            }
            return true;
        }

        if (BlockEnded_) {
            BlockReader_.reset();
            OnBlockEnded();
            return true;
        }

        while (rows->size() < rows->capacity()) {
            if (RowCount_ == Keys_.Size()) {
                BlockEnded_ = true;
                return true;
            }

            if (!KeyFilterTest_[RowCount_]) {
                rows->push_back(TUnversionedRow());
            } else {
                const auto& key = Keys_[RowCount_];
                if (!BlockReader_->SkipToKey(key)) {
                    BlockEnded_ = true;
                    return true;
                }

                if (key == BlockReader_->GetKey()) {
                    auto row = BlockReader_->GetRow(&MemoryPool_);
                    rows->push_back(row);
                    dataWeight += GetDataWeight(row);

                    int blockIndex = BlockIndexes_[CurrentBlockIndex_];
                    const auto& blockMeta = BlockMetaExt_->blocks(blockIndex);
                    RowIndex_ = blockMeta.chunk_row_count() - blockMeta.row_count() + BlockReader_->GetRowIndex();
                } else {
                    rows->push_back(TUnversionedRow());
                }
            }

            ++RowCount_;
        }

        return true;
    };

    bool result = doRead();

    if (PerformanceCounters_) {
        PerformanceCounters_->StaticChunkRowLookupCount += rows->size();
        PerformanceCounters_->StaticChunkRowLookupDataWeightCount += dataWeight;
    }

    return result;
}

void THorizontalSchemalessLookupChunkReader::InitFirstBlock()
{
    int blockIndex = BlockIndexes_[CurrentBlockIndex_];
    const auto& blockMeta = BlockMetaExt_->blocks(blockIndex);

    BlockReader_.reset(new THorizontalBlockReader(
        CurrentBlock_.Get().ValueOrThrow().Data,
        blockMeta,
        ChunkMeta_->ChunkSchema(),
        IdMapping_,
        ChunkKeyColumnCount_,
        KeyColumns_.size(),
        SystemColumnCount_));
}

void THorizontalSchemalessLookupChunkReader::InitNextBlock()
{
    ++CurrentBlockIndex_;
    InitFirstBlock();
}

////////////////////////////////////////////////////////////////////////////////

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
        const TKeyColumns& keyColumns,
        const std::vector<TString>& omittedInaccessibleColumns,
        const TColumnFilter& columnFilter,
        const TReadRange& readRange,
        const TChunkReaderMemoryManagerPtr& memoryManager)
        : TSchemalessChunkReaderBase(
            chunkState,
            config,
            options,
            underlyingReader->GetChunkId(),
            nameTable,
            blockReadOptions,
            columnFilter,
            keyColumns,
            omittedInaccessibleColumns)
        , TColumnarRangeChunkReaderBase(
            chunkMeta,
            config,
            underlyingReader,
            chunkState->BlockCache,
            blockReadOptions,
            memoryManager)
    {
        YT_LOG_DEBUG("Reading range %v", readRange);

        LowerLimit_ = readRange.LowerLimit();
        UpperLimit_ = readRange.UpperLimit();

        RowIndex_ = LowerLimit_.HasRowIndex() ? LowerLimit_.GetRowIndex() : 0;

        ReadyEvent_ = BIND(&TColumnarSchemalessRangeChunkReader::InitializeBlockSequence, MakeStrong(this))
            .AsyncVia(ReaderInvoker_)
            .Run();
    }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        YT_VERIFY(rows->capacity() > 0);
        rows->clear();
        Pool_.Clear();

        if (!ReadyEvent_.IsSet() || !ReadyEvent_.Get().IsOK()) {
            return true;
        }

        if (Completed_) {
            return false;
        }

        i64 dataWeight = 0;
        while (rows->size() < rows->capacity()) {
            ResetExhaustedColumns();

            // Define how many to read.
            i64 rowLimit = static_cast<i64>(rows->capacity() - rows->size());

            // Each read must be fully below or fully above SafeUpperRowLimit,
            // to determine if we should read and validate keys.
            if (RowIndex_ < SafeUpperRowIndex_) {
                rowLimit = std::min(SafeUpperRowIndex_ - RowIndex_, rowLimit);
            } else {
                rowLimit = std::min(HardUpperRowIndex_ - RowIndex_, rowLimit);
            }

            for (const auto& column : Columns_) {
                rowLimit = std::min(column.ColumnReader->GetReadyUpperRowIndex() - RowIndex_, rowLimit);
            }

            YT_VERIFY(rowLimit > 0);

            if (!LowerKeyLimitReached_) {
                auto keys = ReadKeys(rowLimit);

                i64 deltaIndex = 0;
                for (; deltaIndex < rowLimit; ++deltaIndex) {
                    if (keys[deltaIndex] >= LowerLimit_.GetKey()) {
                        break;
                    }
                }

                rowLimit -= deltaIndex;
                RowIndex_ += deltaIndex;

                // Rewind row column readers to proper row index.
                for (const auto& reader : RowColumnReaders_) {
                    reader->SkipToRowIndex(RowIndex_);
                }
                if (SchemalessReader_) {
                    SchemalessReader_->SkipToRowIndex(RowIndex_);
                }

                LowerKeyLimitReached_ = (rowLimit > 0);

                // We could have overcome upper limit, we must check it.
                if (RowIndex_ >= SafeUpperRowIndex_ && UpperLimit_.HasKey()) {
                    auto keyRange = MakeRange(keys.data() + deltaIndex, keys.data() + keys.size());
                    while (rowLimit > 0 && keyRange[rowLimit - 1] >= UpperLimit_.GetKey()) {
                        --rowLimit;
                        Completed_ = true;
                    }
                }
            } else if (RowIndex_ >= SafeUpperRowIndex_ && UpperLimit_.HasKey()) {
                auto keys = ReadKeys(rowLimit);
                while (rowLimit > 0 && keys[rowLimit - 1] >= UpperLimit_.GetKey()) {
                    --rowLimit;
                    Completed_ = true;
                }
            } else {
                // We do not read keys, so we must skip rows for key readers.
                for (const auto& reader : KeyColumnReaders_) {
                    reader->SkipToRowIndex(RowIndex_ + rowLimit);
                }
            }

            dataWeight += ReadRows(rowLimit, rows);

            RowIndex_ += rowLimit;

            if (RowIndex_ == HardUpperRowIndex_) {
                Completed_ = true;
            }

            if (Completed_ || !TryFetchNextRow() ||
                dataWeight > TSchemalessChunkReaderBase::Config_->MaxDataSizePerRead)
            {
                break;
            }
        }

        if (RowSampler_) {
            i64 insertIndex = 0;
            // ToDo(psushin): fix data weight statistics row sampler is used.

            std::vector<TUnversionedRow>& rowsRef = *rows;
            for (i64 rowIndex = 0; rowIndex < rows->size(); ++rowIndex) {
                i64 tableRowIndex = ChunkSpec_.table_row_index() + RowIndex_ - rows->size() + rowIndex;
                if (RowSampler_->ShouldTakeRow(tableRowIndex)) {
                    rowsRef[insertIndex] = rowsRef[rowIndex];
                    ++insertIndex;
                }
            }
            rows->resize(insertIndex);
        }

        RowCount_ += rows->size();
        DataWeight_ += dataWeight;

        return true;
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
            KeyColumns_,
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

    TChunkedMemoryPool Pool_;


    void InitializeBlockSequence()
    {
        YT_VERIFY(ChunkMeta_->GetChunkFormat() == ETableChunkFormat::UnversionedColumnar);
        InitializeSystemColumnIds();

        // Minimum prefix of key columns, that must be included in column filter.
        int minKeyColumnCount = 0;
        if (UpperLimit_.HasKey()) {
            minKeyColumnCount = std::max(minKeyColumnCount, UpperLimit_.GetKey().GetCount());
        }
        if (LowerLimit_.HasKey()) {
            minKeyColumnCount = std::max(minKeyColumnCount, LowerLimit_.GetKey().GetCount());
        }
        bool sortedRead = minKeyColumnCount > 0 || !KeyColumns_.empty();

        if (sortedRead && !ChunkMeta_->Misc().sorted()) {
            THROW_ERROR_EXCEPTION("Requested a sorted read for an unsorted chunk");
        }

        const auto& chunkSchema = ChunkMeta_->ChunkSchema();
        const auto& columnMeta = ChunkMeta_->ColumnMeta();

        ValidateKeyColumns(
            KeyColumns_,
            chunkSchema.GetKeyColumns(),
            Options_->DynamicTable);

        // Cannot read more key columns than stored in chunk, even if range keys are longer.
        minKeyColumnCount = std::min(minKeyColumnCount, chunkSchema.GetKeyColumnCount());

        TNameTablePtr chunkNameTable;
        if (Options_->DynamicTable) {
            chunkNameTable = TNameTable::FromSchema(chunkSchema);
        } else {
            chunkNameTable = ChunkMeta_->ChunkNameTable();
            if (UpperLimit_.HasKey() || LowerLimit_.HasKey()) {
                ChunkMeta_->InitBlockLastKeys(KeyColumns_.empty()
                    ? chunkSchema.GetKeyColumns()
                    : KeyColumns_);
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
            auto columnReader = CreateUnversionedColumnReader(
                chunkSchema.Columns()[columnIndex],
                columnMeta->columns(columnIndex),
                valueIndex,
                NameTable_->GetIdOrRegisterName(chunkSchema.Columns()[columnIndex].Name()));
            RowColumnReaders_.emplace_back(columnReader.get());

            Columns_.emplace_back(std::move(columnReader), columnIndex);
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

        for (int keyIndex = 0; keyIndex < minKeyColumnCount; ++keyIndex) {
            auto columnReader = CreateUnversionedColumnReader(
                chunkSchema.Columns()[keyIndex],
                columnMeta->columns(keyIndex),
                keyIndex,
                keyIndex); // Column id doesn't really matter.
            KeyColumnReaders_.emplace_back(columnReader.get());

            Columns_.emplace_back(std::move(columnReader), keyIndex);
        }

        for (int keyIndex = minKeyColumnCount; keyIndex < KeyColumns_.size(); ++keyIndex) {
            auto columnReader = CreateBlocklessUnversionedNullColumnReader(
                keyIndex,
                keyIndex);
            KeyColumnReaders_.emplace_back(columnReader.get());

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

            ResetExhaustedColumns();
            Initialize(MakeRange(KeyColumnReaders_));
            RowIndex_ = LowerRowIndex_;
            LowerKeyLimitReached_ = !LowerLimit_.HasKey();

            YT_LOG_DEBUG("Initialized start row index (LowerKeyLimitReached: %v, RowIndex: %v)",
                LowerKeyLimitReached_,
                RowIndex_);

            if (RowIndex_ >= HardUpperRowIndex_) {
                Completed_ = true;
            }
        } else {
            Completed_ = true;
        }
    }

    std::vector<TKey> ReadKeys(i64 rowCount)
    {
        std::vector<TKey> keys;

        for (i64 index = 0; index < rowCount; ++index) {
            auto key = TMutableKey::Allocate(
                &Pool_,
                KeyColumnReaders_.size());
            key.SetCount(KeyColumnReaders_.size());
            keys.push_back(key);
        }

        auto range = TMutableRange<TMutableKey>(
            static_cast<TMutableKey*>(keys.data()),
            static_cast<TMutableKey*>(keys.data() + rowCount));

        for (auto& columnReader : KeyColumnReaders_) {
            columnReader->ReadValues(range);
        }
        return keys;
    }

    //! Returns read data weight.
    i64 ReadRows(i64 rowCount, std::vector<TUnversionedRow>* rows)
    {
        std::vector<ui32> schemalessColumnCount(rowCount, 0);
        if (SchemalessReader_) {
            SchemalessReader_->GetValueCounts(TMutableRange<ui32>(
                schemalessColumnCount.data(),
                schemalessColumnCount.size()));
        }

        int rangeBegin = rows->size();
        for (i64 index = 0; index < rowCount; ++index) {
            auto row = TMutableUnversionedRow::Allocate(
                &Pool_,
                RowColumnReaders_.size() + schemalessColumnCount[index] + SystemColumnCount_);
            row.SetCount(RowColumnReaders_.size());
            rows->push_back(row);
        }

        auto range = TMutableRange<TMutableUnversionedRow>(
            static_cast<TMutableUnversionedRow*>(rows->data() + rangeBegin),
            static_cast<TMutableUnversionedRow*>(rows->data() + rangeBegin + rowCount));

        // Read values.
        for (auto& columnReader : RowColumnReaders_) {
            columnReader->ReadValues(range);
        }

        if (SchemalessReader_) {
            SchemalessReader_->ReadValues(range);
        }

        i64 dataWeight = 0;

        // Append system columns.
        for (i64 index = 0; index < rowCount; ++index) {
            auto row = range[index];
            if (Options_->EnableRangeIndex) {
                *row.End() = MakeUnversionedInt64Value(ChunkSpec_.range_index(), RangeIndexId_);
                row.SetCount(row.GetCount() + 1);
            }
            if (Options_->EnableTableIndex) {
                *row.End() = MakeUnversionedInt64Value(ChunkSpec_.table_index(), TableIndexId_);
                row.SetCount(row.GetCount() + 1);
            }
            if (Options_->EnableRowIndex) {
                *row.End() = MakeUnversionedInt64Value(
                    GetTableRowIndex() + index,
                    RowIndexId_);
                row.SetCount(row.GetCount() + 1);
            }
            if (Options_->EnableTabletIndex) {
                *row.End() = MakeUnversionedInt64Value(ChunkSpec_.tablet_index(), TabletIndexId_);
                row.SetCount(row.GetCount() + 1);
            }

            dataWeight += GetDataWeight(row);
        }

        return dataWeight;
    }
};

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
        const TKeyColumns& keyColumns,
        const std::vector<TString>& omittedInaccessibleColumns,
        const TColumnFilter& columnFilter,
        const TSharedRange<TKey>& keys,
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
            keyColumns,
            omittedInaccessibleColumns)
        , TColumnarLookupChunkReaderBase(
            chunkMeta,
            config,
            underlyingReader,
            chunkState->BlockCache,
            blockReadOptions,
            memoryManager)
        , PerformanceCounters_(std::move(performanceCounters))
    {
        Keys_ = keys;

        ReadyEvent_ = BIND(&TColumnarSchemalessLookupChunkReader::InitializeBlockSequence, MakeStrong(this))
            .AsyncVia(ReaderInvoker_)
            .Run();
    }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        rows->clear();
        Pool_.Clear();

        if (!ReadyEvent_.IsSet() || !ReadyEvent_.Get().IsOK()) {
            return true;
        }

        if (NextKeyIndex_ == Keys_.Size()) {
            return false;
        }

        i64 dataWeight = 0;
        while (rows->size() < rows->capacity()) {
            ResetExhaustedColumns();

            if (RowIndexes_[NextKeyIndex_] < ChunkMeta_->Misc().row_count()) {
                const auto& key = Keys_[NextKeyIndex_];

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
                    rows->push_back(TMutableUnversionedRow());
                } else {
                    // Key can be present in exactly one row.
                    YT_VERIFY(upperRowIndex == lowerRowIndex + 1);
                    i64 rowIndex = lowerRowIndex;

                    rows->push_back(ReadRow(rowIndex));
                }
            } else {
                // Key oversteps chunk boundaries.
                rows->push_back(TMutableUnversionedRow());
            }

            dataWeight += GetDataWeight(rows->back());

            if (++NextKeyIndex_ == Keys_.Size() || !TryFetchNextRow() || dataWeight > TSchemalessChunkReaderBase::Config_->MaxDataSizePerRead) {
                break;
            }
        }

        i64 rowCount = rows->size();

        if (PerformanceCounters_) {
            PerformanceCounters_->StaticChunkRowLookupCount += rowCount;
            PerformanceCounters_->StaticChunkRowLookupDataWeightCount += dataWeight;
        }

        RowCount_ += rowCount;
        DataWeight_ += dataWeight;

        return true;
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
            SchemalessReader_->GetValueCounts(TMutableRange<ui32>(&schemalessColumnCount, 1));
        }

        auto row = TMutableUnversionedRow::Allocate(&Pool_, RowColumnReaders_.size() + schemalessColumnCount + SystemColumnCount_);
        row.SetCount(RowColumnReaders_.size());

        // Read values.
        auto range = TMutableRange<TMutableUnversionedRow>(&row, 1);

        for (auto& columnReader : RowColumnReaders_) {
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
        InitializeSystemColumnIds();

        if (!ChunkMeta_->Misc().sorted()) {
            THROW_ERROR_EXCEPTION("Requested a sorted read for an unsorted chunk");
        }

        const auto& chunkSchema = ChunkMeta_->ChunkSchema();
        const auto& columnMeta = ChunkMeta_->ColumnMeta();

        ValidateKeyColumns(
            KeyColumns_,
            chunkSchema.GetKeyColumns(),
            Options_->DynamicTable);

        TNameTablePtr chunkNameTable;
        if (Options_->DynamicTable) {
            chunkNameTable = TNameTable::FromSchema(chunkSchema);
        } else {
            chunkNameTable = ChunkMeta_->ChunkNameTable();
            ChunkMeta_->InitBlockLastKeys(KeyColumns_);
        }

        // Create key column readers.
        KeyColumnReaders_.resize(KeyColumns_.size());
        for (int keyColumnIndex = 0; keyColumnIndex < chunkSchema.GetKeyColumnCount(); ++keyColumnIndex) {
            auto columnReader = CreateUnversionedColumnReader(
                chunkSchema.Columns()[keyColumnIndex],
                columnMeta->columns(keyColumnIndex),
                keyColumnIndex,
                keyColumnIndex);

            KeyColumnReaders_[keyColumnIndex] = columnReader.get();
            Columns_.emplace_back(std::move(columnReader), keyColumnIndex);
        }
        for (int keyColumnIndex = chunkSchema.GetKeyColumnCount(); keyColumnIndex < KeyColumns_.size(); ++keyColumnIndex) {
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

ISchemalessChunkReaderPtr CreateSchemalessChunkReader(
    const TChunkStatePtr& chunkState,
    const TColumnarChunkMetaPtr& chunkMeta,
    TChunkReaderConfigPtr config,
    TChunkReaderOptionsPtr options,
    NChunkClient::IChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    const TClientBlockReadOptions& blockReadOptions,
    const TKeyColumns& keyColumns,
    const std::vector<TString>& omittedInaccessibleColumns,
    const TColumnFilter& columnFilter,
    const TReadRange& readRange,
    std::optional<int> partitionTag,
    const TChunkReaderMemoryManagerPtr& memoryManager)
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
                keyColumns,
                omittedInaccessibleColumns,
                columnFilter,
                readRange,
                partitionTag,
                memoryManager);

        case ETableChunkFormat::UnversionedColumnar:
            return New<TColumnarSchemalessRangeChunkReader>(
                chunkState,
                chunkMeta,
                config,
                options,
                underlyingReader,
                nameTable,
                blockReadOptions,
                keyColumns,
                omittedInaccessibleColumns,
                columnFilter,
                readRange,
                memoryManager);

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessChunkReaderPtr CreateSchemalessChunkReader(
    const TChunkStatePtr& chunkState,
    const TColumnarChunkMetaPtr& chunkMeta,
    TChunkReaderConfigPtr config,
    TChunkReaderOptionsPtr options,
    NChunkClient::IChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    const TClientBlockReadOptions& blockReadOptions,
    const TKeyColumns& keyColumns,
    const std::vector<TString>& omittedInaccessibleColumns,
    const TColumnFilter& columnFilter,
    const TSharedRange<TKey>& keys,
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
                keyColumns,
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
                keyColumns,
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
