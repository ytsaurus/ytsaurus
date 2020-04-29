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
#include "schemaless_chunk_reader.h"
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
#include <yt/ytlib/chunk_client/multi_reader_base.h>
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

TColumnarChunkMetaPtr DownloadChunkMeta(
    IChunkReaderPtr chunkReader,
    const TClientBlockReadOptions& blockReadOptions,
    std::optional<int> partitionTag)
{
    // Download chunk meta.
    std::vector<int> extensionTags = {
        TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value,
        TProtoExtensionTag<NProto::TTableSchemaExt>::Value,
        TProtoExtensionTag<NProto::TBlockMetaExt>::Value,
        TProtoExtensionTag<NProto::TColumnMetaExt>::Value,
        TProtoExtensionTag<NProto::TNameTableExt>::Value,
        TProtoExtensionTag<NProto::TKeyColumnsExt>::Value
    };

    auto asyncChunkMeta = chunkReader->GetMeta(
        blockReadOptions,
        partitionTag,
        extensionTags);
    auto chunkMeta = WaitFor(asyncChunkMeta)
        .ValueOrThrow();

    return New<TColumnarChunkMeta>(*chunkMeta);
}

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

namespace {

TChunkReaderConfigPtr PatchConfig(TChunkReaderConfigPtr config, i64 memoryEstimate)
{
    if (memoryEstimate > config->WindowSize + config->GroupSize) {
        return config;
    }

    auto newConfig = CloneYsonSerializable(config);
    newConfig->WindowSize = std::max(memoryEstimate / 2, (i64) 1);
    newConfig->GroupSize = std::max(memoryEstimate / 2, (i64) 1);
    return newConfig;
}

std::vector<IReaderFactoryPtr> CreateReaderFactories(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    NNative::IClientPtr client,
    const TNodeDescriptor& localDescriptor,
    std::optional<TNodeId> localNodeId,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const TDataSourceDirectoryPtr& dataSourceDirectory,
    const std::vector<TDataSliceDescriptor>& dataSliceDescriptors,
    TNameTablePtr nameTable,
    const TClientBlockReadOptions& blockReadOptions,
    const TColumnFilter& columnFilter,
    const TKeyColumns& keyColumns,
    std::optional<int> partitionTag,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr bandwidthThrottler,
    IThroughputThrottlerPtr rpsThrottler,
    IMultiReaderMemoryManagerPtr multiReaderMemoryManager)
{
    std::vector<IReaderFactoryPtr> factories;
    for (const auto& dataSliceDescriptor : dataSliceDescriptors) {
        const auto& dataSource = dataSourceDirectory->DataSources()[dataSliceDescriptor.GetDataSourceIndex()];

        switch (dataSource.GetType()) {
            case EDataSourceType::UnversionedTable: {
                const auto& chunkSpec = dataSliceDescriptor.GetSingleChunk();

                auto memoryEstimate = GetChunkReaderMemoryEstimate(chunkSpec, config);
                auto createReader = [=] {
                    try {
                        auto remoteReader = CreateRemoteReader(
                            chunkSpec,
                            config,
                            options,
                            client,
                            nodeDirectory,
                            localDescriptor,
                            localNodeId,
                            blockCache,
                            trafficMeter,
                            bandwidthThrottler,
                            rpsThrottler);

                        TReadRange range{
                            chunkSpec.has_lower_limit() ? TReadLimit(chunkSpec.lower_limit()) : TReadLimit(),
                            chunkSpec.has_upper_limit() ? TReadLimit(chunkSpec.upper_limit()) : TReadLimit()
                        };

                        auto asyncChunkMeta = BIND(DownloadChunkMeta, remoteReader, blockReadOptions, partitionTag)
                            .AsyncVia(NChunkClient::TDispatcher::Get()->GetReaderInvoker())
                            .Run();
                        auto chunkMeta = WaitFor(asyncChunkMeta)
                            .ValueOrThrow();
                        chunkMeta->RenameColumns(dataSource.ColumnRenameDescriptors());

                        auto chunkState = New<TChunkState>(
                            blockCache,
                            chunkSpec,
                            nullptr,
                            NullTimestamp,
                            nullptr,
                            nullptr,
                            nullptr);

                        auto chunkReaderMemoryManager =
                            multiReaderMemoryManager->CreateChunkReaderMemoryManager(memoryEstimate);

                        return CreateSchemalessChunkReader(
                            std::move(chunkState),
                            std::move(chunkMeta),
                            PatchConfig(config, memoryEstimate),
                            options,
                            remoteReader,
                            nameTable,
                            blockReadOptions,
                            keyColumns,
                            dataSource.OmittedInaccessibleColumns(),
                            columnFilter.IsUniversal() ? CreateColumnFilter(dataSource.Columns(), nameTable) : columnFilter,
                            range,
                            partitionTag,
                            chunkReaderMemoryManager);
                    } catch(const std::exception& ex) {
                        THROW_ERROR_EXCEPTION("Error creating chunk reader")
                            << TErrorAttribute("chunk_id", chunkSpec.chunk_id())
                            << ex;
                    }
                };

                auto canCreateReader = [=] {
                    return multiReaderMemoryManager->GetFreeMemorySize() >= memoryEstimate;
                };

                factories.emplace_back(CreateReaderFactory(createReader, canCreateReader, dataSliceDescriptor));
                break;
            }

            case EDataSourceType::VersionedTable: {
                auto memoryEstimate = GetDataSliceDescriptorReaderMemoryEstimate(dataSliceDescriptor, config);
                int dataSourceIndex = dataSliceDescriptor.GetDataSourceIndex();
                const auto& dataSource = dataSourceDirectory->DataSources()[dataSourceIndex];
                auto createReader = [=] {
                    return CreateSchemalessMergingMultiChunkReader(
                        config,
                        options,
                        client,
                        localDescriptor,
                        localNodeId,
                        blockCache,
                        nodeDirectory,
                        dataSourceDirectory,
                        dataSliceDescriptor,
                        nameTable,
                        blockReadOptions,
                        columnFilter.IsUniversal() ? CreateColumnFilter(dataSource.Columns(), nameTable) : columnFilter,
                        trafficMeter,
                        bandwidthThrottler,
                        rpsThrottler);
                };

                auto canCreateReader = [=] {
                    return multiReaderMemoryManager->GetFreeMemorySize() >= memoryEstimate;
                };

                factories.emplace_back(CreateReaderFactory(createReader, canCreateReader, dataSliceDescriptor));
                break;
            }

            default:
                YT_ABORT();
        }
    }

    return factories;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

template <class TBase>
class TSchemalessMultiChunkReader
    : public ISchemalessMultiChunkReader
    , public TBase
{
public:
    TSchemalessMultiChunkReader(
        TTableReaderConfigPtr config,
        TTableReaderOptionsPtr options,
        NNative::IClientPtr client,
        const TNodeDescriptor& localDescriptor,
        std::optional<TNodeId> localNodeId,
        IBlockCachePtr blockCache,
        TNodeDirectoryPtr nodeDirectory,
        const TDataSourceDirectoryPtr& dataSourceDirectory,
        const std::vector<TDataSliceDescriptor>& dataSliceDescriptors,
        TNameTablePtr nameTable,
        const TClientBlockReadOptions& blockReadOptions,
        const TColumnFilter& columnFilter,
        const TKeyColumns& keyColumns,
        std::optional<int> partitionTag,
        TTrafficMeterPtr trafficMeter,
        IThroughputThrottlerPtr bandwidthThrottler,
        IThroughputThrottlerPtr rpsThrottler,
        NChunkClient::IMultiReaderMemoryManagerPtr multiReaderMemoryManager);

    ~TSchemalessMultiChunkReader();

    virtual bool Read(std::vector<TUnversionedRow>* rows) override;

    virtual i64 GetSessionRowIndex() const override;
    virtual i64 GetTotalRowCount() const override;
    virtual i64 GetTableRowIndex() const override;

    virtual const TNameTablePtr& GetNameTable() const override;
    virtual const TKeyColumns& GetKeyColumns() const override;

    virtual void Interrupt() override;

    virtual void SkipCurrentReader() override;

    virtual TInterruptDescriptor GetInterruptDescriptor(
        TRange<TUnversionedRow> unreadRows) const override;

    virtual const TDataSliceDescriptor& GetCurrentReaderDescriptor() const override;

private:
    const TNameTablePtr NameTable_;
    const TKeyColumns KeyColumns_;

    ISchemalessChunkReaderPtr CurrentReader_;
    std::atomic<i64> RowIndex_ = {0};
    std::atomic<i64> RowCount_ = {-1};

    TInterruptDescriptor FinishedInterruptDescriptor_;

    std::atomic<bool> Finished_ = {false};

    using TBase::ReadyEvent_;
    using TBase::CurrentSession_;

    virtual void OnReaderSwitched() override;
};

////////////////////////////////////////////////////////////////////////////////

template <class TBase>
TSchemalessMultiChunkReader<TBase>::TSchemalessMultiChunkReader(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    NNative::IClientPtr client,
    const TNodeDescriptor& localDescriptor,
    std::optional<TNodeId> localNodeId,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const TDataSourceDirectoryPtr& dataSourceDirectory,
    const std::vector<TDataSliceDescriptor>& dataSliceDescriptors,
    TNameTablePtr nameTable,
    const TClientBlockReadOptions& blockReadOptions,
    const TColumnFilter& columnFilter,
    const TKeyColumns& keyColumns,
    std::optional<int> partitionTag,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr bandwidthThrottler,
    IThroughputThrottlerPtr rpsThrottler,
    NChunkClient::IMultiReaderMemoryManagerPtr multiReaderMemoryManager)
    : TBase(
        config,
        options,
        CreateReaderFactories(
            config,
            options,
            client,
            localDescriptor,
            localNodeId,
            blockCache,
            nodeDirectory,
            dataSourceDirectory,
            dataSliceDescriptors,
            nameTable,
            blockReadOptions,
            columnFilter,
            keyColumns,
            partitionTag,
            trafficMeter,
            bandwidthThrottler,
            rpsThrottler,
            multiReaderMemoryManager),
        multiReaderMemoryManager)
    , NameTable_(nameTable)
    , KeyColumns_(keyColumns)
    , RowCount_(GetCumulativeRowCount(dataSliceDescriptors))
{
    if (dataSliceDescriptors.empty()) {
        Finished_ = true;
    }
}

template <class TBase>
TSchemalessMultiChunkReader<TBase>::~TSchemalessMultiChunkReader()
{
    const auto& Logger = TMultiReaderBase::Logger;
    // Log all statistics. NB:
    YT_LOG_DEBUG("Reader data statistics (DataStatistics: %v)", TMultiReaderBase::GetDataStatistics());
    YT_LOG_DEBUG("Reader decompression codec statistics (CodecStatistics: %v)", TMultiReaderBase::GetDecompressionStatistics());
}

template <class TBase>
bool TSchemalessMultiChunkReader<TBase>::Read(std::vector<TUnversionedRow>* rows)
{
    rows->clear();

    if (!ReadyEvent_.IsSet() || !ReadyEvent_.Get().IsOK()) {
        return true;
    }

    if (Finished_) {
        RowCount_ = RowIndex_.load();



        return false;
    }

    bool readerFinished = !CurrentReader_->Read(rows);
    if (!rows->empty()) {
        RowIndex_ += rows->size();
        return true;
    }

    if (readerFinished) {
        // This must fill read descriptors with values from finished readers.
        auto interruptDescriptor = CurrentReader_->GetInterruptDescriptor({});
        FinishedInterruptDescriptor_.MergeFrom(std::move(interruptDescriptor));
    }

    if (!TBase::OnEmptyRead(readerFinished)) {
        Finished_ = true;
    }

    return true;
}

template <class TBase>
void TSchemalessMultiChunkReader<TBase>::OnReaderSwitched()
{
    CurrentReader_ = dynamic_cast<ISchemalessChunkReader*>(CurrentSession_.Reader.Get());
    YT_VERIFY(CurrentReader_);
}

template <class TBase>
i64 TSchemalessMultiChunkReader<TBase>::GetTotalRowCount() const
{
    return RowCount_;
}

template <class TBase>
i64 TSchemalessMultiChunkReader<TBase>::GetSessionRowIndex() const
{
    return RowIndex_;
}

template <class TBase>
i64 TSchemalessMultiChunkReader<TBase>::GetTableRowIndex() const
{
    return CurrentReader_ ? CurrentReader_->GetTableRowIndex() : 0;
}

template <class TBase>
const TNameTablePtr& TSchemalessMultiChunkReader<TBase>::GetNameTable() const
{
    return NameTable_;
}

template <class TBase>
const TKeyColumns& TSchemalessMultiChunkReader<TBase>::GetKeyColumns() const
{
    return KeyColumns_;
}

template <class TBase>
void TSchemalessMultiChunkReader<TBase>::Interrupt()
{
    if (!Finished_.exchange(true)) {
        TBase::OnInterrupt();
    }
}

template <class TBase>
void TSchemalessMultiChunkReader<TBase>::SkipCurrentReader()
{
    if (!ReadyEvent_.IsSet() || !ReadyEvent_.Get().IsOK()) {
        return;
    }

     // Pretend that current reader already finished.
    if (!TBase::OnEmptyRead(/* readerFinished */ true)) {
        Finished_ = true;
    }
}

template <class TBase>
TInterruptDescriptor TSchemalessMultiChunkReader<TBase>::GetInterruptDescriptor(
    TRange<TUnversionedRow> unreadRows) const
{
    static TRange<TUnversionedRow> emptyRange;
    auto state = TBase::GetUnreadState();

    auto result = FinishedInterruptDescriptor_;
    if (state.CurrentReader) {
        auto chunkReader = dynamic_cast<ISchemalessChunkReader*>(state.CurrentReader.Get());
        YT_VERIFY(chunkReader);
        result.MergeFrom(chunkReader->GetInterruptDescriptor(unreadRows));
    }
    for (const auto& activeReader : state.ActiveReaders) {
        auto chunkReader = dynamic_cast<ISchemalessChunkReader*>(activeReader.Get());
        YT_VERIFY(chunkReader);
        auto interruptDescriptor = chunkReader->GetInterruptDescriptor(emptyRange);
        result.MergeFrom(std::move(interruptDescriptor));
    }
    for (const auto& factory : state.ReaderFactories) {
        result.UnreadDataSliceDescriptors.emplace_back(factory->GetDataSliceDescriptor());
    }
    return result;
}

template <class TBase>
const TDataSliceDescriptor& TSchemalessMultiChunkReader<TBase>::GetCurrentReaderDescriptor() const
{
    return CurrentReader_->GetCurrentReaderDescriptor();
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessSequentialMultiReader(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    NNative::IClientPtr client,
    const TNodeDescriptor& localDescriptor,
    std::optional<TNodeId> localNodeId,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const TDataSourceDirectoryPtr& dataSourceDirectory,
    const std::vector<TDataSliceDescriptor>& dataSliceDescriptors,
    TNameTablePtr nameTable,
    const TClientBlockReadOptions& blockReadOptions,
    const TColumnFilter& columnFilter,
    const TKeyColumns& keyColumns,
    std::optional<int> partitionTag,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr bandwidthThrottler,
    IThroughputThrottlerPtr rpsThrottler,
    NChunkClient::IMultiReaderMemoryManagerPtr multiReaderMemoryManager)
{
    if (!multiReaderMemoryManager) {
        multiReaderMemoryManager = CreateParallelReaderMemoryManager(
            TParallelReaderMemoryManagerOptions{
                .TotalReservedMemorySize = config->MaxBufferSize,
                .MaxInitialReaderReservedMemory = config->WindowSize
            },
            NChunkClient::TDispatcher::Get()->GetReaderMemoryManagerInvoker());
    }

    auto reader = New<TSchemalessMultiChunkReader<TSequentialMultiReaderBase>>(
        config,
        options,
        client,
        localDescriptor,
        localNodeId,
        blockCache,
        nodeDirectory,
        dataSourceDirectory,
        dataSliceDescriptors,
        nameTable,
        blockReadOptions,
        columnFilter,
        keyColumns,
        partitionTag,
        trafficMeter,
        std::move(bandwidthThrottler),
        std::move(rpsThrottler),
        std::move(multiReaderMemoryManager));

    reader->Open();
    return reader;
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessParallelMultiReader(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    NNative::IClientPtr client,
    const TNodeDescriptor& localDescriptor,
    std::optional<TNodeId> localNodeId,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const TDataSourceDirectoryPtr& dataSourceDirectory,
    const std::vector<TDataSliceDescriptor>& dataSliceDescriptors,
    TNameTablePtr nameTable,
    const TClientBlockReadOptions& blockReadOptions,
    const TColumnFilter& columnFilter,
    const TKeyColumns& keyColumns,
    std::optional<int> partitionTag,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr bandwidthThrottler,
    IThroughputThrottlerPtr rpsThrottler,
    NChunkClient::IMultiReaderMemoryManagerPtr multiReaderMemoryManager)
{
    if (!multiReaderMemoryManager) {
        multiReaderMemoryManager = CreateParallelReaderMemoryManager(
            TParallelReaderMemoryManagerOptions{
                .TotalReservedMemorySize = config->MaxBufferSize,
                .MaxInitialReaderReservedMemory = config->WindowSize
            },
            NChunkClient::TDispatcher::Get()->GetReaderMemoryManagerInvoker());
    }

    auto reader = New<TSchemalessMultiChunkReader<TParallelMultiReaderBase>>(
        config,
        options,
        client,
        localDescriptor,
        localNodeId,
        blockCache,
        nodeDirectory,
        dataSourceDirectory,
        dataSliceDescriptors,
        nameTable,
        blockReadOptions,
        columnFilter,
        keyColumns,
        partitionTag,
        trafficMeter,
        std::move(bandwidthThrottler),
        std::move(rpsThrottler),
        std::move(multiReaderMemoryManager));

    reader->Open();
    return reader;
}

////////////////////////////////////////////////////////////////////////////////

class TSchemalessMergingMultiChunkReader
    : public ISchemalessMultiChunkReader
{
public:
    static ISchemalessMultiChunkReaderPtr Create(
        TTableReaderConfigPtr config,
        TTableReaderOptionsPtr options,
        NNative::IClientPtr client,
        const TNodeDescriptor& localDescriptor,
        std::optional<TNodeId> localNodeId,
        IBlockCachePtr blockCache,
        TNodeDirectoryPtr nodeDirectory,
        const TDataSourceDirectoryPtr& dataSourceDirectory,
        const TDataSliceDescriptor& dataSliceDescriptor,
        TNameTablePtr nameTable,
        const TClientBlockReadOptions& blockReadOptions,
        TColumnFilter columnFilter,
        TTrafficMeterPtr trafficMeter,
        IThroughputThrottlerPtr bandwidthThrottler,
        IThroughputThrottlerPtr rpsThrottler);

    virtual TFuture<void> GetReadyEvent() override
    {
        auto promise = NewPromise<void>();
        promise.TrySetFrom(ErrorPromise_.ToFuture());
        promise.TrySetFrom(UnderlyingReader_->GetReadyEvent());
        return promise.ToFuture();
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        return UnderlyingReader_->GetDataStatistics();
    }

    virtual TCodecStatistics GetDecompressionStatistics() const override
    {
        return UnderlyingReader_->GetDecompressionStatistics();
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        // ToDo(psushin): every reader must implement this method eventually.
        return std::vector<TChunkId>();
    }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        rows->clear();
        SchemafulRows_.clear();
        MemoryPool_.Clear();

        if (Interrupting_) {
            return false;
        }

        if (ErrorPromise_.IsSet()) {
            return true;
        }

        SchemafulRows_.reserve(rows->capacity());
        HasMore_ = UnderlyingReader_->Read(&SchemafulRows_);
        if (SchemafulRows_.empty()) {
            return HasMore_;
        }

        LastKey_ = GetKeyPrefix(SchemafulRows_.back(), Schema_.GetKeyColumnCount());

        YT_VERIFY(HasMore_);

        try {
            for (int index = 0; index < SchemafulRows_.size(); ++index) {
                auto schemalessRow = TMutableUnversionedRow::Allocate(&MemoryPool_, SchemaColumnCount_ + SystemColumnCount_);
                auto schemafulRow = SchemafulRows_[index];

                int schemalessValueIndex = 0;
                for (int valueIndex = 0; valueIndex < schemafulRow.GetCount(); ++valueIndex) {
                    const auto& value = schemafulRow[valueIndex];
                    auto id = IdMapping_[value.Id];

                    if (id >= 0) {
                        ValidateDataValue(value);
                        schemalessRow[schemalessValueIndex] = value;
                        schemalessRow[schemalessValueIndex].Id = id;
                        ++schemalessValueIndex;
                    }
                }

                schemalessRow.SetCount(SchemaColumnCount_);

                if (Options_->EnableRangeIndex) {
                    *schemalessRow.End() = MakeUnversionedInt64Value(RangeIndex_, RangeIndexId_);
                    schemalessRow.SetCount(schemalessRow.GetCount() + 1);
                }
                if (Options_->EnableTableIndex) {
                    *schemalessRow.End() = MakeUnversionedInt64Value(TableIndex_, TableIndexId_);
                    schemalessRow.SetCount(schemalessRow.GetCount() + 1);
                }

                rows->push_back(schemalessRow);
            }

            RowIndex_ += rows->size();
        } catch (const std::exception& ex) {
            SchemafulRows_.clear();
            rows->clear();

            ErrorPromise_.Set(ex);
        }

        return true;
    }

    virtual TInterruptDescriptor GetInterruptDescriptor(
        TRange<TUnversionedRow> unreadRows) const override
    {
        std::vector<TDataSliceDescriptor> unreadDescriptors;
        std::vector<TDataSliceDescriptor> readDescriptors;

        TOwningKey firstUnreadKey;
        if (!unreadRows.Empty()) {
            auto firstSchemafulUnreadRow = SchemafulRows_[SchemafulRows_.size() - unreadRows.Size()];
            firstUnreadKey = GetKeyPrefix(firstSchemafulUnreadRow, Schema_.GetKeyColumnCount());
        } else if (LastKey_) {
            firstUnreadKey = GetKeySuccessor(LastKey_);
        }

        if (!unreadRows.Empty() || HasMore_) {
            unreadDescriptors.emplace_back(DataSliceDescriptor_);
        }
        if (LastKey_) {
            readDescriptors.emplace_back(DataSliceDescriptor_);
        }

        YT_VERIFY(firstUnreadKey || readDescriptors.empty());

        if (firstUnreadKey) {
            // TODO: Estimate row count and data size.
            for (auto& descriptor : unreadDescriptors) {
                for (auto& chunk : descriptor.ChunkSpecs) {
                    ToProto(chunk.mutable_lower_limit()->mutable_key(), firstUnreadKey);
                }
            }
            for (auto& descriptor : readDescriptors) {
                for (auto& chunk : descriptor.ChunkSpecs) {
                    ToProto(chunk.mutable_upper_limit()->mutable_key(), firstUnreadKey);
                }
            }
        }

        return {std::move(unreadDescriptors), std::move(readDescriptors)};
    }

    virtual void Interrupt() override
    {
        Interrupting_ = true;
        ErrorPromise_.TrySet(TError());
    }

    virtual void SkipCurrentReader() override
    {
        // Merging reader doesn't support sub-reader skipping.
    }

    virtual bool IsFetchingCompleted() const override
    {
        return false;
    }

    virtual i64 GetSessionRowIndex() const override
    {
        return RowIndex_;
    }

    virtual i64 GetTotalRowCount() const override
    {
        return RowCount_;
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
        // Not supported for versioned data.
        return -1;
    }

    virtual const TDataSliceDescriptor& GetCurrentReaderDescriptor() const override
    {
        YT_ABORT();
    }

private:
    const TTableReaderOptionsPtr Options_;
    const ISchemafulReaderPtr UnderlyingReader_;
    const TDataSliceDescriptor DataSliceDescriptor_;
    const TTableSchema Schema_;
    const std::vector<int> IdMapping_;
    const TNameTablePtr NameTable_;
    const i64 RowCount_;

    // We keep rows received from underlying schemaful reader
    // to define proper lower limit during interrupt.
    std::vector<TUnversionedRow> SchemafulRows_;

    std::atomic<bool> Interrupting_ = {false};

    // We must assume that there is more data if we read nothing to the moment.
    std::atomic<bool> HasMore_ = {true};
    TOwningKey LastKey_;

    i64 RowIndex_ = 0;

    TChunkedMemoryPool MemoryPool_;

    int TableIndexId_ = -1;
    int RangeIndexId_ = -1;
    int TableIndex_ = -1;
    int RangeIndex_ = -1;
    int SystemColumnCount_ = 0;

    // Number of "active" columns in id mapping.
    int SchemaColumnCount_ = 0;

    // Columns that output row stream is sorted by. May not coincide with schema key columns,
    // because some column may be filtered out by the column filter.
    TKeyColumns KeyColumns_;

    TPromise<void> ErrorPromise_ = NewPromise<void>();

    IMultiReaderMemoryManagerPtr ParallelReaderMemoryManager_;

    TSchemalessMergingMultiChunkReader(
        TTableReaderOptionsPtr options,
        ISchemafulReaderPtr underlyingReader,
        const TDataSliceDescriptor& dataSliceDescriptor,
        TTableSchema schema,
        std::vector<int> idMapping,
        TNameTablePtr nameTable,
        i64 rowCount,
        IMultiReaderMemoryManagerPtr parallelReaderMemoryManager)
        : Options_(options)
        , UnderlyingReader_(std::move(underlyingReader))
        , DataSliceDescriptor_(dataSliceDescriptor)
        , Schema_(std::move(schema))
        , IdMapping_(idMapping)
        , NameTable_(nameTable)
        , RowCount_(rowCount)
        , ParallelReaderMemoryManager_(std::move(parallelReaderMemoryManager))
    {
        if (!DataSliceDescriptor_.ChunkSpecs.empty()) {
            TableIndex_ = DataSliceDescriptor_.ChunkSpecs.front().table_index();
            RangeIndex_ = DataSliceDescriptor_.ChunkSpecs.front().range_index();
        }

        if (Options_->EnableRangeIndex) {
            ++SystemColumnCount_;
            RangeIndexId_ = NameTable_->GetIdOrRegisterName(RangeIndexColumnName);
        }

        if (Options_->EnableTableIndex) {
            ++SystemColumnCount_;
            TableIndexId_ = NameTable_->GetIdOrRegisterName(TableIndexColumnName);
        }

        for(auto id : IdMapping_) {
            if (id >= 0) {
                ++SchemaColumnCount_;
            }
        }

        for (int index = 0; index < Schema_.GetKeyColumnCount(); ++index) {
            if (IdMapping_[index] < 0) {
                break;
            }

            KeyColumns_.push_back(Schema_.Columns()[index].Name());
        }
    }

    DECLARE_NEW_FRIEND();
};

////////////////////////////////////////////////////////////////////////////////

std::pair<TTableSchema, TColumnFilter> CreateVersionedReadParameters(
    const TTableSchema& schema,
    const TColumnFilter& columnFilter)
{
    if (columnFilter.IsUniversal()) {
        return std::make_pair(schema, columnFilter);
    }

    std::vector<NTableClient::TColumnSchema> columns;
    for (int index = 0; index < schema.GetKeyColumnCount(); ++index) {
        columns.push_back(schema.Columns()[index]);
    }

    TColumnFilter::TIndexes columnFilterIndexes;
    for (int index : columnFilter.GetIndexes()) {
        if (index >= schema.GetKeyColumnCount()) {
            columnFilterIndexes.push_back(columns.size());
            columns.push_back(schema.Columns()[index]);
        } else {
            columnFilterIndexes.push_back(index);
        }
    }

    return std::make_pair(TTableSchema(std::move(columns)), TColumnFilter(std::move(columnFilterIndexes)));
}

ISchemalessMultiChunkReaderPtr TSchemalessMergingMultiChunkReader::Create(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    NNative::IClientPtr client,
    const TNodeDescriptor& localDescriptor,
    std::optional<TNodeId> localNodeId,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const TDataSourceDirectoryPtr& dataSourceDirectory,
    const TDataSliceDescriptor& dataSliceDescriptor,
    TNameTablePtr nameTable,
    const TClientBlockReadOptions& blockReadOptions,
    TColumnFilter columnFilter,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr bandwidthThrottler,
    IThroughputThrottlerPtr rpsThrottler)
{
    auto Logger = TableClientLogger;
    if (blockReadOptions.ReadSessionId) {
        Logger.AddTag("ReadSessionId: %v", blockReadOptions.ReadSessionId);
    }

    const auto& dataSource = dataSourceDirectory->DataSources()[dataSliceDescriptor.GetDataSourceIndex()];
    const auto& chunkSpecs = dataSliceDescriptor.ChunkSpecs;

    YT_VERIFY(dataSource.Schema());
    const auto& tableSchema = *dataSource.Schema();
    auto timestamp = dataSource.GetTimestamp();
    auto retentionTimestamp = dataSource.GetRetentionTimestamp();
    const auto& renameDescriptors = dataSource.ColumnRenameDescriptors();

    if (!columnFilter.IsUniversal()) {
        TColumnFilter::TIndexes transformedIndexes;
        for (auto index : columnFilter.GetIndexes()) {
            if (auto* column = tableSchema.FindColumn(nameTable->GetName(index))) {
                auto columnIndex = tableSchema.GetColumnIndex(*column);
                if (std::find(transformedIndexes.begin(), transformedIndexes.end(), columnIndex) ==
                    transformedIndexes.end())
                {
                    transformedIndexes.push_back(columnIndex);
                }
            }
        }
        columnFilter = TColumnFilter(std::move(transformedIndexes));
    }

    ValidateColumnFilter(columnFilter, tableSchema.GetColumnCount());

    TTableSchema versionedReadSchema;
    TColumnFilter versionedColumnFilter;
    std::tie(versionedReadSchema, versionedColumnFilter) = CreateVersionedReadParameters(
        tableSchema,
        columnFilter);

    std::vector<int> idMapping(versionedReadSchema.GetColumnCount());

    try {
        for (int columnIndex = 0; columnIndex < versionedReadSchema.Columns().size(); ++columnIndex) {
            const auto& column = versionedReadSchema.Columns()[columnIndex];
            if (versionedColumnFilter.ContainsIndex(columnIndex)) {
                idMapping[columnIndex] = nameTable->GetIdOrRegisterName(column.Name());
            } else {
                // We should skip this column in schemaless reading.
                idMapping[columnIndex] = -1;
            }
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to update name table for schemaless merging multi chunk reader")
            << ex;
    }

    std::vector<TOwningKey> boundaries;
    boundaries.reserve(chunkSpecs.size());

    auto extractMinKey = [] (const TChunkSpec& chunkSpec) {
        auto type = TypeFromId(FromProto<TChunkId>(chunkSpec.chunk_id()));

        if (chunkSpec.has_lower_limit()) {
            auto limit = FromProto<TReadLimit>(chunkSpec.lower_limit());
            if (limit.HasKey()) {
                return limit.GetKey();
            }
        } else if (IsChunkTabletStoreType(type)) {
            YT_VERIFY(chunkSpec.has_chunk_meta());
            if (FindProtoExtension<NProto::TBoundaryKeysExt>(chunkSpec.chunk_meta().extensions())) {
                auto boundaryKeysExt = GetProtoExtension<NProto::TBoundaryKeysExt>(chunkSpec.chunk_meta().extensions());
                return FromProto<TOwningKey>(boundaryKeysExt.min());
            }
        }
        return TOwningKey();
    };

    for (const auto& chunkSpec : chunkSpecs) {
        TOwningKey minKey = extractMinKey(chunkSpec);
        boundaries.push_back(minKey);
    }

    YT_LOG_DEBUG("Create overlapping range reader (Boundaries: %v, Stores: %v, ColumnFilter: %v)",
        boundaries,
        MakeFormattableView(chunkSpecs, [] (TStringBuilderBase* builder, const TChunkSpec& chunkSpec) {
            FormatValue(builder, FromProto<TChunkId>(chunkSpec.chunk_id()), TStringBuf());
        }),
        columnFilter);

    auto performanceCounters = New<TChunkReaderPerformanceCounters>();

    auto parallelReaderMemoryManager = CreateParallelReaderMemoryManager(
        TParallelReaderMemoryManagerOptions{
            .TotalReservedMemorySize = config->MaxBufferSize,
            .MaxInitialReaderReservedMemory = config->WindowSize
        },
        NChunkClient::TDispatcher::Get()->GetReaderMemoryManagerInvoker());

    auto createVersionedChunkReader = [
        config,
        options,
        client,
        localDescriptor,
        localNodeId,
        blockCache,
        nodeDirectory,
        blockReadOptions,
        chunkSpecs,
        tableSchema,
        versionedReadSchema,
        performanceCounters,
        timestamp,
        trafficMeter,
        bandwidthThrottler,
        rpsThrottler,
        renameDescriptors,
        parallelReaderMemoryManager,
        Logger
    ] (TChunkSpec chunkSpec) -> IVersionedReaderPtr {
        auto chunkId = NYT::FromProto<TChunkId>(chunkSpec.chunk_id());
        auto replicas = NYT::FromProto<TChunkReplicaList>(chunkSpec.replicas());

        TReadLimit lowerLimit;
        TReadLimit upperLimit;

        if (chunkSpec.has_lower_limit()) {
            lowerLimit = NYT::FromProto<TReadLimit>(chunkSpec.lower_limit());
        } else {
            lowerLimit.SetKey(MinKey());
        }

        if (chunkSpec.has_upper_limit()) {
            upperLimit = NYT::FromProto<TReadLimit>(chunkSpec.upper_limit());
        } else {
            upperLimit.SetKey(MaxKey());
        }

        if (lowerLimit.HasRowIndex() || upperLimit.HasRowIndex()) {
            THROW_ERROR_EXCEPTION("Row index limit is not supported");
        }

        YT_LOG_DEBUG("Create versioned chunk reader (ChunkId: %v, Range: <%v : %v>)",
            chunkId,
            lowerLimit,
            upperLimit);

        auto remoteReader = CreateRemoteReader(
            chunkSpec,
            config,
            options,
            client,
            nodeDirectory,
            localDescriptor,
            localNodeId,
            blockCache,
            trafficMeter,
            bandwidthThrottler,
            rpsThrottler);

        auto asyncChunkMeta = TCachedVersionedChunkMeta::Load(
            remoteReader,
            blockReadOptions,
            versionedReadSchema,
            renameDescriptors,
            nullptr /* memoryTracker */);
        auto chunkMeta = WaitFor(asyncChunkMeta)
            .ValueOrThrow();
        auto chunkState = New<TChunkState>(
            blockCache,
            chunkSpec,
            nullptr,
            chunkSpec.has_override_timestamp() ? chunkSpec.override_timestamp() : NullTimestamp,
            nullptr,
            performanceCounters,
            nullptr);
        auto chunkReaderMemoryManager =
            parallelReaderMemoryManager->CreateChunkReaderMemoryManager(chunkMeta->Misc().uncompressed_data_size());

        return CreateVersionedChunkReader(
            config,
            std::move(remoteReader),
            std::move(chunkState),
            std::move(chunkMeta),
            blockReadOptions,
            lowerLimit.GetKey(),
            upperLimit.GetKey(),
            TColumnFilter(),
            timestamp,
            false,
            chunkReaderMemoryManager);
    };

    auto createVersionedReader = [
        config,
        options,
        client,
        blockCache,
        nodeDirectory,
        blockReadOptions,
        chunkSpecs,
        tableSchema,
        columnFilter,
        performanceCounters,
        timestamp,
        trafficMeter,
        bandwidthThrottler,
        rpsThrottler,
        renameDescriptors,
        Logger,
        createVersionedChunkReader
    ] (int index) -> IVersionedReaderPtr {
        const auto& chunkSpec = chunkSpecs[index];
        auto chunkId = NYT::FromProto<TChunkId>(chunkSpec.chunk_id());
        auto type = TypeFromId(chunkId);

        if (type == EObjectType::SortedDynamicTabletStore) {
            return CreateRetryingRemoteDynamicStoreReader(
                chunkSpec,
                tableSchema,
                config->DynamicStoreReader,
                client,
                nodeDirectory,
                trafficMeter,
                bandwidthThrottler,
                rpsThrottler,
                blockReadOptions,
                columnFilter,
                timestamp,
                BIND(createVersionedChunkReader));
        } else {
            return createVersionedChunkReader(chunkSpec);
        }
    };

    struct TSchemalessMergingMultiChunkReaderBufferTag
    { };

    auto rowMerger = std::make_unique<TSchemafulRowMerger>(
        New<TRowBuffer>(TSchemalessMergingMultiChunkReaderBufferTag()),
        versionedReadSchema.Columns().size(),
        versionedReadSchema.GetKeyColumnCount(),
        TColumnFilter(),
        client->GetNativeConnection()->GetColumnEvaluatorCache()->Find(versionedReadSchema),
        retentionTimestamp);

    auto schemafulReader = CreateSchemafulOverlappingRangeReader(
        std::move(boundaries),
        std::move(rowMerger),
        createVersionedReader,
        [] (
            const TUnversionedValue* lhsBegin,
            const TUnversionedValue* lhsEnd,
            const TUnversionedValue* rhsBegin,
            const TUnversionedValue* rhsEnd)
        {
            return CompareRows(lhsBegin, lhsEnd, rhsBegin, rhsEnd);
        });

    i64 rowCount = NChunkClient::GetCumulativeRowCount(chunkSpecs);

    return New<TSchemalessMergingMultiChunkReader>(
        std::move(options),
        std::move(schemafulReader),
        dataSliceDescriptor,
        versionedReadSchema,
        std::move(idMapping),
        std::move(nameTable),
        rowCount,
        std::move(parallelReaderMemoryManager));
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessMergingMultiChunkReader(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    NNative::IClientPtr client,
    const TNodeDescriptor& localDescriptor,
    std::optional<TNodeId> localNodeId,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const TDataSourceDirectoryPtr& dataSourceDirectory,
    const TDataSliceDescriptor& dataSliceDescriptor,
    TNameTablePtr nameTable,
    const TClientBlockReadOptions& blockReadOptions,
    const TColumnFilter& columnFilter,
    NChunkClient::TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr bandwidthThrottler,
    IThroughputThrottlerPtr rpsThrottler)
{
    return TSchemalessMergingMultiChunkReader::Create(
        config,
        options,
        client,
        localDescriptor,
        localNodeId,
        blockCache,
        nodeDirectory,
        dataSourceDirectory,
        dataSliceDescriptor,
        nameTable,
        blockReadOptions,
        columnFilter,
        trafficMeter,
        std::move(bandwidthThrottler),
        std::move(rpsThrottler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
