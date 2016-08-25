#include "cached_versioned_chunk_meta.h"
#include "chunk_reader_base.h"
#include "columnar_chunk_reader_base.h"
#include "config.h"
#include "helpers.h"
#include "legacy_table_chunk_reader.h"
#include "name_table.h"
#include "overlapping_reader.h"
#include "private.h"
#include "row_buffer.h"
#include "row_merger.h"
#include "row_sampler.h"
#include "schema.h"
#include "schemaful_reader.h"
#include "schemaless_block_reader.h"
#include "schemaless_chunk_reader.h"
#include "schemaless_reader_adapter.h"
#include "versioned_chunk_reader.h"

#include <yt/ytlib/api/native_connection.h>
#include <yt/ytlib/api/native_client.h>

#include <yt/ytlib/table_chunk_format/public.h>
#include <yt/ytlib/table_chunk_format/column_reader.h>

#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/multi_reader_base.h>
#include <yt/ytlib/chunk_client/reader_factory.h>
#include <yt/ytlib/chunk_client/replication_reader.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/query_client/column_evaluator.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NTableChunkFormat;
using namespace NTableChunkFormat::NProto;
using namespace NProto;
using namespace NYPath;
using namespace NYTree;
using namespace NRpc;
using namespace NApi;

using NChunkClient::TReadLimit;
using NChunkClient::TReadRange;
using NChunkClient::TChannel;
using NChunkClient::NProto::TMiscExt;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TSchemalessChunkReaderBase
    : public virtual ISchemalessChunkReader
{
public:
    TSchemalessChunkReaderBase(
        const TChunkSpec& chunkSpec,
        TChunkReaderConfigPtr config,
        TChunkReaderOptionsPtr options,
        const TChunkId& chunkId,
        TNameTablePtr nameTable,
        const TColumnFilter& columnFilter,
        const TKeyColumns& keyColumns)
        : ChunkSpec_(chunkSpec)
        , Config_(config)
        , Options_(options)
        , NameTable_(nameTable)
        , ColumnFilter_(columnFilter)
        , KeyColumns_(keyColumns)
        , SystemColumnCount_(GetSystemColumnCount(options))
    {
        if (Config_->SamplingRate) {
            RowSampler_ = CreateChunkRowSampler(
                chunkId,
                Config_->SamplingRate.Get(),
                Config_->SamplingSeed.Get(std::random_device()()));
        }
    }

    virtual TNameTablePtr GetNameTable() const override
    {
        return NameTable_;
    }

    virtual TKeyColumns GetKeyColumns() const override
    {
        return KeyColumns_;
    }

    virtual i64 GetTableRowIndex() const override
    {
        return ChunkSpec_.table_row_index() + RowIndex_;
    }

protected:
    const TChunkSpec ChunkSpec_;

    TChunkReaderConfigPtr Config_;
    TChunkReaderOptionsPtr Options_;
    const TNameTablePtr NameTable_;

    const TColumnFilter ColumnFilter_;
    TKeyColumns KeyColumns_;

    i64 RowIndex_ = 0;
    i64 RowCount_ = 0;

    std::unique_ptr<IRowSampler> RowSampler_;
    const int SystemColumnCount_;

    int RowIndexId_ = -1;
    int RangeIndexId_ = -1;
    int TableIndexId_ = -1;

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
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to add system columns to name table for schemaless chunk reader")
                << ex;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class THorizontalSchemalessChunkReaderBase
    : public TChunkReaderBase
    , public TSchemalessChunkReaderBase
{
public:
    THorizontalSchemalessChunkReaderBase(
        const TChunkSpec& chunkSpec,
        TChunkReaderConfigPtr config,
        TChunkReaderOptionsPtr options,
        IChunkReaderPtr underlyingReader,
        TNameTablePtr nameTable,
        IBlockCachePtr blockCache,
        const TKeyColumns& keyColumns,
        const TColumnFilter& columnFilter,
        TNullable<int> partitionTag);

    virtual TDataStatistics GetDataStatistics() const override;

protected:
    using TSchemalessChunkReaderBase::Config_;

    TNameTablePtr ChunkNameTable_ = New<TNameTable>();

    int ChunkKeyColumnCount_ = 0;

    TNullable<int> PartitionTag_;

    int CurrentBlockIndex_ = 0;

    // Maps chunk name table ids into client id.
    // For filtered out columns maps id to -1.
    std::vector<TColumnIdMapping> IdMapping_;

    std::unique_ptr<THorizontalSchemalessBlockReader> BlockReader_;

    TChunkMeta ChunkMeta_;
    NProto::TBlockMetaExt BlockMetaExt_;

    std::vector<int> BlockIndexes_;

    virtual void DoInitializeBlockSequence() = 0;

    void DownloadChunkMeta(std::vector<int> extensionTags, TNullable<int> partitionTag = Null);

    TFuture<void> InitializeBlockSequence();
};

////////////////////////////////////////////////////////////////////////////////

THorizontalSchemalessChunkReaderBase::THorizontalSchemalessChunkReaderBase(
    const TChunkSpec& chunkSpec,
    TChunkReaderConfigPtr config,
    TChunkReaderOptionsPtr options,
    IChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    IBlockCachePtr blockCache,
    const TKeyColumns& keyColumns,
    const TColumnFilter& columnFilter,
    TNullable<int> partitionTag)
    : TChunkReaderBase(
        config,
        underlyingReader,
        blockCache)
    , TSchemalessChunkReaderBase(
        chunkSpec,
        config,
        options,
        underlyingReader->GetChunkId(),
        nameTable,
        columnFilter,
        keyColumns)
    , PartitionTag_(std::move(partitionTag))
{ }

TFuture<void> THorizontalSchemalessChunkReaderBase::InitializeBlockSequence()
{
    YCHECK(ChunkSpec_.chunk_meta().version() == static_cast<int>(ETableChunkFormat::SchemalessHorizontal));
    YCHECK(BlockIndexes_.empty());

    InitializeSystemColumnIds();

    DoInitializeBlockSequence();

    LOG_DEBUG("Reading %v blocks", BlockIndexes_.size());

    std::vector<TBlockFetcher::TBlockInfo> blocks;
    for (int blockIndex : BlockIndexes_) {
        YCHECK(blockIndex < BlockMetaExt_.blocks_size());
        auto& blockMeta = BlockMetaExt_.blocks(blockIndex);
        TBlockFetcher::TBlockInfo blockInfo;
        blockInfo.Index = blockMeta.block_index();
        blockInfo.UncompressedDataSize = blockMeta.uncompressed_size();
        blockInfo.Priority = blocks.size();
        blocks.push_back(blockInfo);
    }

    return DoOpen(std::move(blocks), GetProtoExtension<TMiscExt>(ChunkMeta_.extensions()));
}

void THorizontalSchemalessChunkReaderBase::DownloadChunkMeta(std::vector<int> extensionTags, TNullable<int> partitionTag)
{
    extensionTags.push_back(TProtoExtensionTag<TMiscExt>::Value);
    extensionTags.push_back(TProtoExtensionTag<NProto::TBlockMetaExt>::Value);
    extensionTags.push_back(TProtoExtensionTag<NProto::TNameTableExt>::Value);
    auto asynChunkMeta = UnderlyingReader_->GetMeta(
        Config_->WorkloadDescriptor,
        partitionTag,
        extensionTags);
    ChunkMeta_ = WaitFor(asynChunkMeta)
        .ValueOrThrow();

    BlockMetaExt_ = GetProtoExtension<NProto::TBlockMetaExt>(ChunkMeta_.extensions());

    auto nameTableExt = GetProtoExtension<NProto::TNameTableExt>(ChunkMeta_.extensions());
    try {
        FromProto(&ChunkNameTable_, nameTableExt);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::CorruptedNameTable,
            "Failed to deserialize name table for schemaless chunk reader")
            << TErrorAttribute("chunk_id", UnderlyingReader_->GetChunkId())
            << ex;
    }

    IdMapping_.reserve(ChunkNameTable_->GetSize());

    if (ColumnFilter_.All) {
        try {
            for (int chunkNameId = 0; chunkNameId < ChunkNameTable_->GetSize(); ++chunkNameId) {
                auto name = ChunkNameTable_->GetName(chunkNameId);
                auto id = NameTable_->GetIdOrRegisterName(name);
                IdMapping_.push_back({chunkNameId, id});
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to update name table for schemaless chunk reader")
                << TErrorAttribute("chunk_id", UnderlyingReader_->GetChunkId())
                << ex;
        }
    } else {
        for (int chunkNameId = 0; chunkNameId < ChunkNameTable_->GetSize(); ++chunkNameId) {
            IdMapping_.push_back({chunkNameId, -1});
        }

        for (auto id : ColumnFilter_.Indexes) {
            auto name = NameTable_->GetName(id);
            auto chunkNameId = ChunkNameTable_->FindId(name);
            if (chunkNameId) {
                IdMapping_[chunkNameId.Get()] = {chunkNameId.Get(), id};
            }
        }
    }
}

TDataStatistics THorizontalSchemalessChunkReaderBase::GetDataStatistics() const
{
    auto dataStatistics = TChunkReaderBase::GetDataStatistics();
    dataStatistics.set_row_count(RowCount_);
    return dataStatistics;
}

////////////////////////////////////////////////////////////////////////////////

class THorizontalSchemalessRangeChunkReader
    : public THorizontalSchemalessChunkReaderBase
{
public:
    THorizontalSchemalessRangeChunkReader(
        const TChunkSpec& chunkSpec,
        TChunkReaderConfigPtr config,
        TChunkReaderOptionsPtr options,
        IChunkReaderPtr underlyingReader,
        TNameTablePtr nameTable,
        IBlockCachePtr blockCache,
        const TKeyColumns& keyColumns,
        const TColumnFilter& columnFilter,
        const TReadRange& readRange,
        TNullable<int> partitionTag);

    virtual bool Read(std::vector<TUnversionedRow>* rows) override;

private:
    TReadRange ReadRange_;

    virtual void DoInitializeBlockSequence() override;

    virtual void InitFirstBlock() override;
    virtual void InitNextBlock() override;

    void CreateBlockSequence(int beginIndex, int endIndex);

    void InitializeBlockSequencePartition();
    void InitializeBlockSequenceSorted();
    void InitializeBlockSequenceUnsorted();
};

DEFINE_REFCOUNTED_TYPE(THorizontalSchemalessRangeChunkReader)

////////////////////////////////////////////////////////////////////////////////

THorizontalSchemalessRangeChunkReader::THorizontalSchemalessRangeChunkReader(
    const TChunkSpec& chunkSpec,
    TChunkReaderConfigPtr config,
    TChunkReaderOptionsPtr options,
    IChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    IBlockCachePtr blockCache,
    const TKeyColumns& keyColumns,
    const TColumnFilter& columnFilter,
    const TReadRange& readRange,
    TNullable<int> partitionTag)
    : THorizontalSchemalessChunkReaderBase(
        chunkSpec,
        std::move(config),
        std::move(options),
        std::move(underlyingReader),
        std::move(nameTable),
        std::move(blockCache),
        keyColumns,
        columnFilter,
        std::move(partitionTag))
    , ReadRange_(readRange)
{
    LOG_DEBUG("Reading range %v", ReadRange_);

    // Ready event must be set only when all initialization is finished and 
    // RowIndex_ is set into proper value.
    // Must be called after the object is constructed and vtable initialized.
    ReadyEvent_ = BIND(&THorizontalSchemalessRangeChunkReader::InitializeBlockSequence, MakeStrong(this))
        .AsyncVia(NChunkClient::TDispatcher::Get()->GetReaderInvoker())
        .Run()
        .Apply(BIND([this, this_ = MakeStrong(this)] () {
            if (InitFirstBlockNeeded_) {
                InitFirstBlock();
                InitFirstBlockNeeded_ = false;
            }
        }));
}

void THorizontalSchemalessRangeChunkReader::DoInitializeBlockSequence()
{
    if (PartitionTag_) {
        InitializeBlockSequencePartition();
    } else {
        bool readSorted = ReadRange_.LowerLimit().HasKey() || ReadRange_.UpperLimit().HasKey() || !KeyColumns_.empty();
        if (readSorted) {
            InitializeBlockSequenceSorted();
        } else {
            InitializeBlockSequenceUnsorted();
        }
    }
}

void THorizontalSchemalessRangeChunkReader::InitializeBlockSequenceSorted()
{
    std::vector<int> extensionTags = {
        TProtoExtensionTag<NProto::TKeyColumnsExt>::Value,
    };

    DownloadChunkMeta(extensionTags);

    auto misc = GetProtoExtension<TMiscExt>(ChunkMeta_.extensions());
    if (!misc.sorted()) {
        THROW_ERROR_EXCEPTION("Requested a sorted read for an unsorted chunk");
    }

    auto keyColumnsExt = GetProtoExtension<NProto::TKeyColumnsExt>(ChunkMeta_.extensions());
    TKeyColumns chunkKeyColumns = NYT::FromProto<TKeyColumns>(keyColumnsExt);
    ChunkKeyColumnCount_ = chunkKeyColumns.size();

    ValidateKeyColumns(KeyColumns_, chunkKeyColumns, Options_->DynamicTable);

    if (KeyColumns_.empty()) {
        KeyColumns_ = chunkKeyColumns;
    }

    TNullable<int> keyColumnCount;
    if (Options_->DynamicTable) {
        keyColumnCount = KeyColumns_.size();
    }

    int beginIndex = std::max(
        ApplyLowerRowLimit(BlockMetaExt_, ReadRange_.LowerLimit()),
        ApplyLowerKeyLimit(BlockMetaExt_, ReadRange_.LowerLimit(), keyColumnCount));
    int endIndex = std::min(
        ApplyUpperRowLimit(BlockMetaExt_, ReadRange_.UpperLimit()),
        ApplyUpperKeyLimit(BlockMetaExt_, ReadRange_.UpperLimit(), keyColumnCount));

    CreateBlockSequence(beginIndex, endIndex);
}

void THorizontalSchemalessRangeChunkReader::InitializeBlockSequencePartition()
{
    YCHECK(ReadRange_.LowerLimit().IsTrivial());
    YCHECK(ReadRange_.UpperLimit().IsTrivial());

    DownloadChunkMeta(std::vector<int>(), PartitionTag_);
    CreateBlockSequence(0, BlockMetaExt_.blocks_size());
}

void THorizontalSchemalessRangeChunkReader::InitializeBlockSequenceUnsorted()
{
    DownloadChunkMeta(std::vector<int>());

    CreateBlockSequence(
        ApplyLowerRowLimit(BlockMetaExt_, ReadRange_.LowerLimit()),
        ApplyUpperRowLimit(BlockMetaExt_, ReadRange_.UpperLimit()));
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
    const auto& blockMeta = BlockMetaExt_.blocks(blockIndex);

    YCHECK(CurrentBlock_ && CurrentBlock_.IsSet());
    BlockReader_.reset(new THorizontalSchemalessBlockReader(
        CurrentBlock_.Get().ValueOrThrow(),
        blockMeta,
        IdMapping_,
        ChunkKeyColumnCount_,
        KeyColumns_.size(),
        SystemColumnCount_));

    RowIndex_ = blockMeta.chunk_row_count() - blockMeta.row_count();

    int keyColumnCount = std::max(ChunkKeyColumnCount_, static_cast<int>(KeyColumns_.size()));
    CheckBlockUpperLimits(BlockMetaExt_.blocks(blockIndex), ReadRange_.UpperLimit(), keyColumnCount);

    const auto& lowerLimit = ReadRange_.LowerLimit();

    if (lowerLimit.HasRowIndex() && RowIndex_ < lowerLimit.GetRowIndex()) {
        YCHECK(BlockReader_->SkipToRowIndex(lowerLimit.GetRowIndex() - RowIndex_));
        RowIndex_ = lowerLimit.GetRowIndex();
    }

    if (lowerLimit.HasKey()) {
        auto blockRowIndex = BlockReader_->GetRowIndex();
        YCHECK(BlockReader_->SkipToKey(lowerLimit.GetKey().Get()));
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
    YCHECK(rows->capacity() > 0);

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
    while (rows->size() < rows->capacity() && dataWeight < Config_->MaxDataSizePerRead) {
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

            rows->push_back(row);
            dataWeight += GetDataWeight(rows->back());
            ++RowCount_;
        }
        ++RowIndex_;

        if (!BlockReader_->NextRow()) {
            BlockEnded_ = true;
            return true;
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

class THorizontalSchemalessLookupChunkReader
    : public THorizontalSchemalessChunkReaderBase
{
public:
    THorizontalSchemalessLookupChunkReader(
        const TChunkSpec& chunkSpec,
        TChunkReaderConfigPtr config,
        TChunkReaderOptionsPtr options,
        NChunkClient::IChunkReaderPtr underlyingReader,
        TNameTablePtr nameTable,
        NChunkClient::IBlockCachePtr blockCache,
        const TKeyColumns& keyColumns,
        const TColumnFilter& columnFilter,
        const TSharedRange<TKey>& keys,
        TChunkReaderPerformanceCountersPtr performanceCounters,
        TNullable<int> partitionTag = Null);

    virtual bool Read(std::vector<TUnversionedRow>* rows) override;

private:
    const TSharedRange<TKey> Keys_;
    const TChunkReaderPerformanceCountersPtr PerformanceCounters_;
    std::vector<bool> KeyFilterTest_;

    virtual void DoInitializeBlockSequence() override;

    virtual void InitFirstBlock() override;
    virtual void InitNextBlock() override;

    bool DoRead(std::vector<TUnversionedRow>* rows);

};

DEFINE_REFCOUNTED_TYPE(THorizontalSchemalessLookupChunkReader)

////////////////////////////////////////////////////////////////////////////////

THorizontalSchemalessLookupChunkReader::THorizontalSchemalessLookupChunkReader(
    const TChunkSpec& chunkSpec,
    TChunkReaderConfigPtr config,
    TChunkReaderOptionsPtr options,
    NChunkClient::IChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    NChunkClient::IBlockCachePtr blockCache,
    const TKeyColumns& keyColumns,
    const TColumnFilter& columnFilter,
    const TSharedRange<TKey>& keys,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    TNullable<int> partitionTag)
    : THorizontalSchemalessChunkReaderBase(
        chunkSpec,
        std::move(config),
        std::move(options),
        std::move(underlyingReader),
        std::move(nameTable),
        std::move(blockCache),
        keyColumns,
        columnFilter,
        std::move(partitionTag))
    , Keys_(keys)
    , PerformanceCounters_(std::move(performanceCounters))
    , KeyFilterTest_(Keys_.Size(), true)
{
    // Ready event must be set only when all initialization is finished and 
    // RowIndex_ is set into proper value.
    // Must be called after the object is constructed and vtable initialized.
    ReadyEvent_ = BIND(&THorizontalSchemalessLookupChunkReader::InitializeBlockSequence, MakeStrong(this))
        .AsyncVia(NChunkClient::TDispatcher::Get()->GetReaderInvoker())
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
    std::vector<int> extensionTags = {
        TProtoExtensionTag<TKeyColumnsExt>::Value,
    };

    DownloadChunkMeta(extensionTags, PartitionTag_);

    auto misc = GetProtoExtension<TMiscExt>(ChunkMeta_.extensions());
    if (!misc.sorted()) {
        THROW_ERROR_EXCEPTION("Requested lookup for an unsorted chunk");
    }
    if (!misc.unique_keys()) {
        THROW_ERROR_EXCEPTION("Requested lookup for a chunk without unique_keys restriction");
    }

    auto keyColumnsExt = GetProtoExtension<TKeyColumnsExt>(ChunkMeta_.extensions());
    TKeyColumns chunkKeyColumns = NYT::FromProto<TKeyColumns>(keyColumnsExt);
    ChunkKeyColumnCount_ = chunkKeyColumns.size();

    if (KeyColumns_.empty()) {
        KeyColumns_ = chunkKeyColumns;
    }

    ValidateKeyColumns(KeyColumns_, chunkKeyColumns, Options_->DynamicTable);

    for (const auto& key : Keys_) {
        TReadLimit readLimit;
        readLimit.SetKey(TOwningKey(key));

        int index = ApplyLowerKeyLimit(BlockMetaExt_, readLimit, KeyColumns_.size());

        if (BlockIndexes_.empty() || BlockIndexes_.back() != index) {
            BlockIndexes_.push_back(index);
        }
    }
}

bool THorizontalSchemalessLookupChunkReader::Read(std::vector<TUnversionedRow>* rows)
{
    bool result = DoRead(rows);
    if (PerformanceCounters_) {
        PerformanceCounters_->StaticChunkRowLookupCount += rows->size();
    }
    return result;
}

bool THorizontalSchemalessLookupChunkReader::DoRead(std::vector<TUnversionedRow>* rows)
{
    YCHECK(rows->capacity() > 0);

    MemoryPool_.Clear();
    rows->clear();

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

                int blockIndex = BlockIndexes_[CurrentBlockIndex_];
                const auto& blockMeta = BlockMetaExt_.blocks(blockIndex);
                RowIndex_ = blockMeta.chunk_row_count() - blockMeta.row_count() + BlockReader_->GetRowIndex();
            } else {
                rows->push_back(TUnversionedRow());
            }
        }
        ++RowCount_;
    }

    return true;
}

void THorizontalSchemalessLookupChunkReader::InitFirstBlock()
{
    int blockIndex = BlockIndexes_[CurrentBlockIndex_];
    const auto& blockMeta = BlockMetaExt_.blocks(blockIndex);

    BlockReader_.reset(new THorizontalSchemalessBlockReader(
        CurrentBlock_.Get().ValueOrThrow(),
        blockMeta,
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

class TColumnarSchemalessChunkReader
    : public TSchemalessChunkReaderBase
    , public TColumnarRangeChunkReaderBase
{
public:
    TColumnarSchemalessChunkReader(
        const TChunkSpec& chunkSpec,
        TChunkReaderConfigPtr config,
        TChunkReaderOptionsPtr options,
        IChunkReaderPtr underlyingReader,
        TNameTablePtr nameTable,
        IBlockCachePtr blockCache,
        const TKeyColumns& keyColumns,
        const TColumnFilter& columnFilter,
        const TReadRange& readRange)
        : TSchemalessChunkReaderBase(
            chunkSpec,
            config,
            options,
            underlyingReader->GetChunkId(),
            nameTable,
            columnFilter,
            keyColumns)
        , TColumnarRangeChunkReaderBase(
            config,
            underlyingReader,
            blockCache)
    {
        LowerLimit_ = readRange.LowerLimit();
        UpperLimit_ = readRange.UpperLimit();

        ReadyEvent_ = BIND(&TColumnarSchemalessChunkReader::InitializeBlockSequence, MakeStrong(this))
            .AsyncVia(NChunkClient::TDispatcher::Get()->GetReaderInvoker())
            .Run();
    }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        YCHECK(rows->capacity() > 0);
        rows->clear();
        Pool_.Clear();

        if (!ReadyEvent_.IsSet() || !ReadyEvent_.Get().IsOK()) {
            return true;
        }

        if (Completed_) {
            return false;
        }

        i64 DataWeight = 0;
        while (rows->size() < rows->capacity()) {
            ResetExhaustedColumns();

            // Define how many to read.
            i64 rowLimit = std::min(HardUpperRowIndex_ - RowIndex_, static_cast<i64>(rows->capacity() - rows->size()));
            for (const auto& column : Columns_) {
                rowLimit = std::min(column.ColumnReader->GetReadyUpperRowIndex() - RowIndex_, rowLimit);
            }
            YCHECK(rowLimit > 0);

            std::vector<ui32> schemalessColumnCount(rowLimit, 0);
            if (SchemalessReader_) {
                SchemalessReader_->GetValueCounts(TMutableRange<ui32>(
                    schemalessColumnCount.data(),
                    schemalessColumnCount.size()));
            }

            int rangeBegin = rows->size();
            for (i64 index = 0; index < rowLimit; ++index) {
                auto row = TMutableUnversionedRow::Allocate(
                    &Pool_,
                    SchemaColumnReaders_.size() + schemalessColumnCount[index] + SystemColumnCount_);
                row.SetCount(SchemaColumnReaders_.size());
                rows->push_back(row);
            }

            auto range = TMutableRange<TMutableUnversionedRow>(
                static_cast<TMutableUnversionedRow*>(rows->data() + rangeBegin),
                static_cast<TMutableUnversionedRow*>(rows->data() + rangeBegin + rowLimit));

            // Read values.
            for (auto& columnReader : SchemaColumnReaders_) {
                columnReader->ReadValues(range);
            }

            if (SchemalessReader_) {
                SchemalessReader_->ReadValues(range);
            }

            if (!LowerKeyLimitReached_) {
                // Since LowerKey can be wider than chunk key in schemaless read,
                // we need to do additional check.
                i64 deltaIndex = 0;
                for (; deltaIndex < rowLimit; ++deltaIndex) {
                    if (CompareRows(
                        range[deltaIndex].Begin(),
                        range[deltaIndex].Begin() + KeyColumns_.size(),
                        LowerLimit_.GetKey().Begin(),
                        LowerLimit_.GetKey().End()) >= 0)
                    {
                        break;
                    }
                }
                if (deltaIndex > 0) {
                    rowLimit -= deltaIndex;
                    RowIndex_ += deltaIndex;

                    for (i64 index = 0; index < rowLimit; ++index) {
                        range[index] = range[index + deltaIndex];
                    }
                    range = range.Slice(range.Begin(), range.Begin() + rowLimit);
                    rows->resize(rows->size() - deltaIndex);
                }
                if (!range.Empty()) {
                    LowerKeyLimitReached_ = true;
                }
            }

            // Append system columns.
            for (i64 index = 0; index < rowLimit; ++index) {
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

                DataWeight += GetDataWeight(row);
            }

            YCHECK(RowIndex_ + rowLimit <= HardUpperRowIndex_);
            if (RowIndex_ + rowLimit > SafeUpperRowIndex_ && UpperLimit_.HasKey()) {
                i64 index = std::max(SafeUpperRowIndex_ - RowIndex_, i64(0));
                for (; index < rowLimit; ++index) {
                    if (CompareRows(
                        range[index].Begin(),
                        range[index].Begin() + KeyColumns_.size(),
                        UpperLimit_.GetKey().Begin(),
                        UpperLimit_.GetKey().End()) >= 0)
                    {
                        Completed_ = true;
                        range = range.Slice(range.Begin(), range.Begin() + index);
                        rows->resize(rows->size() - rowLimit + index);
                        break;
                    }
                }
            }

            if (RowIndex_ + rowLimit == HardUpperRowIndex_) {
                Completed_ = true;
            }

            RowIndex_ += range.Size();
            if (Completed_ || !TryFetchNextRow() || DataWeight > TSchemalessChunkReaderBase::Config_->MaxDataSizePerRead) {
                break;
            }
        }

        if (RowSampler_) {
            i64 insertIndex = 0;

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

        return true;
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        auto dataStatistics = TColumnarRangeChunkReaderBase::GetDataStatistics();
        dataStatistics.set_row_count(RowCount_);
        return dataStatistics;
    }

private:
    std::vector<std::unique_ptr<IUnversionedColumnReader>> SchemaColumnReaders_;
    std::unique_ptr<ISchemalessColumnReader> SchemalessReader_ = nullptr;

    bool Completed_ = false;
    bool LowerKeyLimitReached_ = false;

    TChunkedMemoryPool Pool_;

    void InitializeBlockSequence()
    {
        YCHECK(ChunkSpec_.chunk_meta().version() == static_cast<int>(ETableChunkFormat::UnversionedColumnar));
        InitializeSystemColumnIds();

        // Download chunk meta.
        std::vector<int> extensionTags = {
            TProtoExtensionTag<TMiscExt>::Value,
            TProtoExtensionTag<TTableSchemaExt>::Value,
            TProtoExtensionTag<TBlockMetaExt>::Value,
            TProtoExtensionTag<TColumnMetaExt>::Value,
            TProtoExtensionTag<TNameTableExt>::Value
        };

        auto asynChunkMeta = UnderlyingReader_->GetMeta(
            TColumnarRangeChunkReaderBase::Config_->WorkloadDescriptor,
            Null,
            extensionTags);
        auto chunkMeta = WaitFor(asynChunkMeta)
            .ValueOrThrow();

        auto chunkNameTable = FromProto<TNameTablePtr>(GetProtoExtension<TNameTableExt>(
            chunkMeta.extensions()));

        ChunkMeta_ = New<TColumnarChunkMeta>(std::move(chunkMeta));

        // Minimum prefix of key columns, that must be included in column filter.
        int minKeyColumnCount = KeyColumns_.size();
        if (UpperLimit_.HasKey()) {
            minKeyColumnCount = std::max(minKeyColumnCount, UpperLimit_.GetKey().GetCount());
        }
        if (LowerLimit_.HasKey()) {
            minKeyColumnCount = std::max(minKeyColumnCount, LowerLimit_.GetKey().GetCount());
        }
        bool sortedRead = minKeyColumnCount > 0;

        if (sortedRead && !ChunkMeta_->Misc().sorted()) {
            THROW_ERROR_EXCEPTION("Requested a sorted read for an unsorted chunk");
        }

        ValidateKeyColumns(
            KeyColumns_, 
            ChunkMeta_->ChunkSchema().GetKeyColumns(), 
            false /* require unique keys */);

        // Cannot read more key columns than stored in chunk, even if range keys are longer.
        minKeyColumnCount = std::min(minKeyColumnCount, ChunkMeta_->ChunkSchema().GetKeyColumnCount());

        if (sortedRead && KeyColumns_.empty()) {
            KeyColumns_ = ChunkMeta_->ChunkSchema().GetKeyColumns();
        }

        if (UpperLimit_.HasKey() || LowerLimit_.HasKey()) {
            ChunkMeta_->InitBlockLastKeys(KeyColumns_.size());
        }

        // Define columns to read.
        std::vector<TColumnIdMapping> schemalessIdMapping;
        schemalessIdMapping.resize(chunkNameTable->GetSize(), {-1, -1});

        std::vector<int> schemaColumnIndexes;
        bool readSchemalessColumns = false;
        if (ColumnFilter_.All) {
            for (int index = 0; index < ChunkMeta_->ChunkSchema().Columns().size(); ++index) {
                schemaColumnIndexes.push_back(index);
            }

            for (
                int chunkColumnId = ChunkMeta_->ChunkSchema().Columns().size();
                chunkColumnId < chunkNameTable->GetSize();
                ++chunkColumnId)
            {
                readSchemalessColumns = true;
                schemalessIdMapping[chunkColumnId].ChunkSchemaIndex = chunkColumnId;
                schemalessIdMapping[chunkColumnId].ReaderSchemaIndex = NameTable_->GetIdOrRegisterName(
                    chunkNameTable->GetName(chunkColumnId));
            }
        } else {
            auto filterIndexes = yhash_set<int>(ColumnFilter_.Indexes.begin(), ColumnFilter_.Indexes.end());
            for (int chunkColumnId = 0; chunkColumnId < chunkNameTable->GetSize(); ++chunkColumnId) {
                auto nameTableIndex = NameTable_->GetIdOrRegisterName(chunkNameTable->GetName(chunkColumnId));
                if (filterIndexes.has(nameTableIndex)) {
                    if (chunkColumnId < ChunkMeta_->ChunkSchema().Columns().size()) {
                        schemaColumnIndexes.push_back(chunkColumnId);
                    } else {
                        readSchemalessColumns = true;
                        schemalessIdMapping[chunkColumnId].ChunkSchemaIndex = chunkColumnId;
                        schemalessIdMapping[chunkColumnId].ReaderSchemaIndex = nameTableIndex;

                    }
                } else if (chunkColumnId < minKeyColumnCount) {
                    THROW_ERROR_EXCEPTION("At least %v key columns must be included in column filter for sorted read", minKeyColumnCount);
                }
            }
        }

        // Create column readers.
        for (int valueIndex = 0; valueIndex < schemaColumnIndexes.size(); ++valueIndex) {
            auto columnIndex = schemaColumnIndexes[valueIndex];
            auto columnReader = CreateUnversionedColumnReader(
                ChunkMeta_->ChunkSchema().Columns()[columnIndex],
                ChunkMeta_->ColumnMeta().columns(columnIndex),
                valueIndex,
                NameTable_->GetIdOrRegisterName(ChunkMeta_->ChunkSchema().Columns()[columnIndex].Name));

            Columns_.emplace_back(columnReader.get(), columnIndex);
            SchemaColumnReaders_.emplace_back(std::move(columnReader));
        }

        if (readSchemalessColumns) {
            SchemalessReader_ = CreateSchemalessColumnReader(
                ChunkMeta_->ColumnMeta().columns(ChunkMeta_->ChunkSchema().Columns().size()),
                schemalessIdMapping);

            Columns_.emplace_back(
                SchemalessReader_.get(),
                ChunkMeta_->ChunkSchema().Columns().size());
        }

        InitLowerRowIndex();
        InitUpperRowIndex();

        if (LowerRowIndex_ < HardUpperRowIndex_) {
            // We must continue initialization and set RowIndex_ before
            // ReadyEvent is set for the first time.
            InitBlockFetcher();
            WaitFor(RequestFirstBlocks())
                .ThrowOnError();

            ResetExhaustedColumns();
            Initialize(MakeRange(SchemaColumnReaders_.data(), KeyColumns_.size()));
            RowIndex_ = LowerRowIndex_;
            LowerKeyLimitReached_ = !LowerLimit_.HasKey();

            if (RowIndex_ >= HardUpperRowIndex_) {
                Completed_ = true;
            }
        } else {
            Completed_ = true;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

ISchemalessChunkReaderPtr CreateSchemalessChunkReader(
    const TChunkSpec& chunkSpec,
    TChunkReaderConfigPtr config,
    TChunkReaderOptionsPtr options,
    NChunkClient::IChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    NChunkClient::IBlockCachePtr blockCache,
    const TKeyColumns& keyColumns,
    const TColumnFilter& columnFilter,
    const TReadRange& readRange,
    TNullable<int> partitionTag)
{
    auto type = EChunkType(chunkSpec.chunk_meta().type());
    YCHECK(type == EChunkType::Table);

    auto formatVersion = ETableChunkFormat(chunkSpec.chunk_meta().version());

    switch (formatVersion) {
        case ETableChunkFormat::SchemalessHorizontal:
            return New<THorizontalSchemalessRangeChunkReader>(
                chunkSpec,
                config,
                options,
                underlyingReader,
                nameTable,
                blockCache,
                keyColumns,
                columnFilter,
                readRange,
                std::move(partitionTag));

        case ETableChunkFormat::Old: {
            YCHECK(readRange.LowerLimit().IsTrivial() && readRange.UpperLimit().IsTrivial());
            YCHECK(!partitionTag);

            return New<TLegacyTableChunkReader>(
                chunkSpec,
                config,
                options,
                columnFilter,
                nameTable,
                keyColumns,
                underlyingReader,
                blockCache);
        }

        case ETableChunkFormat::UnversionedColumnar:
            return New<TColumnarSchemalessChunkReader>(
                chunkSpec,
                config,
                options,
                underlyingReader,
                nameTable,
                blockCache,
                keyColumns,
                columnFilter,
                readRange);

        default:
            Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessChunkReaderPtr CreateSchemalessChunkReader(
    const TChunkSpec& chunkSpec,
    TChunkReaderConfigPtr config,
    TChunkReaderOptionsPtr options,
    NChunkClient::IChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    NChunkClient::IBlockCachePtr blockCache,
    const TKeyColumns& keyColumns,
    const TColumnFilter& columnFilter,
    const TSharedRange<TKey>& keys,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    TNullable<int> partitionTag)
{
    auto type = EChunkType(chunkSpec.chunk_meta().type());
    YCHECK(type == EChunkType::Table);

    auto formatVersion = ETableChunkFormat(chunkSpec.chunk_meta().version());

    switch (formatVersion) {
        case ETableChunkFormat::SchemalessHorizontal:
            return New<THorizontalSchemalessLookupChunkReader>(
                chunkSpec,
                std::move(config),
                std::move(options),
                std::move(underlyingReader),
                std::move(nameTable),
                std::move(blockCache),
                keyColumns,
                columnFilter,
                keys,
                std::move(performanceCounters),
                std::move(partitionTag));

        default:
            THROW_ERROR_EXCEPTION(
                "This operation is not supported for chunks in %Qv format",
                formatVersion);
    }
}

////////////////////////////////////////////////////////////////////////////////

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
    INativeClientPtr client,
    const TNodeDescriptor& localDescriptor,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const std::vector<NTableClient::TDataSliceDescriptor>& dataSliceDescriptors,
    TNameTablePtr nameTable,
    TColumnFilter columnFilter,
    const TKeyColumns& keyColumns,
    TNullable<int> partitionTag,
    IThroughputThrottlerPtr throttler)
{
    std::vector<IReaderFactoryPtr> factories;
    for (const auto& dataSliceDescriptor : dataSliceDescriptors) {
        switch (dataSliceDescriptor.Type) {
            case EDataSliceDescriptorType::UnversionedTable: {
                YCHECK(dataSliceDescriptor.ChunkSpecs.size() == 1);
                const auto& chunkSpec = dataSliceDescriptor.ChunkSpecs[0];
                auto memoryEstimate = GetChunkReaderMemoryEstimate(chunkSpec, config);
                auto createReader = [=] () {
                    auto remoteReader = CreateRemoteReader(
                        chunkSpec,
                        config,
                        options,
                        client,
                        nodeDirectory,
                        localDescriptor,
                        blockCache,
                        throttler);

                    using NYT::FromProto;
                    auto channel = chunkSpec.has_channel()
                        ? FromProto<TChannel>(chunkSpec.channel())
                        : TChannel::Universal();

                    TReadRange range = {
                        chunkSpec.has_lower_limit() ? TReadLimit(chunkSpec.lower_limit()) : TReadLimit(),
                        chunkSpec.has_upper_limit() ? TReadLimit(chunkSpec.upper_limit()) : TReadLimit()
                    };

                    return CreateSchemalessChunkReader(
                        chunkSpec,
                        PatchConfig(config, memoryEstimate),
                        options,
                        remoteReader,
                        nameTable,
                        blockCache,
                        keyColumns,
                        columnFilter.All ? CreateColumnFilter(channel, nameTable) : columnFilter,
                        range,
                        partitionTag);
                };

                factories.emplace_back(CreateReaderFactory(createReader, memoryEstimate));
                break;
            }

            case EDataSliceDescriptorType::VersionedTable: {
                auto memoryEstimate = GetDataSliceDescriptorReaderMemoryEstimate(dataSliceDescriptor, config);
                auto createReader  = [=] () {
                    return CreateSchemalessMergingMultiChunkReader(
                        config,
                        options,
                        client,
                        localDescriptor,
                        blockCache,
                        nodeDirectory,
                        dataSliceDescriptor.ChunkSpecs,
                        nameTable,
                        columnFilter,
                        dataSliceDescriptor.Schema,
                        throttler,
                        dataSliceDescriptor.Timestamp);
                };

                factories.emplace_back(CreateReaderFactory(createReader, memoryEstimate));
                break;
            }

            default:
                Y_UNREACHABLE();
        }
    }

    return factories;
}

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
        INativeClientPtr client,
        const TNodeDescriptor& localDescriptor,
        IBlockCachePtr blockCache,
        TNodeDirectoryPtr nodeDirectory,
        const std::vector<NTableClient::TDataSliceDescriptor>& dataSliceDescriptors,
        TNameTablePtr nameTable,
        TColumnFilter columnFilter,
        const TKeyColumns& keyColumns,
        TNullable<int> partitionTag,
        IThroughputThrottlerPtr throttler);

    virtual bool Read(std::vector<TUnversionedRow>* rows) override;

    virtual i64 GetSessionRowIndex() const override;

    virtual i64 GetTotalRowCount() const override;

    virtual TNameTablePtr GetNameTable() const override;

    virtual TKeyColumns GetKeyColumns() const override;

    virtual i64 GetTableRowIndex() const override;

private:
    const TNameTablePtr NameTable_;
    const TKeyColumns KeyColumns_;

    ISchemalessChunkReaderPtr CurrentReader_;
    std::atomic<i64> RowIndex_ = {0};
    std::atomic<i64> RowCount_ = {-1};

    using TBase::ReadyEvent_;
    using TBase::CurrentSession_;

    virtual void OnReaderSwitched() override;
};

////////////////////////////////////////////////////////////////////////////////

template <class TBase>
TSchemalessMultiChunkReader<TBase>::TSchemalessMultiChunkReader(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    INativeClientPtr client,
    const TNodeDescriptor& localDescriptor,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const std::vector<NTableClient::TDataSliceDescriptor>& dataSliceDescriptors,
    TNameTablePtr nameTable,
    TColumnFilter columnFilter,
    const TKeyColumns& keyColumns,
    TNullable<int> partitionTag,
    IThroughputThrottlerPtr throttler)
    : TBase(
        config,
        options,
        CreateReaderFactories(
            config,
            options,
            client,
            localDescriptor,
            blockCache,
            nodeDirectory,
            dataSliceDescriptors,
            nameTable,
            columnFilter,
            keyColumns,
            partitionTag,
            throttler))
    , NameTable_(nameTable)
    , KeyColumns_(keyColumns)
    , RowCount_(GetCumulativeRowCount(dataSliceDescriptors))
{ }

template <class TBase>
bool TSchemalessMultiChunkReader<TBase>::Read(std::vector<TUnversionedRow>* rows)
{
    rows->clear();
    if (!ReadyEvent_.IsSet() || !ReadyEvent_.Get().IsOK()) {
        return true;
    }

    // Nothing to read.
    if (!CurrentReader_) {
        return false;
    }

    bool readerFinished = !CurrentReader_->Read(rows);
    if (!rows->empty()) {
        RowIndex_ += rows->size();
        return true;
    }

    if (TBase::OnEmptyRead(readerFinished)) {
        return true;
    } else {
        RowCount_ = RowIndex_.load();
        CurrentReader_ = nullptr;
        return false;
    }
}

template <class TBase>
void TSchemalessMultiChunkReader<TBase>::OnReaderSwitched()
{
    CurrentReader_ = dynamic_cast<ISchemalessChunkReader*>(CurrentSession_.Reader.Get());
    YCHECK(CurrentReader_);
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
TNameTablePtr TSchemalessMultiChunkReader<TBase>::GetNameTable() const
{
    return NameTable_;
}

template <class TBase>
TKeyColumns TSchemalessMultiChunkReader<TBase>::GetKeyColumns() const
{
    return KeyColumns_;
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessSequentialMultiChunkReader(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    INativeClientPtr client,
    const TNodeDescriptor& localDescriptor,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const std::vector<NTableClient::TDataSliceDescriptor>& dataSliceDescriptors,
    TNameTablePtr nameTable,
    TColumnFilter columnFilter,
    const TKeyColumns& keyColumns,
    TNullable<int> partitionTag,
    IThroughputThrottlerPtr throttler)
{
    auto reader = New<TSchemalessMultiChunkReader<TSequentialMultiReaderBase>>(
        config,
        options,
        client,
        localDescriptor,
        blockCache,
        nodeDirectory,
        dataSliceDescriptors,
        nameTable,
        columnFilter,
        keyColumns,
        partitionTag,
        throttler);

    reader->Open();
    return reader;
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessParallelMultiChunkReader(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    INativeClientPtr client,
    const TNodeDescriptor& localDescriptor,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const std::vector<NTableClient::TDataSliceDescriptor>& dataSliceDescriptors,
    TNameTablePtr nameTable,
    TColumnFilter columnFilter,
    const TKeyColumns& keyColumns,
    TNullable<int> partitionTag,
    IThroughputThrottlerPtr throttler)
{
    auto reader = New<TSchemalessMultiChunkReader<TParallelMultiReaderBase>>(
        config,
        options,
        client,
        localDescriptor,
        blockCache,
        nodeDirectory,
        dataSliceDescriptors,
        nameTable,
        columnFilter,
        keyColumns,
        partitionTag,
        throttler);

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
        INativeClientPtr client,
        const TNodeDescriptor& localDescriptor,
        IBlockCachePtr blockCache,
        TNodeDirectoryPtr nodeDirectory,
        const std::vector<TChunkSpec>& chunkSpecs,
        TNameTablePtr nameTable,
        TColumnFilter columnFilter,
        const TTableSchema& tableSchema,
        IThroughputThrottlerPtr throttler,
        TTimestamp timestamp);

    virtual TFuture<void> GetReadyEvent() override;
    virtual bool Read(std::vector<TUnversionedRow>* rows) override;
    virtual TDataStatistics GetDataStatistics() const override;
    virtual std::vector<TChunkId> GetFailedChunkIds() const override;
    virtual bool IsFetchingCompleted() const override;
    virtual i64 GetSessionRowIndex() const override;
    virtual i64 GetTotalRowCount() const override;
    virtual TNameTablePtr GetNameTable() const override;
    virtual TKeyColumns GetKeyColumns() const override;
    virtual i64 GetTableRowIndex() const override;

private:
    const ISchemalessReaderPtr UnderlyingReader_;

    i64 RowIndex_ = 0;
    const i64 RowCount_;

    TSchemalessMergingMultiChunkReader(
        ISchemalessReaderPtr underlyingReader,
        i64 rowCount);

    DECLARE_NEW_FRIEND();
};

////////////////////////////////////////////////////////////////////////////////

TSchemalessMergingMultiChunkReader::TSchemalessMergingMultiChunkReader(
    ISchemalessReaderPtr underlyingReader,
    i64 rowCount)
    : UnderlyingReader_(std::move(underlyingReader))
    , RowCount_(rowCount)
{ }

ISchemalessMultiChunkReaderPtr TSchemalessMergingMultiChunkReader::Create(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    INativeClientPtr client,
    const TNodeDescriptor& localDescriptor,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const std::vector<TChunkSpec>& chunkSpecs,
    TNameTablePtr nameTable,
    TColumnFilter columnFilter,
    const TTableSchema& tableSchema,
    IThroughputThrottlerPtr throttler,
    TTimestamp timestamp)
{
    std::vector<TOwningKey> boundaries;
    boundaries.reserve(chunkSpecs.size());

    for (const auto& chunkSpec : chunkSpecs) {
        TOwningKey minKey;
        if (chunkSpec.has_lower_limit()) {
            auto limit = NYT::FromProto<TReadLimit>(chunkSpec.lower_limit());
            minKey = limit.GetKey();
        } else if (FindProtoExtension<NProto::TBoundaryKeysExt>(chunkSpec.chunk_meta().extensions())) {
            auto boundaryKeysExt = GetProtoExtension<NProto::TBoundaryKeysExt>(chunkSpec.chunk_meta().extensions());
            minKey = NYT::FromProto<TOwningKey>(boundaryKeysExt.min());
        }
        boundaries.push_back(minKey);
    }

    LOG_DEBUG("Create overlapping range reader (Boundaries: %v, Chunks: %v)",
        boundaries,
        MakeFormattableRange(chunkSpecs, [] (TStringBuilder* builder, const TChunkSpec& chunkSpec) {
            FormatValue(builder, FromProto<TChunkId>(chunkSpec.chunk_id()), TStringBuf());
        }));

    auto performanceCounters = New<TChunkReaderPerformanceCounters>();

    auto createVersionedReader = [
        config,
        options,
        client,
        localDescriptor,
        blockCache,
        nodeDirectory,
        chunkSpecs,
        columnFilter,
        tableSchema,
        performanceCounters,
        timestamp
    ] (int index) -> IVersionedReaderPtr {
        const auto& chunkSpec = chunkSpecs[index];
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

        LOG_DEBUG("Create versioned chunk reader (ChunkId: %v, Range: <%v : %v>)",
            chunkId,
            lowerLimit.GetKey(),
            upperLimit.GetKey());

        auto chunkReader = CreateReplicationReader(
            config,
            options,
            client,
            nodeDirectory,
            localDescriptor,
            chunkId,
            replicas,
            blockCache);

        auto asyncChunkMeta = TCachedVersionedChunkMeta::Load(
            chunkReader,
            config->WorkloadDescriptor,
            tableSchema);
        auto chunkMeta = WaitFor(asyncChunkMeta)
            .ValueOrThrow();

        return CreateVersionedChunkReader(
            config,
            std::move(chunkReader),
            blockCache,
            std::move(chunkMeta),
            lowerLimit,
            upperLimit,
            columnFilter,
            performanceCounters,
            timestamp);
    };

    struct TSchemalessMergingMultiChunkReaderBufferTag
    { };

    auto rowMerger = New<TSchemafulRowMerger>(
        New<TRowBuffer>(TSchemalessMergingMultiChunkReaderBufferTag()),
        tableSchema.Columns().size(),
        tableSchema.GetKeyColumnCount(),
        columnFilter,
        client->GetNativeConnection()->GetColumnEvaluatorCache()->Find(tableSchema));

    auto schemafulReaderFactory = [&] (const TTableSchema&, const TColumnFilter&) {
        return CreateSchemafulOverlappingRangeReader(
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
    };

    auto reader = CreateSchemalessReaderAdapter(
        schemafulReaderFactory,
        nameTable,
        tableSchema,
        columnFilter);

    i64 rowCount = NChunkClient::GetCumulativeRowCount(chunkSpecs);

    return New<TSchemalessMergingMultiChunkReader>(
        std::move(reader),
        rowCount);
}

bool TSchemalessMergingMultiChunkReader::Read(std::vector<TUnversionedRow>* rows)
{
    bool result = UnderlyingReader_->Read(rows);
    RowIndex_ += rows->size();
    return result;
}

TFuture<void> TSchemalessMergingMultiChunkReader::GetReadyEvent()
{
    return UnderlyingReader_->GetReadyEvent();
}

TDataStatistics TSchemalessMergingMultiChunkReader::GetDataStatistics() const
{
    return UnderlyingReader_->GetDataStatistics();
}

std::vector<TChunkId> TSchemalessMergingMultiChunkReader::GetFailedChunkIds() const
{
    return UnderlyingReader_->GetFailedChunkIds();
}

bool TSchemalessMergingMultiChunkReader::IsFetchingCompleted() const
{
    return UnderlyingReader_->IsFetchingCompleted();
}

i64 TSchemalessMergingMultiChunkReader::GetTotalRowCount() const
{
    return RowCount_;
}

i64 TSchemalessMergingMultiChunkReader::GetSessionRowIndex() const
{
    return RowIndex_;
}

i64 TSchemalessMergingMultiChunkReader::GetTableRowIndex() const
{
    return 0;
}

TNameTablePtr TSchemalessMergingMultiChunkReader::GetNameTable() const
{
    return UnderlyingReader_->GetNameTable();
}

TKeyColumns TSchemalessMergingMultiChunkReader::GetKeyColumns() const
{
    return UnderlyingReader_->GetKeyColumns();
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessMergingMultiChunkReader(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    INativeClientPtr client,
    const TNodeDescriptor& localDescriptor,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const std::vector<TChunkSpec>& chunkSpecs,
    TNameTablePtr nameTable,
    TColumnFilter columnFilter,
    const TTableSchema& tableSchema,
    IThroughputThrottlerPtr throttler,
    TTimestamp timestamp)
{
    return TSchemalessMergingMultiChunkReader::Create(
        config,
        options,
        client,
        localDescriptor,
        blockCache,
        nodeDirectory,
        chunkSpecs,
        nameTable,
        columnFilter,
        tableSchema,
        throttler,
        timestamp);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
