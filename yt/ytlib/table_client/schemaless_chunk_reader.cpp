#include "schemaless_chunk_reader.h"
#include "private.h"
#include "chunk_reader_base.h"
#include "config.h"
#include "helpers.h"
#include "legacy_table_chunk_reader.h"
#include "versioned_chunk_reader.h"
#include "schemaful_overlapping_chunk_reader.h"
#include "name_table.h"
#include "row_sampler.h"
#include "schema.h"
#include "schemaless_block_reader.h"
#include "cached_versioned_chunk_meta.h"
#include "schemaful_reader.h"
#include "row_merger.h"

#include <yt/ytlib/api/client.h>
#include <yt/ytlib/api/connection.h>

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

namespace {

void ValidateReadRanges(const std::vector<TReadRange>& readRanges)
{
    YCHECK(!readRanges.empty());

    bool hasRowIndex = false;
    bool hasKey = false;

    for (const auto& range : readRanges) {
        if (range.LowerLimit().HasRowIndex() || range.UpperLimit().HasRowIndex()) {
            hasRowIndex = true;
        }
        if (range.LowerLimit().HasKey() || range.UpperLimit().HasKey()) {
            hasKey = true;
        }
        if (hasRowIndex && hasKey && readRanges.size() > 1) {
            THROW_ERROR_EXCEPTION("Both key ranges and index ranges are not supported when reading multiple ranges");
        }
    }

    if (hasRowIndex) {
        for (int index = 0; index < readRanges.size(); ++index) {
            if ((index > 0 && !readRanges[index].LowerLimit().HasRowIndex()) ||
                ((index < readRanges.size() - 1 && !readRanges[index].UpperLimit().HasRowIndex())))
            {
                THROW_ERROR_EXCEPTION("Overlapping ranges are not supported");
            }
        }

        for (int index = 1; index < readRanges.size(); ++index) {
            const auto& prev = readRanges[index - 1].UpperLimit();
            const auto& next = readRanges[index].LowerLimit();
            if (prev.GetRowIndex() > next.GetRowIndex()) {
                THROW_ERROR_EXCEPTION("Overlapping ranges are not supported");
            }
        }
    }

    if (hasKey) {
        for (int index = 0; index < readRanges.size(); ++index) {
            if ((index > 0 && !readRanges[index].LowerLimit().HasKey()) ||
                ((index < readRanges.size() - 1 && !readRanges[index].UpperLimit().HasKey())))
            {
                THROW_ERROR_EXCEPTION("Overlapping ranges are not supported");
            }
        }

        for (int index = 1; index < readRanges.size(); ++index) {
            const auto& prev = readRanges[index - 1].UpperLimit();
            const auto& next = readRanges[index].LowerLimit();
            if (CompareRows(prev.GetKey(), next.GetKey()) > 0) {
                THROW_ERROR_EXCEPTION("Overlapping ranges are not supported");
            }
        }
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TSchemalessChunkReader
    : public ISchemalessChunkReader
    , public TChunkReaderBase
{
public:
    TSchemalessChunkReader(
        const TChunkSpec& chunkSpec,
        TChunkReaderConfigPtr config,
        TChunkReaderOptionsPtr options,
        IChunkReaderPtr underlyingReader,
        TNameTablePtr nameTable,
        IBlockCachePtr blockCache,
        const TKeyColumns& keyColumns,
        const TColumnFilter& columnFilter,
        std::vector<TReadRange> readRanges,
        TNullable<int> partitionTag);

    virtual bool Read(std::vector<TUnversionedRow>* rows) override;

    virtual TDataStatistics GetDataStatistics() const override;

    virtual TNameTablePtr GetNameTable() const override;

    virtual i64 GetTableRowIndex() const override;

    virtual TKeyColumns GetKeyColumns() const override;

private:
    const TChunkSpec ChunkSpec_;

    TChunkReaderConfigPtr Config_;
    TChunkReaderOptionsPtr Options_;
    const TNameTablePtr NameTable_;
    TNameTablePtr ChunkNameTable_;

    const TColumnFilter ColumnFilter_;
    TKeyColumns KeyColumns_;

    const int SystemColumnCount_;
    std::vector<TReadRange> ReadRanges_;

    TNullable<int> PartitionTag_;

    // Maps chunk name table ids into client id.
    // For filtered out columns maps id to -1.
    std::vector<int> IdMapping_;

    std::unique_ptr<THorizontalSchemalessBlockReader> BlockReader_;

    int CurrentBlockIndex_ = 0;
    i64 CurrentRowIndex_ = -1;
    int CurrentRangeIndex_ = 0;
    i64 RowCount_ = 0;

    bool RangeEnded_ = false;

    int RowIndexId_ = -1;
    int RangeIndexId_ = -1;
    int TableIndexId_ = -1;

    TChunkMeta ChunkMeta_;
    TBlockMetaExt BlockMetaExt_;

    std::unique_ptr<IRowSampler> RowSampler_;
    std::vector<int> BlockIndexes_;

    TFuture<void> InitializeBlockSequence();

    virtual void InitFirstBlock() override;
    virtual void InitNextBlock() override;

    TSequentialReader::TBlockInfo CreateBlockInfo(int index);
    void CreateBlockSequence(int beginIndex, int endIndex);
    void DownloadChunkMeta(std::vector<int> extensionTags, TNullable<int> partitionTag = Null);

    void SkipToCurrrentRange();
    bool InitNextRange();

    void InitializeBlockSequencePartition();
    void InitializeBlockSequenceSorted();
    void InitializeBlockSequenceUnsorted();
};

DEFINE_REFCOUNTED_TYPE(TSchemalessChunkReader)

////////////////////////////////////////////////////////////////////////////////

TSchemalessChunkReader::TSchemalessChunkReader(
    const TChunkSpec& chunkSpec,
    TChunkReaderConfigPtr config,
    TChunkReaderOptionsPtr options,
    IChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    IBlockCachePtr blockCache,
    const TKeyColumns& keyColumns,
    const TColumnFilter& columnFilter,
    std::vector<TReadRange> readRanges,
    TNullable<int> partitionTag)
    : TChunkReaderBase(
        config, 
        underlyingReader,
        blockCache)
    , ChunkSpec_(chunkSpec)
    , Config_(config)
    , Options_(options)
    , NameTable_(nameTable)
    , ChunkNameTable_(New<TNameTable>())
    , ColumnFilter_(columnFilter)
    , KeyColumns_(keyColumns)
    , SystemColumnCount_(GetSystemColumnCount(options))
    , ReadRanges_(readRanges)
    , PartitionTag_(std::move(partitionTag))
{
    if (Config_->SamplingRate) {
        RowSampler_ = CreateChunkRowSampler(
            UnderlyingReader_->GetChunkId(),
            Config_->SamplingRate.Get(),
            Config_->SamplingSeed.Get(std::random_device()()));
    }

    if (ReadRanges_.size() == 1) {
        LOG_DEBUG("Reading single range (Range: %v)", ReadRanges_[0]);
    } else {
        LOG_DEBUG("Reading multiple ranges (RangeCount: %v)", ReadRanges_.size());
    }

    if (Options_->EnableRowIndex) {
        RowIndexId_ = NameTable_->GetIdOrRegisterName(RowIndexColumnName);
    }

    if (Options_->EnableRangeIndex) {
        RangeIndexId_ = NameTable_->GetIdOrRegisterName(RangeIndexColumnName);
    }

    if (Options_->EnableTableIndex) {
        TableIndexId_ = NameTable_->GetIdOrRegisterName(TableIndexColumnName);
    }

    ReadyEvent_ = BIND(&TSchemalessChunkReader::InitializeBlockSequence, MakeStrong(this))
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
        .Run();
}

TFuture<void> TSchemalessChunkReader::InitializeBlockSequence()
{
    YCHECK(ChunkSpec_.chunk_meta().version() == static_cast<int>(ETableChunkFormat::SchemalessHorizontal));
    YCHECK(BlockIndexes_.empty());

    if (PartitionTag_) {
        InitializeBlockSequencePartition();
    } else {
        if (KeyColumns_.empty() &&
            ReadRanges_.size() == 1 &&
            !ReadRanges_[0].LowerLimit().HasKey() &&
            !ReadRanges_[0].UpperLimit().HasKey())
        {
            InitializeBlockSequenceUnsorted();
        } else {
            InitializeBlockSequenceSorted();
        }
    }

    LOG_DEBUG("Reading %v blocks", BlockIndexes_.size());

    std::vector<TSequentialReader::TBlockInfo> blocks;
    for (int index : BlockIndexes_) {
        blocks.push_back(CreateBlockInfo(index));
    }

    return DoOpen(std::move(blocks), GetProtoExtension<TMiscExt>(ChunkMeta_.extensions()));
}

TSequentialReader::TBlockInfo TSchemalessChunkReader::CreateBlockInfo(int blockIndex)
{
    YCHECK(blockIndex < BlockMetaExt_.blocks_size());
    auto& blockMeta = BlockMetaExt_.blocks(blockIndex);
    TSequentialReader::TBlockInfo blockInfo;
    blockInfo.Index = blockMeta.block_index();
    blockInfo.UncompressedDataSize = blockMeta.uncompressed_size();
    return blockInfo;
}

void TSchemalessChunkReader::DownloadChunkMeta(std::vector<int> extensionTags, TNullable<int> partitionTag)
{
    extensionTags.push_back(TProtoExtensionTag<TMiscExt>::Value);
    extensionTags.push_back(TProtoExtensionTag<TBlockMetaExt>::Value);
    extensionTags.push_back(TProtoExtensionTag<TNameTableExt>::Value);
    ChunkMeta_ = WaitFor(UnderlyingReader_->GetMeta(partitionTag, extensionTags))
        .ValueOrThrow();

    BlockMetaExt_ = GetProtoExtension<TBlockMetaExt>(ChunkMeta_.extensions());

    auto nameTableExt = GetProtoExtension<TNameTableExt>(ChunkMeta_.extensions());
    FromProto(&ChunkNameTable_, nameTableExt);

    IdMapping_.resize(ChunkNameTable_->GetSize(), -1);

    if (ColumnFilter_.All) {
        for (int chunkNameId = 0; chunkNameId < ChunkNameTable_->GetSize(); ++chunkNameId) {
            auto name = ChunkNameTable_->GetName(chunkNameId);
            auto id = NameTable_->GetIdOrRegisterName(name);
            IdMapping_[chunkNameId] = id;
        }
    } else {
        for (auto id : ColumnFilter_.Indexes) {
            auto name = NameTable_->GetName(id);
            auto chunkNameId = ChunkNameTable_->FindId(name);
            if (chunkNameId) {
                IdMapping_[chunkNameId.Get()] = id;
            }
        }
    }
}

void TSchemalessChunkReader::InitializeBlockSequenceSorted()
{
    std::vector<int> extensionTags = {
        TProtoExtensionTag<TKeyColumnsExt>::Value,
    };

    DownloadChunkMeta(extensionTags);

    auto misc = GetProtoExtension<TMiscExt>(ChunkMeta_.extensions());
    if (!misc.sorted()) {
        THROW_ERROR_EXCEPTION("Requested a sorted read for an unsorted chunk");
    }

    auto keyColumnsExt = GetProtoExtension<TKeyColumnsExt>(ChunkMeta_.extensions());
    TKeyColumns chunkKeyColumns = NYT::FromProto<TKeyColumns>(keyColumnsExt);

    ValidateKeyColumns(KeyColumns_, chunkKeyColumns);

    if (KeyColumns_.empty()) {
        KeyColumns_ = chunkKeyColumns;
    }

    int lastIndex = -1;
    std::vector<TSequentialReader::TBlockInfo> blocks;
    for (const auto& readRange : ReadRanges_) {
        int beginIndex = std::max(
            ApplyLowerRowLimit(BlockMetaExt_, readRange.LowerLimit()),
            ApplyLowerKeyLimit(BlockMetaExt_, readRange.LowerLimit()));
        int endIndex = std::min(
            ApplyUpperRowLimit(BlockMetaExt_, readRange.UpperLimit()),
            ApplyUpperKeyLimit(BlockMetaExt_, readRange.UpperLimit()));

        if (beginIndex == lastIndex) {
            ++beginIndex;
        }
        YCHECK(beginIndex > lastIndex);

        for (int index = beginIndex; index < endIndex; ++index) {
            BlockIndexes_.push_back(index);
            lastIndex = index;
        }
    }
}

void TSchemalessChunkReader::InitializeBlockSequencePartition()
{
    YCHECK(ReadRanges_.size() == 1);
    YCHECK(ReadRanges_[0].LowerLimit().IsTrivial());
    YCHECK(ReadRanges_[0].UpperLimit().IsTrivial());

    DownloadChunkMeta(std::vector<int>(), PartitionTag_);
    CreateBlockSequence(0, BlockMetaExt_.blocks_size());
}

void TSchemalessChunkReader::InitializeBlockSequenceUnsorted()
{
    YCHECK(ReadRanges_.size() == 1);

    DownloadChunkMeta(std::vector<int>());

    CreateBlockSequence(
        ApplyLowerRowLimit(BlockMetaExt_, ReadRanges_[0].LowerLimit()),
        ApplyUpperRowLimit(BlockMetaExt_, ReadRanges_[0].UpperLimit()));
}

void TSchemalessChunkReader::CreateBlockSequence(int beginIndex, int endIndex)
{
    for (int index = beginIndex; index < endIndex; ++index) {
        BlockIndexes_.push_back(index);
    }
}

TDataStatistics TSchemalessChunkReader::GetDataStatistics() const
{
    auto dataStatistics = TChunkReaderBase::GetDataStatistics();
    dataStatistics.set_row_count(RowCount_);
    return dataStatistics;
}

void TSchemalessChunkReader::InitFirstBlock()
{
    int blockIndex = BlockIndexes_[CurrentBlockIndex_];
    const auto& blockMeta = BlockMetaExt_.blocks(blockIndex);

    CheckBlockUpperLimits(blockMeta, ReadRanges_[CurrentRangeIndex_].UpperLimit());

    BlockReader_.reset(new THorizontalSchemalessBlockReader(
        SequentialReader_->GetCurrentBlock(),
        blockMeta,
        IdMapping_,
        KeyColumns_.size(),
        SystemColumnCount_));

    CurrentRowIndex_ = blockMeta.chunk_row_count() - blockMeta.row_count();

    SkipToCurrrentRange();
}

void TSchemalessChunkReader::InitNextBlock()
{
    ++CurrentBlockIndex_;
    InitFirstBlock();
}

void TSchemalessChunkReader::SkipToCurrrentRange()
{
    int blockIndex = BlockIndexes_[CurrentBlockIndex_];
    CheckBlockUpperLimits(BlockMetaExt_.blocks(blockIndex), ReadRanges_[CurrentRangeIndex_].UpperLimit());

    const auto& lowerLimit = ReadRanges_[CurrentRangeIndex_].LowerLimit();

    if (lowerLimit.HasRowIndex() && CurrentRowIndex_ < lowerLimit.GetRowIndex()) {
        YCHECK(BlockReader_->SkipToRowIndex(lowerLimit.GetRowIndex() - CurrentRowIndex_));
        CurrentRowIndex_ = lowerLimit.GetRowIndex();
    }

    if (lowerLimit.HasKey()) {
        auto blockRowIndex = BlockReader_->GetRowIndex();
        YCHECK(BlockReader_->SkipToKey(lowerLimit.GetKey()));
        CurrentRowIndex_ += BlockReader_->GetRowIndex() - blockRowIndex;
    }
}

bool TSchemalessChunkReader::InitNextRange()
{
    RangeEnded_ = false;
    ++CurrentRangeIndex_;

    if (CurrentRangeIndex_ == ReadRanges_.size()) {
        LOG_DEBUG("Upper limit %v reached", ToString(ReadRanges_.back()));
        return false;
    }

    const auto& lowerLimit = ReadRanges_[CurrentRangeIndex_].LowerLimit();
    const auto& blockMeta = BlockMetaExt_.blocks(BlockIndexes_[CurrentBlockIndex_]);

    if ((lowerLimit.HasRowIndex() && blockMeta.chunk_row_count() <= lowerLimit.GetRowIndex()) ||
        (lowerLimit.HasKey() && CompareRows(FromProto<TOwningKey>(blockMeta.last_key()), lowerLimit.GetKey()) < 0))
    {
        BlockReader_.reset();
        return OnBlockEnded();
    } else {
        SkipToCurrrentRange();
        return true;
    }
}

bool TSchemalessChunkReader::Read(std::vector<TUnversionedRow>* rows)
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

    if (RangeEnded_) {
        return InitNextRange();
    }

    if (BlockEnded_) {
        BlockReader_.reset();
        return OnBlockEnded();
    }

    i64 dataWeight = 0;
    while (rows->size() < rows->capacity() && dataWeight < Config_->MaxDataSizePerRead) {
        if ((CheckRowLimit_ && CurrentRowIndex_ >= ReadRanges_[CurrentRangeIndex_].UpperLimit().GetRowIndex()) ||
            (CheckKeyLimit_ && CompareRows(BlockReader_->GetKey(), ReadRanges_[CurrentRangeIndex_].UpperLimit().GetKey()) >= 0))
        {
            RangeEnded_ = true;
            return true;
        }

        if (!RowSampler_ || RowSampler_->ShouldTakeRow(GetTableRowIndex())) {
            auto row = BlockReader_->GetRow(&MemoryPool_);
            if (Options_->EnableRangeIndex) {
                *row.End() = MakeUnversionedInt64Value(ChunkSpec_.range_index(), RangeIndexId_);
                ++row.GetHeader()->Count;
            }
            if (Options_->EnableTableIndex) {
                *row.End() = MakeUnversionedInt64Value(ChunkSpec_.table_index(), TableIndexId_);
                ++row.GetHeader()->Count;
            }
            if (Options_->EnableRowIndex) {
                *row.End() = MakeUnversionedInt64Value(GetTableRowIndex(), RowIndexId_);
                ++row.GetHeader()->Count;
            }

            rows->push_back(row);
            dataWeight += GetDataWeight(rows->back());
            ++RowCount_;
        }
        ++CurrentRowIndex_;

        if (!BlockReader_->NextRow()) {
            BlockEnded_ = true;
            return true;
        }
    }

    return true;
}

TNameTablePtr TSchemalessChunkReader::GetNameTable() const
{
    return NameTable_;
}

i64 TSchemalessChunkReader::GetTableRowIndex() const
{
    YCHECK(const_cast<TSchemalessChunkReader*>(this)->BeginRead());
    return ChunkSpec_.table_row_index() + CurrentRowIndex_;
}

TKeyColumns TSchemalessChunkReader::GetKeyColumns() const
{
    return KeyColumns_;
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
    std::vector<NChunkClient::TReadRange> readRanges,
    TNullable<int> partitionTag)
{
    auto type = EChunkType(chunkSpec.chunk_meta().type());
    YCHECK(type == EChunkType::Table);

    ValidateReadRanges(readRanges);

    auto formatVersion = ETableChunkFormat(chunkSpec.chunk_meta().version());

    switch (formatVersion) {
        case ETableChunkFormat::SchemalessHorizontal:
            return New<TSchemalessChunkReader>(
                chunkSpec,
                config,
                options,
                underlyingReader,
                nameTable,
                blockCache,
                keyColumns,
                columnFilter,
                std::move(readRanges),
                std::move(partitionTag));

        case ETableChunkFormat::Old: {
            YCHECK(readRanges.size() == 0);
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

        default:
            YUNREACHABLE();
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
    IClientPtr client,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const std::vector<TChunkSpec>& chunkSpecs,
    TNameTablePtr nameTable,
    TColumnFilter columnFilter,
    const TKeyColumns& keyColumns,
    TNullable<int> partitionTag,
    IThroughputThrottlerPtr throttler)
{
    std::vector<IReaderFactoryPtr> factories;
    for (const auto& chunkSpec : chunkSpecs) {
        auto memoryEstimate = GetChunkReaderMemoryEstimate(chunkSpec, config);
        auto createReader = [=] () {
            auto remoteReader = CreateRemoteReader(
                chunkSpec,
                config,
                options,
                client,
                nodeDirectory,
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
            std::vector<TReadRange>(1, std::move(range)),
            partitionTag);
        };

        factories.emplace_back(CreateReaderFactory(createReader, memoryEstimate));
    }

    return factories;
}

////////////////////////////////////////////////////////////////////////////////

template<class TBase>
class TSchemalessMultiChunkReader
    : public ISchemalessMultiChunkReader
    , public TBase
{
public:
    TSchemalessMultiChunkReader(
        TTableReaderConfigPtr config,
        TTableReaderOptionsPtr options,
        IClientPtr client,
        IBlockCachePtr blockCache,
        TNodeDirectoryPtr nodeDirectory,
        const std::vector<TChunkSpec>& chunkSpecs,
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

template<class TBase>
TSchemalessMultiChunkReader<TBase>::TSchemalessMultiChunkReader(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    IClientPtr client,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const std::vector<TChunkSpec>& chunkSpecs,
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
            blockCache,
            nodeDirectory,
            chunkSpecs,
            nameTable,
            columnFilter,
            keyColumns,
            partitionTag,
            throttler))
    , NameTable_(nameTable)
    , KeyColumns_(keyColumns)
    , RowCount_(GetCumulativeRowCount(chunkSpecs))
{ }

template <class TBase>
bool TSchemalessMultiChunkReader<TBase>::Read(std::vector<TUnversionedRow>* rows)
{
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
    IClientPtr client,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const std::vector<TChunkSpec>& chunkSpecs,
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
        blockCache,
        nodeDirectory,
        chunkSpecs,
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
    IClientPtr client,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const std::vector<TChunkSpec>& chunkSpecs,
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
        blockCache,
        nodeDirectory,
        chunkSpecs,
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
        IClientPtr client,
        IBlockCachePtr blockCache,
        TNodeDirectoryPtr nodeDirectory,
        const std::vector<TChunkSpec>& chunkSpecs,
        TNameTablePtr nameTable,
        TColumnFilter columnFilter,
        const TTableSchema& tableSchema,
        IThroughputThrottlerPtr throttler);

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
    const ISchemafulReaderPtr UnderlyingReader_;
    const TKeyColumns KeyColumns_;
    const TNameTablePtr NameTable_;

    i64 RowIndex_ = 0;
    const i64 RowCount_;

    TSchemalessMergingMultiChunkReader(
        ISchemafulReaderPtr underlyingReader,
        TKeyColumns keyColumns,
        TNameTablePtr nameTable,
        i64 rowCount);

    DECLARE_NEW_FRIEND();
};

////////////////////////////////////////////////////////////////////////////////

TSchemalessMergingMultiChunkReader::TSchemalessMergingMultiChunkReader(
    ISchemafulReaderPtr underlyingReader,
    TKeyColumns keyColumns,
    TNameTablePtr nameTable,
    i64 rowCount)
    : UnderlyingReader_(std::move(underlyingReader))
    , KeyColumns_(keyColumns)
    , NameTable_(nameTable)
    , RowCount_(rowCount)
{ }

ISchemalessMultiChunkReaderPtr TSchemalessMergingMultiChunkReader::Create(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    IClientPtr client,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const std::vector<TChunkSpec>& chunkSpecs,
    TNameTablePtr nameTable,
    TColumnFilter columnFilter,
    const TTableSchema& tableSchema,
    IThroughputThrottlerPtr throttler)
{
    for (const auto& column : tableSchema.Columns()) {
        nameTable->RegisterName(column.Name);
    }

    std::vector<TOwningKey> boundaries;
    boundaries.reserve(chunkSpecs.size());

    for (const auto& chunkSpec : chunkSpecs) {
        auto boundaryKeysExt = GetProtoExtension<TBoundaryKeysExt>(chunkSpec.chunk_meta().extensions());
        auto minKey = WidenKey(NYT::FromProto<TOwningKey>(boundaryKeysExt.min()), tableSchema.GetKeyColumnCount());
        boundaries.push_back(minKey);
    }

    auto performanceCounters = New<TChunkReaderPerformanceCounters>();

    auto createReader = [
        config,
        options,
        client,
        blockCache,
        nodeDirectory,
        chunkSpecs,
        nameTable,
        columnFilter,
        tableSchema,
        performanceCounters
    ] (int index) -> IVersionedReaderPtr {
        const auto& chunkSpec = chunkSpecs[index];
        auto chunkId = NYT::FromProto<TChunkId>(chunkSpec.chunk_id());
        auto replicas = NYT::FromProto<TChunkReplica, TChunkReplicaList>(chunkSpec.replicas());

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

        auto chunkReader = CreateReplicationReader(
            config,
            options,
            client,
            nodeDirectory,
            Null,
            chunkId,
            replicas,
            blockCache);

        auto chunkMeta = WaitFor(TCachedVersionedChunkMeta::Load(
            chunkReader,
            tableSchema))
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
            AsyncLastCommittedTimestamp);
    };

    auto rowMerger = New<TSchemafulRowMerger>(
        New<TRowBuffer>(),
        tableSchema.GetKeyColumnCount(),
        columnFilter,
        client->GetConnection()->GetColumnEvaluatorCache()->Find(tableSchema, tableSchema.GetKeyColumnCount()));

    auto reader = CreateSchemafulOverlappingRangeChunkReader(
        std::move(boundaries),
        std::move(rowMerger),
        createReader,
        [] (
            const TUnversionedValue* lhsBegin,
            const TUnversionedValue* lhsEnd,
            const TUnversionedValue* rhsBegin,
            const TUnversionedValue* rhsEnd)
        {
            return CompareRows(lhsBegin, lhsEnd, rhsBegin, rhsEnd);
        });

    i64 rowCount = GetCumulativeRowCount(chunkSpecs);

    return New<TSchemalessMergingMultiChunkReader>(
        std::move(reader),
        tableSchema.GetKeyColumns(),
        nameTable,
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
    TDataStatistics dataStatistics;
    return dataStatistics;
}

std::vector<TChunkId> TSchemalessMergingMultiChunkReader::GetFailedChunkIds() const
{
    return std::vector<TChunkId>();
}

bool TSchemalessMergingMultiChunkReader::IsFetchingCompleted() const
{
    return false;
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
    return NameTable_;
}

TKeyColumns TSchemalessMergingMultiChunkReader::GetKeyColumns() const
{
    return KeyColumns_;
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessMergingMultiChunkReader(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    IClientPtr client,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const std::vector<TChunkSpec>& chunkSpecs,
    TNameTablePtr nameTable,
    TColumnFilter columnFilter,
    const TTableSchema& tableSchema,
    IThroughputThrottlerPtr throttler)
{
    return TSchemalessMergingMultiChunkReader::Create(
        config,
        options,
        client,
        blockCache,
        nodeDirectory,
        chunkSpecs,
        nameTable,
        columnFilter,
        tableSchema,
        throttler);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
