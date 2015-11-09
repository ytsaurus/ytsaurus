#include "stdafx.h"

#include "schemaless_chunk_reader.h"
#include "chunk_reader_base.h"
#include "config.h"
#include "legacy_table_chunk_reader.h"
#include "name_table.h"
#include "private.h"
#include "row_sampler.h"
#include "schema.h"
#include "schemaless_block_reader.h"
#include "row_merger.h"
#include "cached_versioned_chunk_meta.h"
#include "row_buffer.h"
#include "versioned_row.h"
#include "versioned_reader.h"
#include "versioned_chunk_reader.h"
#include "schemaful_reader.h"
#include "schemaful_overlapping_chunk_reader.h"

#include <ytlib/api/client.h>
#include <ytlib/api/connection.h>

#include <ytlib/chunk_client/chunk_spec.h>
#include <ytlib/chunk_client/multi_chunk_reader_base.h>
#include <ytlib/chunk_client/replication_reader.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/query_client/column_evaluator.h>

#include <core/concurrency/scheduler.h>

#include <core/misc/protobuf_helpers.h>

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
        TChunkReaderConfigPtr config,
        IChunkReaderPtr underlyingReader,
        TNameTablePtr nameTable,
        IBlockCachePtr blockCache,
        const TKeyColumns& keyColumns,
        const TChunkMeta& masterMeta,
        std::vector<TReadRange> readRanges,
        const TColumnFilter& columnFilter,
        i64 tableRowIndex,
        i32 rangeIndex,
        TNullable<int> partitionTag);

    virtual bool Read(std::vector<TUnversionedRow>* rows) override;

    virtual TDataStatistics GetDataStatistics() const override;

    virtual TNameTablePtr GetNameTable() const override;

    virtual i64 GetTableRowIndex() const override;

    virtual i32 GetRangeIndex() const override;

    virtual TKeyColumns GetKeyColumns() const override;

private:
    TChunkReaderConfigPtr Config_;
    const TNameTablePtr NameTable_;
    TNameTablePtr ChunkNameTable_;

    const TColumnFilter ColumnFilter_;
    TKeyColumns KeyColumns_;

    std::vector<TReadRange> ReadRanges_;

    const i64 TableRowIndex_;
    const i32 RangeIndex_;

    const TNullable<int> PartitionTag_;

    // Maps chunk name table ids into client id.
    // For filtered out columns maps id to -1.
    std::vector<int> IdMapping_;

    std::unique_ptr<THorizontalSchemalessBlockReader> BlockReader_;

    int CurrentBlockIndex_ = 0;
    i64 CurrentRowIndex_ = -1;
    int CurrentRangeIndex_ = 0;
    i64 RowCount_ = 0;

    bool RangeEnded_ = false;

    TChunkMeta ChunkMeta_;
    TBlockMetaExt BlockMetaExt_;

    std::unique_ptr<IRowSampler> RowSampler_;
    std::vector<int> BlockIndexes_;

    virtual std::vector<TSequentialReader::TBlockInfo> GetBlockSequence() override;

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

DECLARE_REFCOUNTED_CLASS(TSchemalessChunkReader)
DEFINE_REFCOUNTED_TYPE(TSchemalessChunkReader)

////////////////////////////////////////////////////////////////////////////////

TSchemalessChunkReader::TSchemalessChunkReader(
    TChunkReaderConfigPtr config,
    IChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    IBlockCachePtr blockCache,
    const TKeyColumns& keyColumns,
    const TChunkMeta& masterMeta,
    std::vector<TReadRange> readRanges,
    const TColumnFilter& columnFilter,
    i64 tableRowIndex,
    i32 rangeIndex,
    TNullable<int> partitionTag)
    : TChunkReaderBase(
        config,
        underlyingReader,
        GetProtoExtension<TMiscExt>(masterMeta.extensions()),
        blockCache)
    , Config_(config)
    , NameTable_(nameTable)
    , ChunkNameTable_(New<TNameTable>())
    , ColumnFilter_(columnFilter)
    , KeyColumns_(keyColumns)
    , ReadRanges_(std::move(readRanges))
    , TableRowIndex_(tableRowIndex)
    , RangeIndex_(rangeIndex)
    , PartitionTag_(partitionTag)
    , ChunkMeta_(masterMeta)
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
}

std::vector<TSequentialReader::TBlockInfo> TSchemalessChunkReader::GetBlockSequence()
{
    YCHECK(ChunkMeta_.version() == static_cast<int>(ETableChunkFormat::SchemalessHorizontal));
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
    return blocks;
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
    if (!Misc_.sorted()) {
        THROW_ERROR_EXCEPTION("Requested sorted read for unsorted chunk");
    }

    std::vector<int> extensionTags = {
        TProtoExtensionTag<TKeyColumnsExt>::Value,
    };

    DownloadChunkMeta(extensionTags);

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
        KeyColumns_.size()));

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

        ++CurrentRowIndex_;
        if (!RowSampler_ || RowSampler_->ShouldTakeRow(GetTableRowIndex())) {
            rows->push_back(BlockReader_->GetRow(&MemoryPool_));
            dataWeight += GetDataWeight(rows->back());
            ++RowCount_;
        }

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
    return TableRowIndex_ + CurrentRowIndex_;
}

i32 TSchemalessChunkReader::GetRangeIndex() const
{
    return RangeIndex_;
}

TKeyColumns TSchemalessChunkReader::GetKeyColumns() const
{
    return KeyColumns_;
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessChunkReaderPtr CreateSchemalessChunkReader(
    TChunkReaderConfigPtr config,
    IChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    IBlockCachePtr blockCache,
    const TKeyColumns& keyColumns,
    const TChunkMeta& masterMeta,
    const TReadLimit& lowerLimit,
    const TReadLimit& upperLimit,
    const TColumnFilter& columnFilter,
    i64 tableRowIndex,
    i32 rangeIndex,
    TNullable<int> partitionTag)
{
    auto type = EChunkType(masterMeta.type());
    YCHECK(type == EChunkType::Table);

    auto formatVersion = ETableChunkFormat(masterMeta.version());

    switch (formatVersion) {
        case ETableChunkFormat::SchemalessHorizontal:
            return New<TSchemalessChunkReader>(
                config,
                underlyingReader,
                nameTable,
                blockCache,
                keyColumns,
                masterMeta,
                std::vector<TReadRange>{{lowerLimit, upperLimit}},
                columnFilter,
                tableRowIndex,
                rangeIndex,
                partitionTag);

        case ETableChunkFormat::Old:
            YCHECK(!partitionTag);
            return New<TLegacyTableChunkReader>(
                config,
                columnFilter,
                nameTable,
                keyColumns,
                underlyingReader,
                blockCache,
                lowerLimit,
                upperLimit,
                tableRowIndex,
                rangeIndex);

        default:
            YUNREACHABLE();
    }
}

ISchemalessChunkReaderPtr CreateSchemalessChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    NChunkClient::IBlockCachePtr blockCache,
    const TKeyColumns& keyColumns,
    const NChunkClient::NProto::TChunkMeta& masterMeta,
    std::vector<TReadRange> readRanges,
    const TColumnFilter& columnFilter,
    i64 tableRowIndex,
    i32 rangeIndex,
    TNullable<int> partitionTag)
{
    ValidateReadRanges(readRanges);

    auto type = EChunkType(masterMeta.type());
    YCHECK(type == EChunkType::Table);

    auto formatVersion = ETableChunkFormat(masterMeta.version());

    switch (formatVersion) {
        case ETableChunkFormat::SchemalessHorizontal:
            return New<TSchemalessChunkReader>(
                config,
                underlyingReader,
                nameTable,
                blockCache,
                keyColumns,
                masterMeta,
                std::move(readRanges),
                columnFilter,
                tableRowIndex,
                rangeIndex,
                partitionTag);
        default:
            THROW_ERROR_EXCEPTION("Multiple read ranges are not supported for chunk format %Qlv", formatVersion);
    }
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
        TMultiChunkReaderOptionsPtr options,
        IClientPtr client,
        IBlockCachePtr blockCache,
        TNodeDirectoryPtr nodeDirectory,
        const std::vector<TChunkSpec>& chunkSpecs,
        TNameTablePtr nameTable,
        TColumnFilter columnFilter,
        const TKeyColumns& keyColumns,
        IThroughputThrottlerPtr throttler);

    virtual bool Read(std::vector<TUnversionedRow>* rows) override;

    virtual int GetTableIndex() const override;

    virtual i64 GetSessionRowIndex() const override;

    virtual i64 GetTotalRowCount() const override;

    virtual TNameTablePtr GetNameTable() const override;

    virtual TKeyColumns GetKeyColumns() const override;

    virtual i64 GetTableRowIndex() const override;

    virtual i32 GetRangeIndex() const override;

private:
    const TTableReaderConfigPtr Config_;
    const TNameTablePtr NameTable_;
    const TColumnFilter ColumnFilter_;
    const TKeyColumns KeyColumns_;
    const IBlockCachePtr BlockCache_;

    ISchemalessChunkReaderPtr CurrentReader_;
    std::atomic<i64> RowIndex_ = {0};
    std::atomic<i64> RowCount_ = {-1};

    using TBase::ReadyEvent_;
    using TBase::CurrentSession_;
    using TBase::Chunks_;

    virtual IChunkReaderBasePtr CreateTemplateReader(const TChunkSpec& chunkSpec, IChunkReaderPtr asyncReader) override;
    virtual void OnReaderSwitched() override;

};

////////////////////////////////////////////////////////////////////////////////

template<class TBase>
TSchemalessMultiChunkReader<TBase>::TSchemalessMultiChunkReader(
    TTableReaderConfigPtr config,
    TMultiChunkReaderOptionsPtr options,
    IClientPtr client,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const std::vector<TChunkSpec>& chunkSpecs,
    TNameTablePtr nameTable,
    TColumnFilter columnFilter,
    const TKeyColumns& keyColumns,
    IThroughputThrottlerPtr throttler)
    : TBase(
        config,
        options,
        client,
        blockCache,
        nodeDirectory,
        chunkSpecs,
        throttler)
    , Config_(config)
    , NameTable_(nameTable)
    , ColumnFilter_(std::move(columnFilter))
    , KeyColumns_(keyColumns)
    , BlockCache_(blockCache)
    , RowCount_(GetCumulativeRowCount(chunkSpecs))
{ }

template <class TBase>
bool TSchemalessMultiChunkReader<TBase>::Read(std::vector<TUnversionedRow>* rows)
{
    YCHECK(ReadyEvent_.IsSet());
    YCHECK(ReadyEvent_.Get().IsOK());

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
IChunkReaderBasePtr TSchemalessMultiChunkReader<TBase>::CreateTemplateReader(
    const TChunkSpec& chunkSpec,
    IChunkReaderPtr asyncReader)
{
    using NYT::FromProto;

    auto channel = chunkSpec.has_channel()
        ? FromProto<TChannel>(chunkSpec.channel())
        : TChannel::Universal();

    return CreateSchemalessChunkReader(
        Config_,
        asyncReader,
        NameTable_,
        BlockCache_,
        KeyColumns_,
        chunkSpec.chunk_meta(),
        chunkSpec.has_lower_limit() ? TReadLimit(chunkSpec.lower_limit()) : TReadLimit(),
        chunkSpec.has_upper_limit() ? TReadLimit(chunkSpec.upper_limit()) : TReadLimit(),
        ColumnFilter_.All ? CreateColumnFilter(channel, NameTable_) : ColumnFilter_,
        chunkSpec.table_row_index(),
        chunkSpec.range_index(),
        chunkSpec.has_partition_tag() ? MakeNullable(chunkSpec.partition_tag()) : Null);
}

template <class TBase>
void TSchemalessMultiChunkReader<TBase>::OnReaderSwitched()
{
    CurrentReader_ = dynamic_cast<ISchemalessChunkReader*>(CurrentSession_.ChunkReader.Get());
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
i32 TSchemalessMultiChunkReader<TBase>::GetRangeIndex() const
{
    return CurrentReader_ ? CurrentReader_->GetRangeIndex() : 0;
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

template <class TBase>
int TSchemalessMultiChunkReader<TBase>::GetTableIndex() const
{
    return Chunks_[CurrentSession_.ChunkIndex].Spec.table_index();
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessSequentialMultiChunkReader(
    TTableReaderConfigPtr config,
    TMultiChunkReaderOptionsPtr options,
    IClientPtr client,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const std::vector<TChunkSpec>& chunkSpecs,
    TNameTablePtr nameTable,
    TColumnFilter columnFilter,
    const TKeyColumns& keyColumns,
    IThroughputThrottlerPtr throttler)
{
    return New<TSchemalessMultiChunkReader<TSequentialMultiChunkReaderBase>>(
        config,
        options,
        client,
        blockCache,
        nodeDirectory,
        chunkSpecs,
        nameTable,
        columnFilter,
        keyColumns,
        throttler);
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessParallelMultiChunkReader(
    TTableReaderConfigPtr config,
    TMultiChunkReaderOptionsPtr options,
    IClientPtr client,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const std::vector<TChunkSpec>& chunkSpecs,
    TNameTablePtr nameTable,
    TColumnFilter columnFilter,
    const TKeyColumns& keyColumns,
    IThroughputThrottlerPtr throttler)
{
    return New<TSchemalessMultiChunkReader<TParallelMultiChunkReaderBase>>(
        config,
        options,
        client,
        blockCache,
        nodeDirectory,
        chunkSpecs,
        nameTable,
        columnFilter,
        keyColumns,
        throttler);
}

////////////////////////////////////////////////////////////////////////////////

class TSchemalessMergingMultiChunkReader
    : public ISchemalessMultiChunkReader
{
public:
    static ISchemalessMultiChunkReaderPtr Create(
        TTableReaderConfigPtr config,
        TMultiChunkReaderOptionsPtr options,
        IClientPtr client,
        IBlockCachePtr blockCache,
        TNodeDirectoryPtr nodeDirectory,
        const std::vector<TChunkSpec>& chunkSpecs,
        TNameTablePtr nameTable,
        TColumnFilter columnFilter,
        const TTableSchema& tableSchema,
        const TKeyColumns& keyColumns,
        IThroughputThrottlerPtr throttler);

    virtual TFuture<void> Open() override;
    virtual TFuture<void> GetReadyEvent() override;
    virtual bool Read(std::vector<TUnversionedRow>* rows) override;
    virtual TDataStatistics GetDataStatistics() const override;
    virtual std::vector<TChunkId> GetFailedChunkIds() const override;
    virtual bool IsFetchingCompleted() const override;
    virtual int GetTableIndex() const override;
    virtual i64 GetSessionRowIndex() const override;
    virtual i64 GetTotalRowCount() const override;
    virtual TNameTablePtr GetNameTable() const override;
    virtual TKeyColumns GetKeyColumns() const override;
    virtual i64 GetTableRowIndex() const override;
    virtual i32 GetRangeIndex() const override;

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
    TMultiChunkReaderOptionsPtr options,
    IClientPtr client,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const std::vector<TChunkSpec>& chunkSpecs,
    TNameTablePtr nameTable,
    TColumnFilter columnFilter,
    const TTableSchema& tableSchema,
    const TKeyColumns& keyColumns,
    IThroughputThrottlerPtr throttler)
{
    for (const auto& column : tableSchema.Columns()) {
        nameTable->RegisterName(column.Name);
    }

    std::vector<TOwningKey> boundaries;
    boundaries.reserve(chunkSpecs.size());

    for (const auto& chunkSpec : chunkSpecs) {
        auto boundaryKeysExt = GetProtoExtension<TBoundaryKeysExt>(chunkSpec.chunk_meta().extensions());
        auto minKey = WidenKey(NYT::FromProto<TOwningKey>(boundaryKeysExt.min()), keyColumns.size());
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
        keyColumns,
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
            tableSchema,
            keyColumns))
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
        keyColumns.size(),
        columnFilter,
        client->GetConnection()->GetColumnEvaluatorCache()->Find(tableSchema, keyColumns.size()));

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
        keyColumns,
        nameTable,
        rowCount);
}

TFuture<void> TSchemalessMergingMultiChunkReader::Open()
{
    return VoidFuture;
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

i32 TSchemalessMergingMultiChunkReader::GetRangeIndex() const
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

int TSchemalessMergingMultiChunkReader::GetTableIndex() const
{
    return 0;
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessMergingMultiChunkReader(
    TTableReaderConfigPtr config,
    TMultiChunkReaderOptionsPtr options,
    IClientPtr client,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const std::vector<TChunkSpec>& chunkSpecs,
    TNameTablePtr nameTable,
    TColumnFilter columnFilter,
    const TTableSchema& tableSchema,
    const TKeyColumns& keyColumns,
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
        keyColumns,
        throttler);
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
