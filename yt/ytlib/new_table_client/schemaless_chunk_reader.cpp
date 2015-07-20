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

#include <ytlib/chunk_client/chunk_spec.h>
#include <ytlib/chunk_client/multi_chunk_reader_base.h>

#include <core/concurrency/scheduler.h>

#include <core/misc/protobuf_helpers.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NProto;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;
using namespace NRpc;

using NChunkClient::TReadLimit;
using NChunkClient::TChannel;

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
        const TReadLimit& lowerLimit,
        const TReadLimit& upperLimit,
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

    const i64 TableRowIndex_;
    const i32 RangeIndex_;

    const TNullable<int> PartitionTag_;

    // Maps chunk name table ids into client id.
    // For filtered out columns maps id to -1.
    std::vector<int> IdMapping_;

    std::unique_ptr<THorizontalSchemalessBlockReader> BlockReader_;

    int CurrentBlockIndex_ = -1;
    i64 CurrentRowIndex_ = -1;
    i64 RowCount_ = 0;

    TChunkMeta ChunkMeta_;
    TBlockMetaExt BlockMetaExt_;

    std::unique_ptr<IRowSampler> RowSampler_;

    virtual std::vector<TSequentialReader::TBlockInfo> GetBlockSequence() override;

    virtual void InitFirstBlock() override;
    virtual void InitNextBlock() override;

    std::vector<TSequentialReader::TBlockInfo> CreateBlockSequence(int beginIndex, int endIndex);
    void DownloadChunkMeta(std::vector<int> extensionTags, TNullable<int> partitionTag = Null);

    std::vector<TSequentialReader::TBlockInfo> GetBlockSequencePartition(); 
    std::vector<TSequentialReader::TBlockInfo> GetBlockSequenceSorted();
    std::vector<TSequentialReader::TBlockInfo> GetBlockSequenceUnsorted(); 

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
    const TReadLimit& lowerLimit,
    const TReadLimit& upperLimit,
    const TColumnFilter& columnFilter,
    i64 tableRowIndex,
    i32 rangeIndex,
    TNullable<int> partitionTag)
    : TChunkReaderBase(
        config, 
        lowerLimit,
        upperLimit,
        underlyingReader, 
        GetProtoExtension<TMiscExt>(masterMeta.extensions()),
        blockCache)
    , Config_(config)
    , NameTable_(nameTable)
    , ChunkNameTable_(New<TNameTable>())
    , ColumnFilter_(columnFilter)
    , KeyColumns_(keyColumns)
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

    LOG_DEBUG("LowerLimit: %v, UpperLimit: %v",
        LowerLimit_,
        UpperLimit_);
}

std::vector<TSequentialReader::TBlockInfo> TSchemalessChunkReader::GetBlockSequence()
{
    YCHECK(ChunkMeta_.version() == static_cast<int>(ETableChunkFormat::SchemalessHorizontal));

    if (PartitionTag_) {
        return GetBlockSequencePartition();
    }

    bool readSorted = LowerLimit_.HasKey() || UpperLimit_.HasKey() || !KeyColumns_.empty();
    if (readSorted) {
        return GetBlockSequenceSorted();
    } else {
        return GetBlockSequenceUnsorted();
    }
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

std::vector<TSequentialReader::TBlockInfo> TSchemalessChunkReader::GetBlockSequenceSorted() 
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

    int beginIndex = std::max(ApplyLowerRowLimit(BlockMetaExt_), ApplyLowerKeyLimit(BlockMetaExt_));
    int endIndex = std::min(ApplyUpperRowLimit(BlockMetaExt_), ApplyUpperKeyLimit(BlockMetaExt_));

    return CreateBlockSequence(beginIndex, endIndex);
}

std::vector<TSequentialReader::TBlockInfo> TSchemalessChunkReader::GetBlockSequencePartition() 
{
    YCHECK(LowerLimit_.IsTrivial());
    YCHECK(UpperLimit_.IsTrivial());

    DownloadChunkMeta(std::vector<int>(), PartitionTag_);
    return CreateBlockSequence(0, BlockMetaExt_.blocks_size());
}

std::vector<TSequentialReader::TBlockInfo> TSchemalessChunkReader::GetBlockSequenceUnsorted() 
{
    DownloadChunkMeta(std::vector<int>());

    return CreateBlockSequence(
        ApplyLowerRowLimit(BlockMetaExt_),
        ApplyUpperRowLimit(BlockMetaExt_));
}

std::vector<TSequentialReader::TBlockInfo> TSchemalessChunkReader::CreateBlockSequence(int beginIndex, int endIndex)
{
    std::vector<TSequentialReader::TBlockInfo> blocks;

    // NB: Happens while reading with partition tag.
    if (BlockMetaExt_.blocks_size() == 0) {
        CurrentRowIndex_ = 0;
        return blocks;
    }

    if (beginIndex >= BlockMetaExt_.blocks_size()) {
        // Take the last block meta.
        auto& blockMeta = *(--BlockMetaExt_.blocks().end());
        CurrentRowIndex_ = blockMeta.chunk_row_count();
        return blocks;
    }

    CurrentBlockIndex_ = beginIndex;
    auto& blockMeta = BlockMetaExt_.blocks(CurrentBlockIndex_);

    CurrentRowIndex_ = blockMeta.chunk_row_count() - blockMeta.row_count();
    for (int index = CurrentBlockIndex_; index < endIndex; ++index) {
        auto& blockMeta = BlockMetaExt_.blocks(index);
        TSequentialReader::TBlockInfo blockInfo;
        blockInfo.Index = blockMeta.block_index();
        blockInfo.UncompressedDataSize = blockMeta.uncompressed_size();
        blocks.push_back(blockInfo);
    }
    return blocks;
}

TDataStatistics TSchemalessChunkReader::GetDataStatistics() const
{
    auto dataStatistics = TChunkReaderBase::GetDataStatistics();
    dataStatistics.set_row_count(RowCount_);
    return dataStatistics;
}

void TSchemalessChunkReader::InitFirstBlock()
{
    CheckBlockUpperLimits(BlockMetaExt_.blocks(CurrentBlockIndex_));

    BlockReader_.reset(new THorizontalSchemalessBlockReader(
        SequentialReader_->GetCurrentBlock(),
        BlockMetaExt_.blocks(CurrentBlockIndex_),
        IdMapping_,
        KeyColumns_.size()));

    if (LowerLimit_.HasRowIndex() && CurrentRowIndex_ < LowerLimit_.GetRowIndex()) {
        YCHECK(BlockReader_->SkipToRowIndex(LowerLimit_.GetRowIndex() - CurrentRowIndex_));
        CurrentRowIndex_ = LowerLimit_.GetRowIndex();
    }

    if (LowerLimit_.HasKey()) {
        auto blockRowIndex = BlockReader_->GetRowIndex();
        YCHECK(BlockReader_->SkipToKey(LowerLimit_.GetKey()));
        CurrentRowIndex_ += BlockReader_->GetRowIndex() - blockRowIndex;
    }
}

void TSchemalessChunkReader::InitNextBlock()
{
    ++CurrentBlockIndex_;

    CheckBlockUpperLimits(BlockMetaExt_.blocks(CurrentBlockIndex_));

    BlockReader_.reset(new THorizontalSchemalessBlockReader(
        SequentialReader_->GetCurrentBlock(),
        BlockMetaExt_.blocks(CurrentBlockIndex_),
        IdMapping_,
        KeyColumns_.size()));
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

    if (BlockEnded_) {
        BlockReader_.reset();
        return OnBlockEnded();
    }

    i64 dataWeight = 0;
    while (rows->size() < rows->capacity() && dataWeight < Config_->MaxDataSizePerRead) {
        if (CheckRowLimit_ && CurrentRowIndex_ >= UpperLimit_.GetRowIndex()) {
            LOG_DEBUG("Upper limit row index reached %v", CurrentRowIndex_);
            return !rows->empty();
        }

        if (CheckKeyLimit_ && CompareRows(BlockReader_->GetKey(), UpperLimit_.GetKey()) >= 0) {
            LOG_DEBUG("Upper limit key reached %v", BlockReader_->GetKey());
            return !rows->empty();
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
                lowerLimit,
                upperLimit,
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
        IChannelPtr masterChannel,
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
    IChannelPtr masterChannel,
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
        masterChannel,
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
    IChannelPtr masterChannel,
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
        masterChannel,
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
    IChannelPtr masterChannel,
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
        masterChannel,
        blockCache,
        nodeDirectory,
        chunkSpecs,
        nameTable,
        columnFilter,
        keyColumns,
        throttler);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
