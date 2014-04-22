#include "stdafx.h"

#include "schemaless_chunk_reader.h"

#include "chunk_reader_base.h"
#include "config.h"
#include "name_table.h"
#include "private.h"
#include "schema.h"
#include "schemaless_block_reader.h"

#include <ytlib/chunk_client/multi_chunk_reader_base.h>

// TKeyColumnsExt
#include <ytlib/table_client/chunk_meta_extensions.h>

#include <core/concurrency/scheduler.h>

#include <core/misc/protobuf_helpers.h>


namespace NYT {
namespace NVersionedTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NProto;

using NChunkClient::TReadLimit;
using NRpc::IChannelPtr;

////////////////////////////////////////////////////////////////////////////////

class TSchemalessChunkReader
    : public ISchemalessChunkReader
    , public TChunkReaderBase
{
public:
    TSchemalessChunkReader(
        TChunkReaderConfigPtr config,
        TNameTablePtr nameTable,
        const TChunkSpec& chunkSpec,
        const TKeyColumns& keyColumns,
        IAsyncReaderPtr underlyingReader);

    virtual bool Read(std::vector<TUnversionedRow>* rows) override;

private:
    TChunkSpec ChunkSpec_;

    TNameTablePtr NameTable_;
    TNameTablePtr ChunkNameTable_;

    TColumnFilter ColumnFilter_;
    TKeyColumns KeyColumns_;

    // Maps chunk name table ids into client id.
    // For filtered out columns maps id to -1.
    std::vector<int> IdMapping_;

    std::unique_ptr<THorizontalSchemalessBlockReader> BlockReader_;

    int CurrentBlockIndex_;
    i64 CurrentRowIndex_;
    i64 RowCount_;

    TChunkMeta ChunkMeta_;
    TBlockMetaExt BlockMetaExt_;

    virtual std::vector<NChunkClient::TSequentialReader::TBlockInfo> GetBlockSequence() override;

    virtual void InitFirstBlock() override;
    virtual void InitNextBlock() override;

    std::vector<TSequentialReader::TBlockInfo> CreateBlockSequence(int beginIndex, int endIndex);
    void DownloadChunkMeta(std::vector<int> extensionTags, TNullable<int> partitionTag = Null);

    std::vector<TSequentialReader::TBlockInfo> GetBlockSequencePartition(); 
    std::vector<TSequentialReader::TBlockInfo> GetBlockSequenceSorted();
    std::vector<TSequentialReader::TBlockInfo> GetBlockSequenceUnsorted(); 

};

////////////////////////////////////////////////////////////////////////////////

TSchemalessChunkReader::TSchemalessChunkReader(
    TChunkReaderConfigPtr config,
    TNameTablePtr nameTable,
    const TChunkSpec& chunkSpec,
    const TKeyColumns& keyColumns,
    IAsyncReaderPtr underlyingReader)
    : TChunkReaderBase(
        config, 
        TReadLimit(chunkSpec.lower_limit()), 
        TReadLimit(chunkSpec.upper_limit()),
        underlyingReader, 
        GetProtoExtension<TMiscExt>(chunkSpec.chunk_meta().extensions()))
    , ChunkSpec_(chunkSpec)
    , NameTable_(nameTable)
    , ChunkNameTable_(New<TNameTable>())
    , ColumnFilter_(CreateColumnFilter(chunkSpec.channel(), nameTable))
    , KeyColumns_(keyColumns)
{ }

std::vector<TSequentialReader::TBlockInfo> TSchemalessChunkReader::GetBlockSequence() 
{
    YCHECK(ChunkSpec_.chunk_meta().version() == ETableChunkFormat::SchemalessHorizontal);

    if (ChunkSpec_.has_partition_tag()) {
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

    auto errorOrMeta = WaitFor(UnderlyingReader_->AsyncGetChunkMeta(partitionTag, &extensionTags));
    THROW_ERROR_EXCEPTION_IF_FAILED(errorOrMeta);

    ChunkMeta_ = errorOrMeta.Value();
    BlockMetaExt_ = GetProtoExtension<TBlockMetaExt>(ChunkMeta_.extensions());

    FromProto(&ChunkNameTable_, GetProtoExtension<TNameTableExt>(ChunkMeta_.extensions()));

    IdMapping_.resize(ChunkNameTable_->GetSize(), -1);
    auto setMappingForId = [this] (int id) {
        auto name = NameTable_->GetName(id);
        auto chunkNameId = ChunkNameTable_->GetId(name);
        IdMapping_[chunkNameId] = id;
    };

    if (ColumnFilter_.All) {
        for (int id = 0; id < NameTable_->GetSize(); ++id) {
            setMappingForId(id);
        }
    } else {
        for (auto id : ColumnFilter_.Indexes) {
            setMappingForId(id);
        }
    }
}

std::vector<TSequentialReader::TBlockInfo> TSchemalessChunkReader::GetBlockSequenceSorted() 
{
    if (!Misc_.sorted()) {
        THROW_ERROR_EXCEPTION("Requested sorted read for unsorted chunk");
    }

    std::vector<int> extensionTags = {
        TProtoExtensionTag<TBlockIndexExt>::Value,
        TProtoExtensionTag<NTableClient::NProto::TKeyColumnsExt>::Value
    };

    DownloadChunkMeta(extensionTags);

    auto keyColumnsExt = GetProtoExtension<NTableClient::NProto::TKeyColumnsExt>(ChunkMeta_.extensions());
    TKeyColumns chunkKeyColumns = NYT::FromProto<TKeyColumns>(keyColumnsExt);

    ValidateKeyColumns(KeyColumns_, chunkKeyColumns);

    if (KeyColumns_.empty()) {
        KeyColumns_ = chunkKeyColumns;
    }

    auto blockIndexExt = GetProtoExtension<TBlockIndexExt>(ChunkMeta_.extensions());

    int beginIndex = std::max(GetBeginBlockIndex(BlockMetaExt_), GetBeginBlockIndex(blockIndexExt));
    int endIndex = std::min(GetEndBlockIndex(BlockMetaExt_), GetEndBlockIndex(blockIndexExt));

    return CreateBlockSequence(beginIndex, endIndex);
}

std::vector<TSequentialReader::TBlockInfo> TSchemalessChunkReader::GetBlockSequencePartition() 
{
    YCHECK(LowerLimit_.IsTrivial());
    YCHECK(UpperLimit_.IsTrivial());
    YUNIMPLEMENTED();
}

std::vector<TSequentialReader::TBlockInfo> TSchemalessChunkReader::GetBlockSequenceUnsorted() 
{
    DownloadChunkMeta(std::vector<int>());

    return CreateBlockSequence(
        GetBeginBlockIndex(BlockMetaExt_), 
        GetEndBlockIndex(BlockMetaExt_));
}

std::vector<TSequentialReader::TBlockInfo> TSchemalessChunkReader::CreateBlockSequence(int beginIndex, int endIndex)
{
    CurrentBlockIndex_ = beginIndex;
    auto& blockMeta = BlockMetaExt_.entries(CurrentBlockIndex_);

    CurrentRowIndex_ = blockMeta.chunk_row_count() - blockMeta.row_count();

    std::vector<TSequentialReader::TBlockInfo> blocks;
    for (int index = CurrentBlockIndex_; index < endIndex; ++index) {
        TSequentialReader::TBlockInfo blockInfo;
        blockInfo.Index = index;
        blockInfo.Size = BlockMetaExt_.entries(index).block_size();
        blocks.push_back(blockInfo);
    }
    return blocks;
}

void TSchemalessChunkReader::InitFirstBlock()
{
    BlockReader_.reset(new THorizontalSchemalessBlockReader(
        SequentialReader_->GetBlock(),
        BlockMetaExt_.entries(CurrentBlockIndex_),
        IdMapping_,
        KeyColumns_.size()));

    if (LowerLimit_.HasRowIndex()) {
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
    BlockReader_.reset(new THorizontalSchemalessBlockReader(
        SequentialReader_->GetBlock(),
        BlockMetaExt_.entries(CurrentBlockIndex_),
        IdMapping_,
        KeyColumns_.size()));
}

bool TSchemalessChunkReader::Read(std::vector<TUnversionedRow>* rows)
{
    YCHECK(rows->capacity() > 0);

    MemoryPool_.Clear();
    rows->clear();

    if (!ReadyEvent_.IsSet()) {
        // Waiting for the next block.
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

    while (rows->size() < rows->capacity()) {
        ++CurrentRowIndex_;
        if (UpperLimit_.HasRowIndex() && CurrentRowIndex_ == UpperLimit_.GetRowIndex()) {
            return false;
        }

        if (UpperLimit_.HasKey() && CompareRows(BlockReader_->GetKey(), UpperLimit_.GetKey()) >= 0) {
            return false;
        }

        ++RowCount_;
        rows->push_back(BlockReader_->GetRow(&MemoryPool_));
        
        if (!BlockReader_->NextRow()) {
            BlockEnded_ = true;
            return true;
        }
    }
    YUNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessChunkReaderPtr CreateSchemalessChunkReader(
    TChunkReaderConfigPtr config, 
    const TChunkSpec& chunkSpec, 
    IAsyncReaderPtr asyncReader,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns)
{
    return New<TSchemalessChunkReader>(config, nameTable, chunkSpec, keyColumns, asyncReader);
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
        const TKeyColumns& keyColumns);

    virtual bool Read(std::vector<TUnversionedRow>* rows);

private:
    TTableReaderConfigPtr Config_;
    TNameTablePtr NameTable_;
    TKeyColumns KeyColumns_;

    ISchemalessChunkReaderPtr CurrentReader_;

    using TBase::ReadyEvent_;
    using TBase::CurrentSession_;


    virtual IChunkReaderBasePtr CreateTemplateReader(const TChunkSpec& chunkSpec, IAsyncReaderPtr asyncReader) override;
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
    const TKeyColumns& keyColumns)
    : TBase(config, options, masterChannel, blockCache, nodeDirectory, chunkSpecs)
    , Config_(config)
    , NameTable_(nameTable)
    , KeyColumns_(keyColumns)
{ }

template <class TBase>
bool TSchemalessMultiChunkReader<TBase>::Read(std::vector<TUnversionedRow>* rows)
{
    YCHECK(ReadyEvent_.IsSet());
    YCHECK(ReadyEvent_.Get().IsOK());
    YCHECK(CurrentReader_);

    bool readerFinished = !CurrentReader_->Read(rows);
    if (rows->empty()) {
        return TBase::OnEmptyRead(readerFinished);
    } else {
        return true;
    }
}

template <class TBase>
IChunkReaderBasePtr TSchemalessMultiChunkReader<TBase>::CreateTemplateReader(
    const TChunkSpec& chunkSpec,
    IAsyncReaderPtr asyncReader)
{
    return CreateSchemalessChunkReader(Config_, chunkSpec, asyncReader, NameTable_,KeyColumns_);
}

template <class TBase>
void TSchemalessMultiChunkReader<TBase>::OnReaderSwitched()
{
    CurrentReader_ = dynamic_cast<ISchemalessChunkReader*>(CurrentSession_.ChunkReader.Get());
    YCHECK(CurrentReader_);
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
    const TKeyColumns& keyColumns)
{
    return New<TSchemalessMultiChunkReader<TSequentialMultiChunkReaderBase>>(
        config,
        options,
        masterChannel,
        blockCache,
        nodeDirectory,
        chunkSpecs,
        nameTable,
        keyColumns);
}

////////////////////////////////////////////////////////////////////////////////

/*
IPartitionChunkReaderPtr CreatePartitionChunkReader()
{
    // How does it look like.
}
*/

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
