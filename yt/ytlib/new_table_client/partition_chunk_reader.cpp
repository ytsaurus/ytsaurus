#include "stdafx.h"

#include "partition_chunk_reader.h"

#include "chunk_meta_extensions.h"
#include "chunk_reader_base.h"
#include "name_table.h"
#include "schemaless_block_reader.h"
#include "schema.h"

#include <ytlib/chunk_client/config.h>
#include <ytlib/chunk_client/multi_chunk_reader_base.h>

// TKeyColumnsExt
#include <ytlib/table_client/chunk_meta_extensions.h>

#include <core/concurrency/scheduler.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NProto;

using NChunkClient::TReadLimit;
using NNodeTrackerClient::TNodeDirectoryPtr;
using NRpc::IChannelPtr;
using NTableClient::NProto::TKeyColumnsExt;
using NVersionedTableClient::TChunkReaderConfigPtr;

////////////////////////////////////////////////////////////////////////////////

class TPartitionChunkReader
    : public IPartitionChunkReader
    , public TChunkReaderBase
{
public:
    TPartitionChunkReader(
        TChunkReaderConfigPtr config,
        IAsyncReaderPtr underlyingReader,
        TNameTablePtr nameTable,
        const TKeyColumns& keyColumns,
        const TChunkMeta& masterMeta,
        int partitionTag);

    virtual bool Read(
        i64 maxRowCount,
        TValueIterator& valueInserter,
        TRowPointerIterator& rowPointerInserter,
        i64* rowCount) override;

private:
    TNameTablePtr NameTable_;
    TKeyColumns KeyColumns_;

    TChunkMeta ChunkMeta_;
    TBlockMetaExt BlockMetaExt_;

    int PartitionTag_;

    std::vector<int> IdMapping_;

    int CurrentBlockIndex_;
    i64 CurrentRowIndex_;
    std::vector<std::unique_ptr<THorizontalSchemalessBlockReader>> BlockReaders_;

    THorizontalSchemalessBlockReader* BlockReader_;


    virtual std::vector<TSequentialReader::TBlockInfo> GetBlockSequence() override;
    virtual void InitFirstBlock() override;
    virtual void InitNextBlock() override;

    void InitNameTable(TNameTablePtr chunkNameTable);

};

////////////////////////////////////////////////////////////////////////////////

TPartitionChunkReader::TPartitionChunkReader(
    TChunkReaderConfigPtr config,
    IAsyncReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns,
    const TChunkMeta& masterMeta,
    int partitionTag)
    : TChunkReaderBase(
        config,
        TReadLimit(),
        TReadLimit(),
        underlyingReader,
        GetProtoExtension<TMiscExt>(masterMeta.extensions()))
    , NameTable_(nameTable)
    , KeyColumns_(keyColumns)
    , ChunkMeta_(masterMeta)
    , PartitionTag_(partitionTag)
    , CurrentBlockIndex_(0)
    , CurrentRowIndex_(0)
    , BlockReader_(nullptr)
{
    Logger.AddTag(Sprintf("PartitionChunkReader: %p", this));
}

bool TPartitionChunkReader::Read(
    i64 maxRowCount,
    TValueIterator& valueInserter,
    TRowPointerIterator& rowPointerInserter,
    i64* rowCount)
{
    YASSERT(maxRowCount > 0);

    *rowCount = 0;

    if (!ReadyEvent_.IsSet()) {
        // Waiting for the next block.
        return true;
    }

    if (!BlockReader_) {
        // Nothing to read from chunk.
        return false;
    }

    if (BlockEnded_) {
        BlockReader_ = nullptr;
        return OnBlockEnded();
    }

    while (true) {
        ++CurrentRowIndex_;
        ++(*rowCount);

        auto& key = BlockReader_->GetKey();

        std::copy(key.Begin(), key.End(), valueInserter);
        rowPointerInserter = BlockReader_->GetRowPointer();

        if (!BlockReader_->NextRow()) {
            BlockEnded_ = true;
            return true;
        }

        if (*rowCount == maxRowCount) {
            return true;
        }
    }

    return true;
}

std::vector<TSequentialReader::TBlockInfo> TPartitionChunkReader::GetBlockSequence()
{
    YCHECK(ChunkMeta_.version() == ETableChunkFormat::SchemalessHorizontal);

    std::vector<int> extensionTags = {
        TProtoExtensionTag<TBlockMetaExt>::Value,
        TProtoExtensionTag<TNameTableExt>::Value,
        TProtoExtensionTag<TKeyColumnsExt>::Value
    };

    auto errorOrMeta = WaitFor(UnderlyingReader_->AsyncGetChunkMeta(PartitionTag_, &extensionTags));
    THROW_ERROR_EXCEPTION_IF_FAILED(errorOrMeta);

    ChunkMeta_ = errorOrMeta.Value();

    auto chunkNameTable = New<TNameTable>();
    auto nameTableExt = GetProtoExtension<TNameTableExt>(ChunkMeta_.extensions());
    FromProto(&chunkNameTable, nameTableExt);

    InitNameTable(chunkNameTable);

    auto keyColumnsExt = GetProtoExtension<TKeyColumnsExt>(ChunkMeta_.extensions());
    auto chunkKeyColumns = NYT::FromProto<TKeyColumns>(keyColumnsExt);
    YCHECK(chunkKeyColumns == KeyColumns_);

    BlockMetaExt_ = GetProtoExtension<TBlockMetaExt>(ChunkMeta_.extensions());
    std::vector<TSequentialReader::TBlockInfo> blocks;
    for (auto& blockMeta : BlockMetaExt_.entries()) {
        TSequentialReader::TBlockInfo blockInfo;
        blockInfo.Index = blockMeta.block_index();
        blockInfo.Size = blockMeta.block_size();
        blocks.push_back(blockInfo);
    }

    return blocks;
}

void TPartitionChunkReader::InitFirstBlock()
{
    BlockReader_ = new THorizontalSchemalessBlockReader(
            SequentialReader_->GetBlock(),
            BlockMetaExt_.entries(CurrentBlockIndex_),
            IdMapping_,
            KeyColumns_.size());

    BlockReaders_.emplace_back(BlockReader_);
}

void TPartitionChunkReader::InitNextBlock()
{
    ++CurrentBlockIndex_;
    BlockReaders_.emplace_back(new THorizontalSchemalessBlockReader(
        SequentialReader_->GetBlock(),
        BlockMetaExt_.entries(CurrentBlockIndex_),
        IdMapping_,
        KeyColumns_.size()));
}

void TPartitionChunkReader::InitNameTable(TNameTablePtr chunkNameTable)
{
    IdMapping_.resize(chunkNameTable->GetSize());

    for (int chunkNameId = 0; chunkNameId < chunkNameTable->GetSize(); ++chunkNameId) {
        auto& name = chunkNameTable->GetName(chunkNameId);
        YCHECK(NameTable_->GetIdOrRegisterName(name) == chunkNameId);
        IdMapping_[chunkNameId] = chunkNameId;
    }
}

////////////////////////////////////////////////////////////////////////////////

IPartitionChunkReaderPtr CreatePartitionChunkReader(
    TChunkReaderConfigPtr config,
    IAsyncReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns,
    const TChunkMeta& masterMeta,
    int partitionTag)
{
    return New<TPartitionChunkReader>(
        config,
        underlyingReader,
        nameTable,
        keyColumns,
        masterMeta,
        partitionTag);
}

////////////////////////////////////////////////////////////////////////////////

class TPartitionMultiChunkReader
    : public IPartitionMultiChunkReader
    , public TParallelMultiChunkReaderBase
{
public:
    TPartitionMultiChunkReader(
        TMultiChunkReaderConfigPtr config,
        TMultiChunkReaderOptionsPtr options,
        IChannelPtr masterChannel,
        IBlockCachePtr blockCache,
        TNodeDirectoryPtr nodeDirectory,
        const std::vector<TChunkSpec>& chunkSpecs,
        TNameTablePtr nameTable,
        const TKeyColumns& keyColumns);

    virtual bool Read(
        i64 maxRowCount,
        TValueIterator &valueInserter,
        TRowPointerIterator &rowPointerInserter,
        i64* rowCount) override;

private:
    TNameTablePtr NameTable_;
    TKeyColumns KeyColumns_;

    IPartitionChunkReaderPtr CurrentReader_;


    virtual IChunkReaderBasePtr CreateTemplateReader(
        const TChunkSpec& chunkSpec,
        IAsyncReaderPtr asyncReader) override;

    virtual void OnReaderSwitched() override;

};

////////////////////////////////////////////////////////////////////////////////

TPartitionMultiChunkReader::TPartitionMultiChunkReader(
    TMultiChunkReaderConfigPtr config,
    TMultiChunkReaderOptionsPtr options,
    IChannelPtr masterChannel,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const std::vector<TChunkSpec> &chunkSpecs,
    TNameTablePtr nameTable,
    const TKeyColumns &keyColumns)
    : TParallelMultiChunkReaderBase(
          config,
          options,
          masterChannel,
          blockCache,
          nodeDirectory,
          chunkSpecs)
    , NameTable_(nameTable)
    , KeyColumns_(keyColumns)
{ }

bool TPartitionMultiChunkReader::Read(
    i64 maxRowCount,
    TValueIterator& valueInserter,
    TRowPointerIterator& rowPointerInserter,
    i64* rowCount)
{
    YCHECK(ReadyEvent_.IsSet());
    YCHECK(ReadyEvent_.Get().IsOK());

    *rowCount = 0;

    // Nothing to read.
    if (!CurrentReader_)
        return false;

    bool readerFinished = !CurrentReader_->Read(maxRowCount, valueInserter, rowPointerInserter, rowCount);
    if (*rowCount == 0) {
        return TParallelMultiChunkReaderBase::OnEmptyRead(readerFinished);
    } else {
        return true;
    }
}

IChunkReaderBasePtr TPartitionMultiChunkReader::CreateTemplateReader(
    const TChunkSpec& chunkSpec,
    IAsyncReaderPtr asyncReader)
{
    YCHECK(!chunkSpec.has_channel());
    YCHECK(!chunkSpec.has_lower_limit());
    YCHECK(!chunkSpec.has_upper_limit());
    YCHECK(chunkSpec.has_partition_tag());

    TChunkReaderConfigPtr config = Config_;

    return CreatePartitionChunkReader(
        config,
        asyncReader,
        NameTable_,
        KeyColumns_,
        chunkSpec.chunk_meta(),
        chunkSpec.partition_tag());
}

void TPartitionMultiChunkReader::OnReaderSwitched()
{
    CurrentReader_ = dynamic_cast<IPartitionChunkReader*>(CurrentSession_.ChunkReader.Get());
    YCHECK(CurrentReader_);
}

////////////////////////////////////////////////////////////////////////////////

IPartitionMultiChunkReaderPtr CreatePartitionParallelMultiChunkReader(
    TMultiChunkReaderConfigPtr config,
    TMultiChunkReaderOptionsPtr options,
    IChannelPtr masterChannel,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const std::vector<TChunkSpec>& chunkSpecs,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns)
{
    return CreatePartitionParallelMultiChunkReader(
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

} // namespace NVersionedTableClient
} // namespace NYT
