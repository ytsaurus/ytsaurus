#include "stdafx.h"

#include "partition_chunk_reader.h"

#include "chunk_meta_extensions.h"
#include "chunk_reader_base.h"
#include "name_table.h"
#include "schemaless_block_reader.h"
#include "schema.h"

// TKeyColumnsExt
#include <ytlib/table_client/chunk_meta_extensions.h>

#include <core/concurrency/scheduler.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NProto;

using NTableClient::NProto::TKeyColumnsExt;
using NChunkClient::TReadLimit;

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

    virtual bool Read(TValueIterator& valueInserter, TRowPointerIterator& rowPointerInserter) override;

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
    TValueIterator& valueInserter,
    TRowPointerIterator& rowPointerInserter)
{
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
        auto& key = BlockReader_->GetKey();

        std::copy(key.Begin(), key.End(), valueInserter);
        rowPointerInserter = BlockReader_->GetRowPointer();

        if (!BlockReader_->NextRow()) {
            BlockEnded_ = true;
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

////////////////////////////////////////////////////////////////////////////////

IPartitionChunkReaderPtr CreatePartitionChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IAsyncReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns,
    const NChunkClient::NProto::TChunkMeta& masterMeta,
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


////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
