#include "stdafx.h"

#include "partition_chunk_reader.h"

#include "chunk_meta_extensions.h"

#include "name_table.h"
#include "schema.h"

#include <ytlib/chunk_client/config.h>

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

TPartitionChunkReader::TPartitionChunkReader(
    TChunkReaderConfigPtr config,
    IChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    IBlockCachePtr uncompressedBlockCache,
    const TKeyColumns& keyColumns,
    const TChunkMeta& masterMeta,
    int partitionTag)
    : TChunkReaderBase(
        config,
        TReadLimit(),
        TReadLimit(),
        underlyingReader,
        GetProtoExtension<TMiscExt>(masterMeta.extensions()),
        uncompressedBlockCache)
    , NameTable_(nameTable)
    , KeyColumns_(keyColumns)
    , ChunkMeta_(masterMeta)
    , PartitionTag_(partitionTag)
    , CurrentBlockIndex_(0)
    , RowCount_(0)
    , BlockReader_(nullptr)
{
    Logger.AddTag("PartitionChunkReader: %p", this);
}

std::vector<TSequentialReader::TBlockInfo> TPartitionChunkReader::GetBlockSequence()
{
    YCHECK(ChunkMeta_.version() == ETableChunkFormat::SchemalessHorizontal);

    std::vector<int> extensionTags = {
        TProtoExtensionTag<TBlockMetaExt>::Value,
        TProtoExtensionTag<TNameTableExt>::Value,
        TProtoExtensionTag<TKeyColumnsExt>::Value
    };

    auto errorOrMeta = WaitFor(UnderlyingReader_->GetMeta(PartitionTag_, &extensionTags));
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
    for (auto& blockMeta : BlockMetaExt_.blocks()) {
        TSequentialReader::TBlockInfo blockInfo;
        blockInfo.Index = blockMeta.block_index();
        blockInfo.UncompressedDataSize = blockMeta.uncompressed_size();
        blocks.push_back(blockInfo);
    }

    return blocks;
}

TDataStatistics TPartitionChunkReader::GetDataStatistics() const
{
    auto dataStatistics = TChunkReaderBase::GetDataStatistics();
    dataStatistics.set_row_count(RowCount_);
    return dataStatistics;
}

void TPartitionChunkReader::InitFirstBlock()
{
    BlockReader_ = new THorizontalSchemalessBlockReader(
            SequentialReader_->GetCurrentBlock(),
            BlockMetaExt_.blocks(CurrentBlockIndex_),
            IdMapping_,
            KeyColumns_.size());

    BlockReaders_.emplace_back(BlockReader_);
}

void TPartitionChunkReader::InitNextBlock()
{
    ++CurrentBlockIndex_;
    BlockReaders_.emplace_back(new THorizontalSchemalessBlockReader(
        SequentialReader_->GetCurrentBlock(),
        BlockMetaExt_.blocks(CurrentBlockIndex_),
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

TPartitionMultiChunkReader::TPartitionMultiChunkReader(
    TMultiChunkReaderConfigPtr config,
    TMultiChunkReaderOptionsPtr options,
    IChannelPtr masterChannel,
    IBlockCachePtr compressedBlockCache,
    IBlockCachePtr uncompressedBlockCache,
    TNodeDirectoryPtr nodeDirectory,
    const std::vector<TChunkSpec> &chunkSpecs,
    TNameTablePtr nameTable,
    const TKeyColumns &keyColumns)
    : TParallelMultiChunkReaderBase(
          config,
          options,
          masterChannel,
          compressedBlockCache,
          nodeDirectory,
          chunkSpecs)
    , UncompressedBlockCache_(uncompressedBlockCache)
    , NameTable_(nameTable)
    , KeyColumns_(keyColumns)
{ }

IChunkReaderBasePtr TPartitionMultiChunkReader::CreateTemplateReader(
    const TChunkSpec& chunkSpec,
    IChunkReaderPtr asyncReader)
{
    YCHECK(!chunkSpec.has_channel());
    YCHECK(!chunkSpec.has_lower_limit());
    YCHECK(!chunkSpec.has_upper_limit());
    YCHECK(chunkSpec.has_partition_tag());

    TChunkReaderConfigPtr config = Config_;

    return New<TPartitionChunkReader>(
        config,
        asyncReader,
        NameTable_,
        UncompressedBlockCache_,
        KeyColumns_,
        chunkSpec.chunk_meta(),
        chunkSpec.partition_tag());
}

void TPartitionMultiChunkReader::OnReaderSwitched()
{
    CurrentReader_ = dynamic_cast<TPartitionChunkReader*>(CurrentSession_.ChunkReader.Get());
    YCHECK(CurrentReader_);
}

TNameTablePtr TPartitionMultiChunkReader::GetNameTable() const
{
    return NameTable_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
