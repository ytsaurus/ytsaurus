#pragma once

#include "public.h"

#include "chunk_reader_base.h"
#include "schemaless_block_reader.h"

#include <ytlib/chunk_client/multi_chunk_reader_base.h>

#include <ytlib/node_tracker_client/public.h>

#include <ytlib/transaction_client/public.h>

#include <core/rpc/public.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

class TPartitionChunkReader
    : public TChunkReaderBase
{
public:
    TPartitionChunkReader(
        TChunkReaderConfigPtr config,
        NChunkClient::IChunkReaderPtr underlyingReader,
        TNameTablePtr nameTable,
        NChunkClient::IBlockCachePtr uncompressedBlockCache,
        const TKeyColumns& keyColumns,
        const NChunkClient::NProto::TChunkMeta& masterMeta,
        int partitionTag);

    template <class TValueInsertIterator, class TRowPointerInsertIterator>
    bool Read(
        TValueInsertIterator& valueInserter,
        TRowPointerInsertIterator& rowPointerInserter,
        i64* rowCount);

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override;

private:
    TNameTablePtr NameTable_;
    TKeyColumns KeyColumns_;

    NChunkClient::NProto::TChunkMeta ChunkMeta_;
    NProto::TBlockMetaExt BlockMetaExt_;

    int PartitionTag_;

    std::vector<int> IdMapping_;

    int CurrentBlockIndex_;
    i64 RowCount_;
    std::vector<std::unique_ptr<THorizontalSchemalessBlockReader>> BlockReaders_;

    THorizontalSchemalessBlockReader* BlockReader_;


    virtual std::vector<NChunkClient::TSequentialReader::TBlockInfo> GetBlockSequence() override;
    virtual void InitFirstBlock() override;
    virtual void InitNextBlock() override;

    void InitNameTable(TNameTablePtr chunkNameTable);

};

DEFINE_REFCOUNTED_TYPE(TPartitionChunkReader)

////////////////////////////////////////////////////////////////////////////////

//ToDo(psushin): move to inl.

template <class TValueInsertIterator, class TRowPointerInsertIterator>
bool TPartitionChunkReader::Read(
    TValueInsertIterator& valueInserter,
    TRowPointerInsertIterator& rowPointerInserter,
    i64* rowCount)
{
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
        ++RowCount_;
        ++(*rowCount);

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

////////////////////////////////////////////////////////////////////////////////

class TPartitionMultiChunkReader
    : public NChunkClient::TParallelMultiChunkReaderBase
{
public:
    TPartitionMultiChunkReader(
        NChunkClient::TMultiChunkReaderConfigPtr config,
        NChunkClient::TMultiChunkReaderOptionsPtr options,
        NRpc::IChannelPtr masterChannel,
        NChunkClient::IBlockCachePtr compressedBlockCache,
        NChunkClient::IBlockCachePtr uncompressedBlockCache,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        const std::vector<NChunkClient::NProto::TChunkSpec>& chunkSpecs,
        TNameTablePtr nameTable,
        const TKeyColumns& keyColumns);

    template <class TValueInsertIterator, class TRowPointerInsertIterator>
    bool Read(
        TValueInsertIterator& valueInserter,
        TRowPointerInsertIterator& rowPointerInserter,
        i64* rowCount);
private:
    NChunkClient::IBlockCachePtr UncompressedBlockCache_;

    TNameTablePtr NameTable_;
    TKeyColumns KeyColumns_;

    TPartitionChunkReaderPtr CurrentReader_;


    virtual NChunkClient::IChunkReaderBasePtr CreateTemplateReader(
        const NChunkClient::NProto::TChunkSpec& chunkSpec,
        NChunkClient::IChunkReaderPtr asyncReader) override;

    virtual void OnReaderSwitched() override;

};

DEFINE_REFCOUNTED_TYPE(TPartitionMultiChunkReader)

////////////////////////////////////////////////////////////////////////////////

template <class TValueInsertIterator, class TRowPointerInsertIterator>
bool TPartitionMultiChunkReader::Read(
    TValueInsertIterator& valueInserter,
    TRowPointerInsertIterator& rowPointerInserter,
    i64* rowCount)
{
    YCHECK(ReadyEvent_.IsSet());
    YCHECK(ReadyEvent_.Get().IsOK());

    *rowCount = 0;

    // Nothing to read.
    if (!CurrentReader_)
        return false;

    bool readerFinished = !CurrentReader_->Read(valueInserter, rowPointerInserter, rowCount);
    if (*rowCount == 0) {
        return TParallelMultiChunkReaderBase::OnEmptyRead(readerFinished);
    } else {
        return true;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
