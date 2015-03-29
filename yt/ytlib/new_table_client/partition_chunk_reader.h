#pragma once

#include "public.h"

#include "chunk_reader_base.h"
#include "schemaless_block_reader.h"

#include <ytlib/chunk_client/multi_chunk_reader_base.h>

#include <ytlib/node_tracker_client/public.h>

#include <ytlib/transaction_client/public.h>

#include <core/rpc/public.h>

#include <core/concurrency/throughput_throttler.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TRowDescriptor 
{
    THorizontalSchemalessBlockReader* BlockReader;
    i32 RowIndex;
};

////////////////////////////////////////////////////////////////////////////////

class TPartitionChunkReader
    : public TChunkReaderBase
{
public:
    TPartitionChunkReader(
        TChunkReaderConfigPtr config,
        NChunkClient::IChunkReaderPtr underlyingReader,
        TNameTablePtr nameTable,
        NChunkClient::IBlockCachePtr blockCache,
        const TKeyColumns& keyColumns,
        const NChunkClient::NProto::TChunkMeta& masterMeta,
        int partitionTag);

    template <class TValueInsertIterator, class TRowDescriptorInsertIterator>
    bool Read(
        TValueInsertIterator& keyValueInserter,
        TRowDescriptorInsertIterator& rowDescriptorInserter,
        i64* rowCount);

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override;

private:
    TNameTablePtr NameTable_;
    TKeyColumns KeyColumns_;

    NChunkClient::NProto::TChunkMeta ChunkMeta_;
    NProto::TBlockMetaExt BlockMetaExt_;

    int PartitionTag_;

    std::vector<int> IdMapping_;

    int CurrentBlockIndex_ = 0;
    i64 RowCount_ = 0;
    std::vector<std::unique_ptr<THorizontalSchemalessBlockReader>> BlockReaders_;

    THorizontalSchemalessBlockReader* BlockReader_ = nullptr;


    virtual std::vector<NChunkClient::TSequentialReader::TBlockInfo> GetBlockSequence() override;
    virtual void InitFirstBlock() override;
    virtual void InitNextBlock() override;

    void InitNameTable(TNameTablePtr chunkNameTable);

};

DEFINE_REFCOUNTED_TYPE(TPartitionChunkReader)

////////////////////////////////////////////////////////////////////////////////

//ToDo(psushin): move to inl.

template <class TValueInsertIterator, class TRowDescriptorInsertIterator>
bool TPartitionChunkReader::Read(
    TValueInsertIterator& valueInserter,
    TRowDescriptorInsertIterator& rowDescriptorInserter,
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

        const auto& key = BlockReader_->GetKey();

        std::copy(key.Begin(), key.End(), valueInserter);
        rowDescriptorInserter = TRowDescriptor({
            BlockReader_,
            static_cast<i32>(BlockReader_->GetRowIndex())});

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
        NChunkClient::IBlockCachePtr blockCache,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        const std::vector<NChunkClient::NProto::TChunkSpec>& chunkSpecs,
        TNameTablePtr nameTable,
        const TKeyColumns& keyColumns,
        NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler());

    template <class TValueInsertIterator, class TRowDescriptorInsertIterator>
    bool Read(
        TValueInsertIterator& valueInserter,
        TRowDescriptorInsertIterator& rowDescriptorInserter,
        i64* rowCount);

    TNameTablePtr GetNameTable() const;

private:
    const TNameTablePtr NameTable_;
    const TKeyColumns KeyColumns_;

    TPartitionChunkReaderPtr CurrentReader_;


    virtual NChunkClient::IChunkReaderBasePtr CreateTemplateReader(
        const NChunkClient::NProto::TChunkSpec& chunkSpec,
        NChunkClient::IChunkReaderPtr asyncReader) override;

    virtual void OnReaderSwitched() override;

};

DEFINE_REFCOUNTED_TYPE(TPartitionMultiChunkReader)

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): move to inl
template <class TValueInsertIterator, class TRowDescriptorInsertIterator>
bool TPartitionMultiChunkReader::Read(
    TValueInsertIterator& valueInserter,
    TRowDescriptorInsertIterator& rowDescriptorInserter,
    i64* rowCount)
{
    YCHECK(ReadyEvent_.IsSet());
    YCHECK(ReadyEvent_.Get().IsOK());

    *rowCount = 0;

    // Nothing to read.
    if (!CurrentReader_)
        return false;

    bool readerFinished = !CurrentReader_->Read(valueInserter, rowDescriptorInserter, rowCount);
    if (*rowCount == 0) {
        return TParallelMultiChunkReaderBase::OnEmptyRead(readerFinished);
    } else {
        return true;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
