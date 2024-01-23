#pragma once

#include "chunk_reader_base.h"
#include "schemaless_block_reader.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/yt/ytlib/chunk_client/multi_reader_manager.h>

#include <yt/yt/ytlib/chunk_client/data_source.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/client/chunk_client/reader_base.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TRowDescriptor
{
    THorizontalBlockReader* BlockReader;
    i32 RowIndex;
};

////////////////////////////////////////////////////////////////////////////////

class TPartitionChunkReader
    : public TChunkReaderBase
{
public:
    TPartitionChunkReader(
        NChunkClient::TBlockFetcherConfigPtr config,
        NChunkClient::IChunkReaderPtr underlyingReader,
        TNameTablePtr nameTable,
        NChunkClient::IBlockCachePtr blockCache,
        const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
        int partitionTag,
        const NChunkClient::TDataSource& dataSource,
        NChunkClient::TChunkReaderMemoryManagerHolderPtr chunkReaderMemoryManagerHolder = nullptr);

    template <class TValueInsertIterator, class TRowDescriptorInsertIterator>
    bool Read(
        TValueInsertIterator& keyValueInserter,
        TRowDescriptorInsertIterator& rowDescriptorInserter,
        i64* rowCount);

private:
    const TNameTablePtr NameTable_;

    NChunkClient::TRefCountedChunkMetaPtr ChunkMeta_;

    const int PartitionTag_;
    std::vector<ESortOrder> SortOrders_;

    NProto::TDataBlockMetaExt BlockMetaExt_;
    std::vector<int> ChunkToReaderIdMapping_;

    int CurrentBlockIndex_ = 0;
    std::vector<std::unique_ptr<THorizontalBlockReader>> BlockReaders_;

    THorizontalBlockReader* BlockReader_ = nullptr;

    TFuture<void> InitializeBlockSequence();

    void InitFirstBlock() override;
    void InitNextBlock() override;

    void InitNameTable(TNameTablePtr chunkNameTable);
};

DEFINE_REFCOUNTED_TYPE(TPartitionChunkReader)

////////////////////////////////////////////////////////////////////////////////

class TPartitionMultiChunkReader
    : public NChunkClient::IReaderBase
{
public:
    TPartitionMultiChunkReader(NChunkClient::IMultiReaderManagerPtr multiReaderManager);

    template <class TValueInsertIterator, class TRowDescriptorInsertIterator>
    bool Read(
        TValueInsertIterator& valueInserter,
        TRowDescriptorInsertIterator& rowDescriptorInserter,
        i64* rowCount);

    void Open()
    {
        MultiReaderManager_->Open();
    }

    TFuture<void> GetReadyEvent() const override
    {
        return MultiReaderManager_->GetReadyEvent();
    }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        return MultiReaderManager_->GetDataStatistics();
    }

    NChunkClient::TCodecStatistics GetDecompressionStatistics() const override
    {
        return MultiReaderManager_->GetDecompressionStatistics();
    }

    bool IsFetchingCompleted() const override
    {
        return MultiReaderManager_->IsFetchingCompleted();
    }

    std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override
    {
        return MultiReaderManager_->GetFailedChunkIds();
    }


private:
    NChunkClient::IMultiReaderManagerPtr MultiReaderManager_;
    TPartitionChunkReaderPtr CurrentReader_;

    void OnReaderSwitched();
};

DEFINE_REFCOUNTED_TYPE(TPartitionMultiChunkReader)

////////////////////////////////////////////////////////////////////////////////

TPartitionMultiChunkReaderPtr CreatePartitionMultiChunkReader(
    NChunkClient::TMultiChunkReaderConfigPtr config,
    NChunkClient::TMultiChunkReaderOptionsPtr options,
    NChunkClient::TChunkReaderHostPtr chunkReaderHost,
    const NChunkClient::TDataSourceDirectoryPtr& dataSourceDirectory,
    const std::vector<NChunkClient::TDataSliceDescriptor>& dataSliceDescriptors,
    TNameTablePtr nameTable,
    int partitionTag,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    NChunkClient::IMultiReaderMemoryManagerPtr multiReaderMemoryManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

#define PARTITION_CHUNK_READER_INL_H_
#include "partition_chunk_reader-inl.h"
#undef PARTITION_CHUNK_READER_INL_H_
