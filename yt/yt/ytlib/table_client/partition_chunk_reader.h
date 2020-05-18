#pragma once

#include "chunk_reader_base.h"
#include "schemaless_block_reader.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/ytlib/chunk_client/multi_reader_manager.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/client/chunk_client/reader_base.h>

#include <yt/core/concurrency/throughput_throttler.h>

#include <yt/core/rpc/public.h>

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
        const NChunkClient::TClientBlockReadOptions& blockReadOptions,
        const TKeyColumns& keyColumns,
        int partitionTag,
        NChunkClient::TChunkReaderMemoryManagerPtr chunkReaderMemoryManager = nullptr);

    template <class TValueInsertIterator, class TRowDescriptorInsertIterator>
    bool Read(
        TValueInsertIterator& keyValueInserter,
        TRowDescriptorInsertIterator& rowDescriptorInserter,
        i64* rowCount);

private:
    const TNameTablePtr NameTable_;
    const TKeyColumns KeyColumns_;

    NChunkClient::TRefCountedChunkMetaPtr ChunkMeta_;

    const int PartitionTag_;

    NProto::TBlockMetaExt BlockMetaExt_;
    std::vector<TColumnIdMapping> IdMapping_;

    int CurrentBlockIndex_ = 0;
    std::vector<std::unique_ptr<THorizontalBlockReader>> BlockReaders_;

    THorizontalBlockReader* BlockReader_ = nullptr;


    TFuture<void> InitializeBlockSequence();

    virtual void InitFirstBlock() override;
    virtual void InitNextBlock() override;

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

    virtual TFuture<void> GetReadyEvent() override
    {
        return MultiReaderManager_->GetReadyEvent();
    }

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        return MultiReaderManager_->GetDataStatistics();
    }

    virtual NChunkClient::TCodecStatistics GetDecompressionStatistics() const override
    {
        return MultiReaderManager_->GetDecompressionStatistics();
    }

    virtual bool IsFetchingCompleted() const override
    {
        return MultiReaderManager_->IsFetchingCompleted();
    }

    virtual std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override
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
    NApi::NNative::IClientPtr client,
    NChunkClient::IBlockCachePtr blockCache,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const NChunkClient::TDataSourceDirectoryPtr& dataSourceDirectory,
    const std::vector<NChunkClient::TDataSliceDescriptor>& dataSliceDescriptors,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns,
    int partitionTag,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    NChunkClient::TTrafficMeterPtr trafficMeter,
    NConcurrency::IThroughputThrottlerPtr bandwidthThrottler,
    NConcurrency::IThroughputThrottlerPtr rpsThrottler,
    NChunkClient::IMultiReaderMemoryManagerPtr multiReaderMemoryManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

#define PARTITION_CHUNK_READER_INL_H_
#include "partition_chunk_reader-inl.h"
#undef PARTITION_CHUNK_READER_INL_H_
