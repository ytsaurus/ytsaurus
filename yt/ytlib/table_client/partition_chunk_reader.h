#pragma once

#include "chunk_reader_base.h"
#include "schemaless_block_reader.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/ytlib/chunk_client/multi_reader_base.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/concurrency/throughput_throttler.h>

#include <yt/core/rpc/public.h>

namespace NYT::NTableClient {

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
        NChunkClient::TBlockFetcherConfigPtr config,
        NChunkClient::IChunkReaderPtr underlyingReader,
        TNameTablePtr nameTable,
        NChunkClient::IBlockCachePtr blockCache,
        const NChunkClient::TClientBlockReadOptions& blockReadOptions,
        const TKeyColumns& keyColumns,
        int partitionTag);

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
    std::vector<std::unique_ptr<THorizontalSchemalessBlockReader>> BlockReaders_;

    THorizontalSchemalessBlockReader* BlockReader_ = nullptr;


    TFuture<void> InitializeBlockSequence();

    virtual void InitFirstBlock() override;
    virtual void InitNextBlock() override;

    void InitNameTable(TNameTablePtr chunkNameTable);

};

DEFINE_REFCOUNTED_TYPE(TPartitionChunkReader)

////////////////////////////////////////////////////////////////////////////////

class TPartitionMultiChunkReader
    : public NChunkClient::TParallelMultiReaderBase
{
public:
    using TParallelMultiReaderBase::TParallelMultiReaderBase;

    template <class TValueInsertIterator, class TRowDescriptorInsertIterator>
    bool Read(
        TValueInsertIterator& valueInserter,
        TRowDescriptorInsertIterator& rowDescriptorInserter,
        i64* rowCount);

private:
    TPartitionChunkReaderPtr CurrentReader_;

    virtual void OnReaderSwitched() override;

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
    NConcurrency::IThroughputThrottlerPtr rpsThrottler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

#define PARTITION_CHUNK_READER_INL_H_
#include "partition_chunk_reader-inl.h"
#undef PARTITION_CHUNK_READER_INL_H_
