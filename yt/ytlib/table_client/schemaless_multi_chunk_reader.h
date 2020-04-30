#pragma once
#include "schemaless_multi_chunk_reader.h"

#include "public.h"
#include "schemaless_chunk_reader.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/chunk_client/data_slice_descriptor.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/client/chunk_client/read_limit.h>
#include <yt/client/chunk_client/reader_base.h>

#include <yt/client/table_client/schemaless_reader.h>

#include <yt/core/concurrency/throughput_throttler.h>

#include <yt/core/rpc/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct ISchemalessMultiChunkReader
    : public virtual NChunkClient::IReaderBase
    , public ISchemalessChunkReader
{
    //! Return the index of the next, unread row.
    virtual i64 GetSessionRowIndex() const = 0;

    //! Returns the row count readable with this reader.
    //! May change over time and finally converges to actually read row count.
    virtual i64 GetTotalRowCount() const = 0;

    //! Interrupts the reader, notifies the consumer via end of stream in Read() method.
    virtual void Interrupt() = 0;

    //! Should be called only after successful call to #Read()
    //! if caller wants to skip remaining rows from the current data slice.
    //! To wait for readiness after this call, use #GetReadyEvent().
    virtual void SkipCurrentReader() = 0;
};

DEFINE_REFCOUNTED_TYPE(ISchemalessMultiChunkReader)

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessSequentialMultiReader(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    NApi::NNative::IClientPtr client,
    const NNodeTrackerClient::TNodeDescriptor& localDescriptor,
    std::optional<NNodeTrackerClient::TNodeId> localNodeId,
    NChunkClient::IBlockCachePtr blockCache,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const NChunkClient::TDataSourceDirectoryPtr& dataSourceDirectory,
    const std::vector<NChunkClient::TDataSliceDescriptor>& dataSliceDescriptors,
    TNameTablePtr nameTable,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    const TColumnFilter& columnFilter = {},
    const TKeyColumns& keyColumns = {},
    std::optional<int> partitionTag = std::nullopt,
    NChunkClient::TTrafficMeterPtr trafficMeter = nullptr,
    NConcurrency::IThroughputThrottlerPtr bandwidthThrottler = NConcurrency::GetUnlimitedThrottler(),
    NConcurrency::IThroughputThrottlerPtr rpsThrottler = NConcurrency::GetUnlimitedThrottler(),
    NChunkClient::IMultiReaderMemoryManagerPtr multiReaderMemoryManager = nullptr);

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessParallelMultiReader(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    NApi::NNative::IClientPtr client,
    const NNodeTrackerClient::TNodeDescriptor& localDescriptor,
    std::optional<NNodeTrackerClient::TNodeId> localNodeId,
    NChunkClient::IBlockCachePtr blockCache,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const NChunkClient::TDataSourceDirectoryPtr& dataSourceDirectory,
    const std::vector<NChunkClient::TDataSliceDescriptor>& dataSliceDescriptors,
    TNameTablePtr nameTable,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    const TColumnFilter& columnFilter = {},
    const TKeyColumns& keyColumns = {},
    std::optional<int> partitionTag = std::nullopt,
    NChunkClient::TTrafficMeterPtr trafficMeter = nullptr,
    NConcurrency::IThroughputThrottlerPtr bandwidthThrottler = NConcurrency::GetUnlimitedThrottler(),
    NConcurrency::IThroughputThrottlerPtr rpsThrottler = NConcurrency::GetUnlimitedThrottler(),
    NChunkClient::IMultiReaderMemoryManagerPtr multiReaderMemoryManager = nullptr);

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessMergingMultiChunkReader(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    NApi::NNative::IClientPtr client,
    const NNodeTrackerClient::TNodeDescriptor& localDescriptor,
    std::optional<NNodeTrackerClient::TNodeId> localNodeId,
    NChunkClient::IBlockCachePtr blockCache,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const NChunkClient::TDataSourceDirectoryPtr& dataSourceDirectory,
    const NChunkClient::TDataSliceDescriptor& dataSliceDescriptor,
    TNameTablePtr nameTable,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    const TColumnFilter& columnFilter,
    NChunkClient::TTrafficMeterPtr trafficMeter = nullptr,
    NConcurrency::IThroughputThrottlerPtr bandwidthThrottler = NConcurrency::GetUnlimitedThrottler(),
    NConcurrency::IThroughputThrottlerPtr rpsThrottler = NConcurrency::GetUnlimitedThrottler());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
