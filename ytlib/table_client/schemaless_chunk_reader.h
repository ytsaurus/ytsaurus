#pragma once

#include "public.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/chunk_client/data_slice_descriptor.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/client/chunk_client/read_limit.h>
#include <yt/client/chunk_client/reader_base.h>

#include <yt/client/table_client/schemaless_reader.h>

#include <yt/core/concurrency/throughput_throttler.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct ISchemalessChunkReader
    : public virtual NChunkClient::IReaderBase
    , public ISchemalessReader
{
    //! Return the current row index (measured from the start of the table).
    //! Only makes sense if the read range is nonempty.
    virtual i64 GetTableRowIndex() const = 0;

    //! Returns #unreadRows to reader and builds data slice descriptors for read and unread data.
    virtual NChunkClient::TInterruptDescriptor GetInterruptDescriptor(
        TRange<NTableClient::TUnversionedRow> unreadRows) const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISchemalessChunkReader)

////////////////////////////////////////////////////////////////////////////////

ISchemalessChunkReaderPtr CreateSchemalessChunkReader(
    const TChunkStatePtr& chunkState,
    const TColumnarChunkMetaPtr& chunkMeta,
    TChunkReaderConfigPtr config,
    TChunkReaderOptionsPtr options,
    NChunkClient::IChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    const TKeyColumns& keyColumns,
    const TColumnFilter& columnFilter,
    const NChunkClient::TReadRange& readRange,
    TNullable<int> partitionTag = Null);

ISchemalessChunkReaderPtr CreateSchemalessChunkReader(
    const TChunkStatePtr& chunkState,
    const TColumnarChunkMetaPtr& chunkMeta,
    TChunkReaderConfigPtr config,
    TChunkReaderOptionsPtr options,
    NChunkClient::IChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    const TKeyColumns& keyColumns,
    const TColumnFilter& columnFilter,
    const TSharedRange<TKey>& keys,
    TChunkReaderPerformanceCountersPtr performanceCounters = nullptr,
    TNullable<int> partitionTag = Null);

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
};

DEFINE_REFCOUNTED_TYPE(ISchemalessMultiChunkReader)

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessSequentialMultiReader(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    NApi::NNative::IClientPtr client,
    const NNodeTrackerClient::TNodeDescriptor& localDescriptor,
    NChunkClient::IBlockCachePtr blockCache,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const NChunkClient::TDataSourceDirectoryPtr& dataSourceDirectory,
    const std::vector<NChunkClient::TDataSliceDescriptor>& dataSliceDescriptors,
    TNameTablePtr nameTable,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    const TColumnFilter& columnFilter = TColumnFilter(),
    const TKeyColumns &keyColumns = TKeyColumns(),
    TNullable<int> partitionTag = Null,
    NChunkClient::TTrafficMeterPtr trafficMeter = nullptr,
    NConcurrency::IThroughputThrottlerPtr bandwidthThrottler = NConcurrency::GetUnlimitedThrottler(),
    NConcurrency::IThroughputThrottlerPtr rpsThrottler = NConcurrency::GetUnlimitedThrottler());

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessParallelMultiReader(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    NApi::NNative::IClientPtr client,
    const NNodeTrackerClient::TNodeDescriptor& localDescriptor,
    NChunkClient::IBlockCachePtr blockCache,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const NChunkClient::TDataSourceDirectoryPtr& dataSourceDirectory,
    const std::vector<NChunkClient::TDataSliceDescriptor>& dataSliceDescriptors,
    TNameTablePtr nameTable,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    const TColumnFilter& columnFilter = TColumnFilter(),
    const TKeyColumns& keyColumns = TKeyColumns(),
    TNullable<int> partitionTag = Null,
    NChunkClient::TTrafficMeterPtr trafficMeter = nullptr,
    NConcurrency::IThroughputThrottlerPtr bandwidthThrottler = NConcurrency::GetUnlimitedThrottler(),
    NConcurrency::IThroughputThrottlerPtr rpsThrottler = NConcurrency::GetUnlimitedThrottler());

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessMergingMultiChunkReader(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    NApi::NNative::IClientPtr client,
    const NNodeTrackerClient::TNodeDescriptor& localDescriptor,
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

} // namespace NTableClient
} // namespace NYT
