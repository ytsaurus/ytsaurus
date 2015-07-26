#pragma once

#include "public.h"

#include "schemaless_reader.h"

#include <ytlib/chunk_client/chunk_reader_base.h>
#include <ytlib/chunk_client/multi_chunk_reader.h>
#include <ytlib/chunk_client/read_limit.h>

#include <ytlib/node_tracker_client/public.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/api/public.h>

#include <core/rpc/public.h>

#include <core/concurrency/throughput_throttler.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

struct ISchemalessChunkReader
    : public virtual NChunkClient::IChunkReaderBase
    , public ISchemalessReader
{
    virtual i64 GetTableRowIndex() const = 0; 

    virtual i32 GetRangeIndex() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISchemalessChunkReader)

////////////////////////////////////////////////////////////////////////////////

ISchemalessChunkReaderPtr CreateSchemalessChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    NChunkClient::IBlockCachePtr blockCache,
    const TKeyColumns& keyColumns,
    const NChunkClient::NProto::TChunkMeta& masterMeta,
    const NChunkClient::TReadLimit& lowerLimit,
    const NChunkClient::TReadLimit& upperLimit,
    const TColumnFilter& columnFilter,
    i64 tableRowIndex = 0,
    i32 rangeIndex = 0,
    TNullable<int> partitionTag = Null);

////////////////////////////////////////////////////////////////////////////////

struct ISchemalessMultiChunkReader
    : public virtual NChunkClient::IMultiChunkReader
    , public ISchemalessReader
{
    //! Table index of the last read row group.
    virtual int GetTableIndex() const = 0;

    //! Current range index for multi-range reads.
    virtual i32 GetRangeIndex() const = 0;

    //! The current row index (measured from the table beginning).
    //! Only makes sense if the read range is nonempty.
    virtual i64 GetTableRowIndex() const = 0;

    //! Index of the next, unread row.
    virtual i64 GetSessionRowIndex() const = 0;

    //! Approximate row count readable with this reader.
    //! May change over time and finally converges to actually read row count.
    virtual i64 GetTotalRowCount() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISchemalessMultiChunkReader)

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessSequentialMultiChunkReader(
    TTableReaderConfigPtr config,
    NChunkClient::TMultiChunkReaderOptionsPtr options,
    NRpc::IChannelPtr masterChannel,
    NChunkClient::IBlockCachePtr blockCache,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const std::vector<NChunkClient::NProto::TChunkSpec>& chunkSpecs,
    TNameTablePtr nameTable,
    TColumnFilter columnFilter = TColumnFilter(),
    const TKeyColumns& keyColumns = TKeyColumns(),
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler());

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessParallelMultiChunkReader(
    TTableReaderConfigPtr config,
    NChunkClient::TMultiChunkReaderOptionsPtr options,
    NRpc::IChannelPtr masterChannel,
    NChunkClient::IBlockCachePtr blockCache,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const std::vector<NChunkClient::NProto::TChunkSpec>& chunkSpecs,
    TNameTablePtr nameTable,
    TColumnFilter columnFilter = TColumnFilter(),
    const TKeyColumns& keyColumns = TKeyColumns(),
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler());

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessTableReader(
    TTableReaderConfigPtr config,
    NChunkClient::TRemoteReaderOptionsPtr options,
    NApi::IClientPtr client,
    NTransactionClient::TTransactionPtr transaction,
    const NYPath::TRichYPath& richPath,
    TNameTablePtr nameTable,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler());

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
