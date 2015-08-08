#pragma once

#include "public.h"
#include "schemaless_writer.h"

#include <ytlib/api/public.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/chunk_writer_base.h>
#include <ytlib/chunk_client/multi_chunk_writer.h>
#include <ytlib/chunk_client/client_block_cache.h>

#include <ytlib/transaction_client/public.h>

#include <core/rpc/public.h>

#include <core/concurrency/throughput_throttler.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

struct ISchemalessChunkWriter
    : public ISchemalessWriter
    , public virtual NChunkClient::IChunkWriterBase
{ };

DEFINE_REFCOUNTED_TYPE(ISchemalessChunkWriter)

////////////////////////////////////////////////////////////////////////////////

ISchemalessChunkWriterPtr CreateSchemalessChunkWriter(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns,
    NChunkClient::IChunkWriterPtr chunkWriter,
    NChunkClient::IBlockCachePtr blockCache = NChunkClient::GetNullBlockCache());

ISchemalessChunkWriterPtr CreatePartitionChunkWriter(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns,
    NChunkClient::IChunkWriterPtr chunkWriter,
    IPartitioner* partitioner,
    NChunkClient::IBlockCachePtr blockCache = NChunkClient::GetNullBlockCache());

////////////////////////////////////////////////////////////////////////////////

struct ISchemalessMultiChunkWriter
    : public ISchemalessWriter
    , public virtual NChunkClient::IMultiChunkWriter
{ };

DEFINE_REFCOUNTED_TYPE(ISchemalessMultiChunkWriter)

////////////////////////////////////////////////////////////////////////////////

/*!
 *  \param reorderValues - set to |true| if key columns may come out of order, or be absent.
 */
ISchemalessMultiChunkWriterPtr CreateSchemalessMultiChunkWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns,
    NVersionedTableClient::TOwningKey lastKey,
    NApi::IClientPtr client,
    const NTransactionClient::TTransactionId& transactionId,
    const NChunkClient::TChunkListId& parentChunkListId = NChunkClient::NullChunkListId,
    bool reorderValues = false,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler(),
    NChunkClient::IBlockCachePtr blockCache = NChunkClient::GetNullBlockCache());

ISchemalessMultiChunkWriterPtr CreatePartitionMultiChunkWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns,
    NApi::IClientPtr client,
    const NTransactionClient::TTransactionId& transactionId,
    const NChunkClient::TChunkListId& parentChunkListId,
    std::unique_ptr<IPartitioner> partitioner,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler(),
    NChunkClient::IBlockCachePtr blockCache = NChunkClient::GetNullBlockCache());

////////////////////////////////////////////////////////////////////////////////

ISchemalessWriterPtr CreateSchemalessTableWriter(
    TTableWriterConfigPtr config,
    NChunkClient::TRemoteWriterOptionsPtr options,
    const NYPath::TRichYPath& richPath,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns,
    NApi::IClientPtr client,
    NTransactionClient::TTransactionPtr transaction,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler(),
    NChunkClient::IBlockCachePtr blockCache = NChunkClient::GetNullBlockCache());

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
