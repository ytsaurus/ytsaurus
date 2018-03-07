#pragma once

#include "public.h"
#include "schemaless_writer.h"

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/chunk_client/chunk_writer_base.h>
#include <yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/ytlib/chunk_client/multi_chunk_writer.h>
#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/concurrency/throughput_throttler.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct ISchemalessChunkWriter
    : public ISchemalessWriter
    , public virtual NChunkClient::IChunkWriterBase
{ };

DEFINE_REFCOUNTED_TYPE(ISchemalessChunkWriter)

////////////////////////////////////////////////////////////////////////////////

struct TChunkTimestamps
{
    TChunkTimestamps() = default;
    TChunkTimestamps(TTimestamp minTimestamp, TTimestamp maxTimestamp);

    NTransactionClient::TTimestamp MinTimestamp = NTransactionClient::NullTimestamp;
    NTransactionClient::TTimestamp MaxTimestamp = NTransactionClient::NullTimestamp;
};

////////////////////////////////////////////////////////////////////////////////

ISchemalessChunkWriterPtr CreateSchemalessChunkWriter(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    const TTableSchema& schema,
    NChunkClient::IChunkWriterPtr chunkWriter,
    const TChunkTimestamps& chunkTimestamps = TChunkTimestamps(),
    NChunkClient::IBlockCachePtr blockCache = NChunkClient::GetNullBlockCache());

////////////////////////////////////////////////////////////////////////////////

struct ISchemalessMultiChunkWriter
    : public ISchemalessWriter
    , public virtual NChunkClient::IMultiChunkWriter
{ };

DEFINE_REFCOUNTED_TYPE(ISchemalessMultiChunkWriter)

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkWriterPtr CreateSchemalessMultiChunkWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    TNameTablePtr nameTable,
    const TTableSchema& schema,
    NTableClient::TOwningKey lastKey,
    NApi::INativeClientPtr client,
    NObjectClient::TCellTag cellTag,
    const NTransactionClient::TTransactionId& transactionId,
    const NChunkClient::TChunkListId& parentChunkListId = NChunkClient::NullChunkListId,
    const TChunkTimestamps& chunkTimestamps = TChunkTimestamps(),
    NChunkClient::TTrafficMeterPtr trafficMeter = nullptr,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler(),
    NChunkClient::IBlockCachePtr blockCache = NChunkClient::GetNullBlockCache());

ISchemalessMultiChunkWriterPtr CreatePartitionMultiChunkWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    TNameTablePtr nameTable,
    const TTableSchema& schema,
    NApi::INativeClientPtr client,
    NObjectClient::TCellTag cellTag,
    const NTransactionClient::TTransactionId& transactionId,
    const NChunkClient::TChunkListId& parentChunkListId,
    IPartitionerPtr partitioner,
    NChunkClient::TTrafficMeterPtr trafficMeter = nullptr,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler(),
    NChunkClient::IBlockCachePtr blockCache = NChunkClient::GetNullBlockCache());

////////////////////////////////////////////////////////////////////////////////

ISchemalessWriterPtr CreateSchemalessTableWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    const NYPath::TRichYPath& richPath,
    TNameTablePtr nameTable,
    NApi::INativeClientPtr client,
    NApi::ITransactionPtr transaction,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler(),
    NChunkClient::IBlockCachePtr blockCache = NChunkClient::GetNullBlockCache());

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
