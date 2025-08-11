#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/chunk_writer_base.h>
#include <yt/yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/yt/ytlib/chunk_client/data_sink.h>
#include <yt/yt/ytlib/chunk_client/multi_chunk_writer.h>
#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/client/table_client/key.h>
#include <yt/yt/client/table_client/unversioned_writer.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct ISchemalessChunkWriter
    : public IUnversionedWriter
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

//! If #nameTable is |nullptr|, it will be inferred from schema.
ISchemalessChunkWriterPtr CreateSchemalessChunkWriter(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    TTableSchemaPtr schema,
    TNameTablePtr nameTable,
    NChunkClient::IChunkWriterPtr chunkWriter,
    NChunkClient::IChunkWriter::TWriteBlocksOptions writeBlocksOptions,
    const std::optional<NChunkClient::TDataSink>& dataSink = {},
    const TChunkTimestamps& chunkTimestamps = {},
    NChunkClient::IBlockCachePtr blockCache = NChunkClient::GetNullBlockCache());

////////////////////////////////////////////////////////////////////////////////

struct ISchemalessMultiChunkWriter
    : public IUnversionedWriter
    , public virtual NChunkClient::IMultiChunkWriter
{ };

DEFINE_REFCOUNTED_TYPE(ISchemalessMultiChunkWriter)

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkWriterPtr CreateSchemalessMultiChunkWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    TNameTablePtr nameTable,
    TTableSchemaPtr schema,
    NTableClient::TLegacyOwningKey lastKey,
    NApi::NNative::IClientPtr client,
    std::string localHostName,
    NObjectClient::TCellTag cellTag,
    NTransactionClient::TTransactionId transactionId,
    TMasterTableSchemaId schemaId,
    const std::optional<NChunkClient::TDataSink>& dataSink,
    NChunkClient::IChunkWriter::TWriteBlocksOptions writeBlocksOptions,
    NChunkClient::TChunkListId parentChunkListId = NChunkClient::NullChunkListId,
    const TChunkTimestamps& chunkTimestamps = TChunkTimestamps(),
    NChunkClient::TTrafficMeterPtr trafficMeter = nullptr,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler(),
    NChunkClient::IBlockCachePtr blockCache = NChunkClient::GetNullBlockCache(),
    TCallback<void(TKey, TKey)> boundaryKeysProcessor = {});

ISchemalessMultiChunkWriterPtr CreatePartitionMultiChunkWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    TNameTablePtr nameTable,
    TTableSchemaPtr schema,
    NApi::NNative::IClientPtr client,
    std::string localHostName,
    NObjectClient::TCellTag cellTag,
    NTransactionClient::TTransactionId transactionId,
    TMasterTableSchemaId schemaId,
    NChunkClient::TChunkListId parentChunkListId,
    IPartitionerPtr partitioner,
    const std::optional<NChunkClient::TDataSink>& dataSink,
    NChunkClient::IChunkWriter::TWriteBlocksOptions writeBlocksOptions,
    NChunkClient::TTrafficMeterPtr trafficMeter = nullptr,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler(),
    NChunkClient::IBlockCachePtr blockCache = NChunkClient::GetNullBlockCache());

////////////////////////////////////////////////////////////////////////////////

TFuture<IUnversionedWriterPtr> CreateSchemalessTableWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    const NYPath::TRichYPath& richPath,
    TNameTablePtr nameTable,
    NApi::NNative::IClientPtr client,
    std::string localHostName,
    NApi::ITransactionPtr transaction,
    NChunkClient::IChunkWriter::TWriteBlocksOptions writeBlocksOptions,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler(),
    NChunkClient::IBlockCachePtr blockCache = NChunkClient::GetNullBlockCache());

////////////////////////////////////////////////////////////////////////////////

TFuture<IUnversionedTableFragmentWriterPtr> CreateSchemalessTableFragmentWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    const NApi::TWriteFragmentCookie& cookie,
    TNameTablePtr nameTable,
    NApi::NNative::IClientPtr client,
    std::string localHostName,
    NCypressClient::TTransactionId transactionId,
    NChunkClient::IChunkWriter::TWriteBlocksOptions writeBlocksOptions,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler(),
    NChunkClient::IBlockCachePtr blockCache = NChunkClient::GetNullBlockCache());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
