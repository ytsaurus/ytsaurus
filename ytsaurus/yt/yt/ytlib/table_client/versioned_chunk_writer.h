#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/chunk_writer_base.h>
#include <yt/yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/yt/ytlib/chunk_client/data_sink.h>
#include <yt/yt/ytlib/chunk_client/multi_chunk_writer.h>

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/versioned_writer.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IVersionedChunkWriter
    : public IVersionedWriter
    , public virtual NChunkClient::IChunkWriterBase
{
    //! Returns the number of rows written so far.
    virtual i64 GetRowCount() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IVersionedChunkWriter)

IVersionedChunkWriterPtr CreateVersionedChunkWriter(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    TTableSchemaPtr schema,
    NChunkClient::IChunkWriterPtr chunkWriter,
    const std::optional<NChunkClient::TDataSink>& dataSink = {},
    NChunkClient::IBlockCachePtr blockCache = NChunkClient::GetNullBlockCache());

////////////////////////////////////////////////////////////////////////////////

struct IVersionedMultiChunkWriter
    : public IVersionedWriter
    , public virtual NChunkClient::IMultiChunkWriter
{ };

DEFINE_REFCOUNTED_TYPE(IVersionedMultiChunkWriter)

IVersionedMultiChunkWriterPtr CreateVersionedMultiChunkWriter(
    std::function<IVersionedChunkWriterPtr(NChunkClient::IChunkWriterPtr)> chunkWriterFactory,
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    NApi::NNative::IClientPtr client,
    TString localHostName,
    NObjectClient::TCellTag cellTag,
    NTransactionClient::TTransactionId transactionId,
    NChunkClient::TChunkListId parentChunkListId = {},
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler(),
    NChunkClient::IBlockCachePtr blockCache = NChunkClient::GetNullBlockCache());

IVersionedMultiChunkWriterPtr CreateVersionedMultiChunkWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    TTableSchemaPtr schema,
    NApi::NNative::IClientPtr client,
    TString localHostName,
    NObjectClient::TCellTag cellTag,
    NTransactionClient::TTransactionId transactionId,
    const std::optional<NChunkClient::TDataSink>& dataSink = {},
    NChunkClient::TChunkListId parentChunkListId = {},
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler(),
    NChunkClient::IBlockCachePtr blockCache = NChunkClient::GetNullBlockCache());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
