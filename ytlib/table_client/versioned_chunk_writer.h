#pragma once

#include "public.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/chunk_client/chunk_writer_base.h>
#include <yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/ytlib/chunk_client/multi_chunk_writer.h>

#include <yt/client/table_client/schema.h>
#include <yt/client/table_client/versioned_writer.h>

#include <yt/core/concurrency/throughput_throttler.h>

#include <yt/core/rpc/public.h>

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

////////////////////////////////////////////////////////////////////////////////

IVersionedChunkWriterPtr CreateVersionedChunkWriter(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    const TTableSchema& schema,
    NChunkClient::IChunkWriterPtr chunkWriter,
    NChunkClient::IBlockCachePtr blockCache = NChunkClient::GetNullBlockCache());

////////////////////////////////////////////////////////////////////////////////

struct IVersionedMultiChunkWriter
    : public IVersionedWriter
    , public virtual NChunkClient::IMultiChunkWriter
{ };

DEFINE_REFCOUNTED_TYPE(IVersionedMultiChunkWriter)

////////////////////////////////////////////////////////////////////////////////

IVersionedMultiChunkWriterPtr CreateVersionedMultiChunkWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    const TTableSchema& schema,
    NApi::NNative::IClientPtr client,
    NObjectClient::TCellTag cellTag,
    NTransactionClient::TTransactionId transactionId,
    NChunkClient::TChunkListId parentChunkListId = NChunkClient::NullChunkListId,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler(),
    NChunkClient::IBlockCachePtr blockCache = NChunkClient::GetNullBlockCache());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
