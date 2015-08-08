#pragma once

#include "public.h"
#include "schema.h"
#include "versioned_writer.h"

#include <ytlib/api/public.h>

#include <ytlib/chunk_client/chunk_writer_base.h>
#include <ytlib/chunk_client/multi_chunk_writer.h>
#include <ytlib/chunk_client/client_block_cache.h>

#include <core/concurrency/throughput_throttler.h>

#include <core/rpc/public.h>

namespace NYT {
namespace NVersionedTableClient {

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
    const TKeyColumns& keyColumns,
    NChunkClient::IChunkWriterPtr chunkWriter,
    NChunkClient::IBlockCachePtr blockCache = NChunkClient::GetNullBlockCache());

////////////////////////////////////////////////////////////////////////////////

struct IVersionedMultiChunkWriter
    : public IVersionedWriter
    , public virtual NChunkClient::IMultiChunkWriter
{ };

DEFINE_REFCOUNTED_TYPE(IVersionedMultiChunkWriter);

////////////////////////////////////////////////////////////////////////////////

IVersionedMultiChunkWriterPtr CreateVersionedMultiChunkWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    const TTableSchema& schema,
    const TKeyColumns& keyColumns,
    NApi::IClientPtr client,
    const NTransactionClient::TTransactionId& transactionId,
    const NChunkClient::TChunkListId& parentChunkListId = NChunkClient::NullChunkListId,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler(),
    NChunkClient::IBlockCachePtr blockCache = NChunkClient::GetNullBlockCache());

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
