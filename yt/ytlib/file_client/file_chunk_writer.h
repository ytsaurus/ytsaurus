#pragma once

#include "public.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/chunk_client/chunk_writer_base.h>
#include <yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/ytlib/chunk_client/multi_chunk_writer.h>
#include <yt/ytlib/chunk_client/writer_base.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/concurrency/throughput_throttler.h>

#include <yt/core/crypto/crypto.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

struct IFileWriter
    : public virtual NChunkClient::IWriterBase
{
    virtual bool Write(const TRef& data) = 0;
};

DEFINE_REFCOUNTED_TYPE(IFileWriter)

////////////////////////////////////////////////////////////////////////////////

struct IFileChunkWriter
    : public IFileWriter
    , public virtual NChunkClient::IChunkWriterBase
{ };

DEFINE_REFCOUNTED_TYPE(IFileChunkWriter)

////////////////////////////////////////////////////////////////////////////////

IFileChunkWriterPtr CreateFileChunkWriter(
    TFileChunkWriterConfigPtr config,
    NChunkClient::TEncodingWriterOptionsPtr options,
    NChunkClient::IChunkWriterPtr chunkWriter,
    NChunkClient::IBlockCachePtr blockCache = NChunkClient::GetNullBlockCache());

////////////////////////////////////////////////////////////////////////////////

struct IFileMultiChunkWriter
    : public IFileWriter
    , public virtual NChunkClient::IMultiChunkWriter
{ };

DEFINE_REFCOUNTED_TYPE(IFileMultiChunkWriter)

////////////////////////////////////////////////////////////////////////////////

IFileMultiChunkWriterPtr CreateFileMultiChunkWriter(
    NApi::TFileWriterConfigPtr config,
    NChunkClient::TMultiChunkWriterOptionsPtr options,
    NApi::NNative::IClientPtr client,
    NObjectClient::TCellTag cellTag,
    const NTransactionClient::TTransactionId& transactionId,
    const NChunkClient::TChunkListId& parentChunkListId,
    NChunkClient::TTrafficMeterPtr trafficMeter = nullptr,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler(),
    NChunkClient::IBlockCachePtr blockCache = NChunkClient::GetNullBlockCache());

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
