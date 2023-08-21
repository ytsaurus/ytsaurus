#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/chunk_writer_base.h>
#include <yt/yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/yt/ytlib/chunk_client/multi_chunk_writer.h>
#include <yt/yt/ytlib/chunk_client/data_sink.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/crypto/crypto.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NFileClient {

////////////////////////////////////////////////////////////////////////////////

struct IFileWriter
    : public virtual NChunkClient::IWriterBase
{
    virtual bool Write(TRef data) = 0;
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
    const NChunkClient::TDataSink& dataSink,
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
    NTransactionClient::TTransactionId transactionId,
    NChunkClient::TChunkListId parentChunkListId,
    const NChunkClient::TDataSink& dataSink,
    NChunkClient::TTrafficMeterPtr trafficMeter = nullptr,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler(),
    NChunkClient::IBlockCachePtr blockCache = NChunkClient::GetNullBlockCache());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFileClient
