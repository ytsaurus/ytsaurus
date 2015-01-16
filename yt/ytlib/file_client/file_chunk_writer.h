#pragma once

#include "public.h"

#include <ytlib/api/config.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/chunk_writer_base.h>
#include <ytlib/chunk_client/multi_chunk_writer.h>
#include <ytlib/chunk_client/writer_base.h>

#include <ytlib/transaction_client/public.h>

#include <core/rpc/public.h>

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
    NChunkClient::IChunkWriterPtr chunkWriter);

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
    NRpc::IChannelPtr masterChannel,
    const NTransactionClient::TTransactionId& transactionId,
    const NChunkClient::TChunkListId& parentChunkListId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
