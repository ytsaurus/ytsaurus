#include "stdafx.h"

#include "file_chunk_output.h"

#include "config.h"
#include "file_chunk_writer.h"
#include "private.h"

#include <core/misc/address.h>
#include <core/misc/protobuf_helpers.h>

#include <core/compression/codec.h>

#include <core/rpc/helpers.h>
#include <core/concurrency/scheduler.h>

#include <ytlib/api/config.h>
#include <ytlib/api/client.h>
#include <ytlib/api/connection.h>

#include <ytlib/chunk_client/chunk_writer.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/chunk_replica.h>
#include <ytlib/chunk_client/chunk_ypath_proxy.h>
#include <ytlib/chunk_client/lazy_chunk_writer.h>
#include <ytlib/chunk_client/helpers.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/object_client/master_ypath_proxy.h>
#include <ytlib/object_client/helpers.h>

namespace NYT {
namespace NFileClient {

using namespace NYTree;
using namespace NConcurrency;
using namespace NChunkClient;
using namespace NObjectClient;
using namespace NNodeTrackerClient;
using namespace NChunkClient::NProto;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

TFileChunkOutput::TFileChunkOutput(
    TFileWriterConfigPtr config,
    NChunkClient::TMultiChunkWriterOptionsPtr options,
    NApi::IClientPtr client,
    const NObjectClient::TTransactionId& transactionId,
    i64 sizeLimit)
    : Config(config)
    , Options(options)
    , Client(client)
    , TransactionId(transactionId)
    , SizeLimit(sizeLimit)
    , Logger(FileClientLogger)
{
    YCHECK(config);
    YCHECK(client);

    LOG_INFO("File chunk output opened (TransactionId: %v, Account: %v, ReplicationFactor: %v, UploadReplicationFactor: %v)",
        TransactionId,
        Options->Account,
        Options->ReplicationFactor,
        Config->UploadReplicationFactor);

    auto nodeDirectory = New<TNodeDirectory>();
    ChunkWriter = CreateLazyChunkWriter(
        Config,
        Options,
        TransactionId,
        NullChunkListId,
        nodeDirectory,
        Client);

    Writer = CreateFileChunkWriter(
        Config,
        New<TEncodingWriterOptions>(),
        ChunkWriter);
}

void TFileChunkOutput::DoWrite(const void* buf, size_t len)
{
    if (GetSize() > SizeLimit) {
        return;
    }

    if (!Writer->Write(TRef(const_cast<void*>(buf), len))) {
        WaitFor(Writer->GetReadyEvent())
            .ThrowOnError();
    }
}

void TFileChunkOutput::DoFinish()
{
    if (GetSize() > 0) {
        LOG_INFO("Closing file writer");

        WaitFor(Writer->Close())
            .ThrowOnError();
    }

    LOG_INFO("File writer closed");
}

TChunkId TFileChunkOutput::GetChunkId() const
{
    return ChunkWriter->GetChunkId();
}

i64 TFileChunkOutput::GetSize() const
{
    return Writer->GetDataSize();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
