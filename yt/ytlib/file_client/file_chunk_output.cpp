#include "file_chunk_output.h"
#include "private.h"
#include "config.h"
#include "file_chunk_writer.h"

#include <yt/ytlib/api/client.h>
#include <yt/ytlib/api/config.h>
#include <yt/ytlib/api/connection.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_replica.h>
#include <yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/ytlib/chunk_client/confirming_writer.h>
#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/object_client/helpers.h>
#include <yt/ytlib/object_client/master_ypath_proxy.h>
#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/core/compression/codec.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/address.h>
#include <yt/core/misc/protobuf_helpers.h>

#include <yt/core/rpc/helpers.h>

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
    TMultiChunkWriterOptionsPtr options,
    NApi::IClientPtr client,
    const TTransactionId& transactionId,
    i64 sizeLimit)
    : Config(config)
    , Options(options)
    , Client(client)
    , TransactionId(transactionId)
    , SizeLimit(sizeLimit)
    , Logger(FileClientLogger)
{
    YCHECK(Config);
    YCHECK(Client);

    LOG_INFO("File chunk output opened (TransactionId: %v, Account: %v, ReplicationFactor: %v, UploadReplicationFactor: %v)",
        TransactionId,
        Options->Account,
        Options->ReplicationFactor,
        Config->UploadReplicationFactor);

    auto nodeDirectory = New<TNodeDirectory>();
    ChunkWriter = CreateConfirmingWriter(
        Config,
        Options,
        Client->GetConnection()->GetPrimaryMasterCellTag(),
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
