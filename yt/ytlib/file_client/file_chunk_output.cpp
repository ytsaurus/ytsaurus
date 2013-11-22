#include "stdafx.h"
#include "file_chunk_output.h"
#include "file_chunk_writer.h"
#include "private.h"
#include "config.h"

#include <core/misc/sync.h>
#include <core/misc/address.h>
#include <core/misc/protobuf_helpers.h>

#include <core/compression/codec.h>

#include <ytlib/chunk_client/chunk_ypath_proxy.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/chunk_client/chunk_replica.h>
#include <ytlib/chunk_client/replication_writer.h>
#include <ytlib/chunk_client/chunk_helpers.h>

#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/object_client/master_ypath_proxy.h>

#include <ytlib/meta_state/rpc_helpers.h>

namespace NYT {
namespace NFileClient {

using namespace NYTree;
using namespace NChunkClient;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NNodeTrackerClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

TFileChunkOutput::TFileChunkOutput(
    TFileWriterConfigPtr config,
    NChunkClient::TMultiChunkWriterOptionsPtr options,
    NRpc::IChannelPtr masterChannel,
    const NObjectClient::TTransactionId& transactionId)
    : Config(config)
    , Options(options)
    , IsOpen(false)
    , MasterChannel(masterChannel)
    , TransactionId(transactionId)
    , Logger(FileWriterLogger)
{
    YCHECK(config);
    YCHECK(masterChannel);
}

void TFileChunkOutput::Open()
{
    LOG_INFO("Opening file chunk output (TransactionId: %s, Account: %s, ReplicationFactor: %d, UploadReplicationFactor: %d)",
        ~ToString(TransactionId),
        ~Options->Account,
        Options->ReplicationFactor,
        Config->UploadReplicationFactor);

    auto nodeDirectory = New<TNodeDirectory>();
    
    auto rsp = CreateChunk(MasterChannel, Config, Options, EObjectType::Chunk, TransactionId).Get();
    OnChunkCreated(rsp, Config, Options, &ChunkId, &Replicas, nodeDirectory);

    Logger.AddTag(Sprintf("ChunkId: %s", ~ToString(ChunkId)));

    LOG_INFO("Chunk created");

    auto targets = nodeDirectory->GetDescriptors(Replicas);
    AsyncWriter = CreateReplicationWriter(Config, ChunkId, targets);
    AsyncWriter->Open();

    Writer = New<TFileChunkWriter>(
        Config,
        New<TEncodingWriterOptions>(),
        AsyncWriter);

    IsOpen = true;

    LOG_INFO("File chunk output opened");
}

TFileChunkOutput::~TFileChunkOutput() throw()
{
    LOG_DEBUG_IF(IsOpen, "Writer cancelled");
}

void TFileChunkOutput::DoWrite(const void* buf, size_t len)
{
    YCHECK(IsOpen);

    TFileChunkWriter::TFacade* facade = nullptr;
    while ((facade = Writer->GetFacade()) == nullptr) {
        Sync(~Writer, &TFileChunkWriter::GetReadyEvent);
    }

    facade->Write(TRef(const_cast<void*>(buf), len));
}

void TFileChunkOutput::DoFinish()
{
    if (!IsOpen)
        return;

    IsOpen = false;

    LOG_INFO("Closing file writer");

    Sync(~Writer, &TFileChunkWriter::AsyncClose);

    LOG_INFO("Confirming chunk");
    {
        TObjectServiceProxy proxy(MasterChannel);

        auto req = TChunkYPathProxy::Confirm(FromObjectId(ChunkId));
        *req->mutable_chunk_info() = AsyncWriter->GetChunkInfo();
        FOREACH (int index, AsyncWriter->GetWrittenIndexes()) {
            req->add_replicas(ToProto<ui32>(Replicas[index]));
        }
        *req->mutable_chunk_meta() = Writer->GetMasterMeta();
        NMetaState::GenerateMutationId(req);

        auto rsp = proxy.Execute(req).Get();
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error confirming chunk");
    }
    LOG_INFO("Chunk confirmed");

    LOG_INFO("File writer closed");
}

TChunkId TFileChunkOutput::GetChunkId() const
{
    return ChunkId;
}

i64 TFileChunkOutput::GetSize() const
{
    return Writer->GetCurrentSize();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
