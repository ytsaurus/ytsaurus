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

#include <ytlib/chunk_client/chunk_replica.h>
#include <ytlib/chunk_client/replication_writer.h>

#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/object_client/master_ypath_proxy.h>
#include <ytlib/object_client/helpers.h>

#include <ytlib/hydra/rpc_helpers.h>

namespace NYT {
namespace NFileClient {

using namespace NYTree;
using namespace NChunkClient;
using namespace NObjectClient;
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

    LOG_INFO("Creating chunk");
    auto nodeDirectory = New<TNodeDirectory>();
    {
        TObjectServiceProxy proxy(MasterChannel);

        auto req = TMasterYPathProxy::CreateObjects();
        ToProto(req->mutable_transaction_id(), TransactionId);
        req->set_type(EObjectType::Chunk);
        req->set_account(Options->Account);
        NHydra::GenerateMutationId(req);

        auto* reqExt = req->MutableExtension(TReqCreateChunkExt::create_chunk_ext);
        reqExt->set_preferred_host_name(TAddressResolver::Get()->GetLocalHostName());
        reqExt->set_upload_replication_factor(Config->UploadReplicationFactor);
        reqExt->set_replication_factor(Options->ReplicationFactor);
        reqExt->set_movable(Config->ChunksMovable);
        reqExt->set_vital(Options->ChunksVital);

        auto rsp = proxy.Execute(req).Get();
        if (!rsp->IsOK()) {
            auto wrappedError = TError(
                NChunkClient::EErrorCode::MasterCommunicationFailed,
                "Error creating chunk") << *rsp;
            THROW_ERROR_EXCEPTION(wrappedError);
        }

        ChunkId = FromProto<TGuid>(rsp->object_ids(0));

        const auto& rspExt = rsp->GetExtension(TRspCreateChunkExt::create_chunk_ext);
        nodeDirectory->MergeFrom(rspExt.node_directory());
        Replicas = FromProto<TChunkReplica>(rspExt.replicas());
        if (Replicas.size() < Config->UploadReplicationFactor) {
            THROW_ERROR_EXCEPTION("Not enough data nodes available: %d received, %d needed",
                static_cast<int>(Replicas.size()),
                Config->UploadReplicationFactor);
        }
    }

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
        Sync(Writer.Get(), &TFileChunkWriter::GetReadyEvent);
    }

    facade->Write(TRef(const_cast<void*>(buf), len));
}

void TFileChunkOutput::DoFinish()
{
    if (!IsOpen)
        return;

    IsOpen = false;

    LOG_INFO("Closing file writer");

    Sync(Writer.Get(), &TFileChunkWriter::AsyncClose);

    LOG_INFO("Confirming chunk");
    {
        TObjectServiceProxy proxy(MasterChannel);

        auto req = TChunkYPathProxy::Confirm(FromObjectId(ChunkId));
        *req->mutable_chunk_info() = AsyncWriter->GetChunkInfo();
        for (int index : AsyncWriter->GetWrittenIndexes()) {
            req->add_replicas(ToProto<ui32>(Replicas[index]));
        }
        *req->mutable_chunk_meta() = Writer->GetMasterMeta();
        NHydra::GenerateMutationId(req);

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
