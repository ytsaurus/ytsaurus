#include "stdafx.h"
#include "file_chunk_output.h"
#include "private.h"
#include "config.h"

#include <ytlib/misc/sync.h>
#include <ytlib/misc/address.h>

#include <ytlib/chunk_client/chunk_ypath_proxy.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <ytlib/transaction_client/transaction_ypath_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/chunk_client/remote_writer.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/meta_state/rpc_helpers.h>

namespace NYT {
namespace NFileClient {

using namespace NYTree;
using namespace NChunkClient;
using namespace NTransactionClient;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

TFileChunkOutput::TFileChunkOutput(
    TFileWriterConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    TTransactionId transactionId)
    : Config(config)
    , MasterChannel(masterChannel)
    , TransactionId(transactionId)
    , IsOpen(false)
    , Size(0)
    , BlockCount(0)
    , Logger(FileWriterLogger)
{
    YASSERT(config);
    YASSERT(masterChannel);

    Codec = GetCodec(Config->CodecId);
}

void TFileChunkOutput::Open()
{
    LOG_INFO("Opening file chunk output (ReplicationFactor: %d, UploadReplicationFactor: %d)",
        Config->ReplicationFactor,
        Config->UploadReplicationFactor);


    LOG_INFO("Creating chunk");
    std::vector<Stroka> addresses;
    {
        TObjectServiceProxy proxy(MasterChannel);

        auto req = TTransactionYPathProxy::CreateObject(FromObjectId(TransactionId));
        req->set_type(EObjectType::Chunk);
        NMetaState::GenerateRpcMutationId(req);

        auto* reqExt = req->MutableExtension(TReqCreateChunkExt::create_chunk);
        reqExt->set_preferred_host_name(Stroka(GetLocalHostName()));
        reqExt->set_upload_replication_factor(Config->UploadReplicationFactor);
        reqExt->set_replication_factor(Config->ReplicationFactor);
        reqExt->set_movable(Config->ChunkMovable);

        auto rsp = proxy.Execute(req).Get();
        if (!rsp->IsOK()) {
            THROW_ERROR_EXCEPTION("Error creating file chunk")
                << rsp->GetError();
        }

        ChunkId = TChunkId::FromProto(rsp->object_id());
        const auto& rspExt = rsp->GetExtension(TRspCreateChunkExt::create_chunk);
        addresses = FromProto<Stroka>(rspExt.node_addresses());
        if (addresses.size() < Config->UploadReplicationFactor) {
            THROW_ERROR_EXCEPTION("Not enough data nodes available");
        }
    }

    LOG_INFO("Chunk created (ChunkId: %s, NodeAddresses: [%s])",
        ~ChunkId.ToString(),
        ~JoinToString(addresses));

    Logger.AddTag(Sprintf("ChunkId: %s", ~ChunkId.ToString()));

    Writer = New<TRemoteWriter>(Config, ChunkId, addresses);

    Writer->Open();

    IsOpen = true;

    LOG_INFO("File chunk output opened");
}

TFileChunkOutput::~TFileChunkOutput() throw()
{
    LOG_DEBUG_IF(IsOpen, "Writer cancelled");
}

void TFileChunkOutput::DoWrite(const void* buf, size_t len)
{
    YASSERT(IsOpen);

    LOG_DEBUG("Writing data (ChunkId: %s, Size: %d)",
        ~ChunkId.ToString(),
        static_cast<int>(len));

    if (len == 0)
        return;

    if (Buffer.empty()) {
        Buffer.reserve(static_cast<size_t>(Config->BlockSize));
    }

    size_t dataSize = len;
    const ui8* dataPtr = static_cast<const ui8*>(buf);
    while (dataSize != 0) {
        // Copy a part of data trying to fill up the current block.
        size_t bufferSize = Buffer.size();
        size_t remainingSize = static_cast<size_t>(Config->BlockSize) - Buffer.size();
        size_t copySize = Min(dataSize, remainingSize);
        Buffer.resize(Buffer.size() + copySize);
        std::copy(dataPtr, dataPtr + copySize, Buffer.begin() + bufferSize);
        dataPtr += copySize;
        dataSize -= copySize;

        // Flush the block if full.
        if (Buffer.size() == Config->BlockSize) {
            FlushBlock();
        }
    }

    Size += len;
}

void TFileChunkOutput::DoFinish()
{
    if (!IsOpen)
        return;

    IsOpen = false;

    LOG_INFO("Closing file writer");

    // Flush the last block.
    FlushBlock();

    LOG_INFO("Closing chunk");
    {
        Meta.set_type(EChunkType::File);
        Meta.set_version(FormatVersion);

        TMiscExt miscExt;
        miscExt.set_uncompressed_data_size(Size);
        miscExt.set_compressed_data_size(Size);
        miscExt.set_meta_size(Meta.ByteSize());
        miscExt.set_codec_id(Config->CodecId);

        SetProtoExtension(Meta.mutable_extensions(), miscExt);

        try {
            Sync(~Writer, &TRemoteWriter::AsyncClose, Meta);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error closing chunk")
                << ex;
        }
    }
    LOG_INFO("Chunk closed");

    LOG_INFO("Confirming chunk");
    {
        TObjectServiceProxy proxy(MasterChannel);

        auto req = TChunkYPathProxy::Confirm(FromObjectId(ChunkId));
        *req->mutable_chunk_info() = Writer->GetChunkInfo();
        ToProto(req->mutable_node_addresses(), Writer->GetNodeAddresses());
        *req->mutable_chunk_meta() = Meta;
        NMetaState::GenerateRpcMutationId(req);

        auto rsp = proxy.Execute(req).Get();
        if (!rsp->IsOK()) {
            THROW_ERROR_EXCEPTION("Error confirming chunk")
                << rsp->GetError();
        }
    }
    LOG_INFO("Chunk confirmed");

    LOG_INFO("File writer closed");
}

void TFileChunkOutput::FlushBlock()
{
    if (Buffer.empty())
        return;

    LOG_INFO("Writing block (BlockIndex: %d)", BlockCount);
    try {
        auto compressedBuffer = Codec->Compress(MoveRV(Buffer));

        while (!Writer->TryWriteBlock(compressedBuffer)) {
            Sync(~Writer, &TRemoteWriter::GetReadyEvent);
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error writing file block")
            << ex;
    }
    LOG_INFO("Block written (BlockIndex: %d)", BlockCount);

    Buffer.clear();
    ++BlockCount;
}

TChunkId TFileChunkOutput::GetChunkId() const
{
    YASSERT(ChunkId != NullChunkId);
    return ChunkId;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
