#include "stdafx.h"
#include "file_writer_base.h"
#include <ytlib/file_client/file_chunk_meta.pb.h>

#include <ytlib/misc/string.h>
#include <ytlib/misc/sync.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/file_server/file_ypath_proxy.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/chunk_server/chunk_ypath_proxy.h>
#include <ytlib/transaction_server/transaction_ypath_proxy.h>

namespace NYT {
namespace NFileClient {

using namespace NYTree;
using namespace NChunkServer;
using namespace NChunkClient;
using namespace NTransactionServer;
using namespace NCypress;
using namespace NFileClient::NProto;
using namespace NChunkHolder::NProto;
using namespace NChunkServer::NProto;

////////////////////////////////////////////////////////////////////////////////

TFileWriterBase::TFileWriterBase(
    TConfig::TPtr config,
    NRpc::IChannel::TPtr masterChannel)
    : Config(config)
    , MasterChannel(masterChannel)
    , IsOpen(false)
    , Size(0)
    , BlockCount(0)
    , Logger(FileClientLogger)
{
    YASSERT(config);
    YASSERT(masterChannel);

    Codec = GetCodec(Config->CodecId);
}

void TFileWriterBase::Open(NObjectServer::TTransactionId uploadTransactionId)
{
    VERIFY_THREAD_AFFINITY(Client);
    YASSERT(!IsOpen);

    LOG_INFO("Opening file writer (UploadReplicaCount: %d, TotalReplicaCount: %d)",
        Config->UploadReplicaCount,
        Config->TotalReplicaCount);


    LOG_INFO("Creating chunk");
    yvector<Stroka> holderAddresses;
    {
        TCypressServiceProxy cypressProxy(MasterChannel);
        auto req = TTransactionYPathProxy::CreateObject(FromObjectId(uploadTransactionId));
        req->set_type(EObjectType::Chunk);
        auto* reqExt = req->MutableExtension(TReqCreateChunk::create_chunk);
        reqExt->set_holder_count(Config->UploadReplicaCount);
        auto rsp = cypressProxy.Execute(req)->Get();
        if (!rsp->IsOK()) {
            LOG_ERROR_AND_THROW(yexception(), "Error creating file chunk\n%s",
                ~rsp->GetError().ToString());
        }
        ChunkId = TChunkId::FromProto(rsp->object_id());
        const auto& rspExt = rsp->GetExtension(TRspCreateChunk::create_chunk);
        holderAddresses = FromProto<Stroka>(rspExt.holder_addresses());
    }
    LOG_INFO("Chunk created (ChunkId: %s, HolderAddresses: [%s])",
        ~ChunkId.ToString(),
        ~JoinToString(holderAddresses));

    Writer = New<TRemoteWriter>(
        ~Config->RemoteWriter,
        ChunkId,
        holderAddresses);
    Writer->Open();

    IsOpen = true;

    LOG_INFO("File writer opened");
}

void TFileWriterBase::Write(TRef data)
{
    VERIFY_THREAD_AFFINITY(Client);
    YASSERT(IsOpen);

    CheckAborted();

    if (data.Size() == 0)
        return;

    if (Buffer.empty()) {
        Buffer.reserve(static_cast<size_t>(Config->BlockSize));
    }

    size_t dataSize = data.Size();
    char* dataPtr = data.Begin();
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
        if (Buffer.ysize() == Config->BlockSize) {
            FlushBlock();
        }
    }

    Size += data.Size();
}

void TFileWriterBase::Cancel()
{
    VERIFY_THREAD_AFFINITY_ANY();
    YVERIFY(IsOpen);

    IsOpen = false;

    LOG_INFO("File writer canceled");
}

void TFileWriterBase::DoClose(const NChunkServer::TChunkId&)
{
}

void TFileWriterBase::Close()
{
    VERIFY_THREAD_AFFINITY(Client);

    if (!IsOpen)
        return;

    IsOpen = false;

    CheckAborted();

    LOG_INFO("Closing file writer");

    // Flush the last block.
    FlushBlock();

    LOG_INFO("Closing chunk");
    {
        // Construct chunk attributes.
        TChunkAttributes attributes;
        attributes.set_type(EChunkType::File);
        auto* fileAttributes = attributes.MutableExtension(TFileChunkAttributes::file_attributes);
        fileAttributes->set_uncompressed_size(Size);
        fileAttributes->set_codec_id(Config->CodecId);
        try {
            Sync(
                ~Writer, 
                &TRemoteWriter::AsyncClose, 
                std::vector<TSharedRef>(), 
                attributes);
        } catch (const std::exception& ex) {
            LOG_ERROR_AND_THROW(yexception(), "Error closing chunk\n%s", 
                ex.what());
        }
    }
    LOG_INFO("Chunk closed");

    LOG_INFO("Confirming chunk");
    {
        TCypressServiceProxy cypressProxy(MasterChannel);
        auto req = Writer->GetConfirmRequest();
        auto rsp = cypressProxy.Execute(req)->Get();
        if (!rsp->IsOK()) {
            LOG_ERROR_AND_THROW(yexception(), "Error confirming chunk\n%s",
                ~rsp->GetError().ToString());
        }
    }
    LOG_INFO("Chunk confirmed");

    DoClose(ChunkId);

    LOG_INFO("File writer closed");
}

void TFileWriterBase::FlushBlock()
{
    if (Buffer.empty())
        return;

    LOG_INFO("Writing block (BlockIndex: %d)", BlockCount);
    try {
        std::vector<TSharedRef> blocks;
        blocks.push_back(Codec->Compress(MoveRV(Buffer)));
        Sync(~Writer, &TRemoteWriter::AsyncWriteBlocks, blocks);
    } catch (const std::exception& ex) {
        LOG_ERROR_AND_THROW(yexception(), "Error writing file block\n%s",
            ex.what());
    }
    LOG_INFO("Block written (BlockIndex: %d)", BlockCount);

    // AsyncWriteBlock has likely cleared the buffer by swapping it out, but let's make it sure.
    Buffer.clear();
    ++BlockCount;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
