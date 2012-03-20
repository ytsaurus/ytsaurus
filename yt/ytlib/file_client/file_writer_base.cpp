#include "stdafx.h"
#include "file_writer_base.h"
#include "file_chunk_meta.pb.h"

#include <ytlib/misc/string.h>
#include <ytlib/misc/sync.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/file_server/file_ypath_proxy.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/chunk_server/chunk_ypath_proxy.h>

namespace NYT {
namespace NFileClient {

using namespace NYTree;
using namespace NChunkServer;
using namespace NChunkClient;
using namespace NFileServer;
using namespace NProto;
using namespace NChunkHolder::NProto;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TFileWriterBase::TFileWriterBase(
    TConfig* config,
    NRpc::IChannel* masterChannel)
    : Config(config)
    , IsOpen(false)
    , Size(0)
    , BlockCount(0)
    , ChunkProxy(masterChannel)
    , CypressProxy(masterChannel)
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

    auto createChunksReq = ChunkProxy.CreateChunks();
    createChunksReq->set_transaction_id(uploadTransactionId.ToProto());
    createChunksReq->set_chunk_count(1);
    createChunksReq->set_upload_replica_count(Config->UploadReplicaCount);
    auto createChunksRsp = createChunksReq->Invoke()->Get();
    if (!createChunksRsp->IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error allocating file chunk\n%s",
            ~createChunksRsp->GetError().ToString());
    }
    YASSERT(createChunksRsp->chunks_size() == 1);
    const auto& chunkInfo = createChunksRsp->chunks(0);
    ChunkId = TChunkId::FromProto(chunkInfo.chunk_id());
    auto holderAddresses = FromProto<Stroka>(chunkInfo.holder_addresses());
    LOG_INFO("Chunk allocated (ChunkId: %s, HolderAddresses: [%s])",
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

void TFileWriterBase::SpecificClose(const NChunkServer::TChunkId&)
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
    LOG_INFO("Chunk closed");

    LOG_INFO("Confirming chunk");
    auto confirmChunkReq = Writer->GetConfirmRequest();
    auto confirmChunkRsp = CypressProxy.Execute(confirmChunkReq)->Get();
    if (!confirmChunkRsp->IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error confirming chunk\n%s",
            ~confirmChunkRsp->GetError().ToString());
    }
    LOG_INFO("Chunk confirmed");

    SpecificClose(ChunkId);

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
