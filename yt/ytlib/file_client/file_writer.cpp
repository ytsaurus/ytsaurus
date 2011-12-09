#include "stdafx.h"
#include "file_writer.h"
#include "file_chunk_meta.pb.h"

#include "../misc/string.h"
#include "../misc/sync.h"
#include "../misc/serialize.h"
#include "../cypress/cypress_ypath_rpc.h"
#include "../file_server/file_ypath_rpc.h"

namespace NYT {
namespace NFileClient {

using namespace NCypress;
using namespace NChunkServer;
using namespace NChunkClient;
using namespace NFileServer;
using namespace NProto;
using namespace NChunkHolder::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = FileClientLogger;

////////////////////////////////////////////////////////////////////////////////

TFileWriter::TFileWriter(
    const TConfig& config,
    NRpc::IChannel* masterChannel,
    NTransactionClient::ITransaction* transaction,
    const NYTree::TYPath& path,
    int totalReplicaCount,
    int uploadReplicaCount)
    : Config(config)
    , MasterChannel(masterChannel)
    , Transaction(transaction)
    , Path(path)
    , Closed(false)
    , Aborted(false)
    , Size(0)
    , BlockCount(0)
{
    YASSERT(masterChannel != NULL);
    YASSERT(transaction != NULL);

    // TODO: use totalReplicaCount
    UNUSED(totalReplicaCount);

    LOG_INFO("File writer is open (Path: %s, TransactionId: %s)",
        ~Path,
        ~Transaction->GetId().ToString());

    CypressProxy.Reset(new TCypressServiceProxy(~MasterChannel));
    CypressProxy->SetTimeout(config.MasterRpcTimeout);

    ChunkProxy.Reset(new TChunkServiceProxy(~MasterChannel));
    ChunkProxy->SetTimeout(config.MasterRpcTimeout);

    // Create a file node.
    LOG_INFO("Creating node");

    auto createNodeRequest = TCypressYPathProxy::Create();
    createNodeRequest->set_type("file");
    createNodeRequest->set_manifest("{}");

    auto createNodeResponse = CypressProxy->Execute(
        Path,
        Transaction->GetId(),
        ~createNodeRequest)->Get();

    if (!createNodeResponse->IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error creating node\n%s",
            ~createNodeResponse->GetError().ToString());
    }

    NodeId = TNodeId::FromProto(createNodeResponse->nodeid());

    LOG_INFO("Node is created (NodeId: %s)", ~NodeId.ToString());

    // Create a chunk.
    LOG_INFO("Creating chunk (UploadReplicaCount: %d)", uploadReplicaCount);

    auto allocateChunk = ChunkProxy->AllocateChunk();
    allocateChunk->set_transactionid(transaction->GetId().ToProto());
    allocateChunk->set_replicacount(uploadReplicaCount);

    auto createChunkResponse = allocateChunk->Invoke()->Get();
    if (!createChunkResponse->IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error creating chunk\n%s",
            ~createChunkResponse->GetError().ToString());
    }

    ChunkId = TChunkId::FromProto(createChunkResponse->chunkid());
    auto addresses = FromProto<Stroka>(createChunkResponse->holderaddresses());

    LOG_INFO("Chunk is created (ChunkId: %s, HolderAddresses: [%s])",
        ~ChunkId.ToString(),
        ~JoinToString(addresses));

    // Initialize a writer.
    Writer = New<TRemoteWriter>(
        config.RemoteWriter,
        ChunkId,
        addresses);

    Codec = GetCodec(Config.CodecId);

    // Bind to the transaction.
    OnAborted_ = FromMethod(&TFileWriter::OnAborted, TPtr(this));
    transaction->SubscribeAborted(OnAborted_);
}

void TFileWriter::Write(TRef data)
{
    if (Closed) {
        ythrow yexception() << "File writer is already closed";
    }

    CheckAborted();

    if (data.Size() == 0)
        return;

    if (Buffer.empty()) {
        Buffer.reserve(static_cast<size_t>(Config.BlockSize));
    }

    size_t dataSize = data.Size();
    char* dataPtr = data.Begin();
    while (dataSize != 0) {
        // Copy a part of data trying to fill up the current block.
        size_t bufferSize = Buffer.size();
        size_t remainingSize = static_cast<size_t>(Config.BlockSize) - Buffer.size();
        size_t copySize = Min(dataSize, remainingSize);
        Buffer.resize(Buffer.size() + copySize);
        std::copy(dataPtr, dataPtr + copySize, Buffer.begin() + bufferSize);
        dataPtr += copySize;
        dataSize -= copySize;

        // Flush the block if full.
        if (Buffer.ysize() == Config.BlockSize) {
            FlushBlock();
        }
    }

    Size += data.Size();
}

void TFileWriter::Cancel()
{
    if (Closed)
        return;

    Finish();

    LOG_INFO("File writer is canceled");
}

void TFileWriter::Close()
{
    if (Closed)
        return;

    CheckAborted();

    // Flush the last block.
    FlushBlock();

    // Construct chunk attributes.
    TChunkAttributes attributes;
    attributes.set_type(EChunkType::File);
    auto* fileAttributes = attributes.MutableExtension(TFileChunkAttributes::FileAttributes);
    fileAttributes->set_size(Size);
    fileAttributes->set_codecid(Config.CodecId);
    
    // Close the chunk.
    LOG_INFO("Closing chunk");

    try {
        Sync(~Writer, &TRemoteWriter::AsyncClose, attributes);
    } catch (...) {
        LOG_ERROR_AND_THROW(yexception(), "Error closing chunk\n%s",
            ~CurrentExceptionMessage());
    }

    LOG_INFO("Chunk is closed");

    // Confirm chunk at the master.
    LOG_INFO("Confirming chunk");

    auto confirmChunksRequest = ChunkProxy->ConfirmChunks();
    confirmChunksRequest->set_transactionid(Transaction->GetId().ToProto());
    *confirmChunksRequest->add_chunks() = Writer->GetConfirmationInfo();

    auto confirmChunksResponse = confirmChunksRequest->Invoke()->Get();

    if (!confirmChunksResponse->IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error confirming chunk\n%s",
            ~confirmChunksResponse->GetError().ToString());
    }

    LOG_INFO("Chunk is confirmed");

    // Associate the chunk with the file.
    LOG_INFO("Attaching chunk to file");

    auto setChunkRequest = TFileYPathProxy::SetFileChunk();
    setChunkRequest->set_chunkid(ChunkId.ToProto());

    auto setChunkResponse =
        CypressProxy
        ->Execute(GetYPathFromNodeId(NodeId), Transaction->GetId(), ~setChunkRequest)
        ->Get();

    if (!setChunkResponse->IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error attaching chunk\n%s",
            ~setChunkResponse->GetError().ToString());
    }
    
    LOG_INFO("Chunk is attached");

    LOG_INFO("File writer is closed");
}

void TFileWriter::FlushBlock()
{
    if (Buffer.empty())
        return;

    LOG_INFO("Writing block (BlockIndex: %d)", BlockCount);

    try {
        auto compressedBlock = Codec->Compress(MoveRV(Buffer));
        Sync(~Writer, &TRemoteWriter::AsyncWriteBlock, compressedBlock);
    } catch (...) {
        LOG_ERROR_AND_THROW(yexception(), "Error writing file block\n%s",
            ~CurrentExceptionMessage());
    }
    
    LOG_INFO("Block is written (BlockIndex: %d)", BlockCount);

    // AsyncWriteBlock should have done this already, so this is just a precaution.
    Buffer.clear();
    ++BlockCount;
}

void TFileWriter::Finish()
{
    Transaction->UnsubscribeAborted(OnAborted_);
    OnAborted_.Reset();
    Buffer.clear();
    Closed = true;
}

void TFileWriter::CheckAborted()
{
    if (Aborted) {
        Finish();

        LOG_WARNING_AND_THROW(yexception(), "Transaction aborted, file writer canceled");
    }
}

void TFileWriter::OnAborted()
{
    Aborted = true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
