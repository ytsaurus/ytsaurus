#include "stdafx.h"
#include "file_writer.h"
#include "file_chunk_server_meta.pb.h"

#include "../misc/string.h"
#include "../misc/sync.h"
#include "../misc/serialize.h"
#include "../cypress/cypress_ypath_rpc.h"
#include "../chunk_server/chunk_service_rpc.h"
#include "../file_server/file_ypath_rpc.h"

namespace NYT {
namespace NFileClient {

using namespace NCypress;
using namespace NChunkServer;
using namespace NChunkClient;
using namespace NFileServer;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = FileClientLogger;

////////////////////////////////////////////////////////////////////////////////

TFileWriter::TFileWriter(
    const TConfig& config,
    NRpc::IChannel* masterChannel,
    NTransactionClient::ITransaction* transaction,
    NYTree::TYPath path,
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

    // Create a file node.
    LOG_INFO("Creating file node (Path: %s, TransactionId: %s)",
        ~Path,
        ~Transaction->GetId().ToString());

    CypressProxy.Reset(new TCypressServiceProxy(~MasterChannel));
    CypressProxy->SetTimeout(config.MasterRpcTimeout);

    auto createNodeRequest = TCypressYPathProxy::Create();
    createNodeRequest->SetType("file");
    createNodeRequest->SetManifest("{}");

    auto createNodeResponse = CypressProxy->Execute(
        Path,
        Transaction->GetId(),
        ~createNodeRequest)->Get();

    if (!createNodeResponse->IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error creating file node\n%s",
            ~createNodeResponse->GetError().ToString());
    }

    NodeId = TNodeId::FromProto(createNodeResponse->GetNodeId());

    LOG_INFO("File node created (NodeId: %s)", ~NodeId.ToString());

    // Create a chunk.
    LOG_INFO("Creating file chunk (UploadReplicaCount: %d)", uploadReplicaCount);

    TChunkServiceProxy chunkProxy(masterChannel);
    auto createChunkRequest = chunkProxy.CreateChunk();
    createChunkRequest->SetTransactionId(transaction->GetId().ToProto());
    createChunkRequest->SetReplicaCount(uploadReplicaCount);

    auto createChunkResponse = createChunkRequest->Invoke()->Get();
    if (!createChunkResponse->IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error creating file chunk\n%s",
            ~createChunkResponse->GetError().ToString());
    }

    ChunkId = TChunkId::FromProto(createChunkResponse->GetChunkId());
    auto addresses = FromProto<Stroka>(createChunkResponse->GetHolderAddresses());

    LOG_INFO("File chunk created (ChunkId: %s, HolderAddresses: [%s])",
        ~ChunkId.ToString(),
        ~JoinToString(addresses));

    // Initialize a writer.
    Writer = New<TRemoteWriter>(
        config.Writer,
        ChunkId,
        addresses);

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

    LOG_INFO("File writing canceled");
}

void TFileWriter::Close()
{
    if (Closed)
        return;

    CheckAborted();

    // Flush the last block.
    FlushBlock();

    // Construct server meta.
    TChunkServerMeta meta;
    meta.SetBlockCount(BlockCount);
    meta.SetSize(Size);
    TBlob metaBlob;
    SerializeProtobuf(&meta, &metaBlob);

    // Close the chunk.
    LOG_INFO("Closing file chunk");

    try {
        Sync(~Writer, &TRemoteWriter::AsyncClose, TSharedRef(MoveRV(metaBlob)));
    } catch (...) {
        LOG_ERROR_AND_THROW(yexception(), "Error closing file chunk\n%s",
            ~CurrentExceptionMessage());
    }

    LOG_INFO("File chunk closed");

    // Associate the chunk with the file.
    LOG_INFO("Attaching file chunk to file node");

    auto setChunkRequest = TFileYPathProxy::SetFileChunk();
    setChunkRequest->SetChunkId(ChunkId.ToProto());

    auto setChunkResponse = CypressProxy->Execute(NodeId, Transaction->GetId(), ~setChunkRequest)->Get();
    if (!setChunkResponse->IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error setting file chunk\n%s",
            ~setChunkResponse->GetError().ToString());
    }

    LOG_INFO("File chunk is attached");
}

void TFileWriter::FlushBlock()
{
    if (Buffer.empty())
        return;

    LOG_INFO("Writing file block (BlockIndex: %d)", BlockCount);

    try {
        Sync(~Writer, &TRemoteWriter::AsyncWriteBlock, TSharedRef(MoveRV(Buffer)));
    } catch (...) {
        LOG_ERROR_AND_THROW(yexception(), "Error writing file block\n%s",
            ~CurrentExceptionMessage());
    }
    
    LOG_INFO("File block written");

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

        LOG_WARNING_AND_THROW(yexception(), "Transaction aborted, file writing canceled");
    }
}

void TFileWriter::OnAborted()
{
    Aborted = true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
