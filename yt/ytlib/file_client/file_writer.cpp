#include "stdafx.h"
#include "file_writer.h"
#include "file_chunk_meta.pb.h"

#include <ytlib/misc/string.h>
#include <ytlib/misc/sync.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/cypress/cypress_ypath_proxy.h>
#include <ytlib/file_server/file_ypath_proxy.h>
#include <ytlib/chunk_client/block_id.h>

namespace NYT {
namespace NFileClient {

using namespace NYTree;
using namespace NCypress;
using namespace NChunkServer;
using namespace NChunkClient;
using namespace NFileServer;
using namespace NProto;
using namespace NChunkHolder::NProto;
using namespace NTransactionClient;
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): use totalReplicaCount

TFileWriter::TFileWriter(
    TConfig* config,
    NRpc::IChannel* masterChannel,
    ITransaction* transaction,
    TTransactionManager* transactionManager,
    const TYPath& path)
    : Config(config)
    , MasterChannel(masterChannel)
    , Transaction(transaction)
    , TransactionId(transaction ? transaction->GetId() : NullTransactionId)
    , TransactionManager(transactionManager)
    , Path(path)
    , Closed(false)
    , Aborted(false)
    , Size(0)
    , BlockCount(0)
    , Logger(FileClientLogger)
{
    YASSERT(masterChannel);
    YASSERT(transactionManager);

    Codec = GetCodec(Config->CodecId);

    OnAborted_ = FromMethod(&TFileWriter::OnAborted, TPtr(this));

    if (Transaction) {
        Transaction->SubscribeAborted(OnAborted_);
    }

    Logger.AddTag(Sprintf("Path: %s, TransactionId: %s",
        ~Path,
        ~TransactionId.ToString()));

    LOG_INFO("File writer open (UploadReplicaCount: %d, TotalReplicaCount: %d)",
        Config->UploadReplicaCount,
        Config->TotalReplicaCount);

    CypressProxy.Reset(new TCypressServiceProxy(~MasterChannel));
    CypressProxy->SetTimeout(config->MasterRpcTimeout);

    ChunkProxy.Reset(new TChunkServiceProxy(~MasterChannel));
    ChunkProxy->SetTimeout(config->MasterRpcTimeout);

    LOG_INFO("Creating upload transaction");
    try {
        UploadTransaction = TransactionManager->StartTransaction();
    } catch (const std::exception& ex) {
        LOG_ERROR_AND_THROW(yexception(), "Error creating upload transaction\n%s",
            ex.what());
    }
    UploadTransaction->SubscribeAborted(OnAborted_);
    LOG_INFO("Upload transaction created (TransactionId: %s)",
        ~UploadTransaction->GetId().ToString());

    auto createChunksReq = ChunkProxy->CreateChunks();
    createChunksReq->set_transaction_id(UploadTransaction->GetId().ToProto());
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
    auto addresses = FromProto<Stroka>(chunkInfo.holder_addresses());
    LOG_INFO("Chunk allocated (ChunkId: %s, HolderAddresses: [%s])",
        ~ChunkId.ToString(),
        ~JoinToString(addresses));

    // Initialize a writer.
    Writer = New<TRemoteWriter>(
        ~config->RemoteWriter,
        ChunkId,
        addresses);
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

void TFileWriter::Cancel()
{
    if (Closed)
        return;

    if (UploadTransaction) {
        UploadTransaction->Abort();
    }
    Finish();

    LOG_INFO("File writer canceled");
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
    auto* fileAttributes = attributes.MutableExtension(TFileChunkAttributes::file_attributes);
    fileAttributes->set_size(Size);
    fileAttributes->set_codec_id(Config->CodecId);
    
    LOG_INFO("Closing chunk");
    try {
        Sync(~Writer, &TRemoteWriter::AsyncClose, attributes);
    } catch (const std::exception& ex) {
        LOG_ERROR_AND_THROW(yexception(), "Error closing chunk\n%s",
            ex.what());
    }
    LOG_INFO("Chunk closed");

    LOG_INFO("Confirming chunk");
    auto confirmChunksReq = ChunkProxy->ConfirmChunks();
    confirmChunksReq->set_transaction_id(UploadTransaction->GetId().ToProto());
    *confirmChunksReq->add_chunks() = Writer->GetConfirmationInfo();
    auto confirmChunksRsp = confirmChunksReq->Invoke()->Get();
    if (!confirmChunksRsp->IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error confirming chunk\n%s",
            ~confirmChunksRsp->GetError().ToString());
    }
    LOG_INFO("Chunk confirmed");

    LOG_INFO("Creating file node");
    auto createNodeReq = TCypressYPathProxy::Create();
    createNodeReq->set_type(FileTypeName);
    // TODO(babenko): use TConfigurable::Save when it's ready
    createNodeReq->set_manifest(Sprintf("{chunk_id=\"%s\"}", ~ChunkId.ToString()));
    auto createNodeRsp = CypressProxy->Execute(
        Path,
        TransactionId,
        ~createNodeReq)->Get();
    if (!createNodeRsp->IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error creating file node\n%s",
            ~createNodeRsp->GetError().ToString());
    }
    NodeId = TNodeId::FromProto(createNodeRsp->node_id());
    LOG_INFO("File node created (NodeId: %s)", ~NodeId.ToString());

    LOG_INFO("Committing upload transaction");
    try {
        UploadTransaction->Commit();
        UploadTransaction.Reset();
    } catch (const std::exception& ex) {
        LOG_ERROR_AND_THROW(yexception(), "Error committing upload transaction\n%s",
            ex.what());
    }
    LOG_INFO("Upload transaction committed");

    Finish();

    LOG_INFO("File writer closed");
}

void TFileWriter::FlushBlock()
{
    if (Buffer.empty())
        return;

    LOG_INFO("Writing block (BlockIndex: %d)", BlockCount);
    try {
        auto compressedBlock = Codec->Compress(MoveRV(Buffer));
        Sync(~Writer, &TRemoteWriter::AsyncWriteBlock, compressedBlock);
    } catch (const std::exception& ex) {
        LOG_ERROR_AND_THROW(yexception(), "Error writing file block\n%s",
            ex.what());
    }
    LOG_INFO("Block written (BlockIndex: %d)", BlockCount);

    // AsyncWriteBlock has likely cleared the buffer by swapping it out, but let's make it sure.
    Buffer.clear();
    ++BlockCount;
}

void TFileWriter::Finish()
{
    if (Transaction) {
        Transaction->UnsubscribeAborted(OnAborted_);
        Transaction.Reset();
    }
    if (UploadTransaction) {
        UploadTransaction->Abort();
        UploadTransaction->UnsubscribeAborted(OnAborted_);
        UploadTransaction.Reset();
    }
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
