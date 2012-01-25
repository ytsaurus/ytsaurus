#include "stdafx.h"
#include "file_writer.h"
#include "file_chunk_meta.pb.h"

#include <ytlib/misc/string.h>
#include <ytlib/misc/sync.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/cypress/cypress_ypath_proxy.h>
#include <ytlib/file_server/file_ypath_proxy.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/chunk_server/chunk_ypath_proxy.h>

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

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): use totalReplicaCount

TFileWriter::TFileWriter(
    TConfig* config,
    NRpc::IChannel* masterChannel,
    ITransaction* transaction,
    TTransactionManager* transactionManager,
    const TYPath& path)
    : Config(config)
    , Transaction(transaction)
    , TransactionId(transaction ? transaction->GetId() : NullTransactionId)
    , TransactionManager(transactionManager)
    , Path(path)
    , IsOpen(false)
    , IsClosed(false)
    , Size(0)
    , BlockCount(0)
    , CypressProxy(masterChannel)
    , ChunkProxy(masterChannel)
    , Logger(FileClientLogger)
{
    YASSERT(config);
    YASSERT(masterChannel);
    YASSERT(transactionManager);

    Codec = GetCodec(Config->CodecId);

    Logger.AddTag(Sprintf("Path: %s, TransactionId: %s",
        ~Path,
        ~TransactionId.ToString()));

    CypressProxy.SetTimeout(config->MasterRpcTimeout);
    ChunkProxy.SetTimeout(config->MasterRpcTimeout);
}

void TFileWriter::Open()
{
    VERIFY_THREAD_AFFINITY(Client);
    YASSERT(!IsOpen);
    YASSERT(!IsClosed);

    LOG_INFO("Opening file writer (UploadReplicaCount: %d, TotalReplicaCount: %d)",
        Config->UploadReplicaCount,
        Config->TotalReplicaCount);

    LOG_INFO("Creating upload transaction");
    try {
        UploadTransaction = TransactionManager->Start(TransactionId);
    } catch (const std::exception& ex) {
        LOG_ERROR_AND_THROW(yexception(), "Error creating upload transaction\n%s",
            ex.what());
    }
    ListenTransaction(~UploadTransaction);
    LOG_INFO("Upload transaction created (TransactionId: %s)",
        ~UploadTransaction->GetId().ToString());

    auto createChunksReq = ChunkProxy.CreateChunks();
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
    auto holderAddresses = FromProto<Stroka>(chunkInfo.holder_addresses());
    LOG_INFO("Chunk allocated (ChunkId: %s, HolderAddresses: [%s])",
        ~ChunkId.ToString(),
        ~JoinToString(holderAddresses));

    Writer = New<TRemoteWriter>(
        ~Config->RemoteWriter,
        ChunkId,
        holderAddresses);

    if (Transaction) {
        ListenTransaction(~Transaction);
    }

    IsOpen = true;

    LOG_INFO("File writer opened");
}

void TFileWriter::Write(TRef data)
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

void TFileWriter::Cancel()
{
    VERIFY_THREAD_AFFINITY_ANY();
    YVERIFY(IsOpen);

    if (UploadTransaction) {
        UploadTransaction->Abort();
        UploadTransaction.Reset();
    }

    IsOpen = false;
    IsClosed = true;

    LOG_INFO("File writer canceled");
}

void TFileWriter::Close()
{
    VERIFY_THREAD_AFFINITY(Client);

    if (IsClosed)
        return;

    IsOpen = false;
    IsClosed = true;

    CheckAborted();

    LOG_INFO("Closing file writer");

    // Flush the last block.
    FlushBlock();

    LOG_INFO("Closing chunk");
    // Construct chunk attributes.
    TChunkAttributes attributes;
    attributes.set_type(EChunkType::File);
    auto* fileAttributes = attributes.MutableExtension(TFileChunkAttributes::file_attributes);
    fileAttributes->set_size(Size);
    fileAttributes->set_codec_id(Config->CodecId);
    try {
        Sync(~Writer, &TRemoteWriter::AsyncClose, attributes);
    } catch (const std::exception& ex) {
        LOG_ERROR_AND_THROW(yexception(), "Error closing chunk\n%s", 
            ex.what());
    }
    LOG_INFO("Chunk closed");

    LOG_INFO("Confirming chunk");
    auto confirmChunkReq = Writer->GetConfirmRequest();
    auto confirmChunkRsp = CypressProxy.Execute(~confirmChunkReq)->Get();
    if (!confirmChunkRsp->IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error confirming chunk\n%s",
            ~confirmChunkRsp->GetError().ToString());
    }
    LOG_INFO("Chunk confirmed");

    LOG_INFO("Creating file node");
    auto createNodeReq = TCypressYPathProxy::Create(WithTransaction(Path, TransactionId));
    createNodeReq->set_type(EObjectType::File);
    auto manifest = New<TFileManifest>();
    manifest->ChunkId = ChunkId;
    createNodeReq->set_manifest(SerializeToYson(~manifest));
    auto createNodeRsp = CypressProxy.Execute(~createNodeReq)->Get();
    if (!createNodeRsp->IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error creating file node\n%s",
            ~createNodeRsp->GetError().ToString());
    }
    NodeId = TNodeId::FromProto(createNodeRsp->object_id());
    LOG_INFO("File node created (NodeId: %s)", ~NodeId.ToString());

    LOG_INFO("Committing upload transaction");
    try {
        UploadTransaction->Commit();
    } catch (const std::exception& ex) {
        LOG_ERROR_AND_THROW(yexception(), "Error committing upload transaction\n%s",
            ex.what());
    }
    LOG_INFO("Upload transaction committed");

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
