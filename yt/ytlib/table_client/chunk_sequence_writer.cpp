#include "stdafx.h"
#include "chunk_sequence_writer.h"

#include <ytlib/chunk_client/writer_thread.h>
#include <ytlib/misc/string.h>
#include <ytlib/transaction_server/transaction_ypath_proxy.h>
#include <ytlib/object_server/id.h>
#include <ytlib/chunk_server/chunk_list_ypath_proxy.h>

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;
using namespace NChunkServer;
using namespace NCypress;
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkSequenceWriter::TChunkSequenceWriter(
    TConfig* config,
    NRpc::IChannel* masterChannel,
    const TTransactionId& transactionId,
    const TChunkListId& parentChunkList)
    : Config(config)
    , ChunkProxy(masterChannel)
    , CypressProxy(masterChannel)
    , TransactionId(transactionId)
    , ParentChunkList(parentChunkList)
    , CloseChunksAwaiter(New<TParallelAwaiter>(WriterThread->GetInvoker()))
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(config);
    YASSERT(masterChannel);

    // TODO(babenko): use TTaggedLogger here
}

TChunkSequenceWriter::~TChunkSequenceWriter()
{ }

void TChunkSequenceWriter::CreateNextChunk()
{
    YASSERT(!NextChunk);

    NextChunk = New< TFuture<TChunkWriter::TPtr> >();

    LOG_DEBUG("Creating chunk (TransactionId: %s; UploadReplicaCount: %d)",
        ~TransactionId.ToString(),
        Config->UploadReplicaCount);

    /*
    ToDo: create chunks through TTransactionYPathProxy.

    auto req = TTransactionYPathProxy::CreateObject();
    req->set_type(EObjectType::Chunk);
    */

    auto req = ChunkProxy.CreateChunks();
    req->set_chunk_count(1);
    req->set_upload_replica_count(Config->UploadReplicaCount);
    req->set_transaction_id(TransactionId.ToProto());

    req->Invoke()->Subscribe(
        Bind(
            &TChunkSequenceWriter::OnChunkCreated,
            MakeWeak(this))
        .Via(WriterThread->GetInvoker()));
}

void TChunkSequenceWriter::OnChunkCreated(TProxy::TRspCreateChunks::TPtr rsp)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YASSERT(NextChunk);

    if (!State.IsActive()) {
        return;
    }

    if (rsp->IsOK()) {
        YASSERT(rsp->chunks_size() == 1);
        const auto& chunkInfo = rsp->chunks(0);

        auto addresses = FromProto<Stroka>(chunkInfo.holder_addresses());
        auto chunkId = TChunkId::FromProto(chunkInfo.chunk_id());

        LOG_DEBUG("Chunk created (Addresses: [%s]; ChunkId: %s)",
            ~JoinToString(addresses),
            ~chunkId.ToString());

        auto remoteWriter = New<TRemoteWriter>(
            ~Config->RemoteWriter,
            chunkId,
            addresses);
        remoteWriter->Open();

        auto chunkWriter = New<TChunkWriter>(
            ~Config->ChunkWriter,
            ~remoteWriter);

        // Although we call _Async_Open, it return immediately.
        // See TChunkWriter for details.
        chunkWriter->AsyncOpen(Attributes);

        NextChunk->Set(chunkWriter);

    } else {
        State.Fail(rsp->GetError());
    }
}

TAsyncError::TPtr TChunkSequenceWriter::AsyncOpen(
    const NProto::TTableChunkAttributes& attributes)
{
    YASSERT(!State.HasRunningOperation());

    Attributes = attributes;
    CreateNextChunk();

    State.StartOperation();
    NextChunk->Subscribe(Bind(
        &TChunkSequenceWriter::InitCurrentChunk,
        MakeWeak(this)));

    return State.GetOperationError();
}

void TChunkSequenceWriter::InitCurrentChunk(TChunkWriter::TPtr nextChunk)
{
    VERIFY_THREAD_AFFINITY_ANY();

    CurrentChunk = nextChunk;
    NextChunk.Reset();
    State.FinishOperation();
}

TAsyncError::TPtr TChunkSequenceWriter::AsyncEndRow(
    TKey& key,
    std::vector<TChannelWriter::TPtr>& channels)
{
    VERIFY_THREAD_AFFINITY(ClientThread);

    if (!NextChunk && IsNextChunkTime()) {
        LOG_DEBUG("Time to prepare next chunk (TransactioId: %s; CurrentChunkSize: %" PRId64 ")",
            ~TransactionId.ToString(),
            CurrentChunk->GetCurrentSize());
        CreateNextChunk();
    }

    State.StartOperation();

    if (CurrentChunk->GetCurrentSize() > Config->MaxChunkSize) {
        LOG_DEBUG("Switching to next chunk (TransactioId: %s; CurrentChunkSize: %" PRId64 ")",
            ~TransactionId.ToString(),
            CurrentChunk->GetCurrentSize());
        YASSERT(NextChunk);
        // We're not waiting for chunk to be closed.
        FinishCurrentChunk(key, channels);
        NextChunk->Subscribe(Bind(
            &TChunkSequenceWriter::InitCurrentChunk,
            MakeWeak(this)));
    } else {
        // NB! Do not make functor here: action target should be 
        // intrusive or weak pointer to 
        CurrentChunk->AsyncEndRow(key, channels)->Subscribe(Bind(
            &TChunkSequenceWriter::OnRowEnded,
            MakeWeak(this)));
    }

    return State.GetOperationError();
}

void TChunkSequenceWriter::OnRowEnded(TError error)
{
    VERIFY_THREAD_AFFINITY_ANY();
    State.FinishOperation(error);
}

void TChunkSequenceWriter::FinishCurrentChunk(
    TKey& lastKey,
    std::vector<TChannelWriter::TPtr>& channels)
{
    if (CurrentChunk->GetCurrentSize() > 0) {
        LOG_DEBUG("Finishing chunk (ChunkId: %s)",
            ~CurrentChunk->GetChunkId().ToString());

        TAsyncError::TPtr finishResult = New<TAsyncError>();
        CloseChunksAwaiter->Await(finishResult, 
            Bind(
                &TChunkSequenceWriter::OnChunkFinished, 
                MakeWeak(this),
                CurrentChunk->GetChunkId()));

        CurrentChunk->AsyncClose(lastKey, channels)->Subscribe(Bind(
            &TChunkSequenceWriter::OnChunkClosed,
            MakeWeak(this),
            CurrentChunk,
            finishResult));

    } else {
        LOG_DEBUG("Canceling empty chunk (ChunkId: %s)",
            ~CurrentChunk->GetChunkId().ToString());
    }

    CurrentChunk.Reset();
}

bool TChunkSequenceWriter::IsNextChunkTime() const
{
    VERIFY_THREAD_AFFINITY_ANY();
    return (CurrentChunk->GetCurrentSize() / double(Config->MaxChunkSize)) > 
        (Config->NextChunkThreshold / 100.0);
}

void TChunkSequenceWriter::OnChunkClosed(
    TChunkWriter::TPtr currentChunk,
    TAsyncError::TPtr finishResult,
    TError error)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!error.IsOK()) {
        finishResult->Set(error);
        return;
    }

    LOG_DEBUG("Chunk successfully closed (ChunkId: %s).",
        ~currentChunk->GetChunkId().ToString());

    auto batchReq = CypressProxy.ExecuteBatch();
    batchReq->AddRequest(~currentChunk->GetConfirmRequest());
    {
        auto req = TChunkListYPathProxy::Attach(FromObjectId(ParentChunkList));
        req->add_children_ids(currentChunk->GetChunkId().ToProto());

        batchReq->AddRequest(~req);
    }
    {
        auto req = TTransactionYPathProxy::ReleaseObject(FromObjectId(TransactionId));
        req->set_object_id(currentChunk->GetChunkId().ToProto());

        batchReq->AddRequest(~req);
    }

    batchReq->Invoke()->Subscribe(Bind(
        &TChunkSequenceWriter::OnChunkRegistered,
        MakeWeak(this),
        currentChunk->GetChunkId(),
        finishResult));
}

void TChunkSequenceWriter::OnChunkRegistered(
    TChunkId chunkId,
    TAsyncError::TPtr finishResult,
    TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!batchRsp->IsOK()) {
        finishResult->Set(batchRsp->GetError());
        return;
    }

    LOG_DEBUG("Batch chunk registration request succeeded (ChunkId: %s).",
        ~chunkId.ToString());

    for (int i = 0; i < batchRsp->GetSize(); ++i) {
        auto rsp = batchRsp->GetResponse(i);
        if (!rsp->IsOK()) {
            finishResult->Set(rsp->GetError());
            return;
        }
    }

    finishResult->Set(TError());
}

void TChunkSequenceWriter::OnChunkFinished(
    TChunkId chunkId,
    TError error)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!error.IsOK()) {
        State.Fail(error);
        return;
    }

    LOG_DEBUG("Chunk successfully closed and registered (ChunkId: %s).",
        ~chunkId.ToString());
}

TAsyncError::TPtr TChunkSequenceWriter::AsyncClose(
    TKey& lastKey,
    std::vector<TChannelWriter::TPtr>& channels)
{
    VERIFY_THREAD_AFFINITY(ClientThread);

    State.StartOperation();
    FinishCurrentChunk(lastKey, channels);

    CloseChunksAwaiter->Complete(Bind(
        &TChunkSequenceWriter::OnClose,
        MakeWeak(this)));

    return State.GetOperationError();
}

void TChunkSequenceWriter::OnClose()
{
    if (State.IsActive()) {
        State.Close();
    }

    LOG_DEBUG("Sequence writer closed (TransactionId: %s)",
        ~TransactionId.ToString());

    State.FinishOperation();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT 
} // namespace NTableClient
