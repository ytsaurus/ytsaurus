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
    const TChunkListId& parentChunkList,
    const TSchema& schema)
    : Config(config)
    , ChunkProxy(masterChannel)
    , CypressProxy(masterChannel)
    , TransactionId(transactionId)
    , ParentChunkList(parentChunkList)
    , Schema(schema)
    , CloseChunksAwaiter(New<TParallelAwaiter>(WriterThread->GetInvoker()))
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(config);
    YASSERT(masterChannel);

    CreateNextChunk();

    // TODO(babenko): use TTaggedLogger here
}

TChunkSequenceWriter::~TChunkSequenceWriter()
{
    Cancel(TError("Chunk sequence writing aborted"));
}

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
        FromMethod(
            &TChunkSequenceWriter::OnChunkCreated,
            TPtr(this))
        ->Via(WriterThread->GetInvoker()));
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

        auto chunkWriter = New<TRemoteWriter>(
            ~Config->RemoteWriter,
            chunkId,
            addresses);
        chunkWriter->Open();

        NextChunk->Set(New<TChunkWriter>(
            ~Config->ChunkWriter,
            ~chunkWriter,
            Schema));

    } else {
        State.Fail(rsp->GetError());
    }
}

TAsyncError::TPtr TChunkSequenceWriter::AsyncOpen()
{
    YASSERT(!State.HasRunningOperation());

    State.StartOperation();
    NextChunk->Subscribe(FromMethod(
        &TChunkSequenceWriter::InitCurrentChunk,
        TPtr(this)));

    return State.GetOperationError();
}

void TChunkSequenceWriter::InitCurrentChunk(TChunkWriter::TPtr nextChunk)
{
    VERIFY_THREAD_AFFINITY_ANY();
    {
        TGuard<TSpinLock> guard(CurrentSpinLock);
        CurrentChunk = nextChunk;
        NextChunk.Reset();
    }
    State.FinishOperation();
}

void TChunkSequenceWriter::Write(const TColumn& column, TValue value)
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(!State.HasRunningOperation());
    YASSERT(CurrentChunk);

    CurrentChunk->Write(column, value);
}

TAsyncError::TPtr TChunkSequenceWriter::AsyncEndRow()
{
    VERIFY_THREAD_AFFINITY(ClientThread);

    State.StartOperation();

    CurrentChunk->AsyncEndRow()->Subscribe(FromMethod(
        &TChunkSequenceWriter::OnRowEnded,
        TPtr(this)));

    return State.GetOperationError();
}

void TChunkSequenceWriter::OnRowEnded(TError error)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!error.IsOK()) {
        State.FinishOperation(error);
        return;
    }

    if (!NextChunk && IsNextChunkTime()) {
        LOG_DEBUG("Time to prepare next chunk (TransactioId: %s; CurrentChunkSize: %" PRId64 ")",
            ~TransactionId.ToString(),
            CurrentChunk->GetCurrentSize());
        CreateNextChunk();
    }

    if (CurrentChunk->GetCurrentSize() > Config->MaxChunkSize) {
        LOG_DEBUG("Switching to next chunk (TransactioId: %s; CurrentChunkSize: %" PRId64 ")",
            ~TransactionId.ToString(),
            CurrentChunk->GetCurrentSize());
        YASSERT(NextChunk);
        // We're not waiting for chunk to be closed.
        FinishCurrentChunk();
        NextChunk->Subscribe(FromMethod(
            &TChunkSequenceWriter::InitCurrentChunk,
            TPtr(this)));
    } else {
        State.FinishOperation();
    }
}

void TChunkSequenceWriter::FinishCurrentChunk() 
{
    if (CurrentChunk->GetCurrentSize() > 0) {
        LOG_DEBUG("Finishing chunk (ChunkId: %s)",
            ~CurrentChunk->GetChunkId().ToString());

        TAsyncError::TPtr finishResult = New<TAsyncError>();
        CloseChunksAwaiter->Await(finishResult, 
            FromMethod(
                &TChunkSequenceWriter::OnChunkFinished, 
                TPtr(this),
                CurrentChunk->GetChunkId()));

        CurrentChunk->AsyncClose()->Subscribe(FromMethod(
            &TChunkSequenceWriter::OnChunkClosed,
            TPtr(this),
            CurrentChunk,
            finishResult));

    } else {
        LOG_DEBUG("Canceling empty chunk (ChunkId: %s)",
            ~CurrentChunk->GetChunkId().ToString());
        CurrentChunk->Cancel(TError("Chunk is empty"));
    }

    TGuard<TSpinLock> guard(CurrentSpinLock);
    CurrentChunk.Reset();
}

bool TChunkSequenceWriter::IsNextChunkTime() const
{
    VERIFY_THREAD_AFFINITY_ANY();
    return (CurrentChunk->GetCurrentSize() / double(Config->MaxChunkSize)) > 
        (Config->NextChunkThreshold / 100.0);
}

void TChunkSequenceWriter::OnChunkClosed(
    TError error,
    TChunkWriter::TPtr currentChunk,
    TAsyncError::TPtr finishResult)
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

    batchReq->Invoke()->Subscribe(FromMethod(
        &TChunkSequenceWriter::OnChunkRegistered,
        TPtr(this),
        currentChunk->GetChunkId(),
        finishResult));
}

void TChunkSequenceWriter::OnChunkRegistered(
    TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp,
    TChunkId chunkId,
    TAsyncError::TPtr finishResult)
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
    TError error,
    TChunkId chunkId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!error.IsOK()) {
        State.Fail(error);
        return;
    }

    LOG_DEBUG("Chunk successfully closed and registered (ChunkId: %s).",
        ~chunkId.ToString());
}

TAsyncError::TPtr TChunkSequenceWriter::AsyncClose()
{
    VERIFY_THREAD_AFFINITY(ClientThread);

    State.StartOperation();
    FinishCurrentChunk();

    CloseChunksAwaiter->Complete(FromMethod(
        &TChunkSequenceWriter::OnClose,
        TPtr(this)));

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

void TChunkSequenceWriter::Cancel(const TError& error)
{
    VERIFY_THREAD_AFFINITY_ANY();

    State.Cancel(error);

    TGuard<TSpinLock> guard(CurrentSpinLock);
    if (CurrentChunk) {
        CurrentChunk->Cancel(error);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT 
} // namespace NTableClient
