#include "stdafx.h"
#include "chunk_sequence_writer.h"

#include <ytlib/chunk_client/writer_thread.h>
#include <ytlib/misc/string.h>

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;
using namespace NTransactionServer;
using namespace NChunkServer;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkSequenceWriter::TChunkSequenceWriter(
    TConfig* config,
    NRpc::IChannel* masterChannel,
    const TTransactionId& transactionId,
    const TSchema& schema)
    : Config(config)
    , Proxy(masterChannel)
    , TransactionId(transactionId)
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

    auto req = Proxy.CreateChunks();
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

        // ToDo: consider using iterators in constructor to 
        // eliminate tmp vector
        auto chunkWriter = New<TRemoteWriter>(
            ~Config->RemoteWriter,
            chunkId,
            addresses);

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
        CloseChunksAwaiter->Await(CurrentChunk->AsyncClose(), 
            FromMethod(
                &TChunkSequenceWriter::OnChunkClosed, 
                TPtr(this),
                CurrentChunk->GetChunkId()));
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
    TChunkId chunkId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!error.IsOK()) {
        State.Fail(error);
        return;
    }

    LOG_DEBUG("Chunk successfully closed (ChunkId: %s).",
        ~chunkId.ToString());

    TGuard<TSpinLock> guard(WrittenSpinLock);
    WrittenChunks.push_back(chunkId);
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

const yvector<TChunkId>& TChunkSequenceWriter::GetWrittenChunkIds() const
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(State.IsClosed());

    return WrittenChunks;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT 
} // namespace NTableClient
