#include "stdafx.h"
#include "chunk_sequence_writer.h"

#include "../chunk_client/writer_thread.h"
#include "../misc/assert.h"
#include "../misc/string.h"

namespace NYT {
namespace NTableClient {

using namespace NTransactionClient;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkSequenceWriter::TChunkSequenceWriter(
    const TConfig& config,
    const TSchema& schema,
    const TTransactionId& transactionId,
    NRpc::IChannel::TPtr masterChannel)
    : Config(config)
    , Schema(schema)
    , TransactionId(transactionId)
    , Proxy(~masterChannel)
    , CloseChunksAwaiter(New<TParallelAwaiter>(WriterThread->GetInvoker()))
{
    VERIFY_THREAD_AFFINITY(ClientThread);

    CreateNextChunk();
}

void TChunkSequenceWriter::CreateNextChunk()
{
    YASSERT(~NextChunk == NULL);

    NextChunk = New< TFuture<TChunkWriter::TPtr> >();

    LOG_DEBUG("Allocating new chunk (TransactionId: %s; ReplicaCount: %d)",
        ~TransactionId.ToString(),
        Config.ReplicationFactor);

    auto req = Proxy.AllocateChunk();
    req->set_replicacount(Config.ReplicationFactor);
    req->set_transactionid(TransactionId.ToProto());

    req->Invoke()->Subscribe(FromMethod(
        &TChunkSequenceWriter::OnChunkCreated,
        this)->Via(WriterThread->GetInvoker()));
}

void TChunkSequenceWriter::OnChunkCreated(TProxy::TRspAllocateChunk::TPtr rsp)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YASSERT(~NextChunk != NULL);

    if (!State.IsActive()) {
        return;
    }

    if (rsp->IsOK()) {
        const auto& protoAddresses = rsp->holderaddresses();
        yvector<Stroka> addresses(protoAddresses.begin(), protoAddresses.end());
        TChunkId chunkId = TChunkId::FromProto(rsp->chunkid());

        LOG_DEBUG("Allocated new chunk (Addresses: [%s]; ChunkId: %s)",
            ~JoinToString(addresses),
            ~chunkId.ToString());

        // ToDo: consider using iterators in constructor to 
        // eliminate tmp vector
        auto chunkWriter = New<TRemoteWriter>(
            Config.RemoteChunk,
            TChunkId::FromProto(rsp->chunkid()),
            addresses);

        NextChunk->Set(new TChunkWriter(
            Config.TableChunk,
            chunkWriter,
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
    YASSERT(~CurrentChunk != NULL);

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

    if (~NextChunk == NULL && IsNextChunkTime()) {
        LOG_DEBUG("Time to prepare next chunk (TransactioId: %s; CurrentChunkSize: %" PRId64 ")",
            ~TransactionId.ToString(),
            CurrentChunk->GetCurrentSize());
        CreateNextChunk();
    }

    if (CurrentChunk->GetCurrentSize() > Config.MaxChunkSize) {
        LOG_DEBUG("Switching to next chunk (TransactioId: %s; CurrentChunkSize: %" PRId64 ")",
            ~TransactionId.ToString(),
            CurrentChunk->GetCurrentSize());
        YASSERT(~NextChunk != NULL);
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
        CurrentChunk->Cancel("Chunk is empty.");
    }

    TGuard<TSpinLock> guard(CurrentSpinLock);
    CurrentChunk.Reset();
}

bool TChunkSequenceWriter::IsNextChunkTime() const
{
    VERIFY_THREAD_AFFINITY_ANY();
    return (CurrentChunk->GetCurrentSize() / double(Config.MaxChunkSize)) > 
        (Config.NextChunkThreshold / 100.0);
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
    if (~CurrentChunk != NULL) {
        CurrentChunk->Cancel(error);
    }
}

const yvector<TChunkId>& TChunkSequenceWriter::GetWrittenChunks() const
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(State.IsClosed());

    return WrittenChunks;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT 
} // namespace NTableClient
