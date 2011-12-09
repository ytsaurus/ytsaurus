#include "stdafx.h"
#include "chunk_sequence_writer.h"

#include "../chunk_client/writer_thread.h"
#include "../misc/assert.h"

namespace NYT {
namespace NTableClient {

using namespace NTransactionClient;
using namespace NChunkClient;

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
    YASSERT(Config.NextChunkThreshold < 1);

    CreateNextChunk();
}

void TChunkSequenceWriter::CreateNextChunk()
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(~NextChunk == NULL);

    NextChunk = New< TFuture<TChunkWriter::TPtr> >();
    auto req = Proxy.CreateChunk();
    req->set_replicacount(Config.ReplicationFactor);
    req->set_transactionid(TransactionId.ToProto());

    req->Invoke()->Subscribe(FromMethod(
        &TChunkSequenceWriter::OnChunkCreated,
        this)->Via(WriterThread->GetInvoker()));
}

void TChunkSequenceWriter::OnChunkCreated(TRspCreateChunk::TPtr rsp)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YASSERT(~NextChunk != NULL);

    if (!State.IsActive()) {
        return;
    }

    if (rsp->IsOK()) {
        const auto& protoAddresses = rsp->holderaddresses();
        yvector<Stroka> addresses(protoAddresses.begin(), protoAddresses.end());

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
        State.Fail(rsp->GetError().ToString());
    }
}

TAsyncStreamState::TAsyncResult::TPtr TChunkSequenceWriter::AsyncOpen()
{
    YASSERT(!State.HasRunningOperation());

    State.StartOperation();
    NextChunk->Subscribe(FromMethod(
        &TChunkSequenceWriter::InitCurrentChunk,
        TPtr(this)));

    return State.GetOperationResult();
}

void TChunkSequenceWriter::InitCurrentChunk(TChunkWriter::TPtr nextChunk)
{
    VERIFY_THREAD_AFFINITY_ANY();
    {
        TGuard<TSpinLock> guard(CurrentSpinLock);
        CurrentChunk = nextChunk;
        NextChunk = NULL;
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

TAsyncStreamState::TAsyncResult::TPtr TChunkSequenceWriter::AsyncEndRow()
{
    VERIFY_THREAD_AFFINITY(ClientThread);

    State.StartOperation();

    CurrentChunk->AsyncEndRow()->Subscribe(FromMethod(
        &TChunkSequenceWriter::OnRowEnded,
        TPtr(this)));

    return State.GetOperationResult();
}

void TChunkSequenceWriter::OnRowEnded(TAsyncStreamState::TResult result)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!result.IsOK) {
        State.FinishOperation(result);
        return;
    }

    if (~NextChunk == NULL && IsNextChunkTime()) {
        CreateNextChunk();
    }

    if (CurrentChunk->GetCurrentSize() > Config.MaxChunkSize) {
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
        CloseChunksAwaiter->Await(CurrentChunk->AsyncClose(), FromMethod(
            &TChunkSequenceWriter::OnChunkClosed, 
            TPtr(this),
            CurrentChunk->GetChunkId()));
    } else {
        CurrentChunk->Cancel("Chunk is empty.");
    }

    TGuard<TSpinLock> guard(CurrentSpinLock);
    CurrentChunk.Reset();
}

bool TChunkSequenceWriter::IsNextChunkTime() const
{
    VERIFY_THREAD_AFFINITY_ANY();
    return CurrentChunk->GetCurrentSize() / Config.MaxChunkSize > 
        Config.NextChunkThreshold;
}

void TChunkSequenceWriter::OnChunkClosed(
    TAsyncStreamState::TResult result,
    TChunkId chunkId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!result.IsOK) {
        State.Fail(result.ErrorMessage);
        return;
    }

    TGuard<TSpinLock> guard(WrittenSpinLock);
    WrittenChunks.push_back(chunkId);
}

TAsyncStreamState::TAsyncResult::TPtr TChunkSequenceWriter::AsyncClose()
{
    VERIFY_THREAD_AFFINITY(ClientThread);

    State.StartOperation();
    FinishCurrentChunk();

    CloseChunksAwaiter->Complete(FromMethod(
        &TChunkSequenceWriter::OnClose,
        TPtr(this)));

    return State.GetOperationResult();
}

void TChunkSequenceWriter::OnClose()
{
    if (State.IsActive()) {
        State.Close();
    }
    State.FinishOperation();
}

void TChunkSequenceWriter::Cancel(const Stroka& errorMessage)
{
    VERIFY_THREAD_AFFINITY_ANY();

    State.Cancel(errorMessage);

    TGuard<TSpinLock> guard(CurrentSpinLock);
    if (~CurrentChunk != NULL) {
        CurrentChunk->Cancel(errorMessage);
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
