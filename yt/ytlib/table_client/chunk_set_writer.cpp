#include "stdafx.h"
#include "chunk_set_writer.h"

#include "../misc/assert.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TChunkSetWriter::TChunkSetWriter(
    const TConfig& config,
    const TSchema& schema,
    ICodec* codec,
    const TTransactionId& transactionId,
    NRpc::IChannel::TPtr masterChannel)
    : Schema(schema)
    , Codec(codec)
    , Config(config)
    , TransactionId(transactionId)
    , Proxy(masterChannel)
    , CloseChunksAwaiter(New<TParallelAwaiter>(TSyncInvoker::Get()))
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(Config.NextChunkThreshold < 1);

    CreateNextChunk();
}

void TChunkSetWriter::CreateNextChunk()
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(~NextChunk == NULL);

    NextChunk = New< TFuture<TChunkWriter::TPtr> >();
    auto req = Proxy.CreateChunk();
    req->SetReplicaCount(Config.ReplicationFactor);
    req->SetTransactionId(TransactionId.ToProto());

    // ToDo: invoke via writer thread.
    req->Invoke()->Subscribe(FromMethod(
        &TChunkSetWriter::OnChunkCreated,
        this));
}

void TChunkSetWriter::OnChunkCreated(TRspCreateChunk::TPtr rsp)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YASSERT(~NextChunk != NULL);

    if (!State.IsActive()) {
        return;
    }

    if (rsp->IsOK()) {
        const auto& protoAddresses = rsp->GetHolderAddresses();
        yvector<Stroka> addresses(protoAddresses.begin(), protoAddresses.end());

        //ToDo: consider using iterators in constructor to 
        // eliminate tmp vector
        auto chunkWriter = New<TRemoteChunkWriter>(
            Config.ChunkWriterConfig,
            TChunkId::FromProto(rsp->GetChunkId()),
            addresses);

        NextChunk->Set(new TChunkWriter(
            Config.TableChunkConfig,
            chunkWriter,
            Schema,
            Codec));

    } else {
        // ToDo: errorMessage from rpc.
        State.Fail("");
    }
}

TAsyncStreamState::TAsyncResult::TPtr TChunkSetWriter::Init()
{
    YASSERT(!State.HasRunningOperation());

    State.StartOperation();
    NextChunk->Subscribe(FromMethod(
        &TChunkSetWriter::InitCurrentChunk,
        TPtr(this)));

    return State.GetOperationResult();
}

void TChunkSetWriter::InitCurrentChunk(TChunkWriter::TPtr nextChunk)
{
    VERIFY_THREAD_AFFINITY_ANY();
    {
        TGuard<TSpinLock> guard(CurrentSpinLock);
        CurrentChunk = nextChunk;
        NextChunk = NULL;
    }
    State.FinishOperation();
}

void TChunkSetWriter::Write(const TColumn& column, TValue value)
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(!State.HasRunningOperation());
    YASSERT(~CurrentChunk != NULL);

    CurrentChunk->Write(column, value);
}

TAsyncStreamState::TAsyncResult::TPtr TChunkSetWriter::AsyncEndRow()
{
    VERIFY_THREAD_AFFINITY(ClientThread);

    State.StartOperation();

    CurrentChunk->AsyncEndRow()->Subscribe(FromMethod(
        &TChunkSetWriter::OnRowEnded,
        TPtr(this)));

    return State.GetOperationResult();
}

void TChunkSetWriter::OnRowEnded(TAsyncStreamState::TResult result)
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
            &TChunkSetWriter::InitCurrentChunk,
            TPtr(this)));
    } else {
        State.FinishOperation();
    }
}

void TChunkSetWriter::FinishCurrentChunk() 
{
    CloseChunksAwaiter->Await(CurrentChunk->AsyncClose(), FromMethod(
        &TChunkSetWriter::OnChunkClosed, 
        TPtr(this),
        CurrentChunk->GetChunkId()));

    TGuard<TSpinLock> guard(CurrentSpinLock);
    CurrentChunk.Reset();
}

bool TChunkSetWriter::IsNextChunkTime() const
{
    VERIFY_THREAD_AFFINITY_ANY();
    return CurrentChunk->GetCurrentSize() / Config.MaxChunkSize > 
        Config.NextChunkThreshold;
}

void TChunkSetWriter::OnChunkClosed(
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

TAsyncStreamState::TAsyncResult::TPtr TChunkSetWriter::AsyncClose()
{
    VERIFY_THREAD_AFFINITY(ClientThread);

    State.StartOperation();
    FinishCurrentChunk();

    CloseChunksAwaiter->Complete(FromMethod(
        &TChunkSetWriter::OnClose,
        TPtr(this)));

    return State.GetOperationResult();
}

void TChunkSetWriter::OnClose()
{
    if (State.IsActive()) {
        State.Close();
    }
    State.FinishOperation();
}

void TChunkSetWriter::Cancel(const Stroka& errorMessage)
{
    VERIFY_THREAD_AFFINITY_ANY();

    State.Cancel(errorMessage);

    TGuard<TSpinLock> guard(CurrentSpinLock);
    if (~CurrentChunk != NULL) {
        CurrentChunk->Cancel(errorMessage);
    }
}

const yvector<TChunkId>& TChunkSetWriter::GetWrittenChunks() const
{
    VERIFY_THREAD_AFFINITY(ClientThread);
    YASSERT(State.IsClosed());

    return WrittenChunks;
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT 
} // namespace NTableClient
