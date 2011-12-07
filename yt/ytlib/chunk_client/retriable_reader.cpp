#include "stdafx.h"
#include "retriable_reader.h"

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

TRetriableReader::TRetriableReader(
    const TConfig& config,
    const TChunkId& chunkId,
    const NTransactionClient::TTransactionId& transactionId,
    NRpc::IChannel* masterChannel)
    : Config(config)
    , ChunkId(chunkId)
    , TransactionId(transactionId)
    , Proxy(masterChannel)
    , UnderlyingReader(New< TFuture<TRemoteReader::TPtr> >())
    , FailCount(0)
{
    Proxy.SetTimeout(Config.ChunkServiceRpcTimeout);
    RequestHolders();
}

void TRetriableReader::RequestHolders()
{
    auto req = Proxy.FindChunk();
    req->set_chunkid(ChunkId.ToProto());
    req->set_transactionid(TransactionId.ToProto());
    req->Invoke()->Subscribe(FromMethod(
        &TRetriableReader::OnGotHolders,
        TPtr(this)));
}

void TRetriableReader::OnGotHolders(TProxy::TRspFindChunk::TPtr rsp)
{
    if (!rsp->IsOK() || rsp->holderaddresses_size() == 0) {
        TGuard<TSpinLock> guard(SpinLock);
        CumulativeError.append(Sprintf("\n[%d]: %s",
            FailCount,
            rsp->IsOK() 
                ? "No holder addresses returned by master, chunk is considered lost."
                : ~rsp->GetError().GetMessage()));

        Retry();
        return;
    }

    yvector<Stroka> holders(
        rsp->holderaddresses().begin(),
        rsp->holderaddresses().end());
    TRemoteReader::TPtr reader = 
        New<TRemoteReader>(Config.RemoteReader, ChunkId, holders);
    UnderlyingReader->Set(reader);
}

// Must be protected by SpinLock.
void TRetriableReader::Retry()
{
    YASSERT(FailCount <= Config.RetryCount);

    if (FailCount == Config.RetryCount) {
        return;
    }

    ++FailCount;
    UnderlyingReader = New< TFuture<TRemoteReader::TPtr> >();

    if (FailCount == Config.RetryCount) {
        UnderlyingReader->Set(NULL);
        return;
    }

    TDelayedInvoker::Submit(
        ~FromMethod(&TRetriableReader::RequestHolders, TPtr(this)),
        Config.BackoffTime);
}

TFuture<IAsyncReader::TReadResult>::TPtr 
TRetriableReader::AsyncReadBlocks(const yvector<int>& blockIndexes)
{
    auto asyncResult = New< TFuture<TReadResult> >();
    UnderlyingReader->Subscribe(FromMethod(
        &TRetriableReader::DoReadBlocks,
        TPtr(this), 
        blockIndexes,
        asyncResult));

    return asyncResult;
}

TFuture<IAsyncReader::TGetInfoResult>::TPtr TRetriableReader::AsyncGetChunkInfo()
{
    TFuture<TGetInfoResult>::TPtr result = New< TFuture<TGetInfoResult> >();
    UnderlyingReader->Subscribe(FromMethod(
        &TRetriableReader::DoGetChunkInfo,
        TPtr(this),
        result));

    return result;
}


void TRetriableReader::DoReadBlocks(
    TRemoteReader::TPtr reader,
    const yvector<int>& blockIndexes,
    TFuture<TReadResult>::TPtr asyncResult)
{
    if (~reader == NULL) {
        asyncResult->Set(TReadResult(NRpc::EErrorCode::Unavailable,  CumulativeError));
    }

    // Protects FailCount in the next statement.
    TGuard<TSpinLock> guard(SpinLock);
    reader->AsyncReadBlocks(blockIndexes)->Subscribe(FromMethod(
        &TRetriableReader::OnBlocksRead,
        TPtr(this),
        blockIndexes,
        asyncResult,
        FailCount));
}

void TRetriableReader::OnBlocksRead(
    TReadResult result,
    const yvector<int>& blockIndexes,
    TFuture<TReadResult>::TPtr asyncResult,
    int requestFailCount)
{
    if (result.IsOK()) {
        asyncResult->Set(result);
        return;
    }

    {
        TGuard<TSpinLock> guard(SpinLock);
        if (requestFailCount == FailCount) {
            CumulativeError.append(Sprintf("\n[%d]: %s",
                FailCount,
                ~result.GetMessage()));
            Retry();
        }
    }

    UnderlyingReader->Subscribe(FromMethod(
        &TRetriableReader::DoReadBlocks,
        TPtr(this), 
        blockIndexes,
        asyncResult));
}

void TRetriableReader::DoGetChunkInfo(
    TRemoteReader::TPtr reader,
    TFuture<TGetInfoResult>::TPtr result)
{
    if (~reader == NULL) {
        result->Set(TError(NRpc::EErrorCode::Unavailable, CumulativeError));
        return;
    }

    // Protects FailCount in the next statement.
    TGuard<TSpinLock> guard(SpinLock);
    reader->AsyncGetChunkInfo()->Subscribe(FromMethod(
        &TRetriableReader::OnGotChunkInfo,
        TPtr(this),
        result,
        FailCount));
}

void TRetriableReader::OnGotChunkInfo(
    TGetInfoResult result,
    TFuture<TGetInfoResult>::TPtr asyncResult,
    int requestFailCount)
{
    if (result.IsOK()) {
        asyncResult->Set(result);
        return;
    }

    {
        TGuard<TSpinLock> guard(SpinLock);
        if (requestFailCount == FailCount) {
            CumulativeError.append(Sprintf("\n[%d]: %s",
                FailCount,
                ~result.GetMessage()));
            Retry();
        }
    }

    UnderlyingReader->Subscribe(FromMethod(
        &TRetriableReader::DoGetChunkInfo,
        TPtr(this), 
        asyncResult));
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
