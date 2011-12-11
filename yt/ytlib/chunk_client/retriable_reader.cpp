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
    , AsyncReader(New< TFuture<TRemoteReader::TPtr> >())
    , FailCount(0)
{
    Proxy.SetTimeout(Config.MasterRpcTimeout);

    TGuard<TSpinLock> guard(SpinLock);
    RequestHolders();
}

void TRetriableReader::RequestHolders()
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock);
    auto req = Proxy.FindChunk();
    req->set_chunkid(ChunkId.ToProto());
    req->Invoke()->Subscribe(FromMethod(
        &TRetriableReader::OnGotHolders,
        TPtr(this))); 
}

void TRetriableReader::OnGotHolders(TProxy::TRspFindChunk::TPtr rsp)
{
    TGuard<TSpinLock> guard(SpinLock);

    if (!rsp->IsOK()) {
        AppendError(rsp->GetError().ToString());
        Retry();
        return;
    }

    auto holderAddresses = FromProto<Stroka>(rsp->holderaddresses());
    if (holderAddresses.empty()) {
        AppendError("Chunk is unavailable");
        Retry();
        return;
    }

    auto reader = New<TRemoteReader>(Config.RemoteReader, ChunkId, holderAddresses);
    AsyncReader->Set(reader);
}

void TRetriableReader::Retry()
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock);
    YASSERT(FailCount <= Config.RetryCount);

    if (FailCount == Config.RetryCount) {
        return;
    }

    ++FailCount;
    AsyncReader = New< TFuture<TRemoteReader::TPtr> >();
    if (FailCount == Config.RetryCount) {
        AsyncReader->Set(NULL);
        return;
    }

    TDelayedInvoker::Submit(
        ~FromMethod(&TRetriableReader::RequestHolders, TPtr(this)),
        Config.BackoffTime);
}

void TRetriableReader::AppendError(const Stroka& message)
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock);
    CumulativeError.append(Sprintf("\n[%d]: %s",
        FailCount,
        ~message));
}

IAsyncReader::TAsyncReadResult::TPtr TRetriableReader::AsyncReadBlocks(const yvector<int>& blockIndexes)
{
    auto asyncResult = New< TFuture<TReadResult> >();
    AsyncReader->Subscribe(FromMethod(
        &TRetriableReader::DoReadBlocks,
        TPtr(this), 
        blockIndexes,
        asyncResult));
    return asyncResult;
}

void TRetriableReader::DoReadBlocks(
    TRemoteReader::TPtr reader,
    const yvector<int>& blockIndexes,
    TFuture<TReadResult>::TPtr asyncResult)
{
    if (~reader == NULL) {
        asyncResult->Set(TReadResult(NRpc::EErrorCode::Unavailable,  CumulativeError));
    }

    // TODO: check this
    int failCount;
    {
        TGuard<TSpinLock> guard(SpinLock);
        failCount = FailCount;
    }

    reader->AsyncReadBlocks(blockIndexes)->Subscribe(FromMethod(
        &TRetriableReader::OnBlocksRead,
        TPtr(this),
        blockIndexes,
        asyncResult,
        failCount));
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
            AppendError(result.ToString());
            Retry();
        }
    }

    AsyncReader->Subscribe(FromMethod(
        &TRetriableReader::DoReadBlocks,
        TPtr(this), 
        blockIndexes,
        asyncResult));
}

IAsyncReader::TAsyncGetInfoResult::TPtr TRetriableReader::AsyncGetChunkInfo()
{
    auto asyncResult = New< TFuture<TGetInfoResult> >();
    AsyncReader->Subscribe(FromMethod(
        &TRetriableReader::DoGetChunkInfo,
        TPtr(this),
        asyncResult));
    return asyncResult;
}

void TRetriableReader::DoGetChunkInfo(
    TRemoteReader::TPtr reader,
    TFuture<TGetInfoResult>::TPtr result)
{
    if (~reader == NULL) {
        result->Set(TError(NRpc::EErrorCode::Unavailable, CumulativeError));
        return;
    }

    // TODO: check this
    int failCount;
    {
        TGuard<TSpinLock> guard(SpinLock);
        failCount = FailCount;
    }

    reader->AsyncGetChunkInfo()->Subscribe(FromMethod(
        &TRetriableReader::OnGotChunkInfo,
        TPtr(this),
        result,
        failCount));
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
            AppendError(result.ToString());
            Retry();
        }
    }

    AsyncReader->Subscribe(FromMethod(
        &TRetriableReader::DoGetChunkInfo,
        TPtr(this), 
        asyncResult));
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
