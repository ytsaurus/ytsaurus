#include "stdafx.h"
#include "retriable_reader.h"

#include "../misc/thread_affinity.h"

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

TRetriableReader::TRetriableReader(
    TConfig* config,
    const TChunkId& chunkId,
    const NTransactionClient::TTransactionId& transactionId,
    NRpc::IChannel* masterChannel,
    IRemoteReaderFactory* readerFactory)
    : Config(config)
    , ChunkId(chunkId)
    , TransactionId(transactionId)
    , ReaderFactory(readerFactory)
    , Proxy(masterChannel)
    , AsyncReader(New< TFuture<IAsyncReader::TPtr> >())
    , FailCount(0)
{
    Proxy.SetTimeout(Config->MasterRpcTimeout);

    RequestHolders();
}

void TRetriableReader::RequestHolders()
{
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
        AppendError("Chunk is lost.");
        Retry();
        return;
    }

    auto reader = ReaderFactory->Create(ChunkId, holderAddresses);
    AsyncReader->Set(reader);
}

void TRetriableReader::Retry()
{
    VERIFY_SPINLOCK_AFFINITY(SpinLock);
    YASSERT(FailCount <= Config->RetryCount);

    ++FailCount;
    AsyncReader = New< TFuture<IAsyncReader::TPtr> >();
    if (FailCount == Config->RetryCount) {
        AsyncReader->Set(NULL);
        return;
    }

    TDelayedInvoker::Submit(
        ~FromMethod(&TRetriableReader::RequestHolders, TPtr(this)),
        Config->BackoffTime);
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
    IAsyncReader::TPtr reader,
    const yvector<int>& blockIndexes,
    TFuture<TReadResult>::TPtr asyncResult)
{
    if (!reader) {
        asyncResult->Set(TReadResult(NRpc::EErrorCode::Unavailable,  CumulativeError));
    }

    reader->AsyncReadBlocks(blockIndexes)->Subscribe(FromMethod(
        &TRetriableReader::OnBlocksRead,
        TPtr(this),
        blockIndexes,
        asyncResult,
        reader));
}

void TRetriableReader::OnBlocksRead(
    TReadResult result,
    const yvector<int>& blockIndexes,
    TFuture<TReadResult>::TPtr asyncResult,
    IAsyncReader::TPtr reader)
{
    if (result.IsOK()) {
        asyncResult->Set(result);
        return;
    }

    {
        TGuard<TSpinLock> guard(SpinLock);
        if (AsyncReader->IsSet() && ~reader == ~AsyncReader->Get()) {
            CumulativeError.append(Sprintf("\n[%d]: %s",
                FailCount,
                ~result.GetMessage()));
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
    IAsyncReader::TPtr reader,
    TFuture<TGetInfoResult>::TPtr result)
{
    if (!reader) {
        result->Set(TError(NRpc::EErrorCode::Unavailable, CumulativeError));
        return;
    }

    reader->AsyncGetChunkInfo()->Subscribe(FromMethod(
        &TRetriableReader::OnGotChunkInfo,
        TPtr(this),
        result,
        reader));
}

void TRetriableReader::OnGotChunkInfo(
    TGetInfoResult result,
    TFuture<TGetInfoResult>::TPtr asyncResult,
    IAsyncReader::TPtr reader)
{
    if (result.IsOK()) {
        asyncResult->Set(result);
        return;
    }

    if (AsyncReader->IsSet() && ~reader == ~AsyncReader->Get()) {
        CumulativeError.append(Sprintf("\n[%d]: %s",
            FailCount,
            ~result.GetMessage()));
        Retry();
    }

    AsyncReader->Subscribe(FromMethod(
        &TRetriableReader::DoGetChunkInfo,
        TPtr(this), 
        asyncResult));
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
