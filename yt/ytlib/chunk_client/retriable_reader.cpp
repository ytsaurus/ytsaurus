#include "stdafx.h"
#include "retriable_reader.h"

#include "../misc/thread_affinity.h"

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

TRetriableReader::TRetriableReader(
    TConfig* config,
    const TChunkId& chunkId,
    yvector<Stroka>& holderAddresses,
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
    , RetryIndex(0)
{
    Proxy.SetTimeout(Config->MasterRpcTimeout);

    if (holderAddresses.empty()) {
        RequestHolders(0);
    } else {
        SetAsyncReader(holderAddresses);
    }
}

void TRetriableReader::SetAsyncReader(const yvector<Stroka>& holderAddresses)
{
    AsyncReader->Set(ReaderFactory->Create(ChunkId, holderAddresses));
}

void TRetriableReader::GetAsyncReader(
    TFuture<IAsyncReader::TPtr>::TPtr* asyncReader,
    int* retryIndex)
{
    TGuard<TSpinLock> guard(SpinLock);
    if (CumulativeError.IsOK()) {
        *asyncReader = AsyncReader;
        *retryIndex = FailCount;
    } else {
        *asyncReader = ToFuture(IAsyncReader::TPtr(NULL));
        *retryIndex = -1;
    }
}

void TRetriableReader::OnRetryFailed(const TError& error, int retryIndex, bool fatal)
{
    YASSERT(!error.IsOK());

    TGuard<TSpinLock> guard(SpinLock);

    if (FailCount != retryIndex)
        return;

    CumulativeErrorMessage.append(Sprintf("\n[#%d]: %s",
        FailCount,
        ~error.GetMessage()));

    ++FailCount;
    if (FailCount < Config->RetryCount && !fatal) {
        if (AsyncReader->IsSet()) {
            AsyncReader = New< TFuture<IAsyncReader::TPtr> >();
        }
        TDelayedInvoker::Submit(
            ~FromMethod(
                &TRetriableReader::RequestHolders,
                TPtr(this),
                FailCount),
            Config->BackoffTime);
    } else {
        auto asyncReader = AsyncReader;
        CumulativeError = TError(Sprintf("Retriable chunk reader failed, details follow (ChunkId: %s)%s",
            ~ChunkId.ToString(),
            ~CumulativeErrorMessage));
        AsyncReader.Reset();
        guard.Release();
        if (!asyncReader->IsSet()) {
            asyncReader->Set(NULL);
        }
    }
}

TError TRetriableReader::GetCumulativeError() const
{
    YASSERT(!CumulativeError.IsOK());
    return CumulativeError;
}

void TRetriableReader::RequestHolders(int retryIndex)
{
    auto req = Proxy.FindChunk();
    req->set_chunk_id(ChunkId.ToProto());
    req->Invoke()->Subscribe(FromMethod(
        &TRetriableReader::OnGotHolders,
        TPtr(this),
        retryIndex)); 
}

void TRetriableReader::OnGotHolders(TProxy::TRspFindChunk::TPtr rsp, int retryIndex)
{
    if (rsp->IsOK()) {
        auto holderAddresses = FromProto<Stroka>(rsp->holder_addresses());
        if (holderAddresses.empty()) {
            OnRetryFailed(TError("Chunk is lost"), retryIndex, false);
        } else {
            SetAsyncReader(holderAddresses);
        }
    } else {
        OnRetryFailed(
            rsp->GetError(),
            retryIndex,
            rsp->GetErrorCode() == TProxy::EErrorCode::NoSuchChunk);
    }
}

IAsyncReader::TAsyncReadResult::TPtr TRetriableReader::AsyncReadBlocks(const yvector<int>& blockIndexes)
{
    auto asyncResult = New<TAsyncReadResult>();
    DoReadBlocks(blockIndexes, asyncResult);
    return asyncResult;
}

void TRetriableReader::DoReadBlocks(
    const yvector<int>& blockIndexes,
    TAsyncReadResult::TPtr asyncResult)
{
    TRetriableReader::TPtr this_ = this;

    TFuture<IAsyncReader::TPtr>::TPtr asyncReader;
    int retryIndex;
    GetAsyncReader(&asyncReader, &retryIndex);
    
    asyncReader->Subscribe(FromFunctor([=] (IAsyncReader::TPtr reader)
        {
            if (!reader) {
                asyncResult->Set(GetCumulativeError());
                return;
            }

            reader->AsyncReadBlocks(blockIndexes)->Subscribe(FromMethod(
                &TRetriableReader::OnBlocksRead,
                this_,
                blockIndexes,
                asyncResult,
                retryIndex));
        }));
}

void TRetriableReader::OnBlocksRead(
    TReadResult readResult,
    const yvector<int>& blockIndexes,
    TFuture<TReadResult>::TPtr asyncResult,
    int retryIndex)
{
    if (!readResult.IsOK()) {
        OnRetryFailed(readResult, retryIndex, false);
        DoReadBlocks(blockIndexes, asyncResult);
        return;
    }
    asyncResult->Set(readResult);
}

IAsyncReader::TAsyncGetInfoResult::TPtr TRetriableReader::AsyncGetChunkInfo()
{
    auto asyncResult = New<TAsyncGetInfoResult>();
    DoGetChunkInfo(asyncResult);
    return asyncResult;
}

void TRetriableReader::DoGetChunkInfo(
    TAsyncGetInfoResult::TPtr asyncResult)
{
    TRetriableReader::TPtr this_ = this;

    TFuture<IAsyncReader::TPtr>::TPtr asyncReader;
    int retryIndex;
    GetAsyncReader(&asyncReader, &retryIndex);
    
    asyncReader->Subscribe(FromFunctor([=] (IAsyncReader::TPtr reader)
        {
            if (!reader) {
                asyncResult->Set(GetCumulativeError());
                return;
            }

            reader->AsyncGetChunkInfo()->Subscribe(FromMethod(
                &TRetriableReader::OnGotChunkInfo,
                this_,
                asyncResult,
                retryIndex));
        }));
}

void TRetriableReader::OnGotChunkInfo(
    TGetInfoResult infoResult,
    TAsyncGetInfoResult::TPtr asyncResult,
    int retryIndex)
{
    if (!infoResult.IsOK()) {
        OnRetryFailed(infoResult, retryIndex, false);
        DoGetChunkInfo(asyncResult);
        return;
    }
    asyncResult->Set(infoResult);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
