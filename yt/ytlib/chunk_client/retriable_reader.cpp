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
    Proxy.SetTimeout(Config.MasterRpcTimeout);
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
        New<TRemoteReader>(Config.RemoteReaderConfig, ChunkId, holders);
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
    TFuture<TReadResult>::TPtr asyncResult = New< TFuture<TReadResult> >();
    UnderlyingReader->Subscribe(FromMethod(
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
        TReadResult result;
        result.Error = TError(
            NRpc::EErrorCode::Unavailable, 
            CumulativeError);
        asyncResult->Set(result);
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
    if (result.Error.IsOK()) {
        asyncResult->Set(result);
        return;
    }

    {
        TGuard<TSpinLock> guard(SpinLock);
        if (requestFailCount == FailCount) {
            CumulativeError.append(Sprintf("\n[%d]: %s",
                FailCount,
                ~result.Error.GetMessage()));
            Retry();
        }
    }

    UnderlyingReader->Subscribe(FromMethod(
        &TRetriableReader::DoReadBlocks,
        TPtr(this), 
        blockIndexes,
        asyncResult));
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
