#pragma once

#include "common.h"
#include "async_reader.h"
#include "remote_reader.h"

#include "../transaction_client/transaction.h"
#include "../chunk_server/chunk_service_proxy.h"
#include "../rpc/client.h"
#include "../misc/configurable.h"

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////  

//! Wraps TRemoteReader and retries failed request.
class TRetriableReader
    : public IAsyncReader
{
public:
    typedef TIntrusivePtr<TRetriableReader> TPtr;

    struct TConfig
        : TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        //! Interval between fail and successive attempt 
        //! to get holder addresses from master.
        TDuration BackoffTime;
        int RetryCount;
        TDuration MasterRpcTimeout;

        TConfig()
        {
            Register("backoff_time", BackoffTime).Default(TDuration::Seconds(5));
            Register("retry_count", RetryCount).Default(5);
            Register("master_rpc_timeout", MasterRpcTimeout).Default(TDuration::Seconds(5));
        }
    };

    TRetriableReader(
        TConfig* config,
        const TChunkId& chunkId,
        yvector<Stroka>& holderAddresses,
        const NTransactionClient::TTransactionId& transactionId,
        NRpc::IChannel* masterChannel,
        IRemoteReaderFactory* readerFactory);

    TAsyncReadResult::TPtr AsyncReadBlocks(const yvector<int>& blockIndexes);
    TAsyncGetInfoResult::TPtr AsyncGetChunkInfo();

private:
    typedef NChunkServer::TChunkServiceProxy TProxy;
   
    void DoReadBlocks(
        const yvector<int>& blockIndexes,
        TFuture<TReadResult>::TPtr asyncResult);
    void OnBlocksRead(
        TReadResult readResult,
        const yvector<int>& blockIndexes,
        TAsyncReadResult::TPtr asyncResult,
        int retryIndex);

    void DoGetChunkInfo(
        TFuture<TGetInfoResult>::TPtr result);
    void OnGotChunkInfo(
        TGetInfoResult infoResult,
        TAsyncGetInfoResult::TPtr asyncResult,
        int retryIndex);

    void GetAsyncReader(TFuture<IAsyncReader::TPtr>::TPtr* asyncReader, int* retryIndex);
    void RequestHolders(int retryIndex);
    void SetAsyncReader(const yvector<Stroka>& holderAddresses);
    void OnGotHolders(TProxy::TRspFindChunk::TPtr rsp, int retryIndex);
    void OnRetryFailed(const TError& error, int retryIndex, bool fatal);
    TError GetCumulativeError() const;

    TConfig::TPtr Config;
    const TChunkId ChunkId;
    const NTransactionClient::TTransactionId TransactionId;
    IRemoteReaderFactory::TPtr ReaderFactory;
    TProxy Proxy;

    //! Protects #FailCount and #UnderlyingReader.
    TSpinLock SpinLock;
    TFuture<IAsyncReader::TPtr>::TPtr AsyncReader;
    int FailCount;
    int RetryIndex;
    Stroka CumulativeErrorMessage;
    TError CumulativeError;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
