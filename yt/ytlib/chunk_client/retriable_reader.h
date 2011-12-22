#pragma once
#include "common.h"
#include "async_reader.h"
#include "remote_reader.h"

#include "../transaction_client/transaction.h"
#include "../chunk_server/chunk_service_rpc.h"
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

        TRemoteReader::TConfig::TPtr RemoteReader;

        TConfig()
        {
            Register("backoff_time", BackoffTime).Default(TDuration::Seconds(5));
            Register("retry_count", RetryCount).Default(5);
            Register("master_rpc_timeout", MasterRpcTimeout).Default(TDuration::Seconds(5));
            Register("remote_reader", RemoteReader).DefaultNew();
        }
    };

    TRetriableReader(
        TConfig* config,
        const TChunkId& chunkId,
        const NTransactionClient::TTransactionId& transactionId,
        NRpc::IChannel* masterChannel);

    TAsyncReadResult::TPtr AsyncReadBlocks(const yvector<int>& blockIndexes);
    TAsyncGetInfoResult::TPtr AsyncGetChunkInfo();

private:
    typedef NChunkServer::TChunkServiceProxy TProxy;

    void RequestHolders();
    void OnGotHolders(TProxy::TRspFindChunk::TPtr rsp);
    void Retry();
    
    void DoReadBlocks(
        TRemoteReader::TPtr reader,
        const yvector<int>& blockIndexes,
        TFuture<TReadResult>::TPtr asyncResult);
    
    void OnBlocksRead(
        TReadResult result,
        const yvector<int>& blockIndexes,
        TFuture<TReadResult>::TPtr asyncResult,
        TRemoteReader::TPtr reader);

    void DoGetChunkInfo(
        TRemoteReader::TPtr reader,
        TFuture<TGetInfoResult>::TPtr result);
    
    void OnGotChunkInfo(
        TGetInfoResult infoResult,
        TFuture<TGetInfoResult>::TPtr result,
        TRemoteReader::TPtr reader);

    void AppendError(const Stroka& message);

    TConfig::TPtr Config;
    const TChunkId ChunkId;
    const NTransactionClient::TTransactionId TransactionId;
    TProxy Proxy;

    //! Protects #FailCount and #UnderlyingReader.
    TSpinLock SpinLock;
    TFuture<TRemoteReader::TPtr>::TPtr AsyncReader;
    int FailCount;
    Stroka CumulativeError;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
