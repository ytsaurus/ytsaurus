#pragma once

#include "common.h"
#include "async_reader.h"

#include "../misc/metric.h"
#include "../misc/config.h"
#include "../misc/thread_affinity.h"
#include "../chunk_holder/chunk_holder_service_rpc.h"

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

class TRemoteReader
    : public IAsyncReader
{
public:
    typedef TIntrusivePtr<TRemoteReader> TPtr;

    struct TConfig
        : TConfigBase
    {
        //! Holder RPC requests timeout.
        TDuration HolderRpcTimeout;

        TConfig()
        {
            Register("holder_rpc_timeout", HolderRpcTimeout).Default(TDuration::Seconds(30));

            SetDefaults();
        }
    };

    TRemoteReader(
        const TConfig& config,
        const TChunkId& chunkId,
        const yvector<Stroka>& holderAddresses);

    TFuture<TReadResult>::TPtr AsyncReadBlocks(const yvector<int>& blockIndexes);

    TFuture<IAsyncReader::TGetInfoResult>::TPtr AsyncGetChunkInfo();

private:
    typedef NChunkHolder::TChunkHolderServiceProxy TProxy;
    USE_RPC_PROXY_METHOD(TProxy, GetBlocks);

    void DoReadBlocks(
        const yvector<int>& blockIndexes, 
        TFuture<TReadResult>::TPtr result);

    void OnBlocksRead(
        TRspGetBlocks::TPtr rsp, 
        TFuture<TReadResult>::TPtr result,
        const yvector<int>& blockIndexes);

    void DoGetChunkInfo(
        TFuture<TGetInfoResult>::TPtr result);

    void OnGotChunkInfo(
        TProxy::TRspGetChunkInfo::TPtr response,
        TFuture<TGetInfoResult>::TPtr result);

    bool ChangeCurrentHolder();

    const TConfig Config;
    const TChunkId ChunkId;

    const yvector<Stroka> HolderAddresses;

    int CurrentHolder;

    TMetric ExecutionTime;

    DECLARE_THREAD_AFFINITY_SLOT(Response);
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
