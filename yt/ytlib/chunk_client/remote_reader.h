#pragma once

#include "common.h"
#include "async_reader.h"

#include "../logging/tagged_logger.h"
#include "../misc/metric.h"
#include "../misc/configurable.h"
#include "../chunk_holder/chunk_holder_service_proxy.h"

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

class TRemoteReader
    : public IAsyncReader
{
public:
    typedef TIntrusivePtr<TRemoteReader> TPtr;

    struct TConfig
        : TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        //! Holder RPC requests timeout.
        TDuration HolderRpcTimeout;

        TConfig()
        {
            Register("holder_rpc_timeout", HolderRpcTimeout).Default(TDuration::Seconds(30));
        }
    };

    TRemoteReader(
        TConfig* config,
        const TChunkId& chunkId,
        const yvector<Stroka>& holderAddresses);

    TFuture<TReadResult>::TPtr AsyncReadBlocks(const yvector<int>& blockIndexes);

    TFuture<IAsyncReader::TGetInfoResult>::TPtr AsyncGetChunkInfo();

private:
    typedef NChunkHolder::TChunkHolderServiceProxy TProxy;

    void DoReadBlocks(
        const yvector<int>& blockIndexes, 
        TFuture<TReadResult>::TPtr result);

    void OnBlocksRead(
        TProxy::TRspGetBlocks::TPtr response,
        TFuture<TReadResult>::TPtr result,
        const yvector<int>& blockIndexes,
        int holderIndex);

    void DoGetChunkInfo(
        TFuture<TGetInfoResult>::TPtr result);

    void OnGotChunkInfo(
        TProxy::TRspGetChunkInfo::TPtr response,
        TFuture<TGetInfoResult>::TPtr result,
        int holderIndex);

    bool GetCurrentHolderIndex(int* holderIndex) const;
    bool ChangeCurrentHolder(int holderIndex, const TError& error);

    TError GetCumulativeError() const;

    TConfig::TPtr Config;
    const TChunkId ChunkId;

    yvector<Stroka> HolderAddresses;

    TMetric ExecutionTime;

    NLog::TTaggedLogger Logger;

    TSpinLock SpinLock;
    int CurrentHolderIndex;
    Stroka CumulativeErrorMessage;
    TError CumulativeError;

};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
