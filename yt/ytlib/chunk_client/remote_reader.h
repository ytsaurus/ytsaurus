#pragma once

#include "common.h"
#include "async_reader.h"

#include "../chunk_holder/chunk_holder_rpc.h"
#include "../misc/metric.h"
#include "../misc/thread_affinity.h"

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

class TRemoteReader
    : public IAsyncReader
{
public:
    TRemoteReader(
        const TChunkId& chunkId,
        const yvector<Stroka>& holderAddresses);

    TFuture<TReadResult>::TPtr AsyncReadBlocks(const yvector<int>& blockIndexes);

private:
    typedef TIntrusivePtr<TRemoteReader> TPtr;

    typedef NChunkHolder::TChunkHolderProxy TProxy;
    USE_RPC_PROXY_METHOD(TProxy, GetBlocks);

    void DoReadBlocks(
        const yvector<int>& blockIndexes, 
        TFuture<TReadResult>::TPtr result);

    void OnBlocksRead(
        TRspGetBlocks::TPtr rsp, 
        TFuture<TReadResult>::TPtr result,
        const yvector<int>& blockIndexes);

    bool ChangeCurrentHolder();

    const TChunkId ChunkId;
    const TDuration Timeout;

    const yvector<Stroka> HolderAddresses;

    int CurrentHolder;

    TMetric ExecutionTime;

    DECLARE_THREAD_AFFINITY_SLOT(Response);
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
