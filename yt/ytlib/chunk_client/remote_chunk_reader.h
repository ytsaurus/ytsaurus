#pragma once

#include "common.h"
#include "chunk_reader.h"

#include "../chunk_holder/chunk_holder_rpc.h"
#include "../misc/metric.h"

namespace NYT
{

///////////////////////////////////////////////////////////////////////////////

class TRemoteChunkReader
    : public IChunkReader
{
public:
    TRemoteChunkReader(const TChunkId& chunkId, const yvector<Stroka>& holderAddresses);
    TFuture<TReadResult>::TPtr AsyncReadBlocks(const yvector<int>& blockIndexes);

private:
    typedef TIntrusivePtr<TRemoteChunkReader> TPtr;

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

    TChunkId ChunkId;
    TDuration Timeout;

    yvector<Stroka> HolderAddresses;

    int CurrentHolder;

    TMetric ExecutionTime;

};
///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
