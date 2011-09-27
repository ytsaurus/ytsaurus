#pragma once

#include "chunk_reader.h"
#include "common.h"
#include "../chunk_holder/chunk_holder_rpc.h"

namespace NYT
{

///////////////////////////////////////////////////////////////////////////////

class TRemoteChunkReader
    : public IChunkReader
{
public:
    TRemoteChunkReader(const TChunkId& chunkId, Stroka holderAddress);
    TFuture<TReadResult>::TPtr AsyncReadBlocks(const yvector<int>& blockIndexes);

private:
    typedef TIntrusivePtr<TRemoteChunkReader> TPtr;
    typedef NChunkHolder::TChunkHolderProxy TProxy;
    USE_RPC_PROXY_METHOD(TProxy, GetBlocks);

    void OnBlocksRead(TRspGetBlocks::TPtr rsp, TFuture<TReadResult>::TPtr result);

    TChunkId ChunkId;
    TDuration Timeout;
    TProxy Proxy;

    static NRpc::TChannelCache ChannelCache;
};
///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
