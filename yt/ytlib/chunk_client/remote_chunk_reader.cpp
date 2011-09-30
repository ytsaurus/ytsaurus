#include "remote_chunk_reader.h"
#include "../actions/action_util.h"

namespace NYT
{

NRpc::TChannelCache TRemoteChunkReader::ChannelCache;

TRemoteChunkReader::TRemoteChunkReader(const TChunkId& chunkId, Stroka holderAddress)
    : ChunkId(chunkId)
    , Timeout(TDuration::Seconds(15)) // ToDo: make configurable
    , Proxy(~TRemoteChunkReader::ChannelCache.GetChannel(holderAddress))
{ }

TFuture<IChunkReader::TReadResult>::TPtr
TRemoteChunkReader::AsyncReadBlocks(const yvector<int>& blockIndexes)
{
    auto result = New< TFuture<TReadResult> >();
    auto req = Proxy.GetBlocks();
    req->SetChunkId(ChunkId.ToProto());

    for (int i = 0; i < blockIndexes.ysize(); ++i) {
        req->AddBlockIndexes(blockIndexes[i]);
    }

    req->Invoke(Timeout)->Subscribe(FromMethod(
        &TRemoteChunkReader::OnBlocksRead, 
        TPtr(this), 
        result));

    return result;
}

void TRemoteChunkReader::OnBlocksRead(TRspGetBlocks::TPtr rsp, TFuture<TReadResult>::TPtr result)
{
    TReadResult readResult;
    for (int i = 0; i < rsp->Attachments().ysize(); i++) {
        // Since all attachments reference the same rpc response
        // memory will be freed only when all the blocks will die
        readResult.Blocks.push_back(rsp->Attachments()[i]);
    }

    result->Set(readResult);
}

} // namespace NYT
