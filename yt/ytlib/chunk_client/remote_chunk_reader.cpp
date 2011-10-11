#include "remote_chunk_reader.h"
#include "holder_channel_cache.h"

#include "../misc/foreach.h"
#include "../actions/action_util.h"

namespace NYT
{

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("ChunkReader");

///////////////////////////////////////////////////////////////////////////////

TRemoteChunkReader::TRemoteChunkReader(const TChunkId& chunkId, const yvector<Stroka>& holderAddresses)
    : ChunkId(chunkId)
    , Timeout(TDuration::Seconds(15)) // ToDo: make configurable
    , HolderAddresses(holderAddresses)
    , ExecutionTime(0, 1000, 20)
{
    CurrentHolder = 0;
}

TFuture<IChunkReader::TReadResult>::TPtr
TRemoteChunkReader::AsyncReadBlocks(const yvector<int>& blockIndexes)
{
    auto result = New< TFuture<TReadResult> >();

    DoReadBlocks(blockIndexes, result);

    return result;
}

void TRemoteChunkReader::DoReadBlocks(
    const yvector<int>& blockIndexes, 
    TFuture<TReadResult>::TPtr result)
{
    TProxy proxy(~HolderChannelCache->GetChannel(HolderAddresses[CurrentHolder]));
    auto req = proxy.GetBlocks();
    req->SetChunkId(ChunkId.ToProto());

    FOREACH(auto index, blockIndexes) {
        req->AddBlockIndexes(index);
    }

    req->Invoke(Timeout)->Subscribe(FromMethod(
        &TRemoteChunkReader::OnBlocksRead, 
        TPtr(this), 
        result,
        blockIndexes));
}

void TRemoteChunkReader::OnBlocksRead(
    TRspGetBlocks::TPtr rsp, 
    TFuture<TReadResult>::TPtr result, 
    const yvector<int>& blockIndexes)
{
    if (rsp->IsOK()) {
        ExecutionTime.AddDelta(rsp->GetInvokeInstant());

        TReadResult readResult;
        for (int i = 0; i < rsp->Attachments().ysize(); i++) {
            // Since all attachments reference the same rpc response
            // memory will be freed only when all the blocks die
            readResult.Blocks.push_back(rsp->Attachments()[i]);
        }

        readResult.Result = EResult::OK;
        result->Set(readResult);
    } else if (ChangeCurrentHolder()) {
        DoReadBlocks(blockIndexes, result);
    } else {
        TReadResult readResult;
        readResult.Result = EResult::Failed;
        result->Set(readResult);
    }
}

bool TRemoteChunkReader::ChangeCurrentHolder()
{
    ++CurrentHolder;
    if (CurrentHolder < HolderAddresses.ysize()) {
        return true;
    } else {
        return false;
    }
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
