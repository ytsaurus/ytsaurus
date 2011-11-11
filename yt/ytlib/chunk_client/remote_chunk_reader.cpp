#include "stdafx.h"
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
    VERIFY_THREAD_AFFINITY_ANY();
    CurrentHolder = 0;
}

TFuture<IChunkReader::TReadResult>::TPtr
TRemoteChunkReader::AsyncReadBlocks(const yvector<int>& blockIndexes)
{
    VERIFY_THREAD_AFFINITY_ANY();
    auto result = New< TFuture<TReadResult> >();

    DoReadBlocks(blockIndexes, result);

    return result;
}

void TRemoteChunkReader::DoReadBlocks(
    const yvector<int>& blockIndexes, 
    TFuture<TReadResult>::TPtr result)
{
    VERIFY_THREAD_AFFINITY_ANY();
    TProxy proxy(HolderChannelCache->GetChannel(HolderAddresses[CurrentHolder]));
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
    VERIFY_THREAD_AFFINITY(Response);

    if (rsp->IsOK()) {
        ExecutionTime.AddDelta(rsp->GetStartTime());

        TReadResult readResult;
        for (int i = 0; i < rsp->Attachments().ysize(); i++) {
            // Since all attachments reference the same rpc response
            // memory will be freed only when all the blocks die
            readResult.Blocks.push_back(rsp->Attachments()[i]);
        }

        readResult.IsOK = true;
        result->Set(readResult);
    } else if (ChangeCurrentHolder()) {
        DoReadBlocks(blockIndexes, result);
    } else {
        TReadResult readResult;
        readResult.IsOK = false;
        result->Set(readResult);
    }
}

bool TRemoteChunkReader::ChangeCurrentHolder()
{
    // Thread affinity is important here to ensure no race conditions on #CurrentHolder 
    VERIFY_THREAD_AFFINITY(Response);

    ++CurrentHolder;
    if (CurrentHolder < HolderAddresses.ysize()) {
        return true;
    } else {
        return false;
    }
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
