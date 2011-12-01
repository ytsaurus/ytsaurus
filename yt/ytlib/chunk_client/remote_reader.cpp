#include "stdafx.h"
#include "remote_reader.h"
#include "holder_channel_cache.h"
#include "reader_thread.h"

#include "../misc/foreach.h"
#include "../actions/action_util.h"

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

// TODO: use ChunkClientLogger
static NLog::TLogger Logger("ChunkReader");

///////////////////////////////////////////////////////////////////////////////

TRemoteReader::TRemoteReader(
    const TConfig& config,
    const TChunkId& chunkId,
    const yvector<Stroka>& holderAddresses)
    : Config(config)
    , ChunkId(chunkId)
    , HolderAddresses(holderAddresses)
    , ExecutionTime(0, 1000, 20)
{
    CurrentHolder = 0;
}

TFuture<IAsyncReader::TReadResult>::TPtr
TRemoteReader::AsyncReadBlocks(const yvector<int>& blockIndexes)
{
    VERIFY_THREAD_AFFINITY_ANY();
    auto result = New< TFuture<TReadResult> >();

    DoReadBlocks(blockIndexes, result);

    return result;
}

void TRemoteReader::DoReadBlocks(
    const yvector<int>& blockIndexes, 
    TFuture<TReadResult>::TPtr result)
{
    VERIFY_THREAD_AFFINITY_ANY();
    TProxy proxy(~HolderChannelCache->GetChannel(HolderAddresses[CurrentHolder]));
    proxy.SetTimeout(Config.HolderRpcTimeout);

    auto req = proxy.GetBlocks();
    req->SetChunkId(ChunkId.ToProto());

    FOREACH(auto index, blockIndexes) {
        req->AddBlockIndexes(index);
    }

    req->Invoke()->Subscribe(FromMethod(
        &TRemoteReader::OnBlocksRead, 
        TPtr(this), 
        result,
        blockIndexes)->Via(ReaderThread->GetInvoker()));
}

void TRemoteReader::OnBlocksRead(
    TRspGetBlocks::TPtr rsp, 
    TFuture<TReadResult>::TPtr result, 
    const yvector<int>& blockIndexes)
{
    VERIFY_THREAD_AFFINITY(Response);

    if (rsp->IsOK()) {
        ExecutionTime.AddDelta(rsp->GetStartTime());

        TReadResult readResult;
        for (int i = 0; i < rsp->Attachments().ysize(); i++) {
            // Since all attachments reference the same RPC response
            // memory will be freed only when all the blocks die.
            readResult.Blocks.push_back(rsp->Attachments()[i]);
        }
        result->Set(readResult);
    } else if (ChangeCurrentHolder()) {
        DoReadBlocks(blockIndexes, result);
    } else {
        TReadResult readResult;
        readResult.Error = rsp->GetError();
        result->Set(readResult);
    }
}

bool TRemoteReader::ChangeCurrentHolder()
{
    // Thread affinity is important here to ensure no race conditions on #CurrentHolder.
    VERIFY_THREAD_AFFINITY(Response);

    ++CurrentHolder;
    if (CurrentHolder < HolderAddresses.ysize()) {
        return true;
    } else {
        return false;
    }
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
