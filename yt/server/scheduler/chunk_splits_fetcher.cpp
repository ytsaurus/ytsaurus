#include "stdafx.h"
#include "chunk_splits_fetcher.h"
#include "private.h"

#include <ytlib/table_client/key.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <ytlib/rpc/channel_cache.h>

#include <ytlib/misc/protobuf_helpers.h>

#include <ytlib/scheduler/config.h>

namespace NYT {
namespace NScheduler {

using namespace NChunkClient;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////

static NRpc::TChannelCache ChannelCache;

////////////////////////////////////////////////////////////////////

TChunkSplitsFetcher::TChunkSplitsFetcher(
    TSchedulerConfigPtr config,
    TMergeOperationSpecBasePtr spec,
    const TOperationId& operationId,
    const TKeyColumns& keyColumns)
    : Config(config)
    , Spec(spec)
    , KeyColumns(keyColumns)
    , Logger(OperationLogger)

{
    YCHECK(Spec->JobSliceDataSize > 0);
    Logger.AddTag(Sprintf("OperationId: %s", ~operationId.ToString()));
}

NLog::TTaggedLogger& TChunkSplitsFetcher::GetLogger()
{
    return Logger;
}

bool TChunkSplitsFetcher::Prepare(const std::vector<NTableClient::TRefCountedInputChunkPtr>& chunks)
{
    return true;
}

std::vector<TRefCountedInputChunkPtr>& TChunkSplitsFetcher::GetChunkSplits()
{
    return ChunkSplits;
}

void TChunkSplitsFetcher::CreateNewRequest(const Stroka& address)
{
    auto channel = ChannelCache.GetChannel(address);
    TDataNodeServiceProxy proxy(channel);
    proxy.SetDefaultTimeout(Config->NodeRpcTimeout);

    CurrentRequest = proxy.GetChunkSplits();
    CurrentRequest->set_min_split_size(Spec->JobSliceDataSize);
    ToProto(CurrentRequest->mutable_key_columns(), KeyColumns);
}

bool TChunkSplitsFetcher::AddChunkToRequest(NTableClient::TRefCountedInputChunkPtr chunk)
{
    auto chunkId = TChunkId::FromProto(chunk->slice().chunk_id());
    if (chunk->uncompressed_data_size() < Spec->JobSliceDataSize) {
        LOG_DEBUG("Chunk split added (ChunkId: %s, TableIndex: %d)", 
            ~ToString(chunkId),
            chunk->TableIndex);

        ChunkSplits.push_back(chunk);
        return false;
    } else {
        *CurrentRequest->add_input_chunks() = *chunk;
        return true;
    }
}

TFuture<TChunkSplitsFetcher::TResponsePtr> TChunkSplitsFetcher::InvokeRequest()
{
    auto req = CurrentRequest;
    CurrentRequest.Reset();
    return req->Invoke();
}

TError TChunkSplitsFetcher::ProcessResponseItem(
    TResponsePtr rsp, 
    int index,
    TRefCountedInputChunkPtr inputChunk)
{
    YCHECK(rsp->IsOK());

    const auto& splittedChunks = rsp->splitted_chunks(index);
    if (splittedChunks.has_error()) {
        return FromProto(splittedChunks.error());
    }

    LOG_TRACE("Received %d chunk splits for chunk #%d",
        splittedChunks.input_chunks_size(),
        index);

    FOREACH (const auto& splittedChunk, splittedChunks.input_chunks()) {
        ChunkSplits.push_back(New<TRefCountedInputChunk>(splittedChunk, inputChunk->TableIndex));
    }

    return TError();
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
