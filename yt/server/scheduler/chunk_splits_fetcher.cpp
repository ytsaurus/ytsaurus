#include "stdafx.h"
#include "chunk_splits_fetcher.h"
#include "private.h"

#include <ytlib/table_client/helpers.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <ytlib/rpc/channel_cache.h>

#include <ytlib/misc/protobuf_helpers.h>

#include <ytlib/scheduler/config.h>

namespace NYT {
namespace NScheduler {

using namespace NNodeTrackerClient;
using namespace NChunkClient;
using namespace NTableClient;

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
    YCHECK(Config->MergeJobMaxSliceDataSize > 0);
    Logger.AddTag(Sprintf("OperationId: %s", ~ToString(operationId)));
}

NLog::TTaggedLogger& TChunkSplitsFetcher::GetLogger()
{
    return Logger;
}

void TChunkSplitsFetcher::Prepare(const std::vector<NTableClient::TRefCountedInputChunkPtr>& chunks)
{
    LOG_INFO("Started fetching chunk splits (ChunkCount: %d)",
        static_cast<int>(chunks.size()));
}

const std::vector<TRefCountedInputChunkPtr>& TChunkSplitsFetcher::GetChunkSplits()
{
    return ChunkSplits;
}

void TChunkSplitsFetcher::CreateNewRequest(const TNodeDescriptor& descriptor)
{
    auto channel = ChannelCache.GetChannel(descriptor.Address);
    auto retryingChannel = CreateRetryingChannel(Config->NodeChannel, channel);
    TDataNodeServiceProxy proxy(retryingChannel);
    proxy.SetDefaultTimeout(Config->NodeRpcTimeout);

    CurrentRequest = proxy.GetChunkSplits();
    CurrentRequest->set_min_split_size(Config->MergeJobMaxSliceDataSize);
    ToProto(CurrentRequest->mutable_key_columns(), KeyColumns);
}

bool TChunkSplitsFetcher::AddChunkToRequest(NTableClient::TRefCountedInputChunkPtr chunk)
{
    auto chunkId = FromProto<TChunkId>(chunk->chunk_id());

    i64 dataSize;
    GetStatistics(*chunk, &dataSize);

    if (dataSize < Config->MergeJobMaxSliceDataSize) {
        LOG_DEBUG("Chunk split added (ChunkId: %s, TableIndex: %d)",
            ~ToString(chunkId),
            chunk->table_index());
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
        ChunkSplits.push_back(New<TRefCountedInputChunk>(splittedChunk, inputChunk->table_index()));
    }

    return TError();
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
