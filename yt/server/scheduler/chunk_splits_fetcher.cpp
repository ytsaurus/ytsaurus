#include "stdafx.h"
#include "chunk_splits_fetcher.h"
#include "private.h"

#include <core/misc/protobuf_helpers.h>

#include <core/rpc/channel.h>
#include <core/rpc/caching_channel_factory.h>
#include <core/rpc/bus_channel.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <ytlib/table_client/chunk_meta_extensions.h>
#include <ytlib/table_client/private.h>

#include <ytlib/scheduler/config.h>

namespace NYT {
namespace NScheduler {

using namespace NRpc;
using namespace NNodeTrackerClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTableClient;
using namespace NTableClient::NProto;

////////////////////////////////////////////////////////////////////

static IChannelFactoryPtr ChannelFactory(CreateCachingChannelFactory(GetBusChannelFactory()));

////////////////////////////////////////////////////////////////////

TChunkSplitsFetcher::TChunkSplitsFetcher(
    TSchedulerConfigPtr config,
    TMergeOperationSpecBasePtr spec,
    const TOperationId& operationId,
    const TKeyColumns& keyColumns,
    i64 chunkSliceSize)
    : Config(config)
    , Spec(spec)
    , KeyColumns(keyColumns)
    , Logger(OperationLogger)
    , ChunkSliceSize(chunkSliceSize)
{
    YCHECK(ChunkSliceSize > 0);
    Logger.AddTag(Sprintf("OperationId: %s", ~ToString(operationId)));
}

NLog::TTaggedLogger& TChunkSplitsFetcher::GetLogger()
{
    return Logger;
}

void TChunkSplitsFetcher::Prepare(const std::vector<TRefCountedChunkSpecPtr>& chunks)
{
    LOG_INFO("Started fetching chunk splits (ChunkCount: %d)",
        static_cast<int>(chunks.size()));
}

const std::vector<TRefCountedChunkSpecPtr>& TChunkSplitsFetcher::GetChunkSplits()
{
    return ChunkSplits;
}

void TChunkSplitsFetcher::CreateNewRequest(const TNodeDescriptor& descriptor)
{
    auto channel = ChannelFactory->CreateChannel(descriptor.Address);
    auto retryingChannel = CreateRetryingChannel(Config->NodeChannel, channel);
    
    TDataNodeServiceProxy proxy(retryingChannel);
    proxy.SetDefaultTimeout(Config->NodeRpcTimeout);

    CurrentRequest = proxy.GetChunkSplits();
    CurrentRequest->set_min_split_size(ChunkSliceSize);
    ToProto(CurrentRequest->mutable_key_columns(), KeyColumns);
}

bool TChunkSplitsFetcher::AddChunkToRequest(
    TNodeId nodeId,
    TRefCountedChunkSpecPtr chunk)
{
    auto chunkId = EncodeChunkId(*chunk, nodeId);

    i64 dataSize;
    GetStatistics(*chunk, &dataSize);

    auto boundaryKeys = GetProtoExtension<TBoundaryKeysExt>(chunk->chunk_meta().extensions());

    if (dataSize < ChunkSliceSize ||
        CompareKeys(boundaryKeys.start(), boundaryKeys.end(), KeyColumns.size()) == 0)
    {
        LOG_DEBUG("Chunk split added (ChunkId: %s, TableIndex: %d)",
            ~ToString(chunkId),
            chunk->table_index());

        ChunkSplits.push_back(chunk);
        return false;
    } else {
        auto* requestChunk = CurrentRequest->add_chunk_specs();
        *requestChunk = *chunk;
        // Makes sense for erasure chunks only.
        ToProto(requestChunk->mutable_chunk_id(), chunkId);
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
    TRefCountedChunkSpecPtr chunkSpec)
{
    YCHECK(rsp->IsOK());

    const auto& responseChunks = rsp->splitted_chunks(index);
    if (responseChunks.has_error()) {
        return FromProto(responseChunks.error());
    }

    LOG_TRACE("Received %d chunk splits for chunk #%d",
        responseChunks.chunk_specs_size(),
        index);

    for (auto& responseChunk : responseChunks.chunk_specs()) {
        auto split = New<TRefCountedChunkSpec>(std::move(responseChunk));
        // Adjust chunk id (makes sense for erasure chunks only).
        auto chunkId = FromProto<TChunkId>(split->chunk_id());
        auto chunkIdWithIndex = DecodeChunkId(chunkId);
        ToProto(split->mutable_chunk_id(), chunkIdWithIndex.Id);
        split->set_table_index(chunkSpec->table_index());
        ChunkSplits.push_back(split);
    }

    return TError();
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
