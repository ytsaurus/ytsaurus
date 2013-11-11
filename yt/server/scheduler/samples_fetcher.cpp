#include "stdafx.h"
#include "samples_fetcher.h"
#include "private.h"
#include "chunk_pool.h"

#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <core/rpc/channel_cache.h>

#include <core/misc/protobuf_helpers.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/scheduler/config.h>

namespace NYT {
namespace NScheduler {

using namespace NChunkClient;
using namespace NNodeTrackerClient;

using NChunkClient::NProto::TKey;

////////////////////////////////////////////////////////////////////

static NRpc::TChannelCache ChannelCache;

////////////////////////////////////////////////////////////////////

TSamplesFetcher::TSamplesFetcher(
    TSchedulerConfigPtr config,
    TSortOperationSpecPtr spec,
    const TOperationId& operationId)
    : Config(config)
    , Spec(spec)
    , DesiredSampleCount(0)
    , SizeBetweenSamples(0)
    , CurrentSize(0)
    , CurrentSampleCount(0)
    , Logger(OperationLogger)

{
    Logger.AddTag(Sprintf("OperationId: %s", ~ToString(operationId)));
}

void TSamplesFetcher::SetDesiredSampleCount(int desiredSamplesCount)
{
    DesiredSampleCount = desiredSamplesCount;
}

NLog::TTaggedLogger& TSamplesFetcher::GetLogger()
{
    return Logger;
}

void TSamplesFetcher::Prepare(const std::vector<TRefCountedChunkSpecPtr>& chunks)
{
    YCHECK(DesiredSampleCount > 0);

    LOG_INFO("Started fetching chunk samples (ChunkCount: %d, DesiredSampleCount: %d)",
        static_cast<int>(chunks.size()),
        DesiredSampleCount);

    i64 totalSize = 0;
    for (const auto& chunk : chunks) {
        i64 chunkDataSize;
        GetStatistics(*chunk, &chunkDataSize);
        totalSize += chunkDataSize;
    }
    YCHECK(totalSize > 0);

    if (totalSize < DesiredSampleCount) {
        SizeBetweenSamples = 1;
    } else {
        SizeBetweenSamples = totalSize / DesiredSampleCount;
    }
    CurrentSize = SizeBetweenSamples;
}

const std::vector<TKey>& TSamplesFetcher::GetSamples() const
{
    return Samples;
}

void TSamplesFetcher::CreateNewRequest(const TNodeDescriptor& descriptor)
{
    auto channel = ChannelCache.GetChannel(descriptor.Address);
    auto retryingChannel = CreateRetryingChannel(Config->NodeChannel, channel);
    TDataNodeServiceProxy proxy(retryingChannel);
    proxy.SetDefaultTimeout(Config->NodeRpcTimeout);

    CurrentRequest = proxy.GetTableSamples();
    ToProto(CurrentRequest->mutable_key_columns(), Spec->SortBy);
}

bool TSamplesFetcher::AddChunkToRequest(
    TNodeId nodeId,
    TRefCountedChunkSpecPtr chunk)
{
    i64 chunkDataSize;
    GetStatistics(*chunk, &chunkDataSize);

    CurrentSize += chunkDataSize;
    i64 sampleCount = CurrentSize / SizeBetweenSamples;

    if (sampleCount > CurrentSampleCount) {
        auto chunkSampleCount = sampleCount - CurrentSampleCount;
        CurrentSampleCount = sampleCount;

        auto* sampleRequest = CurrentRequest->add_sample_requests();
        auto chunkId = EncodeChunkId(*chunk, nodeId);
        ToProto(sampleRequest->mutable_chunk_id(), chunkId);
        sampleRequest->set_sample_count(chunkSampleCount);

        return true;
    }

    return false;
}

auto TSamplesFetcher::InvokeRequest() -> TFuture<TResponsePtr>
{
    auto req = CurrentRequest;
    CurrentRequest.Reset();
    return req->Invoke();
}

TError TSamplesFetcher::ProcessResponseItem(
    TResponsePtr rsp,
    int index,
    TRefCountedChunkSpecPtr chunk)
{
    YCHECK(rsp->IsOK());

    const auto& chunkSamples = rsp->samples(index);
    if (chunkSamples.has_error()) {
        return FromProto(chunkSamples.error());
    }

    LOG_TRACE("Received %d samples for chunk #%d",
        chunkSamples.items_size(),
        index);

    for (const auto& sample : chunkSamples.items()) {
        Samples.push_back(sample);
    }

    return TError();
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

