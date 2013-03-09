#include "stdafx.h"
#include "samples_fetcher.h"
#include "private.h"
#include "chunk_pool.h"

#include <ytlib/table_client/helpers.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <ytlib/rpc/channel_cache.h>

#include <ytlib/misc/protobuf_helpers.h>

#include <ytlib/scheduler/config.h>

namespace NYT {
namespace NScheduler {

using namespace NChunkClient;

using NTableClient::NProto::TKey;

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

void TSamplesFetcher::Prepare(const std::vector<NTableClient::TRefCountedInputChunkPtr>& chunks)
{
    YCHECK(DesiredSampleCount > 0);

    LOG_INFO("Started fetching chunk samples (ChunkCount: %d, DesiredSampleCount: %d)",
        static_cast<int>(chunks.size()),
        DesiredSampleCount);

    i64 totalSize = 0;
    FOREACH (const auto& chunk, chunks) {
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

bool TSamplesFetcher::AddChunkToRequest(NTableClient::TRefCountedInputChunkPtr chunk)
{
    i64 chunkDataSize;
    GetStatistics(*chunk, &chunkDataSize);

    CurrentSize += chunkDataSize;
    i64 sampleCount = CurrentSize / SizeBetweenSamples;

    if (sampleCount > CurrentSampleCount) {
        auto chunkSampleCount = sampleCount - CurrentSampleCount;
        CurrentSampleCount = sampleCount;
        auto chunkId = FromProto<TChunkId>(chunk->chunk_id());

        auto* sampleRequest = CurrentRequest->add_sample_requests();
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
    NTableClient::TRefCountedInputChunkPtr chunk)
{
    YCHECK(rsp->IsOK());

    const auto& chunkSamples = rsp->samples(index);
    if (chunkSamples.has_error()) {
        return FromProto(chunkSamples.error());
    }

    LOG_TRACE("Received %d samples for chunk #%d",
        chunkSamples.items_size(),
        index);

    FOREACH (const auto& sample, chunkSamples.items()) {
        Samples.push_back(sample);
    }

    return TError();
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

