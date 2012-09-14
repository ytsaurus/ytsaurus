#include "stdafx.h"
#include "samples_fetcher.h"
#include "private.h"

#include <ytlib/table_client/key.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <ytlib/rpc/channel_cache.h>

#include <ytlib/misc/protobuf_helpers.h>

#include <ytlib/scheduler/config.h>

namespace NYT {
namespace NScheduler {

using namespace NChunkClient;
using namespace NTableClient::NProto;
using namespace NChunkClient::NProto;

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
    Logger.AddTag(Sprintf("OperationId: %s", ~operationId.ToString()));
}

void TSamplesFetcher::SetDesiredSamplesCount(int desiredSamplesCount)
{
    DesiredSampleCount = desiredSamplesCount;
}

NLog::TTaggedLogger& TSamplesFetcher::GetLogger()
{
    return Logger;
}

bool TSamplesFetcher::Prepare(const std::vector<NTableClient::TRefCountedInputChunkPtr>& chunks)
{
    YCHECK(DesiredSampleCount > 0);
    i64 totalSize = 0;
    FOREACH(const auto& chunk, chunks) {
        auto miscExt = GetProtoExtension<TMiscExt>(chunk->extensions());
        totalSize += miscExt.uncompressed_data_size();
    }
    YCHECK(totalSize > 0);

    if (totalSize < DesiredSampleCount) {
        SizeBetweenSamples = 1;
    } else {
        SizeBetweenSamples = totalSize / DesiredSampleCount;
    }
    CurrentSize = SizeBetweenSamples;
    return true;
}

const std::vector<TKey>& TSamplesFetcher::GetSamples() const
{
    return Samples;
}

void TSamplesFetcher::CreateNewRequest(const Stroka& address)
{
    auto channel = ChannelCache.GetChannel(address);
    TChunkHolderServiceProxy proxy(channel);
    proxy.SetDefaultTimeout(Config->NodeRpcTimeout);

    CurrentRequest = proxy.GetTableSamples();
}

bool TSamplesFetcher::AddChunkToRequest(NTableClient::TRefCountedInputChunkPtr& chunk)
{
    auto miscExt = GetProtoExtension<TMiscExt>(chunk->extensions());
    CurrentSize += miscExt.uncompressed_data_size();
    i64 sampleCount = CurrentSize / SizeBetweenSamples;

    if (sampleCount > CurrentSampleCount) {
        auto chunkSampleCount = sampleCount - CurrentSampleCount;
        CurrentSampleCount = sampleCount;
        auto chunkId = TChunkId::FromProto(chunk->slice().chunk_id());

        auto* sampleRequest = CurrentRequest->add_sample_requests();
        *sampleRequest->mutable_chunk_id() = chunkId.ToProto();
        sampleRequest->set_sample_count(chunkSampleCount);
        return true;
    }

    return false;
}

auto TSamplesFetcher::InvokeRequest() -> TFuture<TResponsePtr>
{
    auto req(MoveRV(CurrentRequest));
    return req->Invoke();
}

TError TSamplesFetcher::ProcessResponseItem(
    const TResponsePtr& rsp, 
    int index, 
    NTableClient::TRefCountedInputChunkPtr& chunk)
{
    YASSERT(rsp->IsOK());

    const auto& chunkSamples = rsp->samples(index);
    if (chunkSamples.has_error()) {
            auto error = FromProto(chunkSamples.error());
            return error;
    } else {
        LOG_TRACE("Received %d samples for chunk number %d",
            chunkSamples.items_size(),
            index);
        FOREACH (const auto& sample, chunkSamples.items()) {
            Samples.push_back(sample);
        }
        return TError();
    }
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

