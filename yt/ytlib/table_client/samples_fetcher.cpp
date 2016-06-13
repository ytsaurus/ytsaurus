#include "samples_fetcher.h"

#include <yt/ytlib/chunk_client/config.h>
#include <yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/ytlib/chunk_client/input_chunk.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/scheduler/config.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/protobuf_helpers.h>

#include <yt/core/rpc/channel.h>

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NRpc;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////

bool operator==(const TSample& lhs, const TSample& rhs)
{
    return lhs.Key == rhs.Key;
}

bool operator<(const TSample& lhs, const TSample& rhs)
{
    auto result = CompareRows(lhs.Key, rhs.Key);
    if (result == 0) {
        return lhs.Incomplete < rhs.Incomplete;
    }

    return result < 0;
}

////////////////////////////////////////////////////////////////////

TSamplesFetcher::TSamplesFetcher(
    TFetcherConfigPtr config,
    i64 desiredSampleCount,
    const TKeyColumns& keyColumns,
    int maxSampleSize,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    IInvokerPtr invoker,
    TScrapeChunksCallback scraperCallback,
    NApi::INativeClientPtr client,
    const NLogging::TLogger& logger)
    : TFetcherBase(
        config,
        nodeDirectory,
        invoker,
        scraperCallback,
        client,
        logger)
    , KeyColumns_(keyColumns)
    , DesiredSampleCount_(desiredSampleCount)
    , MaxSampleSize_(maxSampleSize)
{
    YCHECK(DesiredSampleCount_ > 0);
}

void TSamplesFetcher::AddChunk(TInputChunkPtr chunk)
{
    TotalDataSize_ += chunk->GetUncompressedDataSize();

    TFetcherBase::AddChunk(chunk);
}

TFuture<void> TSamplesFetcher::Fetch()
{
    LOG_DEBUG("Started fetching chunk samples (ChunkCount: %v, DesiredSampleCount: %v)",
        Chunks_.size(),
        DesiredSampleCount_);

    if (TotalDataSize_ < DesiredSampleCount_) {
        SizeBetweenSamples_ = 1;
    } else {
        SizeBetweenSamples_ = TotalDataSize_ / DesiredSampleCount_;
    }

    return TFetcherBase::Fetch();
}

const std::vector<TSample>& TSamplesFetcher::GetSamples() const
{
    return Samples_;
}

TFuture<void> TSamplesFetcher::FetchFromNode(TNodeId nodeId, std::vector<int> chunkIndexes)
{
    return BIND(&TSamplesFetcher::DoFetchFromNode, MakeWeak(this), nodeId, Passed(std::move(chunkIndexes)))
        .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
        .Run();
}

void TSamplesFetcher::DoFetchFromNode(TNodeId nodeId, std::vector<int> chunkIndexes)
{
    TDataNodeServiceProxy proxy(GetNodeChannel(nodeId));
    proxy.SetDefaultTimeout(Config_->NodeRpcTimeout);

    auto req = proxy.GetTableSamples();
    ToProto(req->mutable_key_columns(), KeyColumns_);
    req->set_max_sample_size(MaxSampleSize_);
    // TODO(babenko): make configurable
    ToProto(req->mutable_workload_descriptor(), TWorkloadDescriptor(EWorkloadCategory::UserBatch));

    i64 currentSize = SizeBetweenSamples_;
    i64 currentSampleCount = 0;

    std::vector<int> requestedChunkIndexes;

    for (auto index : chunkIndexes) {
        const auto& chunk = Chunks_[index];

        currentSize += chunk->GetUncompressedDataSize();
        i64 sampleCount = currentSize / SizeBetweenSamples_;

        if (sampleCount > currentSampleCount) {
            requestedChunkIndexes.push_back(index);
            auto chunkId = EncodeChunkId(chunk, nodeId);

            auto* sampleRequest = req->add_sample_requests();
            ToProto(sampleRequest->mutable_chunk_id(), chunkId);
            sampleRequest->set_sample_count(sampleCount - currentSampleCount);
            if (chunk->LowerLimit() && chunk->LowerLimit()->has_key()) {
                sampleRequest->set_lower_key(chunk->LowerLimit()->key());
            }
            if (chunk->UpperLimit() && chunk->UpperLimit()->has_key()) {
                sampleRequest->set_upper_key(chunk->UpperLimit()->key());
            }
            currentSampleCount = sampleCount;
        }
    }

    if (req->sample_requests_size() == 0)
        return;

    auto rspOrError = WaitFor(req->Invoke());

    if (!rspOrError.IsOK()) {
        LOG_WARNING("Failed to get samples from node (Address: %v, NodeId: %v)",
            NodeDirectory_->GetDescriptor(nodeId).GetDefaultAddress(),
            nodeId);
        OnNodeFailed(nodeId, requestedChunkIndexes);
        return;
    }

    const auto& rsp = rspOrError.Value();
    for (int index = 0; index < requestedChunkIndexes.size(); ++index) {
        const auto& sampleResponse = rsp->sample_responses(index);

        if (sampleResponse.has_error()) {
            auto error = FromProto<TError>(sampleResponse.error());
            OnChunkFailed(nodeId, requestedChunkIndexes[index], error);
            continue;
        }

        LOG_TRACE("Received %v samples for chunk #%v",
            sampleResponse.samples_size(),
            requestedChunkIndexes[index]);

        for (const auto& protoSample : sampleResponse.samples()) {
            TSample sample = {
                FromProto<TOwningKey>(protoSample.key()),
                protoSample.incomplete(),
                protoSample.weight()
            };

            YCHECK(sample.Key.GetCount() == KeyColumns_.size());
            Samples_.push_back(sample);
        }
    }
}

////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

