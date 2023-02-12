#include "samples_fetcher.h"

#include "key_set.h"

#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/rpc/helpers.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/rpc/channel.h>

namespace NYT::NTableClient {

using namespace NTableClient;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NRpc;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TSamplesFetcher::TSamplesFetcher(
    TFetcherConfigPtr config,
    ESamplingPolicy samplingPolicy,
    int desiredSampleCount,
    const TKeyColumns& keyColumns,
    i64 maxSampleSize,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    IInvokerPtr invoker,
    TRowBufferPtr rowBuffer,
    IFetcherChunkScraperPtr chunkScraper,
    NApi::NNative::IClientPtr client,
    const NLogging::TLogger& logger)
    : TFetcherBase(
        config,
        nodeDirectory,
        invoker,
        chunkScraper,
        client,
        logger)
    , RowBuffer_(std::move(rowBuffer))
    , SamplingPolicy_(samplingPolicy)
    , KeyColumns_(keyColumns)
    , DesiredSampleCount_(desiredSampleCount)
    , MaxSampleSize_(maxSampleSize)
{
    YT_VERIFY(DesiredSampleCount_ > 0);
}

void TSamplesFetcher::AddChunk(TInputChunkPtr chunk)
{
    TotalDataSize_ += chunk->GetUncompressedDataSize();

    TFetcherBase::AddChunk(chunk);
}

TFuture<void> TSamplesFetcher::Fetch()
{
    YT_LOG_DEBUG("Started fetching chunk samples (ChunkCount: %v, DesiredSampleCount: %v)",
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

void TSamplesFetcher::ProcessDynamicStore(int /*chunkIndex*/)
{
    // Dynamic stores do not have samples so nothing to do here.
}

TFuture<void> TSamplesFetcher::FetchFromNode(TNodeId nodeId, std::vector<int> chunkIndexes)
{
    return BIND(&TSamplesFetcher::DoFetchFromNode, MakeStrong(this), nodeId, Passed(std::move(chunkIndexes)))
        .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
        .Run();
}

TFuture<void> TSamplesFetcher::DoFetchFromNode(TNodeId nodeId, const std::vector<int>& chunkIndexes)
{
    TDataNodeServiceProxy proxy(GetNodeChannel(nodeId));
    proxy.SetDefaultTimeout(Config_->NodeRpcTimeout);

    auto req = proxy.GetTableSamples();
    // TODO(babenko): make configurable
    SetRequestWorkloadDescriptor(req, TWorkloadDescriptor(EWorkloadCategory::UserBatch));
    req->SetRequestHeavy(true);
    req->SetResponseHeavy(true);
    req->SetMultiplexingBand(EMultiplexingBand::Heavy);
    ToProto(req->mutable_key_columns(), KeyColumns_);
    req->set_max_sample_size(MaxSampleSize_);
    req->set_sampling_policy(ToProto<int>(SamplingPolicy_));

    i64 currentSize = SizeBetweenSamples_;
    i64 currentSampleCount = 0;

    std::vector<int> requestedChunkIndexes;

    for (int index : chunkIndexes) {
        const auto& chunk = Chunks_[index];

        currentSize += chunk->GetUncompressedDataSize();
        i64 sampleCount = currentSize / SizeBetweenSamples_;

        if (sampleCount > currentSampleCount) {
            requestedChunkIndexes.push_back(index);
            auto chunkId = EncodeChunkId(chunk, nodeId);

            auto* sampleRequest = req->add_sample_requests();
            ToProto(sampleRequest->mutable_chunk_id(), chunkId);
            sampleRequest->set_sample_count(sampleCount - currentSampleCount);
            if (chunk->LowerLimit() && chunk->LowerLimit()->HasLegacyKey()) {
                ToProto(sampleRequest->mutable_lower_key(), chunk->LowerLimit()->GetLegacyKey());
            }
            if (chunk->UpperLimit() && chunk->UpperLimit()->HasLegacyKey()) {
                ToProto(sampleRequest->mutable_upper_key(), chunk->UpperLimit()->GetLegacyKey());
            }
            currentSampleCount = sampleCount;
        }
    }

    if (req->sample_requests_size() == 0) {
        return VoidFuture;
    }

    return req->Invoke().Apply(
        BIND(&TSamplesFetcher::OnResponse, MakeStrong(this), nodeId, Passed(std::move(requestedChunkIndexes)))
            .AsyncVia(Invoker_));
}

void TSamplesFetcher::OnResponse(
    TNodeId nodeId,
    const std::vector<int>& requestedChunkIndexes,
    const TDataNodeServiceProxy::TErrorOrRspGetTableSamplesPtr& rspOrError)
{
    if (!rspOrError.IsOK()) {
        YT_LOG_INFO(rspOrError, "Failed to get samples from node (Address: %v, NodeId: %v)",
            NodeDirectory_->GetDescriptor(nodeId).GetDefaultAddress(),
            nodeId);
        OnNodeFailed(nodeId, requestedChunkIndexes);
        return;
    }

    const auto& rsp = rspOrError.Value();

    YT_VERIFY(rsp->Attachments().size() == 1);
    TKeySetReader keysReader(rsp->Attachments()[0]);
    auto keys = keysReader.GetKeys();

    for (int index = 0; index < std::ssize(requestedChunkIndexes); ++index) {
        const auto& sampleResponse = rsp->sample_responses(index);

        if (sampleResponse.has_error()) {
            auto error = FromProto<TError>(sampleResponse.error());
            OnChunkFailed(nodeId, requestedChunkIndexes[index], error);
            continue;
        }

        YT_LOG_TRACE("Received %v samples for chunk #%v",
            sampleResponse.samples_size(),
            requestedChunkIndexes[index]);

        for (const auto& protoSample : sampleResponse.samples()) {
            auto key = RowBuffer_->CaptureRow(keys[protoSample.key_index()]);

            TSample sample{
                key,
                protoSample.incomplete(),
                protoSample.weight()
            };

            YT_VERIFY(sample.Key.GetCount() == KeyColumns_.size());
            Samples_.push_back(sample);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

