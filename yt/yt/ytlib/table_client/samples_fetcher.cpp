#include "samples_fetcher.h"

#include "key_set.h"

#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/offshore_node_service_proxy.h>

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

TFuture<void> TSamplesFetcher::FetchFromNode(TNodeId nodeId, std::vector<TChunkToFetch> chunks)
{
    return BIND(&TSamplesFetcher::DoFetchFromNode, MakeStrong(this), nodeId, Passed(std::move(chunks)))
        .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
        .Run();
}

TFuture<void> TSamplesFetcher::DoFetchFromNode(TNodeId nodeId, std::vector<TChunkToFetch> chunks)
{
    // TODO(pavel-bash): we should unify how the requests are dispatched
    // to the DataNodeService and to the OffshoreNodeService.
    bool isOffshoreNode = nodeId == OffshoreNodeId;

    TDataNodeServiceProxy proxy(GetNodeChannel(nodeId));
    proxy.SetDefaultTimeout(Config_->NodeRpcTimeout);

    TOffshoreNodeServiceProxy offshoreProxy(GetNodeChannel(nodeId));
    offshoreProxy.SetDefaultTimeout(Config_->NodeRpcTimeout);

    auto req = [&] {
        if (isOffshoreNode) {
            return offshoreProxy.GetTableSamples();
        }
        return proxy.GetTableSamples();
    }();

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

    std::vector<TChunkToFetch> requestedChunks;

    for (const auto& requestedChunk: chunks) {
        if (!requestedChunk.Replica.GetSourceUri().empty()) {
            // TODO(pavel-bash): for now we do not support the samples retrieval process for
            // the external data - chunk meta isn't generated and/or saved anywhere, which
            // will lead to a crash later. Some of the operations still work even without
            // the samples (e.g. sort with small amount of data).
            YT_LOG_INFO(
                "Requested to fetch table samples, but it is not supported yet for the external data; "
                "returning empty samples (SourceUri: %v)",
                requestedChunk.Replica.GetSourceUri());
            return VoidFuture;
        }

        const auto& chunk = Chunks_[requestedChunk.ChunkIndex];

        currentSize += chunk->GetUncompressedDataSize();
        i64 sampleCount = currentSize / SizeBetweenSamples_;

        if (sampleCount > currentSampleCount) {
            requestedChunks.push_back(requestedChunk);
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
            ToProto(sampleRequest->mutable_replica_spec(), requestedChunk.Replica);
            currentSampleCount = sampleCount;
        }
    }

    if (req->sample_requests_size() == 0) {
        return VoidFuture;
    }

    return req->Invoke().Apply(
        BIND(&TSamplesFetcher::OnResponse, MakeStrong(this), nodeId, Passed(std::move(requestedChunks)))
            .AsyncVia(Invoker_));
}

void TSamplesFetcher::OnResponse(
    TNodeId nodeId,
    std::vector<TChunkToFetch> requestedChunks,
    const TDataNodeServiceProxy::TErrorOrRspGetTableSamplesPtr& rspOrError)
{
    if (!rspOrError.IsOK()) {
        YT_LOG_INFO(rspOrError, "Failed to get samples from node (Address: %v, NodeId: %v)",
            GetNodeAddress(nodeId),
            nodeId);
        OnNodeFailed(nodeId, GetChunkIndexes(requestedChunks));
        return;
    }

    const auto& rsp = rspOrError.Value();

    YT_VERIFY(rsp->Attachments().size() == 1);
    TKeySetReader keysReader(rsp->Attachments()[0]);
    auto keys = keysReader.GetKeys();

    for (int index = 0; index < std::ssize(requestedChunks); ++index) {
        const auto& requestedChunk = requestedChunks[index];
        const auto& sampleResponse = rsp->sample_responses(index);

        if (sampleResponse.has_error()) {
            auto error = FromProto<TError>(sampleResponse.error());
            OnChunkFailed(nodeId, requestedChunk, error);
            continue;
        }

        YT_LOG_TRACE("Received %v samples for chunk #%v",
            sampleResponse.samples_size(),
            requestedChunk);

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
