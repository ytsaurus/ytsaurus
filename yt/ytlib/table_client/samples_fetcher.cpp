#include "samples_fetcher.h"

#include <yt/ytlib/chunk_client/config.h>
#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/ytlib/chunk_client/input_chunk.h>
#include <yt/ytlib/chunk_client/key_set.h>

#include <yt/client/table_client/row_buffer.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/ytlib/scheduler/config.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/protobuf_helpers.h>

#include <yt/core/rpc/channel.h>

namespace NYT::NTableClient {

using namespace NTableClient;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NRpc;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

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
    return BIND(&TSamplesFetcher::DoFetchFromNode, MakeStrong(this), nodeId, Passed(std::move(chunkIndexes)))
        .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
        .Run();
}

TFuture<void> TSamplesFetcher::DoFetchFromNode(TNodeId nodeId, const std::vector<int>& chunkIndexes)
{
    TDataNodeServiceProxy proxy(GetNodeChannel(nodeId));
    proxy.SetDefaultTimeout(Config_->NodeRpcTimeout);

    auto req = proxy.GetTableSamples();
    req->SetHeavy(true);
    req->SetMultiplexingBand(EMultiplexingBand::Heavy);
    ToProto(req->mutable_key_columns(), KeyColumns_);
    req->set_max_sample_size(MaxSampleSize_);
    req->set_sampling_policy(static_cast<int>(SamplingPolicy_));
    req->set_keys_in_attachment(true);
    // TODO(babenko): make configurable
    ToProto(req->mutable_workload_descriptor(), TWorkloadDescriptor(EWorkloadCategory::UserBatch));

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
            if (chunk->LowerLimit() && chunk->LowerLimit()->HasKey()) {
                ToProto(sampleRequest->mutable_lower_key(), chunk->LowerLimit()->GetKey());
            }
            if (chunk->UpperLimit() && chunk->UpperLimit()->HasKey()) {
                ToProto(sampleRequest->mutable_upper_key(), chunk->UpperLimit()->GetKey());
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
        LOG_INFO(rspOrError, "Failed to get samples from node (Address: %v, NodeId: %v)",
            NodeDirectory_->GetDescriptor(nodeId).GetDefaultAddress(),
            nodeId);
        OnNodeFailed(nodeId, requestedChunkIndexes);
        return;
    }

    const auto& rsp = rspOrError.Value();

    // We keep reader in the scope as a holder for wire reader and uncompressed attachment.
    std::optional<TKeySetReader> keysReader;
    NYT::TRange<TKey> keySet;
    if (rsp->keys_in_attachment()) {
        YCHECK(rsp->Attachments().size() == 1);
        keysReader.emplace(rsp->Attachments().front());
        keySet = keysReader->GetKeys();
    }

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
            TKey key;
            if (protoSample.has_key_index()) {
                YCHECK(keysReader);
                key = RowBuffer_->Capture(keySet[protoSample.key_index()]);
            } else {
                FromProto(&key, protoSample.key(), RowBuffer_);
            }

            TSample sample{
                key,
                protoSample.incomplete(),
                protoSample.weight()
            };

            YCHECK(sample.Key.GetCount() == KeyColumns_.size());
            Samples_.push_back(sample);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

