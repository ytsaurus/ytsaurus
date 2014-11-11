#include "stdafx.h"

#include "samples_fetcher.h"

#include <ytlib/chunk_client/chunk_spec.h>
#include <ytlib/chunk_client/config.h>
#include <ytlib/chunk_client/data_node_service_proxy.h>
#include <ytlib/chunk_client/dispatcher.h>
#include <ytlib/chunk_client/private.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/scheduler/config.h>

#include <core/concurrency/scheduler.h>

#include <core/misc/protobuf_helpers.h>

#include <core/rpc/channel.h>

#include <core/logging/log.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NRpc;

////////////////////////////////////////////////////////////////////

TSamplesFetcher::TSamplesFetcher(
    TFetcherConfigPtr config,
    i64 desiredSampleCount,
    const TKeyColumns& keyColumns,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    IInvokerPtr invoker,
    const NLog::TLogger& logger)
    : TFetcherBase(config, nodeDirectory, invoker, logger)
    , KeyColumns_(keyColumns)
    , DesiredSampleCount_(desiredSampleCount)
    , SizeBetweenSamples_(0)
    , TotalDataSize_(0)
{
    YCHECK(DesiredSampleCount_ > 0);
}

void TSamplesFetcher::AddChunk(TRefCountedChunkSpecPtr chunk)
{
    i64 chunkDataSize;
    GetStatistics(*chunk, &chunkDataSize);
    TotalDataSize_ += chunkDataSize;

    TFetcherBase::AddChunk(chunk);
}

TAsyncError TSamplesFetcher::Fetch()
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

const std::vector<TOwningKey>& TSamplesFetcher::GetSamples() const
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
    NYT::ToProto(req->mutable_key_columns(), KeyColumns_);

    i64 currentSize = SizeBetweenSamples_;
    i64 currentSampleCount = 0;

    std::vector<int> requestedChunkIndexes;

    for (auto index : chunkIndexes) {
        auto& chunk = Chunks_[index];

        i64 chunkDataSize;
        GetStatistics(*chunk, &chunkDataSize);

        currentSize += chunkDataSize;
        i64 sampleCount = currentSize / SizeBetweenSamples_;

        if (sampleCount > currentSampleCount) {
            requestedChunkIndexes.push_back(index);
            auto chunkId = EncodeChunkId(*chunk, nodeId);

            auto* sampleRequest = req->add_sample_requests();
            ToProto(sampleRequest->mutable_chunk_id(), chunkId);
            sampleRequest->set_sample_count(sampleCount - currentSampleCount);
            currentSampleCount = sampleCount;
        }
    }

    if (req->sample_requests_size() == 0)
        return;

    auto rsp = WaitFor(req->Invoke());

    if (!rsp->IsOK()) {
        LOG_WARNING("Failed to get samples from node (Address: %v, NodeId: %v)",
            NodeDirectory_->GetDescriptor(nodeId).GetDefaultAddress(),
            nodeId);
        OnNodeFailed(nodeId, requestedChunkIndexes);
        return;
    }

    for (int index = 0; index < requestedChunkIndexes.size(); ++index) {
        const auto& sampleResponse = rsp->sample_responses(index);
        if (sampleResponse.has_error()) {
            OnChunkFailed(nodeId, requestedChunkIndexes[index]);
            continue;
        }

        LOG_TRACE("Received %v samples for chunk #%v",
            sampleResponse.keys_size(),
            requestedChunkIndexes[index]);

        for (const auto& sample : sampleResponse.keys()) {
            Samples_.push_back(NYT::FromProto<TOwningKey>(sample));
        }
    }
}

////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT

