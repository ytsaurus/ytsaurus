#include "chunk_slices_fetcher.h"
#include "private.h"

#include <yt/ytlib/chunk_client/chunk_replica.h>
#include <yt/ytlib/chunk_client/chunk_slice.h>
#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/config.h>
#include <yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/ytlib/chunk_client/private.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/protobuf_helpers.h>

#include <yt/core/rpc/channel.h>

namespace NYT {
namespace NTableClient {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NRpc;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////

TChunkSlicesFetcher::TChunkSlicesFetcher(
    TFetcherConfigPtr config,
    i64 chunkSliceSize,
    const TKeyColumns& keyColumns,
    bool sliceByKeys,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    IInvokerPtr invoker,
    TScrapeChunksCallback scraperCallback,
    const NLogging::TLogger& logger)
    : TFetcherBase(config, nodeDirectory, invoker, scraperCallback, logger)
    , ChunkSliceSize_(chunkSliceSize)
    , KeyColumns_(keyColumns)
    , SliceByKeys_(sliceByKeys)
{
    YCHECK(ChunkSliceSize_ > 0);
}

TFuture<void> TChunkSlicesFetcher::Fetch()
{
    LOG_DEBUG("Started fetching chunk slices (ChunkCount: %v)",
        Chunks_.size());
    return TFetcherBase::Fetch();
}

std::vector<TChunkSlicePtr> TChunkSlicesFetcher::GetChunkSlices()
{
    std::vector<NChunkClient::TChunkSlicePtr> chunkSlices;

    chunkSlices.reserve(SliceCount_);
    for (const auto& slices: SlicesByChunkIndex_) {
        chunkSlices.insert(chunkSlices.end(), slices.begin(), slices.end());
    }
    return chunkSlices;
}

TFuture<void> TChunkSlicesFetcher::FetchFromNode(TNodeId nodeId, std::vector<int> chunkIndexes)
{
    return BIND(&TChunkSlicesFetcher::DoFetchFromNode, MakeWeak(this), nodeId, Passed(std::move(chunkIndexes)))
        .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
        .Run();
}

void TChunkSlicesFetcher::DoFetchFromNode(TNodeId nodeId, const std::vector<int> chunkIndexes)
{
    TDataNodeServiceProxy proxy(GetNodeChannel(nodeId));
    proxy.SetDefaultTimeout(Config_->NodeRpcTimeout);

    auto req = proxy.GetChunkSlices();
    req->set_slice_data_size(ChunkSliceSize_);
    req->set_slice_by_keys(SliceByKeys_);
    ToProto(req->mutable_key_columns(), KeyColumns_);
    // TODO(babenko): make configurable
    ToProto(req->mutable_workload_descriptor(), TWorkloadDescriptor(EWorkloadCategory::UserBatch));

    std::vector<int> requestedChunkIndexes;
    int keyColumnCount = KeyColumns_.size();

    for (auto index : chunkIndexes) {
        auto& chunk = Chunks_[index];

        i64 chunkDataSize;
        GetStatistics(*chunk, &chunkDataSize);

        TOwningKey minKey, maxKey;
        YCHECK(TryGetBoundaryKeys(chunk->chunk_meta(), &minKey, &maxKey));

        if (chunkDataSize < ChunkSliceSize_ ||
            (SliceByKeys_ && CompareRows(minKey, maxKey, keyColumnCount) == 0))
        {
            auto slice = CreateChunkSlice(
                chunk,
                GetKeyPrefix(minKey.Get(), keyColumnCount),
                GetKeyPrefixSuccessor(maxKey.Get(), keyColumnCount));
            if (SlicesByChunkIndex_.size() <= index) {
                SlicesByChunkIndex_.resize(index + 1, std::vector<NChunkClient::TChunkSlicePtr>());
            }
            SlicesByChunkIndex_[index].push_back(slice);
            SliceCount_++;
        } else {
            requestedChunkIndexes.push_back(index);
            auto chunkId = EncodeChunkId(*chunk, nodeId);

            auto* protoChunk = req->add_chunk_specs();
            *protoChunk = *chunk;
            // Makes sense for erasure chunks only.
            ToProto(protoChunk->mutable_chunk_id(), chunkId);
        }
    }

    if (req->chunk_specs_size() == 0)
        return;

    auto rspOrError = WaitFor(req->Invoke());

    if (!rspOrError.IsOK()) {
        LOG_WARNING("Failed to get chunk slices from node (Address: %v, NodeId: %v)",
            NodeDirectory_->GetDescriptor(nodeId).GetDefaultAddress(),
            nodeId);

        OnNodeFailed(nodeId, requestedChunkIndexes);

        if (rspOrError.FindMatching(EErrorCode::IncomparableType)) {
            // Any exception thrown here interrupts fetching.
            rspOrError.ThrowOnError();
        }
        return;
    }

    const auto& rsp = rspOrError.Value();
    for (int i = 0; i < requestedChunkIndexes.size(); ++i) {
        int index = requestedChunkIndexes[i];
        const auto& chunk = Chunks_[index];
        const auto& slices = rsp->slices(i);

        if (slices.has_error()) {
            auto error = FromProto<TError>(slices.error());
            OnChunkFailed(nodeId, index, error);
            continue;
        }

        LOG_TRACE("Received %v chunk slices for chunk #%v",
            slices.chunk_slices_size(),
            index);

        if (SlicesByChunkIndex_.size() <= index) {
            SlicesByChunkIndex_.resize(index + 1, std::vector<NChunkClient::TChunkSlicePtr>());
        }
        for (auto& protoChunkSlice : slices.chunk_slices()) {
            auto slice = CreateChunkSlice(chunk, protoChunkSlice);
            SlicesByChunkIndex_[index].push_back(slice);
            SliceCount_++;
        }
    }
}

////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
