#include "stdafx.h"

#include "chunk_slices_fetcher.h"

#include "private.h"

#include <ytlib/chunk_client/chunk_replica.h>
#include <ytlib/chunk_client/chunk_slice.h>
#include <ytlib/chunk_client/chunk_spec.h>
#include <ytlib/chunk_client/config.h>
#include <ytlib/chunk_client/data_node_service_proxy.h>
#include <ytlib/chunk_client/dispatcher.h>
#include <ytlib/chunk_client/private.h>

#include <ytlib/table_client/chunk_meta_extensions.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <core/concurrency/scheduler.h>

#include <core/misc/protobuf_helpers.h>

#include <core/rpc/channel.h>

namespace NYT {
namespace NTableClient {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NRpc;

using NYT::FromProto;

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

const std::vector<TChunkSlicePtr>& TChunkSlicesFetcher::GetChunkSlices() const
{
    return ChunkSlices_;
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
    NYT::ToProto(req->mutable_key_columns(), KeyColumns_);

    std::vector<int> requestedChunkIndexes;
    int keyColumnCount = KeyColumns_.size();

    for (auto index : chunkIndexes) {
        auto& chunk = Chunks_[index];

        i64 chunkDataSize;
        GetStatistics(*chunk, &chunkDataSize);

        TOwningKey minKey, maxKey;
        YCHECK(TryGetBoundaryKeys(chunk->chunk_meta(), &minKey, &maxKey));

        if (chunkDataSize < ChunkSliceSize_
                || (SliceByKeys_ && CompareRows(minKey, maxKey, keyColumnCount)) == 0) {
            auto slice = CreateChunkSlice(
                chunk,
                GetKeyPrefix(minKey.Get(), keyColumnCount),
                GetKeyPrefixSuccessor(maxKey.Get(), keyColumnCount));
            ChunkSlices_.push_back(slice);
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
        const auto& chunk = Chunks_[requestedChunkIndexes[i]];
        const auto& slices = rsp->slices(i);

        if (slices.has_error()) {
            auto error = FromProto<TError>(slices.error());
            OnChunkFailed(nodeId, requestedChunkIndexes[i], error);
            continue;
        }

        LOG_TRACE("Received %v chunk slices for chunk #%v",
            slices.chunk_slices_size(),
            requestedChunkIndexes[i]);

        for (auto& protoChunkSlice : slices.chunk_slices()) {
            auto slice = CreateChunkSlice(chunk, protoChunkSlice);
            ChunkSlices_.push_back(slice);
        }
    }
}

////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
