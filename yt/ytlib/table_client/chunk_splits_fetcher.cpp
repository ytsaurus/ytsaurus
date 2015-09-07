#include "stdafx.h"

#include "chunk_splits_fetcher.h"

#include "private.h"

#include <ytlib/chunk_client/chunk_replica.h>
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

TChunkSplitsFetcher::TChunkSplitsFetcher(
    TFetcherConfigPtr config,
    i64 chunkSliceSize,
    const TKeyColumns& keyColumns,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    IInvokerPtr invoker,
    TScrapeChunksCallback scraperCallback,
    const NLogging::TLogger& logger)
    : TFetcherBase(config, nodeDirectory, invoker, scraperCallback, logger)
    , ChunkSliceSize_(chunkSliceSize)
    , KeyColumns_(keyColumns)
{
    YCHECK(ChunkSliceSize_ > 0);
}

TFuture<void> TChunkSplitsFetcher::Fetch()
{
    LOG_DEBUG("Started fetching chunk splits (ChunkCount: %v)",
        Chunks_.size());
    return TFetcherBase::Fetch();
}

const std::vector<TRefCountedChunkSpecPtr>& TChunkSplitsFetcher::GetChunkSplits() const
{
    return ChunkSplits_;
}

TFuture<void> TChunkSplitsFetcher::FetchFromNode(TNodeId nodeId, std::vector<int> chunkIndexes)
{
    return BIND(&TChunkSplitsFetcher::DoFetchFromNode, MakeWeak(this), nodeId, Passed(std::move(chunkIndexes)))
        .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
        .Run();
}

void TChunkSplitsFetcher::DoFetchFromNode(TNodeId nodeId, const std::vector<int> chunkIndexes)
{
    TDataNodeServiceProxy proxy(GetNodeChannel(nodeId));
    proxy.SetDefaultTimeout(Config_->NodeRpcTimeout);

    auto req = proxy.GetChunkSplits();
    req->set_min_split_size(ChunkSliceSize_);
    NYT::ToProto(req->mutable_key_columns(), KeyColumns_);

    std::vector<int> requestedChunkIndexes;
    int keyColumnCount = KeyColumns_.size();

    for (auto index : chunkIndexes) {
        auto& chunk = Chunks_[index];

        i64 chunkDataSize;
        GetStatistics(*chunk, &chunkDataSize);

        TOwningKey minKey, maxKey;
        YCHECK(TryGetBoundaryKeys(chunk->chunk_meta(), &minKey, &maxKey));

        if (chunkDataSize < ChunkSliceSize_ || CompareRows(minKey, maxKey, keyColumnCount) == 0) {
            if (!chunk->lower_limit().has_key()) {
                ToProto(chunk->mutable_lower_limit()->mutable_key(), GetKeyPrefix(minKey.Get(), keyColumnCount));
            }
            if (!chunk->upper_limit().has_key()) {
                ToProto(
                    chunk->mutable_upper_limit()->mutable_key(),
                    GetKeyPrefixSuccessor(maxKey.Get(), keyColumnCount));
            }
            ChunkSplits_.push_back(chunk);
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
        LOG_WARNING("Failed to get chunk splits from node (Address: %v, NodeId: %v)",
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
        const auto& splits = rsp->splits(i);

        if (splits.has_error()) {
            auto error = FromProto<TError>(splits.error());
            OnChunkFailed(nodeId, requestedChunkIndexes[i], error);
            continue;
        }

        LOG_TRACE("Received %v chunk splits for chunk #%v",
            splits.chunk_specs_size(),
            requestedChunkIndexes[i]);

        for (auto& chunkSpec : splits.chunk_specs()) {
            auto split = New<TRefCountedChunkSpec>(std::move(chunkSpec));
            // Adjust chunk id (makes sense for erasure chunks only).
            auto chunkId = FromProto<TChunkId>(split->chunk_id());
            auto chunkIdWithIndex = DecodeChunkId(chunkId);
            ToProto(split->mutable_chunk_id(), chunkIdWithIndex.Id);
            split->set_table_index(chunk->table_index());
            ChunkSplits_.push_back(split);
        }
    }
}

////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
