#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

#include "chunk.h"
#include "private.h"
#include "chunk_list.h"
#include "data_node_tracker.h"

#include <yt/yt/server/master/node_tracker_server/node.h>

#include <yt/yt/ytlib/data_node_tracker_client/location_directory.h>

#include <yt/yt/core/misc/guid.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <library/cpp/yt/small_containers/compact_queue.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

template <class F>
void VisitUniqueAncestors(TChunkList* chunkList, F functor, TChunkTree* child)
{
    while (chunkList != nullptr) {
        functor(chunkList, child);
        const auto& parents = chunkList->Parents();
        if (parents.Empty())
            break;
        YT_VERIFY(parents.Size() == 1);
        child = chunkList;
        chunkList = *parents.begin();
    }
}

template <class F>
void VisitAncestors(TChunkList* chunkList, F functor)
{
    // BFS queue.
    TCompactQueue<TChunkList*, 64> queue;

    // Put seed into the queue.
    queue.Push(chunkList);

    // The main loop.
    while (!queue.Empty()) {
        auto* chunkList = queue.Pop();

        // Fast lane: handle unique parents.
        while (chunkList != nullptr) {
            functor(chunkList);
            const auto& parents = chunkList->Parents();
            if (parents.Size() != 1) {
                break;
            }
            chunkList = *parents.begin();
        }

        if (chunkList != nullptr) {
            // Proceed to parents.
            for (auto* parent : chunkList->Parents()) {
                queue.Push(parent);
            }
        }
    }
}

template <bool FullHeartbeat>
void AlertAndThrowOnInvalidLocationIndex(
    const auto& chunkInfo,
    const TNode* node,
    int locationDirectorySize)
{
    using NYT::FromProto;

    // Heartbeats should no longer contain location uuids but if node was
    // registered before master server update it still can send heartbeats
    // with location uuids.
    YT_ASSERT(!chunkInfo.has_location_uuid());

    using TChunkInfo = std::decay_t<decltype(chunkInfo)>;
    static_assert(std::is_same_v<TChunkInfo, NChunkClient::NProto::TChunkAddInfo> || std::is_same_v<TChunkInfo, NChunkClient::NProto::TChunkRemoveInfo>,
        "TChunkInfo must be either TChunkAddInfo or TChunkRemoveInfo");

    constexpr bool isRemoval = !FullHeartbeat &&
        std::is_same_v<std::decay_t<decltype(chunkInfo)>, NYT::NRpc::TTypedServiceRequest<NYT::NDataNodeTrackerClient::NProto::TReqFullHeartbeat>>;

    auto chunkId = FromProto<TChunkId>(chunkInfo.chunk_id());

    if (chunkInfo.location_index() >= locationDirectorySize) {
        static const auto& Logger = ChunkServerLogger;
        YT_LOG_ALERT(
            "Data node reported %v heartbeat with invalid location index "
            "(%vChunkId: %v, NodeAddress: %v, LocationIndex: %v)",
            FullHeartbeat ? "full" : "incremental",
            FullHeartbeat ? "" : (isRemoval ? "Removed" : "Added"),
            chunkId,
            node->GetDefaultAddress(),
            chunkInfo.location_index());

        THROW_ERROR_EXCEPTION("%v heartbeat contains an incorrect location index",
            FullHeartbeat ? "Full" : "Incremental");
    }
}

template <class TRequest>
TCompactVector<TRealChunkLocation*, TypicalChunkLocationCount> ParseLocationDirectory(
    const IDataNodeTrackerPtr& dataNodeTracker,
    const TRequest& request)
{
    using namespace NDataNodeTrackerClient::NProto;
    using NYT::FromProto;

    auto rawLocationDirectory = FromProto<NDataNodeTrackerClient::TChunkLocationDirectory>(request.location_directory());

    TCompactVector<TRealChunkLocation*, TypicalChunkLocationCount> locationDirectory;
    locationDirectory.reserve(request.location_directory_size());

    for (auto uuid : rawLocationDirectory.Uuids()) {
        // All locations were checked in `TDataNodeTracker::Hydra*Heartbeat()`
        // so it should be safe to use `GetChunkLocationByUuid()` here.
        locationDirectory.push_back(dataNodeTracker->GetChunkLocationByUuid(uuid));
    }

    return locationDirectory;
}

template <class TRequest>
TCompactVector<TRealChunkLocation*, TypicalChunkLocationCount> ParseLocationDirectoryOrThrow(
    const TNode* node,
    const IDataNodeTrackerPtr& dataNodeTracker,
    const TRequest& request)
{
    static_assert(
        std::is_same_v<TRequest, NYT::NRpc::TTypedServiceRequest<NYT::NDataNodeTrackerClient::NProto::TReqFullHeartbeat>> ||
        std::is_same_v<TRequest, NYT::NRpc::TTypedServiceRequest<NYT::NDataNodeTrackerClient::NProto::TReqIncrementalHeartbeat>> ||
        std::is_same_v<TRequest, NDataNodeTrackerClient::NProto::TReqModifyReplicas>,
        "TRequest must be either TReqFullHeartbeat, TReqIncrementalHeartbeat or TReqModifyReplicas");

    constexpr bool fullHeartbeat = std::is_same_v<TRequest, NYT::NRpc::TTypedServiceRequest<NYT::NDataNodeTrackerClient::NProto::TReqFullHeartbeat>>;
    auto checkLocationIndices = [&] (const auto& chunkInfos) {
        for (const auto& chunkInfo : chunkInfos) {
            AlertAndThrowOnInvalidLocationIndex<fullHeartbeat>(chunkInfo, node, request.location_directory_size());
        }
    };

    if constexpr (fullHeartbeat) {
        checkLocationIndices(request.chunks());
    } else {
        checkLocationIndices(request.added_chunks());
        checkLocationIndices(request.removed_chunks());
    }

    return ParseLocationDirectory(dataNodeTracker, request);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
