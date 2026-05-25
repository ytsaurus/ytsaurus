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

#include <library/cpp/yt/compact_containers/compact_queue.h>

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
        while (chunkList) {
            functor(chunkList);
            const auto& parents = chunkList->Parents();
            if (parents.Size() != 1) {
                break;
            }
            chunkList = *parents.begin();
        }

        if (chunkList) {
            // Proceed to parents.
            for (auto parent : chunkList->Parents()) {
                queue.Push(parent);
            }
        }
    }
}

template <class F>
void VisitAllAncestorsInHunkTree(TChunk* hunkChunk, F&& functor)
{
    const auto& Logger = ChunkServerLogger;

    if (hunkChunk->IsConfirmed() && !IsHunkChunkFormat(hunkChunk->GetChunkFormat())) {
        YT_LOG_ALERT("Unexpectedely encountered a non-hunk chunk when visiting its ancestors; skipping it "
            "(ChunkId: %v, ChunkFormat: %v)",
                hunkChunk->GetId(),
                hunkChunk->GetChunkFormat());
        return;
    }

    TChunkListId hunkStorageRootChunkListId;
    THashSet<TChunkListId> rootChunkListIds;
    THashSet<TChunkListId> tabletChunkListIds;

    for (const auto& [chunkParent, _] : hunkChunk->Parents()) {
        const auto& chunkList = chunkParent->AsChunkList();

        if (chunkList->GetKind() != EChunkListKind::Hunk &&
            chunkList->GetKind() != EChunkListKind::HunkTablet)
        {
            YT_LOG_ALERT("Parent chunk list of unexpected kind was encountered upon visiting ancestors in hunk tree "
                "(HunkChunkId: %v, ParentId: %v, ParentChunkListKind: %v)",
                hunkChunk->GetId(),
                chunkParent->GetId(),
                chunkList->GetKind());
            continue;
        }

        if (!tabletChunkListIds.emplace(chunkList->GetId()).second) {
            YT_LOG_ALERT("Tablet chunk list encountered multiple times upon visiting ancestors in hunk tree "
                "(HunkChunkId: %v, ParentId: %v)",
                hunkChunk->GetId(),
                chunkParent->GetId());
            continue;
        }

        functor(chunkList, /*firstOccurrence*/ true);

        for (const auto& chunkListParent : chunkList->Parents()) {
            const auto& rootChunkList = chunkListParent->AsChunkList();

            if (!IsHunkRootChunkList(rootChunkList)) {
                YT_LOG_ALERT("Root chunk list of unexpected kind was encountered upon visiting ancestors in hunk tree "
                    "(ChunkId: %v, ParentId: %v, ParentChunkListKind: %v)",
                    hunkChunk->GetId(),
                    chunkListParent->GetId(),
                    rootChunkList->GetKind());
                continue;
            }

            if (rootChunkList->GetKind() == EChunkListKind::HunkStorageRoot) {
                YT_LOG_ALERT_IF(hunkStorageRootChunkListId,
                    "Multiple ancestor hunk storage roots were encountered upon visiting ancestors in hunk tree "
                    "(ChunkId: %v, FirstParentId: %v, SecondParentId: %v)",
                    hunkChunk->GetId(),
                    hunkStorageRootChunkListId,
                    rootChunkList->GetId());

                hunkStorageRootChunkListId = rootChunkList->GetId();
            }

            functor(
                rootChunkList,
                rootChunkListIds.emplace(rootChunkList->GetId()).second);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
