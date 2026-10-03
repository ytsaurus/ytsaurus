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
void VisitUniqueAncestors(TChunkTree* chunkTree, F&& functor, TChunkTree* child)
{
    while (chunkTree) {
        YT_VERIFY(chunkTree->IsChunkList());

        functor(chunkTree, child);

        TRange<TChunkTreeRawPtr> parents;
        parents = chunkTree->AsChunkList()->Parents();

        if (parents.Empty())
            break;
        YT_VERIFY(parents.Size() == 1);
        child = chunkTree;
        chunkTree = *parents.begin();
    }
}

template <class F>
void VisitHunkTreeAncestors(TChunk* hunkChunk, F&& functor)
{
    const auto& Logger = ChunkServerLogger;

    if (hunkChunk->IsConfirmed() && !IsHunkChunkFormat(hunkChunk->GetChunkFormat())) {
        YT_TLOG_ALERT("Unexpectedely encountered a non-hunk chunk when visiting its ancestors; skipping it")
            .With("ChunkId", hunkChunk->GetId())
            .With("ChunkFormat", hunkChunk->GetChunkFormat());
        return;
    }

    TChunkListId hunkStorageRootChunkListId;
    THashSet<TChunkListId> rootChunkListIds;
    THashSet<TChunkListId> tabletChunkListIds;

    for (const auto& [chunkParent, _] : hunkChunk->Parents()) {
        auto* chunkList = chunkParent->AsChunkList();

        if (chunkList->GetKind() == EChunkListKind::Scratch) {
            // Scratch chunk list holds chunks without maintaining statistics; just skip it.
            continue;
        }

        if (chunkList->GetKind() != EChunkListKind::Hunk &&
            chunkList->GetKind() != EChunkListKind::HunkTablet)
        {
            YT_TLOG_ALERT("Parent chunk list of unexpected kind was encountered upon visiting hunk tree ancestors")
                .With("HunkChunkId", hunkChunk->GetId())
                .With("ParentId", chunkParent->GetId())
                .With("ParentChunkListKind", chunkList->GetKind());
            continue;
        }

        if (!tabletChunkListIds.emplace(chunkList->GetId()).second) {
            YT_TLOG_ALERT("Tablet chunk list encountered multiple times upon visiting hunk tree ancestors")
                .With("HunkChunkId", hunkChunk->GetId())
                .With("ParentId", chunkParent->GetId());
            continue;
        }

        functor(chunkList, /*firstOccurrence*/ true);

        for (const auto& chunkListParent : chunkList->Parents()) {
            auto* rootChunkList = chunkListParent->AsChunkList();

            if (!rootChunkList->IsHunkRoot()) {
                YT_TLOG_ALERT("Root chunk list of unexpected kind was encountered upon visiting hunk tree ancestors")
                    .With("ChunkId", hunkChunk->GetId())
                    .With("ParentId", chunkListParent->GetId())
                    .With("ParentChunkListKind", rootChunkList->GetKind());
                continue;
            }

            if (rootChunkList->GetKind() == EChunkListKind::HunkStorageRoot) {
                YT_TLOG_ALERT_IF(
                    hunkStorageRootChunkListId,
                    "Multiple ancestor hunk storage roots were encountered upon visiting hunk tree ancestors")
                    .With("ChunkId", hunkChunk->GetId())
                    .With("FirstParentId", hunkStorageRootChunkListId)
                    .With("SecondParentId", rootChunkList->GetId());

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
