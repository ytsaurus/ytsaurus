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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
