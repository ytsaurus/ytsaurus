#pragma once
#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

#include "chunk.h"
#include "chunk_list.h"

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
    // BFS queue. Try to avoid allocations.
    SmallVector<TChunkList*, 64> queue;
    size_t frontIndex = 0;

    // Put seed into the queue.
    queue.push_back(chunkList);

    // The main loop.
    while (frontIndex < queue.size()) {
        auto* chunkList = queue[frontIndex++];

        // Fast lane: handle unique parents.
        while (chunkList != nullptr) {
            functor(chunkList);
            const auto& parents = chunkList->Parents();
            if (parents.Size() != 1)
                break;
            chunkList = *parents.begin();
        }

        if (chunkList != nullptr) {
            // Proceed to parents.
            for (auto* parent : chunkList->Parents()) {
                queue.push_back(parent);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
