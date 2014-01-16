#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
#endif
#undef HELPERS_INL_H_

#include "chunk.h"
#include "chunk_list.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

template <class F>
void VisitUniqueAncestors(TChunkList* chunkList, F functor)
{
    while (chunkList != nullptr) {
        functor(chunkList);
        const auto& parents = chunkList->Parents();
        if (parents.empty())
            break;
        YCHECK(parents.size() == 1);
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
            if (parents.size() != 1)
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

template <class F>
void AttachToChunkList(
    TChunkList* chunkList,
    TChunkTree** childrenBegin,
    TChunkTree** childrenEnd,
    F childAction)
{
    // A shortcut.
    if (childrenBegin == childrenEnd)
        return;

    chunkList->IncrementVersion();

    TChunkTreeStatistics delta;
    for (auto it = childrenBegin; it != childrenEnd; ++it) {
        auto* child = *it;
        AddChildStatistics(chunkList, child, &delta);
        chunkList->Children().push_back(child);
        SetChunkTreeParent(chunkList, child);
        childAction(child);
    }

    // Go upwards and apply delta.
    VisitUniqueAncestors(
        chunkList,
        [&] (TChunkList* current) {
            ++delta.Rank;
            current->Statistics().Accumulate(delta);
        });
}

template <class F>
void DetachFromChunkList(
    TChunkList* chunkList,
    TChunkTree** childrenBegin,
    TChunkTree** childrenEnd,
    F childAction)
{
    // A shortcut.
    if (childrenBegin == childrenEnd)
        return;

    chunkList->IncrementVersion();

    yhash_set<TChunkTree*> detachSet;
    for (auto it = childrenBegin; it != childrenEnd; ++it) {
        // Children may possibly be duplicate.
        detachSet.insert(*it);
    }

    ResetChunkListStatistics(chunkList);

    std::vector<TChunkTree*> existingChildren;
    chunkList->Children().swap(existingChildren);

    TChunkTreeStatistics delta;
    for (auto child : existingChildren) {
        if (detachSet.find(child) == detachSet.end()) {
            AddChildStatistics(chunkList, child, &delta);
            chunkList->Children().push_back(child);
        } else {
            ResetChunkTreeParent(chunkList, child);
            childAction(child);
        }
    }

    // Go upwards and recompute statistics.
    VisitUniqueAncestors(
        chunkList,
        [&] (TChunkList* current) {
            RecomputeChunkListStatistics(current);
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
