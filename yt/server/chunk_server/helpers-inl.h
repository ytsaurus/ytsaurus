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
    TSmallVector<TChunkList*, 64> queue;
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
            FOREACH (auto* parent, chunkList->Parents()) {
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
    F chunkAction,
    bool resetSorted)
{
    // A shortcut.
    if (childrenBegin == childrenEnd)
        return;

    for (auto it = childrenBegin; it != childrenEnd; ++it) {
        auto* child = *it;
        if (child->GetType() == EObjectType::Chunk) {
            child->AsChunk()->ValidateConfirmed();
        }
    }

    chunkList->IncrementVersion();

    TChunkTreeStatistics delta;
    for (auto it = childrenBegin; it != childrenEnd; ++it) {
        auto child = *it;
        if (!chunkList->Children().empty()) {
            chunkList->RowCountSums().push_back(
                chunkList->Statistics().RowCount +
                delta.RowCount);
            chunkList->ChunkCountSums().push_back(
                chunkList->Statistics().ChunkCount +
                delta.ChunkCount);
            chunkList->DataSizeSums().push_back(
                chunkList->Statistics().UncompressedDataSize +
                delta.UncompressedDataSize);

        }
        chunkList->Children().push_back(child);
        chunkAction(child);
        SetChunkTreeParent(chunkList, child);
        delta.Accumulate(GetChunkTreeStatistics(child));
    }

    // Go upwards and apply delta.
    // Reset Sorted flags.
    VisitUniqueAncestors(
        chunkList,
        [&] (TChunkList* current) {
            ++delta.Rank;
            current->Statistics().Accumulate(delta);
            if (resetSorted) {
                current->SortedBy().clear();
            }
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
