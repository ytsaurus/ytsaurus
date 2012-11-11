#include "stdafx.h"
#include "chunk_tree_balancer.h"
#include "chunk_list.h"
#include "private.h"
#include "chunk_manager.h"

#include <server/cell_master/bootstrap.h>

namespace NYT {
namespace NChunkServer {

using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkServerLogger;

// NB: Changing these values will invalidate all changelogs!
const int TChunkTreeBalancer::MaxChunkTreeRank = 32;
const int TChunkTreeBalancer::MinChunkListSize = 1024;
const int TChunkTreeBalancer::MaxChunkListSize = 2048;
const double TChunkTreeBalancer::MinChunkListToChunkRatio = 0.01;

////////////////////////////////////////////////////////////////////////////////

TChunkTreeBalancer::TChunkTreeBalancer(NCellMaster::TBootstrap* bootstrap)
    : Bootstrap(bootstrap)
{ }

bool TChunkTreeBalancer::IsRebalanceNeeded(TChunkList* root)
{
    if (!root->Parents().empty()) {
        return false;
    }

    if (root->Children().size() > MaxChunkListSize) {
        return true;
    }

    const auto& statistics = root->Statistics();

    if (statistics.ChunkCount == 0) {
        return true;
    }

    if (statistics.Rank > MaxChunkTreeRank) {
        return true;
    }

    if (statistics.ChunkListCount > 2 &&
        statistics.ChunkListCount > statistics.ChunkCount * MinChunkListToChunkRatio)
    {
        return true;
    }

    return false;
}

void TChunkTreeBalancer::Rebalance(TChunkList* root)
{
    auto chunkManager = Bootstrap->GetChunkManager();
    auto objectManager = Bootstrap->GetObjectManager();

    auto oldStatistics = root->Statistics();

    // Special case: no chunk in the chunk tree.
    if (oldStatistics.ChunkCount == 0) {
        chunkManager->ClearChunkList(root);
        return;
    }

    // Construct new children list.
    std::vector<TChunkTreeRef> newChildren;
    AppendChunkTree(&newChildren, root);
    YCHECK(!newChildren.empty());
    YCHECK(newChildren.front() != root);

    // Rewrite the root with newChildren.
    
    // Add temporary references to the old children.
    auto oldChildren = root->Children();
    FOREACH (auto childRef, oldChildren) {
        objectManager->RefObject(childRef);
    }

    // Replace the children list.
    chunkManager->ClearChunkList(root);
    chunkManager->AttachToChunkList(root, newChildren);

    // Release the temporary references added above.
    FOREACH (auto childRef, oldChildren) {
        objectManager->UnrefObject(childRef.GetId());
    }

    const auto& newStatistics = root->Statistics();
    YCHECK(newStatistics.RowCount == oldStatistics.RowCount);
    YCHECK(newStatistics.UncompressedDataSize == oldStatistics.UncompressedDataSize);
    YCHECK(newStatistics.CompressedDataSize == oldStatistics.CompressedDataSize);
    YCHECK(newStatistics.DataWeight == oldStatistics.DataWeight);
    YCHECK(newStatistics.DiskSpace == oldStatistics.DiskSpace);
    YCHECK(newStatistics.ChunkCount == oldStatistics.ChunkCount);
}

void TChunkTreeBalancer::AppendChunkTree(
    std::vector<TChunkTreeRef>* children,
    TChunkTreeRef child)
{
    auto chunkManager = Bootstrap->GetChunkManager();

    // Expand child chunk lists of rank > 1.
    if (child.GetType() == EObjectType::ChunkList) {
        const auto* chunkList = child.AsChunkList();
        if (chunkList->Statistics().Rank > 1) {
            FOREACH (auto childRef, chunkList->Children()) {
                AppendChunkTree(children, childRef);
            }
            return;
        }
    }

    // Can we reuse the last chunk list?
    bool merge = false;
    if (!children->empty()) {
        auto* lastChild = children->back().AsChunkList();
        if (lastChild->Children().size() < MinChunkListSize) {
            YASSERT(lastChild->Statistics().Rank <= 1);
            YASSERT(lastChild->Children().size() <= MaxChunkListSize);
            if (lastChild->GetObjectRefCounter() > 0) {
                // We want to merge to this chunk list but it is shared.
                // Copy on write.
                auto* clonedLastChild = chunkManager->CreateChunkList();
                chunkManager->AttachToChunkList(clonedLastChild, lastChild->Children());
                children->pop_back();
                children->push_back(clonedLastChild);
            }
            merge = true;
        }
    }

    // Try to add the child as is.
    if (!merge) {
        if (child.GetType() == EObjectType::ChunkList) {
            const auto* chunkList = child.AsChunkList();
            if (chunkList->Children().size() <= MaxChunkListSize) {
                YASSERT(chunkList->GetObjectRefCounter() > 0);
                children->push_back(child);
                return;
            }
        }

        // We need to split the child. So we use usual merging.
        auto* newChunkList = chunkManager->CreateChunkList();
        children->push_back(newChunkList);
    }

    // Merge!
    MergeChunkTrees(children, child);
}

void TChunkTreeBalancer::MergeChunkTrees(
    std::vector<TChunkTreeRef>* children,
    TChunkTreeRef child)
{
    // We are trying to add the child to the last chunk list.
    auto* lastChunkList = children->back().AsChunkList();

    YASSERT(lastChunkList->GetObjectRefCounter() == 0);
    YASSERT(lastChunkList->Statistics().Rank <= 1);
    YASSERT(lastChunkList->Children().size() < MinChunkListSize);

    auto chunkManager = Bootstrap->GetChunkManager();

    switch (child.GetType()) {
        case EObjectType::Chunk: {
            // Just adding the chunk to the last chunk list.
            chunkManager->AttachToChunkList(lastChunkList, child);
            break;
        }

        case EObjectType::ChunkList: {
            const auto* chunkList = child.AsChunkList();
            if (lastChunkList->Children().size() + chunkList->Children().size() <= MaxChunkListSize) {
                // Just appending the chunk list to the last chunk list.
                chunkManager->AttachToChunkList(lastChunkList, chunkList->Children());
            } else {
                // The chunk list is too large. We have to copy chunks by blocks.
                int mergedCount = 0;
                while (mergedCount < chunkList->Children().size()) {
                    if (lastChunkList->Children().size() >= MinChunkListSize) {
                        // The last chunk list is too large. Creating a new one.
                        YASSERT(lastChunkList->Children().size() == MinChunkListSize);
                        lastChunkList = chunkManager->CreateChunkList();
                        children->push_back(TChunkTreeRef(lastChunkList));
                    }
                    int count = std::min(
                        MinChunkListSize - lastChunkList->Children().size(),
                        chunkList->Children().size() - mergedCount);
                    chunkManager->AttachToChunkList(
                        lastChunkList,
                        &*chunkList->Children().begin() + mergedCount,
                        &*chunkList->Children().begin() + mergedCount + count);
                    mergedCount += count;
                }
            }
            break;
        }

        default:
            YUNREACHABLE();
    }
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
