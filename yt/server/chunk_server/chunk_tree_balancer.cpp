#include "stdafx.h"
#include "chunk_tree_balancer.h"
#include "chunk_list.h"
#include "chunk_manager.h"

#include <server/cell_master/bootstrap.h>

#include <stack>

namespace NYT {
namespace NChunkServer {

using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

TChunkTreeBalancer::TChunkTreeBalancer(
    IChunkTreeBalancerCallbacksPtr bootstrap,
    const TChunkTreeBalancerSettings& settings)
    : Bootstrap_(bootstrap)
    , Settings_(settings)
{ }

bool TChunkTreeBalancer::IsRebalanceNeeded(TChunkList* root)
{
    if (!root->Parents().empty()) {
        return false;
    }

    if (root->Children().size() > Settings_.MaxChunkListSize) {
        return true;
    }

    const auto& statistics = root->Statistics();

    if (statistics.ChunkCount == 0) {
        return true;
    }

    if (statistics.Rank > Settings_.MaxChunkTreeRank) {
        return true;
    }

    if (statistics.ChunkListCount > 2 &&
        statistics.ChunkListCount > statistics.ChunkCount * Settings_.MinChunkListToChunkRatio)
    {
        return true;
    }

    return false;
}

void TChunkTreeBalancer::Rebalance(TChunkList* root)
{
    auto oldStatistics = root->Statistics();

    // Special case: no chunk in the chunk tree.
    if (oldStatistics.ChunkCount == 0) {
        Bootstrap_->ClearChunkList(root);
        return;
    }

    // Construct new children list.
    std::vector<TChunkTree*> newChildren;
    AppendChunkTree(&newChildren, root);
    YCHECK(!newChildren.empty());
    YCHECK(newChildren.front() != root);

    // Rewrite the root with newChildren.

    // Add temporary references to the old children.
    auto oldChildren = root->Children();
    for (auto* child : oldChildren) {
        Bootstrap_->RefObject(child);
    }

    // Replace the children list.
    Bootstrap_->ClearChunkList(root);
    Bootstrap_->AttachToChunkList(root, newChildren);

    // Release the temporary references added above.
    for (auto* child : oldChildren) {
        Bootstrap_->UnrefObject(child);
    }

    const auto& newStatistics = root->Statistics();
    YCHECK(newStatistics.RowCount == oldStatistics.RowCount);
    YCHECK(newStatistics.UncompressedDataSize == oldStatistics.UncompressedDataSize);
    YCHECK(newStatistics.CompressedDataSize == oldStatistics.CompressedDataSize);
    YCHECK(newStatistics.DataWeight == oldStatistics.DataWeight);
    YCHECK(newStatistics.RegularDiskSpace == oldStatistics.RegularDiskSpace);
    YCHECK(newStatistics.ErasureDiskSpace == oldStatistics.ErasureDiskSpace);
    YCHECK(newStatistics.ChunkCount == oldStatistics.ChunkCount);
}

void TChunkTreeBalancer::AppendChunkTree(
    std::vector<TChunkTree*>* children,
    TChunkTree* root)
{
    // Run a non-recursive tree traversal calling AppendChild
    // at each chunk and chunk tree of rank <= 1.

    struct TEntry
    {
        TEntry()
            : ChunkTree(nullptr)
            , Index(-1)
        { }

        TEntry(TChunkTree* chunkTree, int index)
            : ChunkTree(chunkTree)
            , Index(index)
        { }

        TChunkTree* ChunkTree;
        int Index;
    };

    std::stack<TEntry> stack;
    stack.push(TEntry(root, 0));

    while (!stack.empty()) {
        auto& currentEntry = stack.top();
        auto* currentChunkTree = currentEntry.ChunkTree;

        if (currentChunkTree->GetType() == EObjectType::ChunkList &&
            currentChunkTree->AsChunkList()->Statistics().Rank > 1)
        {
            auto* currentChunkList = currentChunkTree->AsChunkList();
            int currentIndex = currentEntry.Index;
            if (currentIndex < static_cast<int>(currentChunkList->Children().size())) {
                ++currentEntry.Index;
                stack.push(TEntry(currentChunkList->Children()[currentIndex], 0));
                continue;
            }
        } else {
            AppendChild(children, currentChunkTree);
        }
        stack.pop();
    }
}

void TChunkTreeBalancer::AppendChild(
    std::vector<TChunkTree*>* children,
    TChunkTree* child)
{
    // Can we reuse the last chunk list?
    bool merge = false;
    if (!children->empty()) {
        auto* lastChild = children->back()->AsChunkList();
        if (lastChild->Children().size() < Settings_.MinChunkListSize) {
            YASSERT(lastChild->Statistics().Rank <= 1);
            YASSERT(lastChild->Children().size() <= Settings_.MaxChunkListSize);
            if (lastChild->GetObjectRefCounter() > 0) {
                // We want to merge to this chunk list but it is shared.
                // Copy on write.
                auto* clonedLastChild = Bootstrap_->CreateChunkList();
                Bootstrap_->AttachToChunkList(clonedLastChild, lastChild->Children());
                children->pop_back();
                children->push_back(clonedLastChild);
            }
            merge = true;
        }
    }

    // Try to add the child as is.
    if (!merge) {
        if (child->GetType() == EObjectType::ChunkList) {
            auto* chunkList = child->AsChunkList();
            if (chunkList->Children().size() <= Settings_.MaxChunkListSize) {
                YASSERT(chunkList->GetObjectRefCounter() > 0);
                children->push_back(child);
                return;
            }
        }

        // We need to split the child. So we use usual merging.
        auto* newChunkList = Bootstrap_->CreateChunkList();
        children->push_back(newChunkList);
    }

    // Merge!
    MergeChunkTrees(children, child);
}

void TChunkTreeBalancer::MergeChunkTrees(
    std::vector<TChunkTree*>* children,
    TChunkTree* child)
{
    // We are trying to add the child to the last chunk list.
    auto* lastChunkList = children->back()->AsChunkList();

    YASSERT(lastChunkList->GetObjectRefCounter() == 0);
    YASSERT(lastChunkList->Statistics().Rank <= 1);
    YASSERT(lastChunkList->Children().size() < Settings_.MinChunkListSize);

    switch (child->GetType()) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk: {
            // Just adding the chunk to the last chunk list.
            Bootstrap_->AttachToChunkList(lastChunkList, child);
            break;
        }

        case EObjectType::ChunkList: {
            auto* chunkList = child->AsChunkList();
            if (lastChunkList->Children().size() + chunkList->Children().size() <= Settings_.MaxChunkListSize) {
                // Just appending the chunk list to the last chunk list.
                Bootstrap_->AttachToChunkList(lastChunkList, chunkList->Children());
            } else {
                // The chunk list is too large. We have to copy chunks by blocks.
                int mergedCount = 0;
                while (mergedCount < chunkList->Children().size()) {
                    if (lastChunkList->Children().size() >= Settings_.MinChunkListSize) {
                        // The last chunk list is too large. Creating a new one.
                        YASSERT(lastChunkList->Children().size() == Settings_.MinChunkListSize);
                        lastChunkList = Bootstrap_->CreateChunkList();
                        children->push_back(lastChunkList);
                    }
                    int count = std::min(
                        Settings_.MinChunkListSize - lastChunkList->Children().size(),
                        chunkList->Children().size() - mergedCount);
                    Bootstrap_->AttachToChunkList(
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
