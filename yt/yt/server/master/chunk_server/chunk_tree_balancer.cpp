#include "chunk_tree_balancer.h"
#include "chunk_list.h"
#include "chunk_manager.h"
#include "config.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>

#include <stack>

namespace NYT::NChunkServer {

using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

TChunkTreeBalancer::TChunkTreeBalancer(
    IChunkTreeBalancerCallbacksPtr callbacks)
    : Callbacks_(callbacks)
{ }

bool TChunkTreeBalancer::IsRebalanceNeeded(TChunkList* root, EChunkTreeBalancerMode settingsMode)
{
    if (!root->Parents().Empty()) {
        return false;
    }

    const auto& settings = GetConfig()->GetSettingsForMode(settingsMode);

    if (std::ssize(root->Children()) > settings->MaxChunkListSize) {
        return true;
    }

    const auto& statistics = root->Statistics();

    if (statistics.ChunkCount == 0) {
        return true;
    }

    if (statistics.Rank > settings->MaxChunkTreeRank) {
        return true;
    }

    if (statistics.ChunkListCount > 2 &&
        statistics.ChunkListCount > statistics.ChunkCount * settings->MinChunkListToChunkRatio)
    {
        return true;
    }

    return false;
}

void TChunkTreeBalancer::Rebalance(TChunkList* root)
{
    YT_VERIFY(root->GetKind() == EChunkListKind::Static);

    auto oldStatistics = root->Statistics();

    // Special case: no chunks in the chunk tree.
    if (oldStatistics.ChunkCount == 0) {
        Callbacks_->ClearChunkList(root);
        return;
    }

    // Construct new children list.
    std::vector<TChunkTree*> newChildren;
    AppendChunkTree(&newChildren, root);
    YT_VERIFY(!newChildren.empty());
    YT_VERIFY(newChildren.front() != root);

    // Rewrite the root with newChildren.

    // Add temporary references to the old children.
    auto oldChildren = root->Children();
    for (auto* child : oldChildren) {
        Callbacks_->RefObject(child);
    }

    // Replace the children list.
    Callbacks_->ClearChunkList(root);
    Callbacks_->AttachToChunkList(root, newChildren);

    // Release the temporary references added above.
    for (auto* child : oldChildren) {
        Callbacks_->UnrefObject(child);
    }

    const auto& newStatistics = root->Statistics();
    YT_VERIFY(newStatistics.RowCount == oldStatistics.RowCount);
    YT_VERIFY(newStatistics.LogicalRowCount == oldStatistics.LogicalRowCount);
    YT_VERIFY(newStatistics.UncompressedDataSize == oldStatistics.UncompressedDataSize);
    YT_VERIFY(newStatistics.CompressedDataSize == oldStatistics.CompressedDataSize);
    YT_VERIFY(newStatistics.DataWeight == -1 ||
        oldStatistics.DataWeight == -1 ||
        newStatistics.DataWeight == oldStatistics.DataWeight);
    YT_VERIFY(newStatistics.RegularDiskSpace == oldStatistics.RegularDiskSpace);
    YT_VERIFY(newStatistics.ErasureDiskSpace == oldStatistics.ErasureDiskSpace);
    YT_VERIFY(newStatistics.ChunkCount == oldStatistics.ChunkCount);
    YT_VERIFY(newStatistics.LogicalChunkCount == oldStatistics.LogicalChunkCount);

    // Should we schedule a requisition update here? We shouldn't. Here's why.
    // First of all, it would be prohibitively expensive (trust me, I checked).
    // Second of all, it's not necessary. One reason it may seem so follows.
    //
    // In short, there's a risk of conflict between the balancer and the
    // requisition update traversal. The latter is asynchronous, i.e. it may
    // pause at any chunk list in the subtree. If the balancer messes up subtree
    // structure in the meantime, there's a chance the traversal never reaches
    // all the relevant chunks.
    //
    // This scenario, however, is impossible. The balancer does not change
    // contents of any chunk list that's shared (i.e. has multiple strong refs).
    // And scheduling a requisition update takes a strong ref.
    //
    // The root is the exception: it's contents may, of course, change.
    // Luckily, rebalancing occurs immediately after scheduling a requisition
    // update, which does not start the actual traversal immediately.
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
            YT_VERIFY(currentChunkList->GetKind() == EChunkListKind::Static);
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
    const auto& settings = GetConfig()->GetSettingsForMode(EChunkTreeBalancerMode::Strict);
    if (!children->empty()) {
        auto* lastChild = children->back()->AsChunkList();
        if (std::ssize(lastChild->Children()) < settings->MinChunkListSize) {
            YT_VERIFY(lastChild->Statistics().Rank <= 1);
            YT_VERIFY(std::ssize(lastChild->Children()) <= settings->MaxChunkListSize);
            Callbacks_->FlushObjectUnrefs();
            if (Callbacks_->GetObjectRefCounter(lastChild) > 0) {
                // We want to merge to this chunk list but it is shared.
                // Copy on write.
                auto* clonedLastChild = Callbacks_->CreateChunkList();
                Callbacks_->AttachToChunkList(clonedLastChild, lastChild->Children());
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
            if (std::ssize(chunkList->Children()) <= settings->MaxChunkListSize) {
                Callbacks_->FlushObjectUnrefs();
                YT_VERIFY(Callbacks_->GetObjectRefCounter(chunkList) > 0);
                children->push_back(child);
                return;
            }
        }

        // We need to split the child. So we use usual merging.
        auto* newChunkList = Callbacks_->CreateChunkList();
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

    Callbacks_->FlushObjectUnrefs();
    YT_VERIFY(Callbacks_->GetObjectRefCounter(lastChunkList) == 0);

    YT_VERIFY(lastChunkList->Statistics().Rank <= 1);
    const auto& settings = GetConfig()->GetSettingsForMode(EChunkTreeBalancerMode::Strict);
    YT_VERIFY(std::ssize(lastChunkList->Children()) < settings->MinChunkListSize);

    switch (child->GetType()) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk: {
            // Just adding the chunk to the last chunk list.
            Callbacks_->AttachToChunkList(lastChunkList, {child});
            break;
        }

        case EObjectType::ChunkList: {
            auto* chunkList = child->AsChunkList();
            if (std::ssize(lastChunkList->Children()) + std::ssize(chunkList->Children()) <= settings->MaxChunkListSize) {
                // Just appending the chunk list to the last chunk list.
                Callbacks_->AttachToChunkList(lastChunkList, chunkList->Children());
            } else {
                // The chunk list is too large. We have to copy chunks by blocks.
                int mergedCount = 0;
                while (mergedCount < std::ssize(chunkList->Children())) {
                    if (std::ssize(lastChunkList->Children()) >= settings->MinChunkListSize) {
                        // The last chunk list is too large. Creating a new one.
                        YT_VERIFY(std::ssize(lastChunkList->Children()) == settings->MinChunkListSize);
                        lastChunkList = Callbacks_->CreateChunkList();
                        children->push_back(lastChunkList);
                    }
                    int count = std::min(
                        settings->MinChunkListSize - lastChunkList->Children().size(),
                        chunkList->Children().size() - mergedCount);
                    Callbacks_->AttachToChunkList(
                        lastChunkList,
                        MakeRange(chunkList->Children()).Slice(mergedCount, mergedCount + count));
                    mergedCount += count;
                }
            }
            break;
        }

        default:
            YT_ABORT();
    }
}

const TDynamicChunkTreeBalancerConfigPtr& TChunkTreeBalancer::GetConfig() const
{
    return Callbacks_->GetConfig();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
