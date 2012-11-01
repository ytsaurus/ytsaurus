#include "stdafx.h"
#include "chunk_tree_balancer.h"
#include "config.h"
#include "chunk_list.h"
#include "private.h"
#include "chunk_manager.h"

#include <ytlib/profiling/profiler.h>
#include <server/cell_master/bootstrap.h>

namespace NYT {
namespace NChunkServer {

using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

// TODO(roizner): reuse chunklists with ref-count = 1

TChunkTreeBalancer::TChunkTreeBalancer(
    NCellMaster::TBootstrap* bootstrap,
    TChunkTreeBalancerConfigPtr config)
    : Bootstrap(bootstrap)
    , Config(config)
{ }

bool TChunkTreeBalancer::CheckRebalanceNeeded(
    TChunkList* chunkList,
    NProto::TMetaReqRebalanceChunkTree* message)
{
    bool rebalanceNeeded = false;
    TChunkList* topmostFeasibleChunkList = NULL;
    auto* currentChunkList = chunkList;
    while (true) {
        if (!currentChunkList->GetRigid()) {
            topmostFeasibleChunkList = currentChunkList;
        }

        if (currentChunkList->Children().size() > Config->MaxChunkListSize ||
            currentChunkList->Statistics().Rank > Config->MaxChunkTreeRank)
        {
            rebalanceNeeded = true;
        }

        if (currentChunkList->Parents().size() != 1) {
            break;
        }

        currentChunkList = *currentChunkList->Parents().begin();
    }
    
    if (!rebalanceNeeded || !topmostFeasibleChunkList) {
        return false;
    }

    InitRebalanceMessage(currentChunkList, message);
    return true;
}

bool TChunkTreeBalancer::RebalanceChunkTree(
    TChunkList* root,
    const NProto::TMetaReqRebalanceChunkTree& message)
{

    if (root->GetRigid() ||
        !root->Parents().empty() ||
        root->Statistics().Rank <= 1)
    {
        return false;
    }

    auto chunkManager = Bootstrap->GetChunkManager();
    auto objectManager = Bootstrap->GetObjectManager();

    auto oldStatistics = root->Statistics();

    // Create new children list.
    std::vector<TChunkTreeRef> newChildren;

    AppendChunkTree(&newChildren, root, message);
    YCHECK(!newChildren.empty());
    YCHECK(newChildren.front() != root);

    // Rewrite the root with newChildren.
    
    // Make a copy of key columns and set it back when the root is updated.
    auto sortedBy = root->SortedBy();

    // Add temporary references to the old children.
    auto oldChildren = root->Children();
    FOREACH (auto childRef, oldChildren) {
        objectManager->RefObject(childRef);
    }

    // Replace the children list and restore the key columns.
    chunkManager->ClearChunkList(root);
    chunkManager->AttachToChunkList(root, newChildren);
    root->SortedBy() = sortedBy;

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

    return true;
}

void TChunkTreeBalancer::AppendChunkTree(
    std::vector<TChunkTreeRef>* children,
    TChunkTreeRef child,
    const NProto::TMetaReqRebalanceChunkTree& message)
{
    auto chunkManager = Bootstrap->GetChunkManager();

    // Expand child chunk lists of rank > 1.
    if (child.GetType() == EObjectType::ChunkList) {
        const auto* chunkList = child.AsChunkList();
        if (chunkList->Statistics().Rank > 1) {
            FOREACH (const auto& childRef, chunkList->Children()) {
                AppendChunkTree(children, childRef, message);
            }
            return;
        }
    }

    // Can we reuse the last chunk list?
    bool merge = false;
    if (!children->empty()) {
        auto* lastChild = children->back().AsChunkList();
        if (lastChild->Children().size() < message.min_chunk_list_size()) {
            YASSERT(lastChild->Statistics().Rank <= 1);
            YASSERT(lastChild->Children().size() <= message.max_chunk_list_size());
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
            if (chunkList->Children().size() <= message.max_chunk_list_size()) {
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
    MergeChunkTrees(children, child, message);
}

void TChunkTreeBalancer::MergeChunkTrees(
    std::vector<TChunkTreeRef>* children,
    TChunkTreeRef child,
    const NProto::TMetaReqRebalanceChunkTree& message)
{
    // We are trying to add the child to the last chunk list.
    auto* lastChunkList = children->back().AsChunkList();

    YASSERT(lastChunkList->GetObjectRefCounter() == 0);
    YASSERT(lastChunkList->Statistics().Rank <= 1);
    YASSERT(lastChunkList->Children().size() < message.min_chunk_list_size());

    auto chunkManager = Bootstrap->GetChunkManager();

    switch (child.GetType()) {
    case EObjectType::Chunk: {
        // Just adding the chunk to the last chunk list.
        chunkManager->AttachToChunkList(lastChunkList, child);
        break;
                             }

    case EObjectType::ChunkList: {
        const auto* chunkList = child.AsChunkList();
        if (lastChunkList->Children().size() + chunkList->Children().size() <=
            message.max_chunk_list_size())
        {
            // Just appending the chunk list to the last chunk list.
            chunkManager->AttachToChunkList(lastChunkList, chunkList->Children());
        } else {
            // The chunk list is too large. We have to copy chunks by blocks.
            int mergedCount = 0;
            while (mergedCount < chunkList->Children().size()) {
                if (lastChunkList->Children().size() >= message.min_chunk_list_size()) {
                    // The last chunk list is too large. Creating a new one.
                    YASSERT(lastChunkList->Children().size() == message.min_chunk_list_size());
                    lastChunkList = chunkManager->CreateChunkList();
                    children->push_back(TChunkTreeRef(lastChunkList));
                }
                int count = Min(
                    message.min_chunk_list_size() - lastChunkList->Children().size(),
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

void TChunkTreeBalancer::InitRebalanceMessage(
    TChunkList* chunkList,
    NProto::TMetaReqRebalanceChunkTree* message)
{
    if (!message)
        return;

    *message->mutable_root_id() = chunkList->GetId().ToProto();
    
    message->set_min_chunk_list_size(Config->MinChunkListSize);
    message->set_max_chunk_list_size(Config->MaxChunkListSize);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
