#include "stdafx.h"
#include "chunk_tree_balancer.h"
#include "config.h"
#include "chunk_list.h"
#include "private.h"
#include "chunk_manager.h"

#include <ytlib/profiling/profiler.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/meta_state_facade.h>

namespace NYT {
namespace NChunkServer {

using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkTreeBalancer::TChunkTreeBalancer(
    NCellMaster::TBootstrap* bootstrap,
    TChunkTreeBalancerConfigPtr config)
    : Bootstrap(bootstrap)
    , Config(config)
{ }

bool TChunkTreeBalancer::CheckRebalanceNeeded(
    TChunkList* chunkList,
    NProto::TMetaReqRebalanceChunkTree* request)
{
    bool rebalanceNeeded = false;
    TChunkList* topmostFeasibleChunkList = NULL;
    auto* currentChunkList = chunkList;
    while (true) {
        if (!currentChunkList->GetRigid()) {
            topmostFeasibleChunkList = currentChunkList;
        }

        const auto& statistics = currentChunkList->Statistics();
        if (currentChunkList->Children().size() > Config->MaxChunkListSize ||
            statistics.Rank > Config->MaxChunkTreeRank ||
            statistics.ChunkListCount > statistics.ChunkCount * Config->MinChunkListToChunkRatio &&
            statistics.ChunkListCount > 2)
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

    *request->mutable_root_id() = chunkList->GetId().ToProto();
    request->set_min_chunk_list_size(Config->MinChunkListSize);
    request->set_max_chunk_list_size(Config->MaxChunkListSize);

    return true;
}

TChunkList* TChunkTreeBalancer::RebalanceChunkTree(const NProto::TMetaReqRebalanceChunkTree& request)
{
    auto chunkManager = Bootstrap->GetChunkManager();
    auto objectManager = Bootstrap->GetObjectManager();
    bool isRecovery = Bootstrap->GetMetaStateFacade()->GetManager()->IsRecovery();

    auto rootId = TChunkListId::FromProto(request.root_id());
    auto* root = chunkManager->FindChunkList(rootId);
    if (!root) {
        LOG_DEBUG_UNLESS(isRecovery, "Chunk tree balancing canceled: no such chunk list (RootId: %s)",
            ~ToString(rootId));
        return NULL;
    }

    if (root->GetRigid()) {
        LOG_DEBUG_UNLESS(isRecovery, "Chunk tree balancing canceled: chunk list is rigid (RootId: %s)",
            ~ToString(rootId));
        return NULL;
    }

    LOG_DEBUG_UNLESS(isRecovery, "Chunk tree balancing started (RootId: %s)",
        ~rootId.ToString());

    auto oldStatistics = root->Statistics();

    // Create new children list.
    std::vector<TChunkTreeRef> newChildren;

    AppendChunkTree(&newChildren, root, request);
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

    LOG_DEBUG_UNLESS(isRecovery, "Chunk tree balancing completed");
    return root;
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

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
