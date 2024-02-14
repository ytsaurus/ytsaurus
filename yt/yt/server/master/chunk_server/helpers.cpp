#include "helpers.h"
#include "private.h"
#include "chunk.h"
#include "chunk_owner_base.h"
#include "chunk_manager.h"
#include "chunk_view.h"
#include "chunk_location.h"
#include "dynamic_store.h"
#include "job.h"

#include <yt/yt/server/master/cypress_server/cypress_manager.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>

#include <yt/yt/server/master/node_tracker_server/node.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/journal_client/helpers.h>

#include <yt/yt/ytlib/tablet_client/helpers.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/library/numeric/algorithm_helpers.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NChunkServer {

using namespace NYTree;
using namespace NYson;
using namespace NHydra;
using namespace NJournalClient;
using namespace NObjectClient;
using namespace NCypressServer;
using namespace NCypressClient;
using namespace NChunkClient;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NTabletClient;
using namespace NSecurityServer;
using namespace NObjectServer;
using namespace NConcurrency;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkServerLogger;

static const double ChunkListTombstoneRelativeThreshold = 0.5;
static const double ChunkListTombstoneAbsoluteThreshold = 16;

////////////////////////////////////////////////////////////////////////////////

bool CanUnambiguouslyDetachChild(TChunkList* rootChunkList, const TChunkTree* child)
{
    // Flush just once, avoid flushing in loop below.
    FlushObjectUnrefs();
    while (GetParentCount(child) == 1) {
        auto* parent = GetUniqueParent(child);
        if (parent == rootChunkList) {
            return true;
        }
        if (parent->GetObjectRefCounter() > 1) {
            return false;
        }
        child = parent;
    }

    return HasParent(child, rootChunkList);
}

int GetChildIndex(const TChunkList* chunkList, const TChunkTree* child)
{
    const auto& children = chunkList->Children();
    if (chunkList->HasChildToIndexMapping()) {
        int index = GetOrCrash(chunkList->ChildToIndex(), const_cast<TChunkTree*>(child));
        YT_VERIFY(children[index] == child);
        return index;
    } else {
        // Typically called for the trailing chunks.
        for (int index = static_cast<int>(children.size()) - 1; index >= 0; --index) {
            if (children[index] == child) {
                return index;
            }
        }
        YT_ABORT();
    }
}

TChunkTree* FindFirstUnsealedChild(const TChunkList* chunkList)
{
    const auto& children = chunkList->Children();
    auto index = BinarySearch(0, children.size(), [&] (size_t index) {
        return children[index]->IsSealed();
    });
    return index < children.size() ? children[index] : nullptr;
}

i64 GetJournalChunkStartRowIndex(const TChunk* chunk)
{
    if (!chunk->IsJournal()) {
        THROW_ERROR_EXCEPTION("%v is not a journal chunk",
            chunk->GetId());
    }

    auto* chunkList = GetUniqueParentOrThrow(chunk)->AsChunkList();

    auto chunkIndex = GetChildIndex(chunkList, chunk);
    if (chunkIndex == 0) {
        return 0;
    }

    if (!chunkList->Children()[chunkIndex - 1]->IsSealed()) {
        THROW_ERROR_EXCEPTION("%v is not the first unsealed chunk in chunk list %v",
            chunk->GetId(),
            chunkList->GetId());
    }

    return chunkList->CumulativeStatistics().GetPreviousSum(chunkIndex).RowCount;
}

TChunkList* GetUniqueParent(const TChunkTree* chunkTree)
{
    switch (chunkTree->GetType()) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
        case EObjectType::JournalChunk:
        case EObjectType::ErasureJournalChunk: {
            const auto& parents = chunkTree->AsChunk()->Parents();
            if (parents.empty()) {
                return nullptr;
            }
            YT_VERIFY(parents.size() == 1);
            auto [parent, cardinality] = *parents.begin();
            YT_VERIFY(cardinality == 1);
            YT_VERIFY(parent->GetType() == EObjectType::ChunkList);
            return parent->AsChunkList();
        }

        case EObjectType::ChunkView: {
            const auto& parents = chunkTree->AsChunkView()->Parents();
            if (parents.empty()) {
                return nullptr;
            }
            YT_VERIFY(parents.size() == 1);
            return parents[0];
        }

        case EObjectType::SortedDynamicTabletStore:
        case EObjectType::OrderedDynamicTabletStore: {
            const auto& parents = chunkTree->AsDynamicStore()->Parents();
            if (parents.empty()) {
                return nullptr;
            }
            YT_VERIFY(parents.size() == 1);
            YT_VERIFY(parents[0]->GetType() == EObjectType::ChunkList);
            return parents[0]->AsChunkList();
        }

        case EObjectType::ChunkList: {
            const auto& parents = chunkTree->AsChunkList()->Parents();
            if (parents.Empty()) {
                return nullptr;
            }
            YT_VERIFY(parents.Size() == 1);
            return *parents.begin();
        }

        default:
            YT_ABORT();
    }
}

TChunkList* GetUniqueParentOrThrow(const TChunkTree* chunkTree)
{
    if (auto parentCount = GetParentCount(chunkTree); parentCount != 1) {
        THROW_ERROR_EXCEPTION("Improper number of parents of chunk tree %v: expected 1, actual %v",
            chunkTree->GetId(),
            parentCount);
    }
    return GetUniqueParent(chunkTree);
}

int GetParentCount(const TChunkTree* chunkTree)
{
    switch (chunkTree->GetType()) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
        case EObjectType::JournalChunk:
        case EObjectType::ErasureJournalChunk:
            return chunkTree->AsChunk()->GetParentCount();

        case EObjectType::ChunkView:
            return chunkTree->AsChunkView()->Parents().size();

        case EObjectType::SortedDynamicTabletStore:
        case EObjectType::OrderedDynamicTabletStore:
            return chunkTree->AsDynamicStore()->Parents().size();

        case EObjectType::ChunkList:
            return chunkTree->AsChunkList()->Parents().size();

        default:
            YT_ABORT();
    }
}

bool HasParent(const TChunkTree* chunkTree, TChunkList* potentialParent)
{
    switch (chunkTree->GetType()) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
        case EObjectType::JournalChunk:
        case EObjectType::ErasureJournalChunk:
            return chunkTree->AsChunk()->Parents().contains(potentialParent);

        case EObjectType::ChunkView: {
            const auto& parents = chunkTree->AsChunkView()->Parents();
            return std::find(parents.begin(), parents.end(), potentialParent) != parents.end();
        }

        case EObjectType::SortedDynamicTabletStore:
        case EObjectType::OrderedDynamicTabletStore: {
            const auto& parents = chunkTree->AsDynamicStore()->Parents();
            return std::find(parents.begin(), parents.end(), potentialParent) != parents.end();
        }

        case EObjectType::ChunkList: {
            const auto& parents = chunkTree->AsChunkList()->Parents();
            return std::find(parents.begin(), parents.end(), potentialParent) != parents.end();
        }

        default:
            YT_ABORT();
    }
}

void AttachToChunkList(
    TChunkList* chunkList,
    TRange<TChunkTree*> children)
{
    // A shortcut.
    if (children.empty()) {
        return;
    }

    if (chunkList->GetKind() == EChunkListKind::JournalRoot) {
        // Make sure all new children have the same value of "sealed" and "overlayed" flags.
        const auto* firstChild = children[0];
        bool childrenSealed = firstChild->IsSealed();
        bool childrenOverlayed = firstChild->GetOverlayed();
        for (const auto* child : children) {
            if (bool childSealed = child->IsSealed(); childSealed != childrenSealed) {
                THROW_ERROR_EXCEPTION("Improper sealed flag for child %v: expected %Qlv, actual %Qlv",
                    child->GetId(),
                    childrenSealed,
                    childSealed);
            }
            if (bool childOverlayed = child->GetOverlayed(); childOverlayed != childrenOverlayed) {
                THROW_ERROR_EXCEPTION("Improper overlayed flag for child %v: expected %Qlv, actual %Qlv",
                    child->GetId(),
                    childrenOverlayed,
                    childOverlayed);
            }
        }

        // Unsealed children may only appear at the very end of the resulting chunk list.
        if (!chunkList->IsSealed() && childrenSealed) {
            THROW_ERROR_EXCEPTION("Cannot append a sealed child %v to chunk list %v since the latter is not sealed",
                firstChild->GetId(),
                chunkList->GetId());
        }

        // Multiple unsealed chunks are only possible if they all are overlayed.
        int unsealedCount = 0;
        if (!chunkList->IsSealed()) {
            // NB: Maybe more actually but will suffice for the purpose of the check.
            ++unsealedCount;
        }
        if (!childrenSealed) {
            unsealedCount += std::ssize(children);
        }
        if (unsealedCount > 1) {
            if (!childrenOverlayed) {
                THROW_ERROR_EXCEPTION("Cannot append child %v to unsealed chunk list %v since the former is not overlayed",
                    firstChild->GetId(),
                    chunkList->GetId());
            }
            if (!chunkList->IsSealed()) {
                const auto* lastExistingChild = chunkList->Children().back();
                if (!lastExistingChild->GetOverlayed()) {
                    THROW_ERROR_EXCEPTION("Cannot append child %v to unsealed chunk list %v since its last child %v is not overlayed",
                        firstChild->GetId(),
                        chunkList->GetId(),
                        lastExistingChild->GetId());
                }
            }
        }
    }

    if (chunkList->HasChildToIndexMapping()) {
        for (auto* child : children) {
            if (chunkList->HasChild(child)) {
                THROW_ERROR_EXCEPTION("Cannot append a duplicate child %v to chunk list %v",
                    child->GetId(),
                    chunkList->GetId());
            }
        }
    }

    // NB: Accumulate statistics from left to right to get Sealed flag correct.
    TChunkTreeStatistics statisticsDelta;
    for (auto* child : children) {
        AppendChunkTreeChild(chunkList, child, &statisticsDelta);
        SetChunkTreeParent(chunkList, child);
    }

    chunkList->IncrementVersion();

    ++statisticsDelta.Rank;
    chunkList->Statistics().Accumulate(statisticsDelta);
    AccumulateUniqueAncestorsStatistics(chunkList, statisticsDelta);
}

void DetachFromChunkList(
    TChunkList* chunkList,
    TRange<TChunkTree*> children,
    EChunkDetachPolicy policy)
{
    // A shortcut.
    if (children.empty()) {
        return;
    }

    chunkList->IncrementVersion();

    TChunkTreeStatistics statisticsDelta;
    for (auto* child : children) {
        statisticsDelta.Accumulate(GetChunkTreeStatistics(child));
        ResetChunkTreeParent(chunkList, child);
    }

    auto& existingChildren = chunkList->Children();
    switch (policy) {
        case EChunkDetachPolicy::OrderedTabletSuffix: {
            YT_VERIFY(chunkList->GetKind() == EChunkListKind::OrderedDynamicTablet);

            YT_VERIFY(chunkList->Children().back()->GetType() ==
                EObjectType::OrderedDynamicTabletStore);

            int childCount = std::ssize(children);
            int firstChildIndex = std::ssize(existingChildren) - childCount;

            YT_VERIFY(firstChildIndex >= chunkList->GetTrimmedChildCount());
            for (int index = 0; index < childCount; ++index) {
                if (existingChildren[firstChildIndex + index] != children[index]) {
                    YT_LOG_ALERT("Attempted to detach children from ordered tablet out of order "
                        "(ChunkListId: %v, RelativeStoreIndex: %v, DetachedStoreId: %v, ActualStoreId: %v)",
                        chunkList->GetId(),
                        index,
                        children[index]->GetId(),
                        existingChildren[firstChildIndex + index]->GetId());
                }
            }

            existingChildren.erase(existingChildren.begin() + firstChildIndex, existingChildren.end());
            chunkList->CumulativeStatistics().TrimBack(childCount);
            break;
        }

        case EChunkDetachPolicy::OrderedTabletPrefix: {
            YT_VERIFY(chunkList->GetKind() == EChunkListKind::OrderedDynamicTablet);

            int childIndex = chunkList->GetTrimmedChildCount();
            for (auto* child : children) {
                YT_VERIFY(child == existingChildren[childIndex]);
                existingChildren[childIndex] = nullptr;
                ++childIndex;
            }
            int newTrimmedChildCount = chunkList->GetTrimmedChildCount() + std::ssize(children);
            if (newTrimmedChildCount > ChunkListTombstoneAbsoluteThreshold &&
                newTrimmedChildCount > existingChildren.size() * ChunkListTombstoneRelativeThreshold)
            {
                existingChildren.erase(
                    existingChildren.begin(),
                    existingChildren.begin() + newTrimmedChildCount);

                chunkList->CumulativeStatistics().TrimFront(newTrimmedChildCount);

                chunkList->SetTrimmedChildCount(0);
            } else {
                chunkList->SetTrimmedChildCount(newTrimmedChildCount);
            }
            // NB: Do not change logical row count, chunk count and data weight.
            statisticsDelta.LogicalRowCount = 0;
            statisticsDelta.LogicalChunkCount = 0;
            statisticsDelta.LogicalDataWeight = 0;
            break;
        }

        case EChunkDetachPolicy::SortedTablet:
        case EChunkDetachPolicy::HunkTablet: {
            switch (policy) {
                case EChunkDetachPolicy::SortedTablet:
                    YT_VERIFY(
                        chunkList->GetKind() == EChunkListKind::SortedDynamicTablet ||
                        chunkList->GetKind() == EChunkListKind::SortedDynamicSubtablet ||
                        chunkList->GetKind() == EChunkListKind::Hunk);
                    break;
                case EChunkDetachPolicy::HunkTablet:
                    YT_VERIFY(chunkList->GetKind() == EChunkListKind::HunkTablet);
                    break;
                default:
                    YT_ABORT();
            }

            // Can handle arbitrary children.
            // Used in sorted tablet compaction.
            YT_VERIFY(chunkList->HasChildToIndexMapping());
            auto& childToIndex = chunkList->ChildToIndex();
            for (auto* child : children) {
                auto indexIt = childToIndex.find(child);
                YT_VERIFY(indexIt != childToIndex.end());
                int index = indexIt->second;

                // To remove child from the middle we swap it with the last one and update
                // cumulative statistics accordingly.
                if (index != std::ssize(existingChildren) - 1) {
                    auto delta = TCumulativeStatisticsEntry(GetChunkTreeStatistics(existingChildren.back())) -
                        TCumulativeStatisticsEntry(GetChunkTreeStatistics(existingChildren[index]));
                    chunkList->CumulativeStatistics().Update(index, delta);

                    existingChildren[index] = existingChildren.back();
                    childToIndex[existingChildren[index]] = index;
                }

                chunkList->CumulativeStatistics().PopBack();
                childToIndex.erase(indexIt);
                existingChildren.pop_back();
            }
            break;
        }

        default:
            YT_ABORT();
    }

    // Go upwards and recompute statistics.
    VisitUniqueAncestors(
        chunkList,
        [&] (TChunkList* current, TChunkTree* child) {
            current->Statistics().Deaccumulate(statisticsDelta);
            if (child && current->HasModifiableCumulativeStatistics()) {
                int index = GetChildIndex(current, child);
                current->CumulativeStatistics().Update(
                    index,
                    TCumulativeStatisticsEntry{} - TCumulativeStatisticsEntry(statisticsDelta));
            }
        });
}

void ReplaceChunkListChild(TChunkList* chunkList, int childIndex, TChunkTree* newChild)
{
    auto& children = chunkList->Children();

    auto* oldChild = children[childIndex];

    children[childIndex] = newChild;

    ResetChunkTreeParent(chunkList, oldChild);
    SetChunkTreeParent(chunkList, newChild);

    if (chunkList->HasChildToIndexMapping()) {
        auto& childToIndex = chunkList->ChildToIndex();
        auto oldChildIt = childToIndex.find(oldChild);
        YT_VERIFY(oldChildIt != childToIndex.end());
        childToIndex.erase(oldChildIt);
        YT_VERIFY(childToIndex.emplace(newChild, childIndex).second);
    }
}

void SetChunkTreeParent(TChunkTree* parent, TChunkTree* child)
{
    switch (child->GetType()) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
        case EObjectType::JournalChunk:
        case EObjectType::ErasureJournalChunk:
            child->AsChunk()->AddParent(parent);
            break;
        case EObjectType::ChunkView:
            child->AsChunkView()->AddParent(parent->AsChunkList());
            break;
        case EObjectType::SortedDynamicTabletStore:
        case EObjectType::OrderedDynamicTabletStore:
            child->AsDynamicStore()->AddParent(parent);
            break;
        case EObjectType::ChunkList:
            child->AsChunkList()->AddParent(parent->AsChunkList());
            break;
        default:
            YT_ABORT();
    }
}

void ResetChunkTreeParent(TChunkTree* parent, TChunkTree* child)
{
    switch (child->GetType()) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
        case EObjectType::JournalChunk:
        case EObjectType::ErasureJournalChunk:
            child->AsChunk()->RemoveParent(parent);
            break;
        case EObjectType::ChunkView:
            child->AsChunkView()->RemoveParent(parent->AsChunkList());
            break;
        case EObjectType::SortedDynamicTabletStore:
        case EObjectType::OrderedDynamicTabletStore:
            child->AsDynamicStore()->RemoveParent(parent);
            break;
        case EObjectType::ChunkList:
            child->AsChunkList()->RemoveParent(parent->AsChunkList());
            break;
        default:
            YT_ABORT();
    }
}

TChunkTreeStatistics GetChunkTreeStatistics(TChunkTree* chunkTree)
{
    if (!chunkTree) {
        return TChunkTreeStatistics();
    }
    switch (chunkTree->GetType()) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
        case EObjectType::JournalChunk:
        case EObjectType::ErasureJournalChunk:
            return chunkTree->AsChunk()->GetStatistics();
        case EObjectType::ChunkView:
            return chunkTree->AsChunkView()->GetStatistics();
        case EObjectType::SortedDynamicTabletStore:
        case EObjectType::OrderedDynamicTabletStore:
            return chunkTree->AsDynamicStore()->GetStatistics();
        case EObjectType::ChunkList:
            return chunkTree->AsChunkList()->Statistics();
        default:
            YT_ABORT();
    }
}

void AppendChunkTreeChild(
    TChunkList* chunkList,
    TChunkTree* child,
    TChunkTreeStatistics* statistics)
{
    if (chunkList->HasCumulativeStatistics()) {
        chunkList->CumulativeStatistics().PushBack(TCumulativeStatisticsEntry{
            GetChunkTreeStatistics(child)
        });
    }

    if (child && chunkList->HasChildToIndexMapping()) {
        int index = std::ssize(chunkList->Children());
        YT_VERIFY(chunkList->ChildToIndex().emplace(child, index).second);
    }

    statistics->Accumulate(GetChunkTreeStatistics(child));
    chunkList->Children().push_back(child);
}

void AccumulateAncestorsStatistics(
    TChunkTree* child,
    const TChunkTreeStatistics& statisticsDelta)
{
    for (const auto& [parent, _] : child->AsChunk()->Parents()) {
        auto mutableStatisticsDelta = statisticsDelta;

        VisitUniqueAncestors(
            parent->AsChunkList(),
            [&] (TChunkList* parent, TChunkTree* child) {
                ++mutableStatisticsDelta.Rank;
                parent->Statistics().Accumulate(mutableStatisticsDelta);

                if (parent->HasCumulativeStatistics()) {
                    auto& cumulativeStatistics = parent->CumulativeStatistics();
                    TCumulativeStatisticsEntry entry{mutableStatisticsDelta};

                    int index = GetChildIndex(parent, child);
                    cumulativeStatistics.Update(index, entry);
                }
            },
            child);
    }
}

void AccumulateUniqueAncestorsStatistics(
    TChunkTree* child,
    const TChunkTreeStatistics& statisticsDelta)
{
    auto* parent = GetUniqueParent(child);
    if (!parent) {
        return;
    }

    auto mutableStatisticsDelta = statisticsDelta;
    // TODO(aleksandra-zh): remove copypaste.
    VisitUniqueAncestors(
        parent,
        [&] (TChunkList* parent, TChunkTree* child) {
            ++mutableStatisticsDelta.Rank;
            parent->Statistics().Accumulate(mutableStatisticsDelta);

            if (parent->HasCumulativeStatistics()) {
                auto& cumulativeStatistics = parent->CumulativeStatistics();
                TCumulativeStatisticsEntry entry{mutableStatisticsDelta};

                int index = GetChildIndex(parent, child);
                cumulativeStatistics.Update(index, entry);
            }
        },
        child);
}

void ResetChunkListStatistics(TChunkList* chunkList)
{
    chunkList->CumulativeStatistics().Clear();
    chunkList->Statistics() = TChunkTreeStatistics();
    chunkList->Statistics().ChunkListCount = 1;
    chunkList->Statistics().Rank = 1;
}

void RecomputeChunkListStatistics(TChunkList* chunkList)
{
    ResetChunkListStatistics(chunkList);

    if (chunkList->HasAppendableCumulativeStatistics()) {
        chunkList->CumulativeStatistics().DeclareAppendable();
    } else if (chunkList->HasModifiableCumulativeStatistics()) {
        chunkList->CumulativeStatistics().DeclareModifiable();
    } else if (chunkList->HasTrimmableCumulativeStatistics()) {
        chunkList->CumulativeStatistics().DeclareTrimmable();
    }

    std::vector<TChunkTree*> children;
    children.swap(chunkList->Children());
    chunkList->ChildToIndex().clear();

    TChunkTreeStatistics statistics;
    for (auto* child : children) {
        AppendChunkTreeChild(chunkList, child, &statistics);
    }

    ++statistics.Rank;
    ++statistics.ChunkListCount;
    chunkList->Statistics() = statistics;
}

void RecomputeChildToIndexMapping(TChunkList* chunkList)
{
    YT_VERIFY(chunkList->HasChildToIndexMapping());

    auto& mapping = chunkList->ChildToIndex();
    mapping.clear();

    const auto& children = chunkList->Children();
    for (int index = 0; index < std::ssize(children); ++index) {
        auto* child = children[index];
        YT_VERIFY(mapping.emplace(child, index).second);
    }
}

std::vector<TChunkOwnerBase*> GetOwningNodes(TChunkTree* chunkTree)
{
    THashSet<TChunkOwnerBase*> owningNodes;
    THashSet<TChunkTree*> visitedTrees;
    std::vector<TChunkTree*> queue{chunkTree};

    auto visit = [&] (TChunkTree* chunkTree) {
        if (visitedTrees.insert(chunkTree).second) {
            queue.push_back(chunkTree);
        }
    };

    visit(chunkTree);

    for (int index = 0; index < std::ssize(queue); ++index) {
        chunkTree = queue[index];

        switch (chunkTree->GetType()) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk:
            case EObjectType::JournalChunk:
            case EObjectType::ErasureJournalChunk: {
                for (auto [parent, cardinality] : chunkTree->AsChunk()->Parents()) {
                    visit(parent);
                }
                break;
            }
            case EObjectType::ChunkView: {
                for (auto* parent : chunkTree->AsChunkView()->Parents()) {
                    visit(parent);
                }
                break;
            }
            case EObjectType::SortedDynamicTabletStore:
            case EObjectType::OrderedDynamicTabletStore: {
                for (auto* parent : chunkTree->AsDynamicStore()->Parents()) {
                    visit(parent);
                }
                break;
            }
            case EObjectType::ChunkList: {
                auto* chunkList = chunkTree->AsChunkList();
                owningNodes.insert(chunkList->TrunkOwningNodes().begin(), chunkList->TrunkOwningNodes().end());
                owningNodes.insert(chunkList->BranchedOwningNodes().begin(), chunkList->BranchedOwningNodes().end());
                for (auto* parent : chunkList->Parents()) {
                    visit(parent);
                }
                break;
            }
            default:
                YT_ABORT();
        }
    }

    return std::vector<TChunkOwnerBase*>(owningNodes.begin(), owningNodes.end());
}

namespace {

TYsonString DoGetMulticellOwningNodes(
    NCellMaster::TBootstrap* bootstrap,
    TChunkTreeId chunkTreeId)
{
    std::vector<TVersionedObjectId> nodeIds;

    const auto& chunkManager = bootstrap->GetChunkManager();
    if (auto* chunkTree = chunkManager->FindChunkTree(chunkTreeId); IsObjectAlive(chunkTree)) {
        auto nodes = GetOwningNodes(chunkTree);
        for (const auto* node : nodes) {
            TTransactionId transactionId;
            if (auto* transaction = node->GetTransaction()) {
                transactionId = transaction->IsExternalized()
                    ? transaction->GetOriginalTransactionId()
                    : transaction->GetId();
            }
            nodeIds.emplace_back(node->GetId(), transactionId);
        }
    }

    const auto& multicellManager = bootstrap->GetMulticellManager();

    std::vector<std::pair<TCellTag, TFuture<TChunkServiceProxy::TRspGetChunkOwningNodesPtr>>> requestFutures;
    requestFutures.reserve(multicellManager->GetCellCount());

    // Request owning nodes from all cells.
    auto requestIdsFromCell = [&] (TCellTag cellTag) {
        if (cellTag == multicellManager->GetCellTag()) {
            return;
        }

        auto type = TypeFromId(chunkTreeId);
        if (!IsPhysicalChunkType(type)) {
            return;
        }

        auto channel = multicellManager->GetMasterChannelOrThrow(
            cellTag,
            NHydra::EPeerKind::Follower);
        TChunkServiceProxy proxy(channel);

        auto req = proxy.GetChunkOwningNodes();
        ToProto(req->mutable_chunk_id(), chunkTreeId);

        requestFutures.emplace_back(cellTag, req->Invoke());
    };

    requestIdsFromCell(multicellManager->GetPrimaryCellTag());
    for (auto cellTag : multicellManager->GetSecondaryCellTags()) {
        requestIdsFromCell(cellTag);
    }

    for (const auto& [cellTag, future] : requestFutures) {
        auto rspOrError = WaitFor(future);
        if (rspOrError.GetCode() == NChunkClient::EErrorCode::NoSuchChunk) {
            continue;
        }

        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error requesting owning nodes for chunk %v from cell %v",
            chunkTreeId,
            cellTag);
        const auto& rsp = rspOrError.Value();

        for (const auto& protoNode : rsp->nodes()) {
            nodeIds.emplace_back(
                FromProto<NCypressClient::TNodeId>(protoNode.node_id()),
                FromProto<TTransactionId>(protoNode.transaction_id()));
        }
    }

    {
        const auto& objectManager = bootstrap->GetObjectManager();
        auto pathOrErrors = WaitFor(objectManager->ResolveObjectIdsToPaths(nodeIds))
            .ValueOrThrow();

        TStringStream stream;
        TBufferedBinaryYsonWriter writer(&stream);
        writer.OnBeginList();

        for (int index = 0; index < std::ssize(nodeIds); ++index) {
            const auto& pathOrError = pathOrErrors[index];
            const auto& versionedId = nodeIds[index];
            auto code = pathOrError.GetCode();
            if (code == NYTree::EErrorCode::ResolveError || code == NTransactionClient::EErrorCode::NoSuchTransaction) {
                continue;
            }

            THROW_ERROR_EXCEPTION_IF_FAILED(pathOrError, "Error requesting path for node %v",
                versionedId);
            const auto& path = pathOrError.Value();

            writer.OnListItem();
            SerializeNodePath(&writer, path.Path, path.TransactionId);
        }

        writer.OnEndList();
        writer.Flush();
        return TYsonString(stream.Str());
    }
}

} // namespace

TFuture<TYsonString> GetMulticellOwningNodes(
    NCellMaster::TBootstrap* bootstrap,
    TChunkTree* chunkTree)
{
    return BIND(&DoGetMulticellOwningNodes, bootstrap, chunkTree->GetId())
        .AsyncVia(GetCurrentInvoker())
        .Run();
}

////////////////////////////////////////////////////////////////////////////////

void SerializeNodePath(
    IYsonConsumer* consumer,
    const NYPath::TYPath& path,
    TTransactionId transactionId)
{
    if (transactionId) {
        consumer->OnBeginAttributes();
        consumer->OnKeyedItem("transaction_id");
        NYTree::Serialize(transactionId, consumer);
        consumer->OnEndAttributes();
    }
    consumer->OnStringScalar(path);
}

////////////////////////////////////////////////////////////////////////////////

bool IsEmpty(const TChunkList* chunkList)
{
    return !chunkList || chunkList->Statistics().LogicalChunkCount == 0;
}

bool IsEmpty(const TChunkTree* chunkTree)
{
    if (!chunkTree) {
        return true;
    } else if (chunkTree->GetType() == EObjectType::ChunkList) {
        return IsEmpty(chunkTree->AsChunkList());
    } else {
        return false;
    }
}

////////////////////////////////////////////////////////////////////////////////

TLegacyOwningKey GetUpperBoundKeyOrThrow(const TChunk* chunk, std::optional<int> keyColumnCount)
{
    auto optionalBoundaryKeysExt = chunk->ChunkMeta()->FindExtension<TBoundaryKeysExt>();
    if (!optionalBoundaryKeysExt) {
        THROW_ERROR_EXCEPTION("Cannot compute max key in chunk %v since it's missing boundary info",
            chunk->GetId());
    }

    auto key = FromProto<TLegacyOwningKey>(optionalBoundaryKeysExt->max());
    if (!keyColumnCount || *keyColumnCount == key.GetCount()) {
        return GetKeySuccessor(key);
    } else if (*keyColumnCount < key.GetCount()) {
        return GetKeySuccessor(GetKeyPrefix(key, *keyColumnCount));
    } else {
        // NB: Here we add `Max` at the end of key (instead of `Min` as in another if-branch),
        // however it doesn't affect anything, because key is widened first.
        return WidenKeySuccessor(key, *keyColumnCount);
    }
}

TLegacyOwningKey GetUpperBoundKeyOrThrow(const TChunkView* chunkView, std::optional<int> keyColumnCount)
{
    auto chunkUpperBound = GetUpperBoundKeyOrThrow(chunkView->GetUnderlyingTree(), keyColumnCount);
    const auto& upperLimit = chunkView->ReadRange().UpperLimit();
    if (!upperLimit.HasLegacyKey()) {
        return chunkUpperBound;
    }

    const auto& upperLimitKey = upperLimit.GetLegacyKey();
    if (!keyColumnCount || *keyColumnCount == upperLimitKey.GetCount()) {
        return std::min(chunkUpperBound, upperLimitKey);
    } else {
        if (*keyColumnCount < upperLimitKey.GetCount()) {
            THROW_ERROR_EXCEPTION("Unexpected key shortening for chunk view")
                << TErrorAttribute("chunk_view_id", chunkView->GetId())
                << TErrorAttribute("key_column_count", *keyColumnCount)
                << TErrorAttribute("key", ToString(upperLimitKey));
        }
        return std::min(chunkUpperBound, WidenKey(upperLimitKey, *keyColumnCount));
    }
}

TLegacyOwningKey GetUpperBoundKeyOrThrow(const TChunkTree* chunkTree, std::optional<int> keyColumnCount)
{
    if (IsEmpty(chunkTree)) {
        THROW_ERROR_EXCEPTION("Cannot compute upper bound key in chunk list %v since it contains no chunks",
            chunkTree->GetId());
    }

    auto getLastNonemptyChild = [] (const TChunkList* chunkList) {
        const auto& children = chunkList->Children();
        for (auto it = children.rbegin(); it != children.rend(); ++it) {
            const auto* child = *it;
            if (!IsEmpty(child)) {
                return child;
            }
        }
        YT_ABORT();
    };

    const auto* currentChunkTree = chunkTree;
    while (true) {
        switch (currentChunkTree->GetType()) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk:
                return GetUpperBoundKeyOrThrow(currentChunkTree->AsChunk(), keyColumnCount);

            case EObjectType::ChunkView:
                return GetUpperBoundKeyOrThrow(currentChunkTree->AsChunkView(), keyColumnCount);

            case EObjectType::SortedDynamicTabletStore:
                return MaxKey();

            case EObjectType::ChunkList:
                currentChunkTree = getLastNonemptyChild(currentChunkTree->AsChunkList());
                break;

            default:
                THROW_ERROR_EXCEPTION("Cannot compute max key of %Qlv chunk tree",
                    currentChunkTree->GetType());
        }
    }
}

TOwningKeyBound GetUpperKeyBoundOrThrow(const TChunkTree* chunkTree, int keyColumnCount)
{
    // TODO(max42): rewrite without using function above.
    auto upperBoundKey = GetUpperBoundKeyOrThrow(chunkTree, keyColumnCount);
    // NB: upper bound key may contain min/max sentinels.
    return KeyBoundFromLegacyRow(upperBoundKey, /*isUpper*/ true, keyColumnCount);
}

TLegacyOwningKey GetMinKeyOrThrow(const TChunk* chunk, std::optional<int> keyColumnCount)
{
    auto optionalBoundaryKeysExt = chunk->ChunkMeta()->FindExtension<TBoundaryKeysExt>();
    if (!optionalBoundaryKeysExt) {
        THROW_ERROR_EXCEPTION("Cannot compute min key in chunk %v since it's missing boundary info",
            chunk->GetId());
    }

    auto minKey = FromProto<TLegacyOwningKey>(optionalBoundaryKeysExt->min());
    if (!keyColumnCount || *keyColumnCount == minKey.GetCount()) {
        return minKey;
    } else if (*keyColumnCount < minKey.GetCount()) {
        return GetKeyPrefix(minKey, *keyColumnCount);
    } else {
        return WidenKey(minKey, *keyColumnCount);
    }
}

TLegacyOwningKey GetMinKey(const TChunkView* chunkView, std::optional<int> keyColumnCount)
{
    auto chunkMinKey = GetMinKeyOrThrow(chunkView->GetUnderlyingTree(), keyColumnCount);
    const auto& lowerLimit = chunkView->ReadRange().LowerLimit();
    if (!lowerLimit.HasLegacyKey()) {
        return chunkMinKey;
    }

    const auto& lowerLimitKey = lowerLimit.GetLegacyKey();
    if (!keyColumnCount || *keyColumnCount == lowerLimitKey.GetCount()) {
        return std::max(chunkMinKey, lowerLimitKey);
    } else {
        if (*keyColumnCount < lowerLimitKey.GetCount()) {
            THROW_ERROR_EXCEPTION("Unexpected key shortening for chunk view")
                << TErrorAttribute("chunk_view_id", chunkView->GetId())
                << TErrorAttribute("key_column_count", *keyColumnCount)
                << TErrorAttribute("key", ToString(lowerLimitKey));
        }
        return std::max(chunkMinKey, WidenKey(lowerLimitKey, *keyColumnCount));
    }
}

TLegacyOwningKey GetMinKeyOrThrow(const TChunkTree* chunkTree, std::optional<int> keyColumnCount)
{
    if (IsEmpty(chunkTree)) {
        THROW_ERROR_EXCEPTION("Cannot compute min key in chunk list %v since it contains no chunks",
            chunkTree->GetId());
    }

    auto getFirstNonemptyChild = [] (const TChunkList* chunkList) {
        const auto& children = chunkList->Children();
        for (auto it = children.begin(); it != children.end(); ++it) {
            const auto* child = *it;
            if (!IsEmpty(child)) {
                return child;
            }
        }
        YT_ABORT();
    };

    const auto* currentChunkTree = chunkTree;
    while (true) {
        switch (currentChunkTree->GetType()) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk:
                return GetMinKeyOrThrow(currentChunkTree->AsChunk(), keyColumnCount);

            case EObjectType::ChunkView:
                return GetMinKey(currentChunkTree->AsChunkView(), keyColumnCount);

            case EObjectType::SortedDynamicTabletStore:
                return MinKey();

            case EObjectType::ChunkList:
                currentChunkTree = getFirstNonemptyChild(currentChunkTree->AsChunkList());
                break;

            default:
                THROW_ERROR_EXCEPTION("Cannot compute min key of %Qlv chunk tree",
                    currentChunkTree->GetType());
        }
    }
}

TOwningKeyBound GetLowerKeyBoundOrThrow(const TChunkTree* chunkTree, int keyColumnCount)
{
    // TODO(max42): rewrite without using function above.
    auto lowerBoundKey = GetMinKeyOrThrow(chunkTree, keyColumnCount);
    // NB: min key may contain <min> for dynamic stores.
    return KeyBoundFromLegacyRow(lowerBoundKey, /*isUpper*/ false, keyColumnCount);
}

TLegacyOwningKey GetMaxKeyOrThrow(const TChunk* chunk)
{
    auto optionalBoundaryKeysExt = chunk->ChunkMeta()->FindExtension<TBoundaryKeysExt>();
    if (!optionalBoundaryKeysExt) {
        THROW_ERROR_EXCEPTION("Cannot compute max key in chunk %v since it is missing boundary info",
            chunk->GetId());
    }

    return FromProto<TLegacyOwningKey>(optionalBoundaryKeysExt->max());
}

TLegacyOwningKey GetMaxKeyOrThrow(const TChunkTree* chunkTree)
{
    if (IsEmpty(chunkTree)) {
        THROW_ERROR_EXCEPTION("Cannot compute max key in chunk list %v since it contains no chunks",
            chunkTree->GetId());
    }

    auto getLastNonemptyChild = [] (const TChunkList* chunkList) {
        const auto& children = chunkList->Children();
        for (auto it = children.rbegin(); it != children.rend(); ++it) {
            const auto* child = *it;
            if (!IsEmpty(child)) {
                return child;
            }
        }
        YT_ABORT();
    };

    const auto* currentChunkTree = chunkTree;
    while (true) {
        switch (currentChunkTree->GetType()) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk:
                return GetMaxKeyOrThrow(currentChunkTree->AsChunk());

            case EObjectType::ChunkList:
                currentChunkTree = getLastNonemptyChild(currentChunkTree->AsChunkList());
                break;

            default:
                THROW_ERROR_EXCEPTION("Cannot compute max key of %Qlv chunk tree",
                    currentChunkTree->GetType());
        }
    }
}

std::pair<TUnversionedOwningRow, TUnversionedOwningRow> GetBoundaryKeysOrThrow(const TChunk* chunk)
{
    auto optionalBoundaryKeysExt = chunk->ChunkMeta()->FindExtension<TBoundaryKeysExt>();
    if (!optionalBoundaryKeysExt) {
        THROW_ERROR_EXCEPTION("Cannot compute boundary keys in chunk %v since it's missing boundary info",
            chunk->GetId());
    }

    auto minKey = FromProto<TLegacyOwningKey>(optionalBoundaryKeysExt->min());
    auto maxKey = FromProto<TLegacyOwningKey>(optionalBoundaryKeysExt->max());

    return {std::move(minKey), std::move(maxKey)};
}

std::vector<TChunkViewMergeResult> MergeAdjacentChunkViewRanges(std::vector<TChunkView*> chunkViews)
{
    auto lowerLimitOrEmptyKey = [] (const NChunkServer::TChunkView* chunkView) {
        if (const auto& lowerLimit = chunkView->ReadRange().LowerLimit(); lowerLimit.HasLegacyKey()) {
            return lowerLimit.GetLegacyKey();
        }
        return EmptyKey();
    };

    auto upperLimitOrMaxKey = [] (const NChunkServer::TChunkView* chunkView) {
        if (const auto& upperLimit = chunkView->ReadRange().UpperLimit(); upperLimit.HasLegacyKey()) {
            return upperLimit.GetLegacyKey();
        }
        return MaxKey();
    };

    std::sort(chunkViews.begin(), chunkViews.end(), [&] (const auto* lhs, const auto* rhs) {
        if (int result = CompareButForReadRange(lhs, rhs)) {
            return result < 0;
        }
        return lowerLimitOrEmptyKey(lhs->AsChunkView()) < lowerLimitOrEmptyKey(rhs->AsChunkView());
    });

    std::vector<TChunkViewMergeResult> mergedChunkViews;

    auto beginSameChunkRange = chunkViews.begin();
    auto endSameChunkRange = chunkViews.begin();

    while (beginSameChunkRange != chunkViews.end()) {
        while (endSameChunkRange != chunkViews.end() &&
            CompareButForReadRange(*beginSameChunkRange, *endSameChunkRange) == 0)
        {
            ++endSameChunkRange;
        }

        auto lowerLimit = lowerLimitOrEmptyKey((*beginSameChunkRange)->AsChunkView());
        auto upperLimit = upperLimitOrMaxKey((*beginSameChunkRange)->AsChunkView());

        TChunkViewMergeResult result;
        result.FirstChunkView = result.LastChunkView = *beginSameChunkRange;

        for (auto it = beginSameChunkRange + 1; it != endSameChunkRange; ++it) {
            const auto* chunkTree = *it;
            YT_VERIFY(chunkTree->GetType() == EObjectType::ChunkView);
            const auto* chunkView = chunkTree->AsChunkView();
            auto nextLowerLimit = lowerLimitOrEmptyKey(chunkView);
            if (nextLowerLimit < upperLimit) {
                THROW_ERROR_EXCEPTION("Found intersecting chunk view ranges during merge")
                    << TErrorAttribute("previous_upper_limit", upperLimit)
                    << TErrorAttribute("lower_limit", lowerLimit)
                    << TErrorAttribute("chunk_view_id", chunkView->GetId());
            } else if (nextLowerLimit == upperLimit) {
                upperLimit = upperLimitOrMaxKey(chunkView);
            } else {
                mergedChunkViews.push_back(result);
                result.FirstChunkView = *it;
                lowerLimit = nextLowerLimit;
                upperLimit = upperLimitOrMaxKey(chunkView);
            }

            result.LastChunkView = *it;
        }

        mergedChunkViews.push_back(result);
        beginSameChunkRange = endSameChunkRange;
    }

    return mergedChunkViews;
}

std::vector<NJournalClient::TChunkReplicaDescriptor> GetChunkReplicaDescriptors(const TChunk* chunk)
{
    if (!chunk->IsJournal()) {
        YT_LOG_ALERT("Getting chunk replica descriptors for non-journal chunk");
    }

    std::vector<TChunkReplicaDescriptor> replicas;
    for (auto replica : chunk->StoredReplicas()) {
        replicas.push_back({
            GetChunkLocationNode(replica)->GetDescriptor(),
            replica.GetReplicaIndex(),
            replica.GetPtr()->GetEffectiveMediumIndex(),
        });
    }
    return replicas;
}

void SerializeMediumDirectory(
    NChunkClient::NProto::TMediumDirectory* protoMediumDirectory,
    const IChunkManagerPtr& chunkManager)
{
    for (auto [mediumId, medium] : chunkManager->Media()) {
        auto* protoItem = protoMediumDirectory->add_items();
        protoItem->set_index(medium->GetIndex());
        protoItem->set_name(medium->GetName());
        protoItem->set_priority(medium->GetPriority());
    }
}

void SerializeMediumOverrides(
    TNode* node,
    NDataNodeTrackerClient::NProto::TMediumOverrides* protoMediumOverrides)
{
    for (auto* location : node->RealChunkLocations()) {
        if (const auto& mediumOverride = location->MediumOverride()) {
            auto* protoMediumOverride = protoMediumOverrides->add_overrides();
            ToProto(protoMediumOverride->mutable_location_uuid(), location->GetUuid());
            protoMediumOverride->set_medium_index(mediumOverride->GetIndex());
        }
    }
}

int GetChunkShardIndex(TChunkId chunkId)
{
    return TDirectObjectIdHash()(chunkId) % ChunkShardCount;
}

std::vector<TInstant> GenerateChunkCreationTimeHistogramBucketBounds(TInstant now)
{
    std::vector<TInstant> bounds;
    bounds.reserve(5 + 11 + 3 + 1);
    for (int yearDelta = 5; yearDelta > 0; --yearDelta) {
        bounds.push_back(now - TDuration::Days(yearDelta * 365));
    }
    for (int monthDelta = 11; monthDelta > 0; --monthDelta) {
        bounds.push_back(now - TDuration::Days(monthDelta * 30));
    }
    for (int weekDelta = 3; weekDelta > 0; --weekDelta) {
        bounds.push_back(now - TDuration::Days(weekDelta * 7));
    }
    bounds.push_back(now);
    return bounds;
}

TJobPtr MummifyJob(const TJobPtr& job)
{
    YT_VERIFY(job);

    class TMummyJob
        : public TJob
    {
    public:
        TMummyJob(const TJob& other)
            : TJob(other)
        { }

        bool FillJobSpec(
            NCellMaster::TBootstrap* /*bootstrap*/,
            NProto::TJobSpec* /*jobSpec*/) const override
        {
            YT_UNIMPLEMENTED();
        };
    };

    return New<TMummyJob>(*job);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
