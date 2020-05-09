#include "helpers.h"
#include "private.h"
#include "chunk_owner_base.h"
#include "chunk_manager.h"
#include "chunk_view.h"
#include "chunk.h"
#include "dynamic_store.h"

#include <yt/server/master/cypress_server/cypress_manager.h>

#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/hydra_facade.h>
#include <yt/server/master/cell_master/multicell_manager.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/ytlib/chunk_client/chunk_service_proxy.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/journal_client/helpers.h>

#include <yt/client/table_client/unversioned_row.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NChunkServer {

using namespace NYTree;
using namespace NYson;
using namespace NJournalClient;
using namespace NObjectClient;
using namespace NCypressServer;
using namespace NCypressClient;
using namespace NChunkClient;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NSecurityServer;
using namespace NObjectServer;
using namespace NConcurrency;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static const double ChunkListTombstoneRelativeThreshold = 0.5;
static const double ChunkListTombstoneAbsoluteThreshold = 16;

////////////////////////////////////////////////////////////////////////////////

namespace {

int GetChildIndex(TChunkList* parentChunkList, TChunkTree* child)
{
    return GetOrCrash(parentChunkList->ChildToIndex(), child);
}

} // namespace

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
            return parents[0];
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
    TChunkTree* const* childrenBegin,
    TChunkTree* const* childrenEnd)
{
    // A shortcut.
    if (childrenBegin == childrenEnd) {
        return;
    }

    // NB: Accumulate statistics from left to right to get Sealed flag correct.
    TChunkTreeStatistics statisticsDelta;
    for (auto it = childrenBegin; it != childrenEnd; ++it) {
        chunkList->ValidateSealed();
        auto* child = *it;
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
    TChunkTree* const* childrenBegin,
    TChunkTree* const* childrenEnd)
{
    // A shortcut.
    if (childrenBegin == childrenEnd) {
        return;
    }

    chunkList->IncrementVersion();

    TChunkTreeStatistics statisticsDelta;
    for (auto childIt = childrenBegin; childIt != childrenEnd; ++childIt) {
        auto* child = *childIt;
        statisticsDelta.Accumulate(GetChunkTreeStatistics(child));
        ResetChunkTreeParent(chunkList, child);
    }

    auto& children = chunkList->Children();
    if (chunkList->IsOrdered()) {
        // Can only handle a prefix of non-trimmed children.
        // Used in ordered tablet trim.
        int childIndex = chunkList->GetTrimmedChildCount();
        for (auto childIt = childrenBegin; childIt != childrenEnd; ++childIt, ++childIndex) {
            auto* child = *childIt;
            YT_VERIFY(child == children[childIndex]);
            children[childIndex] = nullptr;
        }
        int newTrimmedChildCount = chunkList->GetTrimmedChildCount() + static_cast<int>(childrenEnd - childrenBegin);
        if (newTrimmedChildCount > ChunkListTombstoneAbsoluteThreshold &&
            newTrimmedChildCount > children.size() * ChunkListTombstoneRelativeThreshold)
        {
            children.erase(
                children.begin(),
                children.begin() + newTrimmedChildCount);

            chunkList->CumulativeStatistics().TrimFront(newTrimmedChildCount);

            chunkList->SetTrimmedChildCount(0);
        } else {
            chunkList->SetTrimmedChildCount(newTrimmedChildCount);
        }

        // NB: Do not change logical row and chunk count.
        statisticsDelta.LogicalRowCount = 0;
        statisticsDelta.LogicalChunkCount = 0;
    } else {
        // Can handle arbitrary children.
        // Used in sorted tablet compaction.
        auto& childToIndex = chunkList->ChildToIndex();
        for (auto childIt = childrenBegin; childIt != childrenEnd; ++childIt) {
            auto* child = *childIt;
            auto indexIt = childToIndex.find(child);
            YT_VERIFY(indexIt != childToIndex.end());
            int index = indexIt->second;

            // To remove child from the middle we swap it with the last one and update
            // cumulative statistics accordingly.
            if (index != children.size() - 1) {
                auto delta = TCumulativeStatisticsEntry(GetChunkTreeStatistics(children.back())) -
                    TCumulativeStatisticsEntry(GetChunkTreeStatistics(children[index]));
                chunkList->CumulativeStatistics().Update(index, delta);

                children[index] = children.back();
                childToIndex[children[index]] = index;
            }

            chunkList->CumulativeStatistics().PopBack();
            childToIndex.erase(indexIt);
            children.pop_back();
        }
    }

    // Go upwards and recompute statistics.
    VisitUniqueAncestors(
        chunkList,
        [&] (TChunkList* current, TChunkTree* child) {
            current->Statistics().Deaccumulate(statisticsDelta);
            if (child && current->HasModifyableCumulativeStatistics()) {
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
    auto& childToIndex = chunkList->ChildToIndex();

    auto* oldChild = children[childIndex];
    ResetChunkTreeParent(chunkList, oldChild);
    SetChunkTreeParent(chunkList, newChild);

    if (!chunkList->IsOrdered()) {
        auto oldChildIt = childToIndex.find(oldChild);
        YT_VERIFY(oldChildIt != childToIndex.end());
        childToIndex.erase(oldChildIt);
        YT_VERIFY(childToIndex.emplace(newChild, childIndex).second);
    }

    children[childIndex] = newChild;
}

void SetChunkTreeParent(TChunkList* parent, TChunkTree* child)
{
    switch (child->GetType()) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
        case EObjectType::JournalChunk:
        case EObjectType::ErasureJournalChunk:
            child->AsChunk()->AddParent(parent);
            break;
        case EObjectType::ChunkView:
            child->AsChunkView()->AddParent(parent);
            break;
        case EObjectType::SortedDynamicTabletStore:
        case EObjectType::OrderedDynamicTabletStore:
            child->AsDynamicStore()->AddParent(parent);
            break;
        case EObjectType::ChunkList:
            child->AsChunkList()->AddParent(parent);
            break;
        default:
            YT_ABORT();
    }
}

void ResetChunkTreeParent(TChunkList* parent, TChunkTree* child)
{
    switch (child->GetType()) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
        case EObjectType::JournalChunk:
        case EObjectType::ErasureJournalChunk:
            child->AsChunk()->RemoveParent(parent);
            break;
        case EObjectType::ChunkView:
            child->AsChunkView()->RemoveParent(parent);
            break;
        case EObjectType::SortedDynamicTabletStore:
        case EObjectType::OrderedDynamicTabletStore:
            child->AsDynamicStore()->RemoveParent(parent);
            break;
        case EObjectType::ChunkList:
            child->AsChunkList()->RemoveParent(parent);
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

    if (child && !chunkList->IsOrdered()) {
        int index = static_cast<int>(chunkList->Children().size());
        YT_VERIFY(chunkList->ChildToIndex().emplace(child, index).second);
    }

    statistics->Accumulate(GetChunkTreeStatistics(child));
    chunkList->Children().push_back(child);
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
    VisitUniqueAncestors(
        parent,
        [&] (TChunkList* parent, TChunkTree* child) {
            ++mutableStatisticsDelta.Rank;
            parent->Statistics().Accumulate(mutableStatisticsDelta);

            if (parent->HasCumulativeStatistics()) {
                auto& cumulativeStatistics = parent->CumulativeStatistics();
                TCumulativeStatisticsEntry entry{mutableStatisticsDelta};

                int index = parent->IsOrdered()
                    ? parent->Children().size() - 1
                    : GetChildIndex(parent, child);
                YT_VERIFY(parent->Children()[index] == child);
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

    // TODO(ifsmirnov): looks like this function is called only for empty
    // chunk lists. Check it and add YT_VERIFY in TChunkList::SetKind
    // that it is called only for empty chunk lists.
    YT_VERIFY(chunkList->Children().empty());

    if (chunkList->HasAppendableCumulativeStatistics()) {
        chunkList->CumulativeStatistics().DeclareAppendable();
    } else if (chunkList->HasModifyableCumulativeStatistics()) {
        chunkList->CumulativeStatistics().DeclareModifiable();
    } else if (chunkList->HasTrimmableCumulativeStatistics()) {
        chunkList->CumulativeStatistics().DeclareTrimmable();
    }

    std::vector<TChunkTree*> children;
    children.swap(chunkList->Children());

    TChunkTreeStatistics statistics;
    for (auto* child : children) {
        AppendChunkTreeChild(chunkList, child, &statistics);
    }

    ++statistics.Rank;
    ++statistics.ChunkListCount;
    chunkList->Statistics() = statistics;
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

    for (int index = 0; index < queue.size(); ++index) {
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

    // Request owning nodes from all cells.
    auto requestIdsFromCell = [&] (TCellTag cellTag) {
        if (cellTag == multicellManager->GetCellTag()) {
            return;
        }

        auto type = TypeFromId(chunkTreeId);
        if (type != EObjectType::Chunk &&
            type != EObjectType::ErasureChunk &&
            type != EObjectType::JournalChunk &&
            type != EObjectType::ErasureJournalChunk)
        {
            return;
        }

        auto channel = multicellManager->GetMasterChannelOrThrow(
            cellTag,
            NHydra::EPeerKind::Follower);
        TChunkServiceProxy proxy(channel);

        auto req = proxy.GetChunkOwningNodes();
        ToProto(req->mutable_chunk_id(), chunkTreeId);

        auto rspOrError = WaitFor(req->Invoke());
        if (rspOrError.GetCode() == NChunkClient::EErrorCode::NoSuchChunk) {
            return;
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
    };

    requestIdsFromCell(multicellManager->GetPrimaryCellTag());
    for (auto cellTag : multicellManager->GetSecondaryCellTags()) {
        requestIdsFromCell(cellTag);
    }

    // Request node paths from the primary cell.
    {
        auto channel = multicellManager->GetMasterChannelOrThrow(
            multicellManager->GetPrimaryCellTag(),
            NHydra::EPeerKind::Follower);
        TObjectServiceProxy proxy(channel);

        // TODO(babenko): improve
        auto batchReq = proxy.ExecuteBatch();
        for (const auto& versionedId : nodeIds) {
            auto req = TCypressYPathProxy::Get(FromObjectId(versionedId.ObjectId) + "/@path");
            SetTransactionId(req, versionedId.TransactionId);
            batchReq->AddRequest(req, "get_path");
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(batchRspOrError, "Error requesting owning nodes paths");
        const auto& batchRsp = batchRspOrError.Value();

        auto rsps = batchRsp->GetResponses<TCypressYPathProxy::TRspGet>("get_path");
        YT_VERIFY(rsps.size() == nodeIds.size());

        TStringStream stream;
        TBufferedBinaryYsonWriter writer(&stream);
        writer.OnBeginList();

        for (int index = 0; index < rsps.size(); ++index) {
            const auto& rspOrError = rsps[index];
            const auto& versionedId = nodeIds[index];
            auto code = rspOrError.GetCode();
            if (code == NYTree::EErrorCode::ResolveError || code == NTransactionClient::EErrorCode::NoSuchTransaction) {
                continue;
            }

            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error requesting path for node %v",
                versionedId);
            const auto& rsp = rspOrError.Value();

            writer.OnListItem();
            if (versionedId.TransactionId) {
                writer.OnBeginAttributes();
                writer.OnKeyedItem("transaction_id");
                writer.OnStringScalar(ToString(versionedId.TransactionId));
                writer.OnEndAttributes();
            }
            writer.OnRaw(rsp->value(), EYsonType::Node);
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

bool IsEmpty(const TChunkList* chunkList)
{
    return !chunkList || chunkList->Statistics().LogicalChunkCount == 0;
}

bool IsEmpty(const TChunkTree* chunkTree)
{
    if (!chunkTree) {
        return true;
    }
    switch (chunkTree->GetType()) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
        case EObjectType::JournalChunk:
        case EObjectType::ErasureJournalChunk:
        case EObjectType::ChunkView:
        case EObjectType::SortedDynamicTabletStore:
        case EObjectType::OrderedDynamicTabletStore:
            return false;

        case EObjectType::ChunkList:
            return IsEmpty(chunkTree->AsChunkList());

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

TOwningKey GetUpperBoundKeyOrThrow(const TChunk* chunk, std::optional<int> keyColumnCount)
{
    auto optionalBoundaryKeysExt = FindProtoExtension<TBoundaryKeysExt>(
        chunk->ChunkMeta().extensions());
    if (!optionalBoundaryKeysExt) {
        THROW_ERROR_EXCEPTION("Cannot compute max key in chunk %v since it's missing boundary info",
            chunk->GetId());
    }

    auto key = FromProto<TOwningKey>(optionalBoundaryKeysExt->max());
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

TOwningKey GetUpperBoundKeyOrThrow(const TChunkView* chunkView, std::optional<int> keyColumnCount)
{
    auto chunkUpperBound = GetUpperBoundKeyOrThrow(chunkView->GetUnderlyingChunk(), keyColumnCount);
    const auto& upperLimit = chunkView->ReadRange().UpperLimit();
    if (!upperLimit.HasKey()) {
        return chunkUpperBound;
    }

    const auto& upperLimitKey = upperLimit.GetKey();
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

TOwningKey GetUpperBoundKeyOrThrow(const TChunkTree* chunkTree, std::optional<int> keyColumnCount)
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

TOwningKey GetMinKeyOrThrow(const TChunk* chunk, std::optional<int> keyColumnCount)
{
    auto optionalBoundaryKeysExt = FindProtoExtension<TBoundaryKeysExt>(
        chunk->ChunkMeta().extensions());
    if (!optionalBoundaryKeysExt) {
        THROW_ERROR_EXCEPTION("Cannot compute min key in chunk %v since it's missing boundary info",
            chunk->GetId());
    }

    auto minKey = FromProto<TOwningKey>(optionalBoundaryKeysExt->min());
    if (!keyColumnCount || *keyColumnCount == minKey.GetCount()) {
        return minKey;
    } else if (*keyColumnCount < minKey.GetCount()) {
        return GetKeyPrefix(minKey, *keyColumnCount);
    } else {
        return WidenKey(minKey, *keyColumnCount);
    }
}

TOwningKey GetMinKey(const TChunkView* chunkView, std::optional<int> keyColumnCount)
{
    auto chunkMinKey = GetMinKeyOrThrow(chunkView->GetUnderlyingChunk(), keyColumnCount);
    const auto& lowerLimit = chunkView->ReadRange().LowerLimit();
    if (!lowerLimit.HasKey()) {
        return chunkMinKey;
    }

    const auto& lowerLimitKey = lowerLimit.GetKey();
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

TOwningKey GetMinKeyOrThrow(const TChunkTree* chunkTree, std::optional<int> keyColumnCount)
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

TOwningKey GetMaxKeyOrThrow(const TChunk* chunk)
{
    auto optionalBoundaryKeysExt = FindProtoExtension<TBoundaryKeysExt>(
        chunk->ChunkMeta().extensions());
    if (!optionalBoundaryKeysExt) {
        THROW_ERROR_EXCEPTION("Cannot compute max key in chunk %v since it is missing boundary info",
            chunk->GetId());
    }

    return FromProto<TOwningKey>(optionalBoundaryKeysExt->max());
}

TOwningKey GetMaxKeyOrThrow(const TChunkTree* chunkTree)
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

std::vector<TChunkViewMergeResult> MergeAdjacentChunkViewRanges(std::vector<TChunkView*> chunkViews)
{
    auto lowerLimitOrEmptyKey = [] (const NChunkServer::TChunkView* chunkView) {
        if (const auto& lowerLimit = chunkView->ReadRange().LowerLimit(); lowerLimit.HasKey()) {
            return lowerLimit.GetKey();
        }
        return EmptyKey();
    };

    auto upperLimitOrMaxKey = [] (const NChunkServer::TChunkView* chunkView) {
        if (const auto& upperLimit = chunkView->ReadRange().UpperLimit(); upperLimit.HasKey()) {
            return upperLimit.GetKey();
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
    std::vector<TChunkReplicaDescriptor> replicas;
    for (auto replica : chunk->StoredReplicas()) {
        replicas.push_back({
            replica.GetPtr()->GetDescriptor(),
            replica.GetReplicaIndex(),
            replica.GetMediumIndex()
        });
    }
    return replicas;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
