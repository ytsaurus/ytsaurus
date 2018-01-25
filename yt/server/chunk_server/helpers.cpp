#include "helpers.h"
#include "chunk_owner_base.h"
#include "chunk_manager.h"

#include <yt/server/cypress_server/cypress_manager.h>

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/hydra_facade.h>
#include <yt/server/cell_master/multicell_manager.h>

#include <yt/ytlib/object_client/helpers.h>
#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/ytlib/table_client/unversioned_row.h>

#include <yt/ytlib/chunk_client/chunk_service_proxy.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NChunkServer {

using namespace NYTree;
using namespace NYson;
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

static const double ChunkListThombstoneRelativeThreshold = 0.5;
static const double ChunkListThombstoneAbsoluteThreshold = 16;

////////////////////////////////////////////////////////////////////////////////

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

    // Go upwards and apply delta.
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
            YCHECK(child == children[childIndex]);
            children[childIndex] = nullptr;
        }
        int newTrimmedChildCount = chunkList->GetTrimmedChildCount() + static_cast<int>(childrenEnd - childrenBegin);
        if (newTrimmedChildCount > ChunkListThombstoneAbsoluteThreshold &&
            newTrimmedChildCount > children.size() * ChunkListThombstoneRelativeThreshold)
        {
            children.erase(
                children.begin(),
                children.begin() + newTrimmedChildCount);

            auto& cumulativeStatistics = chunkList->CumulativeStatistics();
            cumulativeStatistics.erase(
                cumulativeStatistics.begin(),
                std::min(cumulativeStatistics.begin() + newTrimmedChildCount, cumulativeStatistics.end()));

            chunkList->SetTrimmedChildCount(0);
        } else {
            chunkList->SetTrimmedChildCount(newTrimmedChildCount);
        }

        // NB: Do not change logical row and chunk count.
        statisticsDelta.LogicalRowCount = 0;
        statisticsDelta.LogicalChunkCount = 0;
    } else {
        // Can handle arbitrary children.
        // Used in sorted tablet compaction..
        auto& childToIndex = chunkList->ChildToIndex();
        for (auto childIt = childrenBegin; childIt != childrenEnd; ++childIt) {
            auto* child = *childIt;
            auto indexIt = childToIndex.find(child);
            YCHECK(indexIt != childToIndex.end());
            int index = indexIt->second;
            if (index != children.size() - 1) {
                children[index] = children.back();
                childToIndex[children[index]] = index;
            }
            childToIndex.erase(indexIt);
            children.pop_back();
        }
    }

    // Go upwards and recompute statistics.
    VisitUniqueAncestors(
        chunkList,
        [&] (TChunkList* current) {
            current->Statistics().Deaccumulate(statisticsDelta);
        });
}

void SetChunkTreeParent(TChunkList* parent, TChunkTree* child)
{
    switch (child->GetType()) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
        case EObjectType::JournalChunk:
            child->AsChunk()->AddParent(parent);
            break;
        case EObjectType::ChunkList:
            child->AsChunkList()->AddParent(parent);
            break;
        default:
            Y_UNREACHABLE();
    }
}

void ResetChunkTreeParent(TChunkList* parent, TChunkTree* child)
{
    switch (child->GetType()) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
        case EObjectType::JournalChunk:
            child->AsChunk()->RemoveParent(parent);
            break;
        case EObjectType::ChunkList:
            child->AsChunkList()->RemoveParent(parent);
            break;
        default:
            Y_UNREACHABLE();
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
            return chunkTree->AsChunk()->GetStatistics();
        case EObjectType::ChunkList:
            return chunkTree->AsChunkList()->Statistics();
        default:
            Y_UNREACHABLE();
    }
}

void AppendChunkTreeChild(
    TChunkList* chunkList,
    TChunkTree* child,
    TChunkTreeStatistics* statistics)
{
    if (chunkList->IsOrdered()) {
        if (!chunkList->Children().empty()) {
            chunkList->CumulativeStatistics().push_back({
                chunkList->Statistics().LogicalRowCount + statistics->LogicalRowCount,
                chunkList->Statistics().LogicalChunkCount + statistics->LogicalChunkCount,
                chunkList->Statistics().UncompressedDataSize + statistics->UncompressedDataSize
            });
        }
    } else if (child) {
        int index = static_cast<int>(chunkList->Children().size());
        YCHECK(chunkList->ChildToIndex().emplace(child, index).second);
    }
    statistics->Accumulate(GetChunkTreeStatistics(child));
    chunkList->Children().push_back(child);
}

void AccumulateUniqueAncestorsStatistics(
    TChunkList* chunkList,
    const TChunkTreeStatistics& statisticsDelta)
{
    auto mutableStatisticsDelta = statisticsDelta;
    VisitUniqueAncestors(
        chunkList,
        [&] (TChunkList* current) {
            ++mutableStatisticsDelta.Rank;
            current->Statistics().Accumulate(mutableStatisticsDelta);
        });
}

void ResetChunkListStatistics(TChunkList* chunkList)
{
    chunkList->CumulativeStatistics().clear();
    chunkList->Statistics() = TChunkTreeStatistics();
    chunkList->Statistics().ChunkListCount = 1;
    chunkList->Statistics().Rank = 1;
}

void RecomputeChunkListStatistics(TChunkList* chunkList)
{
    ResetChunkListStatistics(chunkList);

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
            case EObjectType::JournalChunk: {
                for (auto* parent : chunkTree->AsChunk()->Parents()) {
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
                Y_UNREACHABLE();
        }
    }

    return std::vector<TChunkOwnerBase*>(owningNodes.begin(), owningNodes.end());
}

namespace {

TYsonString DoGetMulticellOwningNodes(
    NCellMaster::TBootstrap* bootstrap,
    const TChunkTreeId& chunkTreeId)
{
    std::vector<TVersionedObjectId> nodeIds;

    const auto& chunkManager = bootstrap->GetChunkManager();
    auto* chunkTree = chunkManager->FindChunkTree(chunkTreeId);
    if (IsObjectAlive(chunkTree)) {
        auto nodes = GetOwningNodes(chunkTree);
        for (const auto* node : nodes) {
            nodeIds.push_back(node->GetVersionedId());
        }
    }

    const auto& multicellManager = bootstrap->GetMulticellManager();

    // Request owning nodes from all cells.
    auto requestIdsFromCell = [&] (TCellTag cellTag) {
        if (cellTag == bootstrap->GetCellTag())
            return;

        auto type = TypeFromId(chunkTreeId);
        if (type != EObjectType::Chunk &&
            type != EObjectType::ErasureChunk &&
            type != EObjectType::JournalChunk)
            return;

        auto channel = multicellManager->GetMasterChannelOrThrow(
            cellTag,
            NHydra::EPeerKind::LeaderOrFollower);
        TChunkServiceProxy proxy(channel);

        auto req = proxy.GetChunkOwningNodes();
        ToProto(req->mutable_chunk_id(), chunkTreeId);

        auto rspOrError = WaitFor(req->Invoke());
        if (rspOrError.GetCode() == NChunkClient::EErrorCode::NoSuchChunk)
            return;

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

    requestIdsFromCell(bootstrap->GetPrimaryCellTag());
    for (auto cellTag : bootstrap->GetSecondaryCellTags()) {
        requestIdsFromCell(cellTag);
    }

    // Request node paths from the primary cell.
    {
        auto channel = multicellManager->GetMasterChannelOrThrow(
            bootstrap->GetPrimaryCellTag(),
            NHydra::EPeerKind::LeaderOrFollower);
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
        YCHECK(rsps.size() == nodeIds.size());

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
        .AsyncVia(bootstrap->GetHydraFacade()->GetEpochAutomatonInvoker())
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
            return false;

        case EObjectType::ChunkList:
            return IsEmpty(chunkTree->AsChunkList());

        default:
            Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

TOwningKey GetMaxKey(const TChunk* chunk)
{
    TOwningKey key;
    auto boundaryKeysExt = GetProtoExtension<TBoundaryKeysExt>(
        chunk->ChunkMeta().extensions());
    FromProto(&key, boundaryKeysExt.max());

    return GetKeySuccessor(key);
}

TOwningKey GetMaxKey(const TChunkTree* chunkTree)
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
        Y_UNREACHABLE();
    };

    const auto* currentChunkTree = chunkTree;
    while (true) {
        switch (currentChunkTree->GetType()) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk:
                return GetMaxKey(currentChunkTree->AsChunk());

            case EObjectType::ChunkList:
                currentChunkTree = getLastNonemptyChild(currentChunkTree->AsChunkList());
                break;

            default:
                Y_UNREACHABLE();
        }
    }
}

TOwningKey GetMinKey(const TChunk* chunk)
{
    TOwningKey key;
    auto boundaryKeysExt = GetProtoExtension<TBoundaryKeysExt>(
        chunk->ChunkMeta().extensions());
    FromProto(&key, boundaryKeysExt.min());

    return key;
}

TOwningKey GetMinKey(const TChunkTree* chunkTree)
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
        Y_UNREACHABLE();
    };

    const auto* currentChunkTree = chunkTree;
    while (true) {
        switch (currentChunkTree->GetType()) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk:
                return GetMinKey(currentChunkTree->AsChunk());

            case EObjectType::ChunkList:
                currentChunkTree = getFirstNonemptyChild(currentChunkTree->AsChunkList());
                break;

            default:
                Y_UNREACHABLE();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
