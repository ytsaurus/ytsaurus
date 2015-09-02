#include "stdafx.h"
#include "helpers.h"
#include "chunk_owner_base.h"
#include "chunk_manager.h"

#include <core/ytree/fluent.h>

#include <core/concurrency/scheduler.h>

#include <core/misc/protobuf_helpers.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/table_client/unversioned_row.h>
#include <ytlib/table_client/chunk_meta_extensions.h>

#include <ytlib/hive/cell_directory.h>

#include <ytlib/chunk_client/chunk_service_proxy.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>
#include <ytlib/cypress_client/rpc_helpers.h>

#include <server/cypress_server/cypress_manager.h>
#include <server/cypress_server/node_proxy.h>

#include <server/transaction_server/transaction.h>

#include <server/object_server/object.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/hydra_facade.h>
#include <server/cell_master/config.h>

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

void SetChunkTreeParent(TChunkList* parent, TChunkTree* child)
{
    switch (child->GetType()) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
        case EObjectType::JournalChunk:
            child->AsChunk()->Parents().push_back(parent);
            break;
        case EObjectType::ChunkList:
            child->AsChunkList()->Parents().insert(parent);
            break;
        default:
            YUNREACHABLE();
    }
}

void ResetChunkTreeParent(TChunkList* parent, TChunkTree* child)
{
    switch (child->GetType()) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
        case EObjectType::JournalChunk: {
            auto& parents = child->AsChunk()->Parents();
            auto it = std::find(parents.begin(), parents.end(), parent);
            YASSERT(it != parents.end());
            parents.erase(it);
            break;
        }
        case EObjectType::ChunkList: {
            auto& parents = child->AsChunkList()->Parents();
            auto it = parents.find(parent);
            YASSERT(it != parents.end());
            parents.erase(it);
            break;
        }
        default:
            YUNREACHABLE();
    }
}

TChunkTreeStatistics GetChunkTreeStatistics(TChunkTree* chunkTree)
{
    switch (chunkTree->GetType()) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
        case EObjectType::JournalChunk:
            return chunkTree->AsChunk()->GetStatistics();
        case EObjectType::ChunkList:
            return chunkTree->AsChunkList()->Statistics();
        default:
            YUNREACHABLE();
    }
}

void AccumulateChildStatistics(
    TChunkList* chunkList,
    TChunkTree* child,
    TChunkTreeStatistics* statistics)
{
    if (!chunkList->Children().empty()) {
        chunkList->RowCountSums().push_back(
            chunkList->Statistics().RowCount +
            statistics->RowCount);
        chunkList->ChunkCountSums().push_back(
            chunkList->Statistics().ChunkCount +
            statistics->ChunkCount);
        chunkList->DataSizeSums().push_back(
            chunkList->Statistics().UncompressedDataSize +
            statistics->UncompressedDataSize);

    }
    statistics->Accumulate(GetChunkTreeStatistics(child));
}

void ResetChunkListStatistics(TChunkList* chunkList)
{
    chunkList->RowCountSums().clear();
    chunkList->ChunkCountSums().clear();
    chunkList->DataSizeSums().clear();
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
        AccumulateChildStatistics(chunkList, child, &statistics);
        chunkList->Children().push_back(child);
    }

    ++statistics.Rank;
    chunkList->Statistics() = statistics;
}

TClusterResources GetDiskUsage(
    const NChunkClient::NProto::TDataStatistics& statistics,
    int replicationFactor)
{
    TClusterResources result;
    result.DiskSpace =
        statistics.regular_disk_space() * replicationFactor +
        statistics.erasure_disk_space();
    result.ChunkCount = statistics.chunk_count();
    return result;
}

void VisitOwningNodes(
    TChunkTree* chunkTree,
    yhash_set<TChunkTree*>* visitedTrees,
    yhash_set<TChunkOwnerBase*>* owningNodes)
{
    if (!visitedTrees->insert(chunkTree).second)
        return;
    
    switch (chunkTree->GetType()) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
        case EObjectType::JournalChunk: {
            for (auto* parent : chunkTree->AsChunk()->Parents()) {
                VisitOwningNodes(parent, visitedTrees, owningNodes);
            }
            break;
        }
        case EObjectType::ChunkList: {
            auto* chunkList = chunkTree->AsChunkList();
            owningNodes->insert(chunkList->OwningNodes().begin(), chunkList->OwningNodes().end());
            for (auto* parent : chunkList->Parents()) {
                VisitOwningNodes(parent, visitedTrees, owningNodes);
            }
            break;
        }
        default:
            YUNREACHABLE();
    }
}

std::vector<TChunkOwnerBase*> GetOwningNodes(TChunkTree* chunkTree)
{
    yhash_set<TChunkOwnerBase*> owningNodes;
    yhash_set<TChunkTree*> visitedTrees;
    VisitOwningNodes(chunkTree, &visitedTrees, &owningNodes);
    return std::vector<TChunkOwnerBase*>(owningNodes.begin(), owningNodes.end());
}

namespace {

TYsonString DoGetMulticellOwningNodes(
    NCellMaster::TBootstrap* bootstrap,
    const TChunkTreeId& chunkTreeId)
{
    std::vector<TVersionedObjectId> nodeIds;

    auto chunkManager = bootstrap->GetChunkManager();
    auto* chunkTree = chunkManager->FindChunkTree(chunkTreeId);
    if (IsObjectAlive(chunkTree)) {
        auto nodes = GetOwningNodes(chunkTree);
        for (const auto* node : nodes) {
            nodeIds.push_back(node->GetVersionedId());
        }
    }

    auto cellDirectory = bootstrap->GetCellDirectory();

    // Request owning nodes from all cells.
    auto requestIdsFromCell = [&] (TCellTag cellTag) {
        if (cellTag == bootstrap->GetCellTag())
            return;

        auto type = TypeFromId(chunkTreeId);
        if (type != EObjectType::Chunk &&
            type != EObjectType::ErasureChunk &&
            type != EObjectType::JournalChunk)
            return;

        auto cellId = bootstrap->GetSecondaryCellId(cellTag);
        auto channel = cellDirectory->GetChannel(cellId, NHydra::EPeerKind::LeaderOrFollower);

        TChunkServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(bootstrap->GetConfig()->ObjectManager->ForwardingRpcTimeout);

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
                protoNode.has_transaction_id() ? FromProto<TTransactionId>(protoNode.transaction_id()) : NullTransactionId);
        }
    };

    requestIdsFromCell(bootstrap->GetPrimaryCellTag());
    for (auto cellTag : bootstrap->GetSecondaryCellTags()) {
        requestIdsFromCell(cellTag);
    }

    // Request node paths from the primary cell.
    {
        auto cellId = bootstrap->GetPrimaryCellId();
        auto channel = cellDirectory->GetChannel(cellId, NHydra::EPeerKind::LeaderOrFollower);

        // TODO(babenko): improve
        TObjectServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(bootstrap->GetConfig()->ObjectManager->ForwardingRpcTimeout);

        auto batchReq = proxy.ExecuteBatch();
        for (const auto& versionedId : nodeIds) {
            auto req = TCypressYPathProxy::Get(FromObjectId(versionedId.ObjectId) + "/@path");
            SetTransactionId(req, versionedId.TransactionId);
            batchReq->AddRequest(req, "get_path");
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error requesting owning node paths");
        const auto& batchRsp = batchRspOrError.Value();

        auto rsps = batchRsp->GetResponses<TCypressYPathProxy::TRspGet>("get_path");
        YCHECK(rsps.size() == nodeIds.size());

        TStringStream stream;
        TYsonWriter writer(&stream);
        writer.OnBeginList();

        for (int index = 0; index < rsps.size(); ++index) {
            const auto& rsp = rsps[index].Value();
            const auto& versionedId = nodeIds[index];
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

TOwningKey GetMaxKey(const TChunk* chunk)
{
    TOwningKey key;
    auto chunkFormat = ETableChunkFormat(chunk->ChunkMeta().version());
    if (chunkFormat == ETableChunkFormat::Old) {
        // Deprecated chunks.
        auto boundaryKeysExt = GetProtoExtension<TOldBoundaryKeysExt>(
            chunk->ChunkMeta().extensions());
        FromProto(&key, boundaryKeysExt.end());
    } else {
        auto boundaryKeysExt = GetProtoExtension<TBoundaryKeysExt>(
            chunk->ChunkMeta().extensions());
        FromProto(&key, boundaryKeysExt.max());
    }

    return GetKeySuccessor(key.Get());
}

TOwningKey GetMaxKey(const TChunkList* chunkList)
{
    const auto& children = chunkList->Children();
    YASSERT(!children.empty());
    return GetMaxKey(children.back());
}

TOwningKey GetMaxKey(const TChunkTree* chunkTree)
{
    switch (chunkTree->GetType()) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
            return GetMaxKey(chunkTree->AsChunk());

        case EObjectType::ChunkList:
            return GetMaxKey(chunkTree->AsChunkList());

        default:
            YUNREACHABLE();
    }
}

TOwningKey GetMinKey(const TChunk* chunk)
{
    TOwningKey key;
    auto chunkFormat = ETableChunkFormat(chunk->ChunkMeta().version());
    if (chunkFormat == ETableChunkFormat::Old) {
        // Deprecated chunks.
        auto boundaryKeysExt = GetProtoExtension<TOldBoundaryKeysExt>(
            chunk->ChunkMeta().extensions());
        FromProto(&key, boundaryKeysExt.start());
    } else {
        auto boundaryKeysExt = GetProtoExtension<TBoundaryKeysExt>(
            chunk->ChunkMeta().extensions());
        FromProto(&key, boundaryKeysExt.min());
    }

    return key;
}

TOwningKey GetMinKey(const TChunkList* chunkList)
{
    const auto& children = chunkList->Children();
    YASSERT(!children.empty());
    return GetMinKey(children.front());
}

TOwningKey GetMinKey(const TChunkTree* chunkTree)
{
    switch (chunkTree->GetType()) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
            return GetMinKey(chunkTree->AsChunk());

        case EObjectType::ChunkList:
            return GetMinKey(chunkTree->AsChunkList());

        default:
            YUNREACHABLE();
    }
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
