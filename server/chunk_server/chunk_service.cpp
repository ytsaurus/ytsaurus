#include "chunk_service.h"
#include "private.h"
#include "chunk.h"
#include "chunk_manager.h"
#include "helpers.h"
#include "chunk_owner_base.h"
#include "medium.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/hydra_facade.h>
#include <yt/server/cell_master/master_hydra_service.h>
#include <yt/server/cell_master/multicell_manager.h>

#include <yt/server/node_tracker_server/node.h>
#include <yt/server/node_tracker_server/node_directory_builder.h>
#include <yt/server/node_tracker_server/node_tracker.h>

#include <yt/server/transaction_server/transaction.h>

#include <yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/ytlib/chunk_client/session_id.h>

#include <yt/ytlib/hive/cell_directory.h>

#include <yt/core/erasure/codec.h>

namespace NYT {
namespace NChunkServer {

using namespace NHydra;
using namespace NChunkClient;
using namespace NChunkServer;
using namespace NNodeTrackerServer;
using namespace NObjectServer;
using namespace NCellMaster;
using namespace NHydra;
using namespace NTransactionClient;
using namespace NRpc;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TChunkService
    : public NCellMaster::TMasterHydraServiceBase
{
public:
    explicit TChunkService(TBootstrap* bootstrap)
        : TMasterHydraServiceBase(
            bootstrap,
            TChunkServiceProxy::GetDescriptor(),
            EAutomatonThreadQueue::ChunkService,
            ChunkServerLogger)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(LocateChunks)
            .SetInvoker(GetGuardedAutomatonInvoker(EAutomatonThreadQueue::ChunkLocator))
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(TouchChunks)
            .SetInvoker(GetGuardedAutomatonInvoker(EAutomatonThreadQueue::ChunkLocator))
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AllocateWriteTargets)
            .SetInvoker(GetGuardedAutomatonInvoker(EAutomatonThreadQueue::ChunkReplicaAllocator)));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ExportChunks)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ImportChunks)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkOwningNodes));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ExecuteBatch)
            .SetHeavy(true)
            .SetMaxQueueSize(10000)
            .SetMaxConcurrency(10000));
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, LocateChunks)
    {
        context->SetRequestInfo("SubrequestCount: %v",
            request->subrequests_size());

        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::LeaderOrFollower);
        SyncWithUpstream();

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        TNodeDirectoryBuilder nodeDirectoryBuilder(response->mutable_node_directory());
        const auto& cellDirectory = Bootstrap_->GetCellDirectory();
        auto leaderChannel = cellDirectory->GetChannel(Bootstrap_->GetCellId(), EPeerKind::Leader);

        TChunkServiceProxy leaderProxy(std::move(leaderChannel));
        auto leaderRequest = leaderProxy.TouchChunks();
        leaderRequest->SetTimeout(context->GetTimeout());
        leaderRequest->SetUser(context->GetUser());

        const auto& hydraFacade = Bootstrap_->GetHydraFacade();
        bool follower = hydraFacade->GetHydraManager()->IsFollower();

        for (const auto& protoChunkId : request->subrequests()) {
            auto chunkId = FromProto<TChunkId>(protoChunkId);
            auto chunkIdWithIndex = DecodeChunkId(chunkId);

            auto* subresponse = response->add_subresponses();

            auto* chunk = chunkManager->FindChunk(chunkIdWithIndex.Id);
            if (!IsObjectAlive(chunk)) {
                subresponse->set_missing(true);
                continue;
            }

            TChunkPtrWithIndexes chunkWithIndexes(
                chunk,
                chunkIdWithIndex.ReplicaIndex,
                AllMediaIndex);
            auto replicas = chunkManager->LocateChunk(chunkWithIndexes);

            ToProto(subresponse->mutable_replicas(), replicas);
            subresponse->set_erasure_codec(static_cast<int>(chunk->GetErasureCodec()));
            for (auto replica : replicas) {
                nodeDirectoryBuilder.Add(replica);
            }

            if (follower && chunk->IsErasure() && !chunk->IsAvailable()) {
                ToProto(leaderRequest->add_subrequests(), chunkId);
            }
        }

        if (leaderRequest->subrequests_size() > 0) {
            LOG_DEBUG("Touching chunks at leader (Count: %v)",
                leaderRequest->subrequests_size());
            leaderRequest->Invoke();
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, TouchChunks)
    {
        context->SetRequestInfo("SubrequestCount: %v",
            request->subrequests_size());

        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::Leader);

        const auto& chunkManager = Bootstrap_->GetChunkManager();

        for (const auto& protoChunkId : request->subrequests()) {
            auto chunkId = FromProto<TChunkId>(protoChunkId);
            auto chunkIdWithIndex = DecodeChunkId(chunkId);
            auto* chunk = chunkManager->FindChunk(chunkIdWithIndex.Id);
            if (IsObjectAlive(chunk)) {
                chunkManager->TouchChunk(chunk);
            }
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, AllocateWriteTargets)
    {
        context->SetRequestInfo("SubrequestCount: %v",
            request->subrequests_size());

        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::Leader);

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();

        TNodeDirectoryBuilder builder(response->mutable_node_directory());

        for (const auto& subrequest : request->subrequests()) {
            auto sessionId = FromProto<TSessionId>(subrequest.session_id());
            int desiredTargetCount = subrequest.desired_target_count();
            int minTargetCount = subrequest.min_target_count();
            auto replicationFactorOverride = subrequest.has_replication_factor_override()
                ? MakeNullable(subrequest.replication_factor_override())
                : Null;
            auto preferredHostName = subrequest.has_preferred_host_name()
                ? MakeNullable(subrequest.preferred_host_name())
                : Null;
            const auto& forbiddenAddresses = subrequest.forbidden_addresses();

            auto* subresponse = response->add_subresponses();
            try {
                auto* medium = chunkManager->GetMediumByIndexOrThrow(sessionId.MediumIndex);
                auto* chunk = chunkManager->GetChunkOrThrow(sessionId.ChunkId);

                TNodeList forbiddenNodes;
                for (const auto& address : forbiddenAddresses) {
                    auto* node = nodeTracker->FindNodeByAddress(address);
                    if (node) {
                        forbiddenNodes.push_back(node);
                    }
                }
                std::sort(forbiddenNodes.begin(), forbiddenNodes.end());

                auto targets = chunkManager->AllocateWriteTargets(
                    medium,
                    chunk,
                    desiredTargetCount,
                    minTargetCount,
                    replicationFactorOverride,
                    &forbiddenNodes,
                    preferredHostName);

                for (int index = 0; index < static_cast<int>(targets.size()); ++index) {
                    auto* target = targets[index];
                    auto replica = TNodePtrWithIndexes(target, GenericChunkReplicaIndex, sessionId.MediumIndex);
                    builder.Add(replica);
                    subresponse->add_replicas(ToProto<ui32>(replica));
                }

                LOG_DEBUG("Write targets allocated "
                    "(ChunkId: %v, DesiredTargetCount: %v, MinTargetCount: %v, ReplicationFactorOverride: %v, "
                    "PreferredHostName: %v, ForbiddenAddresses: %v, Targets: %v)",
                    sessionId,
                    desiredTargetCount,
                    minTargetCount,
                    replicationFactorOverride,
                    preferredHostName,
                    forbiddenAddresses,
                    MakeFormattableRange(targets, TNodePtrAddressFormatter()));
            } catch (const std::exception& ex) {
                auto error = TError(ex);
                LOG_DEBUG(error, "Error allocating write targets "
                    "(ChunkId: %v, DesiredTargetCount: %v, MinTargetCount: %v, ReplicationFactorOverride: %v, "
                    "PreferredHostName: %v, ForbiddenAddresses: %v, MediumIndex: %v)",
                    sessionId,
                    desiredTargetCount,
                    minTargetCount,
                    replicationFactorOverride,
                    preferredHostName,
                    forbiddenAddresses);
                ToProto(subresponse->mutable_error(), error);
            }
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, ExportChunks)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        context->SetRequestInfo("TransactionId: %v, ChunkCount: %v",
            transactionId,
            request->chunks_size());

        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::Leader);
        SyncWithUpstream();

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        chunkManager
            ->CreateExportChunksMutation(context)
            ->CommitAndReply(context);
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, ImportChunks)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        context->SetRequestInfo("TransactionId: %v, ChunkCount: %v",
            transactionId,
            request->chunks_size());

        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::Leader);
        SyncWithUpstream();

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        chunkManager
            ->CreateImportChunksMutation(context)
            ->CommitAndReply(context);
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetChunkOwningNodes)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());

        context->SetRequestInfo("ChunkId: %v",
            chunkId);

        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::LeaderOrFollower);
        SyncWithUpstream();

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto* chunk = chunkManager->GetChunkOrThrow(chunkId);

        auto owningNodes = GetOwningNodes(chunk);
        for (const auto* node : owningNodes) {
            auto* protoNode = response->add_nodes();
            ToProto(protoNode->mutable_node_id(), node->GetId());
            if (node->GetTransaction()) {
                ToProto(protoNode->mutable_transaction_id(), node->GetTransaction()->GetId());
            }
        }

        context->SetResponseInfo("NodeCount: %v",
            response->nodes_size());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, ExecuteBatch)
    {
        bool suppressUpstreamSync = request->suppress_upstream_sync();

        context->SetRequestInfo(
            "CreateChunkCount: %v, "
            "ConfirmChunkCount: %v, "
            "SealChunkCount: %v, "
            "CreateChunkListsCount: %v, "
            "UsageChunkListsCount: %v, "
            "SuppressUpstreamSync: %v",
            request->create_chunk_subrequests_size(),
            request->confirm_chunk_subrequests_size(),
            request->seal_chunk_subrequests_size(),
            request->create_chunk_lists_subrequests_size(),
            request->unstage_chunk_tree_subrequests_size(),
            suppressUpstreamSync);

        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::Leader);
        if (!suppressUpstreamSync) {
            SyncWithUpstream();
        }

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        chunkManager
            ->CreateExecuteBatchMutation(context)
            ->CommitAndReply(context);
    }
};

IServicePtr CreateChunkService(TBootstrap* boostrap)
{
    return New<TChunkService>(boostrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
