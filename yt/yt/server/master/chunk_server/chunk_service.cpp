#include "chunk_service.h"

#include "private.h"
#include "config.h"
#include "chunk.h"
#include "chunk_manager.h"
#include "chunk_replicator.h"
#include "helpers.h"
#include "chunk_owner_base.h"
#include "dynamic_store.h"
#include "chunk_owner_node_proxy.h"
#include "chunk_replica_fetcher.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/master_hydra_service.h>
#include <yt/yt/server/master/cell_master/multi_phase_cell_sync_session.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>

#include <yt/yt/server/master/sequoia_server/config.h>

#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_directory_builder.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/yt/server/master/tablet_server/tablet_manager.h>

#include <yt/yt/server/master/transaction_server/transaction.h>
#include <yt/yt/server/master/transaction_server/transaction_replication_session.h>

#include <yt/yt/server/lib/hive/hive_manager.h>

#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/ytlib/data_node_tracker_client/location_directory.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/ytlib/object_client/helpers.h>
#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/core/rpc/helpers.h>
#include <yt/yt/core/rpc/per_user_request_queue_provider.h>

#include <yt/yt/library/erasure/impl/codec.h>

namespace NYT::NChunkServer {

using namespace NHydra;
using namespace NChunkClient;
using namespace NChunkServer;
using namespace NConcurrency;
using namespace NNodeTrackerServer;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NCellMaster;
using namespace NHydra;
using namespace NTransactionClient;
using namespace NTransactionServer;
using namespace NRpc;
using namespace NDataNodeTrackerClient;

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
            ChunkServerLogger())
        , ReconfigurationCallback_(CreateReconfigurationCallback(bootstrap))
        , CreateChunkRequestQueueProvider_(New<TPerUserRequestQueueProvider>(
            ReconfigurationCallback_,
            ChunkServiceProfiler
                .WithDefaultDisabled()
                .WithSparse()
                .WithTag("cell_tag", ToString(bootstrap->GetMulticellManager()->GetCellTag()))
                .WithTag("method", "create_chunk")))
        , ExecuteBatchRequestQueueProvider_(New<TPerUserRequestQueueProvider>(
            ReconfigurationCallback_,
            ChunkServiceProfiler
                .WithDefaultDisabled()
                .WithSparse()
                .WithTag("cell_tag", ToString(bootstrap->GetMulticellManager()->GetCellTag()))
                .WithTag("method", "execute_batch")))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(LocateChunks)
            .SetInvoker(GetGuardedAutomatonInvoker(EAutomatonThreadQueue::ChunkLocator))
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(LocateDynamicStores)
            .SetInvoker(GetGuardedAutomatonInvoker(EAutomatonThreadQueue::ChunkLocator))
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(TouchChunks)
            .SetInvoker(GetGuardedAutomatonInvoker(EAutomatonThreadQueue::ChunkLocator))
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AllocateWriteTargets)
            .SetInvoker(GetGuardedAutomatonInvoker(EAutomatonThreadQueue::ChunkReplicaAllocator))
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ExportChunks)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ImportChunks)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkOwningNodes)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CreateChunk)
            .SetHeavy(true)
            .SetQueueSizeLimit(10'000)
            .SetConcurrencyLimit(10'000)
            .SetRequestQueueProvider(CreateChunkRequestQueueProvider_));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ConfirmChunk)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SealChunk)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CreateChunkLists)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UnstageChunkTree)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AttachChunkTrees)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ExecuteBatch)
            .SetHeavy(true)
            .SetQueueSizeLimit(10'000)
            .SetConcurrencyLimit(10'000)
            .SetRequestQueueProvider(ExecuteBatchRequestQueueProvider_));

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TChunkService::OnDynamicConfigChanged, MakeWeak(this)));

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->SubscribeUserRequestThrottlerConfigChanged(
            BIND_NO_PROPAGATE(&TChunkService::OnUserRequestThrottlerConfigChanged, MakeWeak(this)));

        DeclareServerFeature(EMasterFeature::OverlayedJournals);
    }

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    // COMPAT(danilalexeev) ExecuteBatch will be removed in the future.
    TPerUserRequestQueueProvider::TReconfigurationCallback ReconfigurationCallback_;
    TPerUserRequestQueueProviderPtr CreateChunkRequestQueueProvider_;
    TPerUserRequestQueueProviderPtr ExecuteBatchRequestQueueProvider_;

    std::atomic<bool> EnableCypressTransactionsInSequoia_;
    std::atomic<bool> EnableBoomerangsIdentity_;

    static TPerUserRequestQueueProvider::TReconfigurationCallback CreateReconfigurationCallback(TBootstrap* bootstrap)
    {
        return [=] (TString userName, TRequestQueuePtr queue) {
            auto epochAutomatonInvoker = bootstrap->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ChunkService);

            // NB: Upon recovery, OnDynamicConfigChanged will be called and
            // this invoker will be present.
            if (!epochAutomatonInvoker) {
                return;
            }

            epochAutomatonInvoker->Invoke(BIND([bootstrap, userName = std::move(userName), queue = std::move(queue)] {
                const auto& securityManager = bootstrap->GetSecurityManager();

                auto* user = securityManager->FindUserByName(userName, false);
                if (!user) {
                    return;
                }

                user->AlertIfPendingRemoval(
                    Format("User pending for removal has accessed chunk service (User: %v)",
                    user->GetName()));

                const auto& chunkServiceConfig = bootstrap->GetConfigManager()->GetConfig()->ChunkService;

                auto weightThrottlingEnabled = chunkServiceConfig->EnablePerUserRequestWeightThrottling;
                auto bytesThrottlingEnabled = chunkServiceConfig->EnablePerUserRequestBytesThrottling;

                if (weightThrottlingEnabled) {
                    auto weightThrottlerConfig = user->GetChunkServiceUserRequestWeightThrottlerConfig();
                    if (!weightThrottlerConfig) {
                        weightThrottlerConfig = chunkServiceConfig->DefaultPerUserRequestWeightThrottlerConfig;
                    }
                    queue->ConfigureWeightThrottler(weightThrottlerConfig);
                } else {
                    queue->ConfigureWeightThrottler(nullptr);
                }

                if (bytesThrottlingEnabled) {
                    auto bytesThrottlerConfig = user->GetChunkServiceUserRequestBytesThrottlerConfig();
                    if (!bytesThrottlerConfig) {
                        bytesThrottlerConfig = chunkServiceConfig->DefaultPerUserRequestBytesThrottlerConfig;
                    }
                    queue->ConfigureBytesThrottler(bytesThrottlerConfig);
                } else {
                    queue->ConfigureBytesThrottler(nullptr);
                }
            }));
        };
    }

    const TDynamicChunkServiceConfigPtr& GetDynamicConfig() const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return Bootstrap_->GetConfigManager()->GetConfig()->ChunkService;
    }

    bool AreCypressTransactionsInSequoiaEnabled() const noexcept
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return EnableCypressTransactionsInSequoia_.load(std::memory_order::acquire);
    }

    bool IsBoomerangsIdentityEnabled() const noexcept
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return EnableBoomerangsIdentity_.load(std::memory_order::acquire);
    }

    void OnDynamicConfigChanged(const TDynamicClusterConfigPtr& oldConfig)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& config = Bootstrap_->GetConfigManager()->GetConfig();
        EnableBoomerangsIdentity_.store(
            config->EnableBoomerangsIdentity,
            std::memory_order::release);

        const auto& sequoiaManagerConfig = config->SequoiaManager;
        EnableCypressTransactionsInSequoia_.store(
            sequoiaManagerConfig->Enable && sequoiaManagerConfig->EnableCypressTransactionsInSequoia,
            std::memory_order::release);

        const auto& chunkServiceConfig = config->ChunkService;
        const auto& oldChunkServiceConfig = oldConfig->ChunkService;

        try {
            auto* methodInfo = GetMethodInfoOrThrow(RPC_SERVICE_METHOD_DESC(ExecuteBatch).Method);
            auto weightThrottlerConfig = chunkServiceConfig->DefaultRequestWeightThrottlerConfig;
            auto* requestQueue = methodInfo->GetDefaultRequestQueue();
            requestQueue->ConfigureWeightThrottler(weightThrottlerConfig);
        } catch (const std::exception& ex) {
            YT_LOG_ALERT(ex, "Failed to configure request weight throttler for ChunkService.ExecuteBatch default request queue");
        }

        auto configureRequestQueueProvider = [&] (const auto& queueProvider) {
            // Avoid unnecessary reconfiguration of request queues as it might create
            // significant load on the automaton thread.
            bool shouldReconfigureQueues = false;

            if (oldChunkServiceConfig == Bootstrap_->GetConfigManager()->GetConfig()->ChunkService) {
                // Either an epoch change or irrelevant modification to the config.

                queueProvider->UpdateDefaultConfigs({
                    chunkServiceConfig->DefaultPerUserRequestWeightThrottlerConfig,
                    chunkServiceConfig->DefaultPerUserRequestBytesThrottlerConfig});
                // Crucial on epoch change for guaranteeing up-to-date configuration.
                shouldReconfigureQueues = true;
            } else {
                if (oldChunkServiceConfig->DefaultPerUserRequestWeightThrottlerConfig != chunkServiceConfig->DefaultPerUserRequestWeightThrottlerConfig ||
                    oldChunkServiceConfig->DefaultPerUserRequestBytesThrottlerConfig != chunkServiceConfig->DefaultPerUserRequestBytesThrottlerConfig)
                {
                    queueProvider->UpdateDefaultConfigs({
                        chunkServiceConfig->DefaultPerUserRequestWeightThrottlerConfig,
                        chunkServiceConfig->DefaultPerUserRequestBytesThrottlerConfig});
                    shouldReconfigureQueues = true;
                }

                if (oldChunkServiceConfig->EnablePerUserRequestWeightThrottling != chunkServiceConfig->EnablePerUserRequestWeightThrottling ||
                    oldChunkServiceConfig->EnablePerUserRequestBytesThrottling != chunkServiceConfig->EnablePerUserRequestBytesThrottling)
                {
                    queueProvider->UpdateThrottlingEnabledFlags(
                        chunkServiceConfig->EnablePerUserRequestWeightThrottling,
                        chunkServiceConfig->EnablePerUserRequestBytesThrottling);
                    shouldReconfigureQueues = true;
                }
            }

            if (shouldReconfigureQueues) {
                queueProvider->ReconfigureAllUsers();
            }
        };

        configureRequestQueueProvider(CreateChunkRequestQueueProvider_);
        configureRequestQueueProvider(ExecuteBatchRequestQueueProvider_);
    }

    void OnUserRequestThrottlerConfigChanged(TUser* user)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        CreateChunkRequestQueueProvider_->ReconfigureUser(user->GetName());
        ExecuteBatchRequestQueueProvider_->ReconfigureUser(user->GetName());
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, LocateChunks)
    {
        context->SetRequestInfo("SubrequestCount: %v",
            request->subrequests_size());

        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::LeaderOrFollower);
        // TODO(shakurov): only sync with the leader is really needed,
        // not with the primary cell.
        SyncWithUpstream();

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& chunkReplicator = chunkManager->GetChunkReplicator();

        auto addressType = request->has_address_type()
            ? CheckedEnumCast<NNodeTrackerClient::EAddressType>(request->address_type())
            : NNodeTrackerClient::EAddressType::InternalRpc;
        TNodeDirectoryBuilder nodeDirectoryBuilder(response->mutable_node_directory(), addressType);

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        auto revision = hydraManager->GetAutomatonVersion().ToRevision();

        THashMap<NRpc::IChannelPtr, NChunkClient::NProto::TReqTouchChunks> channelToTouchChunksRequest;

        for (const auto& protoChunkId : request->subrequests()) {
            auto chunkId = FromProto<TChunkId>(protoChunkId);
            auto chunkIdWithIndex = DecodeChunkId(chunkId);

            auto* subresponse = response->add_subresponses();

            auto* chunk = chunkManager->FindChunk(chunkIdWithIndex.Id);
            if (!IsObjectAlive(chunk)) {
                subresponse->set_missing(true);
                continue;
            }

            TChunkPtrWithReplicaIndex chunkWithReplicaIndex(
                chunk,
                chunkIdWithIndex.ReplicaIndex);
            subresponse->set_erasure_codec(ToProto<int>(chunk->GetErasureCodec()));

            auto ephemeralChunk = TEphemeralObjectPtr<TChunk>(chunk);
            auto replicasOrError = chunkManager->LocateChunk(chunkWithReplicaIndex);
            if (!replicasOrError.IsOK()) {
                context->Reply(replicasOrError);
                return;
            }

            if (!IsObjectAlive(ephemeralChunk)) {
                subresponse->set_missing(true);
                continue;
            }

            const auto& replicas = replicasOrError.Value();
            for (auto replica : replicas) {
                subresponse->add_legacy_replicas(ToProto<ui32>(replica));
                subresponse->add_replicas(ToProto<ui64>(replica));
                nodeDirectoryBuilder.Add(replica.GetPtr());
            }

            // NB: LocateChunk also touches chunk if its replicator is local.
            if (!chunkReplicator->ShouldProcessChunk(chunk) && chunk->IsErasure() && !chunk->IsAvailable()) {
                if (auto replicatorChannel = chunkManager->FindChunkReplicatorChannel(chunk)) {
                    auto& request = channelToTouchChunksRequest[replicatorChannel];
                    ToProto(request.add_subrequests(), chunkId);
                }
            }
        }

        response->set_revision(revision);

        for (const auto& [channel, request] : channelToTouchChunksRequest) {
            TChunkServiceProxy proxy(channel);
            auto req = proxy.TouchChunks();
            static_cast<NChunkClient::NProto::TReqTouchChunks&>(*req) = request;
            req->SetTimeout(context->GetTimeout());
            NRpc::SetCurrentAuthenticationIdentity(req);

            YT_LOG_DEBUG("Forwarding touch request to remote replicator (ChunkCount: %v)",
                req->subrequests_size());
            YT_UNUSED_FUTURE(req->Invoke());
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, LocateDynamicStores)
    {
        context->SetRequestInfo("SubrequestCount: %v",
            request->subrequests_size());

        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::LeaderOrFollower);
        SyncWithUpstream();

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& chunkReplicaFetcher = chunkManager->GetChunkReplicaFetcher();

        auto addressType = request->has_address_type()
            ? CheckedEnumCast<NNodeTrackerClient::EAddressType>(request->address_type())
            : NNodeTrackerClient::EAddressType::InternalRpc;
        TNodeDirectoryBuilder nodeDirectoryBuilder(response->mutable_node_directory(), addressType);

        std::vector<TFuture<void>> metaFetchFutures;

        for (const auto& protoStoreId : request->subrequests()) {
            auto storeId = FromProto<TDynamicStoreId>(protoStoreId);
            auto* subresponse = response->add_subresponses();

            auto* dynamicStore = chunkManager->FindDynamicStore(storeId);
            if (!IsObjectAlive(dynamicStore) || dynamicStore->IsAbandoned()) {
                subresponse->set_missing(true);
                continue;
            }

            THashSet<int> extensionTags;
            if (!request->fetch_all_meta_extensions()) {
                extensionTags.insert(request->extension_tags().begin(), request->extension_tags().end());
            }

            if (dynamicStore->IsFlushed()) {
                if (auto* chunk = dynamicStore->GetFlushedChunk()) {
                    auto ephemeralChunk = TEphemeralObjectPtr<TChunk>(chunk);

                    auto* spec = subresponse->mutable_chunk_spec();

                    if (dynamicStore->GetType() == EObjectType::OrderedDynamicTabletStore) {
                        spec->set_row_index_is_absolute(true);
                    }

                    auto rowIndex = dynamicStore->GetType() == EObjectType::OrderedDynamicTabletStore
                        ? std::make_optional(dynamicStore->GetTableRowIndex())
                        : std::nullopt;

                    auto replicas = chunkReplicaFetcher->GetChunkReplicas(ephemeralChunk)
                        .ValueOrThrow();

                    BuildChunkSpec(
                        Bootstrap_,
                        chunk,
                        replicas,
                        rowIndex,
                        /*tabletIndex*/ {},
                        /*lowerLimit*/ {},
                        /*upperLimit*/ {},
                        /*timestampTransactionId*/ {},
                        /*fetchParityReplicas*/ true,
                        request->fetch_all_meta_extensions(),
                        extensionTags,
                        &nodeDirectoryBuilder,
                        spec);


                }
            } else {
                const auto& tabletManager = Bootstrap_->GetTabletManager();
                auto* chunkSpec = subresponse->mutable_chunk_spec();
                auto* tablet = dynamicStore->GetTablet();

                ToProto(chunkSpec->mutable_chunk_id(), dynamicStore->GetId());
                if (auto* node = tabletManager->FindTabletLeaderNode(tablet)) {
                    nodeDirectoryBuilder.Add(node);
                    auto replica = TNodePtrWithReplicaAndMediumIndex(node, GenericChunkReplicaIndex, GenericMediumIndex);
                    chunkSpec->add_legacy_replicas(ToProto<ui32>(replica));
                    chunkSpec->add_replicas(ToProto<ui64>(replica));
                }
                ToProto(chunkSpec->mutable_tablet_id(), tablet->GetId());
            }
        }

        if (!metaFetchFutures.empty()) {
            WaitFor(AllSucceeded(std::move(metaFetchFutures)))
                .ThrowOnError();
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, TouchChunks)
    {
        context->SetRequestInfo("SubrequestCount: %v",
            request->subrequests_size());

        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::LeaderOrFollower);

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
        ValidatePeer(EPeerKind::LeaderOrFollower);

        // TODO(gritukan): only sync with the leader is really needed,
        // not with the primary cell.
        SyncWithUpstream();

        TNodeDirectoryBuilder builder(response->mutable_node_directory());

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        const auto& chunkReplicaFetcher = chunkManager->GetChunkReplicaFetcher();

        // Gather chunks.
        std::vector<TEphemeralObjectPtr<TChunk>> chunks;
        for (const auto& subrequest : request->subrequests()) {
            auto sessionId = FromProto<TSessionId>(subrequest.session_id());
            auto* chunk = chunkManager->FindChunk(sessionId.ChunkId);
            // We will fill subresponse with error later.
            if (!IsObjectAlive(chunk)) {
                continue;
            }

            chunks.emplace_back(chunk);
        }

        auto replicas = chunkReplicaFetcher->GetChunkReplicas(chunks);

        for (const auto& subrequest : request->subrequests()) {
            auto sessionId = FromProto<TSessionId>(subrequest.session_id());
            int desiredTargetCount = subrequest.desired_target_count();
            int minTargetCount = subrequest.min_target_count();
            auto replicationFactorOverride = subrequest.has_replication_factor_override()
                ? std::make_optional(subrequest.replication_factor_override())
                : std::nullopt;
            auto preferredHostName = subrequest.has_preferred_host_name()
                ? std::make_optional(subrequest.preferred_host_name())
                : std::nullopt;
            auto forbiddenAddresses = FromProto<std::vector<TString>>(subrequest.forbidden_addresses());
            auto allocatedAddresses = FromProto<std::vector<TString>>(subrequest.allocated_addresses());

            auto* subresponse = response->add_subresponses();
            try {
                auto* medium = chunkManager->GetMediumByIndexOrThrow(sessionId.MediumIndex);
                if (medium->IsOffshore()) {
                    THROW_ERROR_EXCEPTION("Write targets allocation for offshore media is forbidden")
                        << TErrorAttribute("chunk_id", sessionId.ChunkId)
                        << TErrorAttribute("medium_index", medium->GetIndex())
                        << TErrorAttribute("medium_name", medium->GetName())
                        << TErrorAttribute("medium_type", medium->GetType());
                }
                auto* chunk = chunkManager->GetChunkOrThrow(sessionId.ChunkId);

                auto it = replicas.find(sessionId.ChunkId);
                // This is really weird.
                if (it == replicas.end()) {
                    THROW_ERROR_EXCEPTION("Replicas were not fetched for chunk")
                        << TErrorAttribute("chunk_id", sessionId.ChunkId);
                }

                const auto& chunkReplicas = it->second
                    .ValueOrThrow();

                TNodeList forbiddenNodes;
                for (const auto& address : forbiddenAddresses) {
                    if (auto* node = nodeTracker->FindNodeByAddress(address)) {
                        forbiddenNodes.push_back(node);
                    }
                }
                std::sort(forbiddenNodes.begin(), forbiddenNodes.end());

                TNodeList allocatedNodes;
                for (const auto& address : allocatedAddresses) {
                    if (auto* node = nodeTracker->FindNodeByAddress(address)) {
                        allocatedNodes.push_back(node);
                    }
                }
                std::sort(allocatedNodes.begin(), allocatedNodes.end());

                auto targets = chunkManager->AllocateWriteTargets(
                    medium->AsDomestic(),
                    chunk,
                    chunkReplicas,
                    desiredTargetCount,
                    minTargetCount,
                    replicationFactorOverride,
                    &forbiddenNodes,
                    &allocatedNodes,
                    preferredHostName);

                for (int index = 0; index < static_cast<int>(targets.size()); ++index) {
                    auto* target = targets[index];
                    builder.Add(target);
                    auto replica = TNodePtrWithReplicaAndMediumIndex(target, GenericChunkReplicaIndex, medium->GetIndex());
                    subresponse->add_replicas(ToProto<ui64>(replica));
                }

                YT_LOG_DEBUG("Write targets allocated "
                    "(SessionId: %v%v, DesiredTargetCount: %v, MinTargetCount: %v, ReplicationFactorOverride: %v, "
                    "PreferredHostName: %v, ForbiddenAddresses: %v, AllocatedAddresses: %v, Targets: %v)",
                    sessionId,
                    MakeFormatterWrapper([&] (auto* builder) {
                        if (chunk->HasConsistentReplicaPlacementHash()) {
                            builder->AppendFormat(
                                ", ConsistentReplicaPlacementHash: %x",
                                chunk->GetConsistentReplicaPlacementHash());
                        }
                    }),
                    desiredTargetCount,
                    minTargetCount,
                    replicationFactorOverride,
                    preferredHostName,
                    forbiddenAddresses,
                    allocatedAddresses,
                    MakeFormattableView(targets, TNodePtrAddressFormatter()));
            } catch (const std::exception& ex) {
                auto error = TError(ex);
                YT_LOG_DEBUG(error, "Error allocating write targets "
                    "(SessionId: %v, DesiredTargetCount: %v, MinTargetCount: %v, ReplicationFactorOverride: %v, "
                    "PreferredHostName: %v, ForbiddenAddresses: %v, AllocatedAddresses: %v)",
                    sessionId,
                    desiredTargetCount,
                    minTargetCount,
                    replicationFactorOverride,
                    preferredHostName,
                    forbiddenAddresses,
                    allocatedAddresses);
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
        SyncWithTransactionCoordinatorCell(context, transactionId);

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto mutation = chunkManager->CreateExportChunksMutation(context);
        mutation->SetCurrentTraceContext();
        YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, ImportChunks)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        context->SetRequestInfo("TransactionId: %v, ChunkCount: %v",
            transactionId,
            request->chunks_size());

        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::Leader);

        TCellTagList cellTagsToSyncWith;
        cellTagsToSyncWith.push_back(CellTagFromId(transactionId));

        // Syncing with native chunk cells to ensure chunk schema has been received.
        for (const auto& chunk : request->chunks()) {
            auto chunkId = FromProto<TChunkId>(chunk.id());
            cellTagsToSyncWith.push_back(CellTagFromId(chunkId));
        }

        SortUnique(cellTagsToSyncWith);

        auto syncSession = New<TMultiPhaseCellSyncSession>(
            Bootstrap_,
            ChunkServerLogger().WithTag("RequestId: %v", context->GetRequestId()));
        WaitFor(syncSession->Sync(cellTagsToSyncWith))
            .ThrowOnError();

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto mutation = chunkManager->CreateImportChunksMutation(context);
        mutation->SetCurrentTraceContext();
        YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
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
            if (auto* transaction = node->GetTransaction()) {
                auto transactionId = transaction->IsExternalized()
                    ? transaction->GetOriginalTransactionId()
                    : transaction->GetId();
                ToProto(protoNode->mutable_transaction_id(), transactionId);
            }
        }

        context->SetResponseInfo("NodeCount: %v",
            response->nodes_size());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, ExecuteBatch)
    {
        // COMPAT(shakurov): remove the former.
        auto suppressUpstreamSync =
            request->suppress_upstream_sync() ||
            GetSuppressUpstreamSync(context->RequestHeader());

        context->SetRequestInfo(
            "CreateChunkCount: %v, "
            "ConfirmChunkCount: %v, "
            "SealChunkCount: %v, "
            "CreateChunkListsCount: %v, "
            "UnstageChunkTreeCount: %v, "
            "AttachChunkTreesCount: %v, "
            "SuppressUpstreamSync: %v",
            request->create_chunk_subrequests_size(),
            request->confirm_chunk_subrequests_size(),
            request->seal_chunk_subrequests_size(),
            request->create_chunk_lists_subrequests_size(),
            request->unstage_chunk_tree_subrequests_size(),
            request->attach_chunk_trees_subrequests_size(),
            suppressUpstreamSync);

        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::Leader);

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        // NB: supporting lazy transaction replication here is required for (at
        // least) the following reason. When starting operations, controller
        // agents first start all the necessary transactions, only then getting
        // basic attributes for output & debug tables. Thus, at the moment of
        // starting a transaction the set of cells it'll be needed to be
        // replicated to is yet unknown.
        std::vector<TTransactionId> transactionIds;
        for (const auto& createChunkSubrequest : request->create_chunk_subrequests()) {
            transactionIds.push_back(FromProto<TTransactionId>(createChunkSubrequest.transaction_id()));
        }
        for (const auto& createChunkListsSubrequest : request->create_chunk_lists_subrequests()) {
            transactionIds.push_back(FromProto<TTransactionId>(createChunkListsSubrequest.transaction_id()));
        }
        for (const auto& attachChunkTreesSubrequest : request->attach_chunk_trees_subrequests()) {
            if (attachChunkTreesSubrequest.has_transaction_id()) {
                transactionIds.push_back(FromProto<TTransactionId>(attachChunkTreesSubrequest.transaction_id()));
            }
        }
        SortUnique(transactionIds);

        // COMPAT(kvk1920)
        for (const auto& subrequest : request->confirm_chunk_subrequests()) {
            YT_LOG_ALERT_UNLESS(subrequest.location_uuids_supported(),
                "Chunk confirmation request without location uuids is received");
        }

        // TODO(shakurov): use mutation idempotizer for all mutations (not
        // just the Object Service ones), then enable boomerangs here.
        const auto enableMutationBoomerangs = false;

        NTransactionServer::RunTransactionReplicationSessionAndReply(
            !suppressUpstreamSync,
            Bootstrap_,
            std::move(transactionIds),
            context,
            chunkManager->CreateExecuteBatchMutation(context),
            enableMutationBoomerangs,
            AreCypressTransactionsInSequoiaEnabled(),
            IsBoomerangsIdentityEnabled());
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, AttachChunkTrees)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto parentId = FromProto<TChunkListId>(request->parent_id());

        context->SetRequestInfo(
            "TransactionId: %v, "
            "ParentId: %v",
            transactionId,
            parentId);

        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::Leader);

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto suppressUpstreamSync = GetSuppressUpstreamSync(context->RequestHeader());
        // TODO(shakurov): use mutation idempotizer for all mutations (not
        // just the Object Service ones), then enable boomerangs here.
        const auto enableMutationBoomerangs = false;

        NTransactionServer::RunTransactionReplicationSessionAndReply(
            !suppressUpstreamSync,
            Bootstrap_,
            {transactionId},
            context,
            chunkManager->CreateAttachChunkTreesMutation(context),
            enableMutationBoomerangs,
            AreCypressTransactionsInSequoiaEnabled(),
            IsBoomerangsIdentityEnabled());
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, UnstageChunkTree)
    {
        auto chunkTreeId = FromProto<TChunkId>(request->chunk_tree_id());
        context->SetRequestInfo(
            "ChunkTreeId: %v, "
            "Recursive: %v",
            chunkTreeId,
            request->recursive());

        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::Leader);

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto mutation = chunkManager->CreateUnstageChunkTreeMutation(context);
        mutation->SetCurrentTraceContext();
        YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, CreateChunkLists)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        context->SetRequestInfo(
            "TransactionId: %v, "
            "Count: %v",
            transactionId,
            request->count());

        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::Leader);

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto suppressUpstreamSync = GetSuppressUpstreamSync(context->RequestHeader());
        // TODO(shakurov): use mutation idempotizer for all mutations (not
        // just the Object Service ones), then enable boomerangs here.
        const auto enableMutationBoomerangs = false;

        NTransactionServer::RunTransactionReplicationSessionAndReply(
            !suppressUpstreamSync,
            Bootstrap_,
            {transactionId},
            context,
            chunkManager->CreateCreateChunkListsMutation(context),
            enableMutationBoomerangs,
            AreCypressTransactionsInSequoiaEnabled(),
            IsBoomerangsIdentityEnabled());
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, SealChunk)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        context->SetRequestInfo(
            "ChunkId: %v",
            chunkId);

        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::Leader);

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto mutation = chunkManager->CreateSealChunkMutation(context);
        mutation->SetCurrentTraceContext();
        YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, CreateChunk)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        context->SetRequestInfo(
            "TransactionId: %v",
            transactionId);

        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::Leader);

        const auto& chunkManager = Bootstrap_->GetChunkManager();

        auto suppressUpstreamSync = GetSuppressUpstreamSync(context->RequestHeader());
        // NB: supporting lazy transaction replication here is required for (at
        // least) the following reason. When starting operations, controller
        // agents first start all the necessary transactions, only then getting
        // basic attributes for output & debug tables. Thus, at the moment of
        // starting a transaction the set of cells it'll be needed to be
        // replicated to is yet unknown.

        // TODO(shakurov): use mutation idempotizer for all mutations (not
        // just the Object Service ones), then enable boomerangs here.
        const auto enableMutationBoomerangs = false;

        NTransactionServer::RunTransactionReplicationSessionAndReply(
            !suppressUpstreamSync,
            Bootstrap_,
            {transactionId},
            context,
            chunkManager->CreateCreateChunkMutation(context),
            enableMutationBoomerangs,
            AreCypressTransactionsInSequoiaEnabled(),
            IsBoomerangsIdentityEnabled());
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, ConfirmChunk)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        context->SetRequestInfo(
            "ChunkId: %v",
            chunkId);

        ValidateClusterInitialized();
        ValidatePeer(EPeerKind::Leader);

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& configManager = Bootstrap_->GetConfigManager();
        const auto& chunkReplicaFetcher = chunkManager->GetChunkReplicaFetcher();

        const auto& chunkManagerConfig = configManager->GetConfig()->ChunkManager;
        // COMPAT(kvk1920)
        if (!chunkManagerConfig->EnableChunkConfirmationWithoutLocationUuid) {
            YT_LOG_ALERT_UNLESS(
                request->location_uuids_supported(),
                "Chunk confirmation request without location uuids is received");
        }

        if (chunkManagerConfig->SequoiaChunkReplicas->Enable && request->location_uuids_supported()) {
            auto allReplicas = request->replicas();
            context->Request().mutable_replicas()->Clear();
            context->Request().mutable_legacy_replicas()->Clear();

            auto addSequoiaReplicas = std::make_unique<NProto::TReqAddConfirmReplicas>();
            ToProto(addSequoiaReplicas->mutable_chunk_id(), chunkId);

            for (const auto& replica : allReplicas) {
                auto locationUuid = FromProto<TChunkLocationUuid>(replica.location_uuid());
                if (!chunkReplicaFetcher->IsSequoiaChunkReplica(chunkId, locationUuid)) {
                    *context->Request().add_replicas() = replica;
                } else {
                    *addSequoiaReplicas->add_replicas() = replica;
                }
            }

            if (addSequoiaReplicas->replicas_size() > 0) {
                WaitFor(chunkManager->AddSequoiaConfirmReplicas(std::move(addSequoiaReplicas)))
                    .ThrowOnError();
            }
        }

        auto mutation = chunkManager->CreateConfirmChunkMutation(&context->Request(), &context->Response());
        mutation->SetCurrentTraceContext();
        YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
    }

    void SyncWithTransactionCoordinatorCell(const IServiceContextPtr& context, TTransactionId transactionId)
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        const auto& hiveManager = Bootstrap_->GetHiveManager();

        auto cellTag = CellTagFromId(transactionId);
        auto cellId = multicellManager->GetCellId(cellTag);
        auto syncFuture = hiveManager->SyncWith(cellId, /*enableBatching*/ true);

        YT_LOG_DEBUG("Request will synchronize with another cell (RequestId: %v, CellTag: %v)",
            context->GetRequestId(),
            cellTag);

        WaitFor(syncFuture)
            .ThrowOnError();
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateChunkService(TBootstrap* bootstrap)
{
    return New<TChunkService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
