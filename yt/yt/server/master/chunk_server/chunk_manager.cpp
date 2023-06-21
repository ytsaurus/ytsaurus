#include "chunk_manager.h"

#include "private.h"
#include "chunk.h"
#include "chunk_autotomizer.h"
#include "chunk_creation_time_histogram_builder.h"
#include "chunk_list.h"
#include "chunk_list_type_handler.h"
#include "chunk_merger.h"
#include "chunk_owner_base.h"
#include "chunk_reincarnator.h"
#include "chunk_replicator.h"
#include "chunk_placement.h"
#include "chunk_type_handler.h"
#include "chunk_replicator.h"
#include "chunk_sealer.h"
#include "chunk_tree_balancer.h"
#include "chunk_tree_traverser.h"
#include "chunk_view.h"
#include "chunk_view_type_handler.h"
#include "config.h"
#include "data_node_tracker.h"
#include "dynamic_store.h"
#include "dynamic_store_type_handler.h"
#include "helpers.h"
#include "job.h"
#include "job_controller.h"
#include "job_registry.h"
#include "master_cell_chunk_statistics_collector.h"
#include "medium.h"
#include "medium_type_handler.h"
#include "chunk_location.h"

#include <yt/yt/server/master/cell_master/alert_manager.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>
#include <yt/yt/server/master/cell_master/serialize.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/config.h>

#include <yt/yt/server/master/cell_master/proto/multicell_manager.pb.h>

#include <yt/yt/server/master/chunk_server/proto/chunk_manager.pb.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>

#include <yt/yt/server/master/incumbent_server/incumbent_manager.h>

#include <yt/yt/server/lib/hive/helpers.h>

#include <yt/yt/server/lib/hydra_common/composite_automaton.h>
#include <yt/yt/server/lib/hydra_common/entity_map.h>

#include <yt/yt/server/lib/transaction_supervisor/helpers.h>

#include <yt/yt/server/master/node_tracker_server/config.h>
#include <yt/yt/server/master/node_tracker_server/node_directory_builder.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker.h>
#include <yt/yt/server/master/node_tracker_server/rack.h>

#include <yt/yt/server/master/object_server/object_manager.h>
#include <yt/yt/server/master/object_server/type_handler_detail.h>

#include <yt/yt/server/master/security_server/account.h>
#include <yt/yt/server/master/security_server/group.h>
#include <yt/yt/server/master/security_server/security_manager.h>

#include <yt/yt/server/master/sequoia_server/config.h>

// COMPAT(gritukan)
#include <yt/yt/server/master/table_server/table_node.h>

#include <yt/yt/server/master/tablet_server/tablet.h>
#include <yt/yt/server/master/tablet_server/tablet_manager.h>

#include <yt/yt/server/master/transaction_server/transaction.h>
#include <yt/yt/server/master/transaction_server/transaction_manager.h>

#include <yt/yt/server/master/journal_server/journal_node.h>
#include <yt/yt/server/master/journal_server/journal_manager.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/data_node_tracker_client/location_directory.h>

#include <yt/yt/ytlib/data_node_tracker_client/proto/data_node_tracker_service.pb.h>

#include <yt/yt/ytlib/job_tracker_client/helpers.h>
#include <yt/yt/ytlib/job_tracker_client/job_tracker_service_proxy.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>
#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/proto/chunk_service.pb.h>

#include <yt/yt/ytlib/journal_client/helpers.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/sequoia_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/tablet_client/helpers.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>
#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/client/job_tracker_client/helpers.h>

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/core/concurrency/fair_share_action_queue.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/yson/string.h>

#include <util/generic/cast.h>

#include <util/random/random.h>

namespace NYT::NChunkServer {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NRpc;
using namespace NHydra;
using namespace NNodeTrackerServer;
using namespace NIncumbentClient;
using namespace NTransactionServer;
using namespace NObjectServer;
using namespace NObjectClient;
using namespace NObjectClient::NProto;
using namespace NYson;
using namespace NYTree;
using namespace NCellMaster;
using namespace NCypressServer;
using namespace NSecurityServer;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NDataNodeTrackerClient::NProto;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NJobTrackerClient;
using namespace NJournalClient;
using namespace NJournalServer;
using namespace NSequoiaClient;
using namespace NSequoiaServer;
using namespace NTabletServer;
using namespace NTabletClient;
using namespace NTransactionSupervisor;

using NChunkClient::TSessionId;
using NDataNodeTrackerClient::TChunkLocationDirectory;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

struct TChunkToLinkedListNode
{
    auto operator() (TChunk* chunk) const
    {
        return &chunk->GetDynamicData()->LinkedListNode;
    }
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EChunkReplicaEventType,
    ((Add)      (0))
    ((Remove)   (1))
);

////////////////////////////////////////////////////////////////////////////////

class TChunkTreeBalancerCallbacks
    : public IChunkTreeBalancerCallbacks
{
public:
    explicit TChunkTreeBalancerCallbacks(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    const TDynamicChunkTreeBalancerConfigPtr& GetConfig() const override
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager->ChunkTreeBalancer;
    }

    void RefObject(TObject* object) override
    {
        Bootstrap_->GetObjectManager()->RefObject(object);
    }

    void UnrefObject(TObject* object) override
    {
        Bootstrap_->GetObjectManager()->UnrefObject(object);
    }

    void FlushObjectUnrefs() override
    {
        NObjectServer::FlushObjectUnrefs();
    }

    int GetObjectRefCounter(TObject* object) override
    {
        return object->GetObjectRefCounter();
    }

    void ScheduleRequisitionUpdate(TChunkTree* chunkTree) override
    {
        Bootstrap_->GetChunkManager()->ScheduleChunkRequisitionUpdate(chunkTree);
    }

    TChunkList* CreateChunkList() override
    {
        return Bootstrap_->GetChunkManager()->CreateChunkList(EChunkListKind::Static);
    }

    void ClearChunkList(TChunkList* chunkList) override
    {
        Bootstrap_->GetChunkManager()->ClearChunkList(chunkList);
    }

    void AttachToChunkList(
        TChunkList* chunkList,
        const std::vector<TChunkTree*>& children) override
    {
        Bootstrap_->GetChunkManager()->AttachToChunkList(chunkList, children);
    }

    void AttachToChunkList(
        TChunkList* chunkList,
        TChunkTree* child) override
    {
        Bootstrap_->GetChunkManager()->AttachToChunkList(chunkList, child);
    }

    void AttachToChunkList(
        TChunkList* chunkList,
        TChunkTree* const* childrenBegin,
        TChunkTree* const* childrenEnd) override
    {
        Bootstrap_->GetChunkManager()->AttachToChunkList(chunkList, childrenBegin, childrenEnd);
    }

private:
    TBootstrap* const Bootstrap_;
};

////////////////////////////////////////////////////////////////////////////////

class TJobSchedulingContext
    : public IJobSchedulingContext
{
public:
    TJobSchedulingContext(
        TBootstrap* bootstrap,
        TNode* node,
        NNodeTrackerClient::NProto::TNodeResources* nodeResourceUsage,
        NNodeTrackerClient::NProto::TNodeResources* nodeResourceLimits,
        IJobRegistryPtr jobRegistry)
        : Bootstrap_(bootstrap)
        , Node_(node)
        , NodeResourceUsage_(nodeResourceUsage)
        , NodeResourceLimits_(nodeResourceLimits)
        , JobRegistry_(std::move(jobRegistry))
    { }

    TNode* GetNode() const override
    {
        return Node_;
    }

    const NNodeTrackerClient::NProto::TNodeResources& GetNodeResourceUsage() const override
    {
        return *NodeResourceUsage_;
    }

    const NNodeTrackerClient::NProto::TNodeResources& GetNodeResourceLimits() const override
    {
        return *NodeResourceLimits_;
    }

    TJobId GenerateJobId() const override
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        return chunkManager->GenerateJobId();
    }

    void ScheduleJob(const TJobPtr& job) override
    {
        JobRegistry_->RegisterJob(job);

        *NodeResourceUsage_ += job->ResourceUsage();

        ScheduledJobs_.push_back(job);
    }

    const std::vector<TJobPtr>& GetScheduledJobs() const
    {
        return ScheduledJobs_;
    }

    const IJobRegistryPtr& GetJobRegistry() const override
    {
        return JobRegistry_;
    }

private:
    TBootstrap* const Bootstrap_;

    TNode* const Node_;
    NNodeTrackerClient::NProto::TNodeResources* const NodeResourceUsage_;
    NNodeTrackerClient::NProto::TNodeResources* const NodeResourceLimits_;

    const IJobRegistryPtr JobRegistry_;

    std::vector<TJobPtr> ScheduledJobs_;
};

////////////////////////////////////////////////////////////////////////////////

class TJobControllerCallbacks
    : public IJobControllerCallbacks
{
public:
    void AbortJob(const TJobPtr& job) override
    {
        JobsToAbort_.push_back(job);
    }

    const std::vector<TJobPtr>& GetJobsToAbort() const
    {
        return JobsToAbort_;
    }

private:
    std::vector<TJobPtr> JobsToAbort_;
};

////////////////////////////////////////////////////////////////////////////////

class TChunkManager
    : public IChunkManager
    , public TMasterAutomatonPart
{
public:
    explicit TChunkManager(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::ChunkManager)
        , Config_(Bootstrap_->GetConfig()->ChunkManager)
        , ChunkTreeBalancer_(New<TChunkTreeBalancerCallbacks>(Bootstrap_))
        , ConsistentChunkPlacement_(New<TConsistentChunkPlacement>(
            Bootstrap_,
            DefaultConsistentReplicaPlacementReplicasPerChunk))
        , ChunkPlacement_(New<TChunkPlacement>(
            Bootstrap_,
            ConsistentChunkPlacement_))
        , JobRegistry_(CreateJobRegistry(Bootstrap_))
        , ChunkReplicator_(
            New<TChunkReplicator>(
                Config_,
                Bootstrap_,
                ChunkPlacement_,
                JobRegistry_))
        , ChunkSealer_(CreateChunkSealer(Bootstrap_))
        , ChunkAutotomizer_(CreateChunkAutotomizer(Bootstrap_))
        , ChunkMerger_(New<TChunkMerger>(Bootstrap_))
        , ChunkReincarnator_(CreateChunkReincarnator(Bootstrap_))
        , MasterCellChunkStatisticsCollector_(
            CreateMasterCellChunkStatisticsCollector(
                Bootstrap_,
                {CreateChunkCreationTimeHistogramBuilder(bootstrap)}))
    {
        RegisterMethod(BIND(&TChunkManager::HydraConfirmChunkListsRequisitionTraverseFinished, Unretained(this)));
        RegisterMethod(BIND(&TChunkManager::HydraUpdateChunkRequisition, Unretained(this)));
        RegisterMethod(BIND(&TChunkManager::HydraRegisterChunkEndorsements, Unretained(this)));
        RegisterMethod(BIND(&TChunkManager::HydraExportChunks, Unretained(this)));
        RegisterMethod(BIND(&TChunkManager::HydraImportChunks, Unretained(this)));
        RegisterMethod(BIND(&TChunkManager::HydraExecuteBatch, Unretained(this)));
        RegisterMethod(BIND(&TChunkManager::HydraCreateChunk, Unretained(this)));
        RegisterMethod(BIND(&TChunkManager::HydraConfirmChunk, Unretained(this)));
        RegisterMethod(BIND(&TChunkManager::HydraSealChunk, Unretained(this)));
        RegisterMethod(BIND(&TChunkManager::HydraCreateChunkLists, Unretained(this)));
        RegisterMethod(BIND(&TChunkManager::HydraAttachChunkTrees, Unretained(this)));
        RegisterMethod(BIND(&TChunkManager::HydraUnstageChunkTree, Unretained(this)));
        RegisterMethod(BIND(&TChunkManager::HydraUnstageExpiredChunks, Unretained(this)));
        RegisterMethod(BIND(&TChunkManager::HydraRedistributeConsistentReplicaPlacementTokens, Unretained(this)));

        RegisterLoader(
            "ChunkManager.Keys",
            BIND(&TChunkManager::LoadKeys, Unretained(this)));
        RegisterLoader(
            "ChunkManager.Values",
            BIND(&TChunkManager::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "ChunkManager.Keys",
            BIND(&TChunkManager::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "ChunkManager.Values",
            BIND(&TChunkManager::SaveValues, Unretained(this)));

        auto primaryCellTag = Bootstrap_->GetMulticellManager()->GetPrimaryCellTag();
        DefaultStoreMediumId_ = MakeWellKnownId(EObjectType::Medium, primaryCellTag, 0xffffffffffffffff);

        JobController_->RegisterJobController(EJobType::ReplicateChunk, ChunkReplicator_);
        JobController_->RegisterJobController(EJobType::RemoveChunk, ChunkReplicator_);
        JobController_->RegisterJobController(EJobType::RepairChunk, ChunkReplicator_);
        JobController_->RegisterJobController(EJobType::SealChunk, ChunkSealer_);
        JobController_->RegisterJobController(EJobType::MergeChunks, ChunkMerger_);
        JobController_->RegisterJobController(EJobType::AutotomizeChunk, ChunkAutotomizer_);
        JobController_->RegisterJobController(EJobType::ReincarnateChunk, ChunkReincarnator_);

        const auto& hydraFacade = Bootstrap_->GetHydraFacade();
        Y_UNUSED(hydraFacade);
        VERIFY_INVOKER_THREAD_AFFINITY(hydraFacade->GetAutomatonInvoker(EAutomatonThreadQueue::Default), AutomatonThread);
    }

    void Initialize() override
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(CreateChunkTypeHandler(Bootstrap_, EObjectType::Chunk));
        objectManager->RegisterHandler(CreateChunkTypeHandler(Bootstrap_, EObjectType::ErasureChunk));
        objectManager->RegisterHandler(CreateChunkTypeHandler(Bootstrap_, EObjectType::JournalChunk));
        objectManager->RegisterHandler(CreateChunkTypeHandler(Bootstrap_, EObjectType::ErasureJournalChunk));
        for (auto type = MinErasureChunkPartType;
             type <= MaxErasureChunkPartType;
             type = static_cast<EObjectType>(static_cast<int>(type) + 1))
        {
            objectManager->RegisterHandler(CreateChunkTypeHandler(Bootstrap_, type));
        }
        for (auto type = MinErasureJournalChunkPartType;
             type <= MaxErasureJournalChunkPartType;
             type = static_cast<EObjectType>(static_cast<int>(type) + 1))
        {
            objectManager->RegisterHandler(CreateChunkTypeHandler(Bootstrap_, type));
        }
        objectManager->RegisterHandler(CreateChunkViewTypeHandler(Bootstrap_));
        objectManager->RegisterHandler(CreateDynamicStoreTypeHandler(Bootstrap_, EObjectType::SortedDynamicTabletStore));
        objectManager->RegisterHandler(CreateDynamicStoreTypeHandler(Bootstrap_, EObjectType::OrderedDynamicTabletStore));
        objectManager->RegisterHandler(CreateChunkListTypeHandler(Bootstrap_));
        objectManager->RegisterHandler(CreateMediumTypeHandler(Bootstrap_));

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        nodeTracker->SubscribeNodeRegistered(BIND_NO_PROPAGATE(&TChunkManager::OnNodeRegistered, MakeWeak(this)));
        nodeTracker->SubscribeNodeUnregistered(BIND_NO_PROPAGATE(&TChunkManager::OnNodeUnregistered, MakeWeak(this)));
        nodeTracker->SubscribeNodeRackChanged(BIND_NO_PROPAGATE(&TChunkManager::OnNodeRackChanged, MakeWeak(this)));
        nodeTracker->SubscribeNodeDataCenterChanged(BIND_NO_PROPAGATE(&TChunkManager::OnNodeDataCenterChanged, MakeWeak(this)));
        nodeTracker->SubscribeNodeDecommissionChanged(BIND_NO_PROPAGATE(&TChunkManager::OnNodeDecommissionChanged, MakeWeak(this)));
        nodeTracker->SubscribeNodeDisableWriteSessionsChanged(BIND_NO_PROPAGATE(&TChunkManager::OnNodeDisableWriteSessionsChanged, MakeWeak(this)));

        nodeTracker->SubscribeDataCenterCreated(BIND_NO_PROPAGATE(&TChunkManager::OnDataCenterChanged, MakeWeak(this)));
        nodeTracker->SubscribeDataCenterRenamed(BIND_NO_PROPAGATE(&TChunkManager::OnDataCenterChanged, MakeWeak(this)));
        nodeTracker->SubscribeDataCenterDestroyed(BIND_NO_PROPAGATE(&TChunkManager::OnDataCenterChanged, MakeWeak(this)));

        const auto& dataNodeTracker = Bootstrap_->GetDataNodeTracker();
        dataNodeTracker->SubscribeFullHeartbeat(BIND_NO_PROPAGATE(&TChunkManager::OnFullDataNodeHeartbeat, MakeWeak(this)));
        dataNodeTracker->SubscribeIncrementalHeartbeat(BIND_NO_PROPAGATE(&TChunkManager::OnIncrementalDataNodeHeartbeat, MakeWeak(this)));

        const auto& alertManager = Bootstrap_->GetAlertManager();
        alertManager->RegisterAlertSource(BIND_NO_PROPAGATE(&TChunkManager::GetAlerts, MakeStrong(this)));

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TChunkManager::OnDynamicConfigChanged, MakeWeak(this)));

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->RegisterTransactionActionHandlers(
            MakeTransactionActionHandlerDescriptor(BIND_NO_PROPAGATE(&TChunkManager::HydraPrepareCreateChunk, Unretained(this))),
            MakeTransactionActionHandlerDescriptor(
                MakeEmptyTransactionActionHandler<TTransaction, TReqCreateChunk, const NTransactionSupervisor::TTransactionCommitOptions&>()),
            MakeTransactionActionHandlerDescriptor(
                MakeEmptyTransactionActionHandler<TTransaction, TReqCreateChunk, const NTransactionSupervisor::TTransactionAbortOptions&>()));

        transactionManager->RegisterTransactionActionHandlers(
            MakeTransactionActionHandlerDescriptor(BIND_NO_PROPAGATE(&TChunkManager::HydraPrepareConfirmChunk, Unretained(this))),
            MakeTransactionActionHandlerDescriptor(
                MakeEmptyTransactionActionHandler<TTransaction, TReqConfirmChunk, const NTransactionSupervisor::TTransactionCommitOptions&>()),
            MakeTransactionActionHandlerDescriptor(
                MakeEmptyTransactionActionHandler<TTransaction, TReqConfirmChunk, const NTransactionSupervisor::TTransactionAbortOptions&>()));

        BufferedProducer_ = New<TBufferedProducer>();
        ChunkServerProfiler
            .WithDefaultDisabled()
            .WithTag("cell_tag", ToString(Bootstrap_->GetMulticellManager()->GetCellTag()))
            .AddProducer("", BufferedProducer_);

        // This is a temporary measure to make sure solomon quota is at least somewhat stabilized.
        CrpBufferedProducer_ = New<TBufferedProducer>();
        ChunkServerProfiler
            .WithDefaultDisabled()
            .WithSparse()
            .WithTag("cell_tag", ToString(Bootstrap_->GetMulticellManager()->GetCellTag()))
            .AddProducer("", CrpBufferedProducer_);

        BufferedHistogramProducer_ = New<TBufferedProducer>();
        ChunkServerHistogramProfiler
            .WithDefaultDisabled()
            .WithGlobal()
            .WithTag("cell_tag", ToString(Bootstrap_->GetMulticellManager()->GetCellTag()))
            .AddProducer("", BufferedHistogramProducer_);

        auto bucketBounds = GenerateGenericBucketBounds();

        ChunkRowCountHistogram_ = ChunkServerHistogramProfiler.
            GaugeHistogram("/chunk_row_count_histogram", bucketBounds);
        ChunkCompressedDataSizeHistogram_ = ChunkServerHistogramProfiler.
            GaugeHistogram("/chunk_compressed_data_size_histogram", bucketBounds);
        ChunkUncompressedDataSizeHistogram_ = ChunkServerHistogramProfiler.
            GaugeHistogram("/chunk_uncompressed_data_size_histogram", bucketBounds);
        ChunkDataWeightHistogram_ = ChunkServerHistogramProfiler.
            GaugeHistogram("/chunk_data_weight_histogram", bucketBounds);

        RedistributeConsistentReplicaPlacementTokensExecutor_ =
            New<TPeriodicExecutor>(
                Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::DataNodeTracker),
                BIND(&TChunkManager::OnRedistributeConsistentReplicaPlacementTokens, MakeWeak(this)));
        RedistributeConsistentReplicaPlacementTokensExecutor_->Start();

        ProfilingExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Periodic),
            BIND(&TChunkManager::OnProfiling, MakeWeak(this)),
            TDynamicChunkManagerConfig::DefaultProfilingPeriod);
        ProfilingExecutor_->Start();

        ChunkMerger_->Initialize();
        ChunkAutotomizer_->Initialize();
        ChunkReincarnator_->Initialize();
    }

    IYPathServicePtr GetOrchidService() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return IYPathService::FromProducer(BIND(&TChunkManager::BuildOrchidYson, MakeStrong(this)))
            ->Via(Bootstrap_->GetHydraFacade()->GetGuardedAutomatonInvoker(EAutomatonThreadQueue::ChunkManager));
    }

    const IJobRegistryPtr& GetJobRegistry() const override
    {
        Bootstrap_->VerifyPersistentStateRead();

        return JobRegistry_;
    }

    const IChunkAutotomizerPtr& GetChunkAutotomizer() const override
    {
        return ChunkAutotomizer_;
    }

    const TChunkReplicatorPtr& GetChunkReplicator() const override
    {
        return ChunkReplicator_;
    }

    std::unique_ptr<TMutation> CreateUpdateChunkRequisitionMutation(const NProto::TReqUpdateChunkRequisition& request) override
    {
        return CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            request,
            &TChunkManager::HydraUpdateChunkRequisition,
            this);
    }

    std::unique_ptr<TMutation> CreateConfirmChunkListsRequisitionTraverseFinishedMutation(
        const NProto::TReqConfirmChunkListsRequisitionTraverseFinished& request) override
    {
        return CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            request,
            &TChunkManager::HydraConfirmChunkListsRequisitionTraverseFinished,
            this);
    }

    std::unique_ptr<TMutation> CreateRegisterChunkEndorsementsMutation(
        const NProto::TReqRegisterChunkEndorsements& request) override
    {
        return CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            request,
            &TChunkManager::HydraRegisterChunkEndorsements,
            this);
    }

    std::unique_ptr<TMutation> CreateExportChunksMutation(TCtxExportChunksPtr context) override
    {
        return CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            std::move(context),
            &TChunkManager::HydraExportChunks,
            this);
    }

    std::unique_ptr<TMutation> CreateImportChunksMutation(TCtxImportChunksPtr context) override
    {
        return CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            std::move(context),
            &TChunkManager::HydraImportChunks,
            this);
    }

    std::unique_ptr<TMutation> CreateExecuteBatchMutation(TCtxExecuteBatchPtr context) override
    {
        return CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            std::move(context),
            &TChunkManager::HydraExecuteBatch,
            this);
    }

    std::unique_ptr<TMutation> CreateExecuteBatchMutation(
        TReqExecuteBatch* request,
        TRspExecuteBatch* response) override
    {
        return CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            request,
            response,
            &TChunkManager::HydraExecuteBatch,
            this);
    }

    std::unique_ptr<TMutation> CreateCreateChunkMutation(TCtxCreateChunkPtr context) override
    {
        return CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            std::move(context),
            &TChunkManager::HydraCreateChunk,
            this);
    }

    std::unique_ptr<TMutation> CreateConfirmChunkMutation(TCtxConfirmChunkPtr context) override
    {
        return CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            std::move(context),
            &TChunkManager::HydraConfirmChunk,
            this);
    }

    std::unique_ptr<TMutation> CreateSealChunkMutation(TCtxSealChunkPtr context) override
    {
        return CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            std::move(context),
            &TChunkManager::HydraSealChunk,
            this);
    }

    std::unique_ptr<TMutation> CreateCreateChunkListsMutation(TCtxCreateChunkListsPtr context) override
    {
        return CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            std::move(context),
            &TChunkManager::HydraCreateChunkLists,
            this);
    }

    std::unique_ptr<TMutation> CreateUnstageChunkTreeMutation(TCtxUnstageChunkTreePtr context) override
    {
        return CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            std::move(context),
            &TChunkManager::HydraUnstageChunkTree,
            this);
    }

    std::unique_ptr<TMutation> CreateAttachChunkTreesMutation(TCtxAttachChunkTreesPtr context) override
    {
        return CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            std::move(context),
            &TChunkManager::HydraAttachChunkTrees,
            this);
    }

    bool IsSequoiaCreateChunkRequest(const TReqCreateChunk& request) override
    {
        auto account = FromProto<TString>(request.account());
        if (account == NSecurityClient::SequoiaAccountName) {
            return false;
        }

        const auto& config = Bootstrap_->GetConfigManager()->GetConfig();
        if (!config->SequoiaManager->Enable) {
            return false;
        }

        auto type = CheckedEnumCast<EObjectType>(request.type());
        if (IsJournalChunkType(type)) {
            return false;
        }

        auto probability = config->ChunkManager->SequoiaChunkProbability;
        return static_cast<int>(RandomNumber<ui32>() % 100) < probability;
    }

    bool IsSequoiaConfirmChunkRequest(const TReqConfirmChunk& request) override
    {
        auto chunkId = FromProto<TChunkId>(request.chunk_id());
        if (!IsSequoiaId(chunkId)) {
            return false;
        }

        const auto& config = Bootstrap_->GetConfigManager()->GetConfig();
        return config->SequoiaManager->Enable;
    }

    TPreparedExecuteBatchRequestPtr PrepareExecuteBatchRequest(const TReqExecuteBatch& request) override
    {
        auto preparedRequest = New<TPreparedExecuteBatchRequest>();
        preparedRequest->MutationRequest = request;

        auto splitSubrequests = [&]<class TSubrequest>(
            const ::google::protobuf::RepeatedPtrField<TSubrequest>& subrequests,
            ::google::protobuf::RepeatedPtrField<TSubrequest>* mutationSubrequests,
            std::vector<TSubrequest>* sequoiaSubrequests,
            std::vector<bool>* isSubrequestSequoia,
            auto sequoiaFilter)
        {
            mutationSubrequests->Clear();
            isSubrequestSequoia->reserve(subrequests.size());

            for (const auto& subrequest : subrequests) {
                auto isSequoia = (this->*sequoiaFilter)(subrequest);
                isSubrequestSequoia->push_back(isSequoia);

                if (isSequoia) {
                    sequoiaSubrequests->push_back(subrequest);
                } else {
                    *mutationSubrequests->Add() = subrequest;
                }
            }
        };

        splitSubrequests(
            request.create_chunk_subrequests(),
            preparedRequest->MutationRequest.mutable_create_chunk_subrequests(),
            &preparedRequest->SequoiaRequest.CreateChunkSubrequests,
            &preparedRequest->IsCreateChunkSubrequestSequoia,
            &TChunkManager::IsSequoiaCreateChunkRequest);
        splitSubrequests(
            request.confirm_chunk_subrequests(),
            preparedRequest->MutationRequest.mutable_confirm_chunk_subrequests(),
            &preparedRequest->SequoiaRequest.ConfirmChunkSubrequests,
            &preparedRequest->IsConfirmChunkSubrequestSequoia,
            &TChunkManager::IsSequoiaConfirmChunkRequest);
        return preparedRequest;
    }

    void PrepareExecuteBatchResponse(
        TPreparedExecuteBatchRequestPtr request,
        TRspExecuteBatch* response) override
    {
        *response = request->MutationResponse;

        auto mergeSubresponses = [&]<class TSubresponse>(
            const ::google::protobuf::RepeatedPtrField<TSubresponse>& mutationSubresponses,
            const std::vector<TSubresponse>& sequoiaSubresponses,
            ::google::protobuf::RepeatedPtrField<TSubresponse>* subresponses,
            const std::vector<bool>& isSubrequestSequoia)
        {
            subresponses->Clear();

            int mutationIndex = 0;
            int sequoiaIndex = 0;

            for (auto isSequoia : isSubrequestSequoia) {
                auto* subresponse = subresponses->Add();

                if (isSequoia) {
                    *subresponse = sequoiaSubresponses[sequoiaIndex++];
                } else {
                    *subresponse = mutationSubresponses[mutationIndex++];
                }
            }
        };
        mergeSubresponses(
            request->MutationResponse.create_chunk_subresponses(),
            request->SequoiaResponse.CreateChunkSubresponses,
            response->mutable_create_chunk_subresponses(),
            request->IsCreateChunkSubrequestSequoia);
        mergeSubresponses(
            request->MutationResponse.confirm_chunk_subresponses(),
            request->SequoiaResponse.ConfirmChunkSubresponses,
            response->mutable_confirm_chunk_subresponses(),
            request->IsConfirmChunkSubrequestSequoia);
    }

    // COMPAT(aleksandra-zh)
    TFuture<void> ExecuteBatchSequoia(TPreparedExecuteBatchRequestPtr request) override
    {
        int createChunkSubrequestCount = request->SequoiaRequest.CreateChunkSubrequests.size();
        int confirmChunkSubrequestCount = request->SequoiaRequest.ConfirmChunkSubrequests.size();

        std::vector<TFuture<void>> futures;
        futures.reserve(createChunkSubrequestCount + confirmChunkSubrequestCount);
        request->SequoiaResponse.CreateChunkSubresponses.resize(createChunkSubrequestCount);
        for (int index = 0; index < createChunkSubrequestCount; ++index) {
            const auto& subrequest = request->SequoiaRequest.CreateChunkSubrequests[index];
            auto* subresponse = &request->SequoiaResponse.CreateChunkSubresponses[index];

            auto future = CreateChunk(subrequest)
                .Apply(BIND([=, request = request, this_ = MakeStrong(this)] (const TRspCreateChunk& response) {
                    *subresponse = response;
                }));
            futures.push_back(future);
        }

        request->SequoiaResponse.ConfirmChunkSubresponses.resize(confirmChunkSubrequestCount);
        for (int index = 0; index < confirmChunkSubrequestCount; ++index) {
            const auto& subrequest = request->SequoiaRequest.ConfirmChunkSubrequests[index];
            auto* subresponse = &request->SequoiaResponse.ConfirmChunkSubresponses[index];

            auto future = ConfirmChunk(subrequest)
                .Apply(BIND([=, request = request, this_ = MakeStrong(this)] (const TRspConfirmChunk& response) {
                    *subresponse = response;
                }));
            futures.push_back(future);
        }

        return AllSet(std::move(futures)).AsVoid();
    }

    TNodeList AllocateWriteTargets(
        TMedium* medium,
        TChunk* chunk,
        int desiredCount,
        int minCount,
        std::optional<int> replicationFactorOverride,
        const TNodeList* forbiddenNodes,
        const TNodeList* allocatedNodes,
        const std::optional<TString>& preferredHostName) override
    {
        return ChunkPlacement_->AllocateWriteTargets(
            medium,
            chunk,
            desiredCount,
            minCount,
            replicationFactorOverride,
            forbiddenNodes,
            allocatedNodes,
            preferredHostName,
            ESessionType::User);
    }

    TNodeList AllocateWriteTargets(
        TMedium* medium,
        TChunk* chunk,
        int replicaIndex,
        int desiredCount,
        int minCount,
        std::optional<int> replicationFactorOverride) override
    {
        return ChunkPlacement_->AllocateWriteTargets(
            medium,
            chunk,
            replicaIndex == GenericChunkReplicaIndex
                ? TChunkReplicaIndexList()
                : TChunkReplicaIndexList{replicaIndex},
            desiredCount,
            minCount,
            replicationFactorOverride,
            ESessionType::User);
    }

    void ConfirmChunk(
        TChunk* chunk,
        const TChunkReplicaWithLocationList& replicas,
        const TChunkInfo& chunkInfo,
        const TChunkMeta& chunkMeta)
    {
        auto id = chunk->GetId();

        if (chunk->IsConfirmed()) {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Chunk is already confirmed (ChunkId: %v)",
                id);
            return;
        }

        // NB: Figure out and validate all hunk chunks we are about to reference _before_ confirming
        // the chunk and storing its meta. Otherwise, in DestroyChunk one may end up having
        // dangling references to hunk chunks.
        std::vector<TChunk*> referencedHunkChunks;
        if (auto hunkChunkRefsExt = FindProtoExtension<NTableClient::NProto::THunkChunkRefsExt>(chunkMeta.extensions())) {
            referencedHunkChunks.reserve(hunkChunkRefsExt->refs_size());
            for (const auto& protoRef : hunkChunkRefsExt->refs()) {
                auto hunkChunkId = FromProto<TChunkId>(protoRef.chunk_id());
                auto* hunkChunk = FindChunk(hunkChunkId);
                if (!IsObjectAlive(hunkChunk)) {
                    THROW_ERROR_EXCEPTION("Cannot confirm chunk %v since it references an unknown hunk chunk %v",
                        id,
                        hunkChunkId);
                }
                referencedHunkChunks.push_back(hunkChunk);
            }
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (auto* hunkChunk : referencedHunkChunks) {
            objectManager->RefObject(hunkChunk);
        }

        chunk->Confirm(chunkInfo, chunkMeta);

        UpdateChunkWeightStatisticsHistogram(chunk, /*add*/ true);

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();

        const auto* mutationContext = GetCurrentMutationContext();
        auto mutationTimestamp = mutationContext->GetTimestamp();

        for (auto replica : replicas) {
            auto nodeId = replica.GetNodeId();
            auto* node = nodeTracker->FindNode(nodeId);
            if (!IsObjectAlive(node)) {
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Tried to confirm chunk at an unknown node (ChunkId: %v, NodeId: %v)",
                    id,
                    replica.GetNodeId());
                continue;
            }

            auto* location = FindLocationOnConfirmation(chunk, node, replica);
            if (!location) {
                // Failure has been already logged inside FindLocationOnConfirmation.
                continue;
            }

            if (!node->ReportedDataNodeHeartbeat()) {
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Tried to confirm chunk at node that did not report data node heartbeat yet "
                    "(ChunkId: %v, Address: %v, State: %v)",
                    id,
                    node->GetDefaultAddress(),
                    node->GetLocalState());
                continue;
            }

            TChunkPtrWithReplicaIndex chunkWithReplicaIndex(chunk, replica.GetReplicaIndex());
            if (!location->HasReplica(chunkWithReplicaIndex)) {
                TChunkPtrWithReplicaInfo replicaWithState(
                    chunk,
                    replica.GetReplicaIndex(),
                    chunk->IsJournal() ? EChunkReplicaState::Active : EChunkReplicaState::Generic);

                int mediumIndex = location->GetEffectiveMediumIndex();
                const auto* medium = GetMediumByIndexOrThrow(mediumIndex);
                AddChunkReplica(location, medium, replicaWithState, EAddReplicaReason::Confirmation);

                location->AddUnapprovedReplica(chunkWithReplicaIndex, mutationTimestamp);
            }
        }

        // NB: This is true for non-journal chunks.
        if (chunk->IsSealed()) {
            OnChunkSealed(chunk);
        }

        if (!chunk->IsJournal()) {
            UpdateResourceUsage(chunk, +1);
        }

        ScheduleChunkRefresh(chunk);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Chunk confirmed (ChunkId: %v, Replicas: %v, ReferencedHunkChunkIds: %v)",
            chunk->GetId(),
            replicas,
            MakeFormattableView(referencedHunkChunks, TObjectIdFormatter()));
    }

    // Adds #chunk to its staging transaction resource usage.
    void UpdateTransactionResourceUsage(const TChunk* chunk, i64 delta)
    {
        YT_ASSERT(chunk->IsStaged());
        YT_ASSERT(chunk->IsDiskSizeFinal());

        // NB: Use just the local replication as this only makes sense for staged chunks.
        const auto& requisition = ChunkRequisitionRegistry_.GetRequisition(chunk->GetLocalRequisitionIndex());
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->UpdateTransactionResourceUsage(chunk, requisition, delta);
    }

    // Adds #chunk to accounts' resource usage.
    void UpdateAccountResourceUsage(const TChunk* chunk, i64 delta, const TChunkRequisition* forcedRequisition = nullptr)
    {
        YT_ASSERT(chunk->IsDiskSizeFinal());

        const auto& requisition = forcedRequisition
            ? *forcedRequisition
            : chunk->GetAggregatedRequisition(GetChunkRequisitionRegistry());
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->UpdateResourceUsage(chunk, requisition, delta);
    }

    void UpdateResourceUsage(const TChunk* chunk, i64 delta, const TChunkRequisition* forcedRequisition = nullptr)
    {
        if (chunk->IsStaged()) {
            UpdateTransactionResourceUsage(chunk, delta);
        }
        UpdateAccountResourceUsage(chunk, delta, forcedRequisition);
    }

    void SealChunk(TChunk* chunk, const TChunkSealInfo& info) override
    {
        if (!chunk->IsJournal()) {
            THROW_ERROR_EXCEPTION("Chunk %v is not a journal chunk",
                chunk->GetId());
        }

        if (!chunk->IsConfirmed()) {
            THROW_ERROR_EXCEPTION("Chunk %v is not confirmed",
                chunk->GetId());
        }

        if (chunk->IsSealed()) {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Chunk is already sealed (ChunkId: %v)",
                chunk->GetId());
            return;
        }

        for (auto [chunkTree, cardinality] : chunk->Parents()) {
            const auto* chunkList = chunkTree->As<TChunkList>();
            if (chunkList->GetKind() != EChunkListKind::JournalRoot) {
                continue;
            }
            const auto& children = chunkList->Children();
            int index = GetChildIndex(chunkList, chunk);
            if (index == 0) {
                continue;
            }
            const auto* leftSibling = children[index - 1]->AsChunk();
            if (!leftSibling->IsSealed()) {
                THROW_ERROR_EXCEPTION("Cannot seal chunk %v since its left silbing %v in chunk list %v is not sealed yet",
                    chunk->GetId(),
                    leftSibling->GetId(),
                    chunkList->GetId());
            }
        }

        chunk->Seal(info);
        OnChunkSealed(chunk);

        ScheduleChunkRefresh(chunk);

        for (auto [chunkTree, cardinality] : chunk->Parents()) {
            const auto* chunkList = chunkTree->As<TChunkList>();
            if (chunkList->GetKind() != EChunkListKind::JournalRoot) {
                continue;
            }
            const auto& children = chunkList->Children();
            int index = GetChildIndex(chunkList, chunk);
            if (index + 1 == static_cast<int>(children.size())) {
                continue;
            }
            auto* rightSibling = children[index + 1]->AsChunk();
            ScheduleChunkSeal(rightSibling);
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Chunk sealed "
            "(ChunkId: %v, FirstOverlayedRowIndex: %v, RowCount: %v, UncompressedDataSize: %v, CompressedDataSize: %v)",
            chunk->GetId(),
            info.has_first_overlayed_row_index() ? std::make_optional(info.first_overlayed_row_index()) : std::nullopt,
            info.row_count(),
            info.uncompressed_data_size(),
            info.compressed_data_size());
    }


    TChunk* CreateChunk(
        TTransaction* transaction,
        TChunkList* chunkList,
        NObjectClient::EObjectType chunkType,
        TAccount* account,
        int replicationFactor,
        NErasure::ECodec erasureCodecId,
        TMedium* medium,
        int readQuorum,
        int writeQuorum,
        bool movable,
        bool vital,
        bool overlayed,
        TConsistentReplicaPlacementHash consistentReplicaPlacementHash,
        i64 replicaLagLimit,
        TChunkId hintId) override
    {
        YT_VERIFY(HasMutationContext());

        bool isErasure = IsErasureChunkType(chunkType);
        bool isJournal = IsJournalChunkType(chunkType);

        auto* chunk = hintId ? DoCreateChunk(hintId) : DoCreateChunk(chunkType);
        chunk->SetReadQuorum(readQuorum);
        chunk->SetWriteQuorum(writeQuorum);
        chunk->SetReplicaLagLimit(replicaLagLimit);
        chunk->SetErasureCodec(erasureCodecId);
        chunk->SetMovable(movable);
        chunk->SetOverlayed(overlayed);
        chunk->SetConsistentReplicaPlacementHash(consistentReplicaPlacementHash);

        if (chunk->HasConsistentReplicaPlacementHash()) {
            ++CrpChunkCount_;
        }

        YT_ASSERT(chunk->GetLocalRequisitionIndex() == (isErasure ? MigrationErasureChunkRequisitionIndex : MigrationChunkRequisitionIndex));

        int mediumIndex = medium->GetIndex();
        TChunkRequisition requisition(
            account,
            mediumIndex,
            TReplicationPolicy(replicationFactor, false /* dataPartsOnly */),
            false /* committed */);
        requisition.SetVital(vital);
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto requisitionIndex = ChunkRequisitionRegistry_.GetOrCreate(requisition, objectManager);
        chunk->SetLocalRequisitionIndex(requisitionIndex, GetChunkRequisitionRegistry(), objectManager);

        StageChunk(chunk, transaction, account);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->StageObject(transaction, chunk);

        if (chunkList) {
            AttachToChunkList(chunkList, chunk);
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
            "Chunk created "
            "(ChunkId: %v, ChunkListId: %v, TransactionId: %v, Account: %v, Medium: %v, "
            "ReplicationFactor: %v, ErasureCodec: %v, Movable: %v, Vital: %v%v%v)",
            chunk->GetId(),
            GetObjectId(chunkList),
            transaction->GetId(),
            account->GetName(),
            medium->GetName(),
            replicationFactor,
            erasureCodecId,
            movable,
            vital,
            MakeFormatterWrapper([&] (auto* builder) {
                if (isJournal) {
                    builder->AppendFormat(", ReadQuorum: %v, WriteQuorum: %v, Overlayed: %v",
                        readQuorum,
                        writeQuorum,
                        overlayed);
                }
            }),
            MakeFormatterWrapper([&] (auto* builder) {
                if (consistentReplicaPlacementHash != NullConsistentReplicaPlacementHash) {
                    builder->AppendFormat(", ConsistentReplicaPlacementHash: %x",
                        consistentReplicaPlacementHash);
                }
            }));

        return chunk;
    }

    TFuture<TRspConfirmChunk> ConfirmChunk(const TReqConfirmChunk& request) override
    {
        return SequoiaConfirmChunk(request)
            .Apply(BIND([] (const TErrorOr<TRspConfirmChunk>& responseOrError) {
                if (responseOrError.IsOK()) {
                    return responseOrError.Value();
                } else {
                    TRspConfirmChunk response;
                    ToProto(response.mutable_error(), TError(responseOrError));
                    return response;
                }
            }));
    }

    TChunkTreeStatistics ConstructChunkStatistics(
        TChunkId chunkId,
        const TMiscExt& miscExt,
        const TChunkInfo& chunkInfo)
    {
        TChunkTreeStatistics statistics;
        statistics.RowCount = miscExt.row_count();
        statistics.LogicalRowCount = miscExt.row_count();
        statistics.UncompressedDataSize = miscExt.uncompressed_data_size();
        statistics.CompressedDataSize = miscExt.compressed_data_size();
        statistics.DataWeight = miscExt.data_weight();
        statistics.LogicalDataWeight = miscExt.data_weight();
        if (IsErasureChunkId(chunkId)) {
            statistics.ErasureDiskSpace = chunkInfo.disk_space();
        } else {
            statistics.RegularDiskSpace = chunkInfo.disk_space();
        }
        statistics.ChunkCount = 1;
        statistics.LogicalChunkCount = 1;
        statistics.Rank = 0;
        return statistics;
    }

    TFuture<TRspConfirmChunk> SequoiaConfirmChunk(const TReqConfirmChunk& request) override
    {
        const auto& client = Bootstrap_->GetClusterConnection()->CreateNativeClient(NApi::TClientOptions::FromUser(NSecurityClient::RootUserName));
        auto transaction = CreateSequoiaTransaction(client, Logger);

        auto chunkId = FromProto<TChunkId>(request.chunk_id());
        YT_VERIFY(!IsJournalChunkId(chunkId));

        return transaction->Start(/*startOptions*/ {})
            .Apply(BIND([=, request = std::move(request), this, this_ = MakeStrong(this)] {
                const auto& chunkMeta = request.chunk_meta();

                NRecords::TChunkMetaExtensions chunkMetaExtensions{
                    .Key = GetChunkMetaExtensionsKey(chunkId),
                };
                FromProto(&chunkMetaExtensions, chunkMeta.extensions());
                transaction->WriteRow(chunkMetaExtensions);

                transaction->AddTransactionAction(
                    Bootstrap_->GetCellTag(),
                    NTransactionClient::MakeTransactionActionData(request));

                NApi::TTransactionCommitOptions commitOptions{
                    .CoordinatorCellId = Bootstrap_->GetCellId(),
                    .CoordinatorPrepareMode = NApi::ETransactionCoordinatorPrepareMode::Late,
                };

                WaitFor(transaction->Commit(commitOptions))
                    .ThrowOnError();

                TRspConfirmChunk response;
                if (request.request_statistics()) {
                    const auto& chunkInfo = request.chunk_info();
                    const auto& miscExt = GetProtoExtension<TMiscExt>(chunkMeta.extensions());
                    auto statistics = ConstructChunkStatistics(chunkId, miscExt, chunkInfo);
                    *response.mutable_statistics() = statistics.ToDataStatistics();
                }
                return response;
            }));
    }

    TFuture<TRspCreateChunk> CreateChunk(const TReqCreateChunk& request) override
    {
        return SequoiaCreateChunk(request)
            .Apply(BIND([] (const TErrorOr<TRspCreateChunk>& responseOrError) {
                if (responseOrError.IsOK()) {
                    return responseOrError.Value();
                } else {
                    TRspCreateChunk response;
                    ToProto(response.mutable_error(), TError(responseOrError));
                    return response;
                }
            }));
    }

    TFuture<TRspCreateChunk> SequoiaCreateChunk(const TReqCreateChunk& request) override
    {
        const auto& client = Bootstrap_->GetClusterConnection()->CreateNativeClient(NApi::TClientOptions::FromUser(NSecurityClient::RootUserName));
        auto transaction = CreateSequoiaTransaction(client, Logger);

        int mediumIndex;
        try {
            mediumIndex = GetMediumByNameOrThrow(request.medium_name())->GetIndex();
        } catch (const std::exception& ex) {
            return MakeFuture<TRspCreateChunk>(TError(ex));
        }

        return transaction->Start(/*startOptions*/ {})
            .Apply(BIND([=, request = std::move(request), this, this_ = MakeStrong(this)] () mutable {
                auto chunkType = CheckedEnumCast<EObjectType>(request.type());
                auto chunkId = transaction->GenerateObjectId(chunkType, Bootstrap_->GetCellTag());

                NRecords::TChunkMetaExtensions chunkMetaExtensions{
                    .Key = GetChunkMetaExtensionsKey(chunkId),
                    .MiscExt = "tilted",
                };
                transaction->WriteRow(chunkMetaExtensions);

                ToProto(request.mutable_chunk_id(), chunkId);

                transaction->AddTransactionAction(
                    Bootstrap_->GetCellTag(),
                    NTransactionClient::MakeTransactionActionData(request));

                NApi::TTransactionCommitOptions commitOptions{
                    .CoordinatorCellId = Bootstrap_->GetCellId(),
                    .CoordinatorPrepareMode = NApi::ETransactionCoordinatorPrepareMode::Late,
                };

                WaitFor(transaction->Commit(commitOptions))
                    .ThrowOnError();

                TRspCreateChunk response;
                auto sessionId = TSessionId(chunkId, mediumIndex);
                ToProto(response.mutable_session_id(), sessionId);
                return response;
            }));
    }

    void HydraPrepareCreateChunk(
        TTransaction* /*transaction*/,
        TReqCreateChunk* request,
        const NTransactionSupervisor::TTransactionPrepareOptions& options)
    {
        YT_VERIFY(options.Persistent);
        YT_VERIFY(options.LatePrepare);

        ExecuteCreateChunkSubrequest(
            request,
            /*response*/ nullptr);
    }

    void HydraPrepareConfirmChunk(
        TTransaction* /*transaction*/,
        TReqConfirmChunk* request,
        const NTransactionSupervisor::TTransactionPrepareOptions& options)
    {
        YT_VERIFY(options.Persistent);
        YT_VERIFY(options.LatePrepare);

        ExecuteConfirmChunkSubrequest(
            request,
            /*response*/ nullptr);
    }

    void DestroyChunk(TChunk* chunk) override
    {
        if (chunk->IsForeign()) {
            YT_VERIFY(ForeignChunks_.erase(chunk) == 1);
        }

        if (auto hunkChunkRefsExt = chunk->ChunkMeta()->FindExtension<NTableClient::NProto::THunkChunkRefsExt>()) {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            for (const auto& protoRef : hunkChunkRefsExt->refs()) {
                auto hunkChunkId = FromProto<TChunkId>(protoRef.chunk_id());
                auto* hunkChunk = FindChunk(hunkChunkId);
                if (!IsObjectAlive(hunkChunk)) {
                    YT_LOG_ALERT("Chunk being destroyed references an unknown hunk chunk (ChunkId: %v, HunkChunkId: %v)",
                        chunk->GetId(),
                        hunkChunkId);
                    continue;
                }
                objectManager->UnrefObject(hunkChunk);
            }
        }

        // Decrease staging resource usage; release account.
        UnstageChunk(chunk);

        // Abort all chunk jobs.
        auto jobs = chunk->GetJobs();
        for (const auto& job : jobs) {
            AbortAndRemoveJob(job);
        }

        // Cancel all jobs, reset status etc.
        ChunkReplicator_->OnChunkDestroyed(chunk);

        ChunkSealer_->OnChunkDestroyed(chunk);

        ChunkReincarnator_->OnChunkDestroyed(chunk);

        MasterCellChunkStatisticsCollector_->OnChunkDestroyed(chunk);

        if (chunk->HasConsistentReplicaPlacementHash()) {
            ConsistentChunkPlacement_->RemoveChunk(chunk);
        }

        if (chunk->IsNative() && chunk->IsDiskSizeFinal()) {
            // The chunk has been already unstaged.
            UpdateResourceUsage(chunk, -1);
        }

        UpdateChunkWeightStatisticsHistogram(chunk, /*add*/ false);

        // Unregister chunk replicas from all known locations.
        // Schedule removal jobs.
        for (auto storedReplica : chunk->StoredReplicas()) {
            auto* location = storedReplica.GetPtr();
            auto replicaIndex = storedReplica.GetReplicaIndex();
            TChunkPtrWithReplicaIndex replica(chunk, replicaIndex);
            if (!location->RemoveReplica(replica)) {
                continue;
            }

            TChunkIdWithIndex chunkIdWithIndexes(chunk->GetId(), replicaIndex);
            if (location->AddDestroyedReplica(chunkIdWithIndexes)) {
                ++DestroyedReplicaCount_;
            }
        }

        chunk->UnrefUsedRequisitions(
            GetChunkRequisitionRegistry(),
            Bootstrap_->GetObjectManager());

        UnregisterChunk(chunk);

        if (auto* node = chunk->GetNodeWithEndorsement()) {
            RemoveEndorsement(chunk, node);
        }

        ++ChunksDestroyed_;

        if (chunk->IsSequoia()) {
            --SequoiaChunkCount_;
        }

        if (chunk->HasConsistentReplicaPlacementHash()) {
            --CrpChunkCount_;
        }
    }

    void UnstageChunk(TChunk* chunk) override
    {
        if (chunk->IsStaged() && chunk->IsDiskSizeFinal()) {
            UpdateTransactionResourceUsage(chunk, -1);
        }
        UnstageChunkTree(chunk);
    }

    void ExportChunk(TChunk* chunk, TCellTag destinationCellTag) override
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto cellIndex = multicellManager->GetRegisteredMasterCellIndex(destinationCellTag);
        chunk->Export(cellIndex, GetChunkRequisitionRegistry());
    }

    void UnexportChunk(TChunk* chunk, TCellTag destinationCellTag, int importRefCounter) override
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto cellIndex = multicellManager->GetRegisteredMasterCellIndex(destinationCellTag);

        if (!chunk->IsExportedToCell(cellIndex)) {
            YT_LOG_ALERT("Chunk is not exported and cannot be unexported "
                "(ChunkId: %v, CellTag: %v, CellIndex: %v, ImportRefCounter: %v)",
                chunk->GetId(),
                destinationCellTag,
                cellIndex,
                importRefCounter);
            return;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto* requisitionRegistry = GetChunkRequisitionRegistry();

        auto unexportChunk = [&] () {
            chunk->Unexport(cellIndex, importRefCounter, requisitionRegistry, objectManager);
        };

        if (chunk->GetExternalRequisitionIndex(cellIndex) == EmptyChunkRequisitionIndex) {
            // Unexporting will effectively do nothing from the replication and
            // accounting standpoints.
            unexportChunk();
        } else {
            const auto isChunkDiskSizeFinal = chunk->IsDiskSizeFinal();

            const auto& requisitionBefore = chunk->GetAggregatedRequisition(requisitionRegistry);
            auto replicationBefore = requisitionBefore.ToReplication();

            if (isChunkDiskSizeFinal) {
                UpdateResourceUsage(chunk, -1, &requisitionBefore);
            }

            unexportChunk();

            // NB: don't use requisitionBefore after unexporting (but replicationBefore is ok).

            if (isChunkDiskSizeFinal) {
                UpdateResourceUsage(chunk, +1, nullptr);
            }

            OnChunkUpdated(chunk, replicationBefore);
        }
    }


    TChunkView* CreateChunkView(TChunkTree* underlyingTree, TChunkViewModifier modifier) override
    {
        switch (underlyingTree->GetType()) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk: {
                auto* underlyingChunk = underlyingTree->AsChunk();
                auto transactionId = modifier.GetTransactionId();
                auto* chunkView = DoCreateChunkView(underlyingChunk, std::move(modifier));
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Chunk view created (ChunkViewId: %v, ChunkId: %v, TransactionId: %v)",
                    chunkView->GetId(),
                    underlyingChunk->GetId(),
                    transactionId);
                return chunkView;
            }

            case EObjectType::SortedDynamicTabletStore:
            case EObjectType::OrderedDynamicTabletStore: {
                auto* underlyingStore = underlyingTree->AsDynamicStore();
                auto transactionId = modifier.GetTransactionId();
                auto* chunkView = DoCreateChunkView(underlyingStore, std::move(modifier));
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Chunk view created (ChunkViewId: %v, DynamicStoreId: %v, TransactionId: %v)",
                    chunkView->GetId(),
                    underlyingStore->GetId(),
                    transactionId);
                return chunkView;
            }

            case EObjectType::ChunkView: {
                YT_VERIFY(!modifier.GetTransactionId());

                auto* baseChunkView = underlyingTree->AsChunkView();
                auto* underlyingTree = baseChunkView->GetUnderlyingTree();
                auto adjustedModifier = baseChunkView->Modifier().RestrictedWith(modifier);
                auto* chunkView = DoCreateChunkView(underlyingTree, std::move(adjustedModifier));
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Chunk view created (ChunkViewId: %v, ChunkId: %v, BaseChunkViewId: %v)",
                    chunkView->GetId(),
                    underlyingTree->GetId(),
                    baseChunkView->GetId());
                return chunkView;
            }

            default:
                YT_ABORT();
        }
    }

    void DestroyChunkView(TChunkView* chunkView) override
    {
        YT_VERIFY(!chunkView->GetStagingTransaction());

        auto* underlyingTree = chunkView->GetUnderlyingTree();
        const auto& objectManager = Bootstrap_->GetObjectManager();
        ResetChunkTreeParent(chunkView, underlyingTree);
        objectManager->UnrefObject(underlyingTree);

        auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->UnrefTimestampHolder(chunkView->Modifier().GetTransactionId());

        ++ChunkViewsDestroyed_;
    }

    TChunkView* CloneChunkView(TChunkView* chunkView, NChunkClient::TLegacyReadRange readRange) override
    {
        auto modifier = TChunkViewModifier(chunkView->Modifier())
            .WithReadRange(readRange);
        return CreateChunkView(chunkView->GetUnderlyingTree(), std::move(modifier));
    }


    TDynamicStore* CreateDynamicStore(TDynamicStoreId storeId, TTablet* tablet) override
    {
        auto* dynamicStore = DoCreateDynamicStore(storeId, tablet);
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Dynamic store created (StoreId: %v, TabletId: %v)",
            dynamicStore->GetId(),
            tablet->GetId());
        return dynamicStore;
    }

    void DestroyDynamicStore(TDynamicStore* dynamicStore) override
    {
        YT_VERIFY(!dynamicStore->GetStagingTransaction());

        if (auto* chunk = dynamicStore->GetFlushedChunk()) {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            objectManager->UnrefObject(chunk);
        }
    }


    TChunkList* CreateChunkList(EChunkListKind kind) override
    {
        auto* chunkList = DoCreateChunkList(kind);
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Chunk list created (Id: %v, Kind: %v)",
            chunkList->GetId(),
            chunkList->GetKind());
        return chunkList;
    }

    void DestroyChunkList(TChunkList* chunkList) override
    {
        // Release account.
        UnstageChunkList(chunkList, false);

        // Drop references to children.
        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (auto* child : chunkList->Children()) {
            if (child) {
                ResetChunkTreeParent(chunkList, child);
                objectManager->UnrefObject(child);
            }
        }

        ++ChunkListsDestroyed_;
    }

    void ClearChunkList(TChunkList* chunkList) override
    {
        // TODO(babenko): currently we only support clearing a chunklist with no parents.
        YT_VERIFY(chunkList->Parents().Empty());
        chunkList->IncrementVersion();

        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (auto* child : chunkList->Children()) {
            if (child) {
                ResetChunkTreeParent(chunkList, child);
                objectManager->UnrefObject(child);
            }
        }

        chunkList->Children().clear();
        ResetChunkListStatistics(chunkList);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Chunk list cleared (ChunkListId: %v)", chunkList->GetId());
    }

    TChunkList* CloneTabletChunkList(TChunkList* chunkList) override
    {
        auto* newChunkList = CreateChunkList(chunkList->GetKind());

        if (chunkList->GetKind() == EChunkListKind::OrderedDynamicTablet) {
            AttachToChunkList(
                newChunkList,
                chunkList->Children().data() + chunkList->GetTrimmedChildCount(),
                chunkList->Children().data() + chunkList->Children().size());

            // Restoring statistics.
            newChunkList->Statistics().LogicalRowCount = chunkList->Statistics().LogicalRowCount;
            newChunkList->Statistics().LogicalChunkCount = chunkList->Statistics().LogicalChunkCount;
            newChunkList->Statistics().LogicalDataWeight = chunkList->Statistics().LogicalDataWeight;
            newChunkList->CumulativeStatistics() = chunkList->CumulativeStatistics();
            newChunkList->CumulativeStatistics().TrimFront(chunkList->GetTrimmedChildCount());
        } else if (chunkList->GetKind() == EChunkListKind::SortedDynamicTablet) {
            newChunkList->SetPivotKey(chunkList->GetPivotKey());
            auto children = EnumerateStoresInChunkTree(chunkList);
            AttachToChunkList(newChunkList, children);
        } else if (chunkList->GetKind() == EChunkListKind::Hunk) {
            auto children = EnumerateStoresInChunkTree(chunkList);
            AttachToChunkList(newChunkList, children);
        } else {
            YT_ABORT();
        }

        return newChunkList;
    }

    void AttachToChunkList(
        TChunkList* chunkList,
        TChunkTree* const* childrenBegin,
        TChunkTree* const* childrenEnd) override
    {
        NChunkServer::AttachToChunkList(chunkList, childrenBegin, childrenEnd);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (auto it = childrenBegin; it != childrenEnd; ++it) {
            auto* child = *it;
            objectManager->RefObject(child);
        }
    }

    void AttachToChunkList(
        TChunkList* chunkList,
        const std::vector<TChunkTree*>& children) override
    {
        AttachToChunkList(
            chunkList,
            children.data(),
            children.data() + children.size());
    }

    void AttachToChunkList(
        TChunkList* chunkList,
        TChunkTree* child) override
    {
        AttachToChunkList(
            chunkList,
            &child,
            &child + 1);
    }

    void DetachFromChunkList(
        TChunkList* chunkList,
        TChunkTree* const* childrenBegin,
        TChunkTree* const* childrenEnd,
        EChunkDetachPolicy policy) override
    {
        NChunkServer::DetachFromChunkList(chunkList, childrenBegin, childrenEnd, policy);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (auto it = childrenBegin; it != childrenEnd; ++it) {
            auto* child = *it;
            objectManager->UnrefObject(child);
        }
    }

    void DetachFromChunkList(
        TChunkList* chunkList,
        const std::vector<TChunkTree*>& children,
        EChunkDetachPolicy policy) override
    {
        DetachFromChunkList(
            chunkList,
            children.data(),
            children.data() + children.size(),
            policy);
    }

    void DetachFromChunkList(
        TChunkList* chunkList,
        TChunkTree* child,
        EChunkDetachPolicy policy) override
    {
        DetachFromChunkList(
            chunkList,
            &child,
            &child + 1,
            policy);
    }

    void ReplaceChunkListChild(
        TChunkList* chunkList,
        int childIndex,
        TChunkTree* child) override
    {
        auto* oldChild = chunkList->Children()[childIndex];

        if (oldChild == child) {
            return;
        }

        NChunkServer::ReplaceChunkListChild(chunkList, childIndex, child);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(child);
        objectManager->UnrefObject(oldChild);
    }

    void RebalanceChunkTree(TChunkList* chunkList, EChunkTreeBalancerMode settingsMode) override
    {
        if (!ChunkTreeBalancer_.IsRebalanceNeeded(chunkList, settingsMode)) {
            return;
        }

        YT_PROFILE_TIMING("/chunk_server/chunk_tree_rebalance_time") {
            auto chunklistId = chunkList->GetId();
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Chunk tree rebalancing started (RootId: %v, Mode: %v)",
                chunklistId,
                settingsMode);
            ChunkTreeBalancer_.Rebalance(chunkList);
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Chunk tree rebalancing completed (RootId: %v, Mode: %v)",
                chunklistId,
                settingsMode);
        }
    }


    void StageChunkList(TChunkList* chunkList, TTransaction* transaction, TAccount* account)
    {
        StageChunkTree(chunkList, transaction, account);
    }

    void StageChunk(TChunk* chunk, TTransaction* transaction, TAccount* account)
    {
        StageChunkTree(chunk, transaction, account);

        if (chunk->IsDiskSizeFinal()) {
            UpdateTransactionResourceUsage(chunk, +1);
        }
    }

    void StageChunkTree(TChunkTree* chunkTree, TTransaction* transaction, TAccount* account)
    {
        YT_ASSERT(transaction);
        YT_ASSERT(!chunkTree->IsStaged());

        chunkTree->SetStagingTransaction(transaction);

        if (!account) {
            return;
        }

        chunkTree->StagingAccount().Assign(account);
    }

    void UnstageChunkList(TChunkList* chunkList, bool recursive) override
    {
        UnstageChunkTree(chunkList);

        if (recursive) {
            const auto& transactionManager = Bootstrap_->GetTransactionManager();
            for (auto* child : chunkList->Children()) {
                if (child) {
                    transactionManager->UnstageObject(child->GetStagingTransaction(), child, recursive);
                }
            }
        }
    }

    void UnstageChunkTree(TChunkTree* chunkTree)
    {
        chunkTree->SetStagingTransaction(nullptr);
        chunkTree->StagingAccount().Reset();
    }

    TNodePtrWithReplicaIndexList LocateChunk(TChunkPtrWithReplicaIndex chunkWithReplicaIndex) override
    {
        auto* chunk = chunkWithReplicaIndex.GetPtr();
        auto replicaIndex = chunkWithReplicaIndex.GetReplicaIndex();

        TouchChunk(chunk);

        TNodePtrWithReplicaIndexList result;
        for (auto replica : chunk->StoredReplicas()) {
            if (replicaIndex != GenericChunkReplicaIndex && replica.GetReplicaIndex() != replicaIndex) {
                continue;
            }

            auto* chunkLocation = replica.GetPtr();
            result.emplace_back(chunkLocation->GetNode(), replica.GetReplicaIndex());
        }

        return result;
    }

    void TouchChunk(TChunk* chunk) override
    {
        if (chunk->IsErasure()) {
            ChunkReplicator_->TouchChunk(chunk);
        }
    }


    void ProcessJobHeartbeat(TNode* node, const TCtxJobHeartbeatPtr& context) override
    {
        YT_VERIFY(IsLeader() || IsFollower());

        auto* request = &context->Request();
        auto* response = &context->Response();

        const auto& address = node->GetDefaultAddress();

        // Node resource usage and limits should be changed inside a mutation,
        // so we store them at the beginning of the job heartbeat processing, then work
        // with local copies and update real values via mutation at the end.
        auto resourceUsage = request->resource_usage();
        auto resourceLimits = request->resource_limits();

        JobRegistry_->OverrideResourceLimits(
            &resourceLimits,
            *node);

        auto removeJob = [&] (TJobId jobId) {
            ToProto(response->add_jobs_to_remove(), TJobToRemove{jobId});

            if (auto job = JobRegistry_->FindJob(jobId)) {
                JobRegistry_->OnJobFinished(job);
            }
        };

        auto abortJob = [&] (TJobId jobId) {
            AddJobToAbort(response, {jobId, /*AbortReason*/ std::nullopt});
        };

        TJobControllerCallbacks jobControllerCallbacks;

        THashSet<TJobPtr> processedJobs;

        std::vector<TJobId> waitingJobIds;
        waitingJobIds.reserve(request->jobs().size());

        std::vector<TJobId> runningJobIds;
        runningJobIds.reserve(request->jobs().size());

        // Process job events and find missing jobs.
        for (const auto& jobStatus : request->jobs()) {
            auto jobId = FromProto<TJobId>(jobStatus.job_id());
            auto state = CheckedEnumCast<EJobState>(jobStatus.state());
            auto jobError = FromProto<TError>(jobStatus.result().error());
            if (auto job = JobRegistry_->FindJob(jobId)) {
                YT_VERIFY(processedJobs.emplace(job).second);

                auto jobType = job->GetType();
                job->SetState(state);
                if (state == EJobState::Completed || state == EJobState::Failed || state == EJobState::Aborted) {
                    job->Result() = jobStatus.result();
                    job->Error() = jobError;
                }

                switch (state) {
                    case EJobState::Completed:
                        YT_LOG_DEBUG(jobError, "Job completed (JobId: %v, JobType: %v, Address: %v, ChunkId: %v)",
                            jobId,
                            jobType,
                            address,
                            job->GetChunkIdWithIndexes());

                        JobController_->OnJobCompleted(job);
                        removeJob(jobId);
                        break;

                    case EJobState::Failed:
                        YT_LOG_WARNING(jobError, "Job failed (JobId: %v, JobType: %v, Address: %v, ChunkId: %v)",
                            jobId,
                            jobType,
                            address,
                            job->GetChunkIdWithIndexes());

                        JobController_->OnJobFailed(job);
                        removeJob(jobId);
                        break;

                    case EJobState::Aborted:
                        YT_LOG_WARNING(jobError, "Job aborted (JobId: %v, JobType: %v, Address: %v, ChunkId: %v)",
                            jobId,
                            jobType,
                            address,
                            job->GetChunkIdWithIndexes());

                        JobController_->OnJobAborted(job);
                        removeJob(jobId);
                        break;

                    case EJobState::Running:
                        runningJobIds.push_back(jobId);
                        JobController_->OnJobRunning(job, &jobControllerCallbacks);
                        break;

                    case EJobState::Waiting:
                        waitingJobIds.push_back(jobId);
                        JobController_->OnJobWaiting(job, &jobControllerCallbacks);
                        break;

                    default:
                        YT_ABORT();
                }
            } else {
                // Unknown jobs are aborted and removed.
                switch (state) {
                    case EJobState::Completed:
                        YT_LOG_DEBUG(jobError, "Unknown job has completed, removal scheduled (JobId: %v, Address: %v)",
                            jobId,
                            address);
                        removeJob(jobId);
                        break;

                    case EJobState::Failed:
                        YT_LOG_DEBUG(jobError, "Unknown job has failed, removal scheduled (JobId: %v, Address: %v)",
                            jobId,
                            address);
                        removeJob(jobId);
                        break;

                    case EJobState::Aborted:
                        YT_LOG_DEBUG(jobError, "Job aborted, removal scheduled (JobId: %v, Address: %v)",
                            jobId,
                            address);
                        removeJob(jobId);
                        break;

                    case EJobState::Running:
                        YT_LOG_DEBUG("Unknown job is running, abort scheduled (JobId: %v, Address: %v)",
                            jobId,
                            address);
                        abortJob(jobId);
                        break;

                    case EJobState::Waiting:
                        YT_LOG_DEBUG("Unknown job is waiting, abort scheduled (JobId: %v, Address: %v)",
                            jobId,
                            address);
                        abortJob(jobId);
                        break;

                    default:
                        YT_ABORT();
                }
            }
        }

        YT_LOG_DEBUG_UNLESS(
            runningJobIds.empty(),
            "Jobs are running (JobIds: %v, Address: %v)",
            runningJobIds,
            address);

        YT_LOG_DEBUG_UNLESS(
            waitingJobIds.empty(),
            "Jobs are waiting (JobIds: %v, Address: %v)",
            waitingJobIds,
            address);

        for (const auto& jobToAbort : jobControllerCallbacks.GetJobsToAbort()) {
            YT_LOG_DEBUG("Aborting job (JobId: %v, JobType: %v, Address: %v, ChunkId: %v)",
                jobToAbort->GetJobId(),
                jobToAbort->GetType(),
                address,
                jobToAbort->GetChunkIdWithIndexes());
            abortJob(jobToAbort->GetJobId());
        }

        auto nodeJobs = JobRegistry_->GetNodeJobs(node->GetDefaultAddress());
        for (const auto& job : nodeJobs) {
            if (!processedJobs.contains(job)) {
                YT_LOG_WARNING("Job is missing, aborting (JobId: %v, JobType: %v, Address: %v, ChunkId: %v)",
                    job->GetJobId(),
                    job->GetType(),
                    address,
                    job->GetChunkIdWithIndexes());
                AbortAndRemoveJob(job);
            }
        }

        // Now we schedule new jobs.
        TJobSchedulingContext schedulingContext(
            Bootstrap_,
            node,
            &resourceUsage,
            &resourceLimits,
            JobRegistry_);

        if (JobRegistry_->IsOverdraft()) {
            YT_LOG_ERROR("Job throttler is overdrafted, skipping job scheduling (Address: %v)",
                node->GetDefaultAddress());
        } else {
            for (auto jobType : TEnumTraits<EJobType>::GetDomainValues()) {
                if (!IsMasterJobType(jobType)) {
                    continue;
                }

                if (JobRegistry_->IsOverdraft(jobType)) {
                    YT_LOG_ERROR("Job throttler is overdrafted for job type, skipping job scheduling (Address: %v, JobType: %v)",
                        node->GetDefaultAddress(),
                        jobType);
                    continue;
                }

                JobController_->ScheduleJobs(jobType, &schedulingContext);
            }
        }

        for (const auto& scheduledJob : schedulingContext.GetScheduledJobs()) {
            NJobTrackerClient::NProto::TJobSpec jobSpec;
            jobSpec.set_type(static_cast<int>(scheduledJob->GetType()));

            if (!scheduledJob->FillJobSpec(Bootstrap_, &jobSpec)) {
                continue;
            }

            auto* jobInfo = response->add_jobs_to_start();
            ToProto(jobInfo->mutable_job_id(), scheduledJob->GetJobId());
            *jobInfo->mutable_resource_limits() = scheduledJob->ResourceUsage();

            auto serializedJobSpec = SerializeProtoToRefWithEnvelope(jobSpec);
            response->Attachments().push_back(serializedJobSpec);
        }

        // If node resource usage or limits have changed, we commit mutation with new values.
        if (node->ResourceUsage() != resourceUsage || node->ResourceLimits() != resourceLimits) {
            NNodeTrackerServer::NProto::TReqUpdateNodeResources request;
            request.set_node_id(node->GetId());
            request.mutable_resource_usage()->CopyFrom(resourceUsage);
            request.mutable_resource_limits()->CopyFrom(resourceLimits);

            const auto& nodeTracker = Bootstrap_->GetNodeTracker();
            nodeTracker->CreateUpdateNodeResourcesMutation(request)
                ->CommitAndLog(Logger);
        }
    }

    TJobId GenerateJobId() const override
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        return MakeRandomId(EObjectType::MasterJob, multicellManager->GetCellTag());
    }

    const THashSet<TChunk*>& ForeignChunks() const override
    {
        return ForeignChunks_;
    }

    int GetTotalReplicaCount() override
    {
        return TotalReplicaCount_;
    }

    bool IsChunkReplicatorEnabled() override
    {
        return ChunkReplicator_->IsReplicatorEnabled();
    }

    bool IsChunkRefreshEnabled() override
    {
        return ChunkReplicator_->IsRefreshEnabled();
    }

    bool IsChunkRequisitionUpdateEnabled() override
    {
        return ChunkReplicator_->IsRequisitionUpdateEnabled();
    }

    bool IsChunkSealerEnabled() override
    {
        return ChunkSealer_->IsEnabled();
    }


    void ScheduleChunkRefresh(TChunk* chunk) override
    {
        ChunkReplicator_->ScheduleChunkRefresh(chunk);
    }

    void ScheduleConsistentlyPlacedChunkRefresh(std::vector<TChunk*> chunks)
    {
        if (IsChunkRefreshEnabled()) {
            for (auto* chunk : chunks) {
                ScheduleChunkRefresh(chunk);
            }
        }
    }

    void ScheduleNodeRefresh(TNode* node)
    {
        ChunkReplicator_->ScheduleNodeRefresh(node);
    }

    void ScheduleChunkRequisitionUpdate(TChunkTree* chunkTree) override
    {
        switch (chunkTree->GetType()) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk:
            case EObjectType::JournalChunk:
            case EObjectType::ErasureJournalChunk:
                ScheduleChunkRequisitionUpdate(chunkTree->AsChunk());
                break;

            case EObjectType::ChunkView:
                ScheduleChunkRequisitionUpdate(chunkTree->AsChunkView()->GetUnderlyingTree());
                break;

            case EObjectType::ChunkList:
                ScheduleChunkRequisitionUpdate(chunkTree->AsChunkList());
                break;

            case EObjectType::SortedDynamicTabletStore:
            case EObjectType::OrderedDynamicTabletStore:
                break;

            default:
                YT_ABORT();
        }
    }

    void ScheduleChunkRequisitionUpdate(TChunkList* chunkList)
    {
        YT_VERIFY(HasMutationContext());

        if (!IsObjectAlive(chunkList)) {
            return;
        }

        ChunkListsAwaitingRequisitionTraverse_.emplace(chunkList);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Chunk list is awaiting requisition traverse (ChunkListId: %v)",
            chunkList->GetId());

        ChunkReplicator_->ScheduleRequisitionUpdate(chunkList);
    }

    void ScheduleChunkRequisitionUpdate(TChunk* chunk)
    {
        ChunkReplicator_->ScheduleRequisitionUpdate(chunk);
    }

    void ScheduleChunkSeal(TChunk* chunk) override
    {
        ChunkSealer_->ScheduleSeal(chunk);
    }

    void ScheduleChunkMerge(TChunkOwnerBase* node) override
    {
        YT_VERIFY(HasMutationContext());

        ChunkMerger_->ScheduleMerge(node);
    }

    bool IsNodeBeingMerged(NCypressServer::TNodeId nodeId) const override
    {
        return ChunkMerger_->IsNodeBeingMerged(nodeId);
    }

    TChunk* GetChunkOrThrow(TChunkId id) override
    {
        auto* chunk = FindChunk(id);
        if (!IsObjectAlive(chunk)) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::NoSuchChunk,
                "No such chunk %v",
                id);
        }
        return chunk;
    }

    TChunkView* GetChunkViewOrThrow(TChunkViewId id) override
    {
        auto* chunkView = FindChunkView(id);
        if (!IsObjectAlive(chunkView)) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::NoSuchChunkView,
                "No such chunk view %v",
                id);
        }
        return chunkView;
    }

    TDynamicStore* GetDynamicStoreOrThrow(TDynamicStoreId id) override
    {
        auto* dynamicStore = FindDynamicStore(id);
        if (!IsObjectAlive(dynamicStore)) {
             THROW_ERROR_EXCEPTION(
                 NTabletClient::EErrorCode::NoSuchDynamicStore,
                 "No such dynamic store %v",
                 id);
        }
        return dynamicStore;
    }

    TChunkList* GetChunkListOrThrow(TChunkListId id) override
    {
        auto* chunkList = FindChunkList(id);
        if (!IsObjectAlive(chunkList)) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::NoSuchChunkList,
                "No such chunk list %v",
                id);
        }
        return chunkList;
    }

    TMedium* CreateMedium(
        const TString& name,
        std::optional<bool> transient,
        std::optional<int> priority,
        std::optional<int> hintIndex,
        TObjectId hintId) override
    {
        ValidateMediumName(name);

        if (FindMediumByName(name)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Medium %Qv already exists",
                name);
        }

        if (MediumMap_.GetSize() >= MaxMediumCount) {
            THROW_ERROR_EXCEPTION("Medium count limit %v is reached",
                MaxMediumCount);
        }

        auto objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::Medium, hintId);
        auto mediumIndex = hintIndex ? *hintIndex : GetFreeMediumIndex();
        return DoCreateMedium(
            id,
            mediumIndex,
            name,
            transient,
            priority);
    }

    void DestroyMedium(TMedium* medium) override
    {
        UnregisterMedium(medium);
    }

    void RenameMedium(TMedium* medium, const TString& newName) override
    {
        if (medium->GetName() == newName) {
            return;
        }

        if (medium->IsBuiltin()) {
            THROW_ERROR_EXCEPTION("Builtin medium cannot be renamed");
        }

        if (FindMediumByName(newName)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Medium %Qv already exists",
                newName);
        }

        // Update name.
        YT_VERIFY(NameToMediumMap_.erase(medium->GetName()) == 1);
        YT_VERIFY(NameToMediumMap_.emplace(newName, medium).second);
        medium->SetName(newName);
    }

    void SetMediumPriority(TMedium* medium, int priority) override
    {
        if (medium->GetPriority() == priority) {
            return;
        }

        ValidateMediumPriority(priority);

        medium->SetPriority(priority);
    }

    void SetMediumConfig(TMedium* medium, TMediumConfigPtr newConfig) override
    {
        auto oldMaxReplicationFactor = medium->Config()->MaxReplicationFactor;

        medium->Config() = std::move(newConfig);
        if (medium->Config()->MaxReplicationFactor != oldMaxReplicationFactor) {
            ScheduleGlobalChunkRefresh();
        }
    }

    void ScheduleGlobalChunkRefresh() override
    {
        ChunkReplicator_->ScheduleGlobalChunkRefresh();
    }

    TMedium* FindMediumByName(const TString& name) const override
    {
        auto it = NameToMediumMap_.find(name);
        return it == NameToMediumMap_.end() ? nullptr : it->second;
    }

    TMedium* GetMediumByNameOrThrow(const TString& name) const override
    {
        auto* medium = FindMediumByName(name);
        if (!IsObjectAlive(medium)) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::NoSuchMedium,
                "No such medium %Qv",
                name);
        }
        return medium;
    }

    TMedium* GetMediumOrThrow(TMediumId id) const override
    {
        auto* medium = FindMedium(id);
        if (!IsObjectAlive(medium)) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::NoSuchMedium,
                "No such medium %v",
                id);
        }
        return medium;
    }

    TMedium* FindMediumByIndex(int index) const override
    {
        return index >= 0 && index < MaxMediumCount
            ? IndexToMediumMap_[index]
            : nullptr;
    }

    TMedium* GetMediumByIndexOrThrow(int index) const override
    {
        auto* medium = FindMediumByIndex(index);
        if (!IsObjectAlive(medium)) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::NoSuchMedium,
                "No such medium %v",
                index);
        }
        return medium;
    }

    TMedium* GetMediumByIndex(int index) const override
    {
        auto* medium = FindMediumByIndex(index);
        YT_VERIFY(medium);
        return medium;
    }

    TChunkTree* FindChunkTree(TChunkTreeId id) override
    {
        auto type = TypeFromId(id);
        switch (type) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk:
            case EObjectType::JournalChunk:
            case EObjectType::ErasureJournalChunk:
                return FindChunk(id);
            case EObjectType::ChunkList:
                return FindChunkList(id);
            case EObjectType::ChunkView:
                return FindChunkView(id);
            case EObjectType::SortedDynamicTabletStore:
            case EObjectType::OrderedDynamicTabletStore:
                return FindDynamicStore(id);
            default:
                return nullptr;
        }
    }

    TChunkTree* GetChunkTree(TChunkTreeId id) override
    {
        auto* chunkTree = FindChunkTree(id);
        YT_VERIFY(chunkTree);
        return chunkTree;
    }

    TChunkTree* GetChunkTreeOrThrow(TChunkTreeId id) override
    {
        auto* chunkTree = FindChunkTree(id);
        if (!IsObjectAlive(chunkTree)) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::NoSuchChunkTree,
                "No such chunk tree %v",
                id);
        }
        return chunkTree;
    }

    TFuture<TChunkQuorumInfo> GetChunkQuorumInfo(TChunk* chunk) override
    {
        return GetChunkQuorumInfo(
            chunk->GetId(),
            chunk->GetOverlayed(),
            chunk->GetErasureCodec(),
            chunk->GetReadQuorum(),
            chunk->GetReplicaLagLimit(),
            GetChunkReplicaDescriptors(chunk));
    }

    TFuture<TChunkQuorumInfo> GetChunkQuorumInfo(
        TChunkId chunkId,
        bool overlayed,
        NErasure::ECodec codecId,
        int readQuorum,
        i64 replicaLagLimit,
        const std::vector<NJournalClient::TChunkReplicaDescriptor>& replicaDescriptors) override
    {
        return ComputeQuorumInfo(
            chunkId,
            overlayed,
            codecId,
            readQuorum,
            replicaLagLimit,
            replicaDescriptors,
            GetDynamicConfig()->JournalRpcTimeout,
            Bootstrap_->GetNodeChannelFactory());
    }

    TChunkRequisitionRegistry* GetChunkRequisitionRegistry() override
    {
        return &ChunkRequisitionRegistry_;
    }

    const TChunkRequisitionRegistry* GetChunkRequisitionRegistry() const
    {
        return &ChunkRequisitionRegistry_;
    }

    TNodePtrWithReplicaInfoAndMediumIndexList GetConsistentChunkReplicas(TChunk* chunk) const override
    {
        YT_ASSERT(!chunk->IsForeign());
        YT_ASSERT(chunk->HasConsistentReplicaPlacementHash());
        YT_ASSERT(!chunk->IsErasure());

        TNodePtrWithReplicaInfoAndMediumIndexList result;

        const auto& replication = chunk->GetAggregatedReplication(GetChunkRequisitionRegistry());
        for (auto entry : replication) {
            auto mediumIndex = entry.GetMediumIndex();
            auto mediumPolicy = entry.Policy();
            YT_VERIFY(mediumPolicy);

            auto mediumWriteTargets = ConsistentChunkPlacement_->GetWriteTargets(chunk, mediumIndex);
            YT_VERIFY(
                mediumWriteTargets.empty() ||
                std::ssize(mediumWriteTargets) == chunk->GetPhysicalReplicationFactor(mediumIndex, GetChunkRequisitionRegistry()));

            for (auto replicaIndex = 0; replicaIndex < std::ssize(mediumWriteTargets); ++replicaIndex) {
                auto* node = mediumWriteTargets[replicaIndex];
                result.emplace_back(
                    node,
                    chunk->IsErasure() ? replicaIndex : GenericChunkReplicaIndex,
                    mediumIndex);
            }
        }

        return result;
    }

    TGlobalChunkScanDescriptor GetGlobalJournalChunkScanDescriptor(int shardIndex) const override
    {
        const auto& chunkList = JournalChunks_[shardIndex];
        return TGlobalChunkScanDescriptor{
            .FrontChunk = chunkList.GetFront(),
            .ChunkCount = chunkList.GetSize(),
            .ShardIndex = shardIndex,
        };
    }

    TGlobalChunkScanDescriptor GetGlobalBlobChunkScanDescriptor(int shardIndex) const override
    {
        const auto& chunkList = BlobChunks_[shardIndex];
        return TGlobalChunkScanDescriptor{
            .FrontChunk = chunkList.GetFront(),
            .ChunkCount = chunkList.GetSize(),
            .ShardIndex = shardIndex,
        };
    }

    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(Chunk, TChunk);
    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(ChunkView, TChunkView);
    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(DynamicStore, TDynamicStore);
    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(ChunkList, TChunkList);
    DECLARE_ENTITY_ENTITY_WITH_IRREGULAR_PLURAL_MAP_ACCESSORS_OVERRIDE(Medium, Media, TMedium)

    TEntityMap<TChunk>& MutableChunks() override
    {
        return ChunkMap_;
    }

    TEntityMap<TChunkList>& MutableChunkLists() override
    {
        return ChunkListMap_;
    }

    TEntityMap<TChunkView>& MutableChunkViews() override
    {
        return ChunkViewMap_;
    }

    TEntityMap<TDynamicStore>& MutableDynamicStores() override
    {
        return DynamicStoreMap_;
    }

    TEntityMap<TMedium>& MutableMedia() override
    {
        return MediumMap_;
    }

private:
    const TChunkManagerConfigPtr Config_;

    TChunkTreeBalancer ChunkTreeBalancer_;

    int TotalReplicaCount_ = 0;

    // COMPAT(h0pless)
    bool NeedRecomputeChunkWeightStatisticsHistogram_ = false;

    // COMPAT(kvk1920)
    bool NeedTransformOldReplicas_ = false;

    TPeriodicExecutorPtr ProfilingExecutor_;

    TBufferedProducerPtr BufferedProducer_;
    TBufferedProducerPtr CrpBufferedProducer_;
    TBufferedProducerPtr BufferedHistogramProducer_;

    i64 ChunksCreated_ = 0;
    i64 ChunksDestroyed_ = 0;
    i64 ChunkReplicasAdded_ = 0;
    i64 ChunkReplicasRemoved_ = 0;
    i64 ChunkViewsCreated_ = 0;
    i64 ChunkViewsDestroyed_ = 0;
    i64 ChunkListsCreated_ = 0;
    i64 ChunkListsDestroyed_ = 0;

    i64 SequoiaChunkCount_ = 0;
    i64 CrpChunkCount_ = 0;

    i64 ImmediateAllyReplicasAnnounced_ = 0;
    i64 DelayedAllyReplicasAnnounced_ = 0;
    i64 LazyAllyReplicasAnnounced_ = 0;
    i64 EndorsementsAdded_ = 0;
    i64 EndorsementsConfirmed_ = 0;
    i64 EndorsementCount_ = 0;

    i64 DestroyedReplicaCount_ = 0;

    TGaugeHistogram ChunkRowCountHistogram_;
    TGaugeHistogram ChunkCompressedDataSizeHistogram_;
    TGaugeHistogram ChunkUncompressedDataSizeHistogram_;
    TGaugeHistogram ChunkDataWeightHistogram_;

    TMediumMap<std::vector<i64>> ConsistentReplicaPlacementTokenDistribution_;

    TPeriodicExecutorPtr RedistributeConsistentReplicaPlacementTokensExecutor_;

    // Unlike chunk replicator and sealer, this is maintained on all
    // peers and is not cleared on epoch change.
    const TConsistentChunkPlacementPtr ConsistentChunkPlacement_;
    const TChunkPlacementPtr ChunkPlacement_;

    const IJobRegistryPtr JobRegistry_;

    const TChunkReplicatorPtr ChunkReplicator_;
    const IChunkSealerPtr ChunkSealer_;

    const IChunkAutotomizerPtr ChunkAutotomizer_;

    const TChunkMergerPtr ChunkMerger_;

    const IChunkReincarnatorPtr ChunkReincarnator_;

    const IMasterCellChunkStatisticsCollectorPtr MasterCellChunkStatisticsCollector_;

    // Global chunk lists; cf. TChunkDynamicData.
    using TGlobalChunkList = TIntrusiveLinkedList<TChunk, TChunkToLinkedListNode>;
    using TShardedGlobalChunkList = std::array<TGlobalChunkList, ChunkShardCount>;

    TShardedGlobalChunkList BlobChunks_;
    TShardedGlobalChunkList JournalChunks_;

    NHydra::TEntityMap<TChunk> ChunkMap_;
    NHydra::TEntityMap<TChunkView> ChunkViewMap_;
    NHydra::TEntityMap<TDynamicStore> DynamicStoreMap_;
    NHydra::TEntityMap<TChunkList> ChunkListMap_;

    THashSet<TChunk*> ForeignChunks_;

    NHydra::TEntityMap<TMedium> MediumMap_;
    THashMap<TString, TMedium*> NameToMediumMap_;
    std::vector<TMedium*> IndexToMediumMap_;
    TMediumSet UsedMediumIndexes_;

    TMediumId DefaultStoreMediumId_;
    TMedium* DefaultStoreMedium_ = nullptr;

    TChunkRequisitionRegistry ChunkRequisitionRegistry_;

    // Each requisition update scheduled for a chunk list should eventually be
    // converted into a number of requisition update requests scheduled for its
    // chunks. Before that conversion happens, however, the chunk list must be
    // kept alive. Each chunk list in this multiset carries additional (strong) references
    // (whose number coincides with the chunk list's multiplicity) to ensure that.
    THashMultiSet<TChunkListPtr> ChunkListsAwaitingRequisitionTraverse_;

    const ICompositeJobControllerPtr JobController_ = CreateCompositeJobController();

    std::optional<TIncrementalHeartbeatCounters> TotalIncrementalHeartbeatCounters_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    // COMPAT(kvk1920)
    TInstant TestingOnlyLastRecoveryTime_;

    TChunkLocation* FindLocationOnConfirmation(
        TChunk* chunk,
        TNode* node,
        const TChunkReplicaWithLocation& replica)
    {
        if (node->UseImaginaryChunkLocations()) {
            int mediumIndex = replica.GetMediumIndex();
            if (!FindMediumByIndex(mediumIndex)) {
                YT_LOG_ERROR_IF(IsMutationLoggingEnabled(),
                    "Imaginary chunk locations are used, "
                    "but chunk confirmation request contains invalid medium index "
                    "(ChunkId: %v, NodeId: %v, MediumIndex: %v)",
                    chunk->GetId(),
                    node->GetId(),
                    replica.GetMediumIndex());
                return nullptr;
            }
            return node->GetOrCreateImaginaryChunkLocation(mediumIndex);
        }

        auto locationUuid = replica.GetChunkLocationUuid();

        if (locationUuid == InvalidChunkLocationUuid || locationUuid == EmptyChunkLocationUuid) {
            YT_LOG_ALERT(
                "Real chunk locations are used but chunk confirmation request "
                "does not have location UUID "
                "(ChunkId: %v, NodeId: %v)",
                chunk->GetId(),
                node->GetId());
            return nullptr;
        }

        const auto& dataNodeTracker = Bootstrap_->GetDataNodeTracker();
        if (auto* location = dataNodeTracker->FindChunkLocationByUuid(locationUuid)) {
            if (location->GetNode() == nullptr) {
                YT_LOG_ALERT(
                    "Chunk location without a node encountered (LocationUuid: %v, NodeId: %v, Address: %v)",
                    locationUuid,
                    node->GetId(),
                    node->GetDefaultAddress());
                return nullptr;
            }

            return location;
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
            "Real chunk locations are used "
            "but chunk confirmation request has invalid location UUID "
            "(ChunkId: %v, NodeId: %v, LocationUuid: %v)",
            chunk->GetId(),
            node->GetId(),
            locationUuid);

        return nullptr;
    }

    const TIncrementalHeartbeatCounters& GetIncrementalHeartbeatCounters(TNode* node)
    {
        const auto& dynamicConfig = GetDynamicConfig();

        if (dynamicConfig->EnablePerNodeIncrementalHeartbeatProfiling) {
            auto& nodeCounters = node->IncrementalHeartbeatCounters();
            if (!nodeCounters) {
                nodeCounters.emplace(
                    ChunkServerProfiler
                        .WithPrefix("/incremental_heartbeat")
                        .WithTag("node", node->GetDefaultAddress()));
            }
            return *nodeCounters;
        }

        if (!TotalIncrementalHeartbeatCounters_) {
            TotalIncrementalHeartbeatCounters_.emplace(ChunkServerProfiler.WithPrefix(
                "/incremental_heartbeat"));
        }
        return *TotalIncrementalHeartbeatCounters_;
    }

    void BuildOrchidYson(NYson::IYsonConsumer* consumer)
    {
        BuildYsonFluently(consumer)
            .BeginMap()
                .DoIf(DefaultStoreMedium_, [&] (auto fluent) {
                    fluent
                        .Item("requisition_registry").Value(TSerializableChunkRequisitionRegistry(Bootstrap_->GetChunkManager()));
                })
                .Item("endorsement_count").Value(EndorsementCount_)
                .Item("chunk_replicator_enabled").Value(ChunkReplicator_->IsReplicatorEnabled())
            .EndMap();
    }

    const TDynamicChunkManagerConfigPtr& GetDynamicConfig() const
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager;
    }

    bool IsConsistentChunkPlacementEnabled() const
    {
        return GetDynamicConfig()->ConsistentReplicaPlacement->Enable;
    }

    TChunk* DoCreateChunk(EObjectType chunkType)
    {
        auto id = Bootstrap_->GetObjectManager()->GenerateId(chunkType);
        return DoCreateChunk(id);
    }

    TChunk* DoCreateChunk(TChunkId chunkId)
    {
        auto chunkHolder = TPoolAllocator::New<TChunk>(chunkId);
        auto* chunk = ChunkMap_.Insert(chunkId, std::move(chunkHolder));
        chunk->RememberAevum();

        RegisterChunk(chunk);
        chunk->RefUsedRequisitions(GetChunkRequisitionRegistry());
        ++ChunksCreated_;

        if (chunk->IsSequoia()) {
            ++SequoiaChunkCount_;
        }

        MasterCellChunkStatisticsCollector_->OnChunkCreated(chunk);

        return chunk;
    }

    TChunkList* DoCreateChunkList(EChunkListKind kind)
    {
        ++ChunkListsCreated_;
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::ChunkList);
        auto chunkListHolder = TPoolAllocator::New<TChunkList>(id);
        auto* chunkList = ChunkListMap_.Insert(id, std::move(chunkListHolder));
        chunkList->SetKind(kind);
        return chunkList;
    }

    void UpdateChunkWeightStatisticsHistogram(const TChunk* chunk, bool add)
    {
        YT_VERIFY(HasHydraContext());

        if (!chunk->IsBlob() || !chunk->IsConfirmed() || chunk->IsForeign()) {
            return;
        }

        auto rowCount = chunk->GetRowCount();
        auto compressedDataSize = chunk->GetCompressedDataSize();
        auto uncompressedDataSize = chunk->GetUncompressedDataSize();
        auto dataWeight = chunk->GetDataWeight();

        if (add) {
            ChunkRowCountHistogram_.Add(rowCount, 1);
            ChunkCompressedDataSizeHistogram_.Add(compressedDataSize, 1);
            ChunkUncompressedDataSizeHistogram_.Add(uncompressedDataSize, 1);
            ChunkDataWeightHistogram_.Add(dataWeight, 1);
        } else {
            ChunkRowCountHistogram_.Remove(rowCount, 1);
            ChunkCompressedDataSizeHistogram_.Remove(compressedDataSize, 1);
            ChunkUncompressedDataSizeHistogram_.Remove(uncompressedDataSize, 1);
            ChunkDataWeightHistogram_.Remove(dataWeight, 1);
        }
    }

    TChunkView* DoCreateChunkView(TChunkTree* underlyingTree, TChunkViewModifier modifier)
    {
        auto treeType = underlyingTree->GetType();
        YT_VERIFY(IsBlobChunkType(treeType) || IsDynamicTabletStoreType(treeType));

        ++ChunkViewsCreated_;
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::ChunkView);
        auto chunkViewHolder = TPoolAllocator::New<TChunkView>(id);
        auto* chunkView = ChunkViewMap_.Insert(id, std::move(chunkViewHolder));

        chunkView->SetUnderlyingTree(underlyingTree);
        SetChunkTreeParent(chunkView, underlyingTree);

        if (modifier.GetTransactionId()) {
            auto& transactionManager = Bootstrap_->GetTransactionManager();
            transactionManager->CreateOrRefTimestampHolder(modifier.GetTransactionId());
        }

        chunkView->Modifier() = std::move(modifier);
        Bootstrap_->GetObjectManager()->RefObject(underlyingTree);

        return chunkView;
    }

    TDynamicStore* DoCreateDynamicStore(TDynamicStoreId storeId, TTablet* tablet)
    {
        auto holder = TPoolAllocator::New<TDynamicStore>(storeId);
        auto* dynamicStore = DynamicStoreMap_.Insert(storeId, std::move(holder));
        dynamicStore->SetTablet(tablet);
        return dynamicStore;
    }

    void OnNodeRegistered(TNode* node)
    {
        ScheduleNodeRefresh(node);
    }

    void OnNodeUnregistered(TNode* node)
    {
        ChunkPlacement_->OnNodeUnregistered(node);

        YT_VERIFY(!node->ReportedDataNodeHeartbeat());
        OnMaybeNodeWriteTargetValidityChanged(
            node,
            EWriteTargetValidityChange::ReportedDataNodeHeartbeat);

        auto jobs = JobRegistry_->GetNodeJobs(node->GetDefaultAddress());
        for (const auto& job : jobs) {
            AbortAndRemoveJob(job);
        }

        ChunkReplicator_->OnNodeUnregistered(node);

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        node->Reset(nodeTracker);
    }

    void OnNodeDecommissionChanged(TNode* node)
    {
        OnMaybeNodeWriteTargetValidityChanged(
            node,
            EWriteTargetValidityChange::Decommissioned);

        OnNodeChanged(node);
    }

    void OnNodeDisableWriteSessionsChanged(TNode* node)
    {
        OnMaybeNodeWriteTargetValidityChanged(
            node,
            EWriteTargetValidityChange::WriteSessionsDisabled);
    }

    void DisposeNode(TNode* node) override
    {
        YT_VERIFY(HasMutationContext());

        ChunkReplicator_->OnNodeUnregistered(node);

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        node->Reset(nodeTracker);

        DiscardEndorsements(node);

        for (auto* location : node->ChunkLocations()) {
            // maybe all of this is normal for some reason and we should just dispose it, idk
            if (!location->Replicas().empty()) {
                YT_LOG_ALERT("Cleared location still has replicas (NodeId: %v, LocationMediumIndex: %v, ReplicasCount: %v)",
                    node->GetId(),
                    location->GetEffectiveMediumIndex(),
                    location->Replicas().size());
                DisposeLocation(location);
            }
            if (!location->UnapprovedReplicas().empty()) {
                YT_LOG_ALERT("Cleared location still has unapproved replicas (NodeId: %v, LocationMediumIndex: %v, UnapprovedReplicasCount: %v)",
                    node->GetId(),
                    location->GetEffectiveMediumIndex(),
                    location->UnapprovedReplicas().size());
                DisposeLocation(location);
            }
            if (!location->DestroyedReplicas().empty()) {
                YT_LOG_ALERT("Cleared location still has destroyed replicas (NodeId: %v, LocationMediumIndex: %v, DestroyedReplicasCount: %v)",
                    node->GetId(),
                    location->GetEffectiveMediumIndex(),
                    location->DestroyedReplicas().size());
                DisposeLocation(location);
            }
        }

        ChunkPlacement_->OnNodeDisposed(node);

        ChunkReplicator_->OnNodeDisposed(node);
    }

    void DisposeLocation(NChunkServer::TChunkLocation* location) override
    {
        YT_VERIFY(HasMutationContext());

        const auto* medium = FindMediumByIndex(location->GetEffectiveMediumIndex());
        if (!IsObjectAlive(medium)) {
            return;
        }

        for (auto replica : location->Replicas()) {
            TChunkPtrWithReplicaIndex replicaWithoutState(replica);
            bool approved = !location->HasUnapprovedReplica(replicaWithoutState);
            RemoveChunkReplica(
                location,
                replicaWithoutState,
                ERemoveReplicaReason::NodeDisposed,
                approved);

            auto* chunk = replica.GetPtr();
            if (chunk->IsBlob()) {
                ScheduleEndorsement(chunk);
            }
        }

        DestroyedReplicaCount_ -= std::ssize(location->DestroyedReplicas());

        location->ClearReplicas();
    }

    void OnNodeChanged(TNode* node)
    {
        if (node->ReportedDataNodeHeartbeat()) {
            ScheduleNodeRefresh(node);
        }

        ChunkPlacement_->OnNodeUpdated(node);
    }

    void OnNodeRackChanged(TNode* node, TRack* /*oldRack*/)
    {
        OnNodeChanged(node);
    }

    void OnNodeDataCenterChanged(TNode* node, TDataCenter* /*oldDataCenter*/)
    {
        OnNodeChanged(node);
    }

    void OnDataCenterChanged(TDataCenter* dataCenter)
    {
        ChunkPlacement_->OnDataCenterChanged(dataCenter);
    }

    void OnMaybeNodeWriteTargetValidityChanged(TNode* node, EWriteTargetValidityChange change)
    {
        auto isValidWriteTarget = node->IsValidWriteTarget();
        auto wasValidWriteTarget = node->WasValidWriteTarget(change);
        if (isValidWriteTarget == wasValidWriteTarget) {
            return;
        }

        std::vector<TChunk*> affectedChunks;
        if (isValidWriteTarget) {
            affectedChunks = ConsistentChunkPlacement_->AddNode(node);
        } else {
            affectedChunks = ConsistentChunkPlacement_->RemoveNode(node);

            node->ConsistentReplicaPlacementTokenCount().clear();
        }

        ScheduleConsistentlyPlacedChunkRefresh(affectedChunks);
    }

    bool IsExactlyReplicatedByApprovedReplicas(const TChunk* chunk)
    {
        YT_VERIFY(chunk->IsBlob());

        int physicalReplicaCount = chunk->GetAggregatedPhysicalReplicationFactor(
            GetChunkRequisitionRegistry());
        int approvedReplicaCount = chunk->GetApprovedReplicaCount();

        return physicalReplicaCount == approvedReplicaCount;
    }

    void DiscardEndorsements(TNode* node)
    {
        // This node might be the last replica for some chunks.
        for (auto [chunk, revision] : node->ReplicaEndorsements()) {
            YT_VERIFY(chunk->GetNodeWithEndorsement() == node);
            chunk->SetNodeWithEndorsement(nullptr);
        }
        EndorsementCount_ -= ssize(node->ReplicaEndorsements());
        node->ReplicaEndorsements().clear();
    }

    bool IsClusterStableEnoughForImmediateReplicaAnnounces() const
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        const auto& statistics = multicellManager->GetClusterStatistics();

        const auto& globalConfig = GetDynamicConfig();
        const auto& specificConfig = globalConfig->AllyReplicaManager;

        int safeOnlineNodeCount = specificConfig->SafeOnlineNodeCount.value_or(
            globalConfig->SafeOnlineNodeCount);
        if (statistics.online_node_count() < safeOnlineNodeCount) {
            return false;
        }

        int safeLostChunkCount = specificConfig->SafeLostChunkCount.value_or(
            globalConfig->SafeLostChunkCount);
        if (statistics.lost_vital_chunk_count() > safeLostChunkCount) {
            return false;
        }

        return true;
    }

    template <class TResponse>
    void SetAnnounceReplicaRequests(
        TResponse* response,
        TNode* node,
        const std::vector<TChunk*>& chunks)
    {
        const auto& dynamicConfig = GetDynamicConfig()->AllyReplicaManager;
        if (!dynamicConfig->EnableAllyReplicaAnnouncement) {
            return;
        }

        bool clusterIsStableEnough = IsClusterStableEnoughForImmediateReplicaAnnounces();
        if (Bootstrap_->IsPrimaryMaster()) {
            response->set_enable_lazy_replica_announcements(clusterIsStableEnough);
        }

        auto onChunk = [&] (TChunk* chunk, bool confirmationNeeded) {
            // Fast path: no need to announce replicas of chunks with RF=1.
            if (!chunk->IsErasure() &&
                chunk->GetAggregatedPhysicalReplicationFactor(GetChunkRequisitionRegistry()) <= 1)
            {
                return;
            }

            auto* request = response->add_replica_announcement_requests();
            ToProto(request->mutable_chunk_id(), chunk->GetId());
            ToProto(request->mutable_replicas(), chunk->StoredReplicas());
            request->set_confirmation_needed(confirmationNeeded);

            if (!clusterIsStableEnough) {
                request->set_lazy(true);
                ++LazyAllyReplicasAnnounced_;
            } else if (!IsExactlyReplicatedByApprovedReplicas(chunk)) {
                request->set_delay(ToProto<i64>(
                    dynamicConfig->UnderreplicatedChunkAnnouncementRequestDelay));
                ++DelayedAllyReplicasAnnounced_;
            } else {
                ++ImmediateAllyReplicasAnnounced_;
            }
        };

        for (auto* chunk : chunks) {
            onChunk(chunk, false);
        }

        if (dynamicConfig->EnableEndorsements) {
            if (clusterIsStableEnough) {
                auto currentRevision = GetCurrentMutationContext()->GetVersion().ToRevision();
                for (auto& [chunk, revision] : node->ReplicaEndorsements()) {
                    revision = currentRevision;
                    onChunk(chunk, true);
                }
            }
        } else if (!node->ReplicaEndorsements().empty()) {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Discarded endorsements from node "
                "since endorsements are not enabled (NodeId: %v, Address: %v, EndorsementCount: %v)",
                node->GetId(),
                node->GetDefaultAddress(),
                node->ReplicaEndorsements().size());
            DiscardEndorsements(node);
        }
    }

    template <class THeartbeat>
    TCompactVector<TRealChunkLocation*, TypicalChunkLocationCount> ParseLocationDirectoryOrThrow(
        const TNode* node,
        const THeartbeat& request)
    {
        using namespace NDataNodeTrackerClient::NProto;
        static_assert(
            std::is_same_v<THeartbeat, TReqFullHeartbeat> ||
            std::is_same_v<THeartbeat, TReqIncrementalHeartbeat>,
            "THeartbeat must be either TReqFullHeartbeat or TReqIncrementalHeartbeat");

        auto rawLocationDirectory = FromProto<TChunkLocationDirectory>(request.location_directory());
        // NB: Location directory was previously parsed in
        // `TDataNodeService::*Heartbeat()` so there is no need to validate it again.

        constexpr bool fullHeartbeat = std::is_same_v<THeartbeat, TReqFullHeartbeat>;

        auto checkLocationIndices = [&] (const auto& chunkInfos) {
            for (const auto& chunkInfo : chunkInfos) {
                AlertAndThrowOnInvalidLocationIndex<fullHeartbeat>(chunkInfo, node, rawLocationDirectory.Uuids().size());
            }
        };

        if constexpr (fullHeartbeat) {
            checkLocationIndices(request.chunks());
        } else {
            checkLocationIndices(request.added_chunks());
            checkLocationIndices(request.removed_chunks());
        }

        TCompactVector<TRealChunkLocation*, TypicalChunkLocationCount> locationDirectory;
        locationDirectory.reserve(rawLocationDirectory.Uuids().size());

        const auto& dataNodeTracker = Bootstrap_->GetDataNodeTracker();
        for (auto uuid : rawLocationDirectory.Uuids()) {
            // All locations were checked in `TDataNodeTracker::Hydra*Heartbeat()`
            // so it should be safe to use `GetChunkLocationByUuid()` here.
            locationDirectory.push_back(dataNodeTracker->GetChunkLocationByUuid(uuid));
        }

        return locationDirectory;
    }

    void OnFullDataNodeHeartbeat(
        TNode* node,
        NDataNodeTrackerClient::NProto::TReqFullHeartbeat* request,
        NDataNodeTrackerClient::NProto::TRspFullHeartbeat* response)
    {
        for (auto* location : node->ChunkLocations()) {
            YT_VERIFY(location->Replicas().empty());
        }
        // COMPAT(kvk1920)
        if (node->UseImaginaryChunkLocations()) {
            for (const auto& stats : request->chunk_statistics()) {
                auto mediumIndex = stats.medium_index();
                if (!FindMediumByIndex(mediumIndex)) {
                    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                        "Cannot create imaginary chunk location with unknown medium "
                        "(NodeId: %v, MediumIndex: %v)",
                        node->GetId(),
                        mediumIndex);
                    continue;
                }
                auto* location = node->GetOrCreateImaginaryChunkLocation(mediumIndex);
                location->ReserveReplicas(stats.chunk_count());
            }
        } else {
            const auto& dataNodeTracker = Bootstrap_->GetDataNodeTracker();

            for (const auto& stats : request->statistics().chunk_locations()) {
                auto locationUuid = FromProto<TChunkLocationUuid>(stats.location_uuid());
                auto* location = dataNodeTracker->GetChunkLocationByUuid(locationUuid);
                location->ReserveReplicas(stats.chunk_count());
            }
        }

        std::vector<TChunk*> announceReplicaRequests;
        announceReplicaRequests.reserve(request->chunks().size());

        auto locationDirectory = ParseLocationDirectoryOrThrow(node, *request);

        for (const auto& chunkInfo : request->chunks()) {
            if (auto* chunk = ProcessAddedChunk(node, locationDirectory, chunkInfo, false)) {
                if (chunk->IsBlob()) {
                    announceReplicaRequests.push_back(chunk);
                }
            }
        }

        response->set_revision(GetCurrentMutationContext()->GetVersion().ToRevision());
        SetAnnounceReplicaRequests(response, node, announceReplicaRequests);

        ChunkPlacement_->OnNodeRegistered(node);
        ChunkPlacement_->OnNodeUpdated(node);

        // Calculating the exact CRP token count for a node is hard because it
        // requires analyzing total space distribution for all nodes. This is
        // done periodically. In the meantime, use an estimate based on the
        // distribution generated by recent recalculation.
        node->ConsistentReplicaPlacementTokenCount().clear();
        if (node->IsValidWriteTarget()) {
            for (auto [mediumIndex, totalSpace] : node->TotalSpace()) {
                if (totalSpace == 0) {
                    continue;
                }
                auto tokenCount = EstimateNodeConsistentReplicaPlacementTokenCount(node, mediumIndex);
                YT_VERIFY(tokenCount > 0);
                node->ConsistentReplicaPlacementTokenCount()[mediumIndex] = tokenCount;
            }
        }

        YT_VERIFY(node->ReportedDataNodeHeartbeat());
        OnMaybeNodeWriteTargetValidityChanged(
            node,
            EWriteTargetValidityChange::ReportedDataNodeHeartbeat);
    }

    template <bool FullHeartbeat>
    void AlertAndThrowOnInvalidLocationIndex(
        const auto& chunkInfo,
        const TNode* node,
        unsigned int locationDirectorySize)
    {
        // Heartbeats should no longer contain location uuids but if node was
        // registered before master server update it still can send  heartbeats
        // with location uuids.
        YT_ASSERT(
            !chunkInfo.has_location_uuid() ||
            node->GetRegisterTime() < TestingOnlyLastRecoveryTime_);

        using TChunkInfo = std::decay_t<decltype(chunkInfo)>;
        static_assert(std::is_same_v<TChunkInfo, TChunkAddInfo> || std::is_same_v<TChunkInfo, TChunkRemoveInfo>,
            "TChunkInfo must be either TChunkAddInfo or TChunkRemoveInfo");

        constexpr bool isRemoval = !FullHeartbeat &&
            std::is_same_v<std::decay_t<decltype(chunkInfo)>, TChunkRemoveInfo>;

        if (static_cast<ui32>(chunkInfo.location_index()) >= locationDirectorySize) {
            YT_LOG_ALERT(
                "Data node reported % heartbeat with invalid location index "
                "(%vChunkId: %v, NodeAddress: %v, LocationIndex: %v)",
                FullHeartbeat ? "full" : "incremental",
                FullHeartbeat ? "" : (isRemoval ? "Removed" : "Added"),
                FromProto<TChunkId>(chunkInfo.chunk_id()),
                node->GetDefaultAddress(),
                chunkInfo.location_index());

            THROW_ERROR_EXCEPTION("%v heartbeat contains an incorrect location index",
                FullHeartbeat ? "Full" : "Incremental");
        }
    }

    void ScheduleEndorsement(TChunk* chunk)
    {
        if (!chunk->GetEndorsementRequired()) {
            chunk->SetEndorsementRequired(true);
            ScheduleChunkRefresh(chunk);
        }
    }

    void RegisterEndorsement(TChunk* chunk)
    {
        if (!GetDynamicConfig()->AllyReplicaManager->EnableEndorsements) {
            return;
        }

        TNode* nodeWithMaxId = nullptr;

        for (auto replica : chunk->StoredReplicas()) {
            auto* medium = FindMediumByIndex(replica.GetPtr()->GetEffectiveMediumIndex());
            if (!medium) {
                continue;
            }

            // We do not care about approvedness.
            auto* node = replica.GetPtr()->GetNode();
            if (!nodeWithMaxId || node->GetId() > nodeWithMaxId->GetId()) {
                nodeWithMaxId = node;
            }
        }

        if (!nodeWithMaxId) {
            return;
        }

        if (auto* formerNode = chunk->GetNodeWithEndorsement()) {
            if (formerNode == nodeWithMaxId) {
                // If there is an in-flight request to the node that is waiting for
                // confirmation, we should treat it as outdated and send a new one.
                auto& revision = GetOrCrash(formerNode->ReplicaEndorsements(), chunk);
                if (revision) {
                    revision = NullRevision;
                }

                return;
            }

            EraseOrCrash(formerNode->ReplicaEndorsements(), chunk);
            --EndorsementCount_;
        }

        chunk->SetNodeWithEndorsement(nodeWithMaxId);
        nodeWithMaxId->ReplicaEndorsements().emplace(chunk, NullRevision);
        ++EndorsementsAdded_;
        ++EndorsementCount_;

        YT_LOG_TRACE_IF(IsMutationLoggingEnabled(),
            "Chunk replica endorsement added (ChunkId: %v, NodeId: %v, Address: %v)",
            chunk->GetId(),
            nodeWithMaxId->GetId(),
            nodeWithMaxId->GetDefaultAddress());
    }

    void RemoveEndorsement(TChunk* chunk, TNode* node)
    {
        if (chunk->GetNodeWithEndorsement() != node) {
            return;
        }
        EraseOrCrash(node->ReplicaEndorsements(), chunk);
        chunk->SetNodeWithEndorsement(nullptr);
        --EndorsementCount_;
    }

    void OnIncrementalDataNodeHeartbeat(
        TNode* node,
        NDataNodeTrackerClient::NProto::TReqIncrementalHeartbeat* request,
        NDataNodeTrackerClient::NProto::TRspIncrementalHeartbeat* response)
    {
        node->ShrinkHashTables();

        for (const auto& protoRequest : request->confirmed_replica_announcement_requests()) {
            auto chunkId = FromProto<TChunkId>(protoRequest.chunk_id());
            auto revision = FromProto<ui64>(protoRequest.revision());

            if (auto* chunk = FindChunk(chunkId); IsObjectAlive(chunk)) {
                auto it = node->ReplicaEndorsements().find(chunk);
                if (it != node->ReplicaEndorsements().end() && it->second == revision) {
                    RemoveEndorsement(chunk, node);
                    ++EndorsementsConfirmed_;
                }
            }
        }

        auto locationDirectory = ParseLocationDirectoryOrThrow(node, *request);

        std::vector<TChunk*> announceReplicaRequests;
        for (const auto& chunkInfo : request->added_chunks()) {
            if (!node->UseImaginaryChunkLocations() && chunkInfo.caused_by_medium_change()) {
                auto* chunk = FindChunk(FromProto<TChunkId>(chunkInfo.chunk_id()));
                if (IsObjectAlive(chunk)) {
                    if (chunk->IsBlob()) {
                        ScheduleEndorsement(chunk);
                    }
                    // It's ok to call ScheduleChunkRefresh twice.
                    ScheduleChunkRefresh(chunk);
                }
                continue;
            }
            if (auto* chunk = ProcessAddedChunk(node, locationDirectory, chunkInfo, true)) {
                if (chunk->IsBlob()) {
                    announceReplicaRequests.push_back(chunk);
                }
            }
        }

        response->set_revision(GetCurrentMutationContext()->GetVersion().ToRevision());
        SetAnnounceReplicaRequests(response, node, announceReplicaRequests);

        const auto& counters = GetIncrementalHeartbeatCounters(node);
        counters.RemovedChunks.Increment(request->removed_chunks().size());

        for (const auto& chunkInfo : request->removed_chunks()) {
            if (!node->UseImaginaryChunkLocations() && chunkInfo.caused_by_medium_change()) {
                continue;
            }
            if (auto* chunk = ProcessRemovedChunk(node, locationDirectory, chunkInfo)) {
                if (IsObjectAlive(chunk) && chunk->IsBlob()) {
                    ScheduleEndorsement(chunk);
                }
            }
        }

        const auto* mutationContext = GetCurrentMutationContext();
        auto mutationTimestamp = mutationContext->GetTimestamp();

        const auto& dynamicConfig = GetDynamicConfig();

        int removedUnapprovedReplicaCount = 0;
        for (auto* location : node->ChunkLocations()) {
            auto& unapprovedReplicas = location->UnapprovedReplicas();
            for (auto it = unapprovedReplicas.begin(); it != unapprovedReplicas.end();) {
                auto jt = it++;
                auto replica = jt->first;
                auto registerTimestamp = jt->second;
                auto reason = ERemoveReplicaReason::None;
                if (!IsObjectAlive(replica.GetPtr())) {
                    reason = ERemoveReplicaReason::ChunkDestroyed;
                } else if (mutationTimestamp > registerTimestamp + dynamicConfig->ReplicaApproveTimeout) {
                    reason = ERemoveReplicaReason::ApproveTimeout;
                }
                if (reason != ERemoveReplicaReason::None) {
                    RemoveChunkReplica(
                        location,
                        replica,
                        reason,
                        /*approved*/ false);
                    ++removedUnapprovedReplicaCount;
                }
            }
        }

        counters.RemovedUnapprovedReplicas.Increment(removedUnapprovedReplicaCount);

        ChunkPlacement_->OnNodeUpdated(node);
    }

    void OnRedistributeConsistentReplicaPlacementTokens()
    {
        if (!IsLeader()) {
            return;
        }

        NProto::TReqRedistributeConsistentReplicaPlacementTokens request;
        auto mutation = CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            request,
            &TChunkManager::HydraRedistributeConsistentReplicaPlacementTokens,
            this);
        mutation->Commit();
    }

    void HydraRedistributeConsistentReplicaPlacementTokens(
        NProto::TReqRedistributeConsistentReplicaPlacementTokens* /*request*/)
    {
        auto onNodeTokensRedistributed = [&] (TNode* node, int mediumIndex, i64 oldTokenCount, i64 newTokenCount) {
            auto affectedChunks = ConsistentChunkPlacement_->UpdateNodeTokenCount(node, mediumIndex, oldTokenCount, newTokenCount);
            ScheduleConsistentlyPlacedChunkRefresh(affectedChunks);
        };

        auto setNodeTokenCount = [&] (TNode* node, int mediumIndex, int newTokenCount) {
            auto it = node->ConsistentReplicaPlacementTokenCount().find(mediumIndex);
            auto oldTokenCount = (it == node->ConsistentReplicaPlacementTokenCount().end())
                ? 0
                : it->second;

            if (oldTokenCount == newTokenCount) {
                return;
            }

            if (newTokenCount == 0) {
                node->ConsistentReplicaPlacementTokenCount().erase(it);
            } else {
                node->ConsistentReplicaPlacementTokenCount()[mediumIndex] = newTokenCount;
            }

            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Node CRP token count changed (NodeId: %v, Address: %v, MediumIndex: %v, OldTokenCount: %v, NewTokenCount: %v)",
                node->GetId(),
                node->GetDefaultAddress(),
                mediumIndex,
                oldTokenCount,
                newTokenCount);

            onNodeTokensRedistributed(node, mediumIndex, oldTokenCount, newTokenCount);
        };

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();

        for (auto& [_, mediumDistribution] : ConsistentReplicaPlacementTokenDistribution_) {
            mediumDistribution.clear();
        }

        std::vector<std::pair<i64, TNode*>> nodesByTotalSpace;
        nodesByTotalSpace.reserve(nodeTracker->Nodes().size());

        for (auto [_, medium] : Media()) {
            if (!IsObjectAlive(medium)) {
                continue;
            }

            auto mediumIndex = medium->GetIndex();
            auto& mediumDistribution = ConsistentReplicaPlacementTokenDistribution_[mediumIndex];

            for (auto [_, node] : nodeTracker->Nodes()) {
                if (!IsObjectAlive(node)) {
                    continue;
                }

                if (!node->IsValidWriteTarget()) {
                    // A workaround for diverged snapshots. Not really necessary these days.
                    auto it = node->ConsistentReplicaPlacementTokenCount().find(mediumIndex);
                    if (it != node->ConsistentReplicaPlacementTokenCount().end()) {
                        YT_ASSERT(false);
                        node->ConsistentReplicaPlacementTokenCount().erase(it);
                    }

                    continue;
                }

                auto it = node->TotalSpace().find(mediumIndex);
                if (it == node->TotalSpace().end() || it->second == 0) {
                    setNodeTokenCount(node, mediumIndex, 0);
                    continue;
                }

                nodesByTotalSpace.emplace_back(it->second, node);
            }

            std::sort(
                nodesByTotalSpace.begin(),
                nodesByTotalSpace.end(),
                [&] (std::pair<i64, TNode*> lhs, std::pair<i64, TNode*> rhs) {
                    if (lhs.first != rhs.first) {
                        return lhs.first > rhs.first;
                    }

                    // Just for determinism.
                    return NObjectServer::TObjectIdComparer()(lhs.second, rhs.second);
                });

            const auto bucketCount = GetDynamicConfig()->ConsistentReplicaPlacement->TokenDistributionBucketCount;
            auto nodesPerBucket = std::ssize(nodesByTotalSpace) / bucketCount;

            for (auto i = 0; i < bucketCount; ++i) {
                auto bucketBeginIndex = std::min(i * nodesPerBucket, ssize(nodesByTotalSpace));
                auto bucketEndIndex = (i == bucketCount - 1)
                    ? ssize(nodesByTotalSpace)
                    : std::min(bucketBeginIndex + nodesPerBucket, ssize(nodesByTotalSpace));

                auto bucketBegin = nodesByTotalSpace.begin();
                std::advance(bucketBegin, bucketBeginIndex);
                auto bucketEnd = nodesByTotalSpace.begin();
                std::advance(bucketEnd, bucketEndIndex);

                for (auto it = bucketBegin; it != bucketEnd; ++it) {
                    if (it == bucketBegin) {
                        mediumDistribution.push_back(it->first);
                    }

                    auto* node = it->second;
                    auto newTokenCount = GetTokenCountFromBucketNumber(bucketCount - i - 1);

                    setNodeTokenCount(node, mediumIndex, newTokenCount);
                }
            }

            nodesByTotalSpace.clear();
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "CRP tokens redistributed (Distribution: {%v})",
            MakeFormattableView(
                ConsistentReplicaPlacementTokenDistribution_,
                [&] (TStringBuilderBase* builder, const auto& pair) {
                    builder->AppendFormat("%v: %v", pair.first, pair.second);
                }));
    }

    int EstimateNodeConsistentReplicaPlacementTokenCount(TNode* node, int mediumIndex) const
    {
        auto it = ConsistentReplicaPlacementTokenDistribution_.find(mediumIndex);
        if (it == ConsistentReplicaPlacementTokenDistribution_.end() || it->second.empty()) {
            // Either this is a first node to be placed with this medium or the
            // distribution has not been recomputed yet (which happens periodically).
            // In any case, it's too early to bother with any balancing.
            auto bucket = GetDynamicConfig()->ConsistentReplicaPlacement->TokenDistributionBucketCount / 2;
            return GetTokenCountFromBucketNumber(bucket);
        }

        auto& mediumDistribution = it->second;

        auto nodeTotalSpace = GetOrCrash(node->TotalSpace(), mediumIndex);
        YT_VERIFY(nodeTotalSpace != 0);

        auto bucket = 0;
        // NB: binary search could've been used here, but the distribution is very small.
        for (auto it = mediumDistribution.rbegin(); it != mediumDistribution.rend(); ++it) {
            if (nodeTotalSpace <= *it) {
                break;
            }
            ++bucket;
        }
        return GetTokenCountFromBucketNumber(bucket);
    }

    int GetTokenCountFromBucketNumber(int bucket) const
    {
        const auto& config = GetDynamicConfig()->ConsistentReplicaPlacement;
        return std::max<int>(1, (bucket + 1) * config->TokensPerNode);
    }

    void HydraConfirmChunkListsRequisitionTraverseFinished(NProto::TReqConfirmChunkListsRequisitionTraverseFinished* request)
    {
        auto chunkListIds = FromProto<std::vector<TChunkListId>>(request->chunk_list_ids());

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Confirming finished chunk lists requisition traverse (ChunkListIds: %v)",
            chunkListIds);

        for (auto chunkListId : chunkListIds) {
            auto* chunkList = FindChunkList(chunkListId);
            if (!chunkList) {
                YT_LOG_ALERT("Chunk list is missing during requisition traverse finish confirmation (ChunkListId: %v)",
                    chunkListId);
                continue;
            }

            auto it = ChunkListsAwaitingRequisitionTraverse_.find(chunkList);
            if (it == ChunkListsAwaitingRequisitionTraverse_.end()) {
                YT_LOG_ALERT("Chunk list does not hold an additional strong ref during requisition traverse finish confirmation (ChunkListId: %v)",
                    chunkListId);
                continue;
            }

            ChunkListsAwaitingRequisitionTraverse_.erase(it);
        }
    }

    void HydraUpdateChunkRequisition(NProto::TReqUpdateChunkRequisition* request)
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        // NB: Ordered map is a must to make the behavior deterministic.
        std::map<TCellTag, NProto::TReqUpdateChunkRequisition> crossCellRequestMap;
        auto getCrossCellRequest = [&] (const TChunk* chunk) -> NProto::TReqUpdateChunkRequisition& {
            auto cellTag = chunk->GetNativeCellTag();
            auto it = crossCellRequestMap.find(cellTag);
            if (it == crossCellRequestMap.end()) {
                it = crossCellRequestMap.emplace(cellTag, NProto::TReqUpdateChunkRequisition()).first;
                it->second.set_cell_tag(multicellManager->GetCellTag());
            }
            return it->second;
        };

        auto local = request->cell_tag() == multicellManager->GetCellTag();
        int cellIndex = local ? -1 : multicellManager->GetRegisteredMasterCellIndex(request->cell_tag());

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto* requisitionRegistry = GetChunkRequisitionRegistry();

        THashMap<TChunkRequisitionIndex, bool> durabilityRequiredCache;
        std::pair<TChunkRequisitionIndex, bool> lastDurabilityRequiredCacheEntry{EmptyChunkRequisitionIndex, false};
        auto isDurabilityRequired = [&] (TChunkRequisitionIndex requisitionIndex) {
            // Very fast path.
            if (requisitionIndex == lastDurabilityRequiredCacheEntry.first) {
                return lastDurabilityRequiredCacheEntry.second;
            }

            auto durabilityRequired = false;
            auto it = durabilityRequiredCache.find(requisitionIndex);
            // Fast path.
            if (it != durabilityRequiredCache.end()) {
                durabilityRequired = it->second;
            } else {
                auto replication = requisitionRegistry->GetReplication(requisitionIndex);
                durabilityRequired = replication.IsDurabilityRequired(this);
                EmplaceOrCrash(durabilityRequiredCache, requisitionIndex, durabilityRequired);
            }

            lastDurabilityRequiredCacheEntry = {requisitionIndex, durabilityRequired};
            return durabilityRequired;
        };

        auto setChunkRequisitionIndex = [&] (TChunk* chunk, TChunkRequisitionIndex requisitionIndex) {
            if (local) {
                chunk->SetLocalRequisitionIndex(requisitionIndex, requisitionRegistry, objectManager);
            } else {
                chunk->SetExternalRequisitionIndex(
                    cellIndex,
                    requisitionIndex,
                    requisitionRegistry,
                    objectManager);
            }

            auto aggregatedRequisitionIndex = chunk->GetAggregatedRequisitionIndex();
            if (!chunk->IsErasure() && !isDurabilityRequired(aggregatedRequisitionIndex)) {
                chunk->SetHistoricallyNonVital(true);
            }
        };

        const auto updates = TranslateChunkRequisitionUpdateRequest(request);

        // Below, we ref chunks' new requisitions and unref old ones. Such unreffing may
        // remove a requisition which may happen to be the new requisition of subsequent chunks.
        // To avoid such thrashing, ref everything here and unref it afterwards.
        for (const auto& update : updates) {
            requisitionRegistry->Ref(update.TranslatedRequisitionIndex);
        }

        for (const auto& update : updates) {
            auto* chunk = update.Chunk;
            auto newRequisitionIndex = update.TranslatedRequisitionIndex;

            if (!local && !chunk->IsExportedToCell(cellIndex)) {
                // The chunk has already been unexported from that cell.
                continue;
            }

            auto curRequisitionIndex = local ? chunk->GetLocalRequisitionIndex() : chunk->GetExternalRequisitionIndex(cellIndex);

            if (newRequisitionIndex == curRequisitionIndex) {
                continue;
            }

            if (chunk->IsForeign()) {
                setChunkRequisitionIndex(chunk, newRequisitionIndex);

                YT_ASSERT(local);
                auto& crossCellRequest = getCrossCellRequest(chunk);
                auto* crossCellUpdate = crossCellRequest.add_updates();
                ToProto(crossCellUpdate->mutable_chunk_id(), chunk->GetId());
                crossCellUpdate->set_chunk_requisition_index(newRequisitionIndex);
            } else {
                const auto isChunkDiskSizeFinal = chunk->IsDiskSizeFinal();

                // NB: changing chunk's requisition may unreference and destroy the old requisition.
                // Worse yet, this may, in turn, weak-unreference some accounts, thus triggering
                // destruction of their control blocks (that hold strong and weak counters).
                // So be sure to use the old requisition *before* setting the new one.
                const auto& requisitionBefore = chunk->GetAggregatedRequisition(requisitionRegistry);
                auto replicationBefore = requisitionBefore.ToReplication();

                if (isChunkDiskSizeFinal) {
                    UpdateResourceUsage(chunk, -1, &requisitionBefore);
                }

                setChunkRequisitionIndex(chunk, newRequisitionIndex);

                // NB: don't use requisitionBefore after the change.

                if (isChunkDiskSizeFinal) {
                    UpdateResourceUsage(chunk, +1, nullptr);
                }

                OnChunkUpdated(chunk, replicationBefore);
            }
        }

        for (auto& [cellTag, request] : crossCellRequestMap) {
            FillChunkRequisitionDict(&request, *requisitionRegistry);
            multicellManager->PostToMaster(request, cellTag);
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Requesting to update requisition of imported chunks (CellTag: %v, Count: %v)",
                cellTag,
                request.updates_size());
        }

        for (const auto& update : updates) {
            requisitionRegistry->Unref(update.TranslatedRequisitionIndex, objectManager);
        }
    }

    void OnChunkUpdated(TChunk* chunk, const TChunkReplication& oldReplication)
    {
        if (chunk->HasConsistentReplicaPlacementHash()) {
            // NB: reacting on RF change is actually not necessary (CRP does not
            // rely on the actual RF of the chunk - instead, it uses a universal
            // upper bound). But enabling/disabling a medium still needs to be handled.
            ConsistentChunkPlacement_->RemoveChunk(chunk, oldReplication, /*missingOk*/ true);
            ConsistentChunkPlacement_->AddChunk(chunk);
        }

        ScheduleChunkRefresh(chunk);
    }

    void HydraRegisterChunkEndorsements(NProto::TReqRegisterChunkEndorsements* request)
    {
        constexpr static int MaxChunkIdsPerLogMessage = 100;

        std::vector<TChunkId> logQueue;
        auto maybeFlushLogQueue = [&] (bool force) {
            if (force || ssize(logQueue) >= MaxChunkIdsPerLogMessage) {
                YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                    "Registered endorsements for chunks (ChunkIds: %v)",
                    logQueue);
                logQueue.clear();
            }
        };

        for (const auto& protoChunkId : request->chunk_ids()) {
            auto chunkId = FromProto<TChunkId>(protoChunkId);
            auto* chunk = FindChunk(chunkId);
            if (!IsObjectAlive(chunk)) {
                continue;
            }
            if (!chunk->GetEndorsementRequired()) {
                continue;
            }

            RegisterEndorsement(chunk);
            chunk->SetEndorsementRequired(false);

            logQueue.push_back(chunk->GetId());;
            maybeFlushLogQueue(false);
        }

        maybeFlushLogQueue(true);
    }

    struct TRequisitionUpdate
    {
        TChunk* Chunk;
        TChunkRequisitionIndex TranslatedRequisitionIndex;
    };

    std::vector<TRequisitionUpdate> TranslateChunkRequisitionUpdateRequest(NProto::TReqUpdateChunkRequisition* request)
    {
        // NB: this is necessary even for local requests as requisition indexes
        // in the request are different from those in the registry.
        auto translateRequisitionIndex = BuildChunkRequisitionIndexTranslator(*request);

        std::vector<TRequisitionUpdate> updates;
        updates.reserve(request->updates_size());

        for (const auto& update : request->updates()) {
            auto chunkId = FromProto<TChunkId>(update.chunk_id());
            auto* chunk = FindChunk(chunkId);
            if (!IsObjectAlive(chunk)) {
                continue;
            }

            auto newRequisitionIndex = translateRequisitionIndex(update.chunk_requisition_index());
            updates.emplace_back(TRequisitionUpdate{chunk, newRequisitionIndex});
        }

        return updates;
    }

    std::function<TChunkRequisitionIndex(TChunkRequisitionIndex)>
    BuildChunkRequisitionIndexTranslator(const NProto::TReqUpdateChunkRequisition& request)
    {
        THashMap<TChunkRequisitionIndex, TChunkRequisitionIndex> remoteToLocalIndexMap;
        remoteToLocalIndexMap.reserve(request.chunk_requisition_dict_size());
        for (const auto& pair : request.chunk_requisition_dict()) {
            auto remoteIndex = pair.index();

            TChunkRequisition requisition;
            FromProto(&requisition, pair.requisition(), Bootstrap_->GetSecurityManager());
            auto localIndex = ChunkRequisitionRegistry_.GetOrCreate(requisition, Bootstrap_->GetObjectManager());

            YT_VERIFY(remoteToLocalIndexMap.emplace(remoteIndex, localIndex).second);
        }

        return [remoteToLocalIndexMap = std::move(remoteToLocalIndexMap)] (TChunkRequisitionIndex remoteIndex) {
            // The remote side must provide a dictionary entry for every index it sends us.
            return GetOrCrash(remoteToLocalIndexMap, remoteIndex);
        };
    }

    void HydraExportChunks(const TCtxExportChunksPtr& /*context*/, TReqExportChunks* request, TRspExportChunks* response)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* transaction = transactionManager->GetTransactionOrThrow(transactionId);
        if (transaction->GetPersistentState() != ETransactionState::Active) {
            transaction->ThrowInvalidState();
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        std::vector<TChunkId> chunkIds;
        for (const auto& exportData : request->chunks()) {
            auto chunkId = FromProto<TChunkId>(exportData.id());
            auto* chunk = GetChunkOrThrow(chunkId);

            if (chunk->IsForeign()) {
                THROW_ERROR_EXCEPTION("Cannot export a foreign chunk %v", chunkId);
            }

            auto cellTag = exportData.destination_cell_tag();
            if (!multicellManager->IsRegisteredMasterCell(cellTag)) {
                THROW_ERROR_EXCEPTION("Cell %v is not registered");
            }

            transactionManager->ExportObject(transaction, chunk, cellTag);

            if (response) {
                auto* importData = response->add_chunks();
                ToProto(importData->mutable_id(), chunkId);

                auto* chunkInfo = importData->mutable_info();
                chunkInfo->set_disk_space(chunk->GetDiskSpace());

                ToProto(importData->mutable_meta(), chunk->ChunkMeta());

                importData->set_erasure_codec(static_cast<int>(chunk->GetErasureCodec()));
            }

            chunkIds.push_back(chunk->GetId());
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Chunks exported (TransactionId: %v, ChunkIds: %v)",
            transactionId,
            chunkIds);
    }

    void HydraImportChunks(const TCtxImportChunksPtr& /*context*/, TReqImportChunks* request, TRspImportChunks* /*response*/)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* transaction = transactionManager->GetTransactionOrThrow(transactionId);

        if (transaction->GetPersistentState() != ETransactionState::Active) {
            transaction->ThrowInvalidState();
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        std::vector<TChunkId> chunkIds;
        for (auto& importData : *request->mutable_chunks()) {
            auto chunkId = FromProto<TChunkId>(importData.id());
            if (CellTagFromId(chunkId) == multicellManager->GetCellTag()) {
                THROW_ERROR_EXCEPTION("Cannot import a native chunk %v", chunkId);
            }

            auto* chunk = ChunkMap_.Find(chunkId);
            if (!chunk) {
                chunk = DoCreateChunk(chunkId);
                chunk->SetForeign();
                chunk->Confirm(importData.info(), importData.meta());
                chunk->SetErasureCodec(NErasure::ECodec(importData.erasure_codec()));
                YT_VERIFY(ForeignChunks_.insert(chunk).second);
            }

            transactionManager->ImportObject(transaction, chunk);

            chunkIds.push_back(chunk->GetId());
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Chunks imported (TransactionId: %v, ChunkIds: %v)",
            transactionId,
            chunkIds);
    }

    void HydraUnstageExpiredChunks(NProto::TReqUnstageExpiredChunks* request) noexcept
    {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();

        for (const auto& protoId : request->chunk_ids()) {
            auto chunkId = FromProto<TChunkId>(protoId);
            auto* chunk = FindChunk(chunkId);
            if (!IsObjectAlive(chunk)) {
                continue;
            }

            if (!chunk->IsStaged()) {
                continue;
            }

            if (chunk->IsConfirmed()) {
                continue;
            }

            transactionManager->UnstageObject(chunk->GetStagingTransaction(), chunk, false /*recursive*/);

            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Unstaged expired chunk (ChunkId: %v)",
                chunkId);
        }
    }

    void HydraExecuteBatch(
        const TCtxExecuteBatchPtr& /*context*/,
        TReqExecuteBatch* request,
        TRspExecuteBatch* response)
    {
        auto executeSubrequests = [&] (
            auto* subrequests,
            auto* subresponses,
            auto handler,
            const char* errorMessage)
        {
            for (auto& subrequest : *subrequests) {
                auto* subresponse = subresponses ? subresponses->Add() : nullptr;
                try {
                    (this->*handler)(&subrequest, subresponse);
                } catch (const std::exception& ex) {
                    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), TError(errorMessage) << ex);
                    if (subresponse) {
                        ToProto(subresponse->mutable_error(), TError(ex));
                    }
                }
            }
        };

        executeSubrequests(
            request->mutable_create_chunk_subrequests(),
            response ? response->mutable_create_chunk_subresponses() : nullptr,
            &TChunkManager::ExecuteCreateChunkSubrequest,
            "Error creating chunk");

        executeSubrequests(
            request->mutable_confirm_chunk_subrequests(),
            response ? response->mutable_confirm_chunk_subresponses() : nullptr,
            &TChunkManager::ExecuteConfirmChunkSubrequest,
            "Error confirming chunk");

        executeSubrequests(
            request->mutable_seal_chunk_subrequests(),
            response ? response->mutable_seal_chunk_subresponses() : nullptr,
            &TChunkManager::ExecuteSealChunkSubrequest,
            "Error sealing chunk");

        executeSubrequests(
            request->mutable_create_chunk_lists_subrequests(),
            response ? response->mutable_create_chunk_lists_subresponses() : nullptr,
            &TChunkManager::ExecuteCreateChunkListsSubrequest,
            "Error creating chunk lists");

        executeSubrequests(
            request->mutable_unstage_chunk_tree_subrequests(),
            response ? response->mutable_unstage_chunk_tree_subresponses() : nullptr,
            &TChunkManager::ExecuteUnstageChunkTreeSubrequest,
            "Error unstaging chunk tree");

        executeSubrequests(
            request->mutable_attach_chunk_trees_subrequests(),
            response ? response->mutable_attach_chunk_trees_subresponses() : nullptr,
            &TChunkManager::ExecuteAttachChunkTreesSubrequest,
            "Error attaching chunk trees");
    }

    void HydraCreateChunk(
        const TCtxCreateChunkPtr& /*context*/,
        TReqCreateChunk* request,
        TRspCreateChunk* response)
    {
        ExecuteCreateChunkSubrequest(request, response);
    }

    void HydraConfirmChunk(
        const TCtxConfirmChunkPtr& /*context*/,
        TReqConfirmChunk* request,
        TRspConfirmChunk* response)
    {
        ExecuteConfirmChunkSubrequest(request, response);
    }

    void HydraSealChunk(
        const TCtxSealChunkPtr& /*context*/,
        TReqSealChunk* request,
        TRspSealChunk* response)
    {
        ExecuteSealChunkSubrequest(request, response);
    }

    void HydraCreateChunkLists(
        const TCtxCreateChunkListsPtr& /*context*/,
        TReqCreateChunkLists* request,
        TRspCreateChunkLists* response)
    {
        ExecuteCreateChunkListsSubrequest(request, response);
    }

    void HydraUnstageChunkTree(
        const TCtxUnstageChunkTreePtr& /*context*/,
        TReqUnstageChunkTree* request,
        TRspUnstageChunkTree* response)
    {
        ExecuteUnstageChunkTreeSubrequest(request, response);
    }

    void HydraAttachChunkTrees(
        const TCtxAttachChunkTreesPtr& /*context*/,
        TReqAttachChunkTrees* request,
        TRspAttachChunkTrees* response)
    {
        ExecuteAttachChunkTreesSubrequest(request, response);
    }

    void ExecuteCreateChunkSubrequest(
        TReqCreateChunk* subrequest,
        TRspCreateChunk* subresponse)
    {
        YT_VERIFY(HasMutationContext());

        auto chunkType = CheckedEnumCast<EObjectType>(subrequest->type());
        bool isErasure = IsErasureChunkType(chunkType);
        bool isJournal = IsJournalChunkType(chunkType);
        auto erasureCodecId = isErasure ? CheckedEnumCast<NErasure::ECodec>(subrequest->erasure_codec()) : NErasure::ECodec::None;
        int readQuorum = isJournal ? subrequest->read_quorum() : 0;
        int writeQuorum = isJournal ? subrequest->write_quorum() : 0;

        i64 replicaLagLimit = 0;
        // COMPAT(gritukan)
        if (isJournal) {
            if (subrequest->has_replica_lag_limit()) {
                replicaLagLimit = subrequest->replica_lag_limit();
            } else {
                replicaLagLimit = MaxReplicaLagLimit;
            }
        }

        const auto& mediumName = subrequest->medium_name();
        auto* medium = GetMediumByNameOrThrow(mediumName);
        int mediumIndex = medium->GetIndex();

        auto replicationFactor = isErasure ? 1 : subrequest->replication_factor();
        ValidateReplicationFactor(replicationFactor);

        auto transactionId = FromProto<TTransactionId>(subrequest->transaction_id());
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* transaction = transactionManager->GetTransactionOrThrow(transactionId);

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* account = securityManager->GetAccountByNameOrThrow(subrequest->account(), true /*activeLifeStageOnly*/);

        auto overlayed = subrequest->overlayed();
        auto consistentReplicaPlacementHash = subrequest->consistent_replica_placement_hash();

        if (subrequest->validate_resource_usage_increase()) {
            auto resourceUsageIncrease = TClusterResources()
                .SetChunkCount(1)
                .SetMediumDiskSpace(mediumIndex, 1)
                .SetDetailedMasterMemory(EMasterMemoryType::Chunks, 1);
            securityManager->ValidateResourceUsageIncrease(account, resourceUsageIncrease);
        }

        TChunkList* chunkList = nullptr;
        if (subrequest->has_chunk_list_id()) {
            auto chunkListId = FromProto<TChunkListId>(subrequest->chunk_list_id());
            chunkList = GetChunkListOrThrow(chunkListId);
            if (!overlayed) {
                chunkList->ValidateLastChunkSealed();
            }
            chunkList->ValidateUniqueAncestors();
        }

        auto hintId = FromProto<TChunkId>(subrequest->chunk_id());

        // NB: Once the chunk is created, no exceptions could be thrown.
        auto* chunk = CreateChunk(
            transaction,
            chunkList,
            chunkType,
            account,
            replicationFactor,
            erasureCodecId,
            medium,
            readQuorum,
            writeQuorum,
            subrequest->movable(),
            subrequest->vital(),
            overlayed,
            consistentReplicaPlacementHash,
            replicaLagLimit,
            hintId);

        if (chunk->HasConsistentReplicaPlacementHash()) {
            ConsistentChunkPlacement_->AddChunk(chunk);
        }

        if (subresponse) {
            auto sessionId = TSessionId(chunk->GetId(), mediumIndex);
            ToProto(subresponse->mutable_session_id(), sessionId);
        }
    }

    void ExecuteConfirmChunkSubrequest(
        TReqConfirmChunk* subrequest,
        TRspConfirmChunk* subresponse)
    {
        YT_VERIFY(HasMutationContext());

        YT_VERIFY(!subrequest->replicas().empty() || !subrequest->legacy_replicas().empty());

        const auto& config = GetDynamicConfig();

        // COMPAT(kvk1920)
        if (config->EnableMoreChunkConfirmationChecks) {
            // Seems like a bug on client's side.
            if (subrequest->location_uuids_supported() && subrequest->replicas().empty()) {
                THROW_ERROR_EXCEPTION(
                    "Chunk confirmation request supports location uuid "
                    "but doesn't have any replica with location uuid");
            }
        }

        // COMPAT(kvk1920)
        if (!config->EnableChunkConfirmationWithoutLocationUuid) {
            if (!subrequest->location_uuids_supported()) {
                THROW_ERROR_EXCEPTION("Chunk confirmation without location uuids is forbidden");
            }

            if (subrequest->replicas().empty()) {
                THROW_ERROR_EXCEPTION("Attempted to confirm chunk with only legacy replicas");
            }
        } else {
            const auto& configManager = Bootstrap_->GetConfigManager();
            const auto& clusterConfig = configManager->GetConfig();
            const auto& nodeTrackerConfig = clusterConfig->NodeTracker;

            THROW_ERROR_EXCEPTION_IF(nodeTrackerConfig->EnableRealChunkLocations,
                "Chunk confirmation without location uuids is not allowed after "
                "real chunk locations have been enabled");
        }

        auto replicas = FromProto<TChunkReplicaWithLocationList>(subrequest->replicas());

        // COMPAT(kvk1920)
        if (!subrequest->location_uuids_supported()) {
            auto legacyReplicas = FromProto<TChunkReplicaWithMediumList>(subrequest->legacy_replicas());

            const auto& nodeTracker = Bootstrap_->GetNodeTracker();
            for (auto replica : legacyReplicas) {
                auto* node = nodeTracker->FindNode(replica.GetNodeId());
                if (!node) {
                    continue;
                }

                THROW_ERROR_EXCEPTION_UNLESS(node->UseImaginaryChunkLocations(),
                    "Cannot confirm chunk without location uuids after real chunk locations have been enabled");

                replicas.emplace_back(replica, TChunkLocationUuid());
            }
        }

        auto chunkId = FromProto<TChunkId>(subrequest->chunk_id());
        auto* chunk = GetChunkOrThrow(chunkId);

        ConfirmChunk(
            chunk,
            replicas,
            subrequest->chunk_info(),
            subrequest->chunk_meta());

        if (subresponse && subrequest->request_statistics()) {
            *subresponse->mutable_statistics() = chunk->GetStatistics().ToDataStatistics();
        }
    }

    void ExecuteSealChunkSubrequest(
        TReqSealChunk* subrequest,
        TRspSealChunk* /*subresponse*/)
    {
        YT_VERIFY(HasMutationContext());

        auto chunkId = FromProto<TChunkId>(subrequest->chunk_id());
        auto* chunk = GetChunkOrThrow(chunkId);

        const auto& info = subrequest->info();
        SealChunk(chunk, info);
    }

    void ExecuteCreateChunkListsSubrequest(
        TReqCreateChunkLists* subrequest,
        TRspCreateChunkLists* subresponse)
    {
        YT_VERIFY(HasMutationContext());

        auto transactionId = FromProto<TTransactionId>(subrequest->transaction_id());
        int count = subrequest->count();

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* transaction = transactionManager->GetTransactionOrThrow(transactionId);

        std::vector<TChunkListId> chunkListIds;
        chunkListIds.reserve(count);
        for (int index = 0; index < count; ++index) {
            auto* chunkList = DoCreateChunkList(EChunkListKind::Static);
            StageChunkList(chunkList, transaction, nullptr);
            transactionManager->StageObject(transaction, chunkList);
            ToProto(subresponse->add_chunk_list_ids(), chunkList->GetId());
            chunkListIds.push_back(chunkList->GetId());
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
            "Chunk lists created (ChunkListIds: %v, TransactionId: %v)",
            chunkListIds,
            transaction->GetId());
    }

    void ExecuteUnstageChunkTreeSubrequest(
        TReqUnstageChunkTree* subrequest,
        TRspUnstageChunkTree* /*subresponse*/)
    {
        YT_VERIFY(HasMutationContext());

        auto chunkTreeId = FromProto<TTransactionId>(subrequest->chunk_tree_id());
        auto recursive = subrequest->recursive();

        auto* chunkTree = GetChunkTreeOrThrow(chunkTreeId);
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->UnstageObject(chunkTree->GetStagingTransaction(), chunkTree, recursive);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Chunk tree unstaged (ChunkTreeId: %v, Recursive: %v)",
            chunkTreeId,
            recursive);
    }

    void ExecuteAttachChunkTreesSubrequest(
        TReqAttachChunkTrees* subrequest,
        TRspAttachChunkTrees* subresponse)
    {
        YT_VERIFY(HasMutationContext());

        auto parentId = FromProto<TChunkListId>(subrequest->parent_id());
        auto* parent = GetChunkListOrThrow(parentId);
        auto transactionId = subrequest->has_transaction_id()
            ? FromProto<TTransactionId>(subrequest->transaction_id())
            : NullTransactionId;

        std::vector<TChunkTree*> children;
        children.reserve(subrequest->child_ids_size());
        for (const auto& protoChildId : subrequest->child_ids()) {
            auto childId = FromProto<TChunkTreeId>(protoChildId);
            auto* child = GetChunkTreeOrThrow(childId);
            if (parent->GetKind() == EChunkListKind::SortedDynamicSubtablet ||
                parent->GetKind() == EChunkListKind::SortedDynamicTablet)
            {
                if (!IsBlobChunkType(child->GetType())) {
                    YT_LOG_ALERT("Attempted to attach chunk tree of unexpected type to a dynamic table "
                        "(ChunkTreeId: %v, Type: %v, ChunkListId: %v, ChunkListKind: %v)",
                        childId,
                        child->GetType(),
                        parent->GetId(),
                        parent->GetKind());
                    continue;
                }

                if (transactionId) {
                    // Bulk insert. Inserted chunks inherit transaction timestamp.
                    auto chunkView = CreateChunkView(
                        child->AsChunk(),
                        TChunkViewModifier().WithTransactionId(transactionId));
                    children.push_back(chunkView);
                } else {
                    // Remote copy. Inserted chunks preserve original timestamps.
                    YT_VERIFY(parent->GetKind() == EChunkListKind::SortedDynamicTablet);
                    children.push_back(child);
                }
            } else {
                children.push_back(child);
            }
            // YT-6542: Make sure we never attach a chunk list to its parent more than once.
            if (child->GetType() == EObjectType::ChunkList) {
                auto* chunkListChild = child->AsChunkList();
                for (auto* someParent : chunkListChild->Parents()) {
                    if (someParent == parent) {
                        THROW_ERROR_EXCEPTION("Chunk list %v already has %v as its parent",
                            chunkListChild->GetId(),
                            parent->GetId());
                    }
                }
            }
        }

        AttachToChunkList(parent, children);

        if (subrequest->request_statistics()) {
            *subresponse->mutable_statistics() = parent->Statistics().ToDataStatistics();
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Chunk trees attached (ParentId: %v, ChildIds: %v, TransactionId: %v)",
            parentId,
            MakeFormattableView(children, TObjectIdFormatter()),
            transactionId);
    }

    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        ChunkMap_.SaveKeys(context);
        ChunkListMap_.SaveKeys(context);
        MediumMap_.SaveKeys(context);
        ChunkViewMap_.SaveKeys(context);
        DynamicStoreMap_.SaveKeys(context);
    }

    void SaveHistogramValues(
        NCellMaster::TSaveContext& context,
        const THistogramSnapshot& snapshot) const
    {
        Save(context, snapshot.Bounds);
        Save(context, snapshot.Values);
    }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        using NYT::Save;

        ChunkMap_.SaveValuesParallel(context);
        ChunkListMap_.SaveValuesParallel(context);
        MediumMap_.SaveValues(context);
        Save(context, ChunkRequisitionRegistry_);
        Save(context, ChunkListsAwaitingRequisitionTraverse_);
        ChunkViewMap_.SaveValues(context);
        DynamicStoreMap_.SaveValues(context);

        Save(context, ConsistentReplicaPlacementTokenDistribution_);

        SaveHistogramValues(context, ChunkRowCountHistogram_.GetSnapshot());
        SaveHistogramValues(context, ChunkCompressedDataSizeHistogram_.GetSnapshot());
        SaveHistogramValues(context, ChunkUncompressedDataSizeHistogram_.GetSnapshot());
        SaveHistogramValues(context, ChunkDataWeightHistogram_.GetSnapshot());
    }

    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        ChunkMap_.LoadKeys(context);
        ChunkListMap_.LoadKeys(context);
        MediumMap_.LoadKeys(context);
        ChunkViewMap_.LoadKeys(context);
        DynamicStoreMap_.LoadKeys(context);
    }

    void LoadHistogramValues(
        NCellMaster::TLoadContext& context,
        TGaugeHistogram& histogram)
    {
        THistogramSnapshot snapshot;
        Load(context, snapshot.Bounds);
        Load(context, snapshot.Values);
        histogram.LoadSnapshot(snapshot);
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        using NYT::Load;

        ChunkMap_.LoadValues(context);
        ChunkListMap_.LoadValues(context);
        MediumMap_.LoadValues(context);

        // COMPAT(kvk1920): move to OnAfterSnapshotLoaded
        for (auto [mediumId, medium] : MediumMap_) {
            RegisterMedium(medium);
        }

        Load(context, ChunkRequisitionRegistry_);
        Load(context, ChunkListsAwaitingRequisitionTraverse_);
        ChunkViewMap_.LoadValues(context);
        DynamicStoreMap_.LoadValues(context);

        Load(context, ConsistentReplicaPlacementTokenDistribution_);

        LoadHistogramValues(context, ChunkRowCountHistogram_);
        LoadHistogramValues(context, ChunkCompressedDataSizeHistogram_);
        LoadHistogramValues(context, ChunkUncompressedDataSizeHistogram_);
        LoadHistogramValues(context, ChunkDataWeightHistogram_);

        // COMPAT(kvk1920)
        NeedTransformOldReplicas_ = context.GetVersion() < EMasterReign::ChunkLocationInReplica;
    }

    void OnBeforeSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnBeforeSnapshotLoaded();
    }

    void MaybeTransformOldReplicas()
    {
        if (!NeedTransformOldReplicas_) {
            return;
        }

        YT_LOG_INFO("Started transforming old replicas");
        for (auto [chunkId, chunk] : ChunkMap_) {
            chunk->TransformOldReplicas();
        }
        YT_LOG_INFO("Finished transforming old replicas");
    }

    void OnAfterSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        // Populate nodes' chunk replica sets.
        // Compute chunk replica count.

        // COMPAT(kvk1920)
        MaybeTransformOldReplicas();

        YT_LOG_INFO("Started initializing chunks");

        for (auto [chunkId, chunk] : ChunkMap_) {
            RegisterChunk(chunk);

            if (NeedRecomputeChunkWeightStatisticsHistogram_) {
                UpdateChunkWeightStatisticsHistogram(chunk, /*add*/ true);
            }

            for (auto replica : chunk->StoredReplicas()) {
                TChunkPtrWithReplicaInfo chunkWithIndexes(
                    chunk,
                    replica.GetReplicaIndex(),
                    replica.GetReplicaState());
                replica.GetPtr()->AddReplica(chunkWithIndexes);
                ++TotalReplicaCount_;
            }

            if (chunk->IsForeign()) {
                YT_VERIFY(ForeignChunks_.insert(chunk).second);
            }

            if (chunk->IsSequoia()) {
                ++SequoiaChunkCount_;
            }

            if (chunk->HasConsistentReplicaPlacementHash()) {
                ++CrpChunkCount_;
            }
        }

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        for (auto [id, node] : nodeTracker->Nodes()) {
            for (auto [chunk, revision] : node->ReplicaEndorsements()) {
                YT_VERIFY(!chunk->GetNodeWithEndorsement());
                chunk->SetNodeWithEndorsement(node);
            }
            EndorsementCount_ += ssize(node->ReplicaEndorsements());

            for (auto* location : node->ChunkLocations()) {
                DestroyedReplicaCount_ += std::ssize(location->DestroyedReplicas());
            }
        }

        InitBuiltins();

        for (auto [_, node] : nodeTracker->Nodes()) {
            if (node->IsValidWriteTarget()) {
                ConsistentChunkPlacement_->AddNode(node);
            }
        }
        // NB: chunks are added after nodes!
        for (auto [_, chunk] : ChunkMap_) {
            if (chunk->HasConsistentReplicaPlacementHash()) {
                ConsistentChunkPlacement_->AddChunk(chunk);
            }
        }

        ChunkPlacement_->Initialize();

        YT_LOG_INFO("Finished initializing chunks");
    }


    void Clear() override
    {
        TMasterAutomatonPart::Clear();

        for (auto& chunkList : BlobChunks_) {
            chunkList.Clear();
        }
        for (auto& chunkList : JournalChunks_) {
            chunkList.Clear();
        }

        ChunkMap_.Clear();
        ChunkListMap_.Clear();
        ChunkViewMap_.Clear();
        ForeignChunks_.clear();
        TotalReplicaCount_ = 0;

        ChunkRequisitionRegistry_.Clear();

        ConsistentChunkPlacement_->Clear();
        ChunkPlacement_->Clear();

        ChunkListsAwaitingRequisitionTraverse_.clear();

        MediumMap_.Clear();
        NameToMediumMap_.clear();
        IndexToMediumMap_ = std::vector<TMedium*>(MaxMediumCount, nullptr);
        UsedMediumIndexes_.reset();

        ChunksCreated_ = 0;
        ChunksDestroyed_ = 0;
        ChunkReplicasAdded_ = 0;
        ChunkReplicasRemoved_ = 0;
        ChunkViewsCreated_ = 0;
        ChunkViewsDestroyed_ = 0;
        ChunkListsCreated_ = 0;
        ChunkListsDestroyed_ = 0;

        SequoiaChunkCount_ = 0;
        CrpChunkCount_ = 0;

        ImmediateAllyReplicasAnnounced_ = 0;
        DelayedAllyReplicasAnnounced_ = 0;
        LazyAllyReplicasAnnounced_ = 0;
        EndorsementsAdded_ = 0;
        EndorsementsConfirmed_ = 0;
        EndorsementCount_ = 0;

        DestroyedReplicaCount_ = 0;

        ChunkRowCountHistogram_.Reset();
        ChunkCompressedDataSizeHistogram_.Reset();
        ChunkUncompressedDataSizeHistogram_.Reset();
        ChunkDataWeightHistogram_.Reset();

        DefaultStoreMedium_ = nullptr;

        NeedRecomputeChunkWeightStatisticsHistogram_ = false;
    }

    void SetZeroState() override
    {
        TMasterAutomatonPart::SetZeroState();

        InitBuiltins();
        ConsistentChunkPlacement_->Clear();
        ChunkPlacement_->Clear();
    }


    void InitBuiltins()
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        const auto& objectManager = Bootstrap_->GetObjectManager();

        // Chunk requisition registry
        ChunkRequisitionRegistry_.EnsureBuiltinRequisitionsInitialized(
            securityManager->GetChunkWiseAccountingMigrationAccount(),
            objectManager);

        // Media

        // default
        if (EnsureBuiltinMediumInitialized(
            DefaultStoreMedium_,
            DefaultStoreMediumId_,
            DefaultStoreMediumIndex,
            DefaultStoreMediumName))
        {
            DefaultStoreMedium_->Acd().AddEntry(TAccessControlEntry(
                ESecurityAction::Allow,
                securityManager->GetUsersGroup(),
                EPermission::Use));
        }
    }

    bool EnsureBuiltinMediumInitialized(
        TMedium*& medium,
        TMediumId id,
        int mediumIndex,
        const TString& name)
    {
        if (medium) {
            return false;
        }
        medium = FindMedium(id);
        if (medium) {
            return false;
        }
        medium = DoCreateMedium(
            id,
            mediumIndex,
            name,
            /*transient*/ false,
            /*priority*/ std::nullopt);
        return true;
    }

    void RecomputeStatistics(TChunkList* chunkList)
    {
        YT_VERIFY(chunkList->GetKind() != EChunkListKind::OrderedDynamicTablet);

        auto& statistics = chunkList->Statistics();
        auto oldStatistics = statistics;
        statistics = TChunkTreeStatistics{};

        auto& cumulativeStatistics = chunkList->CumulativeStatistics();
        cumulativeStatistics.Clear();
        if (chunkList->HasModifyableCumulativeStatistics()) {
            cumulativeStatistics.DeclareModifiable();
        } else if (chunkList->HasAppendableCumulativeStatistics()) {
            cumulativeStatistics.DeclareAppendable();
        } else {
            YT_ABORT();
        }

        for (auto* child : chunkList->Children()) {
            YT_VERIFY(child);
            auto childStatistics = GetChunkTreeStatistics(child);
            statistics.Accumulate(childStatistics);
            if (chunkList->HasCumulativeStatistics()) {
                cumulativeStatistics.PushBack(TCumulativeStatisticsEntry{childStatistics});
            }
        }

        ++statistics.Rank;
        ++statistics.ChunkListCount;

        if (statistics != oldStatistics) {
            YT_LOG_DEBUG("Chunk list statistics changed (ChunkList: %v, OldStatistics: %v, NewStatistics: %v)",
                chunkList->GetId(),
                oldStatistics,
                statistics);
        }

        if (!chunkList->Children().empty() && chunkList->HasCumulativeStatistics()) {
            auto ultimateCumulativeEntry = chunkList->CumulativeStatistics().Back();
            if (ultimateCumulativeEntry != TCumulativeStatisticsEntry{statistics}) {
                YT_LOG_FATAL(
                    "Chunk list cumulative statistics do not match statistics "
                    "(ChunkListId: %v, Statistics: %v, UltimateCumulativeEntry: %v)",
                    chunkList->GetId(),
                    statistics,
                    ultimateCumulativeEntry);
            }
        }
    }

    // Fix for YT-10619.
    void RecomputeOrderedTabletCumulativeStatistics(TChunkList* chunkList)
    {
        YT_VERIFY(chunkList->GetKind() == EChunkListKind::OrderedDynamicTablet);

        auto getChildStatisticsEntry = [] (TChunkTree* child) {
            return child
                ? TCumulativeStatisticsEntry{child->AsChunk()->GetStatistics()}
                : TCumulativeStatisticsEntry(0 /*RowCount*/, 1 /*ChunkCount*/, 0 /*DataSize*/);
        };

        TCumulativeStatisticsEntry beforeFirst{chunkList->Statistics()};
        for (auto* child : chunkList->Children()) {
            auto childEntry = getChildStatisticsEntry(child);
            beforeFirst = beforeFirst - childEntry;
        }

        YT_VERIFY(chunkList->HasTrimmableCumulativeStatistics());
        auto& cumulativeStatistics = chunkList->CumulativeStatistics();
        cumulativeStatistics.Clear();
        cumulativeStatistics.DeclareTrimmable();
        // Replace default-constructed auxiliary 'before-first' entry.
        cumulativeStatistics.PushBack(beforeFirst);
        cumulativeStatistics.TrimFront(1);

        auto currentStatistics = beforeFirst;
        for (auto* child : chunkList->Children()) {
            auto childEntry = getChildStatisticsEntry(child);
            currentStatistics = currentStatistics + childEntry;
            cumulativeStatistics.PushBack(childEntry);
        }

        YT_VERIFY(currentStatistics == TCumulativeStatisticsEntry{chunkList->Statistics()});
        auto ultimateCumulativeEntry = chunkList->CumulativeStatistics().Empty()
            ? chunkList->CumulativeStatistics().GetPreviousSum(0)
            : chunkList->CumulativeStatistics().Back();
        if (ultimateCumulativeEntry != TCumulativeStatisticsEntry{chunkList->Statistics()}) {
            YT_LOG_FATAL(
                "Chunk list cumulative statistics do not match statistics "
                "(ChunkListId: %v, Statistics: %v, UltimateCumulativeEntry: %v)",
                chunkList->GetId(),
                chunkList->Statistics(),
                ultimateCumulativeEntry);
        }
    }

    // NB(ifsmirnov): This code was used 3 years ago as an ancient COMPAT but might soon
    // be reused when cumulative stats for dyntables come.
    void RecomputeStatistics()
    {
        YT_LOG_INFO("Started recomputing statistics");

        auto visitMark = TChunkList::GenerateVisitMark();

        std::vector<TChunkList*> chunkLists;
        std::vector<std::pair<TChunkList*, int>> stack;

        auto visit = [&] (TChunkList* chunkList) {
            if (chunkList->GetVisitMark() != visitMark) {
                chunkList->SetVisitMark(visitMark);
                stack.emplace_back(chunkList, 0);
            }
        };

        // Sort chunk lists in topological order
        for (auto [chunkListId, chunkList] : ChunkListMap_) {
            visit(chunkList);

            while (!stack.empty()) {
                chunkList = stack.back().first;
                int childIndex = stack.back().second;
                int childCount = chunkList->Children().size();

                if (childIndex == childCount) {
                    chunkLists.push_back(chunkList);
                    stack.pop_back();
                } else {
                    ++stack.back().second;
                    auto* child = chunkList->Children()[childIndex];
                    if (child && child->GetType() == EObjectType::ChunkList) {
                        visit(child->AsChunkList());
                    }
                }
            }
        }

        // Recompute statistics
        for (auto* chunkList : chunkLists) {
            RecomputeStatistics(chunkList);
            auto& statistics = chunkList->Statistics();
            auto oldStatistics = statistics;
            statistics = TChunkTreeStatistics();
            int childCount = chunkList->Children().size();

            auto& cumulativeStatistics = chunkList->CumulativeStatistics();
            cumulativeStatistics.Clear();

            for (int childIndex = 0; childIndex < childCount; ++childIndex) {
                // TODO(ifsmirnov): think of it in context of nullptrs and cumulative statistics.
                auto* child = chunkList->Children()[childIndex];
                if (!child) {
                    continue;
                }

                TChunkTreeStatistics childStatistics;
                switch (child->GetType()) {
                    case EObjectType::Chunk:
                    case EObjectType::ErasureChunk:
                    case EObjectType::JournalChunk:
                    case EObjectType::ErasureJournalChunk:
                        childStatistics.Accumulate(child->AsChunk()->GetStatistics());
                        break;

                    case EObjectType::ChunkList:
                        childStatistics.Accumulate(child->AsChunkList()->Statistics());
                        break;

                    case EObjectType::ChunkView:
                        childStatistics.Accumulate(child->AsChunkView()->GetStatistics());
                        break;

                    default:
                        YT_ABORT();
                }

                if (childIndex + 1 < childCount && chunkList->HasCumulativeStatistics()) {
                    cumulativeStatistics.PushBack(TCumulativeStatisticsEntry{
                        childStatistics.LogicalRowCount,
                        childStatistics.LogicalChunkCount,
                        childStatistics.UncompressedDataSize
                    });
                }

                statistics.Accumulate(childStatistics);
            }

            ++statistics.Rank;
            ++statistics.ChunkListCount;

            if (statistics != oldStatistics) {
                YT_LOG_DEBUG("Chunk list statistics changed (ChunkList: %v, OldStatistics: %v, NewStatistics: %v)",
                    chunkList->GetId(),
                    oldStatistics,
                    statistics);
            }
        }

        YT_LOG_INFO("Finished recomputing statistics");
    }

    void OnRecoveryStarted() override
    {
        TMasterAutomatonPart::OnRecoveryStarted();

        BufferedProducer_->SetEnabled(false);
        CrpBufferedProducer_->SetEnabled(false);
    }

    void OnRecoveryComplete() override
    {
        TMasterAutomatonPart::OnRecoveryComplete();

        BufferedProducer_->SetEnabled(true);
        CrpBufferedProducer_->SetEnabled(true);

        TestingOnlyLastRecoveryTime_ = NProfiling::GetInstant();
    }

    void OnLeaderActive() override
    {
        TMasterAutomatonPart::OnLeaderActive();

        OnEpochStarted();

        ChunkReplicator_->OnLeadingStarted();

        ChunkSealer_->Start();

        {
            NProto::TReqConfirmChunkListsRequisitionTraverseFinished request;
            std::vector<TChunkListId> chunkListIds;
            for (const auto& chunkList : ChunkListsAwaitingRequisitionTraverse_) {
                ToProto(request.add_chunk_list_ids(), chunkList->GetId());
            }

            YT_LOG_INFO("Scheduling chunk lists requisition traverse confirmation (Count: %v)",
                request.chunk_list_ids_size());

            CreateConfirmChunkListsRequisitionTraverseFinishedMutation(request)
                ->CommitAndLog(Logger);
        }
    }

    void OnStopLeading() override
    {
        TMasterAutomatonPart::OnStopLeading();

        OnEpochFinished();

        ChunkReplicator_->OnLeadingFinished();

        ChunkSealer_->Stop();
    }

    void OnStartFollowing() override
    {
        TMasterAutomatonPart::OnStartFollowing();

        OnEpochStarted();
    }

    void OnStopFollowing() override
    {
        TMasterAutomatonPart::OnStopFollowing();

        OnEpochFinished();
    }

    void OnEpochStarted()
    {
        ChunkPlacement_->Initialize();

        ChunkReplicator_->OnEpochStarted();
    }

    void OnEpochFinished()
    {
        ChunkPlacement_->Clear();

        ChunkReplicator_->OnEpochFinished();
    }

    void RegisterChunk(TChunk* chunk)
    {
        GetAllChunksLinkedList(chunk).PushFront(chunk);
    }

    void UnregisterChunk(TChunk* chunk)
    {
        GetAllChunksLinkedList(chunk).Remove(chunk);
    }

    TIntrusiveLinkedList<TChunk, TChunkToLinkedListNode>& GetAllChunksLinkedList(TChunk* chunk)
    {
        auto shardIndex = chunk->GetShardIndex();
        return chunk->IsJournal() ? JournalChunks_[shardIndex] : BlobChunks_[shardIndex];
    }

    void AddChunkReplica(
        TChunkLocation* chunkLocation,
        const TMedium* medium,
        TChunkPtrWithReplicaInfo replica,
        EAddReplicaReason reason)
    {
        auto* chunk = replica.GetPtr();
        auto* node = chunkLocation->GetNode();
        auto nodeId = node->GetId();

        if (!chunkLocation->AddReplica(replica)) {
            return;
        }

        TChunkLocationPtrWithReplicaInfo chunkLocationWithReplicaInfo(
            chunkLocation,
            replica.GetReplicaIndex(),
            replica.GetReplicaState());

        bool approved = reason == EAddReplicaReason::FullHeartbeat ||
            reason == EAddReplicaReason::IncrementalHeartbeat;
        chunk->AddReplica(chunkLocationWithReplicaInfo, medium, approved);

        if (IsMutationLoggingEnabled()) {
            YT_LOG_EVENT(
                Logger,
                reason == EAddReplicaReason::FullHeartbeat ? NLogging::ELogLevel::Trace : NLogging::ELogLevel::Debug,
                "Chunk replica added (ChunkId: %v, NodeId: %v, Address: %v, Reason: %v)",
                replica.GetPtr()->GetId(),
                nodeId,
                node->GetDefaultAddress(),
                reason);
        }

        if (reason == EAddReplicaReason::IncrementalHeartbeat || reason == EAddReplicaReason::Confirmation) {
            ++ChunkReplicasAdded_;
        }

        ScheduleChunkRefresh(chunk);
        ScheduleChunkSeal(chunk);
    }

    void ApproveChunkReplica(
        TChunkLocation* location,
        TChunkPtrWithReplicaInfo chunkWithIndexes)
    {
        auto* chunk = chunkWithIndexes.GetPtr();
        auto* node = location->GetNode();
        TChunkLocationPtrWithReplicaInfo locationWithIndexes(
            location,
            chunkWithIndexes.GetReplicaIndex(),
            chunkWithIndexes.GetReplicaState());

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Chunk approved (NodeId: %v, Address: %v, ChunkId: %v)",
            node->GetId(),
            node->GetDefaultAddress(),
            chunkWithIndexes);

        location->ApproveReplica(chunkWithIndexes);
        chunk->ApproveReplica(locationWithIndexes);

        ScheduleChunkRefresh(chunk);
        ScheduleChunkSeal(chunk);
    }

    void RemoveChunkReplica(
        TChunkLocation* chunkLocation,
        TChunkPtrWithReplicaIndex replica,
        ERemoveReplicaReason reason,
        bool approved)
    {
        auto* chunk = replica.GetPtr();
        YT_VERIFY(chunk);
        auto* node = chunkLocation->GetNode();
        auto nodeId = node->GetId();
        TChunkLocationPtrWithReplicaIndex locationWithIndex(chunkLocation, replica.GetReplicaIndex());

        if (reason == ERemoveReplicaReason::IncrementalHeartbeat && !chunkLocation->HasReplica(replica)) {
            return;
        }

        chunk->RemoveReplica(locationWithIndex, approved);

        switch (reason) {
            case ERemoveReplicaReason::IncrementalHeartbeat:
            case ERemoveReplicaReason::ApproveTimeout:
            case ERemoveReplicaReason::ChunkDestroyed:
                chunkLocation->RemoveReplica(replica);
                ChunkReplicator_->OnReplicaRemoved(chunkLocation, replica, reason);
                break;
            case ERemoveReplicaReason::NodeDisposed:
                // Do nothing.
                break;
            default:
                YT_ABORT();
        }

        if (IsMutationLoggingEnabled()) {
            YT_LOG_EVENT(
                Logger,
                reason == ERemoveReplicaReason::NodeDisposed ||
                reason == ERemoveReplicaReason::ChunkDestroyed
                ? NLogging::ELogLevel::Trace : NLogging::ELogLevel::Debug,
                "Chunk replica removed (ChunkId: %v, Reason: %v, NodeId: %v, Address: %v)",
                replica.GetPtr()->GetId(),
                reason,
                nodeId,
                node->GetDefaultAddress());
        }

        ScheduleChunkRefresh(chunk);

        ++ChunkReplicasRemoved_;
    }


    static EChunkReplicaState GetAddedChunkReplicaState(
        TChunk* chunk,
        const TChunkAddInfo& chunkAddInfo)
    {
        if (chunk->IsJournal()) {
            if (chunkAddInfo.active()) {
                return EChunkReplicaState::Active;
            } else if (chunkAddInfo.sealed()) {
                return EChunkReplicaState::Sealed;
            } else {
                return EChunkReplicaState::Unsealed;
            }
        } else {
            return EChunkReplicaState::Generic;
        }
    }

    std::pair<TChunkLocation*, TMedium*> FindLocationAndMediumOnProcessChunk(
        TNode* node,
        TRealChunkLocation* realLocation,
        TChunkIdWithIndexes chunkIdWithIndexes,
        EChunkReplicaEventType reason)
    {
        if (node->UseImaginaryChunkLocations()) {
            auto* medium = FindMediumByIndex(chunkIdWithIndexes.MediumIndex);
            if (!IsObjectAlive(medium)) {
                YT_LOG_ALERT(
                    "Cannot process chunk event with unknown medium "
                    "(NodeId: %v, Address: %v, ChunkId: %v, MediumIndex: %v, ChunkEventType: %v)",
                    node->GetId(),
                    node->GetDefaultAddress(),
                    chunkIdWithIndexes,
                    reason);
                return {nullptr, nullptr};
            }
            auto* location = node->GetOrCreateImaginaryChunkLocation(chunkIdWithIndexes.MediumIndex);
            return {location, medium};
        }

        int mediumIndex = realLocation->GetEffectiveMediumIndex();
        auto* medium = FindMediumByIndex(mediumIndex);
        if (!IsObjectAlive(medium)) {
            YT_LOG_ALERT(
                "Cannot process chunk event with unknown medium "
                "(NodeId: %v, Address: %v, ChunkId: %v, MediumIndex: %v, ChunkEventType: %v)",
                node->GetId(),
                node->GetDefaultAddress(),
                chunkIdWithIndexes,
                mediumIndex,
                reason);
            return {nullptr, nullptr};
        };

        return {realLocation, medium};
    }

    TChunk* ProcessAddedChunk(
        TNode* node,
        const TCompactVector<TRealChunkLocation*, TypicalChunkLocationCount>& locationDirectory,
        const TChunkAddInfo& chunkAddInfo,
        bool incremental)
    {
        auto nodeId = node->GetId();
        auto chunkId = FromProto<TChunkId>(chunkAddInfo.chunk_id());
        auto chunkIdWithIndex = DecodeChunkId(chunkId);
        TChunkIdWithIndexes chunkIdWithIndexes(chunkIdWithIndex, chunkAddInfo.medium_index());

        auto [location, medium] = FindLocationAndMediumOnProcessChunk(
            node,
            locationDirectory[chunkAddInfo.location_index()],
            chunkIdWithIndexes,
            EChunkReplicaEventType::Add);

        if (!location || !medium) {
            return nullptr;
        }

        const auto* counters = incremental
            ? &GetIncrementalHeartbeatCounters(node)
            : nullptr;

        auto* chunk = FindChunk(chunkIdWithIndexes.Id);
        if (!IsObjectAlive(chunk)) {
            if (incremental) {
                counters->AddedDestroyedReplicas.Increment();
            }

            auto isUnknown = location->AddDestroyedReplica(chunkIdWithIndexes);
            if (isUnknown) {
                ++DestroyedReplicaCount_;
            }
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                "%v removal scheduled (NodeId: %v, Address: %v, ChunkId: %v)",
                isUnknown ? "Unknown chunk added," : "Destroyed chunk",
                nodeId,
                node->GetDefaultAddress(),
                chunkIdWithIndexes);
            return nullptr;
        }

        auto state = GetAddedChunkReplicaState(chunk, chunkAddInfo);
        TChunkPtrWithReplicaInfo chunkWithIndexes(chunk, chunkIdWithIndexes.ReplicaIndex, state);
        TChunkLocationPtrWithReplicaInfo locationWithIndexes(location, chunkIdWithIndexes.ReplicaIndex, state);

        if (location->HasUnapprovedReplica(TChunkPtrWithReplicaIndex(chunk, chunkIdWithIndexes.ReplicaIndex))) {
            if (incremental) {
                counters->ApprovedReplicas.Increment();
            }
            ApproveChunkReplica(location, chunkWithIndexes);
        } else {
            if (incremental) {
                counters->AddedReplicas.Increment();
            }
            AddChunkReplica(
                location,
                medium,
                chunkWithIndexes,
                incremental ? EAddReplicaReason::IncrementalHeartbeat : EAddReplicaReason::FullHeartbeat);
        }

        return chunk;
    }

    TChunk* ProcessRemovedChunk(
        TNode* node,
        const TCompactVector<TRealChunkLocation*, TypicalChunkLocationCount>& locationDirectory,
        const TChunkRemoveInfo& chunkInfo)
    {
        auto nodeId = node->GetId();
        auto chunkIdWithIndex = DecodeChunkId(FromProto<TChunkId>(chunkInfo.chunk_id()));

        auto [location, medium] = FindLocationAndMediumOnProcessChunk(
            node,
            locationDirectory[chunkInfo.location_index()],
            TChunkIdWithIndexes(chunkIdWithIndex, chunkInfo.medium_index()),
            EChunkReplicaEventType::Remove);

        if (!location || !medium) {
            return nullptr;
        }

        auto isDestroyed = location->RemoveDestroyedReplica(chunkIdWithIndex);
        if (isDestroyed) {
            --DestroyedReplicaCount_;
        }
        // NB: Chunk could already be a zombie but we still need to remove the replica.
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
            "%v replica removed (ChunkId: %v, Address: %v, NodeId: %v)",
            isDestroyed ? "Destroyed chunk" : "Chunk",
            chunkIdWithIndex,
            node->GetDefaultAddress(),
            nodeId);

        auto* chunk = FindChunk(chunkIdWithIndex.Id);
        if (!chunk) {
            return nullptr;
        }

        TChunkPtrWithReplicaIndex replica(
            chunk,
            chunkIdWithIndex.ReplicaIndex);
        YT_VERIFY(replica.GetPtr());
        bool approved = !location->HasUnapprovedReplica(replica);
        RemoveChunkReplica(
            location,
            replica,
            ERemoveReplicaReason::IncrementalHeartbeat,
            approved);

        return chunk;
    }


    void OnChunkSealed(TChunk* chunk)
    {
        YT_VERIFY(chunk->IsSealed());

        if (chunk->IsJournal()) {
            UpdateResourceUsage(chunk, +1);
        }

        // Journal hunk chunks require special treatment.
        if (chunk->GetSealable()) {
            const auto& tabletManager = Bootstrap_->GetTabletManager();
            tabletManager->OnHunkJournalChunkSealed(chunk);
            return;
        }

        auto parentCount = chunk->GetParentCount();
        if (parentCount == 0) {
            return;
        }
        if (parentCount > 1) {
            YT_LOG_ALERT("Improper number of parents of a sealed chunk (ChunkId: %v, ParentCount: %v)",
                chunk->GetId(),
                parentCount);
            return;
        }
        auto* chunkList = GetUniqueParent(chunk)->As<TChunkList>();

        // Go upwards and apply delta.
        auto statisticsDelta = chunk->GetStatistics();

        // NB: Journal row count is not a sum of chunk row counts since chunks may overlap.
        if (chunk->IsJournal()) {
            if (!chunkList->Parents().empty()) {
                YT_LOG_ALERT("Journal has a non-trivial chunk tree structure (ChunkId: %v, ChunkListId: %v, ParentCount: %v)",
                    chunk->GetId(),
                    chunkList->GetId(),
                    chunkList->Parents().size());
            }

            auto firstOverlayedRowIndex = chunk->GetFirstOverlayedRowIndex();

            const auto& statistics = chunkList->Statistics();
            YT_VERIFY(statistics.RowCount == statistics.LogicalRowCount);
            i64 oldJournalRowCount = statistics.RowCount;
            i64 newJournalRowCount = GetJournalRowCount(
                oldJournalRowCount,
                firstOverlayedRowIndex,
                chunk->GetRowCount());

            // NB: Last chunk can be nested into another.
            newJournalRowCount = std::max<i64>(newJournalRowCount, oldJournalRowCount);

            i64 rowCountDelta = newJournalRowCount - oldJournalRowCount;
            statisticsDelta.RowCount = statisticsDelta.LogicalRowCount = rowCountDelta;

            if (firstOverlayedRowIndex) {
                if (*firstOverlayedRowIndex > oldJournalRowCount) {
                    YT_LOG_ALERT(
                        "Chunk seal produced row gap in journal (ChunkId: %v, StartRowIndex: %v, FirstOverlayedRowIndex: %v)",
                        chunk->GetId(),
                        oldJournalRowCount,
                        *firstOverlayedRowIndex);
                } else if (*firstOverlayedRowIndex < oldJournalRowCount) {
                    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                        "Journal chunk has a non-trivial overlap with the previous one (ChunkId: %v, StartRowIndex: %v, FirstOverlayedRowIndex: %v)",
                        chunk->GetId(),
                        oldJournalRowCount,
                        *firstOverlayedRowIndex);
                }
            }

            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(),
                "Updating journal statistics after chunk seal (ChunkId: %v, OldJournalRowCount: %v, NewJournalRowCount: %v)",
                chunk->GetId(),
                oldJournalRowCount,
                newJournalRowCount);
        }

        AccumulateUniqueAncestorsStatistics(chunk, statisticsDelta);

        if (chunkList->Children().back() == chunk) {
            auto owningNodes = GetOwningNodes(chunk);

            bool journalNodeLocked = false;
            TJournalNode* trunkJournalNode = nullptr;
            for (auto* node : owningNodes) {
                if (node->GetType() == EObjectType::Journal) {
                    auto* journalNode = node->As<TJournalNode>();
                    if (journalNode->GetUpdateMode() != EUpdateMode::None) {
                        journalNodeLocked = true;
                    }
                    if (trunkJournalNode) {
                        YT_VERIFY(journalNode->GetTrunkNode() == trunkJournalNode);
                    } else {
                        trunkJournalNode = journalNode->GetTrunkNode();
                    }
                }
            }

            if (!journalNodeLocked && IsObjectAlive(trunkJournalNode)) {
                const auto& journalManager = Bootstrap_->GetJournalManager();
                journalManager->SealJournal(trunkJournalNode, nullptr);
            }
        }
    }

    void OnProfiling()
    {
        BufferedProducer_->SetEnabled(true);
        CrpBufferedProducer_->SetEnabled(true);

        TSensorBuffer buffer;
        TSensorBuffer crpBuffer;

        ChunkReplicator_->OnProfiling(&buffer, &crpBuffer);
        ChunkSealer_->OnProfiling(&buffer);
        JobRegistry_->OnProfiling(&buffer);

        if (IsLeader()) {
            ChunkMerger_->OnProfiling(&buffer);
            ChunkAutotomizer_->OnProfiling(&buffer);

            buffer.AddGauge("/chunk_count", ChunkMap_.GetSize());
            buffer.AddGauge("/sequoia_chunk_count", SequoiaChunkCount_);
            buffer.AddGauge("/consistent_placement_enabled_chunk_count", CrpChunkCount_);
            buffer.AddCounter("/chunks_created", ChunksCreated_);
            buffer.AddCounter("/chunks_destroyed", ChunksDestroyed_);

            buffer.AddGauge("/chunk_replica_count", TotalReplicaCount_);
            buffer.AddCounter("/chunk_replicas_added", ChunkReplicasAdded_);
            buffer.AddCounter("/chunk_replicas_removed", ChunkReplicasRemoved_);

            buffer.AddGauge("/chunk_view_count", ChunkViewMap_.GetSize());
            buffer.AddCounter("/chunk_views_created", ChunkViewsCreated_);
            buffer.AddCounter("/chunk_views_destroyed", ChunkViewsDestroyed_);

            buffer.AddGauge("/chunk_list_count", ChunkListMap_.GetSize());
            buffer.AddCounter("/chunk_lists_created", ChunkListsCreated_);
            buffer.AddCounter("/chunk_lists_destroyed", ChunkListsDestroyed_);

            {
                TWithTagGuard guard(&buffer, "mode", "immediate");
                buffer.AddCounter("/ally_replicas_announced", ImmediateAllyReplicasAnnounced_);
            }
            {
                TWithTagGuard guard(&buffer, "mode", "delayed");
                buffer.AddCounter("/ally_replicas_announced", DelayedAllyReplicasAnnounced_);
            }
            {
                TWithTagGuard guard(&buffer, "mode", "lazy");
                buffer.AddCounter("/ally_replicas_announced", LazyAllyReplicasAnnounced_);
            }

            buffer.AddGauge("/endorsement_count", EndorsementCount_);
            buffer.AddCounter("/endorsements_added", EndorsementsAdded_);
            buffer.AddCounter("/endorsements_confirmed", EndorsementsConfirmed_);

            buffer.AddGauge("/destroyed_replica_count", DestroyedReplicaCount_);
        }

        BufferedProducer_->Update(std::move(buffer));
        CrpBufferedProducer_->Update(std::move(crpBuffer));
    }


    int GetFreeMediumIndex()
    {
        for (int index = 0; index < MaxMediumCount; ++index) {
            if (!UsedMediumIndexes_[index]) {
                return index;
            }
        }
        YT_ABORT();
    }

    TMedium* DoCreateMedium(
        TMediumId id,
        int mediumIndex,
        const TString& name,
        std::optional<bool> transient,
        std::optional<int> priority)
    {
        auto mediumHolder = TPoolAllocator::New<TMedium>(id);
        mediumHolder->SetName(name);
        mediumHolder->SetIndex(mediumIndex);
        if (transient) {
            mediumHolder->SetTransient(*transient);
        }
        if (priority) {
            ValidateMediumPriority(*priority);
            mediumHolder->SetPriority(*priority);
        }

        auto* medium = MediumMap_.Insert(id, std::move(mediumHolder));
        RegisterMedium(medium);
        InitializeMediumConfig(medium);

        // Make the fake reference.
        YT_VERIFY(medium->RefObject() == 1);

        return medium;
    }

    void RegisterMedium(TMedium* medium)
    {
        YT_VERIFY(NameToMediumMap_.emplace(medium->GetName(), medium).second);

        auto mediumIndex = medium->GetIndex();
        YT_VERIFY(!UsedMediumIndexes_[mediumIndex]);
        UsedMediumIndexes_.set(mediumIndex);

        YT_VERIFY(!IndexToMediumMap_[mediumIndex]);
        IndexToMediumMap_[mediumIndex] = medium;
    }

    void UnregisterMedium(TMedium* medium)
    {
        YT_VERIFY(NameToMediumMap_.erase(medium->GetName()) == 1);

        auto mediumIndex = medium->GetIndex();
        YT_VERIFY(UsedMediumIndexes_[mediumIndex]);
        UsedMediumIndexes_.reset(mediumIndex);

        YT_VERIFY(IndexToMediumMap_[mediumIndex] == medium);
        IndexToMediumMap_[mediumIndex] = nullptr;
    }

    void InitializeMediumConfig(TMedium* medium)
    {
        InitializeMediumMaxReplicasPerRack(medium);
        InitializeMediumMaxReplicationFactor(medium);
    }

    void InitializeMediumMaxReplicasPerRack(TMedium* medium)
    {
        medium->Config()->MaxReplicasPerRack = Config_->MaxReplicasPerRack;
        medium->Config()->MaxRegularReplicasPerRack = Config_->MaxRegularReplicasPerRack;
        medium->Config()->MaxJournalReplicasPerRack = Config_->MaxJournalReplicasPerRack;
        medium->Config()->MaxErasureReplicasPerRack = Config_->MaxErasureReplicasPerRack;
    }

    // COMPAT(shakurov)
    void InitializeMediumMaxReplicationFactor(TMedium* medium)
    {
        medium->Config()->MaxReplicationFactor = Config_->MaxReplicationFactor;
    }

    void AbortAndRemoveJob(const TJobPtr& job) override
    {
        job->SetState(EJobState::Aborted);

        JobController_->OnJobAborted(job);

        JobRegistry_->OnJobFinished(job);
    }

    NRpc::IChannelPtr FindChunkReplicatorChannel(TChunk* chunk) override
    {
        const auto& incumbentManager = Bootstrap_->GetIncumbentManager();
        auto address = incumbentManager->GetIncumbentAddress(
            EIncumbentType::ChunkReplicator,
            chunk->GetShardIndex());
        if (!address) {
            return nullptr;
        }

        const auto& channelFactory = Bootstrap_->GetClusterConnection()->GetChannelFactory();
        auto channel = channelFactory->CreateChannel(*address);
        return CreateRealmChannel(std::move(channel), Bootstrap_->GetCellId());
    }

    NRpc::IChannelPtr GetChunkReplicatorChannelOrThrow(TChunk* chunk) override
    {
        if (auto channel = FindChunkReplicatorChannel(chunk)) {
            return channel;
        } else {
            THROW_ERROR_EXCEPTION("Chunk replicator for chunk %Qlv is not alive")
                << TErrorAttribute("shard_index", chunk->GetShardIndex());
        }
    }

    std::vector<NRpc::IChannelPtr> GetChunkReplicatorChannels() override
    {
        const auto& incumbentManager = Bootstrap_->GetIncumbentManager();

        THashSet<TString> replicatorAddresses;
        replicatorAddresses.reserve(ChunkShardCount);
        for (int shardIndex = 0; shardIndex < ChunkShardCount; ++shardIndex) {
            auto address = incumbentManager->GetIncumbentAddress(
                EIncumbentType::ChunkReplicator,
                shardIndex);
            if (address) {
                replicatorAddresses.insert(*address);
            }
        }

        std::vector<NRpc::IChannelPtr> channels;
        channels.reserve(replicatorAddresses.size());

        const auto& channelFactory = Bootstrap_->GetClusterConnection()->GetChannelFactory();
        for (const auto& address : replicatorAddresses) {
            auto channel = channelFactory->CreateChannel(address);
            channels.push_back(CreateRealmChannel(std::move(channel), Bootstrap_->GetCellId()));
        }

        return channels;
    }

    TFuture<i64> GetCellLostVitalChunkCount() override
    {
        auto channels = GetChunkReplicatorChannels();

        std::vector<TFuture<TIntrusivePtr<TObjectYPathProxy::TRspGet>>> responseFutures;
        responseFutures.reserve(channels.size());
        for (const auto& channel : channels) {
            auto proxy = TObjectServiceProxy::FromDirectMasterChannel(channel);
            auto req = TYPathProxy::Get("//sys/local_lost_vital_chunks/@count");
            responseFutures.push_back(proxy.Execute(req));
        }

        return AllSet(std::move(responseFutures))
            .Apply(BIND([] (const std::vector<TErrorOr<TIntrusivePtr<TObjectYPathProxy::TRspGet>>>& rspOrErrors) {
                i64 chunkCount = 0;
                for (const auto& rspOrError : rspOrErrors) {
                    if (!rspOrError.IsOK()) {
                        YT_LOG_DEBUG(rspOrError, "Failed to get local lost vital chunk count");
                        continue;
                    }

                    const auto& rsp = rspOrError.Value();
                    auto response = ConvertTo<INodePtr>(TYsonString{rsp->value()});
                    YT_VERIFY(response->GetType() == ENodeType::Int64);
                    chunkCount += response->AsInt64()->GetValue();
                }

                return chunkCount;
            }));
    }

    std::vector<TError> GetAlerts() const
    {
        std::vector<TError> alerts;
        {
            auto chunkPlacementAlerts = ChunkPlacement_->GetAlerts();
            alerts.insert(alerts.end(), chunkPlacementAlerts.begin(), chunkPlacementAlerts.end());
        }

        return alerts;
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr oldConfig)
    {
        const auto& newCrpConfig = GetDynamicConfig()->ConsistentReplicaPlacement;

        RedistributeConsistentReplicaPlacementTokensExecutor_->SetPeriod(
            newCrpConfig->TokenRedistributionPeriod);
        // NB: no need to immediately handle bucket count or token-per-node count
        // changes: this will be done in due time by the periodic.

        ConsistentChunkPlacement_->SetChunkReplicaCount(newCrpConfig->ReplicasPerChunk);

        const auto& oldCrpConfig = oldConfig->ChunkManager->ConsistentReplicaPlacement;
        if (newCrpConfig->Enable != oldCrpConfig->Enable) {
            // Storing a set of CRP-enabled chunks separately would've enabled
            // us refreshing only what's actually necessary here. But it still
            // seems not enough of a reason to.
            ScheduleGlobalChunkRefresh();
        }

        if (oldConfig->ChunkManager->EnablePerNodeIncrementalHeartbeatProfiling !=
            GetDynamicConfig()->EnablePerNodeIncrementalHeartbeatProfiling)
        {
            if (GetDynamicConfig()->EnablePerNodeIncrementalHeartbeatProfiling) {
                TotalIncrementalHeartbeatCounters_.reset();
            } else {
                const auto& nodeTracker = Bootstrap_->GetNodeTracker();
                for (const auto& [id, node] : nodeTracker->Nodes()) {
                    node->IncrementalHeartbeatCounters().reset();
                }
            }
        }

        ProfilingExecutor_->SetPeriod(GetDynamicConfig()->ProfilingPeriod);
    }

    static void ValidateMediumName(const TString& name)
    {
        if (name.empty()) {
            THROW_ERROR_EXCEPTION("Medium name cannot be empty");
        }
    }

    static void ValidateMediumPriority(int priority)
    {
        if (priority < 0 || priority > MaxMediumPriority) {
            THROW_ERROR_EXCEPTION("Medium priority must be in range [0,%v]", MaxMediumPriority);
        }
    }
};

DEFINE_ENTITY_MAP_ACCESSORS(TChunkManager, Chunk, TChunk, ChunkMap_)
DEFINE_ENTITY_MAP_ACCESSORS(TChunkManager, ChunkView, TChunkView, ChunkViewMap_)
DEFINE_ENTITY_MAP_ACCESSORS(TChunkManager, DynamicStore, TDynamicStore, DynamicStoreMap_)
DEFINE_ENTITY_MAP_ACCESSORS(TChunkManager, ChunkList, TChunkList, ChunkListMap_)
DEFINE_ENTITY_WITH_IRREGULAR_PLURAL_MAP_ACCESSORS(TChunkManager, Medium, Media, TMedium, MediumMap_)

////////////////////////////////////////////////////////////////////////////////

IChunkManagerPtr CreateChunkManager(TBootstrap* bootstrap)
{
    return New<TChunkManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
