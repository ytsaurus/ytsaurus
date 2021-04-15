#include "tablet_slot.h"

#include "automaton.h"
#include "private.h"
#include "security_manager.h"
#include "serialize.h"
#include "slot_manager.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_service.h"
#include "transaction_manager.h"
#include "tablet_snapshot_store.h"
#include "hint_manager.h"

#include <yt/yt/server/node/data_node/config.h>

#include <yt/yt/server/lib/cellar_agent/automaton_invoker_hood.h>
#include <yt/yt/server/lib/cellar_agent/occupant.h>

#include <yt/yt/server/lib/election/election_manager.h>

#include <yt/yt/server/lib/hive/hive_manager.h>
#include <yt/yt/server/lib/hive/mailbox.h>
#include <yt/yt/server/lib/hive/transaction_supervisor.h>

#include <yt/yt/server/lib/hydra/remote_changelog_store.h>
#include <yt/yt/server/lib/hydra/remote_snapshot_store.h>
#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>

#include <yt/yt/server/lib/hive/transaction_participant_provider.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/node/cellar_node/master_connector.h>

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/node/data_node/legacy_master_connector.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/chunk_client/chunk_fragment_reader.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/ytlib/election/cell_manager.h>

#include <yt/yt/core/concurrency/fair_share_action_queue.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/rpc/response_keeper.h>

#include <yt/yt/core/ytree/virtual.h>
#include <yt/yt/core/ytree/helpers.h>

#include <yt/yt/core/misc/atomic_object.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NCellarClient;
using namespace NCellarAgent;
using namespace NConcurrency;
using namespace NElection;
using namespace NHiveClient;
using namespace NHiveServer;
using namespace NHydra;
using namespace NObjectClient;
using namespace NRpc;
using namespace NTabletClient::NProto;
using namespace NTabletClient;
using namespace NChunkClient;
using namespace NYTree;
using namespace NYson;

using NHydra::EPeerState;

////////////////////////////////////////////////////////////////////////////////

class TTabletSlot
    : public TAutomatonInvokerHood<EAutomatonThreadQueue>
    , public ITabletSlot
{
private:
    using THood = TAutomatonInvokerHood<EAutomatonThreadQueue>;

public:
    TTabletSlot(
        int slotIndex,
        TTabletNodeConfigPtr config,
        NClusterNode::TBootstrap* bootstrap)
        : THood(Format("TabletSlot:%v", slotIndex))
        , Config_(config)
        , Bootstrap_(bootstrap)
        , SnapshotQueue_(New<TActionQueue>(
            Format("TabletSnap:%v", slotIndex)))
        , Logger(TabletNodeLogger)
    {
        VERIFY_INVOKER_THREAD_AFFINITY(GetAutomatonInvoker(), AutomatonThread);

        ResetEpochInvokers();
        ResetGuardedInvokers();
    }

    virtual void SetOccupant(ICellarOccupantPtr occupant) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(!Occupant_);

        Occupant_ = std::move(occupant);
        Logger = GetLogger();
    }

    virtual IInvokerPtr GetAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return THood::GetAutomatonInvoker(queue);
    }

    virtual IInvokerPtr GetOccupierAutomatonInvoker() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return GetAutomatonInvoker(EAutomatonThreadQueue::Default);
    }

    virtual IInvokerPtr GetMutationAutomatonInvoker() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return GetAutomatonInvoker(EAutomatonThreadQueue::Mutation);
    }

    virtual IInvokerPtr GetEpochAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return THood::GetEpochAutomatonInvoker(queue);
    }

    virtual IInvokerPtr GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return THood::GetGuardedAutomatonInvoker(queue);
    }

    virtual TCellId GetCellId() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Occupant_->GetCellId();
    }

    virtual EPeerState GetAutomatonState() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto hydraManager = GetHydraManager();
        return hydraManager ? hydraManager->GetAutomatonState() : EPeerState::None;
    }

    virtual const TString& GetTabletCellBundleName() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Occupant_->GetCellBundleName();
    }

    virtual IDistributedHydraManagerPtr GetHydraManager() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Occupant_->GetHydraManager();
    }

    virtual const TCompositeAutomatonPtr& GetAutomaton() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return Occupant_->GetAutomaton();
    }

    virtual const THiveManagerPtr& GetHiveManager() override
    {
        return Occupant_->GetHiveManager();
    }

    virtual TMailbox* GetMasterMailbox() override
    {
        return Occupant_->GetMasterMailbox();
    }

    virtual const TTransactionManagerPtr& GetTransactionManager() override
    {
        return TransactionManager_;
    }

    virtual ITransactionManagerPtr GetOccupierTransactionManager() override
    {
        return GetTransactionManager();
    }

    virtual const ITransactionSupervisorPtr& GetTransactionSupervisor() override
    {
        return Occupant_->GetTransactionSupervisor();
    }

    virtual const TTabletManagerPtr& GetTabletManager() override
    {
        return TabletManager_;
    }

    virtual TObjectId GenerateId(EObjectType type) override
    {
        return Occupant_->GenerateId(type);
    }

    virtual TCompositeAutomatonPtr CreateAutomaton() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return New<TTabletAutomaton>(
            this,
            SnapshotQueue_->GetInvoker());
    }

    virtual void Configure(IDistributedHydraManagerPtr hydraManager) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        hydraManager->SubscribeStartLeading(BIND(&TTabletSlot::OnStartEpoch, MakeWeak(this)));
        hydraManager->SubscribeStartFollowing(BIND(&TTabletSlot::OnStartEpoch, MakeWeak(this)));

        hydraManager->SubscribeStopLeading(BIND(&TTabletSlot::OnStopEpoch, MakeWeak(this)));
        hydraManager->SubscribeStopFollowing(BIND(&TTabletSlot::OnStopEpoch, MakeWeak(this)));

        InitGuardedInvokers(hydraManager);

        // NB: Tablet Manager must register before Transaction Manager since the latter
        // will be writing and deleting rows during snapshot loading.
        TabletManager_ = New<TTabletManager>(
            Config_->TabletManager,
            this,
            Bootstrap_);

        TransactionManager_ = New<TTransactionManager>(
            Config_->TransactionManager,
            this,
            Bootstrap_);

        Logger = GetLogger();
    }

    virtual void Initialize() override
    {
        TabletService_ = CreateTabletService(
            this,
            Bootstrap_);

        TabletManager_->Initialize();
    }

    virtual void RegisterRpcServices() override
    {
        const auto& rpcServer = Bootstrap_->GetRpcServer();
        rpcServer->RegisterService(TabletService_);
    }

    virtual void Stop() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();
        snapshotStore->UnregisterTabletSnapshots(this);

        ResetEpochInvokers();
        ResetGuardedInvokers();
    }

    virtual void Finalize() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        TabletManager_.Reset();

        TransactionManager_.Reset();

        if (TabletService_) {
            const auto& rpcServer = Bootstrap_->GetRpcServer();
            rpcServer->UnregisterService(TabletService_);
            TabletService_.Reset();
        }
    }

    virtual ECellarType GetCellarType() override
    {
        return CellarType;
    }

    virtual TCompositeMapServicePtr PopulateOrchidService(TCompositeMapServicePtr orchid) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return orchid
            ->AddChild("life_stage", IYPathService::FromMethod(
                &TTabletManager::GetTabletCellLifeStage,
                MakeWeak(TabletManager_))
                ->Via(GetAutomatonInvoker()))
            ->AddChild("transactions", TransactionManager_->GetOrchidService())
            ->AddChild("tablets", TabletManager_->GetOrchidService());
    }

    virtual const TRuntimeTabletCellDataPtr& GetRuntimeData() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return RuntimeData_;
    }

    virtual double GetUsedCpu(double cpuPerTabletSlot) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return GetDynamicOptions()->CpuPerTabletSlot.value_or(cpuPerTabletSlot);
    }

    virtual TDynamicTabletCellOptionsPtr GetDynamicOptions() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Occupant_->GetDynamicOptions();
    }

    virtual TTabletCellOptionsPtr GetOptions() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Occupant_->GetOptions();
    }

    virtual NProfiling::TProfiler GetProfiler() override
    {
        return TabletNodeProfiler;
    }

    virtual IChunkFragmentReaderPtr CreateChunkFragmentReader(TTablet* tablet) override
    {
        return NChunkClient::CreateChunkFragmentReader(
            tablet->GetSettings().HunkReaderConfig,
            Bootstrap_->GetMasterClient(),
            Bootstrap_->GetTabletNodeHintManager());
    }

private:
    const TTabletNodeConfigPtr Config_;
    NClusterNode::TBootstrap* const Bootstrap_;

    ICellarOccupantPtr Occupant_;

    const TActionQueuePtr SnapshotQueue_;

    TCellDescriptor CellDescriptor_;

    NLogging::TLogger Logger;

    const TRuntimeTabletCellDataPtr RuntimeData_ = New<TRuntimeTabletCellData>();

    TTabletManagerPtr TabletManager_;

    TTransactionManagerPtr TransactionManager_;

    ITransactionSupervisorPtr TransactionSupervisor_;

    NRpc::IServicePtr TabletService_;


    void OnStartEpoch()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        InitEpochInvokers(GetHydraManager());
    }

    void OnStopEpoch()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        ResetEpochInvokers();
    }

    void OnRecoveryComplete()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& clusterNodeMasterConnector = Bootstrap_->GetClusterNodeMasterConnector();
        if (!clusterNodeMasterConnector->IsConnected()) {
            return;
        }

        // Notify master about recovery completion as soon as possible via out-of-order heartbeat.
        if (clusterNodeMasterConnector->UseNewHeartbeats()) {
            const auto& masterConnector = Bootstrap_->GetCellarNodeMasterConnector();
            for (auto masterCellTag : clusterNodeMasterConnector->GetMasterCellTags()) {
                masterConnector->ScheduleHeartbeat(masterCellTag, /* immediately */ true);
            }
        } else {
            // Old heartbeats are heavy, so we send out-of-order heartbeat to primary master cell only.
            auto primaryCellTag = CellTagFromId(Bootstrap_->GetCellId());
            Bootstrap_->GetLegacyMasterConnector()->ScheduleNodeHeartbeat(primaryCellTag, /* immediately */ true);
        }
    }


    NLogging::TLogger GetLogger() const
    {
        return TabletNodeLogger.WithTag("CellId: %v, PeerId: %v",
            Occupant_->GetCellId(),
            Occupant_->GetPeerId());
    }


    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);
};

ITabletSlotPtr CreateTabletSlot(
    int slotIndex,
    TTabletNodeConfigPtr config,
    NClusterNode::TBootstrap* bootstrap)
{
    return New<TTabletSlot>(
        slotIndex,
        std::move(config),
        bootstrap);
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode::NYT
