#include "automaton.h"
#include "private.h"
#include "master_connector.h"
#include "security_manager.h"
#include "serialize.h"
#include "slot_manager.h"
#include "tablet_manager.h"
#include "tablet_service.h"
#include "tablet_slot.h"
#include "transaction_manager.h"
#include "tablet_snapshot_store.h"

#include <yt/yt/server/node/data_node/config.h>

#include <yt/yt/server/lib/cellar_agent/automaton_invoker_hood.h>
#include <yt/yt/server/lib/cellar_agent/occupant.h>

#include <yt/yt/server/lib/election/election_manager.h>

#include <yt/yt/server/lib/hive/hive_manager.h>
#include <yt/yt/server/lib/hive/mailbox.h>
#include <yt/yt/server/lib/hive/transaction_supervisor.h>

#include <yt/yt/server/lib/hydra/remote_changelog_store.h>
#include <yt/yt/server/lib/hydra/remote_snapshot_store.h>

#include <yt/yt/server/lib/hive/transaction_participant_provider.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/node/data_node/legacy_master_connector.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>

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
using namespace NYTree;
using namespace NYson;

using NHydra::EPeerState;

////////////////////////////////////////////////////////////////////////////////

class TTabletSlot::TImpl
    : public TRefCounted
    , public TAutomatonInvokerHood<EAutomatonThreadQueue>
{
    using THood = TAutomatonInvokerHood<EAutomatonThreadQueue>;

public:
    TImpl(
        TTabletSlot* owner,
        int slotIndex,
        TTabletNodeConfigPtr config,
        NClusterNode::TBootstrap* bootstrap)
        : THood(Format("TabletSlot:%v", slotIndex))
        , Owner_(owner)
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

    void SetOccupant(ICellarOccupantPtr occupant)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(!Occupant_);

        Occupant_ = std::move(occupant);
        Logger = GetLogger();
    }

    TCellId GetCellId() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Occupant_->GetCellId();
    }

    EPeerState GetAutomatonState() const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto hydraManager = GetHydraManager();
        return hydraManager ? hydraManager->GetAutomatonState() : EPeerState::None;
    }

    const TString& GetTabletCellBundleName() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Occupant_->GetCellBundleName();
    }

    IDistributedHydraManagerPtr GetHydraManager() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Occupant_->GetHydraManager();
    }

    const TCompositeAutomatonPtr& GetAutomaton() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return Occupant_->GetAutomaton();
    }

    const THiveManagerPtr& GetHiveManager() const
    {
        return Occupant_->GetHiveManager();
    }

    TMailbox* GetMasterMailbox()
    {
        return Occupant_->GetMasterMailbox();
    }

    const TTransactionManagerPtr& GetTransactionManager() const
    {
        return TransactionManager_;
    }

    const ITransactionSupervisorPtr& GetTransactionSupervisor() const
    {
        return Occupant_->GetTransactionSupervisor();
    }

    const TTabletManagerPtr& GetTabletManager() const
    {
        return TabletManager_;
    }

    TObjectId GenerateId(EObjectType type)
    {
        return Occupant_->GenerateId(type);
    }

    int GetDynamicConfigVersion() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return DynamicConfigVersion_;
    }

    void UpdateDynamicConfig(const TUpdateTabletSlotInfo& updateInfo)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto updateVersion = updateInfo.dynamic_config_version();

        if (DynamicConfigVersion_ >= updateVersion) {
            YT_LOG_DEBUG("Received outdated dynamic config update (DynamicConfigVersion: %v, UpdateVersion: %v)",
                DynamicConfigVersion_,
                updateVersion);
            return;
        }

        try {
            TDynamicTabletCellOptionsPtr dynamicOptions;

            if (updateInfo.has_dynamic_options()) {
                dynamicOptions = New<TDynamicTabletCellOptions>();
                dynamicOptions->SetUnrecognizedStrategy(EUnrecognizedStrategy::Keep);
                dynamicOptions->Load(ConvertTo<INodePtr>(TYsonString(updateInfo.dynamic_options())));
                auto unrecognized = dynamicOptions->GetUnrecognized();

                if (unrecognized->GetChildCount() > 0) {
                    THROW_ERROR_EXCEPTION("Dynamic options contains unrecognized parameters (Unrecognized: %v)",
                        ConvertToYsonString(unrecognized, EYsonFormat::Text).AsStringBuf());
                }
            }

            DynamicConfigVersion_ = updateInfo.dynamic_config_version();
            DynamicOptions_.Store(std::move(dynamicOptions));

            YT_LOG_DEBUG("Updated dynamic config (DynamicConfigVersion: %v)",
                DynamicConfigVersion_);

        } catch (const std::exception& ex) {
            // TODO(savrus): Write this to tablet cell errors once we have them.
            YT_LOG_ERROR(ex, "Error while updating dynamic config");
        }
    }

    TCompositeAutomatonPtr CreateAutomaton()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return New<TTabletAutomaton>(
            Owner_,
            SnapshotQueue_->GetInvoker());
    }

    void Configure(IDistributedHydraManagerPtr hydraManager)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        hydraManager->SubscribeStartLeading(BIND(&TImpl::OnStartEpoch, MakeWeak(this)));
        hydraManager->SubscribeStartFollowing(BIND(&TImpl::OnStartEpoch, MakeWeak(this)));

        hydraManager->SubscribeStopLeading(BIND(&TImpl::OnStopEpoch, MakeWeak(this)));
        hydraManager->SubscribeStopFollowing(BIND(&TImpl::OnStopEpoch, MakeWeak(this)));

        InitGuardedInvokers(hydraManager);

        // NB: Tablet Manager must register before Transaction Manager since the latter
        // will be writing and deleting rows during snapshot loading.
        TabletManager_ = New<TTabletManager>(
            Config_->TabletManager,
            Owner_,
            Bootstrap_);

        TransactionManager_ = New<TTransactionManager>(
            Config_->TransactionManager,
            Owner_,
            Bootstrap_);

        Logger = GetLogger();
    }

    void Initialize()
    {
        TabletService_ = CreateTabletService(
            Owner_,
            Bootstrap_);

        TabletManager_->Initialize();
    }

    void RegisterRpcServices()
    {
        const auto& rpcServer = Bootstrap_->GetRpcServer();
        rpcServer->RegisterService(TabletService_);
    }

    void Stop()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& snapshotStore = Bootstrap_->GetTabletSnapshotStore();
        snapshotStore->UnregisterTabletSnapshots(Owner_);

        ResetEpochInvokers();
        ResetGuardedInvokers();
    }

    void Finalize()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        TabletManager_.Reset();

        TransactionManager_.Reset();

        if (TabletService_) {
            const auto& rpcServer = Bootstrap_->GetRpcServer();
            rpcServer->UnregisterService(TabletService_);
        }
        TabletService_.Reset();
    }

    TCompositeMapServicePtr PopulateOrchidService(TCompositeMapServicePtr orchid)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return orchid
            ->AddChild("dynamic_options", IYPathService::FromMethod(
                &TImpl::GetDynamicOptions,
                MakeWeak(this)))
            ->AddChild("dynamic_config_version", IYPathService::FromMethod(
                &TImpl::GetDynamicConfigVersion,
                MakeWeak(this)))
            ->AddChild("life_stage", IYPathService::FromMethod(
                &TTabletManager::GetTabletCellLifeStage,
                MakeWeak(TabletManager_))
                ->Via(GetAutomatonInvoker()))
            ->AddChild("transactions", TransactionManager_->GetOrchidService())
            ->AddChild("tablets", TabletManager_->GetOrchidService());
    }

    const TRuntimeTabletCellDataPtr& GetRuntimeData() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return RuntimeData_;
    }

    double GetUsedCpu(double cpuPerTabletSlot) const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return GetDynamicOptions()->CpuPerTabletSlot.value_or(cpuPerTabletSlot);
    }

    TDynamicTabletCellOptionsPtr GetDynamicOptions() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return DynamicOptions_.Load();
    }

    const TTabletCellOptionsPtr& GetOptions() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Occupant_->GetOptions();
    }


    NProfiling::TProfiler GetProfiler() const
    {
        return TabletNodeProfiler;
    }

private:
    TTabletSlot* const Owner_;
    const TTabletNodeConfigPtr Config_;
    NClusterNode::TBootstrap* const Bootstrap_;

    ICellarOccupantPtr Occupant_;

    const TActionQueuePtr SnapshotQueue_;

    TCellDescriptor CellDescriptor_;

    NLogging::TLogger Logger;

    const TRuntimeTabletCellDataPtr RuntimeData_ = New<TRuntimeTabletCellData>();

    TAtomicObject<TDynamicTabletCellOptionsPtr> DynamicOptions_ = New<TDynamicTabletCellOptions>();
    int DynamicConfigVersion_ = -1;

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
            const auto& masterConnector = Bootstrap_->GetTabletNodeMasterConnector();
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

////////////////////////////////////////////////////////////////////////////////

TTabletSlot::TTabletSlot(
    int slotIndex,
    TTabletNodeConfigPtr config,
    NClusterNode::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(
        this,
        slotIndex,
        std::move(config),
        bootstrap))
{ }

TTabletSlot::~TTabletSlot() = default;

void TTabletSlot::SetOccupant(ICellarOccupantPtr occupant)
{
    Impl_->SetOccupant(std::move(occupant));
}

TCompositeAutomatonPtr TTabletSlot::CreateAutomaton()
{
    return Impl_->CreateAutomaton();
}

void TTabletSlot::Configure(IDistributedHydraManagerPtr hydraManager)
{
    Impl_->Configure(std::move(hydraManager));
}

const ITransactionManagerPtr TTabletSlot::GetOccupierTransactionManager()
{
    return Impl_->GetTransactionManager();
}

void TTabletSlot::Initialize()
{
    Impl_->Initialize();
}

void TTabletSlot::RegisterRpcServices()
{
    Impl_->RegisterRpcServices();
}

IInvokerPtr TTabletSlot::GetOccupierAutomatonInvoker()
{
    return Impl_->GetAutomatonInvoker(EAutomatonThreadQueue::Default);
}

const TString& TTabletSlot::GetTabletCellBundleName() const
{
    return Impl_->GetTabletCellBundleName();
}

IInvokerPtr TTabletSlot::GetMutationAutomatonInvoker()
{
    return Impl_->GetAutomatonInvoker(EAutomatonThreadQueue::Mutation);
}

TCompositeMapServicePtr TTabletSlot::PopulateOrchidService(TCompositeMapServicePtr orchid)
{
    return Impl_->PopulateOrchidService(orchid);
}

void TTabletSlot::Stop()
{
    return Impl_->Stop();
}

void TTabletSlot::Finalize()
{
    return Impl_->Finalize();
}

ECellarType TTabletSlot::GetCellarType()
{
    return CellarType;
}

TCellId TTabletSlot::GetCellId() const
{
    return Impl_->GetCellId();
}

EPeerState TTabletSlot::GetAutomatonState() const
{
    return Impl_->GetAutomatonState();
}

IDistributedHydraManagerPtr TTabletSlot::GetHydraManager() const
{
    return Impl_->GetHydraManager();
}

const TCompositeAutomatonPtr& TTabletSlot::GetAutomaton() const
{
    return Impl_->GetAutomaton();
}

IInvokerPtr TTabletSlot::GetAutomatonInvoker(EAutomatonThreadQueue queue) const
{
    return Impl_->GetAutomatonInvoker(queue);
}

IInvokerPtr TTabletSlot::GetEpochAutomatonInvoker(EAutomatonThreadQueue queue) const
{
    return Impl_->GetEpochAutomatonInvoker(queue);
}

IInvokerPtr TTabletSlot::GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue) const
{
    return Impl_->GetGuardedAutomatonInvoker(queue);
}

const THiveManagerPtr& TTabletSlot::GetHiveManager() const
{
    return Impl_->GetHiveManager();
}

TMailbox* TTabletSlot::GetMasterMailbox()
{
    return Impl_->GetMasterMailbox();
}

const TTransactionManagerPtr& TTabletSlot::GetTransactionManager() const
{
    return Impl_->GetTransactionManager();
}

const ITransactionSupervisorPtr& TTabletSlot::GetTransactionSupervisor() const
{
    return Impl_->GetTransactionSupervisor();
}

const TTabletManagerPtr& TTabletSlot::GetTabletManager() const
{
    return Impl_->GetTabletManager();
}

TObjectId TTabletSlot::GenerateId(EObjectType type)
{
    return Impl_->GenerateId(type);
}

const TRuntimeTabletCellDataPtr& TTabletSlot::GetRuntimeData() const
{
    return Impl_->GetRuntimeData();
}

double TTabletSlot::GetUsedCpu(double cpuPerTabletSlot) const
{
    return Impl_->GetUsedCpu(cpuPerTabletSlot);
}

TDynamicTabletCellOptionsPtr TTabletSlot::GetDynamicOptions() const
{
    return Impl_->GetDynamicOptions();
}

TTabletCellOptionsPtr TTabletSlot::GetOptions() const
{
    return Impl_->GetOptions();
}

int TTabletSlot::GetDynamicConfigVersion() const
{
    return Impl_->GetDynamicConfigVersion();
}

void TTabletSlot::UpdateDynamicConfig(const NTabletClient::NProto::TUpdateTabletSlotInfo& updateInfo)
{
    Impl_->UpdateDynamicConfig(updateInfo);
}

NProfiling::TProfiler TTabletSlot::GetProfiler()
{
    return Impl_->GetProfiler();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode::NYT
