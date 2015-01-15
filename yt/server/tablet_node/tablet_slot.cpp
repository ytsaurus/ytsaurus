#include "tablet_slot.h"
#include "config.h"
#include "tablet_slot_manager.h"
#include "serialize.h"
#include "automaton.h"
#include "tablet_manager.h"
#include "transaction_manager.h"
#include "tablet_service.h"
#include "private.h"

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/scheduler.h>
#include <core/concurrency/action_queue.h>

#include <core/ytree/fluent.h>

#include <core/rpc/server.h>

#include <core/logging/log.h>

#include <ytlib/election/config.h>
#include <ytlib/election/cell_manager.h>

#include <ytlib/hive/cell_directory.h>

#include <ytlib/transaction_client/timestamp_provider.h>

#include <ytlib/tablet_client/config.h>

#include <ytlib/api/connection.h>
#include <ytlib/api/client.h>

#include <server/election/election_manager.h>

#include <server/hydra/changelog.h>
#include <server/hydra/remote_changelog_store.h>
#include <server/hydra/snapshot.h>
#include <server/hydra/remote_snapshot_store.h>
#include <server/hydra/hydra_manager.h>
#include <server/hydra/distributed_hydra_manager.h>
#include <server/hydra/persistent_response_keeper.h>

#include <server/hive/hive_manager.h>
#include <server/hive/mailbox.h>
#include <server/hive/transaction_supervisor.h>

#include <server/cell_node/bootstrap.h>
#include <server/cell_node/config.h>

#include <server/data_node/config.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NRpc;
using namespace NYTree;
using namespace NYson;
using namespace NElection;
using namespace NHydra;
using namespace NHive;
using namespace NNodeTrackerClient::NProto;
using namespace NObjectClient;
using namespace NTransactionClient;

using NHydra::EPeerState;

////////////////////////////////////////////////////////////////////////////////

class TTabletSlot::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TTabletSlot* owner,
        int slotIndex,
        TTabletNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap)
        : Owner_(owner)
        , SlotIndex_(slotIndex)
        , Config_(config)
        , Bootstrap_(bootstrap)
        , AutomatonQueue_(New<TFairShareActionQueue>(
            Format("TabletSlot:%v", SlotIndex_),
            TEnumTraits<EAutomatonThreadQueue>::GetDomainNames()))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(GetAutomatonInvoker(), AutomatonThread);

        SetCellId(NullCellId);
        ResetEpochInvokers();
        ResetGuardedInvokers();
    }


    int GetIndex() const
    {
        return SlotIndex_;
    }

    const TCellId& GetCellId() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return CellId_;
    }

    EPeerState GetControlState() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (HydraManager_) {
            State_ = HydraManager_->GetControlState();
        }

        return State_;
    }

    EPeerState GetAutomatonState() const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return HydraManager_ ? HydraManager_->GetAutomatonState() : EPeerState::None;
    }

    TPeerId GetPeerId() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return PeerId_;
    }

    int GetCellConfigVersion() const
    {
        return CellConfigVersion_;
    }

    TTabletCellConfigPtr GetCellConfig() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return CellConfig_;
    }

    const TTransactionId& GetPrerequisiteTransactionId() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return PrerequisiteTransactionId_;
    }

    IHydraManagerPtr GetHydraManager() const
    {
        return HydraManager_;
    }

    IResponseKeeperPtr GetResponseKeeper() const
    {
        return ResponseKeeper_;
    }

    TTabletAutomatonPtr GetAutomaton() const
    {
        return Automaton_;
    }

    IInvokerPtr GetAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return AutomatonQueue_->GetInvoker(static_cast<int>(queue));
    }

    IInvokerPtr GetEpochAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(InvokersSpinLock_);
        return EpochAutomatonInvokers_[queue];
    }

    IInvokerPtr GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(InvokersSpinLock_);
        return GuardedAutomatonInvokers_[queue];
    }

    THiveManagerPtr GetHiveManager() const
    {
        return HiveManager_;
    }

    TMailbox* GetMasterMailbox()
    {
        // Create master mailbox lazily.
        auto masterCellId = Bootstrap_->GetCellId();
        return HiveManager_->GetOrCreateMailbox(masterCellId);
    }

    TTransactionManagerPtr GetTransactionManager() const
    {
        return TransactionManager_;
    }

    TTransactionSupervisorPtr GetTransactionSupervisor() const
    {
        return TransactionSupervisor_;
    }

    TTabletManagerPtr GetTabletManager() const
    {
        return TabletManager_;
    }

    TObjectId GenerateId(EObjectType type)
    {
        auto* mutationContext = HydraManager_->GetMutationContext();

        const auto& version = mutationContext->GetVersion();

        auto random = mutationContext->RandomGenerator().Generate<ui64>();

        return TObjectId(
            random ^ CellId_.Parts32[0],
            (CellId_.Parts32[1] & 0xffff0000) + static_cast<int>(type),
            version.RecordId,
            version.SegmentId);
    }


    void Initialize(const TCreateTabletSlotInfo& createInfo)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(State_ == EPeerState::None);

        auto cellId = FromProto<TCellId>(createInfo.cell_id());
        SetCellId(cellId);

        Options_ = ConvertTo<TTabletCellOptionsPtr>(TYsonString(createInfo.options()));
        PrerequisiteTransactionId_ = FromProto<TTransactionId>(createInfo.prerequisite_transaction_id());
        State_ = EPeerState::Stopped;

        LOG_INFO("Slot initialized");
    }

    void Configure(const TConfigureTabletSlotInfo& configureInfo)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(State_ != EPeerState::None);

        CellConfigVersion_ = configureInfo.config_version();
        CellConfig_ = ConvertTo<TTabletCellConfigPtr>(TYsonString(configureInfo.config()));
        
        if (HydraManager_) {
            CellManager_->Reconfigure(CellConfig_->ToElection(CellId_));
            LOG_INFO("Slot reconfigured (ConfigVersion: %v, PrerequisiteTransactionId: %v)",
                CellConfigVersion_,
                PrerequisiteTransactionId_);
        } else {
            PeerId_ = configureInfo.peer_id();
            State_ = EPeerState::Elections;

            CellManager_ = New<TCellManager>(
                CellConfig_->ToElection(CellId_),
                Bootstrap_->GetTabletChannelFactory(),
                configureInfo.peer_id());

            Automaton_ = New<TTabletAutomaton>(Owner_);

            std::vector<TTransactionId> prerequisiteTransactionIds;
            prerequisiteTransactionIds.push_back(PrerequisiteTransactionId_);

            auto snapshotStore = CreateRemoteSnapshotStore(
                Config_->Snapshots,
                Options_,
                Format("//sys/tablet_cells/%v/snapshots", CellId_),
                Bootstrap_->GetMasterClient(),
                prerequisiteTransactionIds);

            auto changelogStore = CreateRemoteChangelogStore(
                Config_->Changelogs,
                Options_,
                Format("//sys/tablet_cells/%v/changelogs", CellId_),
                Bootstrap_->GetMasterClient(),
                prerequisiteTransactionIds);

            auto rpcServer = Bootstrap_->GetRpcServer();

            HydraManager_ = CreateDistributedHydraManager(
                Config_->HydraManager,
                Bootstrap_->GetControlInvoker(),
                GetAutomatonInvoker(),
                Automaton_,
                rpcServer,
                CellManager_,
                changelogStore,
                snapshotStore);

            HydraManager_->SubscribeStartLeading(BIND(&TImpl::OnStartEpoch, MakeWeak(this)));
            HydraManager_->SubscribeStartFollowing(BIND(&TImpl::OnStartEpoch, MakeWeak(this)));
            
            HydraManager_->SubscribeStopLeading(BIND(&TImpl::OnStopEpoch, MakeWeak(this)));
            HydraManager_->SubscribeStopFollowing(BIND(&TImpl::OnStopEpoch, MakeWeak(this)));

            {
                TGuard<TSpinLock> guard(InvokersSpinLock_);
                for (auto queue : TEnumTraits<EAutomatonThreadQueue>::GetDomainValues()) {
                    auto unguardedInvoker = GetAutomatonInvoker(queue);
                    GuardedAutomatonInvokers_[queue] = HydraManager_->CreateGuardedAutomatonInvoker(unguardedInvoker);
                }
            }

            ResponseKeeper_ = New<TPersistentResponseKeeper>(
                Config_->HydraManager->ResponseKeeper,
                GetAutomatonInvoker(),
                HydraManager_,
                Automaton_);

            HiveManager_ = New<THiveManager>(
                Config_->HiveManager,
                Bootstrap_->GetMasterClient()->GetConnection()->GetCellDirectory(),
                CellId_,
                GetAutomatonInvoker(),
                HydraManager_,
                Automaton_);

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

            TransactionSupervisor_ = New<TTransactionSupervisor>(
                Config_->TransactionSupervisor,
                GetAutomatonInvoker(),
                HydraManager_,
                Automaton_,
                GetResponseKeeper(),
                HiveManager_,
                TransactionManager_,
                Bootstrap_->GetMasterClient()->GetConnection()->GetTimestampProvider());

            TabletService_ = CreateTabletService(
                Owner_,
                Bootstrap_);

            TabletManager_->Initialize();

            HydraManager_->Initialize();

            rpcServer->RegisterService(TransactionSupervisor_->GetRpcService());
            rpcServer->RegisterService(HiveManager_->GetRpcService());
            rpcServer->RegisterService(TabletService_);

            LOG_INFO("Slot configured (ConfigVersion: %v)",
                CellConfigVersion_);
        }
    }

    void Finalize()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(State_ != EPeerState::None);

        LOG_INFO("Finalizing slot");

        auto tabletSlotManager = Bootstrap_->GetTabletSlotManager();
        tabletSlotManager->UnregisterTabletSnapshots(Owner_);

        State_ = EPeerState::None;

        ResetEpochInvokers();
        ResetGuardedInvokers();

        Bootstrap_->GetControlInvoker()->Invoke(BIND(&TImpl::DoFinalize, MakeStrong(this)));
    }


    void BuildOrchidYson(IYsonConsumer* consumer)
    {
        BuildYsonFluently(consumer)
            .BeginAttributes()
                .Item("opaque").Value(true)
            .EndAttributes()
            .BeginMap()
                .Do(BIND(&TImpl::BuildOrchidYsonControl, Unretained(this)))
                .Do(BIND(&TImpl::BuildOrchidYsonAutomaton, Unretained(this)))
            .EndMap();
    }

private:
    TTabletSlot* const Owner_;
    const int SlotIndex_;
    const TTabletNodeConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;

    TCellId CellId_;
    mutable EPeerState State_ = EPeerState::None;
    TPeerId PeerId_ = InvalidPeerId;
    int CellConfigVersion_ = 0;
    TTabletCellConfigPtr CellConfig_;
    TTabletCellOptionsPtr Options_;
    TTransactionId PrerequisiteTransactionId_;

    TCellManagerPtr CellManager_;

    IHydraManagerPtr HydraManager_;

    TPersistentResponseKeeperPtr ResponseKeeper_;
    
    THiveManagerPtr HiveManager_;

    TTabletManagerPtr TabletManager_;

    TTransactionManagerPtr TransactionManager_;
    TTransactionSupervisorPtr TransactionSupervisor_;

    NRpc::IServicePtr TabletService_;

    TTabletAutomatonPtr Automaton_;
    TFairShareActionQueuePtr AutomatonQueue_;

    TSpinLock InvokersSpinLock_;
    TEnumIndexedVector<IInvokerPtr, EAutomatonThreadQueue> EpochAutomatonInvokers_;
    TEnumIndexedVector<IInvokerPtr, EAutomatonThreadQueue> GuardedAutomatonInvokers_;

    NLog::TLogger Logger = TabletNodeLogger;


    void SetCellId(const TCellId& cellId)
    {
        CellId_ = cellId;
        InitLogger();
    }

    void InitLogger()
    {
        Logger = NLog::TLogger(TabletNodeLogger);
        Logger.AddTag("Slot: %v", SlotIndex_);
        if (CellId_ != NullCellId) {
            Logger.AddTag("CellId: %v", CellId_);
        }
    }


    void ResetEpochInvokers()
    {
        TGuard<TSpinLock> guard(InvokersSpinLock_);
        std::fill(EpochAutomatonInvokers_.begin(), EpochAutomatonInvokers_.end(), GetNullInvoker());
    }

    void ResetGuardedInvokers()
    {
        TGuard<TSpinLock> guard(InvokersSpinLock_);
        std::fill(GuardedAutomatonInvokers_.begin(), GuardedAutomatonInvokers_.end(), GetNullInvoker());
    }


    void OnStartEpoch()
    {
        TGuard<TSpinLock> guard(InvokersSpinLock_);
        for (auto queue : TEnumTraits<EAutomatonThreadQueue>::GetDomainValues()) {
            EpochAutomatonInvokers_[queue] = HydraManager_
                ->GetAutomatonEpochContext()
                ->CancelableContext
                ->CreateInvoker(GetAutomatonInvoker(queue));
        }
    }

    void OnStopEpoch()
    {
        ResetEpochInvokers();
    }


    void DoFinalize()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // Wait for all pending activities in automaton thread to stop.
        LOG_INFO("Flushing automaton thread");

        SwitchTo(GetAutomatonInvoker());
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        // NB: Epoch invokers are already canceled so we don't expect any more callbacks.
        LOG_INFO("Automaton thread flushed");

        SwitchTo(Bootstrap_->GetControlInvoker());
        VERIFY_THREAD_AFFINITY(ControlThread);

        // Stop everything and release the references to break cycles.
        CellManager_.Reset();

        Automaton_.Reset();

        if (HydraManager_) {
            HydraManager_->Finalize();
        }
        HydraManager_.Reset();

        ResponseKeeper_.Reset();

        TabletManager_.Reset();

        TransactionManager_.Reset();

        auto rpcServer = Bootstrap_->GetRpcServer();

        if (TransactionSupervisor_) {
            rpcServer->UnregisterService(TransactionSupervisor_->GetRpcService());
        }
        TransactionSupervisor_.Reset();

        if (HiveManager_) {
            rpcServer->UnregisterService(HiveManager_->GetRpcService());
        }
        HiveManager_.Reset();

        if (TabletService_) {
            rpcServer->UnregisterService(TabletService_);
        }
        TabletService_.Reset();

        TabletManager_.Reset();
    }


    void BuildOrchidYsonControl(IYsonConsumer* consumer)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        BuildYsonMapFluently(consumer)
            .Item("state").Value(GetControlState())
            .Item("prerequisite_transaction_id").Value(PrerequisiteTransactionId_)
            .Item("options").Value(*Options_);
    }

    void BuildOrchidYsonAutomaton(IYsonConsumer* consumer)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!HydraManager_)
            return;
        
        auto epochContext = HydraManager_->GetControlEpochContext();
        if (!epochContext)
            return;

        auto cancelableContext = epochContext->CancelableContext;
        WaitFor(BIND(&TImpl::DoBuildOrchidYsonAutomaton, MakeStrong(this))
            .AsyncVia(GetGuardedAutomatonInvoker())
            .Run(cancelableContext, consumer));
    }

    void DoBuildOrchidYsonAutomaton(TCancelableContextPtr context, IYsonConsumer* consumer)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        // Make sure we're still using the same context.
        // Otherwise cell id, which has already been printed, might be wrong.
        if (context->IsCanceled())
            return;

        BuildYsonMapFluently(consumer)
            .Item("transactions").Do(BIND(&TTransactionManager::BuildOrchidYson, TransactionManager_))
            .Item("tablets").Do(BIND(&TTabletManager::BuildOrchidYson, TabletManager_))
            .Item("hive").Do(BIND(&THiveManager::BuildOrchidYson, HiveManager_));
    }


    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

};

////////////////////////////////////////////////////////////////////////////////

TTabletSlot::TTabletSlot(
    int slotIndex,
    TTabletNodeConfigPtr config,
    NCellNode::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(
        this,
        slotIndex,
        config,
        bootstrap))
{ }

TTabletSlot::~TTabletSlot()
{ }

int TTabletSlot::GetIndex() const
{
    return Impl_->GetIndex();
}

const TCellId& TTabletSlot::GetCellId() const
{
    return Impl_->GetCellId();
}

EPeerState TTabletSlot::GetControlState() const
{
    return Impl_->GetControlState();
}

EPeerState TTabletSlot::GetAutomatonState() const
{
    return Impl_->GetAutomatonState();
}

TPeerId TTabletSlot::GetPeerId() const
{
    return Impl_->GetPeerId();
}

int TTabletSlot::GetCellConfigVersion() const
{
    return Impl_->GetCellConfigVersion();
}

TTabletCellConfigPtr TTabletSlot::GetCellConfig() const
{
    return Impl_->GetCellConfig();
}

const TTransactionId& TTabletSlot::GetPrerequisiteTransactionId() const
{
    return Impl_->GetPrerequisiteTransactionId();
}

IHydraManagerPtr TTabletSlot::GetHydraManager() const
{
    return Impl_->GetHydraManager();
}

IResponseKeeperPtr TTabletSlot::GetResponseKeeper() const
{
    return Impl_->GetResponseKeeper();
}

TTabletAutomatonPtr TTabletSlot::GetAutomaton() const
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

THiveManagerPtr TTabletSlot::GetHiveManager() const
{
    return Impl_->GetHiveManager();
}

TMailbox* TTabletSlot::GetMasterMailbox()
{
    return Impl_->GetMasterMailbox();
}

TTransactionManagerPtr TTabletSlot::GetTransactionManager() const
{
    return Impl_->GetTransactionManager();
}

TTransactionSupervisorPtr TTabletSlot::GetTransactionSupervisor() const
{
    return Impl_->GetTransactionSupervisor();
}

TTabletManagerPtr TTabletSlot::GetTabletManager() const
{
    return Impl_->GetTabletManager();
}

TObjectId TTabletSlot::GenerateId(EObjectType type)
{
    return Impl_->GenerateId(type);
}

void TTabletSlot::Initialize(const TCreateTabletSlotInfo& createInfo)
{
    Impl_->Initialize(createInfo);
}

void TTabletSlot::Configure(const TConfigureTabletSlotInfo& configureInfo)
{
    Impl_->Configure(configureInfo);
}

void TTabletSlot::Finalize()
{
    Impl_->Finalize();
}

void TTabletSlot::BuildOrchidYson(IYsonConsumer* consumer)
{
    return Impl_->BuildOrchidYson(consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NTabletNode
