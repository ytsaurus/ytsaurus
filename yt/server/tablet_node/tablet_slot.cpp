#include "tablet_slot.h"
#include "private.h"
#include "automaton.h"
#include "config.h"
#include "serialize.h"
#include "slot_manager.h"
#include "tablet_manager.h"
#include "tablet_service.h"
#include "transaction_manager.h"

#include <yt/server/data_node/config.h>

#include <yt/server/election/election_manager.h>

#include <yt/server/hive/hive_manager.h>
#include <yt/server/hive/mailbox.h>
#include <yt/server/hive/transaction_supervisor.h>

#include <yt/server/hydra/changelog.h>
#include <yt/server/hydra/distributed_hydra_manager.h>
#include <yt/server/hydra/hydra_manager.h>
#include <yt/server/hydra/remote_changelog_store.h>
#include <yt/server/hydra/remote_snapshot_store.h>
#include <yt/server/hydra/snapshot.h>

#include <yt/server/election/election_manager.h>
#include <yt/server/election/election_manager_thunk.h>

#include <yt/server/hydra/changelog.h>
#include <yt/server/hydra/remote_changelog_store.h>
#include <yt/server/hydra/snapshot.h>
#include <yt/server/hydra/remote_snapshot_store.h>
#include <yt/server/hydra/hydra_manager.h>
#include <yt/server/hydra/distributed_hydra_manager.h>
#include <yt/server/hydra/changelog_store_factory_thunk.h>
#include <yt/server/hydra/snapshot_store_thunk.h>

#include <yt/server/hive/hive_manager.h>
#include <yt/server/hive/mailbox.h>
#include <yt/server/hive/transaction_supervisor.h>

#include <yt/server/cell_node/bootstrap.h>

#include <yt/ytlib/api/client.h>
#include <yt/ytlib/api/connection.h>

#include <yt/ytlib/transaction_client/timestamp_provider.h>

#include <yt/ytlib/tablet_client/config.h>

#include <yt/ytlib/api/connection.h>
#include <yt/ytlib/api/client.h>
#include <yt/ytlib/api/transaction.h>

#include <yt/ytlib/election/cell_manager.h>

#include <yt/core/concurrency/fair_share_action_queue.h>
#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/logging/log.h>

#include <yt/core/rpc/response_keeper.h>
#include <yt/core/rpc/server.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/virtual.h>
#include <yt/core/ytree/helpers.h>

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
using namespace NQueryClient;
using namespace NApi;

using NHydra::EPeerState;

////////////////////////////////////////////////////////////////////////////////

class TTabletSlot::TElectionManager
    : public IElectionManager
{
public:
    TElectionManager(
        IInvokerPtr controlInvoker,
        IElectionCallbacksPtr callbacks,
        TCellManagerPtr cellManager)
        : ControlInvoker_(std::move(controlInvoker))
        , Callbacks_(std::move(callbacks))
        , CellManager_(std::move(cellManager))
    {
        CellManager_->SubscribePeerReconfigured(
            BIND(&TElectionManager::OnPeerReconfigured, MakeWeak(this))
                .Via(ControlInvoker_));
    }

    virtual void Initialize() override
    { }

    virtual void Finalize() override
    {
        Abandon();
    }

    virtual void Participate() override
    {
        ControlInvoker_->Invoke(BIND(&TElectionManager::DoParticipate, MakeStrong(this)));
    }

    virtual void Abandon() override
    {
        ControlInvoker_->Invoke(BIND(&TElectionManager::DoAbandon, MakeStrong(this)));
    }

    virtual TYsonProducer GetMonitoringProducer() override
    {
        YUNREACHABLE();
    }

    void SetEpochId(const TEpochId& epochId)
    {
        YCHECK(epochId);
        EpochId_ = epochId;
    }

private:
    const IInvokerPtr ControlInvoker_;
    const IElectionCallbacksPtr Callbacks_;
    const TCellManagerPtr CellManager_;

    TEpochId EpochId_;
    TEpochContextPtr EpochContext_;


    void DoParticipate()
    {
        DoAbandon();

        EpochContext_ = New<TEpochContext>();
        EpochContext_->LeaderId = GetLeaderId();
        EpochContext_->EpochId = EpochId_;
        EpochContext_->StartTime = TInstant::Now();

        if (IsLeader()) {
            Callbacks_->OnStartLeading(EpochContext_);
        } else {
            Callbacks_->OnStartFollowing(EpochContext_);
        }
    }

    void DoAbandon()
    {
        if (!EpochContext_)
            return;

        EpochContext_->CancelableContext->Cancel();

        if (IsLeader()) {
            Callbacks_->OnStopLeading();
        } else {
            Callbacks_->OnStopFollowing();
        }

        EpochContext_.Reset();
    }

    void OnPeerReconfigured(TPeerId peerId)
    {
        if (!EpochContext_)
            return;

        if (peerId == CellManager_->GetSelfPeerId() || EpochContext_->LeaderId == peerId) {
            DoAbandon();
        }
    }

    TPeerId GetLeaderId()
    {
        for (auto peerId = 0; peerId < CellManager_->GetTotalPeerCount(); ++peerId) {
            const auto& peerConfig = CellManager_->GetPeerConfig(peerId);
            if (peerConfig.Voting) {
                return peerId;
            }
        }
        YUNREACHABLE();
    }

    bool IsLeader()
    {
        return EpochContext_ && EpochContext_->LeaderId == CellManager_->GetSelfPeerId();
    }

};

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
        , SnapshotQueue_(New<TActionQueue>(
            Format("TabletSnap:%v", SlotIndex_)))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(GetAutomatonInvoker(), AutomatonThread);

        Logger.AddTag("Slot: %v", SlotIndex_);
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

        return CellDescriptor_.CellId;
    }

    EPeerState GetControlState() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (Finalizing_) {
            return EPeerState::Stopped;
        }

        if (HydraManager_) {
            return HydraManager_->GetControlState();
        }

        if (Initialized_) {
            return EPeerState::Stopped;
        }

        return EPeerState::None;
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

    const TCellDescriptor& GetCellDescriptor() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return CellDescriptor_;
    }

    IHydraManagerPtr GetHydraManager() const
    {
        return HydraManager_;
    }

    TResponseKeeperPtr GetResponseKeeper() const
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

    IInvokerPtr GetSnapshotInvoker() const
    {
        return SnapshotQueue_->GetInvoker();
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

    TTransactionId GetPrerequisiteTransactionId() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return PrerequisiteTransactionId_;
    }

    TTabletCellOptionsPtr GetOptions() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return Options_;
    }

    TObjectId GenerateId(EObjectType type)
    {
        auto* mutationContext = GetCurrentMutationContext();
        auto version = mutationContext->GetVersion();
        auto random = mutationContext->RandomGenerator().Generate<ui64>();
        auto cellId = GetCellId();
        return TObjectId(
            random ^ cellId.Parts32[0],
            (cellId.Parts32[1] & 0xffff0000) + static_cast<int>(type),
            version.RecordId,
            version.SegmentId);
    }

    TColumnEvaluatorCachePtr GetColumnEvaluatorCache() const
    {
        return Bootstrap_->GetColumnEvaluatorCache();
    }

    void Initialize(const TCreateTabletSlotInfo& createInfo)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(!Initialized_);

        CellDescriptor_.CellId = FromProto<TCellId>(createInfo.cell_id());
        PeerId_ = createInfo.peer_id();
        Options_ = ConvertTo<TTabletCellOptionsPtr>(TYsonString(createInfo.options()));

        Initialized_ = true;

        Logger.AddTag("CellId: %v, PeerId: %v",
            CellDescriptor_.CellId,
            PeerId_);

        LOG_INFO("Slot initialized");
    }


    bool CanConfigure() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return Initialized_ && !Finalizing_;
    }

    void Configure(const TConfigureTabletSlotInfo& configureInfo)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(CanConfigure());

        auto client = Bootstrap_->GetMasterClient();

        CellDescriptor_ = FromProto<TCellDescriptor>(configureInfo.cell_descriptor());

        auto newPrerequisiteTransactionId = FromProto<TTransactionId>(configureInfo.prerequisite_transaction_id());
        if (newPrerequisiteTransactionId != PrerequisiteTransactionId_) {
            LOG_INFO("Prerequisite transaction updated (TransactionId: %v -> %v)",
                PrerequisiteTransactionId_,
                newPrerequisiteTransactionId);
            PrerequisiteTransactionId_ = newPrerequisiteTransactionId;
        }

        PrerequisiteTransaction_.Reset();
        // NB: Prerequisite transaction is only attached by leaders.
        if (PrerequisiteTransactionId_ && CellDescriptor_.Peers[PeerId_].GetVoting()) {
            TTransactionAttachOptions attachOptions;
            attachOptions.Ping = false;
            PrerequisiteTransaction_ = client->AttachTransaction(PrerequisiteTransactionId_, attachOptions);
            LOG_INFO("Prerequisite transaction attached (TransactionId: %v)",
                PrerequisiteTransactionId_);
        }

        auto snapshotStore = CreateRemoteSnapshotStore(
            Config_->Snapshots,
            Options_,
            Format("//sys/tablet_cells/%v/snapshots", GetCellId()),
            Bootstrap_->GetMasterClient(),
            PrerequisiteTransaction_ ? PrerequisiteTransaction_->GetId() : NullTransactionId);
        SnapshotStoreThunk_->SetUnderlying(snapshotStore);

        auto changelogStoreFactory = CreateRemoteChangelogStoreFactory(
            Config_->Changelogs,
            Options_,
            Format("//sys/tablet_cells/%v/changelogs", GetCellId()),
            Bootstrap_->GetMasterClient(),
            PrerequisiteTransaction_ ? PrerequisiteTransaction_->GetId() : NullTransactionId);
        ChangelogStoreFactoryThunk_->SetUnderlying(changelogStoreFactory);

        auto cellConfig = CellDescriptor_.ToConfig(Bootstrap_->GetLocalNetworks());

        if (HydraManager_) {
            ElectionManager_->SetEpochId(PrerequisiteTransactionId_);
            CellManager_->Reconfigure(cellConfig);

            LOG_INFO("Slot reconfigured (ConfigVersion: %v)",
                CellDescriptor_.ConfigVersion);
        } else {
            CellManager_ = New<TCellManager>(
                cellConfig,
                Bootstrap_->GetTabletChannelFactory(),
                PeerId_);

            Automaton_ = New<TTabletAutomaton>(
                Owner_,
                GetSnapshotInvoker());

            auto rpcServer = Bootstrap_->GetRpcServer();

            ResponseKeeper_ = New<TResponseKeeper>(
                Config_->HydraManager->ResponseKeeper,
                GetAutomatonInvoker(),
                Logger,
                TabletNodeProfiler);

            TDistributedHydraManagerOptions hydraManagerOptions;
            hydraManagerOptions.ResponseKeeper = ResponseKeeper_;
            hydraManagerOptions.UseFork = false;
            hydraManagerOptions.WriteChangelogsAtFollowers = false;
            hydraManagerOptions.WriteSnapshotsAtFollowers = false;
            HydraManager_ = CreateDistributedHydraManager(
                Config_->HydraManager,
                Bootstrap_->GetControlInvoker(),
                GetAutomatonInvoker(EAutomatonThreadQueue::Mutation),
                Automaton_,
                rpcServer,
                ElectionManagerThunk_,
                CellManager_,
                ChangelogStoreFactoryThunk_,
                SnapshotStoreThunk_,
                hydraManagerOptions);

            HydraManager_->SubscribeStartLeading(BIND(&TImpl::OnStartEpoch, MakeWeak(this)));
            HydraManager_->SubscribeStartFollowing(BIND(&TImpl::OnStartEpoch, MakeWeak(this)));
            
            HydraManager_->SubscribeStopLeading(BIND(&TImpl::OnStopEpoch, MakeWeak(this)));
            HydraManager_->SubscribeStopFollowing(BIND(&TImpl::OnStopEpoch, MakeWeak(this)));

            HydraManager_->SubscribeLeaderLeaseCheck(
                BIND(&TImpl::OnLeaderLeaseCheckThunk, MakeWeak(this))
                    .AsyncVia(Bootstrap_->GetControlInvoker()));

            {
                TGuard<TSpinLock> guard(InvokersSpinLock_);
                for (auto queue : TEnumTraits<EAutomatonThreadQueue>::GetDomainValues()) {
                    auto unguardedInvoker = GetAutomatonInvoker(queue);
                    GuardedAutomatonInvokers_[queue] = HydraManager_->CreateGuardedAutomatonInvoker(unguardedInvoker);
                }
            }

            ElectionManager_ = New<TElectionManager>(
                Bootstrap_->GetControlInvoker(),
                HydraManager_->GetElectionCallbacks(),
                CellManager_);
            ElectionManager_->SetEpochId(PrerequisiteTransactionId_);
            ElectionManager_->Initialize();

            ElectionManagerThunk_->SetUnderlying(ElectionManager_);

            auto masterConnection = Bootstrap_->GetMasterClient()->GetConnection();
            HiveManager_ = New<THiveManager>(
                Config_->HiveManager,
                masterConnection->GetCellDirectory(),
                GetCellId(),
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

            OrchidService_ = CreateOrchidService();

            LOG_INFO("Slot configured (ConfigVersion: %v)",
                CellDescriptor_.ConfigVersion);
        }
    }

    TFuture<void> Finalize()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (Finalizing_) {
            return FinalizeResult_;
        }

        LOG_INFO("Finalizing slot");

        auto slotManager = Bootstrap_->GetTabletSlotManager();
        slotManager->UnregisterTabletSnapshots(Owner_);

        ResetEpochInvokers();
        ResetGuardedInvokers();

        Finalizing_ = true;
        FinalizeResult_ = BIND(&TImpl::DoFinalize, MakeStrong(this))
            .AsyncVia(Bootstrap_->GetControlInvoker())
            .Run();

        return FinalizeResult_;
    }


    IYPathServicePtr GetOrchidService()
    {
        return OrchidService_;
    }

private:
    TTabletSlot* const Owner_;
    const int SlotIndex_;
    const TTabletNodeConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;

    const TFairShareActionQueuePtr AutomatonQueue_;
    const TActionQueuePtr SnapshotQueue_;

    const TElectionManagerThunkPtr ElectionManagerThunk_ = New<TElectionManagerThunk>();
    const TSnapshotStoreThunkPtr SnapshotStoreThunk_ = New<TSnapshotStoreThunk>();
    const TChangelogStoreFactoryThunkPtr ChangelogStoreFactoryThunk_ = New<TChangelogStoreFactoryThunk>();

    TPeerId PeerId_ = InvalidPeerId;
    TCellDescriptor CellDescriptor_;
    TTabletCellOptionsPtr Options_;
    TTransactionId PrerequisiteTransactionId_;
    ITransactionPtr PrerequisiteTransaction_;  // only created for leaders

    TCellManagerPtr CellManager_;

    TElectionManagerPtr ElectionManager_;

    IHydraManagerPtr HydraManager_;

    TResponseKeeperPtr ResponseKeeper_;
    
    THiveManagerPtr HiveManager_;

    TTabletManagerPtr TabletManager_;

    TTransactionManagerPtr TransactionManager_;
    TTransactionSupervisorPtr TransactionSupervisor_;

    NRpc::IServicePtr TabletService_;

    TTabletAutomatonPtr Automaton_;

    TSpinLock InvokersSpinLock_;
    TEnumIndexedVector<IInvokerPtr, EAutomatonThreadQueue> EpochAutomatonInvokers_;
    TEnumIndexedVector<IInvokerPtr, EAutomatonThreadQueue> GuardedAutomatonInvokers_;

    bool Initialized_ = false;
    bool Finalizing_ = false;
    TFuture<void> FinalizeResult_;

    NLogging::TLogger Logger = TabletNodeLogger;

    IYPathServicePtr OrchidService_;


    IYPathServicePtr CreateOrchidService()
    {
        return New<TCompositeMapService>()
            ->AddAttribute("opaque", BIND([] (IYsonConsumer* consumer) {
                    BuildYsonFluently(consumer)
                        .Value(true);
                }))
            ->AddChild("state", IYPathService::FromMethod(
                &TImpl::GetControlState,
                MakeWeak(this)))
            ->AddChild("prerequisite_transaction_id", IYPathService::FromMethod(
                &TImpl::GetPrerequisiteTransactionId,
                MakeWeak(this)))
            ->AddChild("options", IYPathService::FromMethod(
                &TImpl::GetOptions,
                MakeWeak(this)))
            ->AddChild("transactions", TransactionManager_->GetOrchidService())
            ->AddChild("tablets", TabletManager_->GetOrchidService())
            ->AddChild("hive", HiveManager_->GetOrchidService())
            ->Via(Bootstrap_->GetControlInvoker());
    }

    void ResetEpochInvokers()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(InvokersSpinLock_);
        std::fill(EpochAutomatonInvokers_.begin(), EpochAutomatonInvokers_.end(), GetNullInvoker());
    }

    void ResetGuardedInvokers()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(InvokersSpinLock_);
        std::fill(GuardedAutomatonInvokers_.begin(), GuardedAutomatonInvokers_.end(), GetNullInvoker());
    }


    void OnStartEpoch()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(InvokersSpinLock_);
        for (auto queue : TEnumTraits<EAutomatonThreadQueue>::GetDomainValues()) {
            EpochAutomatonInvokers_[queue] = HydraManager_
                ->GetAutomatonCancelableContext()
                ->CreateInvoker(GetAutomatonInvoker(queue));
        }
    }

    void OnStopEpoch()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        ResetEpochInvokers();
    }


    static TFuture<void> OnLeaderLeaseCheckThunk(TWeakPtr<TImpl> weakThis)
    {
        auto this_ = weakThis.Lock();
        return this_ ? this_->OnLeaderLeaseCheck() : VoidFuture;
    }

    TFuture<void> OnLeaderLeaseCheck()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (PrerequisiteTransaction_) {
            LOG_DEBUG("Checking prerequisite transaction");
            return PrerequisiteTransaction_->Ping();
        } else {
            return MakeFuture<void>(TError("No prerequisite transaction is attached"));
        }
    }


    void DoFinalize()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        CellManager_.Reset();

        // Stop everything and release the references to break cycles.
        if (HydraManager_) {
            WaitFor(HydraManager_->Finalize())
                .ThrowOnError();
        }
        HydraManager_.Reset();

        if (ElectionManager_) {
            ElectionManager_->Finalize();
        }
        ElectionManager_.Reset();

        Automaton_.Reset();

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

const TCellDescriptor& TTabletSlot::GetCellDescriptor() const
{
    return Impl_->GetCellDescriptor();
}

IHydraManagerPtr TTabletSlot::GetHydraManager() const
{
    return Impl_->GetHydraManager();
}

TResponseKeeperPtr TTabletSlot::GetResponseKeeper() const
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

IInvokerPtr TTabletSlot::GetSnapshotInvoker() const
{
    return Impl_->GetSnapshotInvoker();
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

TColumnEvaluatorCachePtr TTabletSlot::GetColumnEvaluatorCache() const
{
    return Impl_->GetColumnEvaluatorCache();
}

void TTabletSlot::Initialize(const TCreateTabletSlotInfo& createInfo)
{
    Impl_->Initialize(createInfo);
}

bool TTabletSlot::CanConfigure() const
{
    return Impl_->CanConfigure();
}

void TTabletSlot::Configure(const TConfigureTabletSlotInfo& configureInfo)
{
    Impl_->Configure(configureInfo);
}

TFuture<void> TTabletSlot::Finalize()
{
    return Impl_->Finalize();
}

IYPathServicePtr TTabletSlot::GetOrchidService()
{
    return Impl_->GetOrchidService();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NTabletNode
