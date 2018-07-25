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
#include <yt/server/hive/transaction_participant_provider.h>

#include <yt/server/cell_node/bootstrap.h>

#include <yt/server/misc/interned_attributes.h>

#include <yt/ytlib/api/native/connection.h>
#include <yt/ytlib/api/native/client.h>

#include <yt/client/api/connection.h>
#include <yt/client/api/client.h>
#include <yt/client/api/transaction.h>

#include <yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/client/transaction_client/timestamp_provider.h>

#include <yt/ytlib/tablet_client/config.h>

#include <yt/ytlib/election/cell_manager.h>

#include <yt/core/concurrency/fair_share_action_queue.h>
#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profile_manager.h>

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
using namespace NHiveClient;
using namespace NHiveServer;
using namespace NTabletClient;
using namespace NTabletClient::NProto;
using namespace NObjectClient;
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
        Y_UNREACHABLE();
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
        if (!EpochContext_) {
            return;
        }

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
        Y_UNREACHABLE();
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
        const TCreateTabletSlotInfo& createInfo,
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
        , PeerId_(createInfo.peer_id())
        , CellDescriptor_(FromProto<TCellId>(createInfo.cell_id()))
        , TabletCellBundle(createInfo.tablet_cell_bundle())
        , ProfilingTagIds_{
            NProfiling::TProfileManager::Get()->RegisterTag(
                "cell_id",
                CellDescriptor_.CellId),
            NProfiling::TProfileManager::Get()->RegisterTag(
                "peer_id",
                PeerId_),
            NProfiling::TProfileManager::Get()->RegisterTag(
                "tablet_cell_bundle",
                TabletCellBundle ? TabletCellBundle : UnknownProfilingTag)
        }
        , OptionsString_(TYsonString(createInfo.options()))
        , Logger(NLogging::TLogger(TabletNodeLogger)
            .AddTag("CellId: %v, PeerId: %v",
                CellDescriptor_.CellId,
                PeerId_))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(GetAutomatonInvoker(), AutomatonThread);

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

    const IHydraManagerPtr& GetHydraManager() const
    {
        return HydraManager_;
    }

    const TResponseKeeperPtr& GetResponseKeeper() const
    {
        return ResponseKeeper_;
    }

    const TTabletAutomatonPtr& GetAutomaton() const
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

    const THiveManagerPtr& GetHiveManager() const
    {
        return HiveManager_;
    }

    TMailbox* GetMasterMailbox()
    {
        // Create master mailbox lazily.
        auto masterCellId = Bootstrap_->GetCellId();
        return HiveManager_->GetOrCreateMailbox(masterCellId);
    }

    const TTransactionManagerPtr& GetTransactionManager() const
    {
        return TransactionManager_;
    }

    const TTransactionSupervisorPtr& GetTransactionSupervisor() const
    {
        return TransactionSupervisor_;
    }

    const TTabletManagerPtr& GetTabletManager() const
    {
        return TabletManager_;
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

    void Initialize()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(!Initialized_);

        Options_ = ConvertTo<TTabletCellOptionsPtr>(OptionsString_);

        Initialized_ = true;

        LOG_INFO("Slot initialized");
    }


    bool CanConfigure() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return Initialized_ && !Finalizing_;
    }

    int GetDynamicConfigVersion() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return DynamicConfigVersion_;
    }

    void UpdateDynamicConfig(const TUpdateTabletSlotInfo& updateInfo)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(CanConfigure());

        try {
            TDynamicTabletCellOptionsPtr dynamicOptions;

            if (updateInfo.has_dynamic_options()) {
                dynamicOptions = New<TDynamicTabletCellOptions>();
                dynamicOptions->SetUnrecognizedStrategy(EUnrecognizedStrategy::Keep);
                dynamicOptions->Load(ConvertTo<INodePtr>(TYsonString(updateInfo.dynamic_options())));
                auto unrecognized = dynamicOptions->GetUnrecognized();

                if (unrecognized->GetChildCount() > 0) {
                    THROW_ERROR_EXCEPTION("Dynamic options contains unrecognized parameters (Unrecognized: %v)",
                        ConvertToYsonString(unrecognized, EYsonFormat::Text).GetData());
                }
            }

            DynamicOptions_ = std::move(dynamicOptions);
            DynamicConfigVersion_ = updateInfo.dynamic_config_version();

            LOG_DEBUG("Updated dynamic config (DynamicConfigVersion: %v)",
                DynamicConfigVersion_);

        } catch (const std::exception& ex) {
            // TODO(savrus): Write this to tablet cell errors once we have them.
            LOG_ERROR(ex, "Error while updating dynamic config");
        }
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
            PrerequisiteTransaction_ ? PrerequisiteTransaction_->GetId() : NullTransactionId,
            ProfilingTagIds_);
        ChangelogStoreFactoryThunk_->SetUnderlying(changelogStoreFactory);

        auto cellConfig = CellDescriptor_.ToConfig(Bootstrap_->GetLocalNetworks());

        if (HydraManager_) {
            ElectionManager_->SetEpochId(PrerequisiteTransactionId_);
            CellManager_->Reconfigure(cellConfig);

            LOG_INFO("Slot reconfigured (ConfigVersion: %v)",
                CellDescriptor_.ConfigVersion);

            ElectionManager_->Abandon();
        } else {
            auto channelFactory = Bootstrap_
                ->GetMasterClient()
                ->GetNativeConnection()
                ->GetChannelFactory();
            CellManager_ = New<TCellManager>(
                cellConfig,
                channelFactory,
                PeerId_);

            Automaton_ = New<TTabletAutomaton>(
                Owner_,
                SnapshotQueue_->GetInvoker());

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
            hydraManagerOptions.ProfilingTagIds = ProfilingTagIds_;
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

            auto masterConnection = Bootstrap_->GetMasterClient()->GetNativeConnection();
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

            auto connection = Bootstrap_->GetMasterClient()->GetNativeConnection();
            connection->GetClusterDirectorySynchronizer()->Start();
            TransactionSupervisor_ = New<TTransactionSupervisor>(
                Config_->TransactionSupervisor,
                GetAutomatonInvoker(),
                Bootstrap_->GetTransactionTrackerInvoker(),
                HydraManager_,
                Automaton_,
                GetResponseKeeper(),
                TransactionManager_,
                GetCellId(),
                connection->GetTimestampProvider(),
                std::vector<ITransactionParticipantProviderPtr>{
                    CreateTransactionParticipantProvider(connection),
                    CreateTransactionParticipantProvider(connection->GetClusterDirectory())
                });

            TabletService_ = CreateTabletService(
                Owner_,
                Bootstrap_);

            TabletManager_->Initialize();

            HydraManager_->Initialize();

            for (const auto& service : TransactionSupervisor_->GetRpcServices()) {
                rpcServer->RegisterService(service);
            }
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

    const IYPathServicePtr& GetOrchidService()
    {
        return OrchidService_;
    }

    const NProfiling::TTagIdList& GetProfilingTagIds()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return ProfilingTagIds_;
    }

    const TRuntimeTabletCellDataPtr& GetRuntimeData() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return RuntimeData_;
    }

    double GetUsedCpu(double cpuPerTabletSlot) const
    {
        return DynamicOptions_->CpuPerTabletSlot.Get(cpuPerTabletSlot);
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

    const TPeerId PeerId_;
    TCellDescriptor CellDescriptor_;

    const TString TabletCellBundle;

    const NProfiling::TTagIdList ProfilingTagIds_;

    const TYsonString OptionsString_;
    TTabletCellOptionsPtr Options_;
    TDynamicTabletCellOptionsPtr DynamicOptions_ = New<TDynamicTabletCellOptions>();

    int DynamicConfigVersion_ = -1;

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

    const TRuntimeTabletCellDataPtr RuntimeData_ = New<TRuntimeTabletCellData>();

    bool Initialized_ = false;
    bool Finalizing_ = false;
    TFuture<void> FinalizeResult_;

    IYPathServicePtr OrchidService_;

    NLogging::TLogger Logger;


    IYPathServicePtr CreateOrchidService()
    {
        return New<TCompositeMapService>()
            ->AddAttribute(EInternedAttributeKey::Opaque, BIND([] (IYsonConsumer* consumer) {
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
            ->AddChild("dynamic_options", IYPathService::FromMethod(
                &TImpl::GetDynamicOptions,
                MakeWeak(this)))
            ->AddChild("dynamic_config_version", IYPathService::FromMethod(
                &TImpl::GetDynamicConfigVersion,
                MakeWeak(this)))
            ->AddChild("memory_usage", IYPathService::FromMethod(
                &TImpl::GetMemoryUsage,
                MakeWeak(this)))
            ->AddChild("transactions", TransactionManager_->GetOrchidService())
            ->AddChild("tablets", TabletManager_->GetOrchidService())
            ->AddChild("hive", HiveManager_->GetOrchidService())
            ->Via(Bootstrap_->GetControlInvoker());
    }

    const TTransactionId& GetPrerequisiteTransactionId() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return PrerequisiteTransactionId_;
    }

    const TTabletCellOptionsPtr& GetOptions() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return Options_;
    }

    const TDynamicTabletCellOptionsPtr& GetDynamicOptions() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return DynamicOptions_;
    }

    TYsonString GetMemoryUsage() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return BuildYsonStringFluently()
            .BeginMap()
                .Item("dynamic_stores").Value(TabletManager_->GetDynamicStoresMemoryUsage())
                .Item("static_stores").Value(TabletManager_->GetStaticStoresMemoryUsage())
                .Item("write_log").Value(TabletManager_->GetWriteLogsMemoryUsage())
            .EndMap();
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
            for (const auto& service : TransactionSupervisor_->GetRpcServices()) {
                rpcServer->UnregisterService(service);
            }
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
    const NTabletClient::NProto::TCreateTabletSlotInfo& createInfo,
    NCellNode::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(
        this,
        slotIndex,
        config,
        createInfo,
        bootstrap))
{ }

TTabletSlot::~TTabletSlot() = default;

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

const IHydraManagerPtr& TTabletSlot::GetHydraManager() const
{
    return Impl_->GetHydraManager();
}

const TResponseKeeperPtr& TTabletSlot::GetResponseKeeper() const
{
    return Impl_->GetResponseKeeper();
}

const TTabletAutomatonPtr& TTabletSlot::GetAutomaton() const
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

const TTransactionSupervisorPtr& TTabletSlot::GetTransactionSupervisor() const
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

void TTabletSlot::Initialize()
{
    Impl_->Initialize();
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

const IYPathServicePtr& TTabletSlot::GetOrchidService()
{
    return Impl_->GetOrchidService();
}

const NProfiling::TTagIdList& TTabletSlot::GetProfilingTagIds()
{
    return Impl_->GetProfilingTagIds();
}

const TRuntimeTabletCellDataPtr& TTabletSlot::GetRuntimeData() const
{
    return Impl_->GetRuntimeData();
}

double TTabletSlot::GetUsedCpu(double cpuPerTabletSlot) const
{
    return Impl_->GetUsedCpu(cpuPerTabletSlot);
}

int TTabletSlot::GetDynamicConfigVersion() const
{
    return Impl_->GetDynamicConfigVersion();
}

void TTabletSlot::UpdateDynamicConfig(const NTabletClient::NProto::TUpdateTabletSlotInfo& updateInfo)
{
    Impl_->UpdateDynamicConfig(updateInfo);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NTabletNode
