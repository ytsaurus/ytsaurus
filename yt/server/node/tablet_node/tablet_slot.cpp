#include "automaton.h"
#include "private.h"
#include "security_manager.h"
#include "serialize.h"
#include "slot_manager.h"
#include "tablet_manager.h"
#include "tablet_service.h"
#include "tablet_slot.h"
#include "transaction_manager.h"

#include <yt/server/node/data_node/config.h>

#include <yt/server/lib/election/election_manager.h>
#include <yt/server/lib/election/distributed_election_manager.h>

#include <yt/server/lib/hive/hive_manager.h>
#include <yt/server/lib/hive/mailbox.h>
#include <yt/server/lib/hive/transaction_supervisor.h>

#include <yt/server/lib/hydra/changelog.h>
#include <yt/server/lib/hydra/distributed_hydra_manager.h>
#include <yt/server/lib/hydra/hydra_manager.h>
#include <yt/server/lib/hydra/remote_changelog_store.h>
#include <yt/server/lib/hydra/remote_snapshot_store.h>
#include <yt/server/lib/hydra/snapshot.h>

#include <yt/server/lib/election/election_manager.h>
#include <yt/server/lib/election/election_manager_thunk.h>

#include <yt/server/lib/hydra/changelog.h>
#include <yt/server/lib/hydra/remote_changelog_store.h>
#include <yt/server/lib/hydra/snapshot.h>
#include <yt/server/lib/hydra/remote_snapshot_store.h>
#include <yt/server/lib/hydra/hydra_manager.h>
#include <yt/server/lib/hydra/distributed_hydra_manager.h>
#include <yt/server/lib/hydra/changelog_store_factory_thunk.h>
#include <yt/server/lib/hydra/snapshot_store_thunk.h>

#include <yt/server/lib/hive/hive_manager.h>
#include <yt/server/lib/hive/mailbox.h>
#include <yt/server/lib/hive/transaction_supervisor.h>
#include <yt/server/lib/hive/transaction_participant_provider.h>

#include <yt/server/lib/tablet_node/config.h>

#include <yt/server/node/cluster_node/bootstrap.h>

#include <yt/server/node/data_node/master_connector.h>

#include <yt/server/lib/misc/interned_attributes.h>

#include <yt/ytlib/api/native/connection.h>
#include <yt/ytlib/api/native/client.h>

#include <yt/client/api/connection.h>
#include <yt/client/api/client.h>
#include <yt/client/api/transaction.h>

#include <yt/client/object_client/helpers.h>

#include <yt/client/security_client/public.h>

#include <yt/client/transaction_client/timestamp_provider.h>

#include <yt/ytlib/hive/cluster_directory_synchronizer.h>

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

namespace NYT::NTabletNode {

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

class TTabletSlot::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TTabletSlot* owner,
        int slotIndex,
        TTabletNodeConfigPtr config,
        const TCreateTabletSlotInfo& createInfo,
        NClusterNode::TBootstrap* bootstrap)
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
        , TabletCellBundle_(createInfo.tablet_cell_bundle())
        , ProfilingTagIds_{
            NProfiling::TProfileManager::Get()->RegisterTag(
                "cell_id",
                CellDescriptor_.CellId),
            NProfiling::TProfileManager::Get()->RegisterTag(
                "tablet_cell_bundle",
                TabletCellBundle_ ? TabletCellBundle_ : UnknownProfilingTag)
        }
        , Options_(ConvertTo<TTabletCellOptionsPtr>(TYsonString(createInfo.options())))
        , Logger(GetLogger())
    {
        VERIFY_INVOKER_THREAD_AFFINITY(GetAutomatonInvoker(), AutomatonThread);

        ResetEpochInvokers();
        ResetGuardedInvokers();
    }

    int GetIndex() const
    {
        return SlotIndex_;
    }

    TCellId GetCellId() const
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

        if (auto hydraManager = GetHydraManager()) {
            return hydraManager->GetControlState();
        }

        if (Initialized_) {
            return EPeerState::Stopped;
        }

        return EPeerState::None;
    }

    int GetConfigVersion() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return CellDescriptor_.ConfigVersion;
    }

    EPeerState GetAutomatonState() const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto hydraManager = GetHydraManager();
        return hydraManager ? hydraManager->GetAutomatonState() : EPeerState::None;
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

    const IDistributedHydraManagerPtr& GetHydraManager() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(HydraManagerLock_);
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

        TGuard<TSpinLock> guard(InvokersLock_);
        return EpochAutomatonInvokers_[queue];
    }

    IInvokerPtr GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(InvokersLock_);
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
        YT_VERIFY(!Initialized_);

        Initialized_ = true;

        YT_LOG_INFO("Slot initialized");
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
        YT_VERIFY(CanConfigure());

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
                        ConvertToYsonString(unrecognized, EYsonFormat::Text).GetData());
                }
            }

            DynamicConfigVersion_ = updateInfo.dynamic_config_version();
            {
                TGuard<TSpinLock> guard(DynamicOptionsLock_);
                DynamicOptions_ = std::move(dynamicOptions);
            }

            YT_LOG_DEBUG("Updated dynamic config (DynamicConfigVersion: %v)",
                DynamicConfigVersion_);

        } catch (const std::exception& ex) {
            // TODO(savrus): Write this to tablet cell errors once we have them.
            YT_LOG_ERROR(ex, "Error while updating dynamic config");
        }
    }

    void Configure(const TConfigureTabletSlotInfo& configureInfo)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(CanConfigure());

        auto client = Bootstrap_->GetMasterClient();

        CellDescriptor_ = FromProto<TCellDescriptor>(configureInfo.cell_descriptor());

        if (configureInfo.has_peer_id()) {
            TPeerId peerId = configureInfo.peer_id();
            if (PeerId_ != peerId) {
                YT_LOG_DEBUG("Peer id updated (PeerId: %v -> %v)",
                    PeerId_,
                    peerId);

                PeerId_ = peerId;

                // Logger has peer_id tag so should be updated.
                Logger = GetLogger();
            }
        }

        TDistributedHydraManagerDynamicOptions hydraManagerDynamicOptions;
        hydraManagerDynamicOptions.AbandonLeaderLeaseDuringRecovery = configureInfo.abandon_leader_lease_during_recovery();

        auto newPrerequisiteTransactionId = FromProto<TTransactionId>(configureInfo.prerequisite_transaction_id());
        if (newPrerequisiteTransactionId != PrerequisiteTransactionId_) {
            YT_LOG_INFO("Prerequisite transaction updated (TransactionId: %v -> %v)",
                PrerequisiteTransactionId_,
                newPrerequisiteTransactionId);
            PrerequisiteTransactionId_ = newPrerequisiteTransactionId;
            if (ElectionManager_) {
                ElectionManager_->Abandon(TError("Tablet slot reconfigured"));
            }
        }

        PrerequisiteTransaction_.Reset();
        // NB: Prerequisite transaction is only attached by leaders.
        if (PrerequisiteTransactionId_ && CellDescriptor_.Peers[PeerId_].GetVoting()) {
            TTransactionAttachOptions attachOptions;
            attachOptions.Ping = false;
            PrerequisiteTransaction_ = client->AttachTransaction(PrerequisiteTransactionId_, attachOptions);
            YT_LOG_INFO("Prerequisite transaction attached (TransactionId: %v)",
                PrerequisiteTransactionId_);
        }

        auto connection = Bootstrap_->GetMasterClient()->GetNativeConnection();
        auto snapshotClient = connection->CreateNativeClient(TClientOptions(NSecurityClient::TabletCellSnapshotterUserName));
        auto changelogClient = connection->CreateNativeClient(TClientOptions(NSecurityClient::TabletCellChangeloggerUserName));

        auto snapshotStore = CreateRemoteSnapshotStore(
            Config_->Snapshots,
            Options_,
            Format("//sys/tablet_cells/%v/snapshots", GetCellId()),
            snapshotClient,
            PrerequisiteTransaction_ ? PrerequisiteTransaction_->GetId() : NullTransactionId);
        SnapshotStoreThunk_->SetUnderlying(snapshotStore);

        auto changelogStoreFactory = CreateRemoteChangelogStoreFactory(
            Config_->Changelogs,
            Options_,
            Format("//sys/tablet_cells/%v/changelogs", GetCellId()),
            changelogClient,
            Bootstrap_->GetSecurityManager(),
            PrerequisiteTransaction_ ? PrerequisiteTransaction_->GetId() : NullTransactionId,
            ProfilingTagIds_);
        ChangelogStoreFactoryThunk_->SetUnderlying(changelogStoreFactory);

        auto cellConfig = CellDescriptor_.ToConfig(Bootstrap_->GetLocalNetworks());

        auto channelFactory = Bootstrap_
            ->GetMasterClient()
            ->GetNativeConnection()
            ->GetChannelFactory();

        CellManager_ = New<TCellManager>(
            cellConfig,
            channelFactory,
            PeerId_);

        if (auto slotHydraManager = GetHydraManager()) {
            slotHydraManager->SetDynamicOptions(hydraManagerDynamicOptions);
            ElectionManager_->ReconfigureCell(CellManager_);

            YT_LOG_INFO("Slot reconfigured (ConfigVersion: %v)",
                CellDescriptor_.ConfigVersion);
        } else {
            Automaton_ = New<TTabletAutomaton>(
                Owner_,
                SnapshotQueue_->GetInvoker());

            auto rpcServer = Bootstrap_->GetRpcServer();

            ResponseKeeper_ = New<TResponseKeeper>(
                Config_->HydraManager->ResponseKeeper,
                GetAutomatonInvoker(),
                Logger,
                TabletNodeProfiler.AppendPath("/response_keeper"));

            TDistributedHydraManagerOptions hydraManagerOptions;
            hydraManagerOptions.ResponseKeeper = ResponseKeeper_;
            hydraManagerOptions.UseFork = false;
            hydraManagerOptions.WriteChangelogsAtFollowers = false;
            hydraManagerOptions.WriteSnapshotsAtFollowers = false;
            hydraManagerOptions.ProfilingTagIds = ProfilingTagIds_;

            auto hydraManager = CreateDistributedHydraManager(
                Config_->HydraManager,
                Bootstrap_->GetControlInvoker(),
                GetAutomatonInvoker(EAutomatonThreadQueue::Mutation),
                Automaton_,
                rpcServer,
                ElectionManagerThunk_,
                GetCellId(),
                ChangelogStoreFactoryThunk_,
                SnapshotStoreThunk_,
                hydraManagerOptions,
                hydraManagerDynamicOptions);
            SetHydraManager(hydraManager);

            hydraManager->SubscribeStartLeading(BIND(&TImpl::OnStartEpoch, MakeWeak(this)));
            hydraManager->SubscribeStartFollowing(BIND(&TImpl::OnStartEpoch, MakeWeak(this)));

            hydraManager->SubscribeStopLeading(BIND(&TImpl::OnStopEpoch, MakeWeak(this)));
            hydraManager->SubscribeStopFollowing(BIND(&TImpl::OnStopEpoch, MakeWeak(this)));

            hydraManager->SubscribeLeaderRecoveryComplete(BIND(&TImpl::OnRecoveryComplete, MakeWeak(this)));
            hydraManager->SubscribeFollowerRecoveryComplete(BIND(&TImpl::OnRecoveryComplete, MakeWeak(this)));

            hydraManager->SubscribeLeaderLeaseCheck(
                BIND(&TImpl::OnLeaderLeaseCheckThunk, MakeWeak(this))
                    .AsyncVia(Bootstrap_->GetControlInvoker()));

            {
                TGuard<TSpinLock> guard(InvokersLock_);
                for (auto queue : TEnumTraits<EAutomatonThreadQueue>::GetDomainValues()) {
                    auto unguardedInvoker = GetAutomatonInvoker(queue);
                    GuardedAutomatonInvokers_[queue] = hydraManager->CreateGuardedAutomatonInvoker(unguardedInvoker);
                }
            }

            ElectionManager_ = CreateDistributedElectionManager(
                Config_->ElectionManager,
                CellManager_,
                Bootstrap_->GetControlInvoker(),
                hydraManager->GetElectionCallbacks(),
                rpcServer);
            ElectionManager_->Initialize();

            ElectionManagerThunk_->SetUnderlying(ElectionManager_);

            auto masterConnection = Bootstrap_->GetMasterClient()->GetNativeConnection();
            HiveManager_ = New<THiveManager>(
                Config_->HiveManager,
                masterConnection->GetCellDirectory(),
                GetCellId(),
                GetAutomatonInvoker(),
                hydraManager,
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
            // NB: Should not start synchronizer while validating snapshot.
            if (GetCellId()) {
                connection->GetClusterDirectorySynchronizer()->Start();
            }
            TransactionSupervisor_ = New<TTransactionSupervisor>(
                Config_->TransactionSupervisor,
                GetAutomatonInvoker(),
                Bootstrap_->GetTransactionTrackerInvoker(),
                hydraManager,
                Automaton_,
                GetResponseKeeper(),
                TransactionManager_,
                Bootstrap_->GetSecurityManager(),
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

            hydraManager->Initialize();

            for (const auto& service : TransactionSupervisor_->GetRpcServices()) {
                rpcServer->RegisterService(service);
            }
            rpcServer->RegisterService(HiveManager_->GetRpcService());
            rpcServer->RegisterService(TabletService_);

            OrchidService_ = CreateOrchidService();

            YT_LOG_INFO("Slot configured (ConfigVersion: %v)",
                CellDescriptor_.ConfigVersion);
        }
    }

    TFuture<void> Finalize()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (Finalizing_) {
            return FinalizeResult_;
        }

        YT_LOG_INFO("Finalizing slot");

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
        return GetDynamicOptions()->CpuPerTabletSlot.value_or(cpuPerTabletSlot);
    }

    const TDynamicTabletCellOptionsPtr GetDynamicOptions() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(DynamicOptionsLock_);
        auto options = DynamicOptions_;
        guard.Release();
        return options;
    }

    const TTabletCellOptionsPtr& GetOptions() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Options_;
    }

private:
    TTabletSlot* const Owner_;
    const int SlotIndex_;
    const TTabletNodeConfigPtr Config_;
    NClusterNode::TBootstrap* const Bootstrap_;

    const TFairShareActionQueuePtr AutomatonQueue_;
    const TActionQueuePtr SnapshotQueue_;

    const TElectionManagerThunkPtr ElectionManagerThunk_ = New<TElectionManagerThunk>();
    const TSnapshotStoreThunkPtr SnapshotStoreThunk_ = New<TSnapshotStoreThunk>();
    const TChangelogStoreFactoryThunkPtr ChangelogStoreFactoryThunk_ = New<TChangelogStoreFactoryThunk>();

    TPeerId PeerId_;
    TCellDescriptor CellDescriptor_;

    const TString TabletCellBundle_;

    const NProfiling::TTagIdList ProfilingTagIds_;

    const TTabletCellOptionsPtr Options_;

    TSpinLock DynamicOptionsLock_;
    TDynamicTabletCellOptionsPtr DynamicOptions_ = New<TDynamicTabletCellOptions>();

    int DynamicConfigVersion_ = -1;

    TTransactionId PrerequisiteTransactionId_;
    ITransactionPtr PrerequisiteTransaction_;  // only created for leaders

    TCellManagerPtr CellManager_;

    IElectionManagerPtr ElectionManager_;

    TSpinLock HydraManagerLock_;
    IDistributedHydraManagerPtr HydraManager_;

    TResponseKeeperPtr ResponseKeeper_;

    THiveManagerPtr HiveManager_;

    TTabletManagerPtr TabletManager_;

    TTransactionManagerPtr TransactionManager_;
    TTransactionSupervisorPtr TransactionSupervisor_;

    NRpc::IServicePtr TabletService_;

    TTabletAutomatonPtr Automaton_;

    TSpinLock InvokersLock_;
    TEnumIndexedVector<EAutomatonThreadQueue, IInvokerPtr> EpochAutomatonInvokers_;
    TEnumIndexedVector<EAutomatonThreadQueue, IInvokerPtr> GuardedAutomatonInvokers_;

    const TRuntimeTabletCellDataPtr RuntimeData_ = New<TRuntimeTabletCellData>();

    bool Initialized_ = false;
    bool Finalizing_ = false;
    TFuture<void> FinalizeResult_;

    IYPathServicePtr OrchidService_;

    NLogging::TLogger Logger;


    void SetHydraManager(IDistributedHydraManagerPtr hydraManager)
    {
        auto guard = Guard(HydraManagerLock_);
        std::swap(HydraManager_, hydraManager);
    }

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
            ->AddChild("config_version", IYPathService::FromMethod(
                &TImpl::GetConfigVersion,
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
            ->AddChild("life_stage", IYPathService::FromMethod(
                &TTabletManager::GetTabletCellLifeStage,
                MakeWeak(TabletManager_))
                ->Via(GetAutomatonInvoker()))
            ->AddChild("transactions", TransactionManager_->GetOrchidService())
            ->AddChild("tablets", TabletManager_->GetOrchidService())
            ->AddChild("hive", HiveManager_->GetOrchidService())
            ->Via(Bootstrap_->GetControlInvoker());
    }

    TTransactionId GetPrerequisiteTransactionId() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return PrerequisiteTransactionId_;
    }

    NLogging::TLogger GetLogger() const
    {
        return NLogging::TLogger(TabletNodeLogger)
            .AddTag("CellId: %v, PeerId: %v",
                CellDescriptor_.CellId,
                PeerId_);
    }

    void ResetEpochInvokers()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        {
            TGuard<TSpinLock> guard(InvokersLock_);
            std::fill(EpochAutomatonInvokers_.begin(), EpochAutomatonInvokers_.end(), GetNullInvoker());
        }
    }

    void ResetGuardedInvokers()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        {
            TGuard<TSpinLock> guard(InvokersLock_);
            std::fill(GuardedAutomatonInvokers_.begin(), GuardedAutomatonInvokers_.end(), GetNullInvoker());
        }
    }


    void OnStartEpoch()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto hydraManager = GetHydraManager();
        if (!hydraManager) {
            return;
        }

        {
            TGuard<TSpinLock> guard(InvokersLock_);
            for (auto queue : TEnumTraits<EAutomatonThreadQueue>::GetDomainValues()) {
                EpochAutomatonInvokers_[queue] = hydraManager
                    ->GetAutomatonCancelableContext()
                    ->CreateInvoker(GetAutomatonInvoker(queue));
            }
        }
    }

    void OnStopEpoch()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        ResetEpochInvokers();
    }

    void OnRecoveryComplete()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        // Notify master about recovery completion as soon as possible via out-of-order heartbeat.
        auto primaryCellTag = CellTagFromId(Bootstrap_->GetCellId());
        Bootstrap_->GetMasterConnector()->ScheduleNodeHeartbeat(primaryCellTag, true);
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
            YT_LOG_DEBUG("Checking prerequisite transaction (PrerequisiteTransactionId: %v)",
                PrerequisiteTransaction_->GetId());
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
        if (auto hydraManager = GetHydraManager()) {
            WaitFor(hydraManager->Finalize())
                .ThrowOnError();
        }
        SetHydraManager(nullptr);

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
    NClusterNode::TBootstrap* bootstrap)
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

TCellId TTabletSlot::GetCellId() const
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

const IDistributedHydraManagerPtr& TTabletSlot::GetHydraManager() const
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode::NYT
