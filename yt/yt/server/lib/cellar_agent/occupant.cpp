#include "occupant.h"

#include "bootstrap_proxy.h"
#include "occupier.h"
#include "private.h"

#include <yt/yt/server/lib/election/election_manager.h>
#include <yt/yt/server/lib/election/distributed_election_manager.h>

#include <yt/yt/server/lib/hive/hive_manager.h>
#include <yt/yt/server/lib/hive/mailbox.h>
#include <yt/yt/server/lib/hive/transaction_supervisor.h>

#include <yt/yt/server/lib/hydra/changelog.h>
#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>
#include <yt/yt/server/lib/hydra/hydra_manager.h>
#include <yt/yt/server/lib/hydra/remote_changelog_store.h>
#include <yt/yt/server/lib/hydra/remote_snapshot_store.h>
#include <yt/yt/server/lib/hydra/snapshot.h>

#include <yt/yt/server/lib/election/election_manager.h>
#include <yt/yt/server/lib/election/election_manager_thunk.h>
#include <yt/yt/server/lib/election/alien_cell_peer_channel_factory.h>

#include <yt/yt/server/lib/hydra/changelog.h>
#include <yt/yt/server/lib/hydra/remote_changelog_store.h>
#include <yt/yt/server/lib/hydra/snapshot.h>
#include <yt/yt/server/lib/hydra/remote_snapshot_store.h>
#include <yt/yt/server/lib/hydra/hydra_manager.h>
#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>
#include <yt/yt/server/lib/hydra/changelog_store_factory_thunk.h>
#include <yt/yt/server/lib/hydra/snapshot_store_thunk.h>

#include <yt/yt/server/lib/hive/hive_manager.h>
#include <yt/yt/server/lib/hive/mailbox.h>
#include <yt/yt/server/lib/hive/transaction_supervisor.h>
#include <yt/yt/server/lib/hive/transaction_participant_provider.h>

#include <yt/yt/server/lib/misc/profiling_helpers.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/client/api/connection.h>
#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/ytlib/election/cell_manager.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/core/concurrency/fair_share_action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/profiling/profile_manager.h>

#include <yt/yt/core/rpc/response_keeper.h>
#include <yt/yt/core/rpc/server.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/virtual.h>
#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NCellarAgent {

using namespace NConcurrency;
using namespace NRpc;
using namespace NYTree;
using namespace NYson;
using namespace NElection;
using namespace NHydra;
using namespace NHiveClient;
using namespace NHiveServer;
using namespace NObjectClient;
using namespace NApi;
using namespace NTabletClient;
using namespace NCellarNodeTrackerClient::NProto;

using NHydra::EPeerState;

////////////////////////////////////////////////////////////////////////////////

const auto& Profiler = CellarAgentProfiler;

////////////////////////////////////////////////////////////////////////////////

class TCellarOccupant
    : public ICellarOccupant
{
public:
    TCellarOccupant(
        TCellarOccupantConfigPtr config,
        ICellarBootstrapProxyPtr bootstrap,
        int index,
        const TCreateCellSlotInfo& createInfo,
        ICellarOccupierPtr occupier)
        : Config_(config)
        , Bootstrap_(bootstrap)
        , Occupier_(std::move(occupier))
        , Index_(index)
        , PeerId_(createInfo.peer_id())
        , CellDescriptor_(FromProto<TCellId>(createInfo.cell_id()))
        , CellBundleName_(createInfo.cell_bundle())
        , Options_(ConvertTo<TTabletCellOptionsPtr>(TYsonString(createInfo.options())))
        , Logger(GetLogger())
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Occupier_->GetOccupierAutomatonInvoker(), AutomatonThread);
    }

    virtual const ICellarOccupierPtr& GetOccupier() const override
    {
        return Occupier_;
    }

    virtual int GetIndex() const override
    {
        return Index_;
    }

    virtual TCellId GetCellId() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return CellDescriptor_.CellId;
    }

    virtual EPeerState GetControlState() const override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (Finalizing_) {
            YT_LOG_DEBUG("Peer is finalized (CellId: %v, State: %v)",
                GetCellId(),
                EPeerState::Stopped);
            return EPeerState::Stopped;
        }

        if (auto hydraManager = GetHydraManager()) {
            return hydraManager->GetControlState();
        }

        if (Initialized_) {
            YT_LOG_DEBUG("Peer is not initialized yet (CellId: %v, State: %v)",
                GetCellId(),
                EPeerState::Stopped);
            return EPeerState::Stopped;
        }

        return EPeerState::None;
    }

    virtual EPeerState GetAutomatonState() const override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto hydraManager = GetHydraManager();
        return hydraManager ? hydraManager->GetAutomatonState() : EPeerState::None;
    }

    virtual TPeerId GetPeerId() const override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return PeerId_;
    }

    virtual const TCellDescriptor& GetCellDescriptor() const override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return CellDescriptor_;
    }

    virtual const IDistributedHydraManagerPtr GetHydraManager() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return HydraManager_.Load();
    }

    virtual const TResponseKeeperPtr& GetResponseKeeper() const override
    {
        return ResponseKeeper_;
    }

    virtual const TCompositeAutomatonPtr& GetAutomaton() const override
    {
        return Automaton_;
    }

    virtual const THiveManagerPtr& GetHiveManager() const override
    {
        return HiveManager_;
    }

    virtual const ITransactionSupervisorPtr& GetTransactionSupervisor() const override
    {
        return TransactionSupervisor_;
    }

    virtual TMailbox* GetMasterMailbox() const override
    {
        // Create master mailbox lazily.
        auto masterCellId = Bootstrap_->GetCellId();
        return HiveManager_->GetOrCreateMailbox(masterCellId);
    }

    virtual TObjectId GenerateId(EObjectType type) const override
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

    virtual void Initialize() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(!Initialized_);

        Initialized_ = true;

        YT_LOG_INFO("Cellar occupant initialized");
    }


    virtual bool CanConfigure() const override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return Initialized_ && !Finalizing_;
    }

    virtual void Configure(const TConfigureCellSlotInfo& configureInfo) override
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

        if (configureInfo.has_options()) {
            YT_LOG_DEBUG("Dynamic cell options updated to: %v",
                ConvertToYsonString(TYsonString(configureInfo.options()), EYsonFormat::Text).AsStringBuf());
            Options_ = ConvertTo<TTabletCellOptionsPtr>(TYsonString(configureInfo.options()));
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
                ElectionManager_->Abandon(TError("Cell slot reconfigured"));
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

        // COMPAT(akozhikhov)
        auto connection = Bootstrap_->GetMasterClient()->GetNativeConnection();
        auto snapshotClient = connection->CreateNativeClient(TClientOptions::FromUser(NSecurityClient::TabletCellSnapshotterUserName));
        auto changelogClient = connection->CreateNativeClient(TClientOptions::FromUser(NSecurityClient::TabletCellChangeloggerUserName));

        bool independent = Options_->IndependentPeers;
        TStringBuilder builder;
        builder.AppendFormat("//sys/tablet_cells/%v", GetCellId());
        if (independent) {
            builder.AppendFormat("/%v", PeerId_);
        }
        auto path = builder.Flush();

        auto snapshotStore = CreateRemoteSnapshotStore(
            Config_->Snapshots,
            Options_,
            path + "/snapshots",
            snapshotClient,
            PrerequisiteTransaction_ ? PrerequisiteTransaction_->GetId() : NullTransactionId);
        SnapshotStoreThunk_->SetUnderlying(snapshotStore);

        auto addTags = [this] (auto profiler) {
            return profiler
                .WithRequiredTag("tablet_cell_bundle", CellBundleName_ ? CellBundleName_ : UnknownProfilingTag)
                .WithTag("cell_id", ToString(CellDescriptor_.CellId), -1);
        };

        auto changelogProfiler = addTags(Occupier_->GetProfiler().WithPrefix("/remote_changelog"));
        auto changelogStoreFactory = CreateRemoteChangelogStoreFactory(
            Config_->Changelogs,
            Options_,
            path + "/changelogs",
            changelogClient,
            Bootstrap_->GetResourceLimitsManager(),
            PrerequisiteTransaction_ ? PrerequisiteTransaction_->GetId() : NullTransactionId,
            TJournalWriterPerformanceCounters{changelogProfiler});
        ChangelogStoreFactoryThunk_->SetUnderlying(changelogStoreFactory);

        auto cellConfig = CellDescriptor_.ToConfig(Bootstrap_->GetLocalNetworks());

        auto channelFactory = Bootstrap_
            ->GetMasterClient()
            ->GetNativeConnection()
            ->GetChannelFactory();
        auto foreignChannelFactory = CreateAlienCellPeerChannelFactory(
            connection->GetClusterDirectory(),
            connection->GetClusterDirectorySynchronizer());

        CellManager_ = New<TCellManager>(
            cellConfig,
            channelFactory,
            foreignChannelFactory,
            PeerId_);

        if (auto slotHydraManager = GetHydraManager()) {
            slotHydraManager->SetDynamicOptions(hydraManagerDynamicOptions);
            ElectionManager_->ReconfigureCell(CellManager_);

            YT_LOG_INFO("Cellar occupant reconfigured (ConfigVersion: %v)",
                CellDescriptor_.ConfigVersion);
        } else {
            Automaton_ = Occupier_->CreateAutomaton();

            ResponseKeeper_ = New<TResponseKeeper>(
                Config_->ResponseKeeper,
                Occupier_->GetOccupierAutomatonInvoker(),
                Logger,
                Profiler);

            auto rpcServer = Bootstrap_->GetRpcServer();

            TDistributedHydraManagerOptions hydraManagerOptions{
                .UseFork = false,
                .WriteChangelogsAtFollowers = independent,
                .WriteSnapshotsAtFollowers = independent,
                .ResponseKeeper = ResponseKeeper_
            };

            auto hydraManager = CreateDistributedHydraManager(
                Config_->HydraManager,
                Bootstrap_->GetControlInvoker(),
                Occupier_->GetMutationAutomatonInvoker(),
                Automaton_,
                rpcServer,
                ElectionManagerThunk_,
                GetCellId(),
                ChangelogStoreFactoryThunk_,
                SnapshotStoreThunk_,
                hydraManagerOptions,
                hydraManagerDynamicOptions);
            HydraManager_.Store(hydraManager);

            if (!independent) {
                hydraManager->SubscribeLeaderLeaseCheck(
                    BIND(&TCellarOccupant::OnLeaderLeaseCheckThunk, MakeWeak(this))
                        .AsyncVia(Bootstrap_->GetControlInvoker()));
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
                Occupier_->GetOccupierAutomatonInvoker(),
                hydraManager,
                Automaton_);

            Occupier_->Configure(hydraManager);

            auto connection = Bootstrap_->GetMasterClient()->GetNativeConnection();
            std::vector<ITransactionParticipantProviderPtr> providers;

            // NB: Should not start synchronizer while validating snapshot.
            if (GetCellId()) {
                connection->GetClusterDirectorySynchronizer()->Start();
                providers = {
                    CreateTransactionParticipantProvider(connection),
                    CreateTransactionParticipantProvider(connection->GetClusterDirectory())};
            }
            TransactionSupervisor_ = CreateTransactionSupervisor(
                Config_->TransactionSupervisor,
                Occupier_->GetOccupierAutomatonInvoker(),
                Bootstrap_->GetTransactionTrackerInvoker(),
                hydraManager,
                Automaton_,
                ResponseKeeper_,
                Occupier_->GetOccupierTransactionManager(),
                GetCellId(),
                connection->GetTimestampProvider(),
                std::move(providers));

            Occupier_->Initialize();

            hydraManager->Initialize();

            for (const auto& service : TransactionSupervisor_->GetRpcServices()) {
                rpcServer->RegisterService(service);
            }
            rpcServer->RegisterService(HiveManager_->GetRpcService());

            Occupier_->RegisterRpcServices();

            OrchidService_ = Occupier_->PopulateOrchidService(CreateOrchidService())
                ->Via(Bootstrap_->GetControlInvoker());

            YT_LOG_INFO("Cellar occupant configured (ConfigVersion: %v)",
                CellDescriptor_.ConfigVersion);
        }
    }

    virtual TDynamicTabletCellOptionsPtr GetDynamicOptions() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return DynamicOptions_.Load();
    }

    virtual int GetDynamicConfigVersion() const override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return DynamicConfigVersion_;
    }

    virtual void UpdateDynamicConfig(const TUpdateCellSlotInfo& updateInfo) override
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
            // TODO(savrus): Write this to cell errors once we have them.
            YT_LOG_ERROR(ex, "Error while updating dynamic config");
        }
    }

    virtual TFuture<void> Finalize() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (Finalizing_) {
            return FinalizeResult_;
        }

        YT_LOG_INFO("Finalizing cellar occupant");

        Finalizing_ = true;

        Occupier_->Stop();

        FinalizeResult_ = BIND(&TCellarOccupant::DoFinalize, MakeStrong(this))
            .AsyncVia(Bootstrap_->GetControlInvoker())
            .Run();

        return FinalizeResult_;
    }

    virtual const IYPathServicePtr& GetOrchidService() const override
    {
        return OrchidService_;
    }

    virtual const TString& GetCellBundleName() const override
    {
        return CellBundleName_;
    }

    virtual const TTabletCellOptionsPtr& GetOptions() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Options_;
    }

private:
    const TCellarOccupantConfigPtr Config_;
    const ICellarBootstrapProxyPtr Bootstrap_;
    ICellarOccupierPtr Occupier_;

    const TElectionManagerThunkPtr ElectionManagerThunk_ = New<TElectionManagerThunk>();
    const TSnapshotStoreThunkPtr SnapshotStoreThunk_ = New<TSnapshotStoreThunk>();
    const TChangelogStoreFactoryThunkPtr ChangelogStoreFactoryThunk_ = New<TChangelogStoreFactoryThunk>();

    int Index_;

    TPeerId PeerId_;
    TCellDescriptor CellDescriptor_;

    const TString CellBundleName_;

    TAtomicObject<TDynamicTabletCellOptionsPtr> DynamicOptions_ = New<TDynamicTabletCellOptions>();
    int DynamicConfigVersion_ = -1;

    TTabletCellOptionsPtr Options_;

    TTransactionId PrerequisiteTransactionId_;
    ITransactionPtr PrerequisiteTransaction_;  // only created for leaders

    TCellManagerPtr CellManager_;

    IElectionManagerPtr ElectionManager_;

    TAtomicObject<IDistributedHydraManagerPtr> HydraManager_;

    TResponseKeeperPtr ResponseKeeper_;

    THiveManagerPtr HiveManager_;

    ITransactionSupervisorPtr TransactionSupervisor_;

    TCompositeAutomatonPtr Automaton_;

    bool Initialized_ = false;
    bool Finalizing_ = false;
    TFuture<void> FinalizeResult_;

    IYPathServicePtr OrchidService_;

    NLogging::TLogger Logger;


    TCompositeMapServicePtr CreateOrchidService()
    {
        return New<TCompositeMapService>()
            ->AddAttribute(EInternedAttributeKey::Opaque, BIND([] (IYsonConsumer* consumer) {
                    BuildYsonFluently(consumer)
                        .Value(true);
                }))
            ->AddChild("state", IYPathService::FromMethod(
                &TCellarOccupant::GetControlState,
                MakeWeak(this)))
            ->AddChild("hydra", IYPathService::FromMethod(
                &TCellarOccupant::GetHydraMonitoring,
                MakeWeak(this)))
            ->AddChild("config_version", IYPathService::FromMethod(
                &TCellarOccupant::GetConfigVersion,
                MakeWeak(this)))
            ->AddChild("dynamic_options", IYPathService::FromMethod(
                &TCellarOccupant::GetDynamicOptions,
                MakeWeak(this)))
            ->AddChild("dynamic_config_version", IYPathService::FromMethod(
                &TCellarOccupant::GetDynamicConfigVersion,
                MakeWeak(this)))
            ->AddChild("prerequisite_transaction_id", IYPathService::FromMethod(
                &TCellarOccupant::GetPrerequisiteTransactionId,
                MakeWeak(this)))
            ->AddChild("options", IYPathService::FromMethod(
                &TCellarOccupant::GetOptions,
                MakeWeak(this)))
            ->AddChild("hive", HiveManager_->GetOrchidService());
    }

    void GetHydraMonitoring(IYsonConsumer* consumer) const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (auto hydraManager = GetHydraManager()) {
            hydraManager->GetMonitoringProducer().Run(consumer);
        } else {
            BuildYsonFluently(consumer)
                .Entity();
        }
    }

    int GetConfigVersion() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return CellDescriptor_.ConfigVersion;
    }

    TTransactionId GetPrerequisiteTransactionId() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return PrerequisiteTransactionId_;
    }


    static TFuture<void> OnLeaderLeaseCheckThunk(TWeakPtr<TCellarOccupant> weakThis)
    {
        auto this_ = weakThis.Lock();
        return this_ ? this_->OnLeaderLeaseCheck() : VoidFuture;
    }

    TFuture<void> OnLeaderLeaseCheck()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (PrerequisiteTransaction_) {
            YT_LOG_DEBUG("Checking prerequisite transaction");
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
        HydraManager_.Store(nullptr);

        if (ElectionManager_) {
            ElectionManager_->Finalize();
        }
        ElectionManager_.Reset();

        Automaton_.Reset();

        ResponseKeeper_.Reset();

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

        Occupier_->Finalize();

        Occupier_.Reset();
    }

    NLogging::TLogger GetLogger() const
    {
        return CellarAgentLogger.WithTag("CellId: %v, PeerId: %v",
            CellDescriptor_.CellId,
            PeerId_);
    }


    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);
};

////////////////////////////////////////////////////////////////////////////////

ICellarOccupantPtr CreateCellarOccupant(
    int index,
    TCellarOccupantConfigPtr config,
    ICellarBootstrapProxyPtr bootstrap,
    const TCreateCellSlotInfo& createInfo,
    ICellarOccupierPtr occupier)
{
    return New<TCellarOccupant>(
        std::move(config),
        std::move(bootstrap),
        index,
        createInfo,
        std::move(occupier));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent
