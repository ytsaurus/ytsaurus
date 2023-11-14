#include "chaos_slot.h"

#include "automaton.h"
#include "bootstrap.h"
#include "private.h"
#include "serialize.h"
#include "slot_manager.h"
#include "chaos_manager.h"
#include "chaos_node_service.h"
#include "coordinator_manager.h"
#include "coordinator_service.h"
#include "transaction_manager.h"
#include "replicated_table_tracker.h"

#include <yt/yt/server/lib/cellar_agent/automaton_invoker_hood.h>
#include <yt/yt/server/lib/cellar_agent/occupant.h>

#include <yt/yt/server/lib/hive/public.h>

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>

#include <yt/yt/server/lib/tablet_server/config.h>
#include <yt/yt/server/lib/tablet_server/replicated_table_tracker.h>

#include <yt/yt/server/lib/chaos_node/config.h>

#include <yt/yt/ytlib/api/public.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/core/concurrency/fair_share_action_queue.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/ytree/virtual.h>
#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NChaosNode {

using namespace NCellarAgent;
using namespace NCellarClient;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NHiveServer;
using namespace NHydra;
using namespace NObjectClient;
using namespace NTabletServer;
using namespace NTransactionClient;
using namespace NTransactionSupervisor;
using namespace NYTree;
using namespace NYson;

using NHydra::EPeerState;

////////////////////////////////////////////////////////////////////////////////

class TChaosSlot
    : public IChaosSlot
    , public TAutomatonInvokerHood<EAutomatonThreadQueue>
{
    using THood = TAutomatonInvokerHood<EAutomatonThreadQueue>;

public:
    TChaosSlot(
        int slotIndex,
        TChaosNodeConfigPtr config,
        IBootstrap* bootstrap)
        : THood(Format("ChaosSlot:%v", slotIndex))
        , Config_(config)
        , Bootstrap_(bootstrap)
        , SnapshotQueue_(New<TActionQueue>(
            Format("ChaosSnap:%v", slotIndex)))
        , Logger(ChaosNodeLogger)
    {
        VERIFY_INVOKER_THREAD_AFFINITY(GetAutomatonInvoker(), AutomatonThread);

        ResetEpochInvokers();
        ResetGuardedInvokers();
    }

    void SetOccupant(ICellarOccupantPtr occupant) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(!Occupant_);

        Occupant_ = std::move(occupant);
        Logger.AddTag("CellId: %v, PeerId: %v",
            Occupant_->GetCellId(),
            Occupant_->GetPeerId());
    }

    TCellId GetCellId() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Occupant_->GetCellId();
    }

    const TString& GetCellBundleName() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Occupant_->GetCellBundleName();
    }

    EPeerState GetAutomatonState() const override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto hydraManager = GetHydraManager();
        return hydraManager ? hydraManager->GetAutomatonState() : EPeerState::None;
    }

    IDistributedHydraManagerPtr GetHydraManager() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Occupant_->GetHydraManager();
    }

    const TCompositeAutomatonPtr& GetAutomaton() const override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return Occupant_->GetAutomaton();
    }

    const IHiveManagerPtr& GetHiveManager() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Occupant_->GetHiveManager();
    }

    TMailbox* GetMasterMailbox() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return Occupant_->GetMasterMailbox();
    }

    ITransactionManagerPtr GetTransactionManager() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return TransactionManager_;
    }

    NTransactionSupervisor::ITransactionManagerPtr GetOccupierTransactionManager() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return TransactionManager_;
    }

    const ITimestampProviderPtr& GetTimestampProvider() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Occupant_->GetTimestampProvider();
    }

    const ITransactionSupervisorPtr& GetTransactionSupervisor() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Occupant_->GetTransactionSupervisor();
    }

    const IChaosManagerPtr& GetChaosManager() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return ChaosManager_;
    }

    const ICoordinatorManagerPtr& GetCoordinatorManager() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return CoordinatorManager_;
    }

    const IShortcutSnapshotStorePtr& GetShortcutSnapshotStore() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetShortcutSnapshotStore();
    }

    const IInvokerPtr& GetSnapshotStoreReadPoolInvoker() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetSnapshotStoreReadPoolInvoker();
    }

    TObjectId GenerateId(EObjectType type) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return Occupant_->GenerateId(type);
    }

    TCompositeAutomatonPtr CreateAutomaton() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return New<TChaosAutomaton>(
            this,
            SnapshotQueue_->GetInvoker());
    }

    void Configure(IDistributedHydraManagerPtr hydraManager) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        hydraManager->SubscribeStartLeading(BIND(&TChaosSlot::OnStartEpoch, MakeWeak(this)));
        hydraManager->SubscribeStartFollowing(BIND(&TChaosSlot::OnStartEpoch, MakeWeak(this)));

        hydraManager->SubscribeStopLeading(BIND(&TChaosSlot::OnStopEpoch, MakeWeak(this)));
        hydraManager->SubscribeStopFollowing(BIND(&TChaosSlot::OnStopEpoch, MakeWeak(this)));

        InitGuardedInvokers(hydraManager);

        ChaosManager_ = CreateChaosManager(
            Config_->ChaosManager,
            this,
            Bootstrap_);

        CoordinatorManager_ = CreateCoordinatorManager(
            Config_->CoordinatorManager,
            this,
            Bootstrap_);

        auto clockClusterTag = Occupant_->GetOptions()->ClockClusterTag != InvalidCellTag
            ? Occupant_->GetOptions()->ClockClusterTag
            : Bootstrap_->GetConnection()->GetClusterTag();

        TransactionManager_ = CreateTransactionManager(
            Config_->TransactionManager,
            this,
            clockClusterTag,
            Bootstrap_);

        ReplicatedTableTracker_ = CreateReplicatedTableTracker(
            CreateReplicatedTableTrackerHost(this),
            Bootstrap_->GetReplicatedTableTrackerConfig(),
            /*profiler*/ {});
    }

    void Initialize() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        ChaosNodeService_ = CreateChaosNodeService(this, Bootstrap_->GetNativeAuthenticator());
        CoordinatorService_ = CreateCoordinatorService(this, Bootstrap_->GetNativeAuthenticator());

        ChaosManager_->Initialize();
        CoordinatorManager_->Initialize();
    }

    void RegisterRpcServices() override
    {
        const auto& rpcServer = Bootstrap_->GetRpcServer();
        rpcServer->RegisterService(ChaosNodeService_);
        rpcServer->RegisterService(CoordinatorService_);
    }

    void Stop() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        ResetEpochInvokers();
        ResetGuardedInvokers();
    }

    void Finalize() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        ReplicatedTableTracker_.Reset();
        ChaosManager_.Reset();
        TransactionManager_.Reset();

        if (ChaosNodeService_) {
            const auto& rpcServer = Bootstrap_->GetRpcServer();
            rpcServer->UnregisterService(ChaosNodeService_);
        }
        ChaosNodeService_.Reset();

        if (CoordinatorService_) {
            const auto& rpcServer = Bootstrap_->GetRpcServer();
            rpcServer->UnregisterService(CoordinatorService_);
        }
        CoordinatorService_.Reset();
    }

    TCompositeMapServicePtr PopulateOrchidService(TCompositeMapServicePtr orchid) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return orchid
            ->AddChild("transactions", TransactionManager_->GetOrchidService())
            ->AddChild("chaos_manager", ChaosManager_->GetOrchidService())
            ->AddChild("coordinator_manager", CoordinatorManager_->GetOrchidService());
    }

    NProfiling::TRegistry GetProfiler() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return ChaosNodeProfiler;
    }

    IInvokerPtr GetAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const override
    {
        return TAutomatonInvokerHood<EAutomatonThreadQueue>::GetAutomatonInvoker(queue);
    }

    IInvokerPtr GetEpochAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const override
    {
        return TAutomatonInvokerHood<EAutomatonThreadQueue>::GetEpochAutomatonInvoker(queue);
    }

    IInvokerPtr GetGuardedAutomatonInvoker(EAutomatonThreadQueue queue = EAutomatonThreadQueue::Default) const override
    {
        return TAutomatonInvokerHood<EAutomatonThreadQueue>::GetGuardedAutomatonInvoker(queue);
    }

    IInvokerPtr GetOccupierAutomatonInvoker() override
    {
        return GetAutomatonInvoker();
    }

    IInvokerPtr GetMutationAutomatonInvoker() override
    {
        return GetAutomatonInvoker(EAutomatonThreadQueue::Mutation);
    }

    ECellarType GetCellarType() override
    {
        return IChaosSlot::CellarType;
    }

    NApi::IClientPtr CreateClusterClient(const TString& clusterName) const override
    {
        const auto& clusterDirectory = Bootstrap_->GetClusterConnection()->GetClusterDirectory();
        auto connection = clusterDirectory->FindConnection(clusterName);
        // TODO(savrus): Consider employing specific user.
        return connection
            ? connection->CreateClient(NApi::TClientOptions::FromUser(NSecurityClient::RootUserName))
            : nullptr;
    }

    const NTabletServer::IReplicatedTableTrackerPtr& GetReplicatedTableTracker() const override
    {
        return ReplicatedTableTracker_;
    }

    void SubscribeReplicatedTableTrackerConfigChanged(TReplicatedTableTrackerConfigUpdateCallback callback) const override
    {
        Bootstrap_->SubscribeReplicatedTableTrackerConfigChanged(std::move(callback));
    }

    TDynamicReplicatedTableTrackerConfigPtr GetReplicatedTableTrackerConfig() const override
    {
        return Bootstrap_->GetReplicatedTableTrackerConfig();
    }

private:
    const TChaosNodeConfigPtr Config_;
    IBootstrap* const Bootstrap_;

    ICellarOccupantPtr Occupant_;

    const TActionQueuePtr SnapshotQueue_;

    TCellDescriptor CellDescriptor_;

    const NProfiling::TTagIdList ProfilingTagIds_;

    IChaosManagerPtr ChaosManager_;
    ICoordinatorManagerPtr CoordinatorManager_;

    ITransactionManagerPtr TransactionManager_;

    IReplicatedTableTrackerPtr ReplicatedTableTracker_;

    NRpc::IServicePtr ChaosNodeService_;
    NRpc::IServicePtr CoordinatorService_;

    IYPathServicePtr OrchidService_;

    NLogging::TLogger Logger;


    void OnStartEpoch()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto hydraManager = GetHydraManager();
        if (!hydraManager) {
            return;
        }

        InitEpochInvokers(hydraManager);
    }

    void OnStopEpoch()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        ResetEpochInvokers();
    }

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);
};

////////////////////////////////////////////////////////////////////////////////

IChaosSlotPtr CreateChaosSlot(
    int slotIndex,
    TChaosNodeConfigPtr config,
    IBootstrap* bootstrap)
{
    return New<TChaosSlot>(
        slotIndex,
        config,
        bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
