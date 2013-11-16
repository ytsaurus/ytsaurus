#include "tablet_slot.h"
#include "config.h"
#include "tablet_cell_controller.h"
#include "serialize.h"
#include "automaton.h"
#include "tablet_manager.h"
#include "transaction_manager.h"
#include "tablet_service.h"
#include "private.h"

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/fiber.h>
#include <core/concurrency/action_queue.h>

#include <core/rpc/server.h>

#include <core/logging/tagged_logger.h>

#include <ytlib/election/config.h>
#include <ytlib/election/cell_manager.h>

#include <ytlib/hive/cell_directory.h>
#include <ytlib/hive/timestamp_provider.h>

#include <server/election/election_manager.h>

#include <server/hydra/changelog.h>
#include <server/hydra/changelog_catalog.h>
#include <server/hydra/snapshot.h>
#include <server/hydra/snapshot_catalog.h>
#include <server/hydra/hydra_manager.h>
#include <server/hydra/distributed_hydra_manager.h>

#include <server/hive/hive_manager.h>
#include <server/hive/mailbox.h>
#include <server/hive/transaction_supervisor.h>

#include <server/cell_node/bootstrap.h>
#include <server/cell_node/config.h>

#include <server/data_node/config.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NElection;
using namespace NHydra;
using namespace NHive;
using namespace NNodeTrackerClient::NProto;
using NHydra::EPeerState;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TTabletSlot::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TTabletSlot* owner,
        int slotIndex,
        NCellNode::TCellNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap)
        : Owner(owner)
        , SlotIndex(slotIndex)
        , Config(config)
        , Bootstrap(bootstrap)
        , AutomatonQueue(New<TActionQueue>(Sprintf("TabletSlot:%d", SlotIndex)))
        , Logger(TabletNodeLogger)
    {
        Reset();
    }


    const TCellGuid& GetCellGuid() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return CellGuid;
    }

    EPeerState GetState() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (HydraManager) {
            State = HydraManager->GetControlState();
        }

        return State;
    }

    TPeerId GetPeerId() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return PeerId;
    }

    const NHydra::NProto::TCellConfig& GetCellConfig() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return CellConfig;
    }

    IHydraManagerPtr GetHydraManager() const
    {
        return HydraManager;
    }

    TTabletAutomatonPtr GetAutomaton() const
    {
        return Automaton;
    }

    IInvokerPtr GetAutomatonInvoker() const
    {
        return AutomatonQueue->GetInvoker();
    }

    IInvokerPtr GetEpochAutomatonInvoker() const
    {
        return EpochAutomatonInvoker;
    }

    THiveManagerPtr GetHiveManager() const
    {
        return HiveManager;
    }

    TMailbox* GetMasterMailbox()
    {
        // Create master mailbox lazily.
        if (!MasterMailbox) {
            auto masterCellGuid = Bootstrap->GetCellGuid();
            MasterMailbox = HiveManager->GetOrCreateMailbox(masterCellGuid);
        }
        return MasterMailbox;
    }

    TTransactionManagerPtr GetTransactionManager() const
    {
        return TransactionManager;
    }

    TTransactionSupervisorPtr GetTransactionSupervisor() const
    {
        return TransactionSupervisor;
    }

    TTabletManagerPtr GetTabletManager() const
    {
        return TabletManager;
    }


    void Load(const TCellGuid& cellGuid)
    {
        // NB: Load is called from bootstrap thread.
        YCHECK(State == EPeerState::None);

        SetCellGuid(cellGuid);

        LOG_INFO("Loading slot");

        State = EPeerState::Initializing;

        auto tabletCellController = Bootstrap->GetTabletCellController();
        ChangelogStore = tabletCellController->GetChangelogCatalog()->GetStore(CellGuid);
        SnapshotStore = tabletCellController->GetSnapshotCatalog()->GetStore(CellGuid);

        State = EPeerState::Stopped;

        LOG_INFO("Slot loaded");
    }

    void Create(const TCreateTabletSlotInfo& createInfo)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(State == EPeerState::None);

        auto cellGuid = FromProto<TCellGuid>(createInfo.cell_guid());
        SetCellGuid(cellGuid);

        LOG_INFO("Creating slot");

        State = EPeerState::Initializing;

        auto this_ = MakeStrong(this);
        BIND([this, this_] () {
            SwitchToIOThread();

            auto tabletCellController = Bootstrap->GetTabletCellController();
            ChangelogStore = tabletCellController->GetChangelogCatalog()->CreateStore(CellGuid);
            SnapshotStore = tabletCellController->GetSnapshotCatalog()->CreateStore(CellGuid);

            SwitchToControlThread();

            State = EPeerState::Stopped;

            LOG_INFO("Slot created");
        })
        .AsyncVia(Bootstrap->GetControlInvoker())
        .Run();
    }

    void Configure(const TConfigureTabletSlotInfo& configureInfo)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(State != EPeerState::Initializing && State != EPeerState::Finalizing);

        auto cellConfig = New<TCellConfig>();
        cellConfig->CellGuid = CellGuid;
        // NB: Missing peers will be represented by empty strings.
        cellConfig->Addresses.resize(configureInfo.config().size());
        for (const auto& peer : configureInfo.config().peers()) {
            cellConfig->Addresses[peer.peer_id()] = peer.address();
        }

        if (HydraManager) {
            CellManager->Reconfigure(cellConfig);
        } else {
            PeerId = configureInfo.peer_id();
            State = EPeerState::Elections;

            CellManager = New<TCellManager>(
                cellConfig,
                configureInfo.peer_id());

            Automaton = New<TTabletAutomaton>(Bootstrap, Owner);

            HydraManager = CreateDistributedHydraManager(
                Config->TabletNode->Hydra,
                Bootstrap->GetControlInvoker(),
                GetAutomatonInvoker(),
                Automaton,
                Bootstrap->GetRpcServer(),
                CellManager,
                ChangelogStore,
                SnapshotStore);

            HydraManager->SubscribeStartLeading(
                BIND(&TImpl::OnStartEpoch, Unretained(this)));
            HydraManager->SubscribeStartFollowing(
                BIND(&TImpl::OnStartEpoch, Unretained(this)));
            HydraManager->SubscribeStopLeading(
                BIND(&TImpl::OnStopEpoch, Unretained(this)));
            HydraManager->SubscribeStopFollowing(
                BIND(&TImpl::OnStopEpoch, Unretained(this)));

            HiveManager = New<THiveManager>(
                CellGuid,
                Config->TabletNode->Hive,
                Bootstrap->GetCellRegistry(),
                GetAutomatonInvoker(),
                Bootstrap->GetRpcServer(),
                HydraManager,
                Automaton);

            TabletManager = New<TTabletManager>(
                Config->TabletNode->TabletManager,
                Owner,
                Bootstrap);

            TransactionManager = New<TTransactionManager>(
                Config->TabletNode->TransactionManager,
                Owner,
                Bootstrap);

            TransactionSupervisor = New<TTransactionSupervisor>(
                Config->TabletNode->TransactionSupervisor,
                GetAutomatonInvoker(),
                Bootstrap->GetRpcServer(),
                HydraManager,
                Automaton,
                HiveManager,
                TransactionManager,
                Bootstrap->GetTimestampProvider());

            TabletService = New<TTabletService>(
                Owner,
                Bootstrap);

            TabletManager->Initialize();
            HydraManager->Start();
            HiveManager->Start();

            auto rpcServer = Bootstrap->GetRpcServer();
            rpcServer->RegisterService(TabletService);
        }

        CellConfig = configureInfo.config();

        LOG_INFO("Slot configured");
    }

    void Remove()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(State != EPeerState::None);
        
        LOG_INFO("Removing slot");
        
        State = EPeerState::Finalizing;

        auto this_ = MakeStrong(this);
        BIND([this, this_] () {
            SwitchToIOThread();

            auto tabletCellController = Bootstrap->GetTabletCellController();
            tabletCellController->GetChangelogCatalog()->RemoveStore(CellGuid);
            tabletCellController->GetSnapshotCatalog()->RemoveStore(CellGuid);

            SwitchToControlThread();

            SnapshotStore.Reset();
            ChangelogStore.Reset();
            Reset();

            LOG_INFO("Slot removed");
        })
        .AsyncVia(Bootstrap->GetControlInvoker())
        .Run();
    }


private:
    TTabletSlot* Owner;
    int SlotIndex;
    NCellNode::TCellNodeConfigPtr Config;
    NCellNode::TBootstrap* Bootstrap;

    TCellGuid CellGuid;
    mutable EPeerState State;
    TPeerId PeerId;
    NHydra::NProto::TCellConfig CellConfig;

    IChangelogStorePtr ChangelogStore;
    ISnapshotStorePtr SnapshotStore;
    TCellManagerPtr CellManager;
    IHydraManagerPtr HydraManager;
    
    THiveManagerPtr HiveManager;
    TMailbox* MasterMailbox;

    TTabletManagerPtr TabletManager;

    TTransactionManagerPtr TransactionManager;
    TTransactionSupervisorPtr TransactionSupervisor;

    TTabletServicePtr TabletService;

    TTabletAutomatonPtr Automaton;
    TActionQueuePtr AutomatonQueue;
    IInvokerPtr EpochAutomatonInvoker;

    NLog::TTaggedLogger Logger;


    void Reset()
    {
        SetCellGuid(NullCellGuid);

        State = EPeerState::None;
        
        PeerId = InvalidPeerId;
        
        CellConfig = NHydra::NProto::TCellConfig();
        
        CellManager.Reset();

        if (HydraManager) {
            HydraManager->Stop();
            HydraManager.Reset();
        }

        if (HiveManager) {
            HiveManager->Stop();
            HiveManager.Reset();
        }

        MasterMailbox = nullptr;

        TabletManager.Reset();

        TransactionManager.Reset();

        TransactionSupervisor.Reset();

        if (TabletService) {
            auto rpcServer = Bootstrap->GetRpcServer();
            rpcServer->UnregisterService(TabletService);
            TabletService.Reset();
        }

        Automaton.Reset();

        EpochAutomatonInvoker.Reset();
    }

    void SetCellGuid(const TCellGuid& cellGuid)
    {
        CellGuid = cellGuid;
        InitLogger();
    }

    void InitLogger()
    {
        Logger = NLog::TTaggedLogger(TabletNodeLogger);
        Logger.AddTag(Sprintf("Slot: %d", SlotIndex));
        if (CellGuid != NullCellGuid) {
            Logger.AddTag(Sprintf("CellGuid: %s", ~ToString(CellGuid)));
        }
    }


    void SwitchToIOThread()
    {
        SwitchTo(HydraIOQueue->GetInvoker());
        VERIFY_THREAD_AFFINITY(IOThread);
    }

    void SwitchToControlThread()
    {
        SwitchTo(Bootstrap->GetControlInvoker());
        VERIFY_THREAD_AFFINITY(ControlThread);
    }


    void OnStartEpoch()
    {
        EpochAutomatonInvoker = HydraManager
            ->GetEpochContext()
            ->CancelableContext
            ->CreateInvoker(GetAutomatonInvoker());
    }

    void OnStopEpoch()
    {
        EpochAutomatonInvoker.Reset();
    }


    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(IOThread);

};

////////////////////////////////////////////////////////////////////////////////

TTabletSlot::TTabletSlot(
    int slotIndex,
    NCellNode::TCellNodeConfigPtr config,
    NCellNode::TBootstrap* bootstrap)
    : Impl(New<TImpl>(
        this,
        slotIndex,
        config,
        bootstrap))
{ }

TTabletSlot::~TTabletSlot()
{ }

const TCellGuid& TTabletSlot::GetCellGuid() const
{
    return Impl->GetCellGuid();
}

EPeerState TTabletSlot::GetState() const
{
    return Impl->GetState();
}

TPeerId TTabletSlot::GetPeerId() const
{
    return Impl->GetPeerId();
}

const NHydra::NProto::TCellConfig& TTabletSlot::GetCellConfig() const
{
    return Impl->GetCellConfig();
}

IHydraManagerPtr TTabletSlot::GetHydraManager() const
{
    return Impl->GetHydraManager();
}

TTabletAutomatonPtr TTabletSlot::GetAutomaton() const
{
    return Impl->GetAutomaton();
}

IInvokerPtr TTabletSlot::GetAutomatonInvoker() const
{
    return Impl->GetAutomatonInvoker();
}

IInvokerPtr TTabletSlot::GetEpochAutomatonInvoker() const
{
    return Impl->GetEpochAutomatonInvoker();
}

THiveManagerPtr TTabletSlot::GetHiveManager() const
{
    return Impl->GetHiveManager();
}

TMailbox* TTabletSlot::GetMasterMailbox()
{
    return Impl->GetMasterMailbox();
}

TTransactionManagerPtr TTabletSlot::GetTransactionManager() const
{
    return Impl->GetTransactionManager();
}

TTransactionSupervisorPtr TTabletSlot::GetTransactionSupervisor() const
{
    return Impl->GetTransactionSupervisor();
}

TTabletManagerPtr TTabletSlot::GetTabletManager() const
{
    return Impl->GetTabletManager();
}

void TTabletSlot::Load(const TCellGuid& cellGuid)
{
    Impl->Load(cellGuid);
}

void TTabletSlot::Create(const TCreateTabletSlotInfo& createInfo)
{
    Impl->Create(createInfo);
}

void TTabletSlot::Configure(const TConfigureTabletSlotInfo& configureInfo)
{
    Impl->Configure(configureInfo);
}

void TTabletSlot::Remove()
{
    Impl->Remove();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NTabletNode
