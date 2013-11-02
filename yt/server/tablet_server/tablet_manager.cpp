#include "stdafx.h"
#include "tablet_manager.h"
#include "tablet_cell.h"
#include "tablet.h"
#include "tablet_cell_proxy.h"
#include "tablet_proxy.h"
#include "cypress_integration.h"
#include "config.h"
#include "private.h"

// TODO(babenko): move with tablet tracker
#include <core/concurrency/periodic_executor.h>

#include <core/misc/address.h>

#include <ytlib/hive/cell_directory.h>

#include <ytlib/new_table_client/schema.h>

#include <server/object_server/type_handler_detail.h>

#include <server/tablet_server/tablet_manager.pb.h>

#include <server/node_tracker_server/node_tracker.h>
#include <server/node_tracker_server/node.h>

#include <server/table_server/table_node.h>

#include <server/tablet_node/tablet_manager.pb.h>

#include <server/hive/hive_manager.h>

#include <server/chunk_server/chunk_list.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/meta_state_facade.h>
#include <server/cell_master/serialization_context.h>

#include <util/random/random.h>

namespace NYT {
namespace NTabletServer {

using namespace NObjectServer;
using namespace NYTree;
using namespace NSecurityServer;
using namespace NTableServer;
using namespace NHydra;
using namespace NTransactionServer;
using namespace NCellMaster;
using namespace NTabletServer::NProto;
using namespace NNodeTrackerServer;
using namespace NNodeTrackerServer::NProto;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NTabletNode::NProto;
using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = TabletServerLogger;

////////////////////////////////////////////////////////////////////////////////

static const TDuration CellsScanPeriod = TDuration::Seconds(3);
using namespace NConcurrency;

class TCandidatePool
{
public:
    explicit TCandidatePool(NCellMaster::TBootstrap* bootstrap)
        : Bootstrap(bootstrap)
    {
        auto nodeTracker = Bootstrap->GetNodeTracker();
        for (auto* node : nodeTracker->Nodes().GetValues()) {
            if (HasFreeSlots(node)) {
                YCHECK(Candidates.insert(node).second);
            }
        }
    }

    TNode* TryAllocate(
        TTabletCell* cell,
        const TSmallSet<Stroka, TypicalCellSize>& forbiddenAddresses)
    {
        for (auto it = Candidates.begin(); it != Candidates.end(); ++it) {
            auto* node = *it;
            if (forbiddenAddresses.count(node->GetAddress()) == 0) {
                node->AddTabletSlotHint();
                if (!HasFreeSlots(node)) {
                    Candidates.erase(it);
                }
                return node;
            }
        }
        return nullptr;
    }

private:
    NCellMaster::TBootstrap* Bootstrap;

    yhash_set<TNode*> Candidates;

    static bool HasFreeSlots(TNode* node)
    {
        return
            node->Statistics().available_tablet_slots() >
            node->TabletCellCreateQueue().size();
    }

};

class TTabletTracker
    : public TRefCounted
{
public:
    explicit TTabletTracker(
        TTabletManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

    void Start();
    void Stop();

private:
    TTabletManagerConfigPtr Config;
    NCellMaster::TBootstrap* Bootstrap;

    TInstant StartTime;
    NConcurrency::TPeriodicExecutorPtr PeriodicExecutor;


    void ScanCells();

    void ScheduleStateChange(TTabletCell* cell);
    void SchedulePeerStart(TTabletCell* cell, TCandidatePool* pool);
    void SchedulePeerFailover(TTabletCell* cell);

    bool IsFailoverNeeded(TTabletCell* cell, TPeerId peerId);
    bool IsFailoverPossible(TTabletCell* cell);
    

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

};

TTabletTracker::TTabletTracker(
    TTabletManagerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap)
    : Config(config)
    , Bootstrap(bootstrap)
{
    YCHECK(Config);
    YCHECK(Bootstrap);
    VERIFY_INVOKER_AFFINITY(Bootstrap->GetMetaStateFacade()->GetInvoker(), AutomatonThread);
}

void TTabletTracker::Start()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    StartTime = TInstant::Now();

    YCHECK(!PeriodicExecutor);
    PeriodicExecutor = New<TPeriodicExecutor>(
        Bootstrap->GetMetaStateFacade()->GetEpochInvoker(),
        BIND(&TTabletTracker::ScanCells, MakeWeak(this)),
        CellsScanPeriod);
    PeriodicExecutor->Start();
}

void TTabletTracker::Stop()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (PeriodicExecutor) {
        PeriodicExecutor->Stop();
        PeriodicExecutor.Reset();
    }

    auto nodeTracker = Bootstrap->GetNodeTracker();
    for (auto* node : nodeTracker->Nodes().GetValues()) {
        node->TabletCellCreateQueue().clear();
    }
}

void TTabletTracker::ScanCells()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TCandidatePool pool(Bootstrap);

    auto tabletManger = Bootstrap->GetTabletManager();
    for (auto* cell : tabletManger->TabletCells().GetValues()) {
        if (!IsObjectAlive(cell))
            continue;

        ScheduleStateChange(cell);
        SchedulePeerStart(cell, &pool);
        SchedulePeerFailover(cell);
    }
}

void TTabletTracker::ScheduleStateChange(TTabletCell* cell)
{
    if (cell->GetState() != ETabletCellState::Starting)
        return;

    if (cell->GetOnlinePeerCount() < cell->GetSize())
        return;

    // All peers online, change state to running.
    TReqSetCellState request;
    ToProto(request.mutable_cell_id(), cell->GetId());
    request.set_state(ETabletCellState::Running);

    Bootstrap
        ->GetTabletManager()
        ->CreateSetCellStateMutation(request)
        ->PostCommit();
}

void TTabletTracker::SchedulePeerStart(TTabletCell* cell, TCandidatePool* pool)
{   
    TReqStartSlots request;
    ToProto(request.mutable_cell_id(), cell->GetId());

    const auto& peers = cell->Peers();
    for (int index = 0; index < static_cast<int>(peers.size()); ++index) {
        request.add_node_ids(InvalidNodeId);
    }

    TSmallSet<Stroka, TypicalCellSize> forbiddenAddresses;
    for (const auto& peer : cell->Peers()) {
        if (peer.Address) {
            forbiddenAddresses.insert(*peer.Address);
        }
    }

    bool assigned = false;
    for (int index = 0; index < static_cast<int>(peers.size()); ++index) {
        if (cell->Peers()[index].Address)
            continue;

        auto* node = pool->TryAllocate(cell, forbiddenAddresses);
        if (!node)
            break;

        request.set_node_ids(index, node->GetId());
        forbiddenAddresses.insert(node->GetAddress());
        assigned = true;
    }

    if (assigned) {
        Bootstrap
            ->GetTabletManager()
            ->CreateStartSlotsMutation(request)
            ->PostCommit();
    }
}

void TTabletTracker::SchedulePeerFailover(TTabletCell* cell)
{
    // Don't perform failover until enough time has passed since the start.
    if (TInstant::Now() < StartTime + Config->PeerFailoverTimeout)
        return;

    const auto& cellId = cell->GetId();

    // Look for timed out peers.
    for (TPeerId peerId = 0; peerId < static_cast<int>(cell->Peers().size()); ++peerId) {
        if (IsFailoverNeeded(cell, peerId) && IsFailoverPossible(cell)) {
            TReqRevokePeer request;
            ToProto(request.mutable_cell_id(), cellId);
            request.set_peer_id(peerId);

            Bootstrap
                ->GetTabletManager()
                ->CreateRevokePeerMutation(request)
                ->PostCommit();
        }
    }
}

bool TTabletTracker::IsFailoverNeeded(TTabletCell* cell, TPeerId peerId)
{
    const auto& peer = cell->Peers()[peerId];
    if (!peer.Address)
        return false;

    if (peer.Node)
        return false;

    if (peer.LastSeenTime > TInstant::Now() - Config->PeerFailoverTimeout)
        return false;

    return true;
}

bool TTabletTracker::IsFailoverPossible(TTabletCell* cell)
{
    switch (cell->GetState()) {
        case ETabletCellState::Starting:
            // Failover is always safe when starting.
            return true;

        case ETabletCellState::Running: {
            // Must have at least quorum.
            if (cell->GetOnlinePeerCount() < (cell->GetSize() + 1) /2)
                return false;

            // Must have completed recovery.
            for (const auto& peer : cell->Peers()) {
                if (peer.Node) {
                    const auto* slot = peer.Node->GetTabletSlot(cell);
                    if (slot->PeerState != EPeerState::Leading && slot->PeerState != EPeerState::Following) {
                        return false;
                    }
                }
            }
            return true;
        }

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

class TTabletManager::TTabletCellTypeHandler
    : public TObjectTypeHandlerWithMapBase<TTabletCell>
{
public:
    explicit TTabletCellTypeHandler(TImpl* owner);

    virtual EObjectType GetType() const override
    {
        return EObjectType::TabletCell;
    }

    virtual TNullable<TTypeCreationOptions> GetCreationOptions() const override
    {
        return TTypeCreationOptions(
            EObjectTransactionMode::Forbidden,
            EObjectAccountMode::Forbidden,
            false);
    }

    virtual TObjectBase* Create(
        TTransaction* transaction,
        TAccount* account,
        IAttributeDictionary* attributes,
        TReqCreateObjects* request,
        TRspCreateObjects* response) override;

private:
    TImpl* Owner;

    virtual Stroka DoGetName(TTabletCell* object) override
    {
        return Sprintf("tablet cell %s", ~ToString(object->GetId()));
    }

    virtual IObjectProxyPtr DoGetProxy(TTabletCell* cell, TTransaction* /*transaction*/) override
    {
        return CreateTabletCellProxy(Bootstrap, cell);
    }
    
    virtual void DoDestroy(TTabletCell* cell) override;

};

////////////////////////////////////////////////////////////////////////////////

class TTabletManager::TTabletTypeHandler
    : public TObjectTypeHandlerWithMapBase<TTablet>
{
public:
    explicit TTabletTypeHandler(TImpl* owner);

    virtual EObjectType GetType() const override
    {
        return EObjectType::Tablet;
    }

private:
    TImpl* Owner;

    virtual Stroka DoGetName(TTablet* object) override
    {
        return Sprintf("tablet %s", ~ToString(object->GetId()));
    }

    virtual IObjectProxyPtr DoGetProxy(TTablet* tablet, TTransaction* /*transaction*/) override
    {
        return CreateTabletProxy(Bootstrap, tablet);
    }

    virtual void DoDestroy(TTablet* tablet) override;

};

////////////////////////////////////////////////////////////////////////////////

class TTabletManager::TImpl
    : public TMasterAutomatonPart
{
public:
    explicit TImpl(
        TTabletManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap)
        , Config(config)
        , TabletTracker(New<TTabletTracker>(Config, Bootstrap))
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap->GetMetaStateFacade()->GetInvoker(), AutomatonThread);

        RegisterLoader(
            "TabletManager.Keys",
            BIND(&TImpl::LoadKeys, MakeStrong(this)));
        RegisterLoader(
            "TabletManager.Values",
            BIND(&TImpl::LoadValues, MakeStrong(this)));

        RegisterSaver(
            ESerializationPriority::Keys,
            "TabletManager.Keys",
            BIND(&TImpl::SaveKeys, MakeStrong(this)));
        RegisterSaver(
            ESerializationPriority::Values,
            "TabletManager.Values",
            BIND(&TImpl::SaveValues, MakeStrong(this)));

        RegisterMethod(BIND(&TImpl::StartSlots, Unretained(this)));
        RegisterMethod(BIND(&TImpl::SetCellState, Unretained(this)));
        RegisterMethod(BIND(&TImpl::RevokePeer, Unretained(this)));
        RegisterMethod(BIND(&TImpl::OnTabletCreated, Unretained(this)));
        RegisterMethod(BIND(&TImpl::OnTabletRemoved, Unretained(this)));

        auto nodeTracker = Bootstrap->GetNodeTracker();
        nodeTracker->SubscribeNodeRegistered(BIND(&TImpl::OnNodeRegistered, MakeWeak(this)));
        nodeTracker->SubscribeNodeUnregistered(BIND(&TImpl::OnNodeUnregistered, MakeWeak(this)));
        nodeTracker->SubscribeIncrementalHeartbeat(BIND(&TImpl::OnIncrementalHeartbeat, MakeWeak(this)));
    }

    void Initialize()
    {
        auto objectManager = Bootstrap->GetObjectManager();
        objectManager->RegisterHandler(New<TTabletCellTypeHandler>(this));
        objectManager->RegisterHandler(New<TTabletTypeHandler>(this));
    }


    TMutationPtr CreateStartSlotsMutation(const TReqStartSlots& request)
    {
        return Bootstrap
            ->GetMetaStateFacade()
            ->CreateMutation(this, request, &TImpl::StartSlots);
    }

    TMutationPtr CreateSetCellStateMutation(const TReqSetCellState& request)
    {
        return Bootstrap
            ->GetMetaStateFacade()
            ->CreateMutation(this, request, &TImpl::SetCellState);
    }

    TMutationPtr CreateRevokePeerMutation(const TReqRevokePeer& request)
    {
        return Bootstrap
            ->GetMetaStateFacade()
            ->CreateMutation(this, request, &TImpl::RevokePeer);
    }


    TTabletCell* CreateCell(int size)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto objectManager = Bootstrap->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::TabletCell);
        auto* cell = new TTabletCell(id);
        cell->SetSize(size);
        cell->Peers().resize(size);
        
        TabletCellMap.Insert(id, cell);

        // Make the fake reference.
        YCHECK(cell->RefObject() == 1);   

        auto hiveManager = Bootstrap->GetHiveManager();
        hiveManager->CreateMailbox(id);

        return cell;
    }

    void DestroyCell(TTabletCell* cell)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        for (const auto& peer : cell->Peers()) {
            if (peer.Node) {
                peer.Node->DetachTabletCell(cell);
            }
        }

        auto hiveManager = Bootstrap->GetHiveManager();
        hiveManager->RemoveMailbox(cell->GetId());
    }


    TTablet* CreateTablet(TTableNode* table, TTabletCell* cell)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto objectManager = Bootstrap->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::Tablet);
        auto* tablet = new TTablet(id);
        tablet->SetTable(table);
        tablet->SetCell(cell);
        TabletMap.Insert(id, tablet);
        return tablet;
    }

    void DestroyTablet(TTablet* tablet)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* cell = tablet->GetCell();
        YCHECK(cell->Tablets().erase(tablet) == 1);
    }


    void MountTable(TTableNode* table)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(table->IsTrunk());

        if (table->IsMounted()) {
            THROW_ERROR_EXCEPTION("Table is already mounted");
        }

        if (!table->IsSorted()) {
            THROW_ERROR_EXCEPTION("Table is not sorted");
        }

        auto objectManager = Bootstrap->GetObjectManager();
        auto hiveManager = Bootstrap->GetHiveManager();

        auto tableProxy = objectManager->GetProxy(table);

        auto* cell = AllocateCell(); // may throw

        auto* tablet = CreateTablet(table, cell);
        tablet->SetState(ETabletState::Initializing);
        table->SetTablet(tablet);
        objectManager->RefObject(tablet);

        YCHECK(cell->Tablets().insert(tablet).second);

        {
            TReqCreateTablet req;
            
            ToProto(req.mutable_tablet_id(), tablet->GetId());
            
            // COMPAT(babenko): schema must be mandatory
            auto schema = tableProxy->Attributes().Get<TTableSchema>("schema", TTableSchema());
            ToProto(req.mutable_schema(), schema);

            const auto* chunkList = table->GetChunkList();
            ToProto(req.mutable_key_columns()->mutable_names(), chunkList->SortedBy());
            
            auto* mailbox = hiveManager->GetMailbox(cell->GetId());
            hiveManager->PostMessage(mailbox, req);
        }

        LOG_INFO_UNLESS(IsRecovery(), "Mounting table (NodeId: %s, TabletId: %s, CellId: %s)",
            ~ToString(table->GetId()),
            ~ToString(tablet->GetId()),
            ~ToString(cell->GetId()));
    }

    void UnmountTable(TTableNode* table)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(table->IsTrunk());

        if (!table->IsMounted()) {
            THROW_ERROR_EXCEPTION("Table is not mounted");
        }

        auto* tablet = table->GetTablet();
        if (tablet->GetState() != ETabletState::Running) {
            THROW_ERROR_EXCEPTION("Tablet is not running");
        }

        tablet->SetState(ETabletState::Finalizing);

        auto* cell = tablet->GetCell();

        auto hiveManager = Bootstrap->GetHiveManager();

        {
            TReqRemoveTablet req;
            ToProto(req.mutable_tablet_id(), tablet->GetId());
            auto* mailbox = hiveManager->GetMailbox(cell->GetId());
            hiveManager->PostMessage(mailbox, req);
        }

        LOG_INFO_UNLESS(IsRecovery(), "Unmounting table (NodeId: %s, TabletId: %s, CellId: %s)",
            ~ToString(table->GetId()),
            ~ToString(tablet->GetId()),
            ~ToString(cell->GetId()));
    }


    DECLARE_ENTITY_MAP_ACCESSORS(TabletCell, TTabletCell, TTabletCellId);
    DECLARE_ENTITY_MAP_ACCESSORS(Tablet, TTablet, TTabletId);

private:
    friend class TTabletCellTypeHandler;
    friend class TTabletTypeHandler;

    TTabletManagerConfigPtr Config;

    TTabletTrackerPtr TabletTracker;

    NHydra::TEntityMap<TTabletCellId, TTabletCell> TabletCellMap;
    NHydra::TEntityMap<TTabletId, TTablet> TabletMap;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    
    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        TabletCellMap.SaveKeys(context);
        TabletMap.SaveKeys(context);
    }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        TabletCellMap.SaveValues(context);
        TabletMap.SaveValues(context);
    }


    virtual void OnBeforeSnapshotLoaded() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        DoClear();
    }

    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TabletCellMap.LoadKeys(context);
        TabletMap.LoadKeys(context);
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TabletCellMap.LoadValues(context);
        TabletMap.LoadValues(context);
    }


    void DoClear()
    {
        TabletCellMap.Clear();
        TabletMap.Clear();
    }

    virtual void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        DoClear();
    }


    void OnNodeRegistered(TNode* node)
    {
        const auto& statistics = node->Statistics();
        node->TabletSlots().resize(statistics.available_tablet_slots() + statistics.used_tablet_slots());
    }

    void OnNodeUnregistered(TNode* node)
    {
        for (const auto& slot : node->TabletSlots()) {
            auto* cell = slot.Cell;
            if (cell) {
                LOG_INFO_UNLESS(IsRecovery(), "Tablet peer offline: node unregistered (Address: %s, CellId: %s, PeerId: %d)",
                    ~node->GetAddress(),
                    ~ToString(cell->GetId()),
                    slot.PeerId);
                cell->DetachPeer(node);
            }
        }

        for (auto* cell : node->TabletCellCreateQueue()) {
            cell->DetachPeer(node);
        }
    }

    void OnIncrementalHeartbeat(
        TNode* node,
        const NNodeTrackerServer::NProto::TReqIncrementalHeartbeat& request,
        TRspIncrementalHeartbeat* response)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        // Various request helpers.

        auto requestCreate = [&] (TTabletCell* cell) {
            if (!response)
                return;

            auto* createInfo = response->add_tablet_slots_to_create();

            const auto& cellId = cell->GetId();
            ToProto(createInfo->mutable_cell_guid(), cell->GetId());

            LOG_INFO_UNLESS(IsRecovery(), "Tablet slot creation requested (Address: %s, CellId: %s)",
                ~node->GetAddress(),
                ~ToString(cellId));
        };

        auto requestConfigure = [&] (TTabletCell* cell) {
            if (!response)
                return;

            auto* configureInfo = response->add_tablet_slots_configure();

            const auto& cellId = cell->GetId();
            ToProto(configureInfo->mutable_cell_guid(), cell->GetId());
            configureInfo->set_peer_id(cell->GetPeerId(node));
            *configureInfo->mutable_config() = cell->Config();

            LOG_INFO_UNLESS(IsRecovery(), "Tablet slot configuration update requested (Address: %s, CellId: %s, Version: %d)",
                ~node->GetAddress(),
                ~ToString(cellId),
                cell->Config().version());
        };

        auto requestRemove = [&] (const TTabletCellId& cellId) {
            if (!response)
                return;

            auto* removeInfo = response->add_tablet_slots_to_remove();
            ToProto(removeInfo->mutable_cell_guid(), cellId);

            LOG_INFO_UNLESS(IsRecovery(), "Tablet slot removal requested (Address: %s, CellId: %s)",
                ~node->GetAddress(),
                ~ToString(cellId));
        };

        auto metaStateFacade = Bootstrap->GetMetaStateFacade();
        auto hydraManager = metaStateFacade->GetManager();
        auto* mutationContext = hydraManager->GetMutationContext();

        // Our expectations.
        yhash_map<TTabletCell*, TNode::TTabletSlot*> expectedCellToSlot;
        for (auto& slot : node->TabletSlots()) {
            if (slot.Cell) {
                YCHECK(expectedCellToSlot.insert(std::make_pair(slot.Cell, &slot)).second);
            }
        }

        // Figure out and analyze the reality.
        yhash_map<TTabletCell*, int> actualCellToIndex;
        for (int slotIndex = 0; slotIndex < request.tablet_slots_size(); ++slotIndex) {
            auto& slot = node->TabletSlots()[slotIndex];
            const auto& slotInfo = request.tablet_slots(slotIndex);

            // Pre-erase slot.
            slot = TNode::TTabletSlot();

            auto cellId = FromProto<TTabletCellId>(slotInfo.cell_guid());
            if (cellId == NullTabletCellId)
                continue;

            auto* cell = FindTabletCell(cellId);
            if (!IsObjectAlive(cell)) {
                LOG_INFO_UNLESS(IsRecovery(), "Unknown tablet slot is running (Address: %s, CellId: %s)",
                    ~node->GetAddress(),
                    ~ToString(cellId));
                requestRemove(cellId);
                continue;
            }

            if (node->TabletCellCreateQueue().erase(cell) == 1) {
                LOG_INFO_UNLESS(IsRecovery(), "Tablet slot created (Address: %s, CellId: %s)",
                    ~node->GetAddress(),
                    ~ToString(cellId));
            }

            auto peerId = cell->FindPeerId(node->GetAddress());
            if (peerId == InvalidPeerId) {
                LOG_INFO_UNLESS(IsRecovery(), "Unexpected tablet cell is running (Address: %s, CellId: %s)",
                    ~node->GetAddress(),
                    ~ToString(cellId));
                requestRemove(cellId);
                continue;
            }

            if (slotInfo.peer_id() != InvalidPeerId && slotInfo.peer_id() != peerId)  {
                LOG_INFO_UNLESS(IsRecovery(), "Invalid peer id for tablet cell: %d instead of %d (Address: %s, CellId: %s)",
                    slotInfo.peer_id(),
                    peerId,
                    ~node->GetAddress(),
                    ~ToString(cellId));
                requestRemove(cellId);
                continue;
            }

            auto expectedIt = expectedCellToSlot.find(cell);
            if (expectedIt == expectedCellToSlot.end()) {
                cell->AttachPeer(node, peerId, slotIndex);
                LOG_INFO_UNLESS(IsRecovery(), "Tablet peer online (Address: %s, CellId: %s, PeerId: %d)",
                    ~node->GetAddress(),
                    ~ToString(cellId),
                    peerId);
            }

            cell->UpdatePeerSeenTime(peerId, mutationContext->GetTimestamp());
            YCHECK(actualCellToIndex.insert(std::make_pair(cell, slotIndex)).second);

            // Populate slot.
            slot.Cell = cell;
            slot.PeerState = EPeerState(slotInfo.peer_state());
            slot.PeerId = slot.Cell->GetPeerId(node); // don't trust peerInfo, it may still be InvalidPeerId

            LOG_DEBUG_UNLESS(IsRecovery(), "Tablet cell is running (Address: %s, CellId: %s, PeerId: %d, State: %s, ConfigVersion: %d)",
                ~node->GetAddress(),
                ~ToString(slot.Cell->GetId()),
                slot.PeerId,
                ~slot.PeerState.ToString(),
                slotInfo.config_version());

            // Request slot reconfiguration if states are appropriate and versions differ.
            if (slot.PeerState != EPeerState::Initializing &&
                slot.Cell->GetState() == ETabletCellState::Running &&
                slotInfo.config_version() != slot.Cell->Config().version())
            {
                requestConfigure(slot.Cell);
            }
        }

        // Check for expected slots that are missing.
        for (const auto& pair : expectedCellToSlot) {
            const auto* slot = pair.second;
            auto* cell = slot->Cell;
            if (actualCellToIndex.find(cell) == actualCellToIndex.end() &&
                node->TabletCellCreateQueue().find(cell) == node->TabletCellCreateQueue().end())
            {
                LOG_INFO_UNLESS(IsRecovery(), "Tablet peer offline: slot is missing (CellId: %s, Address: %s, PeerId: %d)",
                    ~ToString(cell->GetId()),
                    ~node->GetAddress(),
                    slot->PeerId);
                cell->DetachPeer(node);
            }
        }

        // Request slot starts.
        int availableSlots = node->Statistics().available_tablet_slots();
        for (auto* cell : node->TabletCellCreateQueue()) {
            if (availableSlots == 0)
                break;

            // Skip cells whose instances were already found on the node.
            // Only makes sense to protect from asking to create a slot that is already initializing.
            if (actualCellToIndex.find(cell) == actualCellToIndex.end()) {
                requestCreate(cell);
                --availableSlots;
            }
        }

        // Request to remove orphaned Hive cells and reconfigured outdates ones.
        for (const auto& cellInfo : request.hive_cells()) {
            auto cellGuid = FromProto<TCellGuid>(cellInfo.cell_guid());
            if (cellGuid == Bootstrap->GetCellGuid())
                continue;
            auto* cell = FindTabletCell(cellGuid);
            if (cell) {
                if (cellInfo.config_version() < cell->Config().version()) {
                    auto* reconfigureInfo = response->add_hive_cells_to_reconfigure();
                    ToProto(reconfigureInfo->mutable_cell_guid(), cellGuid);
                    reconfigureInfo->mutable_config()->CopyFrom(cell->Config());
                }
            } else {
                auto* unregisterInfo = response->add_hive_cells_to_unregister();
                ToProto(unregisterInfo->mutable_cell_guid(), cellGuid);
            }
        }
    }


    void StartSlots(const TReqStartSlots& request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);       

        auto cellId = FromProto<TTabletCellId>(request.cell_id());
        auto* cell = FindTabletCell(cellId);
        if (!IsObjectAlive(cell))
            return;

        auto metaStateFacade = Bootstrap->GetMetaStateFacade();
        auto hydraManager = metaStateFacade->GetManager();
        auto* mutationContext = hydraManager->GetMutationContext();
        auto nodeTracker = Bootstrap->GetNodeTracker();

        YCHECK(request.node_ids_size() == cell->Peers().size());
        for (TPeerId peerId = 0; peerId < request.node_ids_size(); ++peerId) {
            auto nodeId = request.node_ids(peerId);
            if (nodeId == InvalidNodeId)
                continue;
            
            auto* node = nodeTracker->FindNode(nodeId);
            if (!node)
                continue;

            if (cell->Peers()[peerId].Address)
                continue;

            if (node->TabletCellCreateQueue().find(cell) != node->TabletCellCreateQueue().end())
                continue;

            LOG_INFO_UNLESS(IsRecovery(), "Tablet slot creation scheduled (CellId: %s, Address: %s, PeerId: %d)",
                ~ToString(cellId),
                ~node->GetAddress(),
                peerId);

            cell->AssignPeer(node, peerId);
            cell->UpdatePeerSeenTime(peerId, mutationContext->GetTimestamp());
            YCHECK(node->TabletCellCreateQueue().insert(cell).second);
            ReconfigureCell(cell);
        }
    }

    void SetCellState(const TReqSetCellState& request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);       

        auto cellId = FromProto<TTabletCellId>(request.cell_id());
        auto* cell = FindTabletCell(cellId);
        if (!IsObjectAlive(cell))
            return;

        auto oldState = cell->GetState();
        auto newState = ETabletCellState(request.state());
        cell->SetState(newState);

        LOG_INFO_UNLESS(IsRecovery(), "Tablet cell state changed: %s->%s (CellId: %s)",
            ~oldState.ToString(),
            ~newState.ToString(),
            ~ToString(cellId));
    }

    void RevokePeer(const TReqRevokePeer& request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);       

        auto cellId = FromProto<TTabletCellId>(request.cell_id());
        auto* cell = FindTabletCell(cellId);
        if (!IsObjectAlive(cell))
            return;

        auto peerId = request.peer_id();
        const auto& peer = cell->Peers()[peerId];
        if (!peer.Address || peer.Node)
            return;
   
        LOG_INFO_UNLESS(IsRecovery(), "Tablet peer revoked (CellId: %s, Address: %s, PeerId: %d)",
            ~ToString(cell->GetId()),
            ~*peer.Address,
            peerId);

        cell->RevokePeer(peerId);
        ReconfigureCell(cell);
    }

    void OnTabletCreated(const TReqOnTabletCreated& request)
    {
        auto id = FromProto<TTabletId>(request.tablet_id());
        auto* tablet = FindTablet(id);
        if (!tablet || tablet->GetState() != ETabletState::Initializing)
            return;
        
        tablet->SetState(ETabletState::Running);

        auto* table = tablet->GetTable();
        auto* cell = tablet->GetCell();

        LOG_INFO_UNLESS(IsRecovery(), "Table mounted (NodeId: %s, TabletId: %s, CellId: %s)",
            ~ToString(table->GetId()),
            ~ToString(tablet->GetId()),
            ~ToString(cell->GetId()));
    }

    void OnTabletRemoved(const TReqOnTabletRemoved& request)
    {
        auto id = FromProto<TTabletId>(request.tablet_id());
        auto* tablet = FindTablet(id);
        if (!tablet || tablet->GetState() != ETabletState::Finalizing)
            return;

        tablet->SetState(ETabletState::Finalized);
        
        auto* table = tablet->GetTable();
        auto* cell = tablet->GetCell();

        LOG_INFO_UNLESS(IsRecovery(), "Table unmounted (NodeId: %s, TabletId: %s, CellId: %s)",
            ~ToString(table->GetId()),
            ~ToString(tablet->GetId()),
            ~ToString(cell->GetId()));

        auto objectManager = Bootstrap->GetObjectManager();
        objectManager->UnrefObject(tablet);

        table->SetTablet(nullptr);
    }


    void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TabletTracker->Start();

        auto cellRegistry = Bootstrap->GetCellRegistry();
        cellRegistry->Clear();

        for (const auto& pair : TabletCellMap) {
            auto* cell = pair.second;
            UpdateCellRegistry(cell);
        }
    }

    void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TabletTracker->Stop();
    }


    void ReconfigureCell(TTabletCell* cell)
    {
        auto& config = cell->Config();
        config.set_size(cell->GetSize());
        config.set_version(config.version() + 1);
        config.clear_peers();

        for (int index = 0; index < static_cast<int>(cell->Peers().size()); ++index) {
            const auto& peer = cell->Peers()[index];
            if (!peer.Address)
                continue;

            auto* peerInfo = config.add_peers();
            peerInfo->set_address(*peer.Address);
            peerInfo->set_peer_id(index);
        }

        UpdateCellRegistry(cell);

        LOG_INFO_UNLESS(IsRecovery(), "Tablet cell reconfigured (CellId: %s, Version: %d)",
            ~ToString(cell->GetId()),
            config.version());
    }

    void UpdateCellRegistry(TTabletCell* cell)
    {
        auto cellRegistry = Bootstrap->GetCellRegistry();
        cellRegistry->RegisterCell(cell->GetId(), cell->Config());
    }

    TTabletCell* AllocateCell()
    {
        // TODO(babenko): do something smarter?
        auto cells = TabletCellMap.GetValues();
        
        cells.erase(
            std::remove_if(
                cells.begin(),
                cells.end(),
                [] (const TTabletCell* cell) {
                    return cell->GetHealth() != ETabletCellHealth::Good;
                }),
            cells.end());

        if (cells.empty()) {
            THROW_ERROR_EXCEPTION("No healthy tablet cells");
        }

        return cells[RandomNumber<size_t>(cells.size())];
    }

};

DEFINE_ENTITY_MAP_ACCESSORS(TTabletManager::TImpl, TabletCell, TTabletCell, TTabletCellId, TabletCellMap)
DEFINE_ENTITY_MAP_ACCESSORS(TTabletManager::TImpl, Tablet, TTablet, TTabletId, TabletMap)

///////////////////////////////////////////////////////////////////////////////

TTabletManager::TTabletCellTypeHandler::TTabletCellTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap, &owner->TabletCellMap)
    , Owner(owner)
{ }

TObjectBase* TTabletManager::TTabletCellTypeHandler::Create(
    TTransaction* transaction,
    TAccount* account,
    IAttributeDictionary* attributes,
    TReqCreateObjects* request,
    TRspCreateObjects* response)
{
    int size = attributes->Get<int>("size");
    attributes->Remove("name");

    if (size < 1 || size > 9) {
        THROW_ERROR_EXCEPTION("\"size\" must be in range [1,9]");
    }

    if (size %2 == 0) {
        THROW_ERROR_EXCEPTION("\"size\" must be odd");
    }

    auto* cell = Owner->CreateCell(size);
    return cell;
}

void TTabletManager::TTabletCellTypeHandler::DoDestroy(TTabletCell* cell)
{
    Owner->DestroyCell(cell);
}

///////////////////////////////////////////////////////////////////////////////

TTabletManager::TTabletTypeHandler::TTabletTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap, &owner->TabletMap)
    , Owner(owner)
{ }

void TTabletManager::TTabletTypeHandler::DoDestroy(TTablet* tablet)
{
    Owner->DestroyTablet(tablet);
}

///////////////////////////////////////////////////////////////////////////////

TTabletManager::TTabletManager(
    TTabletManagerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap)
    : Impl(New<TImpl>(config, bootstrap))
{ }

TTabletManager::~TTabletManager()
{ }

void TTabletManager::Initialize()
{
    return Impl->Initialize();
}

TMutationPtr TTabletManager::CreateStartSlotsMutation(const TReqStartSlots& request)
{
    return Impl->CreateStartSlotsMutation(request);
}

TMutationPtr TTabletManager::CreateSetCellStateMutation(const TReqSetCellState& request)
{
    return Impl->CreateSetCellStateMutation(request);
}

TMutationPtr TTabletManager::CreateRevokePeerMutation(const TReqRevokePeer& request)
{
    return Impl->CreateRevokePeerMutation(request);
}

void TTabletManager::MountTable(TTableNode* table)
{
    Impl->MountTable(table);
}

void TTabletManager::UnmountTable(TTableNode* table)
{
    Impl->UnmountTable(table);
}

DELEGATE_ENTITY_MAP_ACCESSORS(TTabletManager, TabletCell, TTabletCell, TTabletCellId, *Impl)
DELEGATE_ENTITY_MAP_ACCESSORS(TTabletManager, Tablet, TTablet, TTabletId, *Impl)

///////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
