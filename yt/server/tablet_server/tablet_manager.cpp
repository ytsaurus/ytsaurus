#include "stdafx.h"
#include "tablet_manager.h"
#include "tablet_cell.h"
#include "tablet.h"
#include "tablet_cell_proxy.h"
#include "tablet_proxy.h"
#include "cypress_integration.h"
#include "config.h"
#include "tablet_tracker.h"
#include "private.h"

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
#include <server/cell_master/serialize.h>

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
        RegisterMethod(BIND(&TImpl::OnTabletMounted, Unretained(this)));
        RegisterMethod(BIND(&TImpl::OnTabletUnmounted, Unretained(this)));

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


    TTablet* CreateTablet(TTableNode* table)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto objectManager = Bootstrap->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::Tablet);
        auto* tablet = new TTablet(id);
        tablet->SetTable(table);
        TabletMap.Insert(id, tablet);
        objectManager->RefObject(tablet);

        LOG_INFO_UNLESS(IsRecovery(), "Tablet created (TableId: %s, TabletId: %s)",
            ~ToString(table->GetId()),
            ~ToString(tablet->GetId()));

        return tablet;
    }

    void DestroyTablet(TTablet* tablet)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* table = tablet->GetTable();

        YCHECK(!tablet->GetCell());

        LOG_INFO_UNLESS(IsRecovery(), "Tablet destroyed (TableId: %s, TabletId: %s)",
            ~ToString(table->GetId()),
            ~ToString(tablet->GetId()));
    }


    TTableSchema GetTableSchema(TTableNode* table)
    {
        auto objectManager = Bootstrap->GetObjectManager();
        auto tableProxy = objectManager->GetProxy(table);

        // COMPAT(babenko): schema must be mandatory
        auto schema = tableProxy->Attributes().Get<TTableSchema>("schema", TTableSchema());
        const auto& keyColumns = table->KeyColumns();

        // Ensure that every key column is mentioned in schema.
        // Move all key columns up the front.
        for (int keyIndex = 0; keyIndex < static_cast<int>(keyColumns.size()); ++keyIndex) {
            auto* column = schema.FindColumn(keyColumns[keyIndex]);
            if (!column) {
                THROW_ERROR_EXCEPTION("Schema does define a key column %s",
                    ~keyColumns[keyIndex].Quote());
            }
            int schemaIndex = schema.GetColumnIndex(*column);
            if (schemaIndex != keyIndex) {
                std::swap(schema.Columns()[schemaIndex], schema.Columns()[keyIndex]);
            }
        }
        return schema;
    }


    void MountTable(
        TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(table->IsTrunk());
        
        if (table->KeyColumns().empty()) {
            THROW_ERROR_EXCEPTION("Table has no key columns");
        }

        auto tabletRange = ParseTabletRange(table, firstTabletIndex, lastTabletIndex); // may throw
        auto schema = GetTableSchema(table); // may throw
        ValidateHasHealthyCells(); // may throw

        if (table->Tablets().empty()) {
            auto* tablet = CreateTablet(table);
            tablet->PivotKey() = EmptyKey();
            table->Tablets().push_back(tablet);
            tabletRange = std::make_pair(table->Tablets().begin(), table->Tablets().end());
        }

        for (auto it = tabletRange.first; it != tabletRange.second; ++it) {
            auto* tablet = *it;
            if (tablet->GetState() == ETabletState::Unmounting) {
                THROW_ERROR_EXCEPTION("Tablet %s is currently unmounting",
                    ~ToString(tablet->GetId()));
            }
        }

        auto objectManager = Bootstrap->GetObjectManager();

        for (auto it = tabletRange.first; it != tabletRange.second; ++it) {
            auto* tablet = *it;
            if (tablet->GetCell())
                continue;

            auto* cell = AllocateCell();
            tablet->SetCell(cell);
            YCHECK(cell->Tablets().insert(tablet).second);
            objectManager->RefObject(cell);

            YCHECK(tablet->GetState() == ETabletState::Unmounted);
            tablet->SetState(ETabletState::Mounting);

            TReqMountTablet req;           
            ToProto(req.mutable_tablet_id(), tablet->GetId());
            ToProto(req.mutable_schema(), schema);
            ToProto(req.mutable_key_columns()->mutable_names(), table->KeyColumns());
            
            auto hiveManager = Bootstrap->GetHiveManager();
            auto* mailbox = hiveManager->GetMailbox(cell->GetId());
            hiveManager->PostMessage(mailbox, req);

            LOG_INFO_UNLESS(IsRecovery(), "Mounting tablet (TableId: %s, TabletId: %s, CellId: %s)",
                ~ToString(table->GetId()),
                ~ToString(tablet->GetId()),
                ~ToString(cell->GetId()));
        }
    }

    void UnmountTable(
        TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(table->IsTrunk());

        auto tabletRange = ParseTabletRange(table, firstTabletIndex, lastTabletIndex); // may throw

        for (auto it = tabletRange.first; it != tabletRange.second; ++it) {
            auto* tablet = *it;
            if (tablet->GetState() == ETabletState::Mounting) {
                THROW_ERROR_EXCEPTION("Tablet %s is currently mounting",
                    ~ToString(tablet->GetId()));
            }
        }

        DoUnmountTable(table, tabletRange);
    }

    void ForceUnmountTable(TTableNode* table)
    {
        DoUnmountTable(
            table,
            std::make_pair(table->Tablets().begin(), table->Tablets().end()));

        for (auto* tablet : table->Tablets()) {
            DoTabletUnmounted(tablet);
        }

        table->Tablets().clear();
    }

    void ReshardTable(
        TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex,
        const std::vector<TOwningKey>& pivotKeys)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(table->IsTrunk());

        auto tabletRange = ParseTabletRange(table, firstTabletIndex, lastTabletIndex); // may throw

        auto& tablets = table->Tablets();
        int oldTabletCount = std::distance(tabletRange.first, tabletRange.second);
        int newTabletCount = static_cast<int>(pivotKeys.size());

        const int MaxTabletCount = 1000;
        if (tablets.size() - oldTabletCount + newTabletCount > MaxTabletCount) {
            THROW_ERROR_EXCEPTION("Tablet count cannot exceed the limit of %d",
                MaxTabletCount);
        }

        if (!pivotKeys.empty()) {
            if (tabletRange.first == tablets.end()) {
                if (CompareRows(pivotKeys[0], EmptyKey()) != 0) {
                    THROW_ERROR_EXCEPTION("First pivot key must be empty");
                }
            } else {
                if (CompareRows(pivotKeys[0], (*tabletRange.first)->PivotKey()) != 0) {
                    THROW_ERROR_EXCEPTION(
                        "First pivot key must match that of the first tablet "
                        "in the resharded range");
                }
            }
        }

        for (int index = 0; index < static_cast<int>(pivotKeys.size()) - 1; ++index) {
            if (CompareRows(pivotKeys[index], pivotKeys[index + 1]) >= 0) {
                THROW_ERROR_EXCEPTION("Pivot keys must be strictly increasing");
            }
        }

        if (tabletRange.second != tablets.end()) {
            if (CompareRows(pivotKeys.back(), (*tabletRange.second)->PivotKey()) >= 0) {
                THROW_ERROR_EXCEPTION(
                    "Last pivot key must be strictly less than that of the tablet "
                    "which follows the resharded range");
            }
        }

        // Validate that all tablets are unmounted.
        for (auto it = tabletRange.first; it != tabletRange.second; ++it) {
            auto* tablet = *it;
            if (tablet->GetState() != ETabletState::Unmounted) {
                THROW_ERROR_EXCEPTION("Cannot reshard table: tablet %s is in %s state",
                    ~ToString(tablet->GetId()),
                    ~FormatEnum(tablet->GetState()).Quote());
            }
        }

        // Perform resharding.
        auto objectManager = Bootstrap->GetObjectManager();
        for (auto it = tabletRange.first; it != tabletRange.second; ++it) {
            auto* tablet = *it;
            objectManager->UnrefObject(tablet);
        }

        std::vector<TTablet*> newTablets;
        for (int index = 0; index < newTabletCount; ++index) {
            auto* tablet = CreateTablet(table);
            tablet->PivotKey() = pivotKeys[index];
            newTablets.push_back(tablet);
        }

        int actualFirstTabletIndex = std::distance(tablets.begin(), tabletRange.first);
        tablets.erase(tabletRange.first, tabletRange.second);
        tablets.insert(tablets.begin() + actualFirstTabletIndex, newTablets.begin(), newTablets.end());
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

        auto requestCreateSlot = [&] (TTabletCell* cell) {
            if (!response)
                return;

            auto* createInfo = response->add_tablet_slots_to_create();

            const auto& cellId = cell->GetId();
            ToProto(createInfo->mutable_cell_guid(), cell->GetId());

            LOG_INFO_UNLESS(IsRecovery(), "Tablet slot creation requested (Address: %s, CellId: %s)",
                ~node->GetAddress(),
                ~ToString(cellId));
        };

        auto requestConfigureSlot = [&] (TTabletCell* cell) {
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

        auto requestRemoveSlot = [&] (const TTabletCellId& cellId) {
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
                requestRemoveSlot(cellId);
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
                requestRemoveSlot(cellId);
                continue;
            }

            if (slotInfo.peer_id() != InvalidPeerId && slotInfo.peer_id() != peerId)  {
                LOG_INFO_UNLESS(IsRecovery(), "Invalid peer id for tablet cell: %d instead of %d (Address: %s, CellId: %s)",
                    slotInfo.peer_id(),
                    peerId,
                    ~node->GetAddress(),
                    ~ToString(cellId));
                requestRemoveSlot(cellId);
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
                requestConfigureSlot(slot.Cell);
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
                requestCreateSlot(cell);
                --availableSlots;
            }
        }

        // Request to remove orphaned Hive cells.
        // Reconfigure missing and outdated ones.
        auto requestReconfigureCell = [&] (TTabletCell* cell) {
            if (!response)
                return;

            auto* reconfigureInfo = response->add_hive_cells_to_reconfigure();
            ToProto(reconfigureInfo->mutable_cell_guid(), cell->GetId());
            reconfigureInfo->mutable_config()->CopyFrom(cell->Config());
        };

        auto requestUnregisterCell = [&] (const TTabletCellId& cellId) {
            if (!response)
                return;

            auto* unregisterInfo = response->add_hive_cells_to_unregister();
            ToProto(unregisterInfo->mutable_cell_guid(), cellId);
        };

        yhash_set<TTabletCell*> missingCells;
        for (const auto& pair : TabletCellMap) {
            YCHECK(missingCells.insert(pair.second).second);
        }
            
        for (const auto& cellInfo : request.hive_cells()) {
            auto cellId = FromProto<TCellGuid>(cellInfo.cell_guid());
            if (cellId == Bootstrap->GetCellGuid())
                continue;

            auto* cell = FindTabletCell(cellId);
            if (cell) {
                YCHECK(missingCells.erase(cell) == 1);
                if (cellInfo.config_version() < cell->Config().version()) {
                    requestReconfigureCell(cell);
                }
            } else {
                requestUnregisterCell(cellId);
            }
        }

        for (auto* cell : missingCells) {
            requestReconfigureCell(cell);
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

    void OnTabletMounted(const TReqOnTabletMounted& request)
    {
        auto id = FromProto<TTabletId>(request.tablet_id());
        auto* tablet = FindTablet(id);
        if (!tablet || tablet->GetState() != ETabletState::Mounting)
            return;
        
        auto* table = tablet->GetTable();
        auto* cell = tablet->GetCell();

        LOG_INFO_UNLESS(IsRecovery(), "Tablet mounted (TableId: %s, TabletId: %s, CellId: %s)",
            ~ToString(table->GetId()),
            ~ToString(tablet->GetId()),
            ~ToString(cell->GetId()));

        tablet->SetState(ETabletState::Mounted);
    }

    void OnTabletUnmounted(const TReqOnTabletUnmounted& request)
    {
        auto id = FromProto<TTabletId>(request.tablet_id());
        auto* tablet = FindTablet(id);
        if (!tablet || tablet->GetState() != ETabletState::Unmounting)
            return;
        
        DoTabletUnmounted(tablet);
    }

    void DoTabletUnmounted(TTablet* tablet)
    {
        auto* table = tablet->GetTable();
        auto* cell = tablet->GetCell();

        LOG_INFO_UNLESS(IsRecovery(), "Tablet unmounted (TableId: %s, TabletId: %s, CellId: %s)",
            ~ToString(table->GetId()),
            ~ToString(tablet->GetId()),
            ~ToString(cell->GetId()));

        auto objectManager = Bootstrap->GetObjectManager();
        tablet->SetState(ETabletState::Unmounted);
        tablet->SetCell(nullptr);
        YCHECK(cell->Tablets().erase(tablet) == 1);
        objectManager->UnrefObject(cell);
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


    void ValidateHasHealthyCells()
    {
        auto cells = TabletCellMap.GetValues();
        for (auto* cell : cells) {
            if (cell->GetHealth() == ETabletCellHealth::Good)
                return;
        }
        THROW_ERROR_EXCEPTION("No healthy tablet cells");
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
        
        YCHECK(!cells.empty());

        std::sort(
            cells.begin(),
            cells.end(),
            [] (TTabletCell* lhs, TTabletCell* rhs) {
                return lhs->GetId() < rhs->GetId();
            });

        auto* mutationContext = Bootstrap->GetMetaStateFacade()->GetManager()->GetMutationContext();
        int index = mutationContext->RandomGenerator().Generate<size_t>() % cells.size();
        return cells[index];
    }


    typedef std::pair<TTableNode::TTabletList::iterator, TTableNode::TTabletList::iterator> TTabletListRange;

    void DoUnmountTable(
        TTableNode* table,
        const TTabletListRange& tabletRange)
    {
        for (auto it = tabletRange.first; it != tabletRange.second; ++it) {
            auto* tablet = *it;
            auto* cell = tablet->GetCell();
            if (tablet->GetState() != ETabletState::Mounted)
                continue;

            tablet->SetState(ETabletState::Unmounting);

            auto hiveManager = Bootstrap->GetHiveManager();

            {
                TReqUnmountTablet req;
                ToProto(req.mutable_tablet_id(), tablet->GetId());
                auto* mailbox = hiveManager->GetMailbox(cell->GetId());
                hiveManager->PostMessage(mailbox, req);
            }

            LOG_INFO_UNLESS(IsRecovery(), "Unmounting tablet (TableId: %s, TabletId: %s, CellId: %s)",
                ~ToString(table->GetId()),
                ~ToString(tablet->GetId()),
                ~ToString(cell->GetId()));
        }
    }


    static TTabletListRange ParseTabletRange(
        TTableNode* table,
        int first,
        int last)
    {
        auto& tablets = table->Tablets();
        if (first == -1 && last == -1) {
            return std::make_pair(tablets.begin(), tablets.end());
        } else {
            if (tablets.empty()) {
                THROW_ERROR_EXCEPTION("Table has no tablets");
            }
            if (first < 0 || first >= tablets.size()) {
                THROW_ERROR_EXCEPTION("First tablet index %d is out of range [%d, %d]",
                    first,
                    0,
                    static_cast<int>(tablets.size()) - 1);
            }
            if (last < 0 || last >= tablets.size()) {
                THROW_ERROR_EXCEPTION("Last tablet index %d is out of range [%d, %d]",
                    last,
                    0,
                    static_cast<int>(tablets.size()) - 1);
            }
            if (first > last) {
                THROW_ERROR_EXCEPTION("First tablet index is greater than last tablet index");
            }
            return std::make_pair(tablets.begin() + first, tablets.begin() + last + 1);
        }
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

TTableSchema TTabletManager::GetTableSchema(TTableNode* table)
{
    return Impl->GetTableSchema(table);
}

void TTabletManager::MountTable(
    TTableNode* table,
    int firstTabletIndex,
    int lastTabletIndex)
{
    Impl->MountTable(
        table,
        firstTabletIndex,
        lastTabletIndex);
}

void TTabletManager::UnmountTable(
    TTableNode* table,
    int firstTabletIndex,
    int lastTabletIndex)
{
    Impl->UnmountTable(
        table,
        firstTabletIndex,
        lastTabletIndex);
}

void TTabletManager::ForceUnmountTable(TTableNode* table)
{
    Impl->ForceUnmountTable(table);
}

void TTabletManager::ReshardTable(
    TTableNode* table,
    int firstTabletIndex,
    int lastTabletIndex,
    const std::vector<TOwningKey>& pivotKeys)
{
    Impl->ReshardTable(
        table,
        firstTabletIndex,
        lastTabletIndex,
        pivotKeys);
}

DELEGATE_ENTITY_MAP_ACCESSORS(TTabletManager, TabletCell, TTabletCell, TTabletCellId, *Impl)
DELEGATE_ENTITY_MAP_ACCESSORS(TTabletManager, Tablet, TTablet, TTabletId, *Impl)

///////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
