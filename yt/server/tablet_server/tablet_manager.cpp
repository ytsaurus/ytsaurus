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
#include <core/misc/string.h>

#include <ytlib/hive/cell_directory.h>

#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/chunk_meta_extensions.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/tablet_client/config.h>

#include <ytlib/chunk_client/config.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <server/object_server/type_handler_detail.h>

#include <server/tablet_server/tablet_manager.pb.h>

#include <server/node_tracker_server/node_tracker.h>
#include <server/node_tracker_server/node.h>

#include <server/table_server/table_node.h>

#include <server/tablet_node/config.h>
#include <server/tablet_node/tablet_manager.pb.h>

#include <server/hive/hive_manager.h>

#include <server/chunk_server/chunk_list.h>
#include <server/chunk_server/chunk_manager.h>
#include <server/chunk_server/chunk_tree_traversing.h>

#include <server/cypress_server/cypress_manager.h>

#include <server/security_server/security_manager.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/meta_state_facade.h>
#include <server/cell_master/serialize.h>

#include <util/random/random.h>

namespace NYT {
namespace NTabletServer {

using namespace NVersionedTableClient;
using namespace NVersionedTableClient::NProto;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NYTree;
using namespace NSecurityServer;
using namespace NTableServer;
using namespace NTabletClient;
using namespace NHydra;
using namespace NTransactionServer;
using namespace NTabletServer::NProto;
using namespace NNodeTrackerServer;
using namespace NNodeTrackerServer::NProto;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NTabletNode::NProto;
using namespace NChunkServer;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NCypressServer;
using namespace NCellMaster;

using NTabletNode::TTableMountConfigPtr;

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
            EObjectAccountMode::Forbidden);
    }

    virtual TObjectBase* Create(
        TTransaction* transaction,
        TAccount* account,
        IAttributeDictionary* attributes,
        TReqCreateObjects* request,
        TRspCreateObjects* response) override;

private:
    TImpl* Owner_;

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
    TImpl* Owner_;

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
        , Config_(config)
        , TabletTracker_(New<TTabletTracker>(Config_, Bootstrap))
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

        RegisterMethod(BIND(&TImpl::HydraAssignPeers, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraSetCellState, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraRevokePeer, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraOnTabletMounted, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraOnTabletUnmounted, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUpdateTabletStores, Unretained(this)));

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


    TTabletCell* CreateCell(int size)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto objectManager = Bootstrap->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::TabletCell);
        auto* cell = new TTabletCell(id);
        cell->SetSize(size);
        cell->Peers().resize(size);
        
        TabletCellMap_.Insert(id, cell);

        // Make the fake reference.
        YCHECK(cell->RefObject() == 1);   

        auto hiveManager = Bootstrap->GetHiveManager();
        hiveManager->CreateMailbox(id);

        auto cellMapNodeProxy = GetCellMapNode();

        auto securityManager = Bootstrap->GetSecurityManager();
        auto* sysAccount = securityManager->GetSysAccount();

        auto cypressManager = Bootstrap->GetCypressManager();
        auto nodeFactory = cypressManager->CreateNodeFactory(
            nullptr,
            sysAccount,
            false);

        auto cellNodeTypeHandler = cypressManager->GetHandler(EObjectType::TabletCellNode);

        auto* cellNode = cypressManager->CreateNode(
            cellNodeTypeHandler,
            nodeFactory,
            nullptr,
            nullptr);

        auto cellNodeProxy = cypressManager->GetNodeProxy(cellNode);
        cellMapNodeProxy->AddChild(cellNodeProxy, ToString(id));

        SyncYPathSet(cellNodeProxy, "", BuildYsonStringFluently()
            .BeginAttributes()
                .Item("opaque").Value(true)
            .EndAttributes()
            .BeginMap()
                .Item("snapshots").BeginMap()
                .EndMap()
            .EndMap());
     
        return cell;
    }

    void DestroyCell(TTabletCell* cell)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto cellMapNodeProxy = GetCellMapNode();
        cellMapNodeProxy->RemoveChild(ToString(cell->GetId()));

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
        TabletMap_.Insert(id, tablet);
        objectManager->RefObject(tablet);

        // Once the first table is created, table is no longer sorted.
        table->SetSorted(false);

        LOG_INFO_UNLESS(IsRecovery(), "Tablet created (TableId: %s, TabletId: %s)",
            ~ToString(table->GetId()),
            ~ToString(tablet->GetId()));

        return tablet;
    }

    void DestroyTablet(TTablet* tablet)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        YCHECK(!tablet->GetCell());

        LOG_INFO_UNLESS(IsRecovery(), "Tablet destroyed (TabletId: %s)",
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

        ParseTabletRange(table, &firstTabletIndex, &lastTabletIndex); // may throw
        auto schema = GetTableSchema(table); // may throw
        ValidateHasHealthyCells(); // may throw

        auto objectManager = Bootstrap->GetObjectManager();
        auto chunkManager = Bootstrap->GetChunkManager();

        const auto& tablets = table->Tablets();

        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            const auto* tablet = tablets[index];
            if (tablet->GetState() == ETabletState::Unmounting) {
                THROW_ERROR_EXCEPTION("Tablet %s is in %s state",
                    ~ToString(tablet->GetId()),
                    ~FormatEnum(tablet->GetState()).Quote());
            }
        }

        TYsonString serializedMountConfig;
        TYsonString serializedWriterOptions;
        GetTableSettings(table, &serializedMountConfig, &serializedWriterOptions);

        // When mounting a table with no tablets, create the tablet automatically.
        if (table->Tablets().empty()) {
            auto* tablet = CreateTablet(table);
            tablet->SetIndex(0);
            tablet->SetPivotKey(EmptyKey());
            table->Tablets().push_back(tablet);
            firstTabletIndex = 0;
            lastTabletIndex = 0;

            auto* oldRootChunkList = table->GetChunkList();
            auto chunks = EnumerateChunksInChunkTree(oldRootChunkList);
            auto* newRootChunkList = chunkManager->CreateChunkList();
            table->SetChunkList(newRootChunkList);
            YCHECK(newRootChunkList->OwningNodes().insert(table).second);
            objectManager->RefObject(newRootChunkList);
            YCHECK(oldRootChunkList->OwningNodes().erase(table) == 1);
            auto* tabletChunkList = chunkManager->CreateChunkList();
            chunkManager->AttachToChunkList(newRootChunkList, tabletChunkList);
            for (auto* chunk : chunks) {
                chunkManager->AttachToChunkList(tabletChunkList, chunk);
            }
            objectManager->UnrefObject(oldRootChunkList);
        }

        const auto& chunkLists = table->GetChunkList()->Children();
        YCHECK(tablets.size() == chunkLists.size());

        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto* tablet = tablets[index];
            auto* nextTablet = index + 1 == static_cast<int>(tablets.size()) ? nullptr : tablets[index + 1];
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
            ToProto(req.mutable_pivot_key(), tablet->GetPivotKey());
            ToProto(req.mutable_next_pivot_key(), nextTablet ? nextTablet->GetPivotKey() : MaxKey());
            req.set_mount_config(serializedMountConfig.Data());
            req.set_writer_options(serializedWriterOptions.Data());

            auto* chunkList = chunkLists[index]->AsChunkList();
            auto chunks = EnumerateChunksInChunkTree(chunkList);
            for (const auto* chunk : chunks) {
                auto* descriptor = req.add_chunk_stores();
                ToProto(descriptor->mutable_store_id(), chunk->GetId());
                descriptor->mutable_chunk_meta()->CopyFrom(chunk->ChunkMeta());
            }

            auto hiveManager = Bootstrap->GetHiveManager();
            auto* mailbox = hiveManager->GetMailbox(cell->GetId());
            hiveManager->PostMessage(mailbox, req);

            LOG_INFO_UNLESS(IsRecovery(), "Mounting tablet (TableId: %s, TabletId: %s, CellId: %s, ChunkCount: %d)",
                ~ToString(table->GetId()),
                ~ToString(tablet->GetId()),
                ~ToString(cell->GetId()),
                static_cast<int>(chunks.size()));
        }
    }

    void UnmountTable(
        TTableNode* table,
        bool force,
        int firstTabletIndex,
        int lastTabletIndex)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(table->IsTrunk());

        ParseTabletRange(table, &firstTabletIndex, &lastTabletIndex); // may throw

        if (!force) {
            for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
                auto* tablet = table->Tablets()[index];
                if (tablet->GetState() == ETabletState::Mounting) {
                    THROW_ERROR_EXCEPTION("Tablet %s is in %s state",
                        ~ToString(tablet->GetId()),
                        ~FormatEnum(tablet->GetState()).Quote());
                }
            }
        }

        DoUnmountTable(table, force, firstTabletIndex, lastTabletIndex);
    }

    void RemountTable(
        TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(table->IsTrunk());

        ParseTabletRange(table, &firstTabletIndex, &lastTabletIndex); // may throw

        TYsonString serializedMountConfig;
        TYsonString serializedWriterOptions;
        GetTableSettings(table, &serializedMountConfig, &serializedWriterOptions);

        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto* tablet = table->Tablets()[index];
            auto* cell = tablet->GetCell();

            if (tablet->GetState() == ETabletState::Mounted ||
                tablet->GetState() == ETabletState::Mounting)
            {
                LOG_INFO_UNLESS(IsRecovery(), "Remounting tablet (TableId: %s, TabletId: %s, CellId: %s)",
                    ~ToString(table->GetId()),
                    ~ToString(tablet->GetId()),
                    ~ToString(cell->GetId()));

                auto hiveManager = Bootstrap->GetHiveManager();

                {
                    TReqRemountTablet request;
                    request.set_mount_config(serializedMountConfig.Data());
                    request.set_writer_options(serializedWriterOptions.Data());
                    ToProto(request.mutable_tablet_id(), tablet->GetId());
                    auto* mailbox = hiveManager->GetMailbox(cell->GetId());
                    hiveManager->PostMessage(mailbox, request);
                }
            }
        }
    }

    void ClearTablets(TTableNode* table)
    {
        if (table->Tablets().empty())
            return;

        DoUnmountTable(
            table,
            true,
            0,
            static_cast<int>(table->Tablets().size()) - 1);

        auto objectManager = Bootstrap->GetObjectManager();
        for (auto* tablet : table->Tablets()) {
            YCHECK(tablet->GetState() == ETabletState::Unmounted);
            objectManager->UnrefObject(tablet);
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

        auto objectManager = Bootstrap->GetObjectManager();
        auto chunkManager = Bootstrap->GetChunkManager();

        ParseTabletRange(table, &firstTabletIndex, &lastTabletIndex); // may throw

        auto& tablets = table->Tablets();
        auto& chunkLists = table->GetChunkList()->Children();
        YCHECK(tablets.size() == chunkLists.size());

        int oldTabletCount = lastTabletIndex - firstTabletIndex + 1;
        int newTabletCount = static_cast<int>(pivotKeys.size());

        if (tablets.size() - oldTabletCount + newTabletCount > TTableNode::MaxTabletCount) {
            THROW_ERROR_EXCEPTION("Tablet count cannot exceed the limit of %d",
                TTableNode::MaxTabletCount);
        }

        if (!pivotKeys.empty()) {
            if (firstTabletIndex > lastTabletIndex) {
                if (pivotKeys[0] != EmptyKey()) {
                    THROW_ERROR_EXCEPTION("First pivot key must be empty");
                }
            } else {
                if (pivotKeys[0] != tablets[firstTabletIndex]->GetPivotKey()) {
                    THROW_ERROR_EXCEPTION(
                        "First pivot key must match that of the first tablet "
                        "in the resharded range");
                }
            }
        }

        for (int index = 0; index < static_cast<int>(pivotKeys.size()) - 1; ++index) {
            if (pivotKeys[index] >= pivotKeys[index + 1]) {
                THROW_ERROR_EXCEPTION("Pivot keys must be strictly increasing");
            }
        }

        if (lastTabletIndex != tablets.size() - 1) {
            if (pivotKeys.back() >= tablets[lastTabletIndex + 1]->GetPivotKey()) {
                THROW_ERROR_EXCEPTION(
                    "Last pivot key must be strictly less than that of the tablet "
                    "which follows the resharded range");
            }
        }

        // Validate that all tablets are unmounted.
        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto* tablet = table->Tablets()[index];
            if (tablet->GetState() != ETabletState::Unmounted) {
                THROW_ERROR_EXCEPTION("Cannot reshard table: tablet %s is in %s state",
                    ~ToString(tablet->GetId()),
                    ~FormatEnum(tablet->GetState()).Quote());
            }
        }

        // Drop old tablets.
        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto* tablet = table->Tablets()[index];
            objectManager->UnrefObject(tablet);
        }

        // Create new tablets.
        std::vector<TTablet*> newTablets;
        for (int index = 0; index < newTabletCount; ++index) {
            auto* tablet = CreateTablet(table);
            tablet->SetPivotKey(pivotKeys[index]);
            newTablets.push_back(tablet);
        }

        tablets.erase(tablets.begin() + firstTabletIndex, tablets.begin() + lastTabletIndex + 1);
        tablets.insert(tablets.begin() + firstTabletIndex, newTablets.begin(), newTablets.end());

        // Update all indexes.
        for (int index = 0; index < static_cast<int>(tablets.size()); ++index) {
            auto* tablet = tablets[index];
            tablet->SetIndex(index);
        }

        // Update chunk lists.
        auto* oldRootChunkList = table->GetChunkList();
        auto* newRootChunkList = chunkManager->CreateChunkList();
        chunkManager->AttachToChunkList(
            newRootChunkList,
            chunkLists.data(),
            chunkLists.data() + firstTabletIndex);
        for (int index = 0; index < newTabletCount; ++index) {
            auto* tabletChunkList = chunkManager->CreateChunkList();
            chunkManager->AttachToChunkList(newRootChunkList, tabletChunkList);
        }
        chunkManager->AttachToChunkList(
            newRootChunkList,
            chunkLists.data() + lastTabletIndex + 1,
            chunkLists.data() + chunkLists.size());

        // Move chunks from the resharded tablets to appropriate chunk lists.
        std::vector<TChunk*> chunks;
        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            EnumerateChunksInChunkTree(chunkLists[index]->AsChunkList(), &chunks);
        }

        for (auto* chunk : chunks) {
            auto boundaryKeysExt = GetProtoExtension<TBoundaryKeysExt>(chunk->ChunkMeta().extensions());
            auto minKey = FromProto<TOwningKey>(boundaryKeysExt.min());
            auto maxKey = FromProto<TOwningKey>(boundaryKeysExt.min());
            auto range = table->GetIntersectingTablets(minKey, maxKey);
            for (auto it = range.first; it != range.second; ++it) {
                auto* tablet = *it;
                chunkManager->AttachToChunkList(
                    newRootChunkList->Children()[tablet->GetIndex()]->AsChunkList(),
                    chunk);
            }
        }

        // Replace root chunk list.
        table->SetChunkList(newRootChunkList);
        YCHECK(newRootChunkList->OwningNodes().insert(table).second);
        objectManager->RefObject(newRootChunkList);
        YCHECK(oldRootChunkList->OwningNodes().erase(table) == 1);
        objectManager->UnrefObject(oldRootChunkList);
    }


    DECLARE_ENTITY_MAP_ACCESSORS(TabletCell, TTabletCell, TTabletCellId);
    DECLARE_ENTITY_MAP_ACCESSORS(Tablet, TTablet, TTabletId);

private:
    friend class TTabletCellTypeHandler;
    friend class TTabletTypeHandler;

    TTabletManagerConfigPtr Config_;

    TTabletTrackerPtr TabletTracker_;

    NHydra::TEntityMap<TTabletCellId, TTabletCell> TabletCellMap_;
    NHydra::TEntityMap<TTabletId, TTablet> TabletMap_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    
    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        TabletCellMap_.SaveKeys(context);
        TabletMap_.SaveKeys(context);
    }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        TabletCellMap_.SaveValues(context);
        TabletMap_.SaveValues(context);
    }


    virtual void OnBeforeSnapshotLoaded() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        DoClear();
    }

    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TabletCellMap_.LoadKeys(context);
        TabletMap_.LoadKeys(context);
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TabletCellMap_.LoadValues(context);
        TabletMap_.LoadValues(context);
    }


    void DoClear()
    {
        TabletCellMap_.Clear();
        TabletMap_.Clear();
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
        yhash_set<TTabletCell*> expectedCells;
        for (auto& slot : node->TabletSlots()) {
            if (IsObjectAlive(slot.Cell)) {
                YCHECK(expectedCells.insert(slot.Cell).second);
            }
        }

        // Figure out and analyze the reality.
        yhash_set<TTabletCell*> actualCells;
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

            if (node->IsTabletCellStartScheduled(cell)) {
                LOG_INFO_UNLESS(IsRecovery(), "Tablet slot created (Address: %s, CellId: %s)",
                    ~node->GetAddress(),
                    ~ToString(cellId));
                node->CancelTabletCellStart(cell);
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

            auto expectedIt = expectedCells.find(cell);
            if (expectedIt == expectedCells.end()) {
                cell->AttachPeer(node, peerId, slotIndex);
                LOG_INFO_UNLESS(IsRecovery(), "Tablet peer online (Address: %s, CellId: %s, PeerId: %d)",
                    ~node->GetAddress(),
                    ~ToString(cellId),
                    peerId);
            }

            cell->UpdatePeerSeenTime(peerId, mutationContext->GetTimestamp());
            YCHECK(actualCells.insert(cell).second);

            // Populate slot.
            slot.Cell = cell;
            slot.PeerState = EPeerState(slotInfo.peer_state());
            slot.PeerId = slot.Cell->GetPeerId(node); // don't trust peerInfo, it may still be InvalidPeerId

            LOG_DEBUG_UNLESS(IsRecovery(), "Tablet cell is running (Address: %s, CellId: %s, PeerId: %d, State: %s, ConfigVersion: %d)",
                ~node->GetAddress(),
                ~ToString(slot.Cell->GetId()),
                slot.PeerId,
                ~ToString(slot.PeerState),
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
        for (auto* cell : expectedCells) {
            if (actualCells.find(cell) == actualCells.end() && !node->IsTabletCellStartScheduled(cell)) {
                LOG_INFO_UNLESS(IsRecovery(), "Tablet peer offline: slot is missing (CellId: %s, Address: %s)",
                    ~ToString(cell->GetId()),
                    ~node->GetAddress());
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
            if (actualCells.find(cell) == actualCells.end()) {
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
        for (const auto& pair : TabletCellMap_) {
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


    void HydraAssignPeers(const TReqAssignPeers& request)
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

            if (node->IsTabletCellStartScheduled(cell))
                continue;

            LOG_INFO_UNLESS(IsRecovery(), "Tablet slot creation scheduled (CellId: %s, Address: %s, PeerId: %d)",
                ~ToString(cellId),
                ~node->GetAddress(),
                peerId);

            cell->AssignPeer(node, peerId);
            cell->UpdatePeerSeenTime(peerId, mutationContext->GetTimestamp());
            node->ScheduleTabletCellStart(cell);
            ReconfigureCell(cell);
        }
    }

    void HydraSetCellState(const TReqSetCellState& request)
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
            ~ToString(oldState),
            ~ToString(newState),
            ~ToString(cellId));
    }

    void HydraRevokePeer(const TReqRevokePeer& request)
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

        // Remove cell from the start queue.
        auto nodeTracker = Bootstrap->GetNodeTracker();
        auto* node = nodeTracker->FindNodeByAddress(*peer.Address);
        if (node) {
            node->CancelTabletCellStart(cell);
        }
   
        cell->RevokePeer(peerId);
        ReconfigureCell(cell);
    }

    void HydraOnTabletMounted(const TRspMountTablet& response)
    {
        auto tabletId = FromProto<TTabletId>(response.tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet))
            return;

        if (tablet->GetState() != ETabletState::Mounting) {
            LOG_INFO_UNLESS(IsRecovery(), "Mounted notification received for a tablet in %s state, ignored (TabletId: %s)",
                ~FormatEnum(tablet->GetState()).Quote(),
                ~ToString(tabletId));
            return;
        }
        
        auto* table = tablet->GetTable();
        auto* cell = tablet->GetCell();

        LOG_INFO_UNLESS(IsRecovery(), "Tablet mounted (TableId: %s, TabletId: %s, CellId: %s)",
            ~ToString(table->GetId()),
            ~ToString(tablet->GetId()),
            ~ToString(cell->GetId()));

        tablet->SetState(ETabletState::Mounted);
    }

    void HydraOnTabletUnmounted(const TRspUnmountTablet& response)
    {
        auto tabletId = FromProto<TTabletId>(response.tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet))
            return;
        
        if (tablet->GetState() != ETabletState::Unmounting) {
            LOG_INFO_UNLESS(IsRecovery(), "Unmounted notification received for a tablet in %s state, ignored (TabletId: %s)",
                ~FormatEnum(tablet->GetState()).Quote(),
                ~ToString(tabletId));
            return;
        }

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

    void HydraUpdateTabletStores(const TReqUpdateTabletStores& request)
    {
        auto tabletId = FromProto<TTabletId>(request.tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet))
            return;

        // NB: Stores may be updated while unmounting to facilitate flush.
        if (tablet->GetState() != ETabletState::Mounted &&
            tablet->GetState() != ETabletState::Unmounting)
        {
            LOG_INFO_UNLESS(IsRecovery(), "Requested to update stoares for a tablet in %s state, ignored (TabletId: %s)",
                ~FormatEnum(tablet->GetState()).Quote(),
                ~ToString(tabletId));
            return;
        }

        auto* cell = tablet->GetCell();
        auto* table = tablet->GetTable();

        TRspUpdateTabletStores response;
        response.mutable_tablet_id()->MergeFrom(request.tablet_id());
        response.mutable_stores_to_add()->MergeFrom(request.stores_to_add());
        response.mutable_stores_to_remove()->MergeFrom(request.stores_to_remove());

        try {
            auto chunkManager = Bootstrap->GetChunkManager();
            auto securityManager = Bootstrap->GetSecurityManager();

            // Collect all changes first.
            std::vector<TChunkTree*> chunksToAttach;
            i64 attachedRowCount = 0;
            for (const auto& descriptor : request.stores_to_add()) {
                auto storeId = FromProto<TStoreId>(descriptor.store_id());
                if (TypeFromId(storeId) == EObjectType::Chunk ||
                    TypeFromId(storeId) == EObjectType::ErasureChunk)
                {
                    auto* chunk = chunkManager->GetChunkOrThrow(storeId);
                    auto miscExt = GetProtoExtension<TMiscExt>(chunk->ChunkMeta().extensions());
                    attachedRowCount += miscExt.row_count();
                    chunksToAttach.push_back(chunk);
                }
            }

            std::vector<TChunkTree*> chunksToDetach;
            i64 detachedRowCount = 0;
            for (const auto& descriptor : request.stores_to_remove()) {
                auto storeId = FromProto<TStoreId>(descriptor.store_id());
                if (TypeFromId(storeId) == EObjectType::Chunk ||
                    TypeFromId(storeId) == EObjectType::ErasureChunk)
                {
                    auto* chunk = chunkManager->GetChunkOrThrow(storeId);
                    auto miscExt = GetProtoExtension<TMiscExt>(chunk->ChunkMeta().extensions());
                    detachedRowCount += miscExt.row_count();
                    chunksToDetach.push_back(chunk);
                }
            }

            // Apply all requested changes.
            auto* chunkList = table->GetChunkList()->Children()[tablet->GetIndex()]->AsChunkList();
            chunkManager->AttachToChunkList(chunkList, chunksToAttach);
            chunkManager->DetachFromChunkList(chunkList, chunksToDetach);

            // Unstage just attached chunks.
            // Update table resource usage.
            for (auto* chunk : chunksToAttach) {
                chunkManager->UnstageChunk(chunk->AsChunk());
            }
            securityManager->UpdateAccountNodeUsage(table);

            LOG_INFO_UNLESS(IsRecovery(), "Tablet stores updated (TabletId: %s, AttachedChunkIds: [%s], DetachedChunkIds: [%s], "
                "AttachedRowCount: %" PRId64 ", DetachedRowCount: %" PRId64 ")",
                ~ToString(tabletId),
                ~JoinToString(ToObjectIds(chunksToAttach)),
                ~JoinToString(ToObjectIds(chunksToDetach)),
                attachedRowCount,
                detachedRowCount);
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            LOG_WARNING(error, "Error updating tablet stores (TabletId: %s)",
                ~ToString(tabletId));
            ToProto(response.mutable_error(), error);
        }

        auto hiveManager = Bootstrap->GetHiveManager();
        auto* mailbox = hiveManager->GetMailbox(cell->GetId());
        hiveManager->PostMessage(mailbox, response);
    }


    void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TabletTracker_->Start();

        auto cellDirectory = Bootstrap->GetCellDirectory();
        cellDirectory->Clear();

        for (const auto& pair : TabletCellMap_) {
            auto* cell = pair.second;
            UpdateCellDirectory(cell);
        }
    }

    void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TabletTracker_->Stop();
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

        UpdateCellDirectory(cell);

        LOG_INFO_UNLESS(IsRecovery(), "Tablet cell reconfigured (CellId: %s, Version: %d)",
            ~ToString(cell->GetId()),
            config.version());
    }

    void UpdateCellDirectory(TTabletCell* cell)
    {
        auto cellDirectory = Bootstrap->GetCellDirectory();
        cellDirectory->RegisterCell(cell->GetId(), cell->Config());
    }


    void ValidateHasHealthyCells()
    {
        auto cells = TabletCellMap_.GetValues();
        for (auto* cell : cells) {
            if (cell->GetHealth() == ETabletCellHealth::Good)
                return;
        }
        THROW_ERROR_EXCEPTION("No healthy tablet cells");
    }

    TTabletCell* AllocateCell()
    {
        // TODO(babenko): do something smarter?
        auto cells = TabletCellMap_.GetValues();

        cells.erase(
            std::remove_if(
                cells.begin(),
                cells.end(),
                [] (const TTabletCell* cell) {
                    return cell->IsAlive() && cell->GetHealth() != ETabletCellHealth::Good;
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


    void DoUnmountTable(
        TTableNode* table,
        bool force,
        int firstTabletIndex,
        int lastTabletIndex)
    {
        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto* tablet = table->Tablets()[index];
            auto* cell = tablet->GetCell();

            if (tablet->GetState() == ETabletState::Mounted) {
                LOG_INFO_UNLESS(IsRecovery(), "Unmounting tablet (TableId: %s, TabletId: %s, CellId: %s)",
                    ~ToString(table->GetId()),
                    ~ToString(tablet->GetId()),
                    ~ToString(cell->GetId()));

                tablet->SetState(ETabletState::Unmounting);

                auto hiveManager = Bootstrap->GetHiveManager();

                {
                    TReqUnmountTablet request;
                    ToProto(request.mutable_tablet_id(), tablet->GetId());
                    request.set_force(force);
                    auto* mailbox = hiveManager->GetMailbox(cell->GetId());
                    hiveManager->PostMessage(mailbox, request);
                }
            }

            if (force && tablet->GetState() != ETabletState::Unmounted) {
                DoTabletUnmounted(tablet);
            }
        }
    }


    void GetTableSettings(
        TTableNode* table,
        TYsonString* serializedMountConfig,
        TYsonString* serializedWriterOptions)
    {
        auto objectManager = Bootstrap->GetObjectManager();
        auto tableProxy = objectManager->GetProxy(table);
        const auto& tableAttributes = tableProxy->Attributes();

        // Parse and prepare mount config.
        TTableMountConfigPtr mountConfig;
        try {
            mountConfig = ConvertTo<TTableMountConfigPtr>(tableAttributes);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing table mount configuration")
                << ex;
        }

        *serializedMountConfig = ConvertToYsonString(mountConfig);

        // Prepare tablet writer options.
        auto writerOptions = New<NTabletNode::TTabletWriterOptions>();
        writerOptions->ReplicationFactor = table->GetReplicationFactor();
        writerOptions->Account = table->GetAccount()->GetName();
        writerOptions->CompressionCodec = tableAttributes.Get<NCompression::ECodec>("compression_codec");
        writerOptions->ErasureCodec = tableAttributes.Get<NErasure::ECodec>("erasure_codec", NErasure::ECodec::None);
        *serializedWriterOptions = ConvertToYsonString(writerOptions);
    }

    static void ParseTabletRange(
        TTableNode* table,
        int* first,
        int* last)
    {
        auto& tablets = table->Tablets();
        if (*first == -1 && *last == -1) {
            *first = 0;
            *last = static_cast<int>(tablets.size() - 1);
        } else {
            if (tablets.empty()) {
                THROW_ERROR_EXCEPTION("Table has no tablets");
            }
            if (*first < 0 || *first >= tablets.size()) {
                THROW_ERROR_EXCEPTION("First tablet index %d is out of range [%d, %d]",
                    *first,
                    0,
                    static_cast<int>(tablets.size()) - 1);
            }
            if (*last < 0 || *last >= tablets.size()) {
                THROW_ERROR_EXCEPTION("Last tablet index %d is out of range [%d, %d]",
                    *last,
                    0,
                    static_cast<int>(tablets.size()) - 1);
            }
            if (*first > *last) {
                THROW_ERROR_EXCEPTION("First tablet index is greater than last tablet index");
            }
        }
    }


    IMapNodePtr GetCellMapNode()
    {
        auto cypressManager = Bootstrap->GetCypressManager();
        auto resolver = cypressManager->CreateResolver();
        return resolver->ResolvePath("//sys/tablet_cells")->AsMap();
    }

};

DEFINE_ENTITY_MAP_ACCESSORS(TTabletManager::TImpl, TabletCell, TTabletCell, TTabletCellId, TabletCellMap_)
DEFINE_ENTITY_MAP_ACCESSORS(TTabletManager::TImpl, Tablet, TTablet, TTabletId, TabletMap_)

///////////////////////////////////////////////////////////////////////////////

TTabletManager::TTabletCellTypeHandler::TTabletCellTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap, &owner->TabletCellMap_)
    , Owner_(owner)
{ }

TObjectBase* TTabletManager::TTabletCellTypeHandler::Create(
    TTransaction* transaction,
    TAccount* account,
    IAttributeDictionary* attributes,
    TReqCreateObjects* request,
    TRspCreateObjects* response)
{
    int size = attributes->Get<int>("size");
    attributes->Remove("size");

    if (size < 1 || size > 9) {
        THROW_ERROR_EXCEPTION("\"size\" must be in range [1,9]");
    }

    if (size %2 == 0) {
        THROW_ERROR_EXCEPTION("\"size\" must be odd");
    }

    auto* cell = Owner_->CreateCell(size);
    return cell;
}

void TTabletManager::TTabletCellTypeHandler::DoDestroy(TTabletCell* cell)
{
    Owner_->DestroyCell(cell);
}

///////////////////////////////////////////////////////////////////////////////

TTabletManager::TTabletTypeHandler::TTabletTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap, &owner->TabletMap_)
    , Owner_(owner)
{ }

void TTabletManager::TTabletTypeHandler::DoDestroy(TTablet* tablet)
{
    Owner_->DestroyTablet(tablet);
}

///////////////////////////////////////////////////////////////////////////////

TTabletManager::TTabletManager(
    TTabletManagerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

TTabletManager::~TTabletManager()
{ }

void TTabletManager::Initialize()
{
    return Impl_->Initialize();
}

TTableSchema TTabletManager::GetTableSchema(TTableNode* table)
{
    return Impl_->GetTableSchema(table);
}

void TTabletManager::MountTable(
    TTableNode* table,
    int firstTabletIndex,
    int lastTabletIndex)
{
    Impl_->MountTable(
        table,
        firstTabletIndex,
        lastTabletIndex);
}

void TTabletManager::UnmountTable(
    TTableNode* table,
    bool force,
    int firstTabletIndex,
    int lastTabletIndex)
{
    Impl_->UnmountTable(
        table,
        force,
        firstTabletIndex,
        lastTabletIndex);
}

void TTabletManager::RemountTable(
    TTableNode* table,
    int firstTabletIndex,
    int lastTabletIndex)
{
    Impl_->RemountTable(
        table,
        firstTabletIndex,
        lastTabletIndex);
}

void TTabletManager::ClearTablets(TTableNode* table)
{
    Impl_->ClearTablets(table);
}

void TTabletManager::ReshardTable(
    TTableNode* table,
    int firstTabletIndex,
    int lastTabletIndex,
    const std::vector<TOwningKey>& pivotKeys)
{
    Impl_->ReshardTable(
        table,
        firstTabletIndex,
        lastTabletIndex,
        pivotKeys);
}

DELEGATE_ENTITY_MAP_ACCESSORS(TTabletManager, TabletCell, TTabletCell, TTabletCellId, *Impl_)
DELEGATE_ENTITY_MAP_ACCESSORS(TTabletManager, Tablet, TTablet, TTabletId, *Impl_)

///////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
