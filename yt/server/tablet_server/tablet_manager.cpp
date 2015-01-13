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
#include <core/misc/collection_helpers.h>

#include <core/concurrency/periodic_executor.h>

#include <ytlib/election/config.h>

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

#include <server/object_server/object_manager.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/hydra_facade.h>
#include <server/cell_master/serialize.h>

#include <util/random/random.h>

namespace NYT {
namespace NTabletServer {

using namespace NConcurrency;
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
using NNodeTrackerServer::NProto::TReqIncrementalHeartbeat;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletServerLogger;
static const auto CleanupPeriod = TDuration::Seconds(10);

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
        return Format("tablet cell %v", object->GetId());
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
        return Format("tablet %v", object->GetId());
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
        , TabletTracker_(New<TTabletTracker>(Config_, Bootstrap_))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(), AutomatonThread);

        RegisterLoader(
            "TabletManager.Keys",
            BIND(&TImpl::LoadKeys, Unretained(this)));
        RegisterLoader(
            "TabletManager.Values",
            BIND(&TImpl::LoadValues, Unretained(this)));

        RegisterSaver(
            ESerializationPriority::Keys,
            "TabletManager.Keys",
            BIND(&TImpl::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESerializationPriority::Values,
            "TabletManager.Values",
            BIND(&TImpl::SaveValues, Unretained(this)));

        RegisterMethod(BIND(&TImpl::HydraAssignPeers, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraRevokePeer, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraOnTabletMounted, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraOnTabletUnmounted, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUpdateTabletStores, Unretained(this)));

        auto nodeTracker = Bootstrap_->GetNodeTracker();
        nodeTracker->SubscribeNodeRegistered(BIND(&TImpl::OnNodeRegistered, MakeWeak(this)));
        nodeTracker->SubscribeNodeUnregistered(BIND(&TImpl::OnNodeUnregistered, MakeWeak(this)));
        nodeTracker->SubscribeIncrementalHeartbeat(BIND(&TImpl::OnIncrementalHeartbeat, MakeWeak(this)));
    }

    void Initialize()
    {
        auto objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(New<TTabletCellTypeHandler>(this));
        objectManager->RegisterHandler(New<TTabletTypeHandler>(this));

        auto transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->SubscribeTransactionCommitted(BIND(&TImpl::OnTransactionFinished, MakeWeak(this)));
        transactionManager->SubscribeTransactionAborted(BIND(&TImpl::OnTransactionFinished, MakeWeak(this)));
    }


    TTabletCell* CreateCell(int size, IAttributeDictionary* attributes)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::TabletCell);
        auto cell_ = std::make_unique<TTabletCell>(id);
        auto* cell = cell_.get();

        cell->SetSize(size);
        cell->SetOptions(ConvertTo<TTabletCellOptionsPtr>(attributes)); // may throw
        cell->Peers().resize(size);
        
        TabletCellMap_.Insert(id, cell_.release());

        // Make the fake reference.
        YCHECK(cell->RefObject() == 1);   

        auto hiveManager = Bootstrap_->GetHiveManager();
        hiveManager->CreateMailbox(id);

        auto cellMapNodeProxy = GetCellMapNode();

        auto securityManager = Bootstrap_->GetSecurityManager();
        auto* sysAccount = securityManager->GetSysAccount();

        auto cypressManager = Bootstrap_->GetCypressManager();
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
                .Item("changelogs").BeginMap()
                .EndMap()
            .EndMap());
     
        return cell;
    }

    void DestroyCell(TTabletCell* cell)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto cellMapNodeProxy = GetCellMapNode();
        cellMapNodeProxy->RemoveChild(ToString(cell->GetId()));

        auto hiveManager = Bootstrap_->GetHiveManager();
        hiveManager->RemoveMailbox(cell->GetId());

        for (const auto& peer : cell->Peers()) {
            if (peer.Node) {
                peer.Node->DetachTabletCell(cell);
            }
            if (peer.Address) {
                RemoveFromAddressToCellMap(*peer.Address, cell);
            }
        }

        AbortPrerequisiteTransaction(cell);
    }


    TTablet* CreateTablet(TTableNode* table)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::Tablet);
        auto* tablet = new TTablet(id);
        tablet->SetTable(table);
        TabletMap_.Insert(id, tablet);
        objectManager->RefObject(tablet);

        // Once the first table is created, table is no longer sorted.
        table->SetSorted(false);

        LOG_INFO_UNLESS(IsRecovery(), "Tablet created (TableId: %v, TabletId: %v)",
            table->GetId(),
            tablet->GetId());

        return tablet;
    }

    void DestroyTablet(TTablet* tablet)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        YCHECK(!tablet->GetCell());

        LOG_INFO_UNLESS(IsRecovery(), "Tablet destroyed (TabletId: %v)",
            tablet->GetId());
    }


    int GetAssignedTabletCellCount(const Stroka& address) const
    {
        auto range = AddressToCell_.equal_range(address);
        return std::distance(range.first, range.second);
    }

    TTableSchema GetTableSchema(TTableNode* table)
    {
        auto objectManager = Bootstrap_->GetObjectManager();
        auto tableProxy = objectManager->GetProxy(table);
        // COMPAT(babenko): schema must be mandatory
        return tableProxy->Attributes().Get<TTableSchema>("schema", TTableSchema());
    }

    TTabletStatistics GetTabletStatistics(const TTablet* tablet)
    {
        const auto* table = tablet->GetTable();
        const auto* rootChunkList = table->GetChunkList();
        const auto* tabletChunkList = rootChunkList->Children()[tablet->GetIndex()]->AsChunkList();
        const auto& treeStatistics = tabletChunkList->Statistics();
        TTabletStatistics tabletStatistics;
        if (tablet->GetState() == ETabletState::Mounted) {
            const auto& nodeStatistics = tablet->NodeStatistics();
            tabletStatistics.PartitionCount = nodeStatistics.partition_count();
            tabletStatistics.StoreCount = nodeStatistics.store_count();
        }
        tabletStatistics.UnmergedRowCount = treeStatistics.RowCount;
        tabletStatistics.UncompressedDataSize = treeStatistics.UncompressedDataSize;
        tabletStatistics.CompressedDataSize = treeStatistics.CompressedDataSize;
        tabletStatistics.DiskSpace =
            treeStatistics.RegularDiskSpace * table->GetReplicationFactor() +
            treeStatistics.ErasureDiskSpace;
        tabletStatistics.ChunkCount = treeStatistics.ChunkCount;
        return tabletStatistics;
    }


    void MountTable(
        TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex,
        TTabletCellId cellId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(table->IsTrunk());
        
        ParseTabletRange(table, &firstTabletIndex, &lastTabletIndex); // may throw
        auto schema = GetTableSchema(table); // may throw
        ValidateTableSchemaAndKeyColumns(schema, table->KeyColumns()); // may throw

        TTabletCell* hintedCell;
        if (cellId == NullTabletCellId) {
            ValidateHasHealthyCells(); // may throw
            hintedCell = nullptr;
        } else {
            hintedCell = GetTabletCellOrThrow(cellId); // may throw
        }

        auto objectManager = Bootstrap_->GetObjectManager();
        auto chunkManager = Bootstrap_->GetChunkManager();

        const auto& tablets = table->Tablets();

        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            const auto* tablet = tablets[index];
            if (tablet->GetState() == ETabletState::Unmounting) {
                THROW_ERROR_EXCEPTION("Tablet %v is in %Qlv state",
                    tablet->GetId(),
                    tablet->GetState());
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

        auto cells = AllocateCells(hintedCell, lastTabletIndex - firstTabletIndex + 1);

        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto* tablet = tablets[index];
            auto* nextTablet = index + 1 == static_cast<int>(tablets.size()) ? nullptr : tablets[index + 1];
            if (tablet->GetCell())
                continue;

            auto* cell = cells[index - firstTabletIndex];
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

            auto hiveManager = Bootstrap_->GetHiveManager();
            auto* mailbox = hiveManager->GetMailbox(cell->GetId());
            hiveManager->PostMessage(mailbox, req);

            LOG_INFO_UNLESS(IsRecovery(), "Mounting tablet (TableId: %v, TabletId: %v, CellId: %v, ChunkCount: %v)",
                table->GetId(),
                tablet->GetId(),
                cell->GetId(),
                chunks.size());
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
                    THROW_ERROR_EXCEPTION("Tablet %v is in %Qlv state",
                        tablet->GetId(),
                        tablet->GetState());
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
                LOG_INFO_UNLESS(IsRecovery(), "Remounting tablet (TableId: %v, TabletId: %v, CellId: %v)",
                    table->GetId(),
                    tablet->GetId(),
                    cell->GetId());

                auto hiveManager = Bootstrap_->GetHiveManager();

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

        auto objectManager = Bootstrap_->GetObjectManager();
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

        auto objectManager = Bootstrap_->GetObjectManager();
        auto chunkManager = Bootstrap_->GetChunkManager();

        ParseTabletRange(table, &firstTabletIndex, &lastTabletIndex); // may throw

        auto& tablets = table->Tablets();
        auto& chunkLists = table->GetChunkList()->Children();
        YCHECK(tablets.size() == chunkLists.size());

        int oldTabletCount = lastTabletIndex - firstTabletIndex + 1;
        int newTabletCount = static_cast<int>(pivotKeys.size());

        if (tablets.size() - oldTabletCount + newTabletCount > MaxTabletCount) {
            THROW_ERROR_EXCEPTION("Tablet count cannot exceed the limit of %v",
                MaxTabletCount);
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
                THROW_ERROR_EXCEPTION("Cannot reshard table: tablet %v is in %Qlv state",
                    tablet->GetId(),
                    tablet->GetState());
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

        // NB: Evaluation order is important here, consider the case lastTabletIndex == -1.
        tablets.erase(tablets.begin() + firstTabletIndex, tablets.begin() + (lastTabletIndex + 1));
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

    TTabletCell* GetTabletCellOrThrow(const TTabletCellId& id)
    {
        auto* cell = FindTabletCell(id);
        if (!IsObjectAlive(cell)) {
            THROW_ERROR_EXCEPTION("No such tablet cell %v", id);
        }
        return cell;
    }

    DECLARE_ENTITY_MAP_ACCESSORS(TabletCell, TTabletCell, TTabletCellId);
    DECLARE_ENTITY_MAP_ACCESSORS(Tablet, TTablet, TTabletId);

private:
    friend class TTabletCellTypeHandler;
    friend class TTabletTypeHandler;

    TTabletManagerConfigPtr Config_;

    TTabletTrackerPtr TabletTracker_;

    TEntityMap<TTabletCellId, TTabletCell> TabletCellMap_;
    TEntityMap<TTabletId, TTablet> TabletMap_;

    yhash_multimap<Stroka, TTabletCell*> AddressToCell_;
    yhash_map<TTransaction*, TTabletCell*> TransactionToCellMap_;

    TPeriodicExecutorPtr CleanupExecutor_;

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

    virtual void OnAfterSnapshotLoaded() override
    {
        AddressToCell_.clear();

        for (const auto& pair : TabletCellMap_) {
            auto* cell = pair.second;
            for (const auto& peer : cell->Peers()) {
                if (peer.Address) {
                    AddToAddressToCellMap(*peer.Address, cell);
                }
            }
            auto* transaction = cell->GetPrerequisiteTransaction();
            if (transaction) {
                YCHECK(TransactionToCellMap_.insert(std::make_pair(transaction, cell)).second);
            }
        }
    }


    void DoClear()
    {
        TabletCellMap_.Clear();
        TabletMap_.Clear();
        AddressToCell_.clear();
        TransactionToCellMap_.clear();
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
                LOG_INFO_UNLESS(IsRecovery(), "Tablet cell peer offline: node unregistered (Address: %v, CellId: %v, PeerId: %v)",
                    node->GetAddress(),
                    cell->GetId(),
                    slot.PeerId);
                cell->DetachPeer(node);
            }
        }
    }

    void OnIncrementalHeartbeat(
        TNode* node,
        const TReqIncrementalHeartbeat& request,
        TRspIncrementalHeartbeat* response)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        // Various request helpers.
        auto requestCreateSlot = [&] (TTabletCell* cell) {
            if (!response)
                return;

            auto* protoInfo = response->add_tablet_slots_to_create();

            const auto& cellId = cell->GetId();
            ToProto(protoInfo->mutable_cell_id(), cell->GetId());
            protoInfo->set_options(ConvertToYsonString(cell->GetOptions()).Data());
            ToProto(protoInfo->mutable_prerequisite_transaction_id(), cell->GetPrerequisiteTransaction()->GetId());

            LOG_INFO_UNLESS(IsRecovery(), "Tablet slot creation requested (Address: %v, CellId: %v, PrerequisiteTransactionId: %v)",
                node->GetAddress(),
                cellId,
                cell->GetPrerequisiteTransaction()->GetId());
        };

        auto requestConfigureSlot = [&] (TTabletCell* cell) {
            if (!response)
                return;

            auto* protoInfo = response->add_tablet_slots_configure();

            const auto& cellId = cell->GetId();
            ToProto(protoInfo->mutable_cell_id(), cell->GetId());
            protoInfo->set_config_version(cell->GetConfigVersion());
            protoInfo->set_config(ConvertToYsonString(cell->GetConfig()).Data());
            protoInfo->set_peer_id(cell->GetPeerId(node));
            
            LOG_INFO_UNLESS(IsRecovery(), "Tablet slot configuration update requested (Address: %v, CellId: %v, Version: %v)",
                node->GetAddress(),
                cellId,
                cell->GetConfigVersion());
        };

        auto requestRemoveSlot = [&] (const TTabletCellId& cellId) {
            if (!response)
                return;

            auto* protoInfo = response->add_tablet_slots_to_remove();
            ToProto(protoInfo->mutable_cell_id(), cellId);

            LOG_INFO_UNLESS(IsRecovery(), "Tablet slot removal requested (Address: %v, CellId: %v)",
                node->GetAddress(),
                cellId);
        };

        auto hydraFacade = Bootstrap_->GetHydraFacade();
        auto hydraManager = hydraFacade->GetHydraManager();
        auto* mutationContext = hydraManager->GetMutationContext();
        const auto& address = node->GetAddress();

        // Our expectations.
        yhash_set<TTabletCell*> expectedCells;
        for (const auto& slot : node->TabletSlots()) {
            auto* cell = slot.Cell;
            if (IsObjectAlive(cell)) {
                YCHECK(expectedCells.insert(cell).second);
            }
        }

        // Figure out and analyze the reality.
        yhash_set<TTabletCell*> actualCells;
        for (int slotIndex = 0; slotIndex < request.tablet_slots_size(); ++slotIndex) {
            // Pre-erase slot.
            auto& slot = node->TabletSlots()[slotIndex];
            slot = TNode::TTabletSlot();

            const auto& slotInfo = request.tablet_slots(slotIndex);

            auto state = EPeerState(slotInfo.peer_state());
            if (state == EPeerState::None)
                continue;

            auto cellId = FromProto<TTabletCellId>(slotInfo.cell_id());
            auto* cell = FindTabletCell(cellId);
            if (!IsObjectAlive(cell)) {
                LOG_INFO_UNLESS(IsRecovery(), "Unknown tablet slot is running (Address: %v, CellId: %v)",
                    address,
                    cellId);
                requestRemoveSlot(cellId);
                continue;
            }

            auto peerId = cell->FindPeerId(address);
            if (peerId == InvalidPeerId) {
                LOG_INFO_UNLESS(IsRecovery(), "Unexpected tablet cell is running (Address: %v, CellId: %v)",
                    address,
                    cellId);
                requestRemoveSlot(cellId);
                continue;
            }

            if (slotInfo.peer_id() != InvalidPeerId && slotInfo.peer_id() != peerId)  {
                LOG_INFO_UNLESS(IsRecovery(), "Invalid peer id for tablet cell: %v instead of %v (Address: %v, CellId: %v)",
                    slotInfo.peer_id(),
                    peerId,
                    address,
                    cellId);
                requestRemoveSlot(cellId);
                continue;
            }

            auto prerequisiteTransactionId = FromProto<TTransactionId>(slotInfo.prerequisite_transaction_id());
            if (cell->GetPrerequisiteTransaction() && prerequisiteTransactionId != cell->GetPrerequisiteTransaction()->GetId())  {
                LOG_INFO_UNLESS(IsRecovery(), "Invalid prerequisite transaction id for tablet cell: %v instead of %v (Address: %v, CellId: %v)",
                    prerequisiteTransactionId,
                    cell->GetPrerequisiteTransaction()->GetId(),
                    address,
                    cellId);
                requestRemoveSlot(cellId);
                continue;
            }

            auto expectedIt = expectedCells.find(cell);
            if (expectedIt == expectedCells.end()) {
                cell->AttachPeer(node, peerId);
                LOG_INFO_UNLESS(IsRecovery(), "Tablet cell peer online (Address: %v, CellId: %v, PeerId: %v)",
                    address,
                    cellId,
                    peerId);
            }

            cell->UpdatePeerSeenTime(peerId, mutationContext->GetTimestamp());
            YCHECK(actualCells.insert(cell).second);

            // Populate slot.
            slot.Cell = cell;
            slot.PeerState = state;
            slot.PeerId = slot.Cell->GetPeerId(node); // don't trust peerInfo, it may still be InvalidPeerId

            LOG_DEBUG_UNLESS(IsRecovery(), "Tablet cell is running (Address: %v, CellId: %v, PeerId: %v, State: %v, ConfigVersion: %v)",
                address,
                slot.Cell->GetId(),
                slot.PeerId,
                slot.PeerState,
                slotInfo.config_version());

            // Request slot reconfiguration if states are appropriate and versions differ.
            if (slotInfo.config_version() != slot.Cell->GetConfigVersion()) {
                requestConfigureSlot(slot.Cell);
            }
        }

        // Check for expected slots that are missing.
        for (auto* cell : expectedCells) {
            if (actualCells.find(cell) == actualCells.end()) {
                LOG_INFO_UNLESS(IsRecovery(), "Tablet cell peer offline: slot is missing (CellId: %v, Address: %v)",
                    cell->GetId(),
                    address);
                cell->DetachPeer(node);
            }
        }

        // Request slot starts.
        {
            int availableSlots = node->Statistics().available_tablet_slots();
            auto range = AddressToCell_.equal_range(address);
            for (auto it = range.first; it != range.second; ++it) {
                auto* cell = it->second;
                if (IsObjectAlive(cell) && actualCells.find(cell) == actualCells.end()) {
                    requestCreateSlot(cell);
                    --availableSlots;
                }
            }
        }

        // Copy tablet statistics.
        for (auto& tabletInfo : request.tablets()) {
            auto tabletId = FromProto<TTabletId>(tabletInfo.tablet_id());
            auto* tablet = FindTablet(tabletId);
            if (!tablet || tablet->GetState() != ETabletState::Mounted)
                continue;
            tablet->NodeStatistics() = tabletInfo.statistics();
        }

        // Request to remove orphaned Hive cells.
        // Reconfigure missing and outdated ones.
        auto requestReconfigureCell = [&] (TTabletCell* cell) {
            if (!response)
                return;

            auto* protoInfo = response->add_hive_cells_to_reconfigure();
            auto config = cell->GetConfig()->ToElection(cell->GetId());
            protoInfo->set_config_version(cell->GetConfigVersion());
            protoInfo->set_config(ConvertToYsonString(config).Data());
        };

        auto requestUnregisterCell = [&] (const TTabletCellId& cellId) {
            if (!response)
                return;

            auto* unregisterInfo = response->add_hive_cells_to_unregister();
            ToProto(unregisterInfo->mutable_cell_id(), cellId);
        };

        yhash_set<TTabletCell*> missingCells;
        for (const auto& pair : TabletCellMap_) {
            auto* cell = pair.second;
            if (IsObjectAlive(cell)) {
               YCHECK(missingCells.insert(pair.second).second);
            }
        }
            
        for (const auto& cellInfo : request.hive_cells()) {
            auto cellId = FromProto<TCellId>(cellInfo.cell_id());
            if (cellId == Bootstrap_->GetCellId())
                continue;

            auto* cell = FindTabletCell(cellId);
            if (IsObjectAlive(cell)) {
                YCHECK(missingCells.erase(cell) == 1);
                if (cellInfo.config_version() < cell->GetConfigVersion()) {
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


    void AddToAddressToCellMap(const Stroka& address, TTabletCell* cell)
    {
        AddressToCell_.insert(std::make_pair(address, cell));
    }

    void RemoveFromAddressToCellMap(const Stroka& address, TTabletCell* cell)
    {
        auto range = AddressToCell_.equal_range(address);
        for (auto it = range.first; it != range.second; ++it) {
            if (it->second == cell) {
                AddressToCell_.erase(it);
                break;
            }
        }  
    }


    void HydraAssignPeers(const TReqAssignPeers& request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto cellId = FromProto<TTabletCellId>(request.cell_id());
        auto* cell = FindTabletCell(cellId);
        if (!IsObjectAlive(cell))
            return;

        auto hydraFacade = Bootstrap_->GetHydraFacade();
        auto hydraManager = hydraFacade->GetHydraManager();
        auto* mutationContext = hydraManager->GetMutationContext();
        auto nodeTracker = Bootstrap_->GetNodeTracker();

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

            const auto& address = node->GetAddress();
            AddToAddressToCellMap(address, cell);
            cell->AssignPeer(address, peerId);
            cell->UpdatePeerSeenTime(peerId, mutationContext->GetTimestamp());

            LOG_INFO_UNLESS(IsRecovery(), "Tablet cell peer assigned (CellId: %v, Address: %v, PeerId: %v)",
                cellId,
                address,
                peerId);
        }

        StartPrerequisiteTransaction(cell);
        ReconfigureCell(cell);
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

        const auto& address = *peer.Address;

        LOG_INFO_UNLESS(IsRecovery(), "Tablet cell peer revoked (CellId: %v, Address: %v, PeerId: %v)",
            cell->GetId(),
            address,
            peerId);

        RemoveFromAddressToCellMap(address, cell);
        cell->RevokePeer(peerId);
        
        AbortPrerequisiteTransaction(cell);
    }

    void HydraOnTabletMounted(const TRspMountTablet& response)
    {
        auto tabletId = FromProto<TTabletId>(response.tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet))
            return;

        if (tablet->GetState() != ETabletState::Mounting) {
            LOG_INFO_UNLESS(IsRecovery(), "Mounted notification received for a tablet in %Qlv state, ignored (TabletId: %v)",
                tablet->GetState(),
                tabletId);
            return;
        }
        
        auto* table = tablet->GetTable();
        auto* cell = tablet->GetCell();

        LOG_INFO_UNLESS(IsRecovery(), "Tablet mounted (TableId: %v, TabletId: %v, CellId: %v)",
            table->GetId(),
            tablet->GetId(),
            cell->GetId());

        tablet->SetState(ETabletState::Mounted);
    }

    void HydraOnTabletUnmounted(const TRspUnmountTablet& response)
    {
        auto tabletId = FromProto<TTabletId>(response.tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet))
            return;
        
        if (tablet->GetState() != ETabletState::Unmounting) {
            LOG_INFO_UNLESS(IsRecovery(), "Unmounted notification received for a tablet in %Qlv state, ignored (TabletId: %v)",
                tablet->GetState(),
                tabletId);
            return;
        }

        DoTabletUnmounted(tablet);
    }

    void DoTabletUnmounted(TTablet* tablet)
    {
        auto* table = tablet->GetTable();
        auto* cell = tablet->GetCell();

        LOG_INFO_UNLESS(IsRecovery(), "Tablet unmounted (TableId: %v, TabletId: %v, CellId: %v)",
            table->GetId(),
            tablet->GetId(),
            cell->GetId());

        auto objectManager = Bootstrap_->GetObjectManager();
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
            LOG_INFO_UNLESS(IsRecovery(), "Requested to update stoares for a tablet in %Qlv state, ignored (TabletId: %v)",
                tablet->GetState(),
                tabletId);
            return;
        }

        auto* cell = tablet->GetCell();
        auto* table = tablet->GetTable();
        if (!IsObjectAlive(table))
            return;

        auto cypressManager = Bootstrap_->GetCypressManager();
        cypressManager->SetModified(table, nullptr);

        TRspUpdateTabletStores response;
        response.mutable_tablet_id()->MergeFrom(request.tablet_id());
        response.mutable_stores_to_add()->MergeFrom(request.stores_to_add());
        response.mutable_stores_to_remove()->MergeFrom(request.stores_to_remove());

        try {
            auto chunkManager = Bootstrap_->GetChunkManager();
            auto securityManager = Bootstrap_->GetSecurityManager();

            // Collect all changes first.
            std::vector<TChunkTree*> chunksToAttach;
            i64 attachedRowCount = 0;
            for (const auto& descriptor : request.stores_to_add()) {
                auto storeId = FromProto<TStoreId>(descriptor.store_id());
                if (TypeFromId(storeId) == EObjectType::Chunk ||
                    TypeFromId(storeId) == EObjectType::ErasureChunk)
                {
                    auto* chunk = chunkManager->GetChunkOrThrow(storeId);
                    const auto& miscExt = chunk->MiscExt();
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
                    const auto& miscExt = chunk->MiscExt();
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

            LOG_INFO_UNLESS(IsRecovery(), "Tablet stores updated (TabletId: %v, AttachedChunkIds: [%v], DetachedChunkIds: [%v], "
                "AttachedRowCount: %v, DetachedRowCount: %v)",
                tabletId,
                JoinToString(ToObjectIds(chunksToAttach)),
                JoinToString(ToObjectIds(chunksToDetach)),
                attachedRowCount,
                detachedRowCount);
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            LOG_WARNING_UNLESS(IsRecovery(), error, "Error updating tablet stores (TabletId: %v)",
                tabletId);
            ToProto(response.mutable_error(), error);
        }

        auto hiveManager = Bootstrap_->GetHiveManager();
        auto* mailbox = hiveManager->GetMailbox(cell->GetId());
        hiveManager->PostMessage(mailbox, response);
    }


    virtual void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TabletTracker_->Start();

        auto cellDirectory = Bootstrap_->GetCellDirectory();
        cellDirectory->Clear();

        for (const auto& pair : TabletCellMap_) {
            auto* cell = pair.second;
            UpdateCellDirectory(cell);
        }

        CleanupExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(),
            BIND(&TImpl::OnCleanup, MakeWeak(this)),
            CleanupPeriod);
        CleanupExecutor_->Start();
    }

    virtual void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TabletTracker_->Stop();

        if (CleanupExecutor_) {
            CleanupExecutor_->Stop();
            CleanupExecutor_.Reset();
        }
    }


    void ReconfigureCell(TTabletCell* cell)
    {
        cell->SetConfigVersion(cell->GetConfigVersion() + 1);
        
        auto config = cell->GetConfig();
        config->Addresses.clear();
        for (int index = 0; index < static_cast<int>(cell->Peers().size()); ++index) {
            const auto& peer = cell->Peers()[index];
            config->Addresses.push_back(peer.Address ? *peer.Address : "");
        }

        UpdateCellDirectory(cell);

        LOG_INFO_UNLESS(IsRecovery(), "Tablet cell reconfigured (CellId: %v, Version: %v)",
            cell->GetId(),
            cell->GetConfigVersion());
    }

    void UpdateCellDirectory(TTabletCell* cell)
    {
        auto cellDirectory = Bootstrap_->GetCellDirectory();
        cellDirectory->RegisterCell(
            cell->GetConfig()->ToElection(cell->GetId()),
            cell->GetConfigVersion());
    }


    void ValidateHasHealthyCells()
    {
        for (const auto& pair : TabletCellMap_) {
            auto* cell = pair.second;
            if (cell->GetHealth() == ETabletCellHealth::Good)
                return;
        }
        THROW_ERROR_EXCEPTION("No healthy tablet cells");
    }

    std::vector<TTabletCell*> AllocateCells(TTabletCell* hintedCell, int count)
    {
        // TODO(babenko): do something smarter?
        if (hintedCell) {
            return std::vector<TTabletCell*>(count, hintedCell);
        }

        auto allCells = GetValues(TabletCellMap_);

        allCells.erase(
            std::remove_if(
                allCells.begin(),
                allCells.end(),
                [] (const TTabletCell* cell) {
                    return cell->IsAlive() && cell->GetHealth() != ETabletCellHealth::Good;
                }),
            allCells.end());
        
        YCHECK(!allCells.empty());

        std::sort(
            allCells.begin(),
            allCells.end(),
            [] (TTabletCell* lhs, TTabletCell* rhs) {
                return lhs->GetId() < rhs->GetId();
            });

        auto* mutationContext = Bootstrap_
            ->GetHydraFacade()
            ->GetHydraManager()
            ->GetMutationContext();

        std::vector<TTabletCell*> cells;
        for (int index = 0; index < count; ++index) {
            cells.push_back(allCells[mutationContext->RandomGenerator().Generate<size_t>() % allCells.size()]);
        }

        return cells;
    }


    void StartPrerequisiteTransaction(TTabletCell* cell)
    {
        AbortPrerequisiteTransaction(cell);

        auto transactionManager = Bootstrap_->GetTransactionManager();
        auto* transaction = transactionManager->StartTransaction(nullptr, Null);

        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title", Format("Prerequisite for cell %v", cell->GetId()));
        auto objectManager = Bootstrap_->GetObjectManager();
        objectManager->FillAttributes(transaction, *attributes);

        cell->SetPrerequisiteTransaction(transaction);
        YCHECK(TransactionToCellMap_.insert(std::make_pair(transaction, cell)).second);

        LOG_INFO_UNLESS(IsRecovery(), "Tablet cell prerequisite transaction started (CellId: %v, TransactionId: %v)",
            cell->GetId(),
            transaction->GetId());
    }

    void AbortPrerequisiteTransaction(TTabletCell* cell)
    {
        auto* transaction = cell->GetPrerequisiteTransaction();
        if (!transaction)
            return;

        // NB: Make a copy, transaction will die soon.
        auto transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->AbortTransaction(transaction, true);

        // NB: Cell-to-transaction link is broken in OnTransactionFinished from AbortTransaction.
        YCHECK(!cell->GetPrerequisiteTransaction());
    }

    void OnTransactionFinished(TTransaction* transaction)
    {
        auto it = TransactionToCellMap_.find(transaction);
        if (it == TransactionToCellMap_.end())
            return;

        auto* cell = it->second;
        cell->SetPrerequisiteTransaction(nullptr);
        TransactionToCellMap_.erase(it);

        LOG_INFO_UNLESS(IsRecovery(), "Tablet cell prerequisite transaction aborted (CellId: %v, TransactionId: %v)",
            cell->GetId(),
            transaction->GetId());

        if (IsObjectAlive(cell)) {
            for (auto peerId = 0; peerId < cell->Peers().size(); ++peerId) {
                const auto& peer = cell->Peers()[peerId];
                if (peer.Node) {
                    cell->DetachPeer(peer.Node);
                }
                if (peer.Address) {
                    RemoveFromAddressToCellMap(*peer.Address, cell);
                    cell->RevokePeer(peerId);
                }
            }
            ReconfigureCell(cell);
        }
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
                LOG_INFO_UNLESS(IsRecovery(), "Unmounting tablet (TableId: %v, TabletId: %v, CellId: %v, Force: %v)",
                    table->GetId(),
                    tablet->GetId(),
                    cell->GetId(),
                    force);

                tablet->SetState(ETabletState::Unmounting);
                tablet->NodeStatistics().Clear();

                auto hiveManager = Bootstrap_->GetHiveManager();

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
        auto objectManager = Bootstrap_->GetObjectManager();
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
        writerOptions->ChunksVital = table->GetVital();
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
                THROW_ERROR_EXCEPTION("First tablet index %v is out of range [%v, %v]",
                    *first,
                    0,
                    tablets.size() - 1);
            }
            if (*last < 0 || *last >= tablets.size()) {
                THROW_ERROR_EXCEPTION("Last tablet index %v is out of range [%v, %v]",
                    *last,
                    0,
                    tablets.size() - 1);
            }
            if (*first > *last) {
                THROW_ERROR_EXCEPTION("First tablet index is greater than last tablet index");
            }
        }
    }


    IMapNodePtr GetCellMapNode()
    {
        auto cypressManager = Bootstrap_->GetCypressManager();
        auto resolver = cypressManager->CreateResolver();
        return resolver->ResolvePath("//sys/tablet_cells")->AsMap();
    }


    void OnCleanup()
    {
        auto cypressManager = Bootstrap_->GetCypressManager();
        auto resolver = cypressManager->CreateResolver();
        for (const auto& pair : TabletCellMap_) {
            const auto& cellId = pair.first;
            const auto* cell = pair.second;
            if (!IsObjectAlive(cell))
                continue;

            auto snapshotsPath = Format("//sys/tablet_cells/%v/snapshots", cellId);
            auto snapshotsMap = resolver->ResolvePath(snapshotsPath)->AsMap();
            if (!snapshotsMap)
                continue;

            std::vector<int> snapshotIds;
            auto snapshotKeys = SyncYPathList(snapshotsMap, "");
            for (const auto& key : snapshotKeys) {
                int snapshotId;
                try {
                    snapshotId = FromString<int>(key);
                } catch (const std::exception& ex) {
                    LOG_WARNING("Unrecognized item %Qv in tablet snapshot store (CellId: %v)",
                        key,
                        cellId);
                    continue;
                }
                snapshotIds.push_back(snapshotId);
            }

            if (snapshotIds.size() <= Config_->MaxSnapshotsToKeep)
                continue;

            std::sort(snapshotIds.begin(), snapshotIds.end());
            int thresholdId = snapshotIds[snapshotIds.size() - Config_->MaxSnapshotsToKeep];

            auto objectManager = Bootstrap_->GetObjectManager();
            auto rootService = objectManager->GetRootService();

            for (const auto& key : snapshotKeys) {
                try {
                    int snapshotId = FromString<int>(key);
                    if (snapshotId < thresholdId) {
                        LOG_INFO("Removing tablet cell snapshot %v (CellId: %v)",
                            snapshotId,
                            cellId);
                        auto req = TYPathProxy::Remove(snapshotsPath + "/" + key);
                        ExecuteVerb(rootService, req).Subscribe(BIND([=] (const TYPathProxy::TErrorOrRspRemovePtr& rspOrError) {
                            if (rspOrError.IsOK()) {
                                LOG_INFO("Tablet cell snapshot %v removed successfully (CellId: %v)",
                                    snapshotId,
                                    cellId);
                            } else {
                                LOG_INFO(rspOrError, "Error removing tablet cell snapshot %v (CellId: %v)",
                                    snapshotId,
                                    cellId);
                            }
                        }));
                    }
                } catch (const std::exception& ex) {
                    // Ignore, cf. logging above.
                }
            }

            auto changelogsPath = Format("//sys/tablet_cells/%v/changelogs", cellId);
            auto changelogsMap = resolver->ResolvePath(changelogsPath)->AsMap();
            if (!changelogsMap)
                continue;

            auto changelogKeys = SyncYPathList(changelogsMap, "");
            for (const auto& key : changelogKeys) {
                int changelogId;
                try {
                    changelogId = FromString<int>(key);
                } catch (const std::exception& ex) {
                    LOG_WARNING("Unrecognized item %Qv in tablet changelog store (CellId: %v)",
                        key,
                        cellId);
                    continue;
                }
                if (changelogId < thresholdId) {
                    LOG_INFO("Removing tablet cell changelog %v (CellId: %v)",
                        changelogId,
                        cellId);
                    auto req = TYPathProxy::Remove(changelogsPath + "/" + key);
                    ExecuteVerb(rootService, req).Subscribe(BIND([=] (const TYPathProxy::TErrorOrRspRemovePtr& rspOrError) {
                        if (rspOrError.IsOK()) {
                            LOG_INFO("Tablet cell changelog %v removed successfully (CellId: %v)",
                                changelogId,
                                cellId);
                        } else {
                            LOG_INFO(rspOrError, "Error removing tablet cell changelog %v (CellId: %v)",
                                changelogId,
                                cellId);
                        }
                    }));;
                }
            }
        }
    }

};

DEFINE_ENTITY_MAP_ACCESSORS(TTabletManager::TImpl, TabletCell, TTabletCell, TTabletCellId, TabletCellMap_)
DEFINE_ENTITY_MAP_ACCESSORS(TTabletManager::TImpl, Tablet, TTablet, TTabletId, TabletMap_)

///////////////////////////////////////////////////////////////////////////////

TTabletManager::TTabletCellTypeHandler::TTabletCellTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->TabletCellMap_)
    , Owner_(owner)
{ }

TObjectBase* TTabletManager::TTabletCellTypeHandler::Create(
    TTransaction* transaction,
    TAccount* account,
    IAttributeDictionary* attributes,
    TReqCreateObjects* request,
    TRspCreateObjects* response)
{
    // TODO(babenko): support arbitrary size
    auto* cell = Owner_->CreateCell(1, attributes);
    return cell;
}

void TTabletManager::TTabletCellTypeHandler::DoDestroy(TTabletCell* cell)
{
    Owner_->DestroyCell(cell);
}

///////////////////////////////////////////////////////////////////////////////

TTabletManager::TTabletTypeHandler::TTabletTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->TabletMap_)
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

int TTabletManager::GetAssignedTabletCellCount(const Stroka& address) const
{
    return Impl_->GetAssignedTabletCellCount(address);
}

TTableSchema TTabletManager::GetTableSchema(TTableNode* table)
{
    return Impl_->GetTableSchema(table);
}

TTabletStatistics TTabletManager::GetTabletStatistics(const TTablet* tablet)
{
    return Impl_->GetTabletStatistics(tablet);
}

void TTabletManager::MountTable(
    TTableNode* table,
    int firstTabletIndex,
    int lastTabletIndex,
    TTabletCellId cellId)
{
    Impl_->MountTable(
        table,
        firstTabletIndex,
        lastTabletIndex,
        cellId);
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

TTabletCell* TTabletManager::GetTabletCellOrThrow(const TTabletCellId& id)
{
    return Impl_->GetTabletCellOrThrow(id);
}

DELEGATE_ENTITY_MAP_ACCESSORS(TTabletManager, TabletCell, TTabletCell, TTabletCellId, *Impl_)
DELEGATE_ENTITY_MAP_ACCESSORS(TTabletManager, Tablet, TTablet, TTabletId, *Impl_)

///////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
