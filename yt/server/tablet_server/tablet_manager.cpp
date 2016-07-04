#include "tablet_manager.h"
#include "private.h"
#include "config.h"
#include "cypress_integration.h"
#include "tablet.h"
#include "tablet_cell.h"
#include "tablet_cell_proxy.h"
#include "tablet_cell_bundle.h"
#include "tablet_cell_bundle_proxy.h"
#include "tablet_proxy.h"
#include "tablet_tracker.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/hydra_facade.h>
#include <yt/server/cell_master/serialize.h>

#include <yt/server/chunk_server/chunk_list.h>
#include <yt/server/chunk_server/chunk_manager.h>
#include <yt/server/chunk_server/chunk_tree_traversing.h>

#include <yt/server/cypress_server/cypress_manager.h>

#include <yt/server/hive/hive_manager.h>

#include <yt/server/node_tracker_server/node.h>
#include <yt/server/node_tracker_server/node_tracker.h>

#include <yt/server/object_server/object_manager.h>
#include <yt/server/object_server/type_handler_detail.h>

#include <yt/server/security_server/security_manager.h>
#include <yt/server/security_server/group.h>
#include <yt/server/security_server/subject.h>

#include <yt/server/table_server/table_node.h>

#include <yt/server/tablet_node/config.h>
#include <yt/server/tablet_node/tablet_manager.pb.h>

#include <yt/server/tablet_server/tablet_manager.pb.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/config.h>

#include <yt/ytlib/election/config.h>

#include <yt/ytlib/hive/cell_directory.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/ytlib/table_client/schema.h>

#include <yt/ytlib/tablet_client/config.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/misc/address.h>
#include <yt/core/misc/collection_helpers.h>
#include <yt/core/misc/string.h>

#include <util/random/random.h>

#include <algorithm>

namespace NYT {
namespace NTabletServer {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NObjectClient;
using namespace NObjectClient::NProto;
using namespace NObjectServer;
using namespace NYTree;
using namespace NSecurityServer;
using namespace NTableServer;
using namespace NTabletClient;
using namespace NHydra;
using namespace NHiveClient;
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
using namespace NCypressClient;
using namespace NCellMaster;

using NTabletNode::TTableMountConfigPtr;
using NTabletNode::EInMemoryMode;
using NTabletNode::EStoreType;
using NNodeTrackerServer::NProto::TReqIncrementalHeartbeat;
using NNodeTrackerClient::TNodeDescriptor;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletServerLogger;
static const auto CleanupPeriod = TDuration::Seconds(10);

////////////////////////////////////////////////////////////////////////////////

class TTabletManager::TTabletCellBundleTypeHandler
    : public TObjectTypeHandlerWithMapBase<TTabletCellBundle>
{
public:
    explicit TTabletCellBundleTypeHandler(TImpl* owner);

    virtual EObjectType GetType() const override
    {
        return EObjectType::TabletCellBundle;
    }

    virtual ETypeFlags GetFlags() const override
    {
        return
            ETypeFlags::ReplicateCreate |
            ETypeFlags::ReplicateDestroy |
            ETypeFlags::ReplicateAttributes |
            ETypeFlags::Creatable;
    }

    virtual TObjectBase* CreateObject(
        const TObjectId& hintId,
        IAttributeDictionary* attributes) override;

private:
    TImpl* const Owner_;

    virtual TCellTagList DoGetReplicationCellTags(const TTabletCellBundle* /*cellBundle*/) override
    {
        return AllSecondaryCellTags();
    }

    virtual Stroka DoGetName(const TTabletCellBundle* cellBundle) override
    {
        return Format("tablet cell bundle %Qv", cellBundle->GetName());
    }

    virtual TAccessControlDescriptor* DoFindAcd(TTabletCellBundle* cellBundle) override
    {
        return &cellBundle->Acd();
    }

    virtual IObjectProxyPtr DoGetProxy(TTabletCellBundle* cellBundle, TTransaction* /*transaction*/) override
    {
        return CreateTabletCellBundleProxy(Bootstrap_, &Metadata_, cellBundle);
    }

    virtual void DoDestroyObject(TTabletCellBundle* cellBundle) override;

};

////////////////////////////////////////////////////////////////////////////////

class TTabletManager::TTabletCellTypeHandler
    : public TObjectTypeHandlerWithMapBase<TTabletCell>
{
public:
    explicit TTabletCellTypeHandler(TImpl* owner);

    virtual ETypeFlags GetFlags() const override
    {
        return
            ETypeFlags::ReplicateCreate |
            ETypeFlags::ReplicateDestroy |
            ETypeFlags::ReplicateAttributes |
            ETypeFlags::Creatable;
    }

    virtual EObjectType GetType() const override
    {
        return EObjectType::TabletCell;
    }

    virtual TObjectBase* CreateObject(
        const TObjectId& hintId,
        IAttributeDictionary* attributes) override;

private:
    TImpl* const Owner_;

    virtual TCellTagList DoGetReplicationCellTags(const TTabletCell* /*cell*/) override
    {
        return AllSecondaryCellTags();
    }

    virtual Stroka DoGetName(const TTabletCell* cell) override
    {
        return Format("tablet cell %v", cell->GetId());
    }

    virtual IObjectProxyPtr DoGetProxy(TTabletCell* cell, TTransaction* /*transaction*/) override
    {
        return CreateTabletCellProxy(Bootstrap_, &Metadata_, cell);
    }

    virtual void DoZombifyObject(TTabletCell* cell) override;

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
    TImpl* const Owner_;

    virtual Stroka DoGetName(const TTablet* object) override
    {
        return Format("tablet %v", object->GetId());
    }

    virtual IObjectProxyPtr DoGetProxy(TTablet* tablet, TTransaction* /*transaction*/) override
    {
        return CreateTabletProxy(Bootstrap_, &Metadata_, tablet);
    }

    virtual void DoDestroyObject(TTablet* tablet) override;

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
            ESyncSerializationPriority::Keys,
            "TabletManager.Keys",
            BIND(&TImpl::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "TabletManager.Values",
            BIND(&TImpl::SaveValues, Unretained(this)));

        auto cellTag = Bootstrap_->GetPrimaryCellTag();
        DefaultTabletCellBundleId_ = MakeWellKnownId(EObjectType::TabletCellBundle, cellTag, 0xffffffffffffffff);

        RegisterMethod(BIND(&TImpl::HydraAssignPeers, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraRevokePeers, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraSetLeadingPeer, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraOnTabletMounted, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraOnTabletUnmounted, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraOnTabletFrozen, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraOnTabletUnfrozen, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUpdateTabletStores, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUpdateTabletTrimmedRowCount, Unretained(this)));

        if (Bootstrap_->IsPrimaryMaster()) {
            auto nodeTracker = Bootstrap_->GetNodeTracker();
            nodeTracker->SubscribeNodeRegistered(BIND(&TImpl::OnNodeRegistered, MakeWeak(this)));
            nodeTracker->SubscribeNodeUnregistered(BIND(&TImpl::OnNodeUnregistered, MakeWeak(this)));
            nodeTracker->SubscribeIncrementalHeartbeat(BIND(&TImpl::OnIncrementalHeartbeat, MakeWeak(this)));
        }
    }

    void Initialize()
    {
        auto objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(New<TTabletCellBundleTypeHandler>(this));
        objectManager->RegisterHandler(New<TTabletCellTypeHandler>(this));
        objectManager->RegisterHandler(New<TTabletTypeHandler>(this));

        auto transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->SubscribeTransactionCommitted(BIND(&TImpl::OnTransactionFinished, MakeWeak(this)));
        transactionManager->SubscribeTransactionAborted(BIND(&TImpl::OnTransactionFinished, MakeWeak(this)));
    }

    TTabletCellBundle* CreateTabletCellBundle(const Stroka& name, const TObjectId& hintId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        ValidateTabletCellBundleName(name);

        if (FindTabletCellBundleByName(name)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Tablet cell bundle %Qv already exists",
                name);
        }

        auto objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::TabletCellBundle, hintId);
        return DoCreateTabletCellBundle(id, name);
    }

    TTabletCellBundle* DoCreateTabletCellBundle(const TTabletCellBundleId& id, const Stroka& name)
    {
        auto cellBundleHolder = std::make_unique<TTabletCellBundle>(id);
        cellBundleHolder->SetName(name);

        auto* cellBundle = TabletCellBundleMap_.Insert(id, std::move(cellBundleHolder));
        YCHECK(NameToTabletCellBundleMap_.insert(std::make_pair(cellBundle->GetName(), cellBundle)).second);

        auto objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(cellBundle);

        return cellBundle;
    }

    void DestroyTabletCellBundle(TTabletCellBundle* cellBundle)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        // Remove tablet cell bundle from maps.
        YCHECK(NameToTabletCellBundleMap_.erase(cellBundle->GetName()) == 1);
    }

    TTabletCell* CreateTabletCell(TTabletCellBundle* cellBundle, const TObjectId& hintId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ValidatePermission(cellBundle, EPermission::Use);

        auto objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::TabletCell, hintId);
        auto cellHolder = std::make_unique<TTabletCell>(id);

        cellHolder->Peers().resize(cellBundle->GetOptions()->PeerCount);
        cellHolder->SetCellBundle(cellBundle);
        YCHECK(cellBundle->TabletCells().insert(cellHolder.get()).second);
        objectManager->RefObject(cellBundle);

        ReconfigureCell(cellHolder.get());

        auto* cell = TabletCellMap_.Insert(id, std::move(cellHolder));

        // Make the fake reference.
        YCHECK(cell->RefObject() == 1);

        auto hiveManager = Bootstrap_->GetHiveManager();
        hiveManager->CreateMailbox(id);

        auto cellMapNodeProxy = GetCellMapNode();
        auto cellNodePath = "/" + ToString(id);

        try {
            // NB: Users typically are not allowed to create these types.
            auto securityManager = Bootstrap_->GetSecurityManager();
            auto* rootUser = securityManager->GetRootUser();
            TAuthenticatedUserGuard userGuard(securityManager, rootUser);
            
            // Create Cypress node.
            {
                auto req = TCypressYPathProxy::Create(cellNodePath);
                req->set_type(static_cast<int>(EObjectType::TabletCellNode));

                auto attributes = CreateEphemeralAttributes();
                attributes->Set("opaque", true);
                ToProto(req->mutable_node_attributes(), *attributes);

                SyncExecuteVerb(cellMapNodeProxy, req);
            }

            // Create "snapshots" child.
            {
                auto req = TCypressYPathProxy::Create(cellNodePath + "/snapshots");
                req->set_type(static_cast<int>(EObjectType::MapNode));

                SyncExecuteVerb(cellMapNodeProxy, req);
            }

            // Create "changelogs" child.
            {
                auto req = TCypressYPathProxy::Create(cellNodePath + "/changelogs");
                req->set_type(static_cast<int>(EObjectType::MapNode));

                SyncExecuteVerb(cellMapNodeProxy, req);
            }
        } catch (const std::exception& ex) {
            LOG_ERROR_UNLESS(IsRecovery(), ex, "Error registering tablet cell in Cypress");
        }

        return cell;
    }

    void DestroyTabletCell(TTabletCell* cell)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto hiveManager = Bootstrap_->GetHiveManager();
        hiveManager->RemoveMailbox(cell->GetId());

        for (const auto& peer : cell->Peers()) {
            if (peer.Node) {
                peer.Node->DetachTabletCell(cell);
            }
            if (!peer.Descriptor.IsNull()) {
                RemoveFromAddressToCellMap(peer.Descriptor, cell);
            }
        }

        auto* cellBundle = cell->GetCellBundle();
        YCHECK(cellBundle->TabletCells().erase(cell) == 1);
        auto objectManager = Bootstrap_->GetObjectManager();
        objectManager->UnrefObject(cellBundle);
        cell->SetCellBundle(nullptr);

        AbortPrerequisiteTransaction(cell);
        AbortCellSubtreeTransactions(cell);

        auto cellNodeProxy = FindCellNode(cell->GetId());
        if (cellNodeProxy) {
            // NB: This should succeed regardless of user permissions.
            auto securityManager = Bootstrap_->GetSecurityManager();
            auto* rootUser = securityManager->GetRootUser();
            TAuthenticatedUserGuard userGuard(securityManager, rootUser);

            try {
                // NB: Subtree transactions were already aborted in AbortPrerequisiteTransaction.
                cellNodeProxy->GetParent()->RemoveChild(cellNodeProxy);
            } catch (const std::exception& ex) {
                LOG_ERROR_UNLESS(IsRecovery(), ex, "Error unregisterting tablet cell from Cypress");
            }
        }
    }


    TTablet* CreateTablet(TTableNode* table)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::Tablet, NullObjectId);
        auto tabletHolder = std::make_unique<TTablet>(id);
        tabletHolder->SetTable(table);

        auto* tablet = TabletMap_.Insert(id, std::move(tabletHolder));
        objectManager->RefObject(tablet);

        LOG_INFO_UNLESS(IsRecovery(), "Tablet created (TableId: %v, TabletId: %v)",
            table->GetId(),
            tablet->GetId());

        return tablet;
    }

    void DestroyTablet(TTablet* tablet)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        YCHECK(!tablet->GetCell());
    }


    int GetAssignedTabletCellCount(const Stroka& address) const
    {
        auto range = AddressToCell_.equal_range(address);
        return std::distance(range.first, range.second);
    }

    TTabletStatistics GetTabletStatistics(const TTablet* tablet)
    {
        const auto* table = tablet->GetTable();
        const auto* rootChunkList = table->GetChunkList();
        const auto* tabletChunkList = rootChunkList->Children()[tablet->GetIndex()]->AsChunkList();
        const auto& treeStatistics = tabletChunkList->Statistics();
        const auto& nodeStatistics = tablet->NodeStatistics();

        TTabletStatistics tabletStatistics;
        tabletStatistics.PartitionCount = nodeStatistics.partition_count();
        tabletStatistics.StoreCount = nodeStatistics.store_count();
        tabletStatistics.PreloadPendingStoreCount = nodeStatistics.preload_pending_store_count();
        tabletStatistics.PreloadCompletedStoreCount = nodeStatistics.preload_completed_store_count();
        tabletStatistics.PreloadFailedStoreCount = nodeStatistics.preload_failed_store_count();
        tabletStatistics.UnmergedRowCount = treeStatistics.RowCount;
        tabletStatistics.UncompressedDataSize = treeStatistics.UncompressedDataSize;
        tabletStatistics.CompressedDataSize = treeStatistics.CompressedDataSize;
        switch (tablet->GetInMemoryMode()) {
            case EInMemoryMode::Compressed:
                tabletStatistics.MemorySize = tabletStatistics.CompressedDataSize;
                break;
            case EInMemoryMode::Uncompressed:
                tabletStatistics.MemorySize = tabletStatistics.UncompressedDataSize;
                break;
            case EInMemoryMode::None:
                tabletStatistics.MemorySize = 0;
                break;
            default:
                YUNREACHABLE();
        }
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
        TTabletCell* hintCell,
        bool freeze)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(table->IsTrunk());

        if (!table->IsDynamic()) {
            THROW_ERROR_EXCEPTION("Cannot mount a static table");
        }

        ParseTabletRange(table, &firstTabletIndex, &lastTabletIndex); // may throw

        if (hintCell && hintCell->GetCellBundle() != table->GetTabletCellBundle()) {
            // Will throw :)
            THROW_ERROR_EXCEPTION("Cannot mount tablets into cell %v since it belongs to bundle %Qv while the table "
                "is configured to use bundle %Qv",
                hintCell->GetId(),
                hintCell->GetCellBundle()->GetName(),
                table->GetTabletCellBundle()->GetName());
        }

        if (!hintCell) {
            ValidateHasHealthyCells(table->GetTabletCellBundle()); // may throw
        }

        auto objectManager = Bootstrap_->GetObjectManager();
        auto chunkManager = Bootstrap_->GetChunkManager();

        const auto& allTablets = table->Tablets();

        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            const auto* tablet = allTablets[index];
            auto state = tablet->GetState();
            if (state == ETabletState::Unmounting) {
                THROW_ERROR_EXCEPTION("Tablet %v is in %Qlv state",
                    tablet->GetId(),
                    state);
            }
        }

        TTableMountConfigPtr mountConfig;
        NTabletNode::TTabletWriterOptionsPtr writerOptions;
        GetTableSettings(table, &mountConfig, &writerOptions);

        auto serializedMountConfig = ConvertToYsonString(mountConfig);
        auto serializedWriterOptions = ConvertToYsonString(writerOptions);

        std::vector<TTablet*> tabletsToMount;
        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto* tablet = allTablets[index];
            if (!tablet->GetCell()) {
                tabletsToMount.push_back(tablet);
            }
        }

        const auto& chunkLists = table->GetChunkList()->Children();
        YCHECK(allTablets.size() == chunkLists.size());

        auto assignment = ComputeTabletAssignment(
            table,
            mountConfig,
            hintCell,
            std::move(tabletsToMount));

        for (const auto& pair : assignment) {
            auto* tablet = pair.first;
            int tabletIndex = tablet->GetIndex();

            auto* cell = pair.second;
            tablet->SetCell(cell);
            YCHECK(cell->Tablets().insert(tablet).second);
            objectManager->RefObject(cell);

            YCHECK(tablet->GetState() == ETabletState::Unmounted);
            tablet->SetState(ETabletState::Mounting);
            tablet->SetInMemoryMode(mountConfig->InMemoryMode);

            const auto* context = GetCurrentMutationContext();
            tablet->SetMountRevision(context->GetVersion().ToRevision());

            TReqMountTablet req;
            ToProto(req.mutable_tablet_id(), tablet->GetId());
            req.set_mount_revision(tablet->GetMountRevision());
            ToProto(req.mutable_table_id(), table->GetId());
            ToProto(req.mutable_schema(), table->TableSchema());
            if (table->IsSorted()) {
                ToProto(req.mutable_pivot_key(), tablet->GetPivotKey());
                ToProto(req.mutable_next_pivot_key(), tablet->GetIndex() + 1 == allTablets.size()
                    ? MaxKey()
                    : allTablets[tabletIndex + 1]->GetPivotKey());
            } else {
                req.set_trimmed_row_count(tablet->GetTrimmedRowCount());
            }
            req.set_mount_config(serializedMountConfig.Data());
            req.set_writer_options(serializedWriterOptions.Data());
            req.set_atomicity(static_cast<int>(table->GetAtomicity()));
            req.set_freeze(freeze);

            auto* chunkList = chunkLists[tabletIndex]->AsChunkList();
            auto chunks = EnumerateChunksInChunkTree(chunkList);
            auto storeType = table->IsSorted() ? EStoreType::SortedChunk : EStoreType::OrderedChunk;
            i64 startingRowIndex = tablet->GetTrimmedStoresRowCount();
            for (const auto* chunk : chunks) {
                auto* descriptor = req.add_stores();
                descriptor->set_store_type(static_cast<int>(storeType));
                ToProto(descriptor->mutable_store_id(), chunk->GetId());
                descriptor->mutable_chunk_meta()->CopyFrom(chunk->ChunkMeta());
                descriptor->set_starting_row_index(startingRowIndex);
                startingRowIndex += chunk->MiscExt().row_count();
            }

            auto hiveManager = Bootstrap_->GetHiveManager();
            auto* mailbox = hiveManager->GetMailbox(cell->GetId());
            hiveManager->PostMessage(mailbox, req);

            LOG_INFO_UNLESS(IsRecovery(), "Mounting tablet (TableId: %v, TabletId: %v, CellId: %v, ChunkCount: %v, "
                "Atomicity: %v, Freeze: %v)",
                table->GetId(),
                tablet->GetId(),
                cell->GetId(),
                chunks.size(),
                table->GetAtomicity(),
                freeze);
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

        if (!table->IsDynamic()) {
            THROW_ERROR_EXCEPTION("Cannot unmount a static table");
        }

        ParseTabletRange(table, &firstTabletIndex, &lastTabletIndex); // may throw

        if (!force) {
            for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
                auto* tablet = table->Tablets()[index];
                auto state = tablet->GetState();
                if (state != ETabletState::Mounted &&
                    state != ETabletState::Frozen &&
                    state != ETabletState::Freezing &&
                    state != ETabletState::Unmounted &&
                    state != ETabletState::Unmounting)
                {
                    THROW_ERROR_EXCEPTION("Tablet %v is in %Qlv state",
                        tablet->GetId(),
                        state);
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

        if (!table->IsDynamic()) {
            THROW_ERROR_EXCEPTION("Cannot remount a static table");
        }

        ParseTabletRange(table, &firstTabletIndex, &lastTabletIndex); // may throw

        TTableMountConfigPtr mountConfig;
        NTabletNode::TTabletWriterOptionsPtr writerOptions;
        GetTableSettings(table, &mountConfig, &writerOptions);

        auto serializedMountConfig = ConvertToYsonString(mountConfig);
        auto serializedWriterOptions = ConvertToYsonString(writerOptions);

        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto* tablet = table->Tablets()[index];
            auto* cell = tablet->GetCell();
            auto state = tablet->GetState();

            if (state == ETabletState::Mounted ||
                state == ETabletState::Mounting ||
                state == ETabletState::Frozen ||
                state == ETabletState::Freezing)
            {
                LOG_INFO_UNLESS(IsRecovery(), "Remounting tablet (TableId: %v, TabletId: %v, CellId: %v)",
                    table->GetId(),
                    tablet->GetId(),
                    cell->GetId());

                cell->TotalStatistics() -= GetTabletStatistics(tablet);
                tablet->SetInMemoryMode(mountConfig->InMemoryMode);
                cell->TotalStatistics() += GetTabletStatistics(tablet);

                auto hiveManager = Bootstrap_->GetHiveManager();

                TReqRemountTablet request;
                request.set_mount_config(serializedMountConfig.Data());
                request.set_writer_options(serializedWriterOptions.Data());
                ToProto(request.mutable_tablet_id(), tablet->GetId());

                auto* mailbox = hiveManager->GetMailbox(cell->GetId());
                hiveManager->PostMessage(mailbox, request);
            }
        }
    }

    void FreezeTable(
        TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(table->IsTrunk());

        if (!table->IsDynamic()) {
            THROW_ERROR_EXCEPTION("Cannot freeze a static table");
        }

        ParseTabletRange(table, &firstTabletIndex, &lastTabletIndex); // may throw

        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto* tablet = table->Tablets()[index];
            auto state = tablet->GetState();
            if (state != ETabletState::Mounted &&
                state != ETabletState::Freezing &&
                state != ETabletState::Frozen)
            {
                THROW_ERROR_EXCEPTION("Tablet %v is in %Qlv state",
                    tablet->GetId(),
                    state);
            }
        }

        auto hiveManager = Bootstrap_->GetHiveManager();

        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto* tablet = table->Tablets()[index];
            auto* cell = tablet->GetCell();

            if (tablet->GetState() == ETabletState::Mounted) {
                LOG_INFO_UNLESS(IsRecovery(), "Freezing tablet (TableId: %v, TabletId: %v, CellId: %v)",
                    table->GetId(),
                    tablet->GetId(),
                    cell->GetId());

                tablet->SetState(ETabletState::Freezing);

                TReqFreezeTablet request;
                ToProto(request.mutable_tablet_id(), tablet->GetId());

                auto* mailbox = hiveManager->GetMailbox(cell->GetId());
                hiveManager->PostMessage(mailbox, request);
            }
        }
    }

    void UnfreezeTable(
        TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(table->IsTrunk());

        if (!table->IsDynamic()) {
            THROW_ERROR_EXCEPTION("Cannot unfreeze a static table");
        }

        ParseTabletRange(table, &firstTabletIndex, &lastTabletIndex); // may throw

        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto* tablet = table->Tablets()[index];
            auto state = tablet->GetState();
            if (state != ETabletState::Mounted &&
                state != ETabletState::Frozen &&
                state != ETabletState::Unfreezing)
            {
                THROW_ERROR_EXCEPTION("Tablet %v is in %Qlv state",
                    tablet->GetId(),
                    state);
            }
        }

        auto hiveManager = Bootstrap_->GetHiveManager();

        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto* tablet = table->Tablets()[index];
            auto* cell = tablet->GetCell();

            if (tablet->GetState() == ETabletState::Frozen) {
                LOG_INFO_UNLESS(IsRecovery(), "Unfreezing tablet (TableId: %v, TabletId: %v, CellId: %v)",
                    table->GetId(),
                    tablet->GetId(),
                    cell->GetId());

                tablet->SetState(ETabletState::Unfreezing);

                TReqUnfreezeTablet request;
                ToProto(request.mutable_tablet_id(), tablet->GetId());

                auto* mailbox = hiveManager->GetMailbox(cell->GetId());
                hiveManager->PostMessage(mailbox, request);
            }
        }
    }

    void DestroyTable(TTableNode* table)
    {
        if (table->GetTabletCellBundle()) {
            auto objectManager = Bootstrap_->GetObjectManager();
            objectManager->UnrefObject(table->GetTabletCellBundle());
            table->SetTabletCellBundle(nullptr);
        }

        if (!table->Tablets().empty()) {
            DoUnmountTable(
                table,
                true,
                0,
                static_cast<int>(table->Tablets().size()) - 1);

            auto objectManager = Bootstrap_->GetObjectManager();
            for (auto* tablet : table->Tablets()) {
                tablet->SetTable(nullptr);
                YCHECK(tablet->GetState() == ETabletState::Unmounted);
                objectManager->UnrefObject(tablet);
            }

            table->Tablets().clear();
        }
    }

    void ReshardTable(
        TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex,
        int newTabletCount,
        const std::vector<TOwningKey>& pivotKeys)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(table->IsTrunk());

        if (!table->IsDynamic()) {
            THROW_ERROR_EXCEPTION("Cannot reshard a static table");
        }

        auto objectManager = Bootstrap_->GetObjectManager();
        auto chunkManager = Bootstrap_->GetChunkManager();

        ParseTabletRange(table, &firstTabletIndex, &lastTabletIndex); // may throw

        auto& tablets = table->Tablets();
        YCHECK(tablets.size() == table->GetChunkList()->Children().size());

        int oldTabletCount = lastTabletIndex - firstTabletIndex + 1;
        if (tablets.size() - oldTabletCount + newTabletCount > MaxTabletCount) {
            THROW_ERROR_EXCEPTION("Tablet count cannot exceed the limit of %v",
                MaxTabletCount);
        }

        if (table->IsSorted()) {
            if (pivotKeys.empty()) {
                THROW_ERROR_EXCEPTION("Table is sorted; must provide pivot keys");
            }

            if (pivotKeys[0] != tablets[firstTabletIndex]->GetPivotKey()) {
                THROW_ERROR_EXCEPTION(
                    "First pivot key must match that of the first tablet "
                        "in the resharded range");
            }

            if (lastTabletIndex != tablets.size() - 1) {
                if (pivotKeys.back() >= tablets[lastTabletIndex + 1]->GetPivotKey()) {
                    THROW_ERROR_EXCEPTION(
                        "Last pivot key must be strictly less than that of the tablet "
                            "which follows the resharded range");
                }
            }

            for (int index = 0; index < static_cast<int>(pivotKeys.size()) - 1; ++index) {
                if (pivotKeys[index] >= pivotKeys[index + 1]) {
                    THROW_ERROR_EXCEPTION("Pivot keys must be strictly increasing");
                }
            }

            // Validate pivot keys against table schema.
            for (const auto& pivotKey : pivotKeys) {
                ValidatePivotKey(pivotKey, table->TableSchema());
            }
        } else {
            if (!pivotKeys.empty()) {
                THROW_ERROR_EXCEPTION("Table is sorted; must provide tablet count");
            }
        }

        // Validate that all tablets are unmounted.
        if (table->GetTabletState() != ETabletState::Unmounted) {
            THROW_ERROR_EXCEPTION("Cannot reshard table since not all of its tablets are in %Qlv state",
                ETabletState::Unmounted);
        }

        // For ordered tablets, if the number of tablets decreases then validate that the trailing ones
        // (which are about to drop) are properly trimmed.
        if (newTabletCount < oldTabletCount) {
            for (int index = firstTabletIndex + newTabletCount; index < firstTabletIndex + oldTabletCount; ++index) {
                auto* tablet = table->Tablets()[index];
                if (tablet->GetTrimmedRowCount() != tablet->GetTrimmedStoresRowCount()) {
                    THROW_ERROR_EXCEPTION("Some chunks of tablet %v are not fully trimmed; such a tablet cannot "
                        "participate in resharding",
                        tablet->GetId());
                }
            }
        }

        // Create new tablets.
        std::vector<TTablet*> newTablets;
        for (int index = 0; index < newTabletCount; ++index) {
            auto* newTablet = CreateTablet(table);
            auto* oldTablet = index < oldTabletCount ? tablets[index + firstTabletIndex] : nullptr;
            if (table->IsSorted()) {
                newTablet->SetPivotKey(pivotKeys[index]);
            } else if (oldTablet) {
                newTablet->SetTrimmedRowCount(oldTablet->GetTrimmedRowCount());
                newTablet->SetTrimmedStoresRowCount(oldTablet->GetTrimmedStoresRowCount());
            }
            newTablets.push_back(newTablet);
        }

        // Drop old tablets.
        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto* tablet = tablets[index];
            objectManager->UnrefObject(tablet);
        }

        // NB: Evaluation order is important here, consider the case lastTabletIndex == -1.
        tablets.erase(tablets.begin() + firstTabletIndex, tablets.begin() + (lastTabletIndex + 1));
        tablets.insert(tablets.begin() + firstTabletIndex, newTablets.begin(), newTablets.end());

        // Update all indexes.
        for (int index = 0; index < static_cast<int>(tablets.size()); ++index) {
            auto* tablet = tablets[index];
            tablet->SetIndex(index);
        }

        // Copy chunk tree if somebody holds a reference.
        CopyChunkListIfShared(table, firstTabletIndex, lastTabletIndex);

        auto* oldRootChunkList = table->GetChunkList();
        const auto& oldTabletChunkTrees = oldRootChunkList->Children();

        auto* newRootChunkList = chunkManager->CreateChunkList(false);
        const auto& newTabletChunkTrees = newRootChunkList->Children();

        // Update tablet chunk lists.
        chunkManager->AttachToChunkList(
            newRootChunkList,
            oldTabletChunkTrees.data(),
            oldTabletChunkTrees.data() + firstTabletIndex);
        for (int index = 0; index < newTabletCount; ++index) {
            auto* tabletChunkList = chunkManager->CreateChunkList(!table->IsSorted());
            chunkManager->AttachToChunkList(newRootChunkList, tabletChunkList);
        }
        chunkManager->AttachToChunkList(
            newRootChunkList,
            oldTabletChunkTrees.data() + lastTabletIndex + 1,
            oldTabletChunkTrees.data() + oldTabletChunkTrees.size());

        auto enumerateChunks = [&] (int firstTabletIndex, int lastTabletIndex) {
            std::vector<TChunk*> chunks;
            for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
                EnumerateChunksInChunkTree(oldTabletChunkTrees[index]->AsChunkList(), &chunks);
            }
            return chunks;
        };

        if (table->IsSorted()) {
            // Move chunks from the resharded tablets to appropriate chunk lists.
            auto chunks = enumerateChunks(firstTabletIndex, lastTabletIndex);
            std::sort(chunks.begin(), chunks.end(), TObjectRefComparer::Compare);
            chunks.erase(
                std::unique(chunks.begin(), chunks.end()),
                chunks.end());

            int keyColumnCount = table->TableSchema().GetKeyColumnCount();
            for (auto* chunk : chunks) {
                auto boundaryKeysExt = GetProtoExtension<TBoundaryKeysExt>(chunk->ChunkMeta().extensions());
                auto minKey = WidenKey(FromProto<TOwningKey>(boundaryKeysExt.min()), keyColumnCount);
                auto maxKey = WidenKey(FromProto<TOwningKey>(boundaryKeysExt.max()), keyColumnCount);
                auto range = GetIntersectingTablets(newTablets, minKey, maxKey);
                for (auto it = range.first; it != range.second; ++it) {
                    auto* tablet = *it;
                    chunkManager->AttachToChunkList(
                        newTabletChunkTrees[tablet->GetIndex()]->AsChunkList(),
                        chunk);
                }
            }
        } else {
            // If the number of tablets increases, just leave the new trailing ones empty.
            // If the number of tablets decreases, merge the original trailing ones.
            for (int index = firstTabletIndex; index < firstTabletIndex + std::min(oldTabletCount, newTabletCount); ++index) {
                auto chunks = enumerateChunks(
                    index,
                    index == firstTabletIndex + newTabletCount - 1 ? lastTabletIndex : index);
                auto* chunkList = newTabletChunkTrees[index]->AsChunkList();
                for (auto* chunk : chunks) {
                    chunkManager->AttachToChunkList(chunkList, chunk);
                }
            }
        }

        // Replace root chunk list.
        table->SetChunkList(newRootChunkList);
        newRootChunkList->AddOwningNode(table);
        objectManager->RefObject(newRootChunkList);
        oldRootChunkList->RemoveOwningNode(table);
        objectManager->UnrefObject(oldRootChunkList);

        table->SnapshotStatistics() = table->GetChunkList()->Statistics().ToDataStatistics();
    }

    TCloneTableDataPtr BeginCloneTable(
        TTableNode* sourceTable,
        TTableNode* clonedTable,
        ENodeCloneMode mode)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(!sourceTable->Tablets().empty());
        YCHECK(clonedTable->Tablets().empty());

        try {
            auto tabletState = sourceTable->GetTabletState();
            switch (mode) {
                case ENodeCloneMode::Copy:
                    if (!sourceTable->IsSorted()) {
                        THROW_ERROR_EXCEPTION("Cannot copy dynamic ordered table");
                    }
                    if (tabletState != ETabletState::Unmounted && tabletState != ETabletState::Frozen) {
                        THROW_ERROR_EXCEPTION("Cannot copy dynamic table since not all of its tablets are in %Qlv or %Qlv mode",
                            ETabletState::Unmounted,
                            ETabletState::Frozen);
                    }
                    break;

                case ENodeCloneMode::Move:
                    if (tabletState != ETabletState::Unmounted) {
                        THROW_ERROR_EXCEPTION("Cannot move dynamic table since not all of its tablets are in %Qlv state",
                            ETabletState::Unmounted);
                    }
                    break;

                default:
                    YUNREACHABLE();
            }
        } catch (const std::exception& ex) {
            auto cypressManager = Bootstrap_->GetCypressManager();
            auto sourceTableProxy = cypressManager->GetNodeProxy(sourceTable->GetTrunkNode(), sourceTable->GetTransaction());
            THROW_ERROR_EXCEPTION("Error cloning table %v",
                sourceTableProxy->GetPath());
        }

        auto data = New<TCloneTableData>();
        data->Mode = mode;

        switch (data->Mode) {
            case ENodeCloneMode::Move:
                data->Tablets.swap(sourceTable->Tablets());
                break;

            case ENodeCloneMode::Copy:
                break;

            default:
                YUNREACHABLE();
        }

        return data;
    }

    void CommitCloneTable(
        TTableNode* sourceTable,
        TTableNode* clonedTable,
        TCloneTableDataPtr data)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(clonedTable->Tablets().empty());

        switch (data->Mode) {
            case ENodeCloneMode::Move:
                YCHECK(sourceTable->Tablets().empty());
                data->Tablets.swap(clonedTable->Tablets());
                for (auto* tablet : clonedTable->Tablets()) {
                    tablet->SetTable(clonedTable);
                }
                break;

            case ENodeCloneMode::Copy: {
                // Undo the harm done in TChunkOwnerTypeHandler::DoClone.
                auto* fakeClonedRootChunkList = clonedTable->GetChunkList();
                fakeClonedRootChunkList->RemoveOwningNode(clonedTable);
                auto objectManager = Bootstrap_->GetObjectManager();
                objectManager->UnrefObject(fakeClonedRootChunkList);

                const auto& sourceTablets = sourceTable->Tablets();
                YCHECK(!sourceTablets.empty());
                auto& clonedTablets = clonedTable->Tablets();
                YCHECK(clonedTablets.empty());

                auto chunkManager = Bootstrap_->GetChunkManager();
                auto* clonedRootChunkList = chunkManager->CreateChunkList(false);
                clonedTable->SetChunkList(clonedRootChunkList);
                objectManager->RefObject(clonedRootChunkList);
                clonedRootChunkList->AddOwningNode(clonedTable);

                clonedTablets.reserve(sourceTablets.size());
                auto* sourceRootChunkList = sourceTable->GetChunkList();
                YCHECK(sourceRootChunkList->Children().size() == sourceTablets.size());
                for (int index = 0; index < static_cast<int>(sourceTablets.size()); ++index) {
                    const auto* sourceTablet = sourceTablets[index];

                    auto* clonedTablet = CreateTablet(clonedTable);
                    clonedTablet->CopyFrom(*sourceTablet);

                    auto* tabletChunkList = sourceRootChunkList->Children()[index];
                    chunkManager->AttachToChunkList(clonedRootChunkList, tabletChunkList);

                    clonedTablets.push_back(clonedTablet);
                }
                break;
            }

            default:
                YUNREACHABLE();
        }
    }

    void RollbackCloneTable(
        TTableNode* sourceTable,
        TTableNode* clonedTable,
        TCloneTableDataPtr data)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(sourceTable->Tablets().empty());
        YCHECK(clonedTable->Tablets().empty());

        switch (data->Mode) {
            case ENodeCloneMode::Move:
                data->Tablets.swap(sourceTable->Tablets());
                break;

            case ENodeCloneMode::Copy:
                break;

            default:
                YUNREACHABLE();
        }
    }

    void MakeTableDynamic(TTableNode* table)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(table->IsTrunk());

        if (table->IsDynamic()) {
            return;
        }

        auto* oldRootChunkList = table->GetChunkList();

        if (table->IsSorted() && !oldRootChunkList->Children().empty()) {
            THROW_ERROR_EXCEPTION("Cannot switch a static non-empty sorted table into dynamic mode");
        }

        std::vector<TChunk*> chunks;
        EnumerateChunksInChunkTree(oldRootChunkList, &chunks);

        // Check for duplicates.
        // Compute last commit timestamp.
        yhash_set<TChunk*> chunkSet;
        chunkSet.resize(chunks.size());
        auto lastCommitTimestamp = NTransactionClient::MinTimestamp;
        for (auto* chunk : chunks) {
            if (!chunkSet.insert(chunk).second) {
                THROW_ERROR_EXCEPTION("Cannot switch table into dynamic mode since it contains duplicate chunk %v",
                    chunk->GetId());
            }

            const auto& miscExt = chunk->MiscExt();
            if (miscExt.has_max_timestamp()) {
                lastCommitTimestamp = std::max(lastCommitTimestamp, static_cast<TTimestamp>(miscExt.max_timestamp()));
            }
        }
        table->SetLastCommitTimestamp(lastCommitTimestamp);

        auto chunkManager = Bootstrap_->GetChunkManager();
        auto* newRootChunkList = chunkManager->CreateChunkList(false);

        auto objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(newRootChunkList);

        table->SetChunkList(newRootChunkList);
        newRootChunkList->AddOwningNode(table);

        auto* tablet = CreateTablet(table);
        tablet->SetIndex(0);
        if (table->IsSorted()) {
            tablet->SetPivotKey(EmptyKey());
        }
        table->Tablets().push_back(tablet);

        auto* tabletChunkList = chunkManager->CreateChunkList(!table->IsSorted());
        chunkManager->AttachToChunkList(newRootChunkList, tabletChunkList);

        std::vector<TChunkTree*> chunkTrees(chunks.begin(), chunks.end());
        chunkManager->AttachToChunkList(tabletChunkList, chunkTrees);

        oldRootChunkList->RemoveOwningNode(table);
        objectManager->UnrefObject(oldRootChunkList);

        LOG_DEBUG_UNLESS(IsRecovery(), "Table is switched to dynamic mode (TableId: %v)",
            table->GetId());
    }

    void MakeTableStatic(TTableNode* table)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YCHECK(table->IsTrunk());

        if (!table->IsDynamic()) {
            return;
        }

        if (table->IsSorted()) {
            THROW_ERROR_EXCEPTION("Cannot switch sorted static table to dynamic mode");
        }

        if (table->GetTabletState() != ETabletState::Unmounted) {
            THROW_ERROR_EXCEPTION("Cannot switch dynamic table to static mode since not all of its tablets are in %Qlv state",
                ETabletState::Unmounted);
        }

        auto* oldRootChunkList = table->GetChunkList();

        auto chunkManager = Bootstrap_->GetChunkManager();
        auto newRootChunkList = chunkManager->CreateChunkList();

        auto objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(newRootChunkList);

        table->SetChunkList(newRootChunkList);
        newRootChunkList->AddOwningNode(table);

        std::vector<TChunk*> chunks;
        EnumerateChunksInChunkTree(oldRootChunkList, &chunks);
        std::vector<TChunkTree*> chunkTrees(chunks.begin(), chunks.end());
        chunkManager->AttachToChunkList(newRootChunkList, chunkTrees);

        oldRootChunkList->RemoveOwningNode(table);
        objectManager->UnrefObject(oldRootChunkList);

        for (auto* tablet : table->Tablets()) {
            objectManager->UnrefObject(tablet);
        }
        table->Tablets().clear();

        table->SetLastCommitTimestamp(NullTimestamp);

        LOG_DEBUG_UNLESS(IsRecovery(), "Table is switched to static mode (TableId: %v)",
            table->GetId());
    }


    TTabletCell* GetTabletCellOrThrow(const TTabletCellId& id)
    {
        auto* cell = FindTabletCell(id);
        if (!IsObjectAlive(cell)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::ResolveError,
                "No such tablet cell %v",
                id);
        }
        return cell;
    }

    TTabletCellBundle* FindTabletCellBundleByName(const Stroka& name)
    {
        auto it = NameToTabletCellBundleMap_.find(name);
        return it == NameToTabletCellBundleMap_.end() ? nullptr : it->second;
    }

    TTabletCellBundle* GetTabletCellBundleByNameOrThrow(const Stroka& name)
    {
        auto* cellBundle = FindTabletCellBundleByName(name);
        if (!cellBundle) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::ResolveError,
                "No such tablet cell bundle %Qv",
                name);
        }
        return cellBundle;
    }

    void RenameTabletCellBundle(TTabletCellBundle* cellBundle, const Stroka& newName)
    {
        if (newName == cellBundle->GetName()) {
            return;
        }

        ValidateTabletCellBundleName(newName);

        if (FindTabletCellBundleByName(newName)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Tablet cell bundle %Qv already exists",
                newName);
        }

        YCHECK(NameToTabletCellBundleMap_.erase(cellBundle->GetName()) == 1);
        YCHECK(NameToTabletCellBundleMap_.insert(std::make_pair(newName, cellBundle)).second);
        cellBundle->SetName(newName);
    }

    TTabletCellBundle* GetDefaultTabletCellBundle()
    {
        return DefaultTabletCellBundle_;
    }

    void SetTabletCellBundle(TTableNode* table, TTabletCellBundle* cellBundle)
    {
        YCHECK(table->IsTrunk());

        if (table->GetTabletCellBundle() == cellBundle) {
            return;
        }

        auto securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ValidatePermission(cellBundle, EPermission::Use);

        auto objectManager = Bootstrap_->GetObjectManager();
        if (table->GetTabletCellBundle()) {
            objectManager->UnrefObject(table->GetTabletCellBundle());
        }
        objectManager->RefObject(cellBundle);

        table->SetTabletCellBundle(cellBundle);
    }

    void ResetTabletCellBundle(TTableNode* table)
    {
        YCHECK(table->IsTrunk());

    }


    DECLARE_ENTITY_MAP_ACCESSORS(TabletCellBundle, TTabletCellBundle);
    DECLARE_ENTITY_MAP_ACCESSORS(TabletCell, TTabletCell);
    DECLARE_ENTITY_MAP_ACCESSORS(Tablet, TTablet);

private:
    friend class TTabletCellBundleTypeHandler;
    friend class TTabletCellTypeHandler;
    friend class TTabletTypeHandler;

    const TTabletManagerConfigPtr Config_;

    const TTabletTrackerPtr TabletTracker_;

    TEntityMap<TTabletCellBundle> TabletCellBundleMap_;
    TEntityMap<TTabletCell> TabletCellMap_;
    TEntityMap<TTablet> TabletMap_;

    yhash_map<Stroka, TTabletCellBundle*> NameToTabletCellBundleMap_;

    yhash_multimap<Stroka, TTabletCell*> AddressToCell_;
    yhash_map<TTransaction*, TTabletCell*> TransactionToCellMap_;

    bool InitializeCellBundles_ = false;
    TTabletCellBundleId DefaultTabletCellBundleId_;
    TTabletCellBundle* DefaultTabletCellBundle_ = nullptr;

    bool UpdateChunkListsOrderedMode_ = false;

    TPeriodicExecutorPtr CleanupExecutor_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        TabletCellBundleMap_.SaveKeys(context);
        TabletCellMap_.SaveKeys(context);
        TabletMap_.SaveKeys(context);
    }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        TabletCellBundleMap_.SaveValues(context);
        TabletCellMap_.SaveValues(context);
        TabletMap_.SaveValues(context);
    }


    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (context.GetVersion() >= 202) {
            TabletCellBundleMap_.LoadKeys(context);
        }
        TabletCellMap_.LoadKeys(context);
        TabletMap_.LoadKeys(context);
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        // COMPAT(babenko): YT-4348
        if (context.GetVersion() >= 202) {
            TabletCellBundleMap_.LoadValues(context);
        }
        TabletCellMap_.LoadValues(context);
        TabletMap_.LoadValues(context);
        // COMPAT(babenko)
        InitializeCellBundles_ = (context.GetVersion() < 400);
        // COMPAT(babenko)
        UpdateChunkListsOrderedMode_ = (context.GetVersion() < 401);
    }


    virtual void OnAfterSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        InitBuiltins();

        NameToTabletCellBundleMap_.clear();
        for (const auto& pair : TabletCellBundleMap_) {
            auto* cellBundle = pair.second;
            YCHECK(NameToTabletCellBundleMap_.insert(std::make_pair(cellBundle->GetName(), cellBundle)).second);
        }

        AddressToCell_.clear();

        for (const auto& pair : TabletCellMap_) {
            auto* cell = pair.second;
            for (const auto& peer : cell->Peers()) {
                if (!peer.Descriptor.IsNull()) {
                    AddToAddressToCellMap(peer.Descriptor, cell);
                }
            }
            auto* transaction = cell->GetPrerequisiteTransaction();
            if (transaction) {
                YCHECK(TransactionToCellMap_.insert(std::make_pair(transaction, cell)).second);
            }
        }

        // COMPAT(babenko)
        if (InitializeCellBundles_) {
            auto cypressManager = Bootstrap_->GetCypressManager();
            for (const auto& pair : cypressManager->Nodes()) {
                auto* node = pair.second;
                if (node->IsTrunk() && node->GetType() == EObjectType::Table) {
                    auto* table = static_cast<TTableNode*>(node);
                    if (table->IsDynamic()) {
                        table->SetTabletCellBundle(DefaultTabletCellBundle_);
                        DefaultTabletCellBundle_->RefObject();
                    }
                }
            }

            for (const auto& pair : TabletCellMap_) {
                auto* cell = pair.second;
                cell->SetCellBundle(DefaultTabletCellBundle_);
                DefaultTabletCellBundle_->RefObject();
            }
        }

        // COMPAT(babenko)
        if (UpdateChunkListsOrderedMode_) {
            auto cypressManager = Bootstrap_->GetCypressManager();
            for (const auto& pair : cypressManager->Nodes()) {
                auto* node = pair.second;
                if (node->IsTrunk() && node->GetType() == EObjectType::Table) {
                    auto* table = static_cast<TTableNode*>(node);
                    if (table->IsDynamic()) {
                        auto* rootChunkList = table->GetChunkList();
                        YCHECK(rootChunkList->GetOrdered());
                        rootChunkList->SetOrdered(false);
                        for (auto* child : rootChunkList->Children()) {
                            auto* tabletChunkList = child->AsChunkList();
                            YCHECK(tabletChunkList->GetOrdered());
                            tabletChunkList->SetOrdered(false);
                        }
                    }
                }
            }
        }
    }

    virtual void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::Clear();

        TabletCellBundleMap_.Clear();
        TabletCellMap_.Clear();
        TabletMap_.Clear();
        NameToTabletCellBundleMap_.clear();
        AddressToCell_.clear();
        TransactionToCellMap_.clear();

        InitBuiltins();
    }

    void InitBuiltins()
    {
        DefaultTabletCellBundle_ = FindTabletCellBundle(DefaultTabletCellBundleId_);
        if (!DefaultTabletCellBundle_) {
            DefaultTabletCellBundle_ = DoCreateTabletCellBundle(
                DefaultTabletCellBundleId_,
                DefaultTabletCellBundleName);

            auto securityManager = Bootstrap_->GetSecurityManager();
            DefaultTabletCellBundle_->Acd().AddEntry(TAccessControlEntry(
                ESecurityAction::Allow,
                securityManager->GetUsersGroup(),
                EPermission::Use));
        }
    }


    void OnNodeRegistered(TNode* node)
    {
        node->InitTabletSlots();
    }

    void OnNodeUnregistered(TNode* node)
    {
        for (const auto& slot : node->TabletSlots()) {
            auto* cell = slot.Cell;
            if (cell) {
                LOG_INFO_UNLESS(IsRecovery(), "Tablet cell peer offline: node unregistered (Address: %v, CellId: %v, PeerId: %v)",
                    node->GetDefaultAddress(),
                    cell->GetId(),
                    slot.PeerId);
                cell->DetachPeer(node);
            }
        }
        node->ClearTabletSlots();
    }

    void OnIncrementalHeartbeat(
        TNode* node,
        TReqIncrementalHeartbeat* request,
        TRspIncrementalHeartbeat* response)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        // Various request helpers.
        auto requestCreateSlot = [&] (const TTabletCell* cell) {
            if (!response)
                return;

            if (!cell->GetPrerequisiteTransaction())
                return;

            auto* protoInfo = response->add_tablet_slots_to_create();

            const auto& cellId = cell->GetId();
            auto peerId = cell->GetPeerId(node->GetDefaultAddress());

            ToProto(protoInfo->mutable_cell_id(), cell->GetId());
            protoInfo->set_peer_id(peerId);

            const auto* cellBundle = cell->GetCellBundle();
            protoInfo->set_options(ConvertToYsonString(cellBundle->GetOptions()).Data());

            LOG_INFO_UNLESS(IsRecovery(), "Tablet slot creation requested (Address: %v, CellId: %v, PeerId: %v)",
                node->GetDefaultAddress(),
                cellId,
                peerId);
        };

        auto requestConfigureSlot = [&] (const TNode::TTabletSlot* slot) {
            if (!response)
                return;

            const auto* cell = slot->Cell;
            if (!cell->GetPrerequisiteTransaction())
                return;

            auto* protoInfo = response->add_tablet_slots_configure();

            const auto& cellId = cell->GetId();
            auto cellDescriptor = cell->GetDescriptor();

            const auto& prerequisiteTransactionId = cell->GetPrerequisiteTransaction()->GetId();

            ToProto(protoInfo->mutable_cell_descriptor(), cellDescriptor);
            ToProto(protoInfo->mutable_prerequisite_transaction_id(), prerequisiteTransactionId);

            LOG_INFO_UNLESS(IsRecovery(), "Tablet slot configuration update requested "
                "(Address: %v, CellId: %v, Version: %v, PrerequisiteTransactionId: %v)",
                node->GetDefaultAddress(),
                cellId,
                cellDescriptor.ConfigVersion,
                prerequisiteTransactionId);
        };

        auto requestRemoveSlot = [&] (const TTabletCellId& cellId) {
            if (!response)
                return;

            auto* protoInfo = response->add_tablet_slots_to_remove();
            ToProto(protoInfo->mutable_cell_id(), cellId);

            LOG_INFO_UNLESS(IsRecovery(), "Tablet slot removal requested (Address: %v, CellId: %v)",
                node->GetDefaultAddress(),
                cellId);
        };

        const auto* mutationContext = GetCurrentMutationContext();
        auto mutationTimestamp = mutationContext->GetTimestamp();

        const auto& address = node->GetDefaultAddress();

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
        for (int slotIndex = 0; slotIndex < request->tablet_slots_size(); ++slotIndex) {
            // Pre-erase slot.
            auto& slot = node->TabletSlots()[slotIndex];
            slot = TNode::TTabletSlot();

            const auto& slotInfo = request->tablet_slots(slotIndex);

            auto state = EPeerState(slotInfo.peer_state());
            if (state == EPeerState::None)
                continue;

            auto cellInfo = FromProto<TCellInfo>(slotInfo.cell_info());
            const auto& cellId = cellInfo.CellId;
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

            auto expectedIt = expectedCells.find(cell);
            if (expectedIt == expectedCells.end()) {
                cell->AttachPeer(node, peerId);
                LOG_INFO_UNLESS(IsRecovery(), "Tablet cell peer online (Address: %v, CellId: %v, PeerId: %v)",
                    address,
                    cellId,
                    peerId);
            }

            cell->UpdatePeerSeenTime(peerId, mutationTimestamp);
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
                cellInfo.ConfigVersion);

            if (cellInfo.ConfigVersion != slot.Cell->GetConfigVersion()) {
                requestConfigureSlot(&slot);
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

        // Copy tablet statistics, update performance counters.
        auto now = TInstant::Now();
        for (auto& tabletInfo : request->tablets()) {
            auto tabletId = FromProto<TTabletId>(tabletInfo.tablet_id());
            auto* tablet = FindTablet(tabletId);
            if (!tablet || tablet->GetState() != ETabletState::Mounted)
                continue;

            auto* cell = tablet->GetCell();
            cell->TotalStatistics() -= GetTabletStatistics(tablet);
            tablet->NodeStatistics() = tabletInfo.statistics();
            cell->TotalStatistics() += GetTabletStatistics(tablet);

            auto updatePerformanceCounter = [&] (TTabletPerformanceCounter* counter, i64 curValue) {
                i64 prevValue = counter->Count;
                auto timeDelta = std::max(1.0, (now - tablet->PerformanceCounters().Timestamp).SecondsFloat());
                counter->Rate = (std::max(curValue, prevValue) - prevValue) / timeDelta;
                counter->Count = curValue;
            };

            #define XX(name, Name) updatePerformanceCounter( \
                &tablet->PerformanceCounters().Name, \
                tabletInfo.performance_counters().name ## _count());
            ITERATE_TABLET_PERFORMANCE_COUNTERS(XX)
            #undef XX
            tablet->PerformanceCounters().Timestamp = now;
        }
    }


    void AddToAddressToCellMap(const TNodeDescriptor& descriptor, TTabletCell* cell)
    {
        AddressToCell_.insert(std::make_pair(descriptor.GetDefaultAddress(), cell));
    }

    void RemoveFromAddressToCellMap(const TNodeDescriptor& descriptor, TTabletCell* cell)
    {
        auto range = AddressToCell_.equal_range(descriptor.GetDefaultAddress());
        for (auto it = range.first; it != range.second; ++it) {
            if (it->second == cell) {
                AddressToCell_.erase(it);
                break;
            }
        }
    }


    void HydraAssignPeers(TReqAssignPeers* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto cellId = FromProto<TTabletCellId>(request->cell_id());
        auto* cell = FindTabletCell(cellId);
        if (!IsObjectAlive(cell))
            return;

        const auto* mutationContext = GetCurrentMutationContext();
        auto mutationTimestamp = mutationContext->GetTimestamp();

        bool leadingPeerAssigned = false;
        for (const auto& peerInfo : request->peer_infos()) {
            auto peerId = peerInfo.peer_id();
            auto descriptor = FromProto<TNodeDescriptor>(peerInfo.node_descriptor());

            auto& peer = cell->Peers()[peerId];
            if (!peer.Descriptor.IsNull())
                continue;

            if (peerId == cell->GetLeadingPeerId()) {
                leadingPeerAssigned = true;
            }

            AddToAddressToCellMap(descriptor, cell);
            cell->AssignPeer(descriptor, peerId);
            cell->UpdatePeerSeenTime(peerId, mutationTimestamp);

            LOG_INFO_UNLESS(IsRecovery(), "Tablet cell peer assigned (CellId: %v, Address: %v, PeerId: %v)",
                cellId,
                descriptor.GetDefaultAddress(),
                peerId);
        }

        // Once a peer is assigned, we must ensure that the cell has a valid prerequisite transaction.
        if (leadingPeerAssigned || !cell->GetPrerequisiteTransaction()) {
            RestartPrerequisiteTransaction(cell);
        }

        ReconfigureCell(cell);
    }

    void HydraRevokePeers(TReqRevokePeers* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto cellId = FromProto<TTabletCellId>(request->cell_id());
        auto* cell = FindTabletCell(cellId);
        if (!IsObjectAlive(cell))
            return;

        bool leadingPeerRevoked = false;
        for (auto peerId : request->peer_ids()) {
            if (peerId == cell->GetLeadingPeerId()) {
                leadingPeerRevoked = true;
            }
            DoRevokePeer(cell, peerId);
        }

        if (leadingPeerRevoked) {
            AbortPrerequisiteTransaction(cell);
            AbortCellSubtreeTransactions(cell);
        }
        ReconfigureCell(cell);
    }

    void HydraSetLeadingPeer(TReqSetLeadingPeer* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto cellId = FromProto<TTabletCellId>(request->cell_id());
        auto* cell = FindTabletCell(cellId);
        if (!IsObjectAlive(cell)) {
            return;
        }

        auto peerId = request->peer_id();
        cell->SetLeadingPeerId(peerId);

        const auto& descriptor = cell->Peers()[peerId].Descriptor;
        LOG_INFO_UNLESS(IsRecovery(), "Tablet cell leading peer updated (CellId: %v, Address: %v, PeerId: %v)",
            cellId,
            descriptor.GetDefaultAddress(),
            peerId);

        RestartPrerequisiteTransaction(cell);
        ReconfigureCell(cell);
    }

    void HydraOnTabletMounted(TRspMountTablet* response)
    {
        auto tabletId = FromProto<TTabletId>(response->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet)) {
            return;
        }

        auto state = tablet->GetState();
        if (state != ETabletState::Mounting) {
            LOG_INFO_UNLESS(IsRecovery(), "Mounted notification received for a tablet in %Qlv state, ignored (TabletId: %v)",
                state,
                tabletId);
            return;
        }

        bool frozen = response->frozen();
        auto* table = tablet->GetTable();
        auto* cell = tablet->GetCell();

        LOG_INFO_UNLESS(IsRecovery(), "Tablet mounted (TableId: %v, TabletId: %v, MountRevision: %v, CellId: %v, Frozen: %v)",
            table->GetId(),
            tablet->GetId(),
            tablet->GetMountRevision(),
            cell->GetId(),
            frozen);

        cell->TotalStatistics() += GetTabletStatistics(tablet);

        tablet->SetState(frozen ? ETabletState::Frozen : ETabletState::Mounted);
    }

    void HydraOnTabletUnmounted(TRspUnmountTablet* response)
    {
        auto tabletId = FromProto<TTabletId>(response->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet)) {
            return;
        }

        auto state = tablet->GetState();
        if (state != ETabletState::Unmounting) {
            LOG_INFO_UNLESS(IsRecovery(), "Unmounted notification received for a tablet in %Qlv state, ignored (TabletId: %v)",
                state,
                tabletId);
            return;
        }

        DoTabletUnmounted(tablet);
    }

    void HydraOnTabletFrozen(TRspFreezeTablet* response)
    {
        auto tabletId = FromProto<TTabletId>(response->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet)) {
            return;
        }

        const auto* table = tablet->GetTable();
        const auto* cell = tablet->GetCell();

        auto state = tablet->GetState();
        if (state != ETabletState::Freezing) {
            LOG_INFO_UNLESS(IsRecovery(), "Frozen notification received for a tablet in %Qlv state, ignored (TabletId: %v)",
                state,
                tabletId);
            return;
        }

        LOG_INFO_UNLESS(IsRecovery(), "Tablet frozen (TableId: %v, TabletId: %v, CellId: %v)",
            table->GetId(),
            tablet->GetId(),
            cell->GetId());

        tablet->SetState(ETabletState::Frozen);
    }

    void HydraOnTabletUnfrozen(TRspUnfreezeTablet* response)
    {
        auto tabletId = FromProto<TTabletId>(response->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet)) {
            return;
        }

        const auto* table = tablet->GetTable();
        const auto* cell = tablet->GetCell();

        auto state = tablet->GetState();
        if (state != ETabletState::Unfreezing) {
            LOG_INFO_UNLESS(IsRecovery(), "Unfrozen notification received for a tablet in %Qlv state, ignored (TabletId: %v)",
                state,
                tabletId);
            return;
        }

        LOG_INFO_UNLESS(IsRecovery(), "Tablet unfrozen (TableId: %v, TabletId: %v, CellId: %v)",
            table->GetId(),
            tablet->GetId(),
            cell->GetId());

        tablet->SetState(ETabletState::Mounted);
    }

    void DoTabletUnmounted(TTablet* tablet)
    {
        auto* table = tablet->GetTable();
        auto* cell = tablet->GetCell();

        LOG_INFO_UNLESS(IsRecovery(), "Tablet unmounted (TableId: %v, TabletId: %v, CellId: %v)",
            table->GetId(),
            tablet->GetId(),
            cell->GetId());

        cell->TotalStatistics() -= GetTabletStatistics(tablet);

        tablet->NodeStatistics().Clear();
        tablet->PerformanceCounters() = TTabletPerformanceCounters();
        tablet->SetInMemoryMode(EInMemoryMode::None);
        tablet->SetState(ETabletState::Unmounted);
        tablet->SetCell(nullptr);

        auto objectManager = Bootstrap_->GetObjectManager();
        YCHECK(cell->Tablets().erase(tablet) == 1);
        objectManager->UnrefObject(cell);
    }

    void CopyChunkListIfShared(
        TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex)
    {
        auto* oldRootChunkList = table->GetChunkList();
        auto& chunkLists = oldRootChunkList->Children();
        auto chunkManager = Bootstrap_->GetChunkManager();
        auto objectManager = Bootstrap_->GetObjectManager();

        if (oldRootChunkList->GetObjectRefCounter() > 1) {
            auto statistics = oldRootChunkList->Statistics();
            auto* newRootChunkList = chunkManager->CreateChunkList(false);

            chunkManager->AttachToChunkList(
                newRootChunkList,
                chunkLists.data(),
                chunkLists.data() + firstTabletIndex);

            for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
                auto* tabletChunkList = chunkLists[index]->AsChunkList();
                auto* newTabletChunkList = chunkManager->CreateChunkList(!table->IsSorted());
                chunkManager->AttachToChunkList(newTabletChunkList, tabletChunkList->Children());
                chunkManager->AttachToChunkList(newRootChunkList, newTabletChunkList);
            }

            chunkManager->AttachToChunkList(
                newRootChunkList,
                chunkLists.data() + lastTabletIndex + 1,
                chunkLists.data() + chunkLists.size());

            // Replace root chunk list.
            table->SetChunkList(newRootChunkList);
            newRootChunkList->AddOwningNode(table);
            objectManager->RefObject(newRootChunkList);
            oldRootChunkList->RemoveOwningNode(table);
            objectManager->UnrefObject(oldRootChunkList);
            YCHECK(newRootChunkList->Statistics() == statistics);
        } else {
            auto statistics = oldRootChunkList->Statistics();

            for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
                auto* tabletChunkList = chunkLists[index]->AsChunkList();
                if (tabletChunkList->GetObjectRefCounter() > 1) {
                    auto* newTabletChunkList = chunkManager->CreateChunkList(!table->IsSorted());
                    chunkManager->AttachToChunkList(newTabletChunkList, tabletChunkList->Children());
                    chunkLists[index] = newTabletChunkList;

                    // TODO(savrus): make a helper to replace a tablet chunk list.
                    newTabletChunkList->AddParent(oldRootChunkList);
                    objectManager->RefObject(newTabletChunkList);
                    tabletChunkList->RemoveParent(oldRootChunkList);
                    objectManager->UnrefObject(tabletChunkList);
                }
            }

            YCHECK(oldRootChunkList->Statistics() == statistics);
        }
    }

    void HydraUpdateTabletStores(TReqUpdateTabletStores* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet)) {
            return;
        }

        auto mountRevision = request->mount_revision();

        auto* cell = tablet->GetCell();
        auto* table = tablet->GetTable();
        if (!IsObjectAlive(table)) {
            return;
        }

        auto cypressManager = Bootstrap_->GetCypressManager();
        cypressManager->SetModified(table, nullptr);

        TRspUpdateTabletStores response;
        response.mutable_tablet_id()->MergeFrom(request->tablet_id());
        // NB: Take mount revision from the request, not from the tablet.
        response.set_mount_revision(mountRevision);
        response.mutable_stores_to_add()->MergeFrom(request->stores_to_add());
        response.mutable_stores_to_remove()->MergeFrom(request->stores_to_remove());

        try {
            tablet->ValidateMountRevision(mountRevision);

            auto state = tablet->GetState();
            if (state != ETabletState::Mounted &&
                state != ETabletState::Unmounting &&
                state != ETabletState::Freezing)
            {
                THROW_ERROR_EXCEPTION("Cannot update stores while tablet is in %Qlv state",
                    state);
            }

            if (!table->IsSorted()) {
                auto* rootChunkList = table->GetChunkList();
                auto* tabletChunkList = rootChunkList->Children()[tablet->GetIndex()]->AsChunkList();

                if (request->stores_to_add_size() > 0) {
                    if (request->stores_to_add_size() > 1) {
                        THROW_ERROR_EXCEPTION("Cannot attach more than one store to an ordered table at once");
                    }

                    const auto& descriptor = request->stores_to_add(0);
                    auto storeId = FromProto<TStoreId>(descriptor.store_id());
                    YCHECK(descriptor.has_starting_row_index());
                    if (tabletChunkList->Statistics().RowCount != descriptor.starting_row_index()) {
                        THROW_ERROR_EXCEPTION("Attempted to attach store %v with invalid starting row index: expected %v, got %v",
                            storeId,
                            tabletChunkList->Statistics().RowCount,
                            descriptor.starting_row_index());
                    }
                }

                if (request->stores_to_remove_size() > 0) {
                    int childIndex = tabletChunkList->GetTrimmedChildCount();
                    const auto& children = tabletChunkList->Children();
                    for (const auto& descriptor : request->stores_to_remove()) {
                        auto storeId = FromProto<TStoreId>(descriptor.store_id());
                        if (TypeFromId(storeId) == EObjectType::OrderedDynamicTabletStore) {
                            continue;
                        }

                        if (childIndex >= children.size()) {
                            THROW_ERROR_EXCEPTION("Attempted to trim store %v which is not part of the tablet",
                                storeId);
                        }
                        if (children[childIndex]->GetId() != storeId) {
                            THROW_ERROR_EXCEPTION("Attempted to trim store %v while store %v is expected",
                                storeId,
                                children[childIndex]->GetId());
                        }
                        ++childIndex;
                    }
                }
            }

            // Collect all changes first.
            auto chunkManager = Bootstrap_->GetChunkManager();
            std::vector<TChunkTree*> chunksToAttach;
            i64 attachedRowCount = 0;
            auto lastCommitTimestamp = table->GetLastCommitTimestamp();
            for (const auto& descriptor : request->stores_to_add()) {
                auto storeId = FromProto<TStoreId>(descriptor.store_id());
                if (TypeFromId(storeId) == EObjectType::Chunk ||
                    TypeFromId(storeId) == EObjectType::ErasureChunk)
                {
                    auto* chunk = chunkManager->GetChunkOrThrow(storeId);
                    const auto& miscExt = chunk->MiscExt();
                    if (miscExt.has_max_timestamp()) {
                        lastCommitTimestamp = std::max(lastCommitTimestamp, static_cast<TTimestamp>(miscExt.max_timestamp()));
                    }
                    attachedRowCount += miscExt.row_count();
                    chunksToAttach.push_back(chunk);
                }
            }

            std::vector<TChunkTree*> chunksToDetach;
            i64 detachedRowCount = 0;
            for (const auto& descriptor : request->stores_to_remove()) {
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

            // Update last commit timestamp.
            table->SetLastCommitTimestamp(lastCommitTimestamp);

            // Copy chunk tree if somebody holds a reference.
            CopyChunkListIfShared(table, tablet->GetIndex(), tablet->GetIndex());

            // Apply all requested changes.
            cell->TotalStatistics() -= GetTabletStatistics(tablet);
            auto* rootChunkList = table->GetChunkList();
            auto* tabletChunkList = rootChunkList->Children()[tablet->GetIndex()]->AsChunkList();
            chunkManager->AttachToChunkList(tabletChunkList, chunksToAttach);
            chunkManager->DetachFromChunkList(tabletChunkList, chunksToDetach);
            cell->TotalStatistics() += GetTabletStatistics(tablet);
            table->SnapshotStatistics() = table->GetChunkList()->Statistics().ToDataStatistics();

            // Unstage just attached chunks.
            // Update table resource usage.
            for (auto* chunk : chunksToAttach) {
                chunkManager->UnstageChunk(chunk->AsChunk());
            }

            if (!table->IsSorted()) {
                tablet->SetFlushedRowCount(tablet->GetFlushedRowCount() + attachedRowCount);
                tablet->SetTrimmedStoresRowCount(tablet->GetTrimmedStoresRowCount() + detachedRowCount);
            }

            auto securityManager = Bootstrap_->GetSecurityManager();
            securityManager->UpdateAccountNodeUsage(table);

            LOG_DEBUG_UNLESS(IsRecovery(), "Tablet stores updated (TableId: %v, TabletId: %v, "
                "AttachedChunkIds: %v, DetachedChunkIds: %v, "
                "AttachedRowCount: %v, DetachedRowCount: %v)",
                table->GetId(),
                tabletId,
                MakeFormattableRange(chunksToAttach, TObjectIdFormatter()),
                MakeFormattableRange(chunksToDetach, TObjectIdFormatter()),
                attachedRowCount,
                detachedRowCount);
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            LOG_WARNING_UNLESS(IsRecovery(), error, "Error updating tablet stores (TabletId: %v)",
                tabletId);
            ToProto(response.mutable_error(), error.Sanitize());
        }

        auto hiveManager = Bootstrap_->GetHiveManager();
        auto* mailbox = hiveManager->GetMailbox(cell->GetId());
        hiveManager->PostMessage(mailbox, response);
    }

    void HydraUpdateTabletTrimmedRowCount(TReqUpdateTabletTrimmedRowCount* request)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto* tablet = FindTablet(tabletId);
        if (!IsObjectAlive(tablet)) {
            return;
        }

        auto mountRevision = request->mount_revision();
        if (tablet->GetMountRevision() != mountRevision) {
            return;
        }

        auto trimmedRowCount = request->trimmed_row_count();

        tablet->SetTrimmedRowCount(trimmedRowCount);

        LOG_DEBUG_UNLESS(IsRecovery(), "Tablet trimmed row count updated (TabletId: %v, TrimmedRowCount: %v)",
            tabletId,
            trimmedRowCount);
    }

    virtual void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnLeaderActive();

        if (Bootstrap_->IsPrimaryMaster()) {
            TabletTracker_->Start();
        }

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

        TMasterAutomatonPart::OnStopLeading();

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
        for (const auto& peer : cell->Peers()) {
            auto nodeTracker = Bootstrap_->GetNodeTracker();
            if (peer.Descriptor.IsNull()) {
                config->Addresses.push_back(Null);
            } else {
                config->Addresses.push_back(peer.Descriptor.GetInterconnectAddress());
            }
        }

        UpdateCellDirectory(cell);

        LOG_INFO_UNLESS(IsRecovery(), "Tablet cell reconfigured (CellId: %v, Version: %v)",
            cell->GetId(),
            cell->GetConfigVersion());
    }

    void UpdateCellDirectory(TTabletCell* cell)
    {
        auto cellDirectory = Bootstrap_->GetCellDirectory();
        cellDirectory->ReconfigureCell(cell->GetDescriptor());
    }


    void ValidateHasHealthyCells(TTabletCellBundle* cellBundle)
    {
        for (const auto& pair : TabletCellMap_) {
            auto* cell = pair.second;
            if (cell->GetCellBundle() == cellBundle && cell->GetHealth() == ETabletCellHealth::Good) {
                return;
            }
        }
        THROW_ERROR_EXCEPTION("No healthy tablet cells in bundle %Qv",
            cellBundle->GetName());
    }

    std::vector<std::pair<TTablet*, TTabletCell*>> ComputeTabletAssignment(
        TTableNode* table,
        TTableMountConfigPtr mountConfig,
        TTabletCell* hintCell,
        std::vector<TTablet*> tabletsToMount)
    {
        if (hintCell) {
            std::vector<std::pair<TTablet*, TTabletCell*>> assignment;
            for (auto* tablet : tabletsToMount) {
                assignment.emplace_back(tablet, hintCell);
            }
            return assignment;
        }

        struct TCellKey
        {
            i64 Size;
            TTabletCell* Cell;

            //! Compares by |(size, cellId)|.
            bool operator < (const TCellKey& other) const
            {
                if (Size < other.Size) {
                    return true;
                } else if (Size > other.Size) {
                    return false;
                }
                return Cell->GetId() < other.Cell->GetId();
            }
        };

        auto getCellSize = [&] (const TTabletCell* cell) -> i64 {
            i64 result = 0;
            switch (mountConfig->InMemoryMode) {
                case EInMemoryMode::None:
                    result += cell->TotalStatistics().UncompressedDataSize;
                    break;
                case EInMemoryMode::Uncompressed:
                case EInMemoryMode::Compressed:
                    result += cell->TotalStatistics().MemorySize;
                    break;
                default:
                    YUNREACHABLE();
            }
            result += cell->Tablets().size() * Config_->TabletDataSizeFootprint;
            return result;
        };

        std::set<TCellKey> cellKeys;
        for (const auto& pair : TabletCellMap_) {
            auto* cell = pair.second;
            if (cell->GetCellBundle() == table->GetTabletCellBundle() &&
                cell->GetHealth() == ETabletCellHealth::Good)
            {
                YCHECK(cellKeys.insert(TCellKey{getCellSize(cell), cell}).second);
            }
        }
        YCHECK(!cellKeys.empty());

        auto getTabletSize = [&] (const TTablet* tablet) -> i64 {
            i64 result = 0;
            auto statistics = GetTabletStatistics(tablet);
            switch (mountConfig->InMemoryMode) {
                case EInMemoryMode::None:
                case EInMemoryMode::Uncompressed:
                    result += statistics.UncompressedDataSize;
                    break;
                case EInMemoryMode::Compressed:
                    result += statistics.CompressedDataSize;
                    break;
                default:
                    YUNREACHABLE();
            }
            result += Config_->TabletDataSizeFootprint;
            return result;
        };

        // Sort tablets by decreasing size to improve greedy heuristic performance.
        std::sort(
            tabletsToMount.begin(),
            tabletsToMount.end(),
            [&] (const TTablet* lhs, const TTablet* rhs) {
                return
                    std::make_tuple(getTabletSize(lhs), lhs->GetId()) >
                    std::make_tuple(getTabletSize(rhs), rhs->GetId());
            });

        auto chargeCell = [&] (std::set<TCellKey>::iterator it, TTablet* tablet) {
            const auto& existingKey = *it;
            auto newKey = TCellKey{existingKey.Size + getTabletSize(tablet), existingKey.Cell};
            cellKeys.erase(it);
            YCHECK(cellKeys.insert(newKey).second);
        };

        // Iteratively assign tablets to least-loaded cells.
        std::vector<std::pair<TTablet*, TTabletCell*>> assignment;
        for (auto* tablet : tabletsToMount) {
            auto it = cellKeys.begin();
            assignment.emplace_back(tablet, it->Cell);
            chargeCell(it, tablet);
        }

        return assignment;
    }


    void RestartPrerequisiteTransaction(TTabletCell* cell)
    {
        AbortPrerequisiteTransaction(cell);
        AbortCellSubtreeTransactions(cell);
        StartPrerequisiteTransaction(cell);
    }

    void StartPrerequisiteTransaction(TTabletCell* cell)
    {
        auto multicellManager = Bootstrap_->GetMulticellManager();
        const auto& secondaryCellTags = multicellManager->GetRegisteredMasterCellTags();

        auto transactionManager = Bootstrap_->GetTransactionManager();
        auto* transaction = transactionManager->StartTransaction(
            nullptr,
            secondaryCellTags,
            Null,
            Format("Prerequisite for cell %v", cell->GetId()));

        auto objectManager = Bootstrap_->GetObjectManager();
        for (auto cellTag : secondaryCellTags) {
            objectManager->ReplicateObjectCreationToSecondaryMaster(transaction, cellTag);
        }

        YCHECK(!cell->GetPrerequisiteTransaction());
        cell->SetPrerequisiteTransaction(transaction);
        YCHECK(TransactionToCellMap_.insert(std::make_pair(transaction, cell)).second);

        LOG_INFO_UNLESS(IsRecovery(), "Tablet cell prerequisite transaction started (CellId: %v, TransactionId: %v)",
            cell->GetId(),
            transaction->GetId());
    }

    void AbortCellSubtreeTransactions(TTabletCell* cell)
    {
        auto cypressManager = Bootstrap_->GetCypressManager();
        auto cellNodeProxy = FindCellNode(cell->GetId());
        if (cellNodeProxy) {
            cypressManager->AbortSubtreeTransactions(cellNodeProxy);
        }
    }

    void AbortPrerequisiteTransaction(TTabletCell* cell)
    {
        auto* transaction = cell->GetPrerequisiteTransaction();
        if (!transaction) {
            return;
        }

        // Suppress calling OnTransactionFinished.
        YCHECK(TransactionToCellMap_.erase(transaction) == 1);
        cell->SetPrerequisiteTransaction(nullptr);

        // NB: Make a copy, transaction will die soon.
        auto transactionId = transaction->GetId();

        auto transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->AbortTransaction(transaction, true);

        LOG_INFO_UNLESS(IsRecovery(), "Tablet cell prerequisite aborted (CellId: %v, TransactionId: %v)",
            cell->GetId(),
            transactionId);
    }

    void OnTransactionFinished(TTransaction* transaction)
    {
        auto it = TransactionToCellMap_.find(transaction);
        if (it == TransactionToCellMap_.end()) {
            return;
        }

        auto* cell = it->second;
        cell->SetPrerequisiteTransaction(nullptr);
        TransactionToCellMap_.erase(it);

        LOG_INFO_UNLESS(IsRecovery(), "Tablet cell prerequisite transaction aborted (CellId: %v, TransactionId: %v)",
            cell->GetId(),
            transaction->GetId());

        for (auto peerId = 0; peerId < cell->Peers().size(); ++peerId) {
            DoRevokePeer(cell, peerId);
        }
    }


    void DoRevokePeer(TTabletCell* cell, TPeerId peerId)
    {
        const auto& peer = cell->Peers()[peerId];
        const auto& descriptor = peer.Descriptor;
        if (descriptor.IsNull()) {
            return;
        }

        LOG_INFO_UNLESS(IsRecovery(), "Tablet cell peer revoked (CellId: %v, Address: %v, PeerId: %v)",
            cell->GetId(),
            descriptor.GetDefaultAddress(),
            peerId);

        if (peer.Node) {
            peer.Node->DetachTabletCell(cell);
        }
        RemoveFromAddressToCellMap(descriptor, cell);
        cell->RevokePeer(peerId);
    }

    void DoUnmountTable(
        TTableNode* table,
        bool force,
        int firstTabletIndex,
        int lastTabletIndex)
    {
        auto hiveManager = Bootstrap_->GetHiveManager();

        for (int index = firstTabletIndex; index <= lastTabletIndex; ++index) {
            auto* tablet = table->Tablets()[index];
            auto* cell = tablet->GetCell();

            auto state = tablet->GetState();
            if (state == ETabletState::Mounted ||
                state == ETabletState::Frozen)
            {
                LOG_INFO_UNLESS(IsRecovery(), "Unmounting tablet (TableId: %v, TabletId: %v, CellId: %v, Force: %v)",
                    table->GetId(),
                    tablet->GetId(),
                    cell->GetId(),
                    force);

                tablet->SetState(ETabletState::Unmounting);
            }

            if (cell) {
                TReqUnmountTablet request;
                ToProto(request.mutable_tablet_id(), tablet->GetId());
                request.set_force(force);
                auto* mailbox = hiveManager->GetMailbox(cell->GetId());
                hiveManager->PostMessage(mailbox, request);
            }

            if (force && tablet->GetState() != ETabletState::Unmounted) {
                DoTabletUnmounted(tablet);
            }
        }
    }


    void GetTableSettings(
        TTableNode* table,
        TTableMountConfigPtr* mountConfig,
        TTableWriterOptionsPtr* writerOptions)
    {
        auto objectManager = Bootstrap_->GetObjectManager();
        auto tableProxy = objectManager->GetProxy(table);
        const auto& tableAttributes = tableProxy->Attributes();

        // Parse and prepare mount config.
        try {
            *mountConfig = ConvertTo<TTableMountConfigPtr>(tableAttributes);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing table mount configuration")
                << ex;
        }

        // Prepare tablet writer options.
        *writerOptions = New<TTableWriterOptions>();
        (*writerOptions)->ReplicationFactor = table->GetReplicationFactor();
        (*writerOptions)->Account = table->GetAccount()->GetName();
        (*writerOptions)->CompressionCodec = tableAttributes.Get<NCompression::ECodec>("compression_codec");
        (*writerOptions)->ErasureCodec = tableAttributes.Get<NErasure::ECodec>("erasure_codec", NErasure::ECodec::None);
        (*writerOptions)->ChunksVital = table->GetVital();
        (*writerOptions)->OptimizeFor = tableAttributes.Get<EOptimizeFor>("optimize_for", NTableClient::EOptimizeFor::Lookup);
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

    INodePtr FindCellNode(const TCellId& cellId)
    {
        auto cellMapNodeProxy = GetCellMapNode();
        return cellMapNodeProxy->FindChild(ToString(cellId));
    }


    void OnCleanup()
    {
        try {
            auto cypressManager = Bootstrap_->GetCypressManager();
            auto resolver = cypressManager->CreateResolver();
            for (const auto& pair : TabletCellMap_) {
                const auto& cellId = pair.first;
                const auto* cell = pair.second;
                if (!IsObjectAlive(cell))
                    continue;

                auto snapshotsPath = Format("//sys/tablet_cells/%v/snapshots", cellId);
                IMapNodePtr snapshotsMap;
                try {
                    snapshotsMap = resolver->ResolvePath(snapshotsPath)->AsMap();
                } catch (const std::exception&) {
                    continue;
                }

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
                IMapNodePtr changelogsMap;
                try {
                    changelogsMap = resolver->ResolvePath(changelogsPath)->AsMap();
                } catch (const std::exception&) {
                    continue;
                }

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
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error performing tablets cleanup");
        }
    }

    std::pair<std::vector<TTablet*>::iterator, std::vector<TTablet*>::iterator> GetIntersectingTablets(
        std::vector<TTablet*>& tablets,
        const TOwningKey& minKey,
        const TOwningKey& maxKey)
    {
        auto beginIt = std::upper_bound(
            tablets.begin(),
            tablets.end(),
            minKey,
            [] (const TOwningKey& key, const TTablet* tablet) {
                return key < tablet->GetPivotKey();
            });

        if (beginIt != tablets.begin()) {
            --beginIt;
        }

        auto endIt = beginIt;
        while (endIt != tablets.end() && maxKey >= (*endIt)->GetPivotKey()) {
            ++endIt;
        }

        return std::make_pair(beginIt, endIt);
    }


    static void ValidateTabletCellBundleName(const Stroka& name)
    {
        if (name.empty()) {
            THROW_ERROR_EXCEPTION("Tablet cell bundle name cannot be empty");
        }
    }
};

DEFINE_ENTITY_MAP_ACCESSORS(TTabletManager::TImpl, TabletCellBundle, TTabletCellBundle, TabletCellBundleMap_)
DEFINE_ENTITY_MAP_ACCESSORS(TTabletManager::TImpl, TabletCell, TTabletCell, TabletCellMap_)
DEFINE_ENTITY_MAP_ACCESSORS(TTabletManager::TImpl, Tablet, TTablet, TabletMap_)

///////////////////////////////////////////////////////////////////////////////

TTabletManager::TTabletCellBundleTypeHandler::TTabletCellBundleTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->TabletCellBundleMap_)
    , Owner_(owner)
{ }

TObjectBase* TTabletManager::TTabletCellBundleTypeHandler::CreateObject(
    const TObjectId& hintId,
    IAttributeDictionary* attributes)
{
    auto name = attributes->GetAndRemove<Stroka>("name");

    return Owner_->CreateTabletCellBundle(name, hintId);
}

void TTabletManager::TTabletCellBundleTypeHandler::DoDestroyObject(TTabletCellBundle* cellBundle)
{
    TObjectTypeHandlerWithMapBase::DoDestroyObject(cellBundle);
    Owner_->DestroyTabletCellBundle(cellBundle);
}

///////////////////////////////////////////////////////////////////////////////

TTabletManager::TTabletCellTypeHandler::TTabletCellTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->TabletCellMap_)
    , Owner_(owner)
{ }

TObjectBase* TTabletManager::TTabletCellTypeHandler::CreateObject(
    const TObjectId& hintId,
    IAttributeDictionary* attributes)
{
    auto cellBundleName = attributes->GetAndRemove("tablet_cell_bundle", DefaultTabletCellBundleName);
    auto* cellBundle = Owner_->GetTabletCellBundleByNameOrThrow(cellBundleName);

    return Owner_->CreateTabletCell(cellBundle, hintId);
}

void TTabletManager::TTabletCellTypeHandler::DoZombifyObject(TTabletCell* cell)
{
    TObjectTypeHandlerWithMapBase::DoZombifyObject(cell);
    // NB: Destroy the cell right away and do not wait for GC to prevent
    // dangling links from occurring in //sys/tablet_cells.
    Owner_->DestroyTabletCell(cell);
}

///////////////////////////////////////////////////////////////////////////////

TTabletManager::TTabletTypeHandler::TTabletTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->TabletMap_)
    , Owner_(owner)
{ }

void TTabletManager::TTabletTypeHandler::DoDestroyObject(TTablet* tablet)
{
    TObjectTypeHandlerWithMapBase::DoDestroyObject(tablet);
    Owner_->DestroyTablet(tablet);
}

///////////////////////////////////////////////////////////////////////////////

TTabletManager::TTabletManager(
    TTabletManagerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

TTabletManager::~TTabletManager() = default;

void TTabletManager::Initialize()
{
    return Impl_->Initialize();
}

int TTabletManager::GetAssignedTabletCellCount(const Stroka& address) const
{
    return Impl_->GetAssignedTabletCellCount(address);
}

TTabletStatistics TTabletManager::GetTabletStatistics(const TTablet* tablet)
{
    return Impl_->GetTabletStatistics(tablet);
}

void TTabletManager::MountTable(
    TTableNode* table,
    int firstTabletIndex,
    int lastTabletIndex,
    TTabletCell* hintCell,
    bool freeze)
{
    Impl_->MountTable(
        table,
        firstTabletIndex,
        lastTabletIndex,
        hintCell,
        freeze);
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

void TTabletManager::FreezeTable(
    TTableNode* table,
    int firstTabletIndex,
    int lastTabletIndex)
{
    Impl_->FreezeTable(
        table,
        firstTabletIndex,
        lastTabletIndex);
}

void TTabletManager::UnfreezeTable(
    TTableNode* table,
    int firstTabletIndex,
    int lastTabletIndex)
{
    Impl_->UnfreezeTable(
        table,
        firstTabletIndex,
        lastTabletIndex);
}

void TTabletManager::DestroyTable(TTableNode* table)
{
    Impl_->DestroyTable(table);
}

void TTabletManager::ReshardTable(
    TTableNode* table,
    int firstTabletIndex,
    int lastTabletIndex,
    int newTabletCount,
    const std::vector<TOwningKey>& pivotKeys)
{
    Impl_->ReshardTable(
        table,
        firstTabletIndex,
        lastTabletIndex,
        newTabletCount,
        pivotKeys);
}

TTabletManager::TCloneTableDataPtr TTabletManager::BeginCloneTable(
    TTableNode* sourceTable,
    TTableNode* clonedTable,
    ENodeCloneMode mode)
{
    return Impl_->BeginCloneTable(
        sourceTable,
        clonedTable,
        mode);
}

void TTabletManager::CommitCloneTable(
    TTableNode* sourceTable,
    TTableNode* clonedTable,
    TCloneTableDataPtr data)
{
    Impl_->CommitCloneTable(
        sourceTable,
        clonedTable,
        std::move(data));
}

void TTabletManager::RollbackCloneTable(
    TTableNode* sourceTable,
    TTableNode* clonedTable,
    TCloneTableDataPtr data)
{
    Impl_->RollbackCloneTable(
        sourceTable,
        clonedTable,
        std::move(data));
}

void TTabletManager::MakeTableDynamic(TTableNode* table)
{
    Impl_->MakeTableDynamic(table);
}

void TTabletManager::MakeTableStatic(TTableNode* table)
{
    Impl_->MakeTableStatic(table);
}

TTabletCell* TTabletManager::GetTabletCellOrThrow(const TTabletCellId& id)
{
    return Impl_->GetTabletCellOrThrow(id);
}

TTabletCellBundle* TTabletManager::FindTabletCellBundleByName(const Stroka& name)
{
    return Impl_->FindTabletCellBundleByName(name);
}

TTabletCellBundle* TTabletManager::GetTabletCellBundleByNameOrThrow(const Stroka& name)
{
    return Impl_->GetTabletCellBundleByNameOrThrow(name);
}

void TTabletManager::RenameTabletCellBundle(TTabletCellBundle* cellBundle, const Stroka& newName)
{
    return Impl_->RenameTabletCellBundle(cellBundle, newName);
}

TTabletCellBundle* TTabletManager::GetDefaultTabletCellBundle()
{
    return Impl_->GetDefaultTabletCellBundle();
}

void TTabletManager::SetTabletCellBundle(TTableNode* table, TTabletCellBundle* cellBundle)
{
    Impl_->SetTabletCellBundle(table, cellBundle);
}

DELEGATE_ENTITY_MAP_ACCESSORS(TTabletManager, TabletCellBundle, TTabletCellBundle, *Impl_)
DELEGATE_ENTITY_MAP_ACCESSORS(TTabletManager, TabletCell, TTabletCell, *Impl_)
DELEGATE_ENTITY_MAP_ACCESSORS(TTabletManager, Tablet, TTablet, *Impl_)

///////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
