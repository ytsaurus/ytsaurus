#include "table_manager.h"
#include "private.h"
#include "master_table_schema_proxy.h"
#include "mount_config_attributes.h"
#include "replicated_table_node.h"
#include "schemaful_node.h"
#include "table_collocation.h"
#include "table_collocation_type_handler.h"
#include "table_node.h"

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>

#include <yt/yt/server/master/chunk_server/chunk_owner_base.h>

#include <yt/yt/server/master/object_server/object_manager.h>
#include <yt/yt/server/master/object_server/type_handler_detail.h>

#include <yt/yt/server/master/table_server/helpers.h>
#include <yt/yt/server/master/tablet_server/config.h>
#include <yt/yt/server/master/tablet_server/mount_config_storage.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

#include <yt/yt/server/lib/table_server/proto/table_manager.pb.h>

#include <yt/yt/server/lib/tablet_server/replicated_table_tracker.h>

#include <yt/yt/library/heavy_schema_validation/schema_validation.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/misc/random_access_queue.h>

namespace NYT::NTableServer {

using namespace NCellMaster;
using namespace NChunkClient::NProto;
using namespace NChunkServer;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NCypressServer;
using namespace NCypressServer::NProto;
using namespace NHydra;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NTableClient;
using namespace NTabletServer;
using namespace NTransactionServer;
using namespace NYson;
using namespace NYTree;

using NCypressServer::TNodeId;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TableServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TTableManager;

////////////////////////////////////////////////////////////////////////////////
class TMasterTableSchemaTypeHandler
    : public TObjectTypeHandlerWithMapBase<TMasterTableSchema>
{
public:
    explicit TMasterTableSchemaTypeHandler(TTableManager* owner);

    ETypeFlags GetFlags() const override
    {
        // NB: schema objects are cell-local. Different cells have differing schema sets.
        return ETypeFlags::None;
    }

    EObjectType GetType() const override
    {
        return EObjectType::MasterTableSchema;
    }

    TObject* CreateObject(
        TObjectId hintId,
        IAttributeDictionary* attributes) override;

private:
    TTableManager* const Owner_;

    IObjectProxyPtr DoGetProxy(TMasterTableSchema* user, TTransaction* transaction) override;

    void DoZombifyObject(TMasterTableSchema* user) override;
};

////////////////////////////////////////////////////////////////////////////////

class TTableManager
    : public ITableManager
    , public TMasterAutomatonPart
{
public:
    explicit TTableManager(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::TableManager)
        , StatisticsGossipThrottler_(CreateReconfigurableThroughputThrottler(
            New<TThroughputThrottlerConfig>(),
            TableServerLogger,
            TableServerProfiler.WithPrefix("/table_statistics_gossip_throttler")))
    {
        RegisterLoader(
            "TableManager.Keys",
            BIND(&TTableManager::LoadKeys, Unretained(this)));
        RegisterLoader(
            "TableManager.Values",
            BIND(&TTableManager::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "TableManager.Keys",
            BIND(&TTableManager::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "TableManager.Values",
            BIND(&TTableManager::SaveValues, Unretained(this)));

        // COMPAT(shakurov, gritukan)
        RegisterMethod(BIND(&TTableManager::HydraSendTableStatisticsUpdates, Unretained(this)), /*aliases*/ {"NYT.NTabletServer.NProto.TReqSendTableStatisticsUpdates"});
        RegisterMethod(BIND(&TTableManager::HydraUpdateTableStatistics, Unretained(this)), /*aliases*/ {"NYT.NTabletServer.NProto.TReqUpdateTableStatistics"});
        RegisterMethod(BIND(&TTableManager::HydraNotifyContentRevisionCasFailed, Unretained(this)));

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(New<TMasterTableSchemaTypeHandler>(this));
        objectManager->RegisterHandler(CreateTableCollocationTypeHandler(Bootstrap_, &TableCollocationMap_));

        auto primaryCellTag = Bootstrap_->GetMulticellManager()->GetPrimaryCellTag();
        EmptyMasterTableSchemaId_ = MakeWellKnownId(EObjectType::MasterTableSchema, primaryCellTag, 0xffffffffffffffff);

        // COMPAT(h0pless): Remove this after schema migration is complete.
        auto cellTag = Bootstrap_->GetMulticellManager()->GetCellTag();
        OldEmptyMasterTableSchemaId_ = MakeWellKnownId(EObjectType::MasterTableSchema, cellTag, 0xffffffffffffffff);
    }

    void Initialize() override
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND(&TTableManager::OnDynamicConfigChanged, MakeWeak(this)));
    }


    void ScheduleStatisticsUpdate(
        TChunkOwnerBase* chunkOwner,
        bool updateDataStatistics,
        bool updateTabletStatistics,
        bool useNativeContentRevisionCas) override
    {
        YT_ASSERT(!updateTabletStatistics || IsTabletOwnerType(chunkOwner->GetType()));

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsSecondaryMaster()) {
            YT_LOG_DEBUG("Schedule node statistics update (NodeId: %v, UpdateDataStatistics: %v, UpdateTabletStatistics: %v, UseNativeContentRevisionCas: %v)",
                chunkOwner->GetId(),
                updateDataStatistics,
                updateTabletStatistics,
                useNativeContentRevisionCas);

            auto& statistics = StatisticsUpdateRequests_[chunkOwner->GetId()];
            statistics.UpdateTabletResourceUsage |= updateTabletStatistics;
            statistics.UpdateDataStatistics |= updateDataStatistics;
            statistics.UpdateModificationTime = true;
            statistics.UpdateAccessTime = true;
            statistics.UseNativeContentRevisionCas = useNativeContentRevisionCas;
        }
    }

    void SendStatisticsUpdate(
        TChunkOwnerBase* chunkOwner,
        bool useNativeContentRevisionCas) override
    {
        if (chunkOwner->IsNative()) {
            return;
        }

        YT_VERIFY(IsSupportedNodeType(chunkOwner->GetType()));

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsSecondaryMaster());

        YT_LOG_DEBUG("Sending node statistics update (NodeId: %v)",
            chunkOwner->GetId());

        NTableServer::NProto::TReqUpdateTableStatistics req;
        auto* entry = req.add_entries();
        ToProto(entry->mutable_node_id(), chunkOwner->GetId());
        ToProto(entry->mutable_data_statistics(), chunkOwner->SnapshotStatistics());
        if (IsTableType(chunkOwner->GetType())) {
            auto* table = chunkOwner->As<TTableNode>();
            ToProto(entry->mutable_tablet_resource_usage(), table->GetTabletResourceUsage());
        }
        entry->set_modification_time(ToProto<ui64>(chunkOwner->GetModificationTime()));
        entry->set_access_time(ToProto<ui64>(chunkOwner->GetAccessTime()));
        if (useNativeContentRevisionCas) {
            entry->set_expected_content_revision(chunkOwner->GetNativeContentRevision());
        }
        multicellManager->PostToMaster(req, chunkOwner->GetNativeCellTag());

        StatisticsUpdateRequests_.Pop(chunkOwner->GetId());
    }


    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(MasterTableSchema, TMasterTableSchema);
    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(TableCollocation, TTableCollocation);


    TTableNode* GetTableNodeOrThrow(TTableId id) override
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto* object = objectManager->GetObjectOrThrow(id);
        if (!IsObjectAlive(object)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::ResolveError,
                "No such table %v",
                id);
        }
        if (!IsTableType(object->GetType())) {
            THROW_ERROR_EXCEPTION("Object %v is expected to be a table, actual type is %Qlv",
                id,
                object->GetType());
        }

        return object->As<TTableNode>();
    }

    TTableNode* FindTableNode(TTableId id) override
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto* object = objectManager->FindObject(id);
        if (object && IsObjectAlive(object) && IsTableType(object->GetType())) {
            return object->As<TTableNode>();
        }
        return nullptr;
    }

    TMasterTableSchema* GetEmptyMasterTableSchema() const override
    {
        YT_VERIFY(EmptyMasterTableSchema_);
        return EmptyMasterTableSchema_;
    }

    void SetTableSchema(ISchemafulNode* table, TMasterTableSchema* schema) override
    {
        if (!schema) {
            // During cross-shard copying of an opaque table, it is materialized
            // by EndCopy (non-inplace) without any schema.
            return;
        }

        // NB: a newly created schema object has zero reference count.
        // Thus the new schema may technically be not alive here.
        YT_VERIFY(!schema->IsGhost());

        if (schema == table->GetSchema()) {
            // Typical when branched nodes are being merged back in.
            return;
        }

        ResetTableSchema(table);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        table->SetSchema(schema);
        objectManager->RefObject(schema);

        auto* tableAccount = table->GetAccount();
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->UpdateMasterMemoryUsage(schema, tableAccount);
    }

    void SetTableSchemaOrThrow(ISchemafulNode* table, TMasterTableSchemaId schemaId) override
    {
        auto* existingSchema = GetMasterTableSchemaOrThrow(schemaId);
        SetTableSchema(table, existingSchema);
    }

    void SetTableSchemaOrCrash(ISchemafulNode* table, TMasterTableSchemaId schemaId) override
    {
        auto* existingSchema = GetMasterTableSchema(schemaId);
        SetTableSchema(table, existingSchema);
    }

    void ResetTableSchema(ISchemafulNode* table) override
    {
        auto* oldSchema = table->GetSchema();
        if (!oldSchema) {
            return;
        }
        YT_VERIFY(IsObjectAlive(oldSchema));
        table->SetSchema(nullptr);

        if (auto* account = table->GetAccount()) {
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            securityManager->ResetMasterMemoryUsage(oldSchema, account);
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->UnrefObject(oldSchema);
    }

    TMasterTableSchema* GetMasterTableSchemaOrThrow(TMasterTableSchemaId id) override
    {
        auto* schema = FindMasterTableSchema(id);
        if (!IsObjectAlive(schema)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::ResolveError,
                "No such schema %v",
                id);
        }

        return schema;
    }

    TMasterTableSchema* FindNativeMasterTableSchema(const TTableSchema& tableSchema) const override
    {
        auto it = NativeTableSchemaToObjectMap_.find(tableSchema);
        return it != NativeTableSchemaToObjectMap_.end()
            ? it->second
            : nullptr;
    }

    TMasterTableSchema* FindImportedMasterTableSchema(const TTableSchema& tableSchema, const TCellTag cellTag) const override
    {
        auto it = ImportedTableSchemaToObjectMap_.find(TCellTaggedTableSchema(tableSchema, cellTag));
        return it != ImportedTableSchemaToObjectMap_.end()
            ? it->second
            : nullptr;
    }

    TMasterTableSchema* CreateImportedMasterTableSchema(
        const NTableClient::TTableSchema& tableSchema,
        ISchemafulNode* schemaHolder,
        TMasterTableSchemaId hintId) override
    {
        return CreateMasterTableSchema(tableSchema, schemaHolder, /* isNative */ false, hintId);
    }

    TMasterTableSchema* CreateImportedMasterTableSchema(
        const NTableClient::TTableSchema& tableSchema,
        NTransactionServer::TTransaction* schemaHolder,
        TMasterTableSchemaId hintId) override
    {
        return CreateMasterTableSchema(tableSchema, schemaHolder, /* isNative */ false, hintId);
    }

    TMasterTableSchema* GetOrCreateNativeMasterTableSchema(
        const TTableSchema& schema,
        ISchemafulNode* schemaHolder) override
    {
        auto* existingSchema = FindNativeMasterTableSchema(schema);
        if (!existingSchema) {
            return CreateMasterTableSchema(schema, schemaHolder, /* isNative */ true);
        }

        SetTableSchema(schemaHolder, existingSchema);
        return existingSchema;
    }

    TMasterTableSchema* GetOrCreateNativeMasterTableSchema(
        const TTableSchema& schema,
        TTransaction* schemaHolder) override
    {
        auto* existingSchema = FindNativeMasterTableSchema(schema);
        if (!existingSchema) {
            return CreateMasterTableSchema(schema, schemaHolder, /* isNative */ true);
        }

        if (!schemaHolder->StagedObjects().contains(existingSchema)) {
            const auto& transactionManager = Bootstrap_->GetTransactionManager();
            transactionManager->StageObject(schemaHolder, existingSchema);
        }
        return existingSchema;
    }

    TMasterTableSchema* CreateMasterTableSchema(
        const TTableSchema& tableSchema,
        ISchemafulNode* schemaHolder,
        bool isNative,
        TMasterTableSchemaId hintId = NObjectClient::NullObjectId)
    {
        auto* schema = FindMasterTableSchema(hintId);
        if (!schema) {
            schema = DoCreateMasterTableSchema(tableSchema, hintId, isNative);
        } else if(!IsObjectAlive(schema)) {
            ResurrectMasterTableSchema(schema);
            YT_VERIFY(*schema->TableSchema_ == tableSchema);
        }
        SetTableSchema(schemaHolder, schema);
        YT_VERIFY(IsObjectAlive(schema));
        return schema;
    }

    TMasterTableSchema* CreateMasterTableSchema(
        const TTableSchema& tableSchema,
        TTransaction* schemaHolder,
        bool isNative,
        TMasterTableSchemaId hintId = NObjectClient::NullObjectId)
    {
        YT_VERIFY(IsObjectAlive(schemaHolder));

        auto* schema = FindMasterTableSchema(hintId);
        if (!schema) {
            schema = DoCreateMasterTableSchema(tableSchema, hintId, isNative);
        } else if (!IsObjectAlive(schema)) {
            ResurrectMasterTableSchema(schema);
            YT_VERIFY(*schema->TableSchema_ == tableSchema);
        }

        if (!schemaHolder->StagedObjects().contains(schema)) {
            const auto& transactionManager = Bootstrap_->GetTransactionManager();
            transactionManager->StageObject(schemaHolder, schema);
        }
        YT_VERIFY(IsObjectAlive(schema));
        return schema;
    }

    TMasterTableSchema* DoCreateMasterTableSchema(
        const TTableSchema& tableSchema,
        TMasterTableSchemaId hintId,
        bool isNative)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::MasterTableSchema, hintId);

        auto sharedTableSchema = New<TTableSchema>(tableSchema);

        TMasterTableSchema* schema;
        if (isNative){
            auto it = EmplaceOrCrash(
                NativeTableSchemaToObjectMap_,
                std::move(sharedTableSchema),
                nullptr);

            auto schemaHolder = TPoolAllocator::New<TMasterTableSchema>(id, it);
            schema = MasterTableSchemaMap_.Insert(id, std::move(schemaHolder));
            it->second = schema;
            YT_VERIFY(schema->GetNativeTableSchemaToObjectMapIterator()->second == schema);
        } else {
            auto [it, emplaced] = ImportedTableSchemaToObjectMap_.emplace(
                TCellTaggedTableSchemaPtr(std::move(sharedTableSchema), CellTagFromId(id)),
                nullptr);

            if (!emplaced) {
                // This schema should become zombie, thus it's doomed.
                auto* doomedSchema = it->second;
                YT_VERIFY(doomedSchema->GetId() != hintId);
                YT_LOG_DEBUG("Rewriting table schema (OldSchemaId: %v, NewSchemaId: %v)",
                    doomedSchema->GetId(),
                    hintId);
                doomedSchema->ResetImportedTableSchemaToObjectMapIterator();
            }

            auto schemaHolder = TPoolAllocator::New<TMasterTableSchema>(id, it);
            schema = MasterTableSchemaMap_.Insert(id, std::move(schemaHolder));
            it->second = schema;
            YT_VERIFY(schema->GetImportedTableSchemaToObjectMapIterator()->second == schema);
        }

        YT_VERIFY(!schema->CellTagToExportCount_);
        YT_VERIFY(schema->IsNative() == isNative);

        YT_LOG_DEBUG("Schema created (Id: %v)", id);

        return schema;
    }

    void ZombifyMasterTableSchema(TMasterTableSchema* schema)
    {
        YT_VERIFY(schema != EmptyMasterTableSchema_);

        schema->AlertIfNonEmptyExportCount();

        if (!schema->ReferencingAccounts().empty()) {
            YT_LOG_ALERT("Table schema being destroyed is still referenced by some accounts (SchemaId: %v, AccountCount: %v)",
                schema->GetId(),
                schema->ReferencingAccounts().size());
        }

        if (!schema->ChargedMasterMemoryUsage().empty()) {
            YT_LOG_ALERT("Table schema being destroyed is still charged to some accounts (SchemaId: %v, AccountCount: %v)",
                schema->GetId(),
                schema->ChargedMasterMemoryUsage().size());
        }

        // COMPAT(h0pless): Remove this after schema migration is complete.
        if (schema->GetId() == OldEmptyMasterTableSchemaId_) {
            return;
        }

        if (schema->IsNative()) {
            if (auto it = schema->GetNativeTableSchemaToObjectMapIterator()) {
                NativeTableSchemaToObjectMap_.erase(it);
            }
            schema->ResetNativeTableSchemaToObjectMapIterator();
        } else {
            if (auto it = schema->GetImportedTableSchemaToObjectMapIterator()) {
                ImportedTableSchemaToObjectMap_.erase(it);
            }
            schema->ResetImportedTableSchemaToObjectMapIterator();
        }

    }

    void ResurrectMasterTableSchema(TMasterTableSchema* schema) {
        YT_VERIFY(!schema->IsDisposed());
        YT_VERIFY(FindMasterTableSchema(schema->GetId()));

        YT_LOG_DEBUG("Resurrecting master table schema object (SchemaId: %v)",
            schema->GetId());

        const auto& tableSchema = schema->TableSchema_;
        if (schema->IsNative()) {
            auto it = EmplaceOrCrash(
                NativeTableSchemaToObjectMap_,
                tableSchema,
                schema);
            schema->SetNativeTableSchemaToObjectMapIterator(it);
        } else {
            auto cellTag = CellTagFromId(schema->GetId());
            auto it = EmplaceOrCrash(
                ImportedTableSchemaToObjectMap_,
                TCellTaggedTableSchemaPtr(tableSchema, cellTag),
                schema);
            schema->SetImportedTableSchemaToObjectMapIterator(it);
        }
    }

    TMasterTableSchema::TNativeTableSchemaToObjectMapIterator RegisterNativeSchema(
        TMasterTableSchema* schema,
        TTableSchema tableSchema) override
    {
        YT_VERIFY(IsObjectAlive(schema));
        auto sharedTableSchema = New<TTableSchema>(std::move(tableSchema));
        return EmplaceOrCrash(
            NativeTableSchemaToObjectMap_,
            sharedTableSchema,
            schema);
    }

    TMasterTableSchema::TImportedTableSchemaToObjectMapIterator RegisterImportedSchema(
        TMasterTableSchema* schema,
        TTableSchema tableSchema) override
    {
        YT_VERIFY(IsObjectAlive(schema));
        auto cellTag = CellTagFromId(schema->GetId());
        auto sharedTableSchema = New<TTableSchema>(std::move(tableSchema));
        return EmplaceOrCrash(
            ImportedTableSchemaToObjectMap_,
            TCellTaggedTableSchemaPtr(std::move(sharedTableSchema), cellTag),
            schema);
    }

    void ValidateTableSchemaCorrespondence(
        TVersionedNodeId nodeId,
        const TTableSchemaPtr& tableSchema,
        TMasterTableSchemaId schemaId) override
    {
        auto type = TypeFromId(nodeId.ObjectId);
        YT_VERIFY(IsSchemafulType(type));

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto nativeCellTag = CellTagFromId(nodeId.ObjectId);
        auto native = nativeCellTag == multicellManager->GetCellTag();

        const auto& tableManager = Bootstrap_->GetTableManager();
        TMasterTableSchema* schemaById = nullptr;
        if (native) {
            if (schemaId) {
                schemaById = tableManager->GetMasterTableSchemaOrThrow(schemaId);
            }
        } else {
            // On external cells schemaId should always be present.
            // COMPAT(h0pless): Change this to YT_VERIFY after schema migration is complete.
            YT_LOG_ALERT_IF(!schemaId, "Request to create a foreign node has no schema id on external cell "
                "(NodeId: %v, NativeCellTag: %v, CellTag: %v)",
                nodeId,
                nativeCellTag,
                multicellManager->GetCellTag());

            schemaById = FindMasterTableSchema(schemaId);

            if (!schemaById) {
                // COMPAT(h0pless): Change this to YT_VERIFY after schema migration is complete.
                YT_LOG_ALERT_IF(!tableSchema, "Request to create a foreign node has schema id of an unimported schema on external cell "
                    "(NodeId: %v, NativeCellTag: %v, CellTag: %v, SchemaId: %v)",
                    nodeId,
                    nativeCellTag,
                    multicellManager->GetCellTag(),
                    schemaId);
            }
        }

        if (tableSchema && schemaById && schemaById->IsNative()) {
            auto* schemaByYson = tableManager->FindNativeMasterTableSchema(*tableSchema);
            if (IsObjectAlive(schemaByYson)) {
                if (schemaById != schemaByYson) {
                    THROW_ERROR_EXCEPTION("Both \"schema\" and \"schema_id\" specified and they refer to different schemas");
                }
            } else {
                if (*schemaById->AsTableSchema(/* crashOnZombie */ false) != *tableSchema) {
                    THROW_ERROR_EXCEPTION("Both \"schema\" and \"schema_id\" specified and the schemas do not match");
                }
            }
        }
    }

    const TTableSchema* ProcessSchemaFromAttributes(
        TTableSchemaPtr& tableSchema,
        TMasterTableSchemaId schemaId,
        bool dynamic,
        bool chaos,
        TVersionedNodeId nodeId) override
    {
        auto type = TypeFromId(nodeId.ObjectId);

        if (dynamic && !chaos && !tableSchema && !schemaId) {
            THROW_ERROR_EXCEPTION("Either \"schema\" or \"schema_id\" must be specified for dynamic tables");
        }

        ValidateTableSchemaCorrespondence(
            nodeId,
            tableSchema,
            schemaId);

        const auto& tableManager = Bootstrap_->GetTableManager();
        const TTableSchema* effectiveTableSchema = nullptr;
        auto* schemaById = tableManager->FindMasterTableSchema(schemaId);
        if (schemaById) {
            effectiveTableSchema = schemaById->AsTableSchema(/* crashOnZombie */ false).Get();
        } else if (tableSchema) {
            effectiveTableSchema = tableSchema.Get();
        } else {
            return nullptr;
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto native = CellTagFromId(nodeId.ObjectId) == multicellManager->GetCellTag();

        // NB: Sorted dynamic tables contain unique keys, set this for user.
        if (dynamic && effectiveTableSchema->IsSorted() && !effectiveTableSchema->GetUniqueKeys()) {
            if (!native) {
                // COMPAT(h0pless): Change this to YT_VERIFY after schema migration is complete.
                YT_LOG_ALERT("Schema doesn't have \"unique_keys\" set to true on the external cell for a dynamic table (TableId: %v, Schema: %v)",
                    nodeId,
                    tableSchema);
            }

            tableSchema = effectiveTableSchema->ToUniqueKeys();
            effectiveTableSchema = tableSchema.Get();
        }

        if (effectiveTableSchema->HasNontrivialSchemaModification()) {
            THROW_ERROR_EXCEPTION("Cannot create table with nontrivial schema modification");
        }

        const auto& dynamicConfig = Bootstrap_->GetConfigManager()->GetConfig();
        ValidateTableSchemaUpdateInternal(TTableSchema(), *effectiveTableSchema, GetSchemaUpdateEnabledFeatures(dynamicConfig), dynamic, true);

        if (!dynamicConfig->EnableDescendingSortOrder || (dynamic && !dynamicConfig->EnableDescendingSortOrderDynamic)) {
            ValidateNoDescendingSortOrder(*effectiveTableSchema);
        }

        if (!dynamicConfig->EnableTableColumnRenaming ||
            dynamic && !dynamicConfig->EnableDynamicTableColumnRenaming) {
            ValidateNoRenamedColumns(*effectiveTableSchema);
        }

        if (type == EObjectType::ReplicationLogTable && !effectiveTableSchema->IsSorted()) {
            THROW_ERROR_EXCEPTION("Could not create unsorted replication log table");
        }

        return effectiveTableSchema;
    }

    void EnsureSchemaExported(
        const ISchemafulNode* node,
        TCellTag externalCellTag,
        TTransactionId externalizedTransactionId,
        const IMulticellManagerPtr& multicellManager) override
    {
        auto* schema = node->GetSchema();
        YT_VERIFY(schema);

        if (!schema->IsExported(externalCellTag)) {
            TReqExportTableSchema exportRequest;

            ToProto(exportRequest.mutable_schema_id(), schema->GetId());
            ToProto(exportRequest.mutable_schema(), schema->AsTableSchema());
            ToProto(exportRequest.mutable_transaction_id(), externalizedTransactionId);

            multicellManager->PostToMaster(exportRequest, externalCellTag);
        }

        schema->ExportRef(externalCellTag);
    }

    TTableCollocation* CreateTableCollocation(
        TObjectId hintId,
        ETableCollocationType type,
        THashSet<TTableNode*> collocatedTables) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (collocatedTables.empty()) {
            THROW_ERROR_EXCEPTION("Collocated table set must be non-empty");
        }

        const auto& config = Bootstrap_->GetConfigManager()->GetConfig();
        auto maxTableCollocationSize = config->TabletManager->MaxTableCollocationSize;
        if (std::ssize(collocatedTables) > maxTableCollocationSize) {
            THROW_ERROR_EXCEPTION("Too many tables in collocation: %v > %v",
                std::ssize(collocatedTables),
                maxTableCollocationSize);
        }

        TCellTag externalCellTag = InvalidCellTag;
        for (auto* table : GetValuesSortedByKey(collocatedTables)) {
            switch (type) {
                case ETableCollocationType::Replication:
                    if (!table->IsReplicated()) {
                        THROW_ERROR_EXCEPTION("Unexpected type of table %v: expected %Qlv, actual %Qlv",
                            table->GetId(),
                            EObjectType::ReplicatedTable,
                            table->GetType());
                    }

                    if (auto* collocation = table->GetReplicationCollocation()) {
                        YT_VERIFY(IsObjectAlive(collocation));
                        THROW_ERROR_EXCEPTION("Table %v already belongs to replication collocation %v",
                            table->GetId(),
                            collocation->GetId());
                    }

                    break;

                default:
                    YT_ABORT();
            }

            auto primaryCellTag = Bootstrap_->GetMulticellManager()->GetPrimaryCellTag();
            if (primaryCellTag != table->GetNativeCellTag()) {
                // TODO(akozhikhov): Support portals with collocation.
                THROW_ERROR_EXCEPTION("Unexpected native cell tag for table %v: expected %v, actual %v",
                    table->GetId(),
                    primaryCellTag,
                    table->GetNativeCellTag());
            }

            auto tableExternalCellTag = table->GetExternalCellTag();
            if (externalCellTag != InvalidCellTag && externalCellTag != tableExternalCellTag) {
                THROW_ERROR_EXCEPTION("Collocated tables must have same external cell tag, found %v and %v",
                    externalCellTag,
                    tableExternalCellTag);
            }
            externalCellTag = tableExternalCellTag;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::TableCollocation, hintId);
        auto collocationHolder = TPoolAllocator::New<TTableCollocation>(id);

        auto* collocation = TableCollocationMap_.Insert(id, std::move(collocationHolder));
        YT_VERIFY(collocation->RefObject() == 1);

        collocation->SetExternalCellTag(externalCellTag);
        collocation->Tables() = std::move(collocatedTables);
        collocation->SetType(type);
        switch (type) {
            case ETableCollocationType::Replication:
                for (auto* table : collocation->Tables()) {
                    table->SetReplicationCollocation(collocation);
                }
                OnReplicationCollocationUpdated(collocation);
                break;

            default:
                YT_ABORT();
        }

        YT_LOG_DEBUG("Table collocation created "
            "(CollocationId: %v, CollocationType: %v, ExternalCellTag: %v, TableIds: %v)",
            collocation->GetId(),
            type,
            externalCellTag,
            MakeFormattableView(collocation->Tables(), [] (auto* builder, TTableNode* table) {
                builder->AppendFormat("%v", table->GetId());
            }));

        return collocation;
    }

    void ZombifyTableCollocation(TTableCollocation* collocation) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        switch (collocation->GetType()) {
            case ETableCollocationType::Replication:
                for (auto* table : collocation->Tables()) {
                    table->SetReplicationCollocation(nullptr);
                }
                break;

            default:
                YT_ABORT();
        }

        collocation->Tables().clear();

        ReplicationCollocationDestroyed_.Fire(collocation->GetId());

        YT_VERIFY(collocation->GetObjectRefCounter() == 0);

        YT_LOG_DEBUG("Table collocation zombified (TableCollocationId: %v)",
            collocation->GetId());
    }

    void AddTableToCollocation(
        TTableNode* table,
        TTableCollocation* collocation) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        YT_VERIFY(IsObjectAlive(collocation));
        YT_VERIFY(IsObjectAlive(table));

        const auto& config = Bootstrap_->GetConfigManager()->GetConfig();
        auto maxTableCollocationSize = config->TabletManager->MaxTableCollocationSize;
        if (std::ssize(collocation->Tables()) + 1 > maxTableCollocationSize) {
            THROW_ERROR_EXCEPTION("Too many tables in collocation: %v > %v",
                std::ssize(collocation->Tables()) + 1,
                maxTableCollocationSize);
        }

        if (collocation->GetExternalCellTag() != table->GetExternalCellTag()) {
            THROW_ERROR_EXCEPTION("Collocated tables must have same external cell tag, found %v and %v",
                collocation->GetExternalCellTag(),
                table->GetExternalCellTag());
        }

        auto primaryCellTag = Bootstrap_->GetMulticellManager()->GetPrimaryCellTag();
        if (primaryCellTag != table->GetNativeCellTag()) {
            // TODO(akozhikhov): Support portals with collocation.
            THROW_ERROR_EXCEPTION("Unexpected native cell tag for table %v: found %v, expected %v",
                table->GetId(),
                table->GetNativeCellTag(),
                primaryCellTag);
        }

        auto collocationType = collocation->GetType();
        switch (collocationType) {
            case ETableCollocationType::Replication:
                if (auto* tableCollocation = table->GetReplicationCollocation()) {
                    YT_VERIFY(IsObjectAlive(tableCollocation));
                    if (tableCollocation != collocation) {
                        THROW_ERROR_EXCEPTION("Table %v already belongs to replication collocation %v",
                            table->GetId(),
                            tableCollocation->GetId());
                    }
                    return;
                }

                if (!table->IsReplicated()) {
                    THROW_ERROR_EXCEPTION("Unexpected type of table %v: expected %Qlv, actual %Qlv",
                        table->GetId(),
                        EObjectType::ReplicatedTable,
                        table->GetType());
                }

                break;

            default:
                YT_ABORT();
        }

        if (!collocation->Tables().insert(table).second) {
            YT_LOG_ALERT("Table %v is already present in collocation %v",
                table->GetId(),
                collocation->GetId());
        }

        switch (collocationType) {
            case ETableCollocationType::Replication:
                table->SetReplicationCollocation(collocation);
                OnReplicationCollocationUpdated(collocation);
                break;

            default:
                YT_ABORT();
        }

        YT_LOG_DEBUG("Added table to collocation "
            "(CollocationId: %v, CollocationType: %v, TableId: %v, NewCollocationSize: %v)",
            collocation->GetId(),
            collocationType,
            table->GetId(),
            collocation->Tables().size());
    }

    void RemoveTableFromCollocation(TTableNode* table, TTableCollocation* collocation) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        YT_VERIFY(IsObjectAlive(collocation));

        auto collocationType = collocation->GetType();
        switch (collocationType) {
            case ETableCollocationType::Replication:
                table->SetReplicationCollocation(nullptr);
                break;

            default:
                YT_ABORT();
        }

        if (collocation->Tables().erase(table) != 1) {
            YT_LOG_ALERT("Table %v is already missing from collocation %v",
                table->GetId(),
                collocation->GetId());
            return;
        }

        YT_LOG_DEBUG("Removed table from collocation "
            "(CollocationId: %v, CollocationType: %v, TableId: %v, NewCollocationSize: %v)",
            collocation->GetId(),
            collocationType,
            table->GetId(),
            collocation->Tables().size());

        OnReplicationCollocationUpdated(collocation);

        // NB: On secondary master collocation should only be destroyed via foreign object removal mechanism.
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster() &&
            collocation->Tables().empty())
        {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            objectManager->UnrefObject(collocation);
        }
    }

    TTableCollocation* GetTableCollocationOrThrow(TTableCollocationId id) const override
    {
        auto* collocation = FindTableCollocation(id);
        if (!IsObjectAlive(collocation)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::ResolveError,
                "No such table collocation %v",
                id);
        }

        return collocation;
    }

    const THashSet<TTableNode*>& GetQueues() const override
    {
        Bootstrap_->VerifyPersistentStateRead();

        return Queues_;
    }

    void RegisterQueue(TTableNode* node) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        if (!Queues_.insert(node).second) {
            YT_LOG_ALERT("Attempting to register a queue twice (Node: %v, Path: %v)",
                node->GetId(),
                Bootstrap_->GetCypressManager()->GetNodePath(node, /*transaction*/ nullptr));
        }
    }

    void UnregisterQueue(TTableNode* node) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        if (!Queues_.erase(node)) {
            YT_LOG_ALERT("Attempting to unregister an unknown queue (Node: %v, Path: %v)",
                node->GetId(),
                Bootstrap_->GetCypressManager()->GetNodePath(node, /*transaction*/ nullptr));
        }
    }

    const THashSet<TTableNode*>& GetConsumers() const override
    {
        Bootstrap_->VerifyPersistentStateRead();

        return Consumers_;
    }

    void RegisterConsumer(TTableNode* node) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        if (!Consumers_.insert(node).second) {
            YT_LOG_ALERT(
                "Attempting to register a consumer twice (Node: %v, Path: %Qv)",
                node->GetId(),
                Bootstrap_->GetCypressManager()->GetNodePath(node, /*transaction*/ nullptr));
        }
    }

    void UnregisterConsumer(TTableNode* node) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasHydraContext());

        if (!Consumers_.erase(node)) {
            YT_LOG_ALERT(
                "Attempting to unregister an unknown consumer (Node: %v, Path: %Qv)",
                node->GetId(),
                Bootstrap_->GetCypressManager()->GetNodePath(node, /*transaction*/ nullptr));
        }
    }

    TFuture<TYsonString> GetQueueAgentObjectRevisionsAsync() const override
    {
        Bootstrap_->VerifyPersistentStateRead();

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        const auto& cypressManager = Bootstrap_->GetCypressManager();

        using ObjectRevisionMap = THashMap<TString, THashMap<TString, TRevision>>;

        ObjectRevisionMap objectRevisions;
        auto addToObjectRevisions = [&] (const TString& key, const THashSet<TTableNode*>& nodes) {
            objectRevisions[key] = {};
            for (auto* node : nodes) {
                if (IsObjectAlive(node)) {
                    EPathRootType pathRootType;
                    auto path = cypressManager->GetNodePath(node, /*transaction*/ nullptr, /*pathRootType*/ &pathRootType);
                    // We are intentionally skipping dangling trunk nodes.
                    if (pathRootType != EPathRootType::Other) {
                        objectRevisions[key][path] = node->GetAttributeRevision();
                    }
                }
            }
        };
        addToObjectRevisions("queues", GetQueues());
        addToObjectRevisions("consumers", GetConsumers());

        if (multicellManager->IsPrimaryMaster() && multicellManager->GetRoleMasterCellCount(EMasterCellRole::CypressNodeHost) > 1) {
            std::vector<TFuture<TYPathProxy::TRspGetPtr>> asyncResults;
            for (auto cellTag : multicellManager->GetRoleMasterCells(EMasterCellRole::CypressNodeHost)) {
                if (multicellManager->GetCellTag() != cellTag) {
                    YT_LOG_DEBUG("Requesting queue agent objects from secondary cell (CellTag: %v)",
                        cellTag);
                    auto proxy = CreateObjectServiceReadProxy(
                        Bootstrap_->GetRootClient(),
                        NApi::EMasterChannelKind::Follower,
                        cellTag);
                    auto req = TYPathProxy::Get("//sys/@queue_agent_object_revisions");
                    asyncResults.push_back(proxy.Execute(req));
                }
            }
            return AllSucceeded(std::move(asyncResults)).Apply(BIND([objectRevisions] (const std::vector<TYPathProxy::TRspGetPtr>& responses) mutable {
                for (const auto& rsp : responses) {
                    auto objects = ConvertTo<ObjectRevisionMap>(TYsonString{rsp->value()});
                    for (const auto& [key, items] : objects) {
                        objectRevisions[key].insert(items.begin(), items.end());
                    }
                }
                return ConvertToYsonString(objectRevisions);
            }));
        } else {
            return MakeFuture(ConvertToYsonString(objectRevisions));
        }
    }

    void TransformForeignSchemaIdsToNative() override
    {
        std::vector<std::unique_ptr<TMasterTableSchema>> schemasToUpdate;
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto cellTag = multicellManager->GetCellTag();
        for (auto it = MasterTableSchemaMap_.begin(); it != MasterTableSchemaMap_.end();) {
            auto schemaId = it->first;
            ++it;

            if (CellTagFromId(schemaId) != cellTag && schemaId != EmptyMasterTableSchemaId_) {
                auto schemaPtr = MasterTableSchemaMap_.Release(schemaId);

                auto newSchemaId = GenerateNativeSchemaIdFromForeign(schemaPtr->GetId(), cellTag);
                schemaPtr->SetId(newSchemaId);

                YT_LOG_ALERT("Updating schema ID for a native schema (NewSchemaId: %v, SchemaId: %v, Schema: %v)",
                    schemaId,
                    schemaPtr->AsTableSchema(),
                    newSchemaId);

                schemasToUpdate.push_back(std::move(schemaPtr));
            }
        }

        for (auto i = 0; i < std::ssize(schemasToUpdate); ++i) {
            auto schemaId = schemasToUpdate[i]->GetId();
            MasterTableSchemaMap_.Insert(schemaId, std::move(schemasToUpdate[i]));
        }
    }

    DEFINE_SIGNAL_OVERRIDE(void(TTableCollocationData), ReplicationCollocationUpdated);
    DEFINE_SIGNAL_OVERRIDE(void(TTableCollocationId), ReplicationCollocationDestroyed);

private:
    struct TStatisticsUpdateRequest
    {
        bool UpdateDataStatistics;
        bool UpdateTabletResourceUsage;
        bool UpdateModificationTime;
        bool UpdateAccessTime;
        bool UseNativeContentRevisionCas;

        void Persist(const NCellMaster::TPersistenceContext& context)
        {
            using NYT::Persist;

            Persist(context, UpdateDataStatistics);
            Persist(context, UpdateTabletResourceUsage);
            Persist(context, UpdateModificationTime);
            Persist(context, UpdateAccessTime);
            Persist(context, UseNativeContentRevisionCas);
        }
    };

    friend class TMasterTableSchemaTypeHandler;

    static const TTableSchema EmptyTableSchema;
    static const NYson::TYsonString EmptyYsonTableSchema;

    NHydra::TEntityMap<TMasterTableSchema> MasterTableSchemaMap_;

    TMasterTableSchema::TNativeTableSchemaToObjectMap NativeTableSchemaToObjectMap_;
    TMasterTableSchema::TImportedTableSchemaToObjectMap ImportedTableSchemaToObjectMap_;

    // COMPAT(h0pless): Remove this after schema migration is complete.
    TMasterTableSchemaId OldEmptyMasterTableSchemaId_;

    TMasterTableSchemaId EmptyMasterTableSchemaId_;
    TMasterTableSchema* EmptyMasterTableSchema_ = nullptr;

    NHydra::TEntityMap<TTableCollocation> TableCollocationMap_;

    TRandomAccessQueue<TNodeId, TStatisticsUpdateRequest> StatisticsUpdateRequests_;
    TPeriodicExecutorPtr StatisticsGossipExecutor_;
    IReconfigurableThroughputThrottlerPtr StatisticsGossipThrottler_;

    //! Contains native trunk nodes for which IsQueue() is true.
    THashSet<TTableNode*> Queues_;
    //! Contains native trunk nodes for which IsConsumer() is true.
    THashSet<TTableNode*> Consumers_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    const TDynamicTablesMulticellGossipConfigPtr& GetGossipConfig()
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        return configManager->GetConfig()->TabletManager->MulticellGossip;
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
    {
        const auto& gossipConfig = GetGossipConfig();
        StatisticsGossipThrottler_->Reconfigure(gossipConfig->TableStatisticsGossipThrottler);

        if (StatisticsGossipExecutor_) {
            StatisticsGossipExecutor_->SetPeriod(gossipConfig->TableStatisticsGossipPeriod);
        }
    }


    static bool IsSupportedNodeType(EObjectType type)
    {
        return IsTableType(type) || type == EObjectType::File || type == EObjectType::HunkStorage;
    }

    TMasterTableSchemaId GenerateNativeSchemaIdFromForeign(TMasterTableSchemaId oldSchemaId, TCellTag cellTag) {
        YT_VERIFY(oldSchemaId);

        auto imposterCellTag = CellTagFromId(oldSchemaId);
        auto type = TypeFromId(oldSchemaId);
        YT_VERIFY(type == EObjectType::MasterTableSchema);
        return TMasterTableSchemaId(
            (oldSchemaId.Parts32[0] & 0xffff) | (imposterCellTag << 16),   // keep the original cell tag
            (cellTag << 16) | static_cast<ui32>(type),                     // keep type and replace native cell tag
            oldSchemaId.Parts32[2],
            oldSchemaId.Parts32[3]);
    }

    void InitBuiltins()
    {
        if (EmptyMasterTableSchema_) {
            return;
        }

        EmptyMasterTableSchema_ = FindMasterTableSchema(EmptyMasterTableSchemaId_);

        if (EmptyMasterTableSchema_) {
            return;
        }

        EmptyMasterTableSchema_ = DoCreateMasterTableSchema(EmptyTableSchema, EmptyMasterTableSchemaId_, /* isNative */ true);
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        EmptyMasterTableSchema_->ExportRef(multicellManager->GetPrimaryCellTag());
        for (auto cellTag : multicellManager->GetSecondaryCellTags()) {
            EmptyMasterTableSchema_->ExportRef(cellTag);
        }

        // Bring export counter on the current cell to 0.
        EmptyMasterTableSchema_->UnexportRef(multicellManager->GetCellTag());

        YT_VERIFY(EmptyMasterTableSchema_->RefObject() == 1);
    }


    void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnLeaderActive();

        const auto& gossipConfig = GetGossipConfig();

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsSecondaryMaster()) {
            StatisticsGossipExecutor_ = New<TPeriodicExecutor>(
                Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::TabletGossip),
                BIND(&TTableManager::OnStatisticsGossip, MakeWeak(this)),
                gossipConfig->TableStatisticsGossipPeriod);
            StatisticsGossipExecutor_->Start();
        }
    }

    void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStopLeading();

        if (StatisticsGossipExecutor_) {
            StatisticsGossipExecutor_->Stop();
        }
    }


    void OnStatisticsGossip()
    {
        auto nodeCount = StatisticsUpdateRequests_.Size();
        nodeCount = StatisticsGossipThrottler_->TryAcquireAvailable(nodeCount);
        if (nodeCount == 0) {
            return;
        }

        NTableServer::NProto::TReqSendTableStatisticsUpdates request;
        request.set_node_count(nodeCount);
        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        CreateMutation(hydraManager, request)
            ->CommitAndLog(Logger);
    }

    void HydraSendTableStatisticsUpdates(NTableServer::NProto::TReqSendTableStatisticsUpdates* request)
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsSecondaryMaster());

        auto remainingNodeCount = request->node_count();

        std::vector<TNodeId> nodeIds;
        // NB: Ordered map is needed to make things deterministic.
        std::map<TCellTag, NTableServer::NProto::TReqUpdateTableStatistics> cellTagToRequest;
        while (remainingNodeCount-- > 0 && !StatisticsUpdateRequests_.IsEmpty()) {
            const auto& [nodeId, statistics] = StatisticsUpdateRequests_.Pop();

            auto* node = cypressManager->FindNode(TVersionedNodeId(nodeId));
            if (!IsObjectAlive(node)) {
                continue;
            }
            YT_VERIFY(IsSupportedNodeType(node->GetType()));

            if (statistics.UpdateTabletResourceUsage && !IsTabletOwnerType(node->GetType())) {
                YT_LOG_ALERT("Requested to send tablet resource usage update for a non-tablet owner node; ignored (NodeId: %v)",
                    nodeId);
                continue;
            }

            auto* chunkOwner = node->As<TChunkOwnerBase>();

            auto cellTag = CellTagFromId(nodeId);
            auto& request = cellTagToRequest[cellTag];
            auto* entry = request.add_entries();
            ToProto(entry->mutable_node_id(), nodeId);
            if (statistics.UpdateDataStatistics) {
                ToProto(entry->mutable_data_statistics(), chunkOwner->SnapshotStatistics());
            }
            if (statistics.UpdateTabletResourceUsage) {
                // Correctness of this cast has been verified above.
                auto* table = chunkOwner->As<TTableNode>();
                ToProto(entry->mutable_tablet_resource_usage(), table->GetTabletResourceUsage());
            }
            if (statistics.UpdateModificationTime) {
                entry->set_modification_time(ToProto<ui64>(chunkOwner->GetModificationTime()));
            }
            if (statistics.UpdateAccessTime) {
                entry->set_access_time(ToProto<ui64>(chunkOwner->GetAccessTime()));
            }
            if (statistics.UseNativeContentRevisionCas) {
                entry->set_expected_content_revision(chunkOwner->GetNativeContentRevision());
            }
            nodeIds.push_back(nodeId);
        }

        YT_LOG_DEBUG("Sending node statistics update (RequestedNodeCount: %v, NodeIds: %v)",
            request->node_count(),
            nodeIds);

        for (const auto& [cellTag, request] : cellTagToRequest) {
            multicellManager->PostToMaster(request, cellTag);
        }
    }

    void HydraUpdateTableStatistics(NTableServer::NProto::TReqUpdateTableStatistics* request)
    {
        TCompactVector<TTableId, 8> nodeIds;
        nodeIds.reserve(request->entries_size());
        for (const auto& entry : request->entries()) {
            nodeIds.push_back(FromProto<TNodeId>(entry.node_id()));
        }

        YT_LOG_DEBUG("Received node statistics update (NodeIds: %v)",
            nodeIds);

        TCompactVector<TTableId, 8> nodeIdsToRetry; // Just for logging.
        NTableServer::NProto::TReqNotifyContentRevisionCasFailed retryRequest;

        auto externalCellTag = InvalidCellTag;
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        for (const auto& entry : request->entries()) {
            auto nodeId = FromProto<TTableId>(entry.node_id());
            auto* node = cypressManager->FindNode(TVersionedNodeId(nodeId));
            if (!IsObjectAlive(node)) {
                continue;
            }

            YT_VERIFY(IsSupportedNodeType(node->GetType()));
            auto* chunkOwner = node->As<TChunkOwnerBase>();

            YT_VERIFY(
                externalCellTag == InvalidCellTag ||
                externalCellTag == chunkOwner->GetExternalCellTag());
            externalCellTag = chunkOwner->GetExternalCellTag();

            if (entry.has_expected_content_revision() &&
                chunkOwner->GetContentRevision() != entry.expected_content_revision())
            {
                nodeIdsToRetry.push_back(nodeId);
                auto* retryEntry = retryRequest.add_entries();
                ToProto(retryEntry->mutable_node_id(), nodeId);
                retryEntry->set_update_data_statistics(entry.has_data_statistics());
                retryEntry->set_update_tablet_statistics(entry.has_tablet_resource_usage());
                // Sending actual revisions fits nicely into the protocol but, strictly
                // speaking, this is only necessary for migrating on update. (A newly
                // updated external cell doesn't know native content revisions - and it
                // has no way of acquiring that knowledge other than these notifications.)
                retryEntry->set_actual_content_revision(chunkOwner->GetContentRevision());
                continue;
            }

            if (entry.has_tablet_resource_usage()) {
                if (IsTabletOwnerType(chunkOwner->GetType())) {
                    auto* table = chunkOwner->As<TTableNode>();
                    auto tabletResourceUsage = FromProto<TTabletResources>(entry.tablet_resource_usage());
                    table->SetExternalTabletResourceUsage(tabletResourceUsage);
                } else {
                    YT_LOG_ALERT("Received tablet resource usage update for a non-tablet owner node (NodeId: %v)",
                        nodeId);
                }
            }

            if (entry.has_data_statistics()) {
                chunkOwner->SnapshotStatistics() = entry.data_statistics();
            }

            if (entry.has_modification_time()) {
                chunkOwner->SetModificationTime(std::max(
                    chunkOwner->GetModificationTime(),
                    FromProto<TInstant>(entry.modification_time())));
            }

            if (entry.has_access_time()) {
                chunkOwner->SetAccessTime(std::max(
                    chunkOwner->GetAccessTime(),
                    FromProto<TInstant>(entry.access_time())));
            }
        }

        if (!nodeIdsToRetry.empty()) {
            YT_VERIFY(std::ssize(nodeIdsToRetry) == retryRequest.entries_size());

            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            multicellManager->PostToMaster(retryRequest, externalCellTag);

            YT_LOG_DEBUG("Content revision CASes failed, requesting retries (NodeIds: %v)",
                nodeIdsToRetry);
        }
    }

    void HydraNotifyContentRevisionCasFailed(NTableServer::NProto::TReqNotifyContentRevisionCasFailed* request)
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();

        for (const auto& entry : request->entries()) {
            auto nodeId = FromProto<TNodeId>(entry.node_id());
            auto* node = cypressManager->FindNode(TVersionedNodeId(nodeId));
            if (!IsObjectAlive(node)) {
                continue;
            }

            YT_VERIFY(IsSupportedNodeType(node->GetType()));
            auto* chunkOwner = node->As<TChunkOwnerBase>();

            auto actualContentRevision = entry.actual_content_revision();
            if (actualContentRevision >= chunkOwner->GetNativeContentRevision()) {
                chunkOwner->SetNativeContentRevision(actualContentRevision);
            } else {
                YT_LOG_ALERT("Received non-monotonic revision with content revision CAS failure notification; ignored (NodeId: %v, ReceivedRevision: %x, NodeRevision: %x)",
                    nodeId,
                    actualContentRevision,
                    chunkOwner->GetNativeContentRevision());
            }

            ScheduleStatisticsUpdate(
                chunkOwner,
                entry.update_data_statistics(),
                entry.update_tablet_statistics(),
                /*useNativeContentRevisionCas*/ true);
        }
    }


    void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::Clear();

        MasterTableSchemaMap_.Clear();
        NativeTableSchemaToObjectMap_.clear();
        ImportedTableSchemaToObjectMap_.clear();
        EmptyMasterTableSchema_ = nullptr;
        TableCollocationMap_.Clear();
        StatisticsUpdateRequests_.Clear();
        Queues_.clear();
        Consumers_.clear();
    }

    void SetZeroState() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TCompositeAutomatonPart::SetZeroState();

        InitBuiltins();
    }

    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        MasterTableSchemaMap_.LoadKeys(context);
        TableCollocationMap_.LoadKeys(context);
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        MasterTableSchemaMap_.LoadValues(context);

        // COMPAT(h0pless)
        if (context.GetVersion() < EMasterReign::ExportMasterTableSchemas) {
            // Forcefully resetting old empty schema.
            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            if (!multicellManager->IsPrimaryMaster()) {
                const auto& objectManager = Bootstrap_->GetObjectManager();
                auto* oldEmptyMasterTableSchema_ = GetMasterTableSchema(OldEmptyMasterTableSchemaId_);

                objectManager->UnrefObject(oldEmptyMasterTableSchema_);
                NativeTableSchemaToObjectMap_.erase(oldEmptyMasterTableSchema_->GetNativeTableSchemaToObjectMapIterator());
                oldEmptyMasterTableSchema_->ResetNativeTableSchemaToObjectMapIterator();

                EmptyMasterTableSchema_ = DoCreateMasterTableSchema(
                    EmptyTableSchema,
                    EmptyMasterTableSchemaId_,
                    /* isNative */ true);
                YT_VERIFY(EmptyMasterTableSchema_->RefObject() == 1);
            } else {
                EmptyMasterTableSchema_ = GetMasterTableSchema(EmptyMasterTableSchemaId_);
            }

            EmptyMasterTableSchema_->ExportRef(multicellManager->GetPrimaryCellTag());
            for (auto cellTag : multicellManager->GetSecondaryCellTags()) {
                EmptyMasterTableSchema_->ExportRef(cellTag);
            }

            // Bring export counter on the current cell to 0.
            EmptyMasterTableSchema_->UnexportRef(multicellManager->GetCellTag());
        }

        Load(context, StatisticsUpdateRequests_);
        TableCollocationMap_.LoadValues(context);

        Load(context, Queues_);
        Load(context, Consumers_);
    }

    void OnBeforeSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnBeforeSnapshotLoaded();
    }

    void OnAfterSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        // Usually the empty table schema, like any other, is loaded from snapshot.
        // However:
        //   - we still need to initialize the pointer;
        //   - on old snapshots that don't contain schema map (or this automaton
        //     part altogether) this initialization is crucial.
        InitBuiltins();
    }

    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        MasterTableSchemaMap_.SaveKeys(context);
        TableCollocationMap_.SaveKeys(context);
    }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        MasterTableSchemaMap_.SaveValues(context);
        Save(context, StatisticsUpdateRequests_);
        TableCollocationMap_.SaveValues(context);
        Save(context, Queues_);
        Save(context, Consumers_);
    }

    void OnReplicationCollocationUpdated(TTableCollocation* collocation)
    {
        std::vector<TTableId> tableIds;
        tableIds.reserve(collocation->Tables().size());
        for (auto* table : collocation->Tables()) {
            if (IsObjectAlive(table)) {
                tableIds.push_back(table->GetId());
            }
        }
        if (!tableIds.empty()) {
            ReplicationCollocationUpdated_.Fire(TTableCollocationData{
                .Id = collocation->GetId(),
                .TableIds = std::move(tableIds),
            });
        }
    }
};

DEFINE_ENTITY_MAP_ACCESSORS(TTableManager, MasterTableSchema, TMasterTableSchema, MasterTableSchemaMap_);
DEFINE_ENTITY_MAP_ACCESSORS(TTableManager, TableCollocation, TTableCollocation, TableCollocationMap_);

////////////////////////////////////////////////////////////////////////////////

const TTableSchema TTableManager::EmptyTableSchema;
const NYson::TYsonString TTableManager::EmptyYsonTableSchema = BuildYsonStringFluently().Value(EmptyTableSchema);

////////////////////////////////////////////////////////////////////////////////

TMasterTableSchemaTypeHandler::TMasterTableSchemaTypeHandler(TTableManager* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->MasterTableSchemaMap_)
    , Owner_(owner)
{ }

TObject* TMasterTableSchemaTypeHandler::CreateObject(
    TObjectId /*hintId*/,
    IAttributeDictionary* /*attributes*/)
{
    THROW_ERROR_EXCEPTION("%Qv objects cannot be created manually", EObjectType::MasterTableSchema);
}

IObjectProxyPtr TMasterTableSchemaTypeHandler::DoGetProxy(
    TMasterTableSchema* schema,
    TTransaction* /*transaction*/)
{
    return CreateMasterTableSchemaProxy(Owner_->Bootstrap_, &Metadata_, schema);
}

void TMasterTableSchemaTypeHandler::DoZombifyObject(TMasterTableSchema* schema)
{
    TObjectTypeHandlerWithMapBase::DoZombifyObject(schema);
    Owner_->ZombifyMasterTableSchema(schema);
}

////////////////////////////////////////////////////////////////////////////////

ITableManagerPtr CreateTableManager(TBootstrap* bootstrap)
{
    return New<TTableManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
