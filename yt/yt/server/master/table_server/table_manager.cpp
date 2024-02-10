#include "table_manager.h"
#include "private.h"
#include "config.h"
#include "master_table_schema_proxy.h"
#include "mount_config_attributes.h"
#include "replicated_table_node.h"
#include "schemaful_node.h"
#include "secondary_index.h"
#include "secondary_index_type_handler.h"
#include "table_collocation.h"
#include "table_collocation_type_handler.h"
#include "table_node.h"

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>

#include <yt/yt/server/master/chaos_server/chaos_manager.h>
#include <yt/yt/server/master/chaos_server/chaos_replicated_table_node.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
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
using namespace NLogging;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NProto;
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
        RegisterMethod(BIND(&TTableManager::HydraConfirmTableStatisticsUpdate, Unretained(this)), /*aliases*/ {"NYT.NTabletServer.NProto.TReqNotifyContentRevisionCasFailed"});

        RegisterMethod(BIND(&TTableManager::HydraImportMasterTableSchema, Unretained(this)));
        RegisterMethod(BIND(&TTableManager::HydraUnimportMasterTableSchema, Unretained(this)));

        // COMPAT(h0pless): RefactorSchemaExport
        RegisterMethod(BIND(&TTableManager::HydraTakeArtificialRef, Unretained(this)));

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(New<TMasterTableSchemaTypeHandler>(this));
        objectManager->RegisterHandler(CreateTableCollocationTypeHandler(Bootstrap_, &TableCollocationMap_));
        objectManager->RegisterHandler(CreateSecondaryIndexTypeHandler(Bootstrap_, &SecondaryIndexMap_));

        // COMPAT(h0pless): Remove this after empty schema migration is complete.
        auto primaryCellTag = Bootstrap_->GetMulticellManager()->GetPrimaryCellTag();
        PrimaryCellEmptyMasterTableSchemaId_ = MakeWellKnownId(EObjectType::MasterTableSchema, primaryCellTag, 0xffffffffffffffff);

        auto cellTag = Bootstrap_->GetMulticellManager()->GetCellTag();
        EmptyMasterTableSchemaId_ = MakeWellKnownId(EObjectType::MasterTableSchema, cellTag, 0xffffffffffffffff);
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

            auto& ongoingUpdate = NodeIdToOngoingStatisticsUpdate_[chunkOwner->GetId()];
            ongoingUpdate.RequestCount++;
            ongoingUpdate.EffectiveRequest |= statistics;
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

        TStatisticsUpdateRequest statistics{
            .UpdateDataStatistics = true,
            .UpdateTabletResourceUsage = IsTableType(chunkOwner->GetType()),
            .UpdateModificationTime = true,
            .UpdateAccessTime = true,
            .UseNativeContentRevisionCas = useNativeContentRevisionCas,
        };
        NTableServer::NProto::TReqUpdateTableStatistics req;
        auto* entry = req.add_entries();
        ToProto(entry->mutable_node_id(), chunkOwner->GetId());
        ToProto(entry->mutable_data_statistics(), chunkOwner->SnapshotStatistics());
        if (statistics.UpdateTabletResourceUsage) {
            auto* table = chunkOwner->As<TTableNode>();
            ToProto(entry->mutable_tablet_resource_usage(), table->GetTabletResourceUsage());
        }
        entry->set_modification_time(ToProto<ui64>(chunkOwner->GetModificationTime()));
        entry->set_access_time(ToProto<ui64>(chunkOwner->GetAccessTime()));
        if (statistics.UseNativeContentRevisionCas) {
            entry->set_expected_content_revision(chunkOwner->GetNativeContentRevision());
        }
        multicellManager->PostToMaster(req, chunkOwner->GetNativeCellTag());

        StatisticsUpdateRequests_.Pop(chunkOwner->GetId());
        auto& ongoingUpdate = NodeIdToOngoingStatisticsUpdate_[chunkOwner->GetId()];
        ongoingUpdate.RequestCount++;
        ongoingUpdate.EffectiveRequest |= statistics;
    }


    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(MasterTableSchema, TMasterTableSchema);
    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(TableCollocation, TTableCollocation);
    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(SecondaryIndex, TSecondaryIndex);


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

    void SetTableSchema(TSchemafulNode* table, TMasterTableSchema* schema) override
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

        if (table->IsExternal()) {
            ExportMasterTableSchema(schema, table->GetExternalCellTag());
        }

        auto* tableAccount = table->GetAccount();
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->UpdateMasterMemoryUsage(schema, tableAccount, 1);
    }

    void SetTableSchemaOrThrow(TSchemafulNode* table, TMasterTableSchemaId schemaId) override
    {
        auto* existingSchema = GetMasterTableSchemaOrThrow(schemaId);
        SetTableSchema(table, existingSchema);
    }

    void SetTableSchemaOrCrash(TSchemafulNode* table, TMasterTableSchemaId schemaId) override
    {
        auto* existingSchema = GetMasterTableSchema(schemaId);
        SetTableSchema(table, existingSchema);
    }

    void ResetTableSchema(TSchemafulNode* table) override
    {
        auto* oldSchema = table->GetSchema();
        if (!oldSchema) {
            return;
        }
        YT_VERIFY(IsObjectAlive(oldSchema));

        if (table->IsExternal()) {
            UnexportMasterTableSchema(oldSchema, table->GetExternalCellTag());
        }

        table->SetSchema(nullptr);

        if (auto* account = table->GetAccount()) {
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            securityManager->UpdateMasterMemoryUsage(oldSchema, account, -1);
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->UnrefObject(oldSchema);
    }

    void SetChunkSchema(TChunk* chunk, TMasterTableSchema* schema) override
    {
        YT_VERIFY(schema);

        // Since chunk is immutable, schema should never change.
        auto& chunkSchema =  chunk->Schema();
        YT_VERIFY(!chunkSchema);

        // NB: a newly created schema object has zero reference count.
        // Thus the new schema may technically be not alive here.
        YT_VERIFY(!schema->IsGhost());

        chunkSchema = TMasterTableSchemaPtr(schema);
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

    TMasterTableSchema* CreateImportedMasterTableSchema(
        const NTableClient::TTableSchema& tableSchema,
        TMasterTableSchemaId hintId) override
    {
        // NB: An existing schema can be recreated without resurrection.
        auto* masterTableSchema = CreateMasterTableSchema(tableSchema, /*isNative*/ false, hintId);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        // All imported schemas should have an artificial ref.
        // This is done to make sure that native cell manages schema lifetime.
        objectManager->RefObject(masterTableSchema);

        return masterTableSchema;
    }

    TMasterTableSchema* CreateImportedTemporaryMasterTableSchema(
        const NTableClient::TTableSchema& tableSchema,
        TTransaction* schemaHolder,
        TMasterTableSchemaId hintId) override
    {
        YT_VERIFY(IsObjectAlive(schemaHolder));

        auto* schema = FindMasterTableSchema(hintId);
        if (!schema) {
            schema = DoCreateMasterTableSchema(tableSchema, hintId, /*isNative*/ false);
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

    TMasterTableSchema* GetOrCreateNativeMasterTableSchema(
        const TTableSchema& schema,
        TSchemafulNode* schemaHolder) override
    {
        auto* masterTableSchema = FindNativeMasterTableSchema(schema);
        if (!masterTableSchema) {
            masterTableSchema = CreateMasterTableSchema(schema, /*isNative*/ true);
        }

        SetTableSchema(schemaHolder, masterTableSchema);
        YT_VERIFY(IsObjectAlive(masterTableSchema));

        return masterTableSchema;
    }

    TMasterTableSchema* GetOrCreateNativeMasterTableSchema(
        const TTableSchema& schema,
        TTransaction* schemaHolder) override
    {
        YT_VERIFY(IsObjectAlive(schemaHolder));

        auto* masterTableSchema = FindNativeMasterTableSchema(schema);
        if (!masterTableSchema) {
            masterTableSchema = CreateMasterTableSchema(schema, /*isNative*/ true);
        }

        if (!schemaHolder->StagedObjects().contains(masterTableSchema)) {
            const auto& transactionManager = Bootstrap_->GetTransactionManager();
            transactionManager->StageObject(schemaHolder, masterTableSchema);
        }
        YT_VERIFY(IsObjectAlive(masterTableSchema));

        return masterTableSchema;
    }

    TMasterTableSchema* GetOrCreateNativeMasterTableSchema(
        const NTableClient::TTableSchema& schema,
        TChunk* schemaHolder) override
    {
        // NB: a newly created chunk in operation can have zero reference count.
        // Thus it may technically be not alive here.
        YT_VERIFY(!schemaHolder->IsGhost());

        auto* masterTableSchema = FindNativeMasterTableSchema(schema);
        if (!masterTableSchema) {
            masterTableSchema = CreateMasterTableSchema(schema, /*isNative*/ true);
        }

        SetChunkSchema(schemaHolder, masterTableSchema);
        YT_VERIFY(IsObjectAlive(masterTableSchema));

        return masterTableSchema;
    }

    TMasterTableSchema* CreateMasterTableSchema(
        const TTableSchema& tableSchema,
        bool isNative,
        TMasterTableSchemaId hintId = NObjectClient::NullObjectId)
    {
        auto* schema = FindMasterTableSchema(hintId);
        if (!schema) {
            schema = DoCreateMasterTableSchema(tableSchema, hintId, isNative);
        } else if (!IsObjectAlive(schema)) {
            ResurrectMasterTableSchema(schema);
            YT_VERIFY(*schema->TableSchema_ == tableSchema);
        } else {
            // Just a sanity check.
            YT_ASSERT(*schema->TableSchema_ == tableSchema);
        }

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
            auto schemaHolder = TPoolAllocator::New<TMasterTableSchema>(
                id,
                std::move(sharedTableSchema));
            schema = MasterTableSchemaMap_.Insert(id, std::move(schemaHolder));
        }

        YT_VERIFY(schema->CellTagToExportCount().empty());
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

        if (schema->IsNative()) {
            if (auto it = schema->GetNativeTableSchemaToObjectMapIterator()) {
                NativeTableSchemaToObjectMap_.erase(it);
            }
            schema->ResetNativeTableSchemaToObjectMapIterator();
        }
        // There's no reverse index for imported schemas.
    }

    void ResurrectMasterTableSchema(TMasterTableSchema* schema) {
        YT_VERIFY(!schema->IsDisposed());
        YT_VERIFY(FindMasterTableSchema(schema->GetId()));

        YT_LOG_DEBUG("Resurrecting master table schema object (SchemaId: %v)",
            schema->GetId());

        const auto& tableSchema = schema->AsTableSchema(/*crashOnZombie*/ false);
        if (schema->IsNative()) {
            auto it = EmplaceOrCrash(
                NativeTableSchemaToObjectMap_,
                tableSchema,
                schema);
            schema->SetNativeTableSchemaToObjectMapIterator(it);
        }
        // There's no reverse index for imported schemas.
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

    void ExportMasterTableSchema(TMasterTableSchema* schema, TCellTag dstCellTag) override
    {
        YT_VERIFY(schema);

        if (!schema->IsExported(dstCellTag)) {
            TReqImportMasterTableSchema request;

            ToProto(request.mutable_schema_id(), schema->GetId());
            // NB: this sometimes will lead to a synchronous serialization of
            // schemas in automaton thread. This is unavoidable.
            ToProto(request.mutable_schema(), schema->AsTableSchema());

            YT_LOG_DEBUG("Exporting schema to another cell (SchemaId: %v, DestinationCellTag: %v)",
                schema->GetId(),
                dstCellTag);

            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            multicellManager->PostToMaster(request, dstCellTag);
        }

        schema->ExportRef(dstCellTag);
    }

    void UnexportMasterTableSchema(TMasterTableSchema* schema, TCellTag dstCellTag, int decreaseBy = 1) override
    {
        YT_VERIFY(schema && schema->IsExported(dstCellTag));

        schema->UnexportRef(dstCellTag, decreaseBy);

        if (!schema->IsExported(dstCellTag)) {
            TReqUnimportMasterTableSchema request;
            ToProto(request.mutable_schema_id(), schema->GetId());

            YT_LOG_DEBUG("Unexporting schema from another cell (SchemaId: %v, DestinationCellTag: %v)",
                schema->GetId(),
                dstCellTag);

            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            multicellManager->PostToMaster(request, dstCellTag);
        }
    }

    void ValidateTableSchemaCorrespondence(
        TVersionedNodeId nodeId,
        const TTableSchemaPtr& schema,
        TMasterTableSchemaId schemaId,
        bool isChunkSchema = false) override
    {
        auto type = TypeFromId(nodeId.ObjectId);
        YT_VERIFY(IsSchemafulType(type));

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto nativeCellTag = CellTagFromId(nodeId.ObjectId);
        auto native = nativeCellTag == multicellManager->GetCellTag();

        // I know that this is a hack, and not even a good one.
        // But I really don't want to butcher this validation code everywhere else
        // just to make a temporary compat message a tiny bit prettier.
        // COMPAT(h0pless): AddChunkSchemas
        auto messagePart = isChunkSchema ? "chunk schema" : "schema";

        const auto& tableManager = Bootstrap_->GetTableManager();
        TMasterTableSchema* schemaById = nullptr;
        if (native) {
            if (schemaId) {
                schemaById = tableManager->GetMasterTableSchemaOrThrow(schemaId);
            }
        } else {
            // On external cells schemaId should always be present.
            // COMPAT(h0pless): Change this to YT_VERIFY after schema migration is complete.
            YT_LOG_ALERT_IF(!schemaId, "Request to create a foreign node has no %v id on external cell "
                "(NodeId: %v, NativeCellTag: %v, CellTag: %v)",
                messagePart,
                nodeId,
                nativeCellTag,
                multicellManager->GetCellTag());

            schemaById = FindMasterTableSchema(schemaId);

            if (!schemaById) {
                // COMPAT(h0pless): Change this to YT_VERIFY after schema migration is complete.
                YT_LOG_ALERT_IF(!schema, "Request to create a foreign node has %v id of an unimported schema on external cell "
                    "(NodeId: %v, NativeCellTag: %v, CellTag: %v, SchemaId: %v)",
                    messagePart,
                    nodeId,
                    nativeCellTag,
                    multicellManager->GetCellTag(),
                    schemaId);
            }
        }

        if (schema && schemaById && schemaById->IsNative()) {
            auto* schemaByYson = tableManager->FindNativeMasterTableSchema(*schema);
            if (IsObjectAlive(schemaByYson)) {
                if (schemaById != schemaByYson) {
                    THROW_ERROR_EXCEPTION("Both \"schema\" and \"schema_id\" specified and they refer to different schemas");
                }
            } else {
                if (*schemaById->AsTableSchema(/*crashOnZombie*/ false) != *schema) {
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
            effectiveTableSchema = schemaById->AsTableSchema(/*crashOnZombie*/ false).Get();
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

    TSecondaryIndex* CreateSecondaryIndex(
        TObjectId hintId,
        ESecondaryIndexKind kind,
        TTableNode* table,
        TTableNode* indexTable) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto validate = [&] {
            if (!indexTable->SecondaryIndices().empty()) {
                THROW_ERROR_EXCEPTION("Cannot use a table with indices as an index");
            }
            if (indexTable->GetIndexTo()) {
                THROW_ERROR_EXCEPTION("Index cannot have multiple primary tables");
            }

            if (table->GetExternalCellTag() != indexTable->GetExternalCellTag()) {
                THROW_ERROR_EXCEPTION("Table and index table external cell tags differ")
                    << TErrorAttribute("table_external_cell_tag", table->GetExternalCellTag())
                    << TErrorAttribute("index_table_external_cell_tag", indexTable->GetExternalCellTag());
            }

            table->ValidateAllTabletsUnmounted("Cannot create index on a mounted table");
        };

        if (table->IsNative()) {
            validate();
        } else {
            try {
                validate();
            } catch (const std::exception& ex) {
                YT_LOG_ALERT(ex, "Index creation validation failed on the foreign cell");
            }
        }

        auto tableType = TypeFromId(table->GetId());
        auto indexTableType = TypeFromId(indexTable->GetId());
        if (tableType != indexTableType) {
            THROW_ERROR_EXCEPTION("Type mismatch: trying to create index of type %Qlv for a table of type %Qlv",
                indexTableType,
                tableType);
        }

        switch (tableType) {
            case EObjectType::Table: {
                break;
            }

            case EObjectType::ReplicatedTable: {
                auto* tableCollocation = table->GetReplicationCollocation();
                auto* indexCollocation = indexTable->GetReplicationCollocation();

                if (tableCollocation == nullptr || tableCollocation != indexCollocation) {
                    auto tableCollocationId = tableCollocation ? tableCollocation->GetId() : NullObjectId;
                    auto indexCollocationId = indexCollocation ? indexCollocation->GetId() : NullObjectId;
                    THROW_ERROR_EXCEPTION("Table and index table must belong to the same non-null table collocation")
                        << TErrorAttribute("table_collocation_id", tableCollocationId)
                        << TErrorAttribute("index_table_collocation_id", indexCollocationId);
                }

                if (auto type = tableCollocation->GetType(); type != ETableCollocationType::Replication) {
                    THROW_ERROR_EXCEPTION("Unsupported collocation type %Qlv", type);
                }

                break;
            }

            default:
                THROW_ERROR_EXCEPTION("Unsupported table type %Qlv", tableType);
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto secondaryIndexId = objectManager->GenerateId(EObjectType::SecondaryIndex, hintId);

        auto* secondaryIndex = SecondaryIndexMap_.Insert(
            secondaryIndexId,
            TPoolAllocator::New<TSecondaryIndex>(secondaryIndexId));
        // A single reference ensures index object is deleted when either primary or index table is deleted.
        YT_VERIFY(secondaryIndex->RefObject() == 1);
        secondaryIndex->SetKind(kind);
        secondaryIndex->SetTable(table);
        secondaryIndex->SetIndexTable(indexTable);
        secondaryIndex->SetExternalCellTag(table->IsNative()
            ? table->GetExternalCellTag()
            : NotReplicatedCellTagSentinel);

        indexTable->SetIndexTo(secondaryIndex);
        InsertOrCrash(table->MutableSecondaryIndices(), secondaryIndex);

        return secondaryIndex;
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
                if (Bootstrap_->GetConfigManager()->GetConfig()->TabletManager->ReplicateTableCollocations) {
                    THROW_ERROR_EXCEPTION("Collocated tables must have same external cell tag, found %v and %v",
                        externalCellTag,
                        tableExternalCellTag);
                }
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
                OnReplicationCollocationCreated(collocation);
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
                    YT_VERIFY(!table->GetIndexTo() && table->SecondaryIndices().empty());
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
            if (Bootstrap_->GetConfigManager()->GetConfig()->TabletManager->ReplicateTableCollocations) {
                THROW_ERROR_EXCEPTION("Collocated tables must have same external cell tag, found %v and %v",
                    collocation->GetExternalCellTag(),
                    table->GetExternalCellTag());
            }
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
                OnReplicationCollocationCreated(collocation);
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
                YT_VERIFY(table->SecondaryIndices().empty() && !table->GetIndexTo());
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

        OnReplicationCollocationCreated(collocation);

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
                "Attempting to register a consumer twice (Node: %v, Path: %v)",
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
                "Attempting to unregister an unknown consumer (Node: %v, Path: %v)",
                node->GetId(),
                Bootstrap_->GetCypressManager()->GetNodePath(node, /*transaction*/ nullptr));
        }
    }

    TFuture<TYsonString> GetQueueAgentObjectRevisionsAsync() const override
    {
        Bootstrap_->VerifyPersistentStateRead();

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& chaosManager = Bootstrap_->GetChaosManager();

        using ObjectRevisionMap = THashMap<TString, THashMap<TString, TRevision>>;

        ObjectRevisionMap objectRevisions;
        auto addToObjectRevisions = [&] <typename T>(const TString& key, const THashSet<T>& nodes) {
            static_assert(std::is_same_v<T, TTableNode*> || std::is_same_v<T, NChaosServer::TChaosReplicatedTableNode*>,
                "Templated type T has to be TTableNode* or TChaosReplicatedTableNode");
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
        objectRevisions["queues"] = {};
        objectRevisions["consumers"] = {};
        addToObjectRevisions("queues", GetQueues());
        addToObjectRevisions("consumers", GetConsumers());
        addToObjectRevisions("queues", chaosManager->GetQueues());
        addToObjectRevisions("consumers", chaosManager->GetConsumers());

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

            if (CellTagFromId(schemaId) != cellTag) {
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

    DEFINE_SIGNAL_OVERRIDE(void(TTableCollocationData), ReplicationCollocationCreated);
    DEFINE_SIGNAL_OVERRIDE(void(TTableCollocationId), ReplicationCollocationDestroyed);

private:
    struct TStatisticsUpdateRequest
    {
        bool UpdateDataStatistics = false;
        bool UpdateTabletResourceUsage = false;
        bool UpdateModificationTime = false;
        bool UpdateAccessTime = false;
        bool UseNativeContentRevisionCas = false;

        void Persist(const NCellMaster::TPersistenceContext& context)
        {
            using NYT::Persist;

            Persist(context, UpdateDataStatistics);
            Persist(context, UpdateTabletResourceUsage);
            Persist(context, UpdateModificationTime);
            Persist(context, UpdateAccessTime);
            Persist(context, UseNativeContentRevisionCas);
        }

        TStatisticsUpdateRequest& operator|=(const TStatisticsUpdateRequest& rhs)
        {
            UpdateDataStatistics |= rhs.UpdateDataStatistics;
            UpdateTabletResourceUsage |= rhs.UpdateTabletResourceUsage;
            UpdateModificationTime |= rhs.UpdateModificationTime;
            UpdateAccessTime |= rhs.UpdateAccessTime;
            UseNativeContentRevisionCas |= rhs.UseNativeContentRevisionCas;
            return *this;
        }
    };

    struct TOngoingStatisticsUpdate
    {
        i64 RequestCount = 0;
        TStatisticsUpdateRequest EffectiveRequest;

        void Persist(const NCellMaster::TPersistenceContext& context)
        {
            using NYT::Persist;

            Persist(context, RequestCount);
            Persist(context, EffectiveRequest);
        }
    };

    friend class TMasterTableSchemaTypeHandler;

    static const TTableSchema EmptyTableSchema;
    static const NYson::TYsonString EmptyYsonTableSchema;

    NHydra::TEntityMap<TMasterTableSchema> MasterTableSchemaMap_;

    TMasterTableSchema::TNativeTableSchemaToObjectMap NativeTableSchemaToObjectMap_;

    // COMPAT(h0pless): Remove this after empty schema migration is complete.
    TMasterTableSchemaId PrimaryCellEmptyMasterTableSchemaId_;

    // COMPAT(h0pless): RefactorSchemaExport
    bool NeedTakeArtificialRef_ = false;

    TMasterTableSchemaId EmptyMasterTableSchemaId_;
    TMasterTableSchema* EmptyMasterTableSchema_ = nullptr;

    NHydra::TEntityMap<TTableCollocation> TableCollocationMap_;

    NHydra::TEntityMap<TSecondaryIndex> SecondaryIndexMap_;

    TRandomAccessQueue<TNodeId, TStatisticsUpdateRequest> StatisticsUpdateRequests_;
    THashMap<TNodeId, TOngoingStatisticsUpdate> NodeIdToOngoingStatisticsUpdate_;
    TPeriodicExecutorPtr StatisticsGossipExecutor_;
    IReconfigurableThroughputThrottlerPtr StatisticsGossipThrottler_;

    // COMPAT(cherepashka, achulkov2)
    bool NeedToAddReplicatedQueues_ = false;

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
            // keep the original cell tag
            (oldSchemaId.Parts32[0] & 0xffff) | (static_cast<ui32>(imposterCellTag.Underlying()) << 16),
            // keep type and replace native cell tag
            (static_cast<ui32>(cellTag.Underlying()) << 16) | static_cast<ui32>(type),
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

        EmptyMasterTableSchema_ = DoCreateMasterTableSchema(EmptyTableSchema, EmptyMasterTableSchemaId_, /*isNative*/ true);
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
            YT_UNUSED_FUTURE(StatisticsGossipExecutor_->Stop());
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
        YT_UNUSED_FUTURE(CreateMutation(hydraManager, request)
            ->CommitAndLog(Logger));
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
        NTableServer::NProto::TReqConfirmTableStatisticsUpdate confirmRequest;

        auto externalCellTag = InvalidCellTag;
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        for (const auto& entry : request->entries()) {
            auto nodeId = FromProto<TTableId>(entry.node_id());
            ToProto(confirmRequest.add_requested_node_ids(), nodeId);
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
                auto* retryEntry = confirmRequest.add_content_revision_cas_failed_nodes();
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
            YT_VERIFY(std::ssize(nodeIdsToRetry) == confirmRequest.content_revision_cas_failed_nodes_size());

            YT_LOG_DEBUG("Content revision CASes failed, requesting retries (NodeIds: %v)",
                nodeIdsToRetry);
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        multicellManager->PostToMaster(confirmRequest, externalCellTag);
    }

    void HydraConfirmTableStatisticsUpdate(NTableServer::NProto::TReqConfirmTableStatisticsUpdate* request)
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();

        for (const auto& entry : request->content_revision_cas_failed_nodes()) {
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

        for (const auto& protoNodeId : request->requested_node_ids()) {
            auto nodeId = FromProto<TNodeId>(protoNodeId);
            auto it = NodeIdToOngoingStatisticsUpdate_.find(nodeId);
            if (it == NodeIdToOngoingStatisticsUpdate_.end()) {
                YT_LOG_WARNING("Received excessive statistics update confirmation for node; ignored (NodeId: %v)",
                    nodeId);
                continue;
            }

            auto& count = it->second.RequestCount;
            YT_VERIFY(count > 0);
            if (--count == 0) {
                NodeIdToOngoingStatisticsUpdate_.erase(it);
            }
        }
    }

    void HydraImportMasterTableSchema(NProto::TReqImportMasterTableSchema* request)
    {
        auto schema = FromProto<TTableSchema>(request->schema());
        auto schemaId = FromProto<TMasterTableSchemaId>(request->schema_id());
        CreateImportedMasterTableSchema(schema, schemaId);
    }

    void HydraUnimportMasterTableSchema(NProto::TReqUnimportMasterTableSchema* request)
    {
        auto schemaId = FromProto<TMasterTableSchemaId>(request->schema_id());
        auto* schema = GetMasterTableSchema(schemaId);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        // Getting rid of an artificial ref.
        objectManager->UnrefObject(schema);
    }

    void HydraTakeArtificialRef(NProto::TReqTakeArtificialRef* request)
    {
        YT_LOG_DEBUG("Received TakeArtificialRef request (RequestSize: %v)",
            request->schema_ids_size());
        for (auto protoSchemaId : request->schema_ids()) {
            auto schemaId = FromProto<TMasterTableSchemaId>(protoSchemaId);
            auto* schema = GetMasterTableSchema(schemaId);

            const auto& objectManager = Bootstrap_->GetObjectManager();
            // All imported schemas should have an artificial ref.
            // This is done to make sure that native cell is managing their lifetime.
            objectManager->RefObject(schema);
        }
    }

    void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::Clear();

        MasterTableSchemaMap_.Clear();
        NativeTableSchemaToObjectMap_.clear();
        EmptyMasterTableSchema_ = nullptr;
        TableCollocationMap_.Clear();
        SecondaryIndexMap_.Clear();
        StatisticsUpdateRequests_.Clear();
        NodeIdToOngoingStatisticsUpdate_.clear();
        Queues_.clear();
        Consumers_.clear();

        NeedToAddReplicatedQueues_ = false;

        NeedTakeArtificialRef_ = false;
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

        // COMPAT(sabdenovch)
        if (context.GetVersion() >= EMasterReign::SecondaryIndex) {
            SecondaryIndexMap_.LoadKeys(context);
        }
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        MasterTableSchemaMap_.LoadValues(context);

        Load(context, StatisticsUpdateRequests_);
        // COMPAT(danilalexeev)
        if (context.GetVersion() >= EMasterReign::FixAsyncTableStatisticsUpdate) {
            Load(context, NodeIdToOngoingStatisticsUpdate_);
        }
        TableCollocationMap_.LoadValues(context);

        // COMPAT(sabdenovch)
        if (context.GetVersion() >= EMasterReign::SecondaryIndex) {
            SecondaryIndexMap_.LoadValues(context);
        }

        Load(context, Queues_);
        Load(context, Consumers_);

        NeedToAddReplicatedQueues_ = context.GetVersion() < EMasterReign::QueueReplicatedTablesList;

        auto schemaExportMode = ESchemaMigrationMode::None;
        if (context.GetVersion() < EMasterReign::ExportMasterTableSchemas) {
            schemaExportMode = ESchemaMigrationMode::AllSchemas;
        } else if (context.GetVersion() < EMasterReign::ExportEmptyMasterTableSchemas) {
            schemaExportMode = ESchemaMigrationMode::EmptySchemaOnly;
        }

        if (EMasterReign::ExportMasterTableSchemas < context.GetVersion() &&
            context.GetVersion() < EMasterReign::RefactorSchemaExport)
        {
            NeedTakeArtificialRef_ = true;
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        // COMPAT(h0pless): Remove this after schema cmigration is complete.
        if (schemaExportMode == ESchemaMigrationMode::None) {
            return;
        }

        if (schemaExportMode == ESchemaMigrationMode::AllSchemas || multicellManager->IsPrimaryMaster()) {
            // Just load schemas as usual.
            EmptyMasterTableSchema_ = GetMasterTableSchema(EmptyMasterTableSchemaId_);
            YT_VERIFY(EmptyMasterTableSchema_->IsNative());
        } else if (schemaExportMode == ESchemaMigrationMode::EmptySchemaOnly) {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            auto* oldEmptyMasterTableSchema = GetMasterTableSchema(PrimaryCellEmptyMasterTableSchemaId_);

            // Un-import old empty schema.
            NativeTableSchemaToObjectMap_.erase(oldEmptyMasterTableSchema->GetNativeTableSchemaToObjectMapIterator());
            oldEmptyMasterTableSchema->ResetNativeTableSchemaToObjectMapIterator();
            // The above iterator reset should've retained the actual
            // TTableSchema inside the old object.

            // Historically, this schema hasn't been flagged as foreign, even
            // though it actually is.
            oldEmptyMasterTableSchema->SetForeign();

            // Allow it to die peacefully if need be.
            objectManager->UnrefObject(oldEmptyMasterTableSchema);
            oldEmptyMasterTableSchema->ResetExportRefCounters();

            // Replace new empty schema with an even newer one.
            EmptyMasterTableSchema_ = DoCreateMasterTableSchema(
                EmptyTableSchema,
                EmptyMasterTableSchemaId_,
                /*isNative*/ true);
            YT_VERIFY(EmptyMasterTableSchema_->RefObject() == 1);
        }
    }

    void OnBeforeSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnBeforeSnapshotLoaded();
    }

    THashMap<TMasterTableSchema*, THashMap<TAccount*, int>> ComputeMasterTableSchemaReferencingAccounts()
    {
        THashMap<TMasterTableSchema*, THashMap<TAccount*, int>> schemaToReferencingAccounts;
        for (auto [schemaId, schema] : MasterTableSchemaMap_) {
            EmplaceOrCrash(schemaToReferencingAccounts, schema, THashMap<TAccount*, int>());
        }

        auto referenceAccount = [&] (TMasterTableSchema* schema, TAccount* account) {
            auto [it, inserted] = schemaToReferencingAccounts[schema].emplace(account, 1);
            if (!inserted) {
                ++it->second;
            }
        };

        // Nodes reference schemas.
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        for (auto [nodeId, node] : cypressManager->Nodes()) {
            // NB: Zombie nodes still hold ref.
            if (!IsSchemafulType(node->GetType())) {
                continue;
            }

            auto* account = node->Account().Get();

            const auto& handler = cypressManager->FindHandler(node->GetType());
            if (auto* schema = handler->FindSchema(node)) {
                referenceAccount(schema, account);
            }
        }

        // Chunks reference schemas. Accounts are charged per-requisition.
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& requisitionRegistry = chunkManager->GetChunkRequisitionRegistry();
        for (auto [chunkId, chunk] : chunkManager->Chunks()) {
            const auto& chunkSchema = chunk->Schema();
            if (!chunkSchema) {
                continue;
            }

            for (const auto& requisition : chunk->GetAggregatedRequisition(requisitionRegistry)) {
                referenceAccount(chunkSchema.Get(), requisition.Account);
            }
        }

        return schemaToReferencingAccounts;
    }

    THashMap<TMasterTableSchema*, THashMap<TCellTag, int>> ComputeMasterTableSchemaExportRefCounters()
    {
        THashMap<TMasterTableSchema*, THashMap<TCellTag, int>> schemaToExportRefCounters;
        for (auto [schemaId, schema] : MasterTableSchemaMap_) {
            EmplaceOrCrash(schemaToExportRefCounters, schema, THashMap<TCellTag, int>());
        }

        auto increaseSchemaExportRefCounter = [&] (TMasterTableSchema* schema, TCellTag cellTag, int counter = 1) {
            auto [it, inserted] = schemaToExportRefCounters[schema].emplace(cellTag, counter);
            if (!inserted) {
                it->second += counter;
            }
        };

        // Exported nodes increase export ref counter.
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        for (auto [nodeId, node] : cypressManager->Nodes()) {
            // NB: Zombie nodes still hold export ref.
            auto nodeType = node->GetType();
            if (!IsSchemafulType(nodeType)) {
                continue;
            }
            if (!node->IsExternal()) {
                continue;
            }

            const auto& handler = cypressManager->FindHandler(nodeType);
            if (auto* schema = handler->FindSchema(node)) {
                increaseSchemaExportRefCounter(schema, node->GetExternalCellTag());
            }
        }

        // Exported chunks increase export ref counter.
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        for (auto [chunkId, chunk] : chunkManager->Chunks()) {
            if (!chunk->IsExported()) {
                continue;
            }

            const auto& chunkSchema = chunk->Schema();
            if (!chunkSchema) {
                continue;
            }

            for (auto cellTag : multicellManager->GetRegisteredMasterCellTags()) {
                if (chunk->IsExportedToCell(cellTag)) {
                    auto exportData = chunk->GetExportData(cellTag);
                    increaseSchemaExportRefCounter(chunkSchema.Get(), cellTag, exportData.RefCounter);
                }
            }
        }

        return schemaToExportRefCounters;
    }

    THashMap<TMasterTableSchema*, i64> ComputeMasterTableSchemaRefCounters()
    {
        THashMap<TMasterTableSchema*, i64> schemaToRefCounter;
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        // Empty schema has 1 additional artificial refcounter.
        for (auto [schemaId, schema] : MasterTableSchemaMap_) {
            auto isEmpty = schemaId == EmptyMasterTableSchemaId_ ? 1 : 0;
            EmplaceOrCrash(schemaToRefCounter, schema, isEmpty);
        }

        // Schemaful nodes hold strong refs.
        for (auto [nodeId, node] : cypressManager->Nodes()) {
            auto nodeType = node->GetType();
            if (!IsSchemafulType(nodeType)) {
                continue;
            }

            // NB: Zombie nodes still hold ref.
            const auto& handler = cypressManager->FindHandler(nodeType);
            if (auto* schema = handler->FindSchema(node)) {
                ++schemaToRefCounter[schema];
            }
        }

        // Chunks hold strong refs.
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        for (auto [chunkId, chunk] : chunkManager->Chunks()) {
            if (const auto& schema = chunk->Schema()) {
                ++schemaToRefCounter[schema.Get()];
            }
        }

        // Schema can be staged in a transaction.
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        for (auto [transactionId, transaction] : transactionManager->Transactions()) {
            for (auto* object : transaction->StagedObjects()) {
                if (object->GetType() == EObjectType::MasterTableSchema) {
                    auto* schema = object->As<TMasterTableSchema>();
                    ++schemaToRefCounter[schema];
                }
            }
        }

        return schemaToRefCounter;
    }

    void RecomputeMasterTableSchemaExportRefCounters(ELogLevel logLevel) override
    {
        auto schemaToExportRefCounters = ComputeMasterTableSchemaExportRefCounters();
        for (auto [schemaId, schema] : MasterTableSchemaMap_) {
            auto expectedExportRefCounters = GetOrCrash(schemaToExportRefCounters, schema);
            for (auto [cellTag, actualRefCounter] : schema->CellTagToExportCount()) {
                auto it = expectedExportRefCounters.find(cellTag);

                auto expectedRefCounter = it != expectedExportRefCounters.end()
                    ? it->second
                    : 0;

                if (expectedRefCounter != actualRefCounter) {
                    YT_LOG_EVENT(Logger, logLevel, "Table schema has invalid export ref counter; setting proper value "
                        "(SchemaId: %v, CellTag: %v, ExpectedRefCounter: %v, ActualRefCounter: %v)",
                        schemaId,
                        cellTag,
                        expectedRefCounter,
                        actualRefCounter);

                    if (actualRefCounter > expectedRefCounter) {
                        for (int index = 0; index < actualRefCounter - expectedRefCounter; ++index) {
                            UnexportMasterTableSchema(schema, cellTag);
                        }
                    } else {
                        for (int index = 0; index < expectedRefCounter - actualRefCounter; ++index) {
                            ExportMasterTableSchema(schema, cellTag);
                        }
                    }
                }

                expectedExportRefCounters.erase(it);
            }

            for (auto [cellTag, refCounter] : expectedExportRefCounters) {
                YT_LOG_EVENT(Logger, logLevel, "Table schema has missing entries in export ref counter; setting proper value "
                    "(SchemaId: %v, CellTag: %v, ExpectedRefCounter: %v)",
                    schemaId,
                    cellTag,
                    refCounter);

                for (i64 index = 0; index < refCounter; ++index) {
                    ExportMasterTableSchema(schema, cellTag);
                }
            }
        }
    }

    void RecomputeMasterTableSchemaRefCounters(ELogLevel logLevel) override
    {
        auto schemaToRefCounter = ComputeMasterTableSchemaRefCounters();
        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (auto [schemaId, schema] : MasterTableSchemaMap_) {
            auto expectedRefCounter = GetOrCrash(schemaToRefCounter, schema);
            auto actualRefCounter = schema->GetObjectRefCounter();

            if (expectedRefCounter != actualRefCounter) {
                YT_LOG_EVENT(Logger, logLevel, "Table schema has invalid ref counter; setting proper value "
                    "(SchemaId: %v, ExpectedRefCounter: %v, ActualRefCounter: %v)",
                    schemaId,
                    expectedRefCounter,
                    actualRefCounter);

                if (actualRefCounter > expectedRefCounter) {
                    for (int index = 0; index < actualRefCounter - expectedRefCounter; ++index) {
                        objectManager->UnrefObject(schema);
                    }
                } else {
                    for (int index = 0; index < expectedRefCounter - actualRefCounter; ++index) {
                        objectManager->RefObject(schema);
                    }
                }

                YT_VERIFY(schema->GetObjectRefCounter() == expectedRefCounter);
            }
        }
    }

    void OnTableCopied(TTableNode* sourceNode, TTableNode* clonedNode) override
    {
        if (!sourceNode->IsForeign()) {
            return;
        }

        auto it = NodeIdToOngoingStatisticsUpdate_.find(sourceNode->GetId());
        if (it != NodeIdToOngoingStatisticsUpdate_.end()) {
            const auto& request = it->second.EffectiveRequest;
            ScheduleStatisticsUpdate(clonedNode,
                request.UpdateDataStatistics,
                request.UpdateTabletResourceUsage,
                request.UseNativeContentRevisionCas);
        }
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

        if (NeedToAddReplicatedQueues_) {
            for (auto [nodeId, node] : Bootstrap_->GetCypressManager()->Nodes()) {
                if (node->GetType() == NObjectClient::EObjectType::ReplicatedTable) {
                    auto* replicatedTableNode = node->As<TReplicatedTableNode>();
                    if (replicatedTableNode->IsTrackedQueueObject()) {
                        RegisterQueue(replicatedTableNode);
                    }
                }
            }
        }

        if (NeedTakeArtificialRef_) {
            THashMap<TCellTag, std::vector<TMasterTableSchemaId>> cellTagToExportedSchemaIds;
            for (auto [schemaId, schema] : MasterTableSchemaMap_) {
                if (!IsObjectAlive(schema)) {
                    continue;
                }
                if (!schema->IsNative()) {
                    continue;
                }

                const auto& cellTagToExportCount = schema->CellTagToExportCount();
                for (const auto& [cellTag, refCounter] : cellTagToExportCount) {
                    cellTagToExportedSchemaIds[cellTag].push_back(schemaId);
                }
            }

            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            auto sendRequestAndLog = [&] (TCellTag cellTag, NProto::TReqTakeArtificialRef& req) {
                YT_LOG_DEBUG("Sending TakeArtificialRef request (DestinationCellTag: %v, RequestSize: %v)",
                    cellTag,
                    req.schema_ids_size());
                multicellManager->PostToMaster(req, cellTag);
                req.clear_schema_ids();
            };

            const auto maxBatchSize = 10000;
            for (const auto& [cellTag, schemaIds] : cellTagToExportedSchemaIds) {
                auto batchSize = 0;
                NProto::TReqTakeArtificialRef req;
                for (auto schemaId : schemaIds) {
                    auto subreq = req.add_schema_ids();
                    ToProto(subreq, schemaId);
                    ++batchSize;

                    if (batchSize >= maxBatchSize) {
                        sendRequestAndLog(cellTag, req);
                        batchSize = 0;
                    }
                }

                if (batchSize != 0) {
                    sendRequestAndLog(cellTag, req);
                }
            }
        }
    }

    void CheckInvariants() override
    {
        auto schemaToExportRefCounters = ComputeMasterTableSchemaExportRefCounters();
        auto schemaToRefCounter = ComputeMasterTableSchemaRefCounters();
        auto schemaToReferencingAccounts = ComputeMasterTableSchemaReferencingAccounts();
        for (auto [schemaId, schema] : MasterTableSchemaMap_) {
            // Check export ref counters.
            auto expectedExportRefCounters = GetOrCrash(schemaToExportRefCounters, schema);
            const auto& actualExportRefCounters = schema->CellTagToExportCount();
            for (auto [cellTag, actualRefCounter] : actualExportRefCounters) {
                auto it = expectedExportRefCounters.find(cellTag);

                auto expectedRefCounter = it != expectedExportRefCounters.end()
                    ? it->second
                    : 0;

                YT_LOG_FATAL_UNLESS(expectedRefCounter == actualRefCounter,
                    "Table schema has unexpected export ref counter "
                    "(SchemaId: %v, CellTag: %v, ExpectedRefCounter: %v, ActualRefCounter: %v)",
                    schemaId,
                    cellTag,
                    expectedRefCounter,
                    actualRefCounter);

                expectedExportRefCounters.erase(it);
            }

            auto actualExportRefCountersSize = actualExportRefCounters.size();
            YT_LOG_FATAL_UNLESS(expectedExportRefCounters.empty(),
                "Table schema has missing entries in export ref counter map "
                "(SchemaId: %v, ExpectedExportRefCounterMapSize: %v, ActualExportRefCounterMapSize: %v)",
                schemaId,
                expectedExportRefCounters.size() + actualExportRefCountersSize,
                actualExportRefCountersSize);

            // Check nativeness.
            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            auto expectedNativeness = CellTagFromId(schemaId) == multicellManager->GetCellTag();
            YT_LOG_FATAL_UNLESS(schema->IsNative() == expectedNativeness,
                "Table schema has unexpected nativeness "
                "(SchemaId: %v, ExpectedNativeness: %v, ActualNativeness: %v)",
                schemaId,
                expectedNativeness,
                schema->IsNative());

            // Check ref counters.
            auto expectedRefCounter = GetOrCrash(schemaToRefCounter, schema);
            auto actualRefCounter = schema->GetObjectRefCounter();
            YT_LOG_FATAL_UNLESS(
                expectedRefCounter == actualRefCounter ||
                (expectedRefCounter + 1 == actualRefCounter && schema->IsForeign()),
                "Table schema has unexpected ref counter "
                "(SchemaId: %v, ExpectedRefCounter: %v, ActualRefCounter: %v)",
                schemaId,
                expectedRefCounter,
                actualRefCounter);

            // Check referencing accounts.
            auto expectedReferencingAccounts = GetOrCrash(schemaToReferencingAccounts, schema);
            const auto& actualReferencingAccounts = schema->ReferencingAccounts();
            for (const auto& [account, actualRefCounter] : actualReferencingAccounts) {
                auto it = expectedReferencingAccounts.find(account.Get());

                auto expectedRefCounter = it != expectedReferencingAccounts.end()
                    ? it->second
                    : 0;

                YT_LOG_FATAL_UNLESS(expectedRefCounter == actualRefCounter,
                    "Table schema has unexpected referencing accounts counter "
                    "(SchemaId: %v, AccountId: %v, Account: %v, ExpectedRefCounter: %v, ActualRefCounter: %v)",
                    schemaId,
                    account->GetId(),
                    account->GetName(),
                    expectedRefCounter,
                    actualRefCounter);

                expectedReferencingAccounts.erase(it);
            }

            auto actualReferencingAccountsSize = actualReferencingAccounts.size();
            YT_LOG_FATAL_UNLESS(expectedExportRefCounters.empty(),
                "Table schema has missing entries in referencing accounts map "
                "(SchemaId: %v, ExpectedReferencingAccountsMapSize: %v, ActualReferencingAccountsMapSize: %v)",
                schemaId,
                expectedReferencingAccounts.size() + actualReferencingAccountsSize,
                actualReferencingAccountsSize);
        }
    }

    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        MasterTableSchemaMap_.SaveKeys(context);
        TableCollocationMap_.SaveKeys(context);
        SecondaryIndexMap_.SaveKeys(context);
    }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        MasterTableSchemaMap_.SaveValues(context);
        Save(context, StatisticsUpdateRequests_);
        Save(context, NodeIdToOngoingStatisticsUpdate_);
        TableCollocationMap_.SaveValues(context);
        SecondaryIndexMap_.SaveValues(context);
        Save(context, Queues_);
        Save(context, Consumers_);
    }

    void OnReplicationCollocationCreated(TTableCollocation* collocation)
    {
        std::vector<TTableId> tableIds;
        tableIds.reserve(collocation->Tables().size());
        for (auto* table : collocation->Tables()) {
            if (IsObjectAlive(table)) {
                tableIds.push_back(table->GetId());
            }
        }
        if (!tableIds.empty()) {
            ReplicationCollocationCreated_.Fire(TTableCollocationData{
                .Id = collocation->GetId(),
                .TableIds = std::move(tableIds),
            });
        }
    }

    // COMPAT(h0pless): RefactorSchemaExport
    TMasterTableSchema* CreateImportedMasterTableSchema(
        const NTableClient::TTableSchema& tableSchema,
        TSchemafulNode* schemaHolder,
        TMasterTableSchemaId hintId) override
    {
        auto* schema = FindMasterTableSchema(hintId);
        if (!schema) {
            schema = DoCreateMasterTableSchema(tableSchema, hintId, /*isNative*/ false);
        } else if (!IsObjectAlive(schema)) {
            ResurrectMasterTableSchema(schema);
            YT_VERIFY(*schema->TableSchema_ == tableSchema);
        }
        SetTableSchema(schemaHolder, schema);
        YT_VERIFY(IsObjectAlive(schema));

        return schema;
    }
};

DEFINE_ENTITY_MAP_ACCESSORS(TTableManager, MasterTableSchema, TMasterTableSchema, MasterTableSchemaMap_);
DEFINE_ENTITY_MAP_ACCESSORS(TTableManager, TableCollocation, TTableCollocation, TableCollocationMap_);
DEFINE_ENTITY_MAP_ACCESSORS(TTableManager, SecondaryIndex, TSecondaryIndex, SecondaryIndexMap_);

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
