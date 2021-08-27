#include "table_manager.h"
#include "private.h"
#include "master_table_schema_proxy.h"
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

#include <yt/yt/server/master/tablet_server/config.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

#include <yt/yt/server/lib/table_server/proto/table_manager.pb.h>

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
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NHydra;
using namespace NSecurityServer;
using namespace NTableClient;
using namespace NTabletServer;
using namespace NTransactionServer;
using namespace NYTree;

using NCypressServer::TNodeId;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TableServerLogger;

///////////////////////////////////////////////////////////////////////////////

class TTableManager::TMasterTableSchemaTypeHandler
    : public TObjectTypeHandlerWithMapBase<TMasterTableSchema>
{
public:
    explicit TMasterTableSchemaTypeHandler(TImpl* owner);

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
    TImpl* const Owner_;

    IObjectProxyPtr DoGetProxy(TMasterTableSchema* user, TTransaction* transaction) override;

    void DoZombifyObject(TMasterTableSchema* user) override;
};

////////////////////////////////////////////////////////////////////////////////

class TTableManager::TImpl
    : public TMasterAutomatonPart
{
public:
    explicit TImpl(NCellMaster::TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, NCellMaster::EAutomatonThreadQueue::TableManager)
        , StatisticsGossipThrottler_(CreateReconfigurableThroughputThrottler(
            New<TThroughputThrottlerConfig>(),
            TableServerLogger,
            TableServerProfiler.WithPrefix("/table_statistics_gossip_throttler")))
    {
        RegisterLoader(
            "TableManager.Keys",
            BIND(&TImpl::LoadKeys, Unretained(this)));
        RegisterLoader(
            "TableManager.Values",
            BIND(&TImpl::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "TableManager.Keys",
            BIND(&TImpl::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "TableManager.Values",
            BIND(&TImpl::SaveValues, Unretained(this)));

        // COMPAT(shakurov, gritukan)
        RegisterMethod(BIND(&TImpl::HydraSendTableStatisticsUpdates, Unretained(this)), /*aliases*/ {"NYT.NTabletServer.NProto.TReqSendTableStatisticsUpdates"});
        RegisterMethod(BIND(&TImpl::HydraUpdateTableStatistics, Unretained(this)), /*aliases*/ {"NYT.NTabletServer.NProto.TReqUpdateTableStatistics"});
        RegisterMethod(BIND(&TImpl::HydraNotifyContentRevisionCasFailed, Unretained(this)));

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(New<TMasterTableSchemaTypeHandler>(this));

        auto cellTag = Bootstrap_->GetMulticellManager()->GetCellTag();
        EmptyMasterTableSchemaId_ = MakeWellKnownId(EObjectType::MasterTableSchema, cellTag, 0xffffffffffffffff);
    }

    void Initialize()
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND(&TImpl::OnDynamicConfigChanged, MakeWeak(this)));
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
                BIND(&TImpl::OnStatisticsGossip, MakeWeak(this)),
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

    void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::Clear();

        StatisticsUpdateRequests_.Clear();
    }

    void ScheduleStatisticsUpdate(
        TChunkOwnerBase* chunkOwner,
        bool updateDataStatistics,
        bool updateTabletStatistics,
        bool useNativeContentRevisionCas)
    {
        YT_ASSERT(!updateTabletStatistics || IsTableType(chunkOwner->GetType()));

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsSecondaryMaster()) {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Schedule node statistics update (NodeId: %v, UpdateDataStatistics: %v, UpdateTabletStatistics: %v, UseNativeContentRevisionCas: %v)",
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

    void SendStatisticsUpdate(
        TChunkOwnerBase* chunkOwner,
        bool useNativeContentRevisionCas)
    {
        if (chunkOwner->IsNative()) {
            return;
        }

        YT_VERIFY(IsSupportedNodeType(chunkOwner->GetType()));

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsSecondaryMaster());

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Sending node statistics update (NodeId: %v)",
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

            if (statistics.UpdateTabletResourceUsage && !IsTableType(node->GetType())) {
                YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Requested to send tablet resource usage update for a non-table node; ignored (NodeId: %v)",
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

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Sending node statistics update (RequestedNodeCount: %v, NodeIds: %v)",
            request->node_count(),
            nodeIds);

        for  (const auto& [cellTag, request] : cellTagToRequest) {
            multicellManager->PostToMaster(request, cellTag);
        }
    }

    void HydraUpdateTableStatistics(NTableServer::NProto::TReqUpdateTableStatistics* request)
    {
        SmallVector<TTableId, 8> nodeIds;
        nodeIds.reserve(request->entries_size());
        for (const auto& entry : request->entries()) {
            nodeIds.push_back(FromProto<TNodeId>(entry.node_id()));
        }

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Received node statistics update (NodeIds: %v)",
            nodeIds);

        SmallVector<TTableId, 8> nodeIdsToRetry; // Just for logging.
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
                if (IsTableType(chunkOwner->GetType())) {
                    auto* table = chunkOwner->As<TTableNode>();
                    auto tabletResourceUsage = FromProto<TTabletResources>(entry.tablet_resource_usage());
                    table->SetExternalTabletResourceUsage(tabletResourceUsage);
                } else {
                    YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Received tablet resource usage update for a non-table node (NodeId: %v)",
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

            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Content revision CASes failed, requesting retries (NodeIds: %v)",
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
                YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Received non-monotonic revision with content revision CAS failure notification; ignored (NodeId: %v, ReceivedRevision: %llx, NodeRevision: %llx)",
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

    void LoadStatisticsUpdateRequests(NCellMaster::TLoadContext& context)
    {
        Load(context, StatisticsUpdateRequests_);
    }

    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        // COMPAT(shakurov)
        if (context.GetVersion() >= EMasterReign::TrueTableSchemaObjects) {
            MasterTableSchemaMap_.LoadKeys(context);
        }
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        // COMPAT(shakurov)
        if (context.GetVersion() >= EMasterReign::TrueTableSchemaObjects) {
            MasterTableSchemaMap_.LoadValues(context);
        }

        // COMPAT(shakurov)
        if (context.GetVersion() >= EMasterReign::MoveTableStatisticsGossipToTableManager) {
            LoadStatisticsUpdateRequests(context);
        } // Otherwise loading is initiated from tablet manager.

        // COMPAT(shakurov)
        if (context.GetVersion() < EMasterReign::RefBuiltinEmptySchema) {
            NeedFixEmptyMasterTableSchemaRefCounter_ = true;
        }
    }

    void OnBeforeSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnBeforeSnapshotLoaded();

        NeedFixEmptyMasterTableSchemaRefCounter_ = false;
    }

    void OnAfterSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        // COMPAT(shakurov)
        if (NeedFixEmptyMasterTableSchemaRefCounter_) {
            GetEmptyMasterTableSchema()->RefObject();
        }
    }

    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        MasterTableSchemaMap_.SaveKeys(context);
    }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        MasterTableSchemaMap_.SaveValues(context);
        Save(context, StatisticsUpdateRequests_);
    }

    DECLARE_ENTITY_MAP_ACCESSORS(MasterTableSchema, TMasterTableSchema);

    TMasterTableSchema* GetEmptyMasterTableSchema()
    {
        return GetBuiltin(EmptyMasterTableSchema_);
    }

    void SetTableSchema(TTableNode* table, TMasterTableSchema* schema)
    {
        if (!schema) {
            // During cross-shard copying of an opaque table, it is materialized
            // by EndCopy (non-inplace) without any schema.
            return;
        }

        // NB: a newly created schema object has zero reference count.
        // Thus the new schema may technically be not alive here.
        YT_VERIFY(!schema->IsDestroyed());

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

    void ResetTableSchema(TTableNode* table)
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

    TMasterTableSchema* GetMasterTableSchemaOrThrow(TMasterTableSchemaId id)
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

    TMasterTableSchema* FindMasterTableSchema(const TTableSchema& tableSchema) const
    {
        auto it = TableSchemaToObjectMap_.find(tableSchema);
        return it != TableSchemaToObjectMap_.end()
            ? it->second
            : nullptr;
    }

    TMasterTableSchema* GetOrCreateMasterTableSchema(const TTableSchema& tableSchema, TTableNode* schemaHolder)
    {
        auto* schema = DoGetOrCreateMasterTableSchema(tableSchema);
        SetTableSchema(schemaHolder, schema);
        YT_VERIFY(IsObjectAlive(schema));
        return schema;
    }

    TMasterTableSchema* GetOrCreateMasterTableSchema(const TTableSchema& tableSchema, TTransaction* schemaHolder)
    {
        YT_VERIFY(IsObjectAlive(schemaHolder));

        auto* schema = DoGetOrCreateMasterTableSchema(tableSchema);
        if (!schemaHolder->StagedObjects().contains(schema)) {
            const auto& transactionManager = Bootstrap_->GetTransactionManager();
            transactionManager->StageObject(schemaHolder, schema);
        }
        YT_VERIFY(IsObjectAlive(schema));
        return schema;
    }

    TMasterTableSchema* DoGetOrCreateMasterTableSchema(const TTableSchema& tableSchema)
    {
        // NB: freshly created schemas have zero refcounter and are technically not alive.
        if (auto* schema = FindMasterTableSchema(tableSchema)) {
            return schema;
        }

        return CreateMasterTableSchema(tableSchema);
    }

    TMasterTableSchema* CreateMasterTableSchemaUnsafely(
        TMasterTableSchemaId tableSchemaId,
        const TTableSchema& tableSchema)
    {
        // During compat-migration, Create...Unsafely should only be called once per unique schema.
        YT_VERIFY(!FindMasterTableSchema(tableSchema));
        YT_VERIFY(!FindMasterTableSchema(tableSchemaId));

        return DoCreateMasterTableSchema(tableSchemaId, tableSchema);
    }

    TMasterTableSchema* GetBuiltin(TMasterTableSchema*& builtin)
    {
        if (!builtin) {
            InitBuiltins();
        }
        YT_VERIFY(builtin);
        return builtin;
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

        EmptyMasterTableSchema_ = DoCreateMasterTableSchema(EmptyMasterTableSchemaId_, EmptyTableSchema);
        YT_VERIFY(EmptyMasterTableSchema_->RefObject() == 1);
    }

    TMasterTableSchema* CreateMasterTableSchema(const TTableSchema& tableSchema)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::MasterTableSchema, NullObjectId);
        auto* schema = DoCreateMasterTableSchema(id, tableSchema);

        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Schema created (Id: %v)", id);

        return schema;
    }

    void ZombifyMasterTableSchema(TMasterTableSchema* schema)
    {
        YT_VERIFY(schema != EmptyMasterTableSchema_);

        if (!schema->ReferencingAccounts().empty()) {
            YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Table schema being destroyed is still referenced by some accounts (SchemaId: %v, AccountCount: %v)",
                schema->GetId(),
                schema->ReferencingAccounts().size());
        }

        if (!schema->ChargedMasterMemoryUsage().empty()) {
            YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Table schema being destroyed is still charged to some accounts (SchemaId: %v, AccountCount: %v)",
                schema->GetId(),
                schema->ChargedMasterMemoryUsage().size());
        }

        TableSchemaToObjectMap_.erase(schema->GetTableSchemaToObjectMapIterator());
        schema->ResetTableSchemaToObjectMapIterator();
    }

    TMasterTableSchema* DoCreateMasterTableSchema(TMasterTableSchemaId id, TTableSchema tableSchema)
    {
        auto sharedTableSchema = New<TTableSchema>(std::move(tableSchema));
        auto [it, inserted] = TableSchemaToObjectMap_.emplace(std::move(sharedTableSchema), nullptr);
        YT_VERIFY(inserted);

        auto schemaHolder = TPoolAllocator::New<TMasterTableSchema>(id, it);
        auto* schema = MasterTableSchemaMap_.Insert(id, std::move(schemaHolder));
        it->second = schema;

        YT_VERIFY(schema->GetTableSchemaToObjectMapIterator()->second == schema);

        return schema;
    }

    TMasterTableSchema::TTableSchemaToObjectMapIterator RegisterSchema(
        TMasterTableSchema* schema,
        TTableSchema tableSchema)
    {
        YT_VERIFY(IsObjectAlive(schema));
        auto sharedTableSchema = New<TTableSchema>(std::move(tableSchema));
        auto [it, inserted] = TableSchemaToObjectMap_.emplace(std::move(sharedTableSchema), schema);
        YT_VERIFY(inserted);
        return it;
    }

private:

    const TDynamicTablesMulticellGossipConfigPtr& GetGossipConfig()
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        return configManager->GetConfig()->TabletManager->MulticellGossip;
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/ = nullptr)
    {
        const auto& gossipConfig = GetGossipConfig();
        StatisticsGossipThrottler_->Reconfigure(gossipConfig->TableStatisticsGossipThrottler);

        if (StatisticsGossipExecutor_) {
            StatisticsGossipExecutor_->SetPeriod(gossipConfig->TableStatisticsGossipPeriod);
        }
    }

    static bool IsSupportedNodeType(EObjectType type)
    {
        return IsTableType(type) || type == EObjectType::File;
    }

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
            // COMPAT(shakurov)
            if (context.GetVersion() >= EMasterReign::NativeContentRevision) {
                Persist(context, UseNativeContentRevisionCas);
            }
        }
    };

    friend class TMasterTableSchemaTypeHandler;

    static const TTableSchema EmptyTableSchema;
    static const NYson::TYsonString EmptyYsonTableSchema;

    NHydra::TEntityMap<TMasterTableSchema> MasterTableSchemaMap_;
    TMasterTableSchema::TTableSchemaToObjectMap TableSchemaToObjectMap_;

    TMasterTableSchemaId EmptyMasterTableSchemaId_;
    TMasterTableSchema* EmptyMasterTableSchema_ = nullptr;

    TRandomAccessQueue<TNodeId, TStatisticsUpdateRequest> StatisticsUpdateRequests_;
    TPeriodicExecutorPtr StatisticsGossipExecutor_;
    IReconfigurableThroughputThrottlerPtr StatisticsGossipThrottler_;

    // COMPAT(shakurov)
    bool NeedFixEmptyMasterTableSchemaRefCounter_ = false;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);
};

DEFINE_ENTITY_MAP_ACCESSORS(TTableManager::TImpl, MasterTableSchema, TMasterTableSchema, MasterTableSchemaMap_)

////////////////////////////////////////////////////////////////////////////////

const TTableSchema TTableManager::TImpl::EmptyTableSchema;
const NYson::TYsonString TTableManager::TImpl::EmptyYsonTableSchema = BuildYsonStringFluently().Value(EmptyTableSchema);

////////////////////////////////////////////////////////////////////////////////

TTableManager::TMasterTableSchemaTypeHandler::TMasterTableSchemaTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->MasterTableSchemaMap_)
    , Owner_(owner)
{ }

TObject* TTableManager::TMasterTableSchemaTypeHandler::CreateObject(
    TObjectId /*hintId*/,
    IAttributeDictionary* /*attributes*/)
{
    THROW_ERROR_EXCEPTION("%Qv objects cannot be created manually", EObjectType::MasterTableSchema);
}

IObjectProxyPtr TTableManager::TMasterTableSchemaTypeHandler::DoGetProxy(
    TMasterTableSchema* schema,
    TTransaction* /*transaction*/)
{
    return CreateMasterTableSchemaProxy(Owner_->Bootstrap_, &Metadata_, schema);
}

void TTableManager::TMasterTableSchemaTypeHandler::DoZombifyObject(TMasterTableSchema* schema)
{
    TObjectTypeHandlerWithMapBase::DoZombifyObject(schema);
    Owner_->ZombifyMasterTableSchema(schema);
}

////////////////////////////////////////////////////////////////////////////////

TTableManager::TTableManager(NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(bootstrap))
{ }

TTableManager::~TTableManager() = default;

void TTableManager::Initialize()
{
    Impl_->Initialize();
}

void TTableManager::ScheduleStatisticsUpdate(
    TChunkOwnerBase* chunkOwner,
    bool updateDataStatistics,
    bool updateTabletStatistics,
    bool useNativeContentRevisionCas)
{
    Impl_->ScheduleStatisticsUpdate(
        chunkOwner,
        updateDataStatistics,
        updateTabletStatistics,
        useNativeContentRevisionCas);
}

void TTableManager::SendStatisticsUpdate(
    TChunkOwnerBase* chunkOwner,
    bool useNativeContentRevisionCas)
{
    Impl_->SendStatisticsUpdate(chunkOwner, useNativeContentRevisionCas);
}

void TTableManager::LoadStatisticsUpdateRequests(NCellMaster::TLoadContext& context)
{
    Impl_->LoadStatisticsUpdateRequests(context);
}

TMasterTableSchema* TTableManager::GetMasterTableSchemaOrThrow(TMasterTableSchemaId id)
{
    return Impl_->GetMasterTableSchemaOrThrow(id);
}

TMasterTableSchema* TTableManager::FindMasterTableSchema(const TTableSchema& tableSchema) const
{
    return Impl_->FindMasterTableSchema(tableSchema);
}

TMasterTableSchema* TTableManager::GetOrCreateMasterTableSchema(
    const TTableSchema& schema,
    TTableNode* schemaHolder)
{
    return Impl_->GetOrCreateMasterTableSchema(schema, schemaHolder);
}

TMasterTableSchema* TTableManager::GetOrCreateMasterTableSchema(
    const TTableSchema& schema,
    TTransaction* schemaHolder)
{
    return Impl_->GetOrCreateMasterTableSchema(schema, schemaHolder);
}

TMasterTableSchema* TTableManager::CreateMasterTableSchemaUnsafely(
    TMasterTableSchemaId schemaId,
    const TTableSchema& schema)
{
    return Impl_->CreateMasterTableSchemaUnsafely(schemaId, schema);
}

TMasterTableSchema* TTableManager::GetEmptyMasterTableSchema()
{
    return Impl_->GetEmptyMasterTableSchema();
}

TMasterTableSchema::TTableSchemaToObjectMapIterator TTableManager::RegisterSchema(
    TMasterTableSchema* schema,
    TTableSchema tableSchema)
{
    return Impl_->RegisterSchema(schema, std::move(tableSchema));
}

void TTableManager::SetTableSchema(TTableNode* table, TMasterTableSchema* schema)
{
    return Impl_->SetTableSchema(table, schema);
}

void TTableManager::ResetTableSchema(TTableNode* table)
{
    return Impl_->ResetTableSchema(table);
}

DELEGATE_ENTITY_MAP_ACCESSORS(TTableManager, MasterTableSchema, TMasterTableSchema, *Impl_)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
