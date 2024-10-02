#include "object_manager.h"

#include "config.h"
#include "connection_validators.h"
#include "db_config.h"
#include "db_schema.h"
#include "object_table_reader.h"
#include "persistence.h"
#include "private.h"
#include "session.h"
#include "transaction.h"
#include "transaction_manager.h"
#include "type_handler.h"
#include "watch_log.h"
#include "watch_manager.h"
#include "key_util.h"

#include <yt/yt/orm/server/master/bootstrap.h>
#include <yt/yt/orm/server/master/config.h>
#include <yt/yt/orm/server/master/helpers.h>
#include <yt/yt/orm/server/master/yt_connector.h>
#include <yt/yt/orm/server/access_control/access_control_manager.h>

#include <yt/yt/orm/client/objects/registry.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/atomic_object.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/library/query/base/ast.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/string/format.h>

namespace NYT::NOrm::NServer::NObjects {

using namespace NServer::NMaster;

using namespace NYT::NTransactionClient;
using namespace NYT::NYPath;
using namespace NYT::NYson;
using namespace NYT::NYTree;
using namespace NYT::NCypressClient;
using namespace NYT::NTableClient;
using namespace NYT::NApi;
using namespace NYT::NConcurrency;
using namespace NYT::NQueryClient::NAst;
using namespace NYT::NOrm::NClient;
using namespace NYT::NOrm::NClient::NObjects;

////////////////////////////////////////////////////////////////////////////////

namespace {

static const TString ObjectSweeperUserTag = "object-sweeper";

struct TSweepCounter
{
    explicit TSweepCounter(const NProfiling::TProfiler& profiler)
        : RowCount(profiler.Counter("/sweep/row_count"))
        , ErrorCount(profiler.WithSparse().Counter("/sweep/error_count"))
    { }

    NProfiling::TCounter RowCount;
    NProfiling::TCounter ErrorCount;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TObjectManager::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TObjectManager* owner,
        IBootstrap* bootstrap,
        TObjectManagerConfigPtr initialConfig,
        std::vector<const TDBTable*> dataModelTables)
        : Owner_(owner)
        , Bootstrap_(bootstrap)
        , InitialConfig_(std::move(initialConfig))
        , DataModelTables_(std::move(dataModelTables))
        , SweepExecutor_(New<TPeriodicExecutor>(
            SweepQueue_->GetInvoker(),
            BIND(&TImpl::OnSweep, MakeWeak(this)),
            InitialConfig_->RemovedObjectsSweepPeriod))
        , ColumnEvaluatorCache_(NQueryClient::CreateColumnEvaluatorCache(InitialConfig_->ColumnEvaluatorCache))
        , AsynchronousRemovalsEnabled_(bootstrap->GetDBConfig().EnableAsynchronousRemovals)
    {
        SetConfig(InitialConfig_);
        Bootstrap_->SubscribeConfigUpdate(BIND(&TImpl::OnConfigUpdate, MakeWeak(this)));
    }

    void Initialize()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        ytConnector->SubscribeValidateConnection(BIND(&TImpl::OnValidateConnection, MakeWeak(this)));
        ytConnector->SubscribeStartedLeading(BIND(&TImpl::OnStartedLeading, MakeWeak(this)));
        ytConnector->SubscribeStoppedLeading(BIND(&TImpl::OnStoppedLeading, MakeWeak(this)));

        Owner_->RegisterTypeHandlers();

        for (const auto& [value, typeHandler] : TypeHandlers_) {
            typeHandler->Initialize();
            typeHandler->PostInitialize();
            typeHandler->Validate();
        }
    }

    IObjectTypeHandler* GetTypeHandlerOrCrash(TObjectTypeValue type) const
    {
        auto* typeHandler = FindTypeHandler(type);
        YT_VERIFY(typeHandler);
        return typeHandler;
    }

    IObjectTypeHandler* GetTypeHandlerOrThrow(TObjectTypeValue type) const
    {
        auto* typeHandler = FindTypeHandler(type);
        if (!typeHandler) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::InvalidObjectType,
                "Unknown object type %v",
                GetGlobalObjectTypeRegistry()->FormatTypeValue(type));
        }
        return typeHandler;
    }

    IObjectTypeHandler* FindTypeHandler(TObjectTypeValue type) const
    {
        auto it = TypeHandlers_.find(type);
        return it == TypeHandlers_.end()
            ? nullptr
            : it->second.get();
    }

    EAttributesExtensibilityMode GetAttributesExtensibilityMode() const
    {
        return InitialConfig_->AttributesExtensibilityMode;
    }

    std::vector<NYPath::TYPath> GetAllowedExtensibleAttributePaths(TObjectTypeValue objectType)
    {
        auto config = GetConfig();
        return config->GetAllowedExtensibleAttributePaths(objectType);
    }

    bool IsHistoryEnabled() const
    {
        return InitialConfig_->EnableHistory;
    }

    bool IsHistoryDisabledForType(TObjectTypeValue type) const
    {
        return InitialConfig_->HistoryDisabledTypes.contains(
            GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(type));
    }

    bool IsHistoryIndexAttributeStoreEnabled(TObjectTypeValue type, const NYPath::TYPath& attributePath) const
    {
        return GetConfig()->IsHistoryIndexAttributeStoreEnabled(
            GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(type),
            attributePath);
    }

    bool IsHistoryIndexAttributeQueryEnabled(TObjectTypeValue type, const NYPath::TYPath& attributePath) const
    {
        return GetConfig()->IsHistoryIndexAttributeQueryEnabled(
            GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(type),
            attributePath);
    }

    NYTree::IYPathServicePtr CreateOrchidService()
    {
        auto orchidProducer = BIND(&TImpl::BuildOrchid, MakeStrong(this));
        return NYTree::IYPathService::FromProducer(std::move(orchidProducer));
    }

    NQueryClient::IColumnEvaluatorCachePtr GetColumnEvaluatorCache() const
    {
        return ColumnEvaluatorCache_;
    }

    void RegisterTypeHandler(std::unique_ptr<IObjectTypeHandler> handler)
    {
        auto type = handler->GetType();
        YT_VERIFY(!TypeHandlers_[type]);

        TypeHandlers_[type] = std::move(handler);
    }

    void ValidateDBTables(const std::vector<const TDBTable*>& tables)
    {
        THashMap<const TDBTable*, std::vector<const TScalarAttributeIndexDescriptor*>> tableToIndexes;
        for (const auto& [objectTypeValue, typeHandler] : TypeHandlers_) {
            tableToIndexes.emplace(typeHandler->GetTable(), typeHandler->GetIndexes());
        }

        auto tableValidator = CreateTableValidator(
            Bootstrap_->GetYTConnector(),
            GetConfig()->EnableTableSchemaValidation,
            Bootstrap_->GetDBConfig().VersionCompatibility);

        for (const auto* table : tables) {
            TItemToValidateTable itemToValidate{.Table = table};
            if (auto it = tableToIndexes.find(table); it != tableToIndexes.end()) {
                const auto& indexes = it->second;
                itemToValidate.CommonLockGroupFieldsList.reserve(indexes.size());
                for (const auto& index : indexes) {
                    THashSet<const TDBField*> fields;
                    for (const auto& descriptor : index->IndexedAttributeDescriptors) {
                        fields.insert(descriptor.AttributeDescriptor->Field);
                    }
                    for (const auto& descriptor : index->PredicateAttributeDescriptors) {
                        fields.insert(descriptor.AttributeDescriptor->Field);
                    }
                    itemToValidate.CommonLockGroupFieldsList.emplace_back(
                        std::make_move_iterator(fields.begin()),
                        std::make_move_iterator(fields.end()));
                }
            }
            tableValidator->Schedule(std::move(itemToValidate));
        }
        tableValidator->Validate();
    }

    TObjectManagerConfigPtr GetConfig() const
    {
        return Config_.Acquire();
    }

    DEFINE_SIGNAL(void(const TObjectManagerConfigPtr&), ConfigUpdate);

    IBootstrap* GetBootstrap() const
    {
        return Bootstrap_;
    }

private:
    TObjectManager* const Owner_;
    IBootstrap* const Bootstrap_;

    //! Contains config passed to the master at program start via command line.
    //! May be different from the actual config after dynamic config updates.
    const TObjectManagerConfigPtr InitialConfig_;

    const std::vector<const TDBTable*> DataModelTables_;

    const TActionQueuePtr SweepQueue_ = New<TActionQueue>("Sweep");
    const TPeriodicExecutorPtr SweepExecutor_;

    THashMap<TObjectTypeValue, std::unique_ptr<IObjectTypeHandler>> TypeHandlers_;

    //! Actual config after dynamic config updates.
    TAtomicIntrusivePtr<TObjectManagerConfig> Config_;

    THashMap<TObjectTypeValue, TSweepCounter> SweepCounters_;

    NQueryClient::IColumnEvaluatorCachePtr ColumnEvaluatorCache_;

    bool AsynchronousRemovalsEnabled_;

    void SetConfig(TObjectManagerConfigPtr config)
    {
        SweepExecutor_->SetPeriod(config->RemovedObjectsSweepPeriod);
        Config_.Store(std::move(config));
    }

    void OnConfigUpdate(const TMasterDynamicConfigPtr& ormMasterConfig)
    {
        auto config = ormMasterConfig->GetObjectManagerConfig();
        if (NOrm::NServer::NMaster::AreConfigsEqual(GetConfig(), config)) {
            return;
        }

        YT_LOG_INFO("Updating object manager configuration");
        SetConfig(std::move(config));
        ConfigUpdate_.Fire(GetConfig());
    }

    void OnValidateConnection()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        auto transaction = ytConnector->GetInstanceTransaction();

        {
            const auto& path = ytConnector->GetDBPath();
            YT_LOG_INFO("Locking DB (Path: %v)",
                path);

            WaitFor(transaction->LockNode(path, ELockMode::Shared))
                .ThrowOnError();
        }

        {
            auto tables = Tables;
            if (auto parentsTable = Bootstrap_->GetParentsTable()) {
                tables.push_back(parentsTable);
            } else if (GetConfig()->MayReadLegacyParentsTable()) {
                THROW_ERROR_EXCEPTION("Some objects may use the disabled 'parents' table");
            }
            tables.insert(
                tables.end(),
                DataModelTables_.begin(),
                DataModelTables_.end());
            ValidateDBTables(tables);
        }

        {
            auto typeHandlerValidator = CreateTypeHandlerValidator();
            for (const auto& [type, handler] : TypeHandlers_) {
                YT_VERIFY(handler);
                typeHandlerValidator->Schedule(handler.get());
            }
            typeHandlerValidator->Validate();
        }
    }

    void OnStartedLeading()
    {
        SweepExecutor_->Start();
    }

    void OnStoppedLeading()
    {
        YT_UNUSED_FUTURE(SweepExecutor_->Stop());
    }

    void OnSweep()
    {
        auto config = GetConfig();
        if (!AsynchronousRemovalsEnabled_ || config->RemoveMode == ERemoveObjectMode::SynchronousExclusive) {
            return;
        }
        for (const auto& [typeValue, typeHandler] : TypeHandlers_) {
            auto counterIt = SweepCounters_.find(typeValue);
            if (counterIt == SweepCounters_.end()) {
                counterIt = SweepCounters_.emplace(
                    typeValue,
                    Profiler
                        .WithGlobal()
                        .WithTag("object_type",
                            GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(typeValue)))
                    .first;
            }
            const auto& counter = counterIt->second;
            try {
                YT_VERIFY(typeHandler);
                RemoveRowsWithPendingRemovalsTable(typeHandler->GetTable(), typeHandler->GetType(), config, counter);
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Failed to sweep removed objects of type %v",
                    GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(typeValue));
                counter.ErrorCount.Increment();
            }
        }
    }

    void RemoveRows(
        const TDBTable* table,
        const TDBField* removalTimeField,
        const TObjectManagerConfigPtr& config,
        const TSweepCounter& counter)
    {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();

        auto deadline = TInstant::Now() - config->RemovedObjectsGraceTimeout;

        NAccessControl::TAuthenticatedUserGuard guard(
            Bootstrap_->GetAccessControlManager(),
            NRpc::TAuthenticationIdentity(NRpc::RootUserName, ObjectSweeperUserTag));

        const auto& tableKeyFields = table->GetKeyFields(/*filterEvaluatedFields*/ true);
        std::vector<TString> fieldNames;
        fieldNames.reserve(tableKeyFields.size());
        for (const auto* field : tableKeyFields) {
            fieldNames.push_back(field->Name);
        }

        YT_LOG_INFO("Selecting rows to remove from table (TableName: %v, Deadline: %v)",
            table->GetName(),
            deadline);

        // Do not filter objects by missing removal time because this module must process removed objects by itself.
        auto objectTableReader = CreateObjectTableReader(
            Bootstrap_,
            config->RemovedObjectTableReader,
            table,
            std::move(fieldNames),
            /*removalTimeFieldName*/ "");

        auto filter = Format("not is_null(%v) and %v < %vu",
            FormatId(removalTimeField->Name),
            FormatId(removalTimeField->Name),
            deadline.MicroSeconds());
        objectTableReader->SetCustomObjectFilter(std::move(filter));

        while (!objectTableReader->IsFinished()) {
            auto transaction = WaitFor(transactionManager->StartReadWriteTransaction())
                .ValueOrThrow();

            YT_LOG_INFO("Removal transaction started (TransactionId: %v, TableName: %v)",
                transaction->GetId(),
                table->GetName());

            auto* session = transaction->GetSession();
            auto rowset = objectTableReader->Read(session);
            const size_t rowsCount = rowset->GetRows().size();

            session->ScheduleStore(
                [&] (IStoreContext* context) {
                    for (auto row : rowset->GetRows()) {
                        TObjectKey key;
                        FromUnversionedRow(row, &key, tableKeyFields.size());
                        context->DeleteRow(table, key);
                    }
                });

            YT_LOG_INFO("Committing removal transaction (TransactionId: %v, TableName: %v)",
                transaction->GetId(),
                table->GetName());

            WaitFor(transaction->Commit())
                .ThrowOnError();

            YT_LOG_INFO("Removal transaction committed (TransactionId: %v, TableName: %v)",
                transaction->GetId(),
                table->GetName());

            counter.RowCount.Increment(rowsCount);
        }
    }

    void RemoveRowsWithPendingRemovalsTable(
        const TDBTable* table,
        TObjectTypeValue objectType,
        const TObjectManagerConfigPtr& config,
        const TSweepCounter& counter)
    {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();

        NAccessControl::TAuthenticatedUserGuard guard(
            Bootstrap_->GetAccessControlManager(),
            NRpc::TAuthenticationIdentity(NRpc::RootUserName, ObjectSweeperUserTag));

        auto deadline = TInstant::Now() - config->RemovedObjectsGraceTimeout;

        YT_LOG_INFO("Selecting rows to remove from pending removes table (ObjectType: %v, Deadline: %v)",
            GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(objectType),
            deadline);

        const auto& pendingRemovesKeyFields = PendingRemovalsTable.GetKeyFields(/*filterEvaluatedFields*/ true);
        std::vector<TString> pendingRemovesKeyFieldNames;
        pendingRemovesKeyFieldNames.reserve(pendingRemovesKeyFields.size());
        for (const auto* field : pendingRemovesKeyFields) {
            pendingRemovesKeyFieldNames.push_back(field->Name);
        }

        // Do not filter objects by missing removal time because this module must process removed objects by itself.
        auto objectTableReader = CreateObjectTableReader(
            Bootstrap_,
            config->RemovedObjectTableReader,
            &PendingRemovalsTable,
            std::move(pendingRemovesKeyFieldNames),
            /*removalTimeFieldName*/ "");

        auto filter = Format("%v = %v and %v < %vu",
            FormatId(PendingRemovalsTable.Fields.ObjectType.Name),
            objectType,
            FormatId(PendingRemovalsTable.Fields.RemovalTime.Name),
            deadline.MicroSeconds());
        objectTableReader->SetCustomObjectFilter(std::move(filter));

        while (!objectTableReader->IsFinished()) {
            auto transaction = WaitFor(transactionManager->StartReadWriteTransaction())
                .ValueOrThrow();

            YT_LOG_INFO("Removal transaction started (TransactionId: %v, TableName: %v)",
                transaction->GetId(),
                table->GetName());

            auto* session = transaction->GetSession();
            auto rowset = objectTableReader->Read(session);
            const size_t rowsCount = rowset->GetRows().size();

            if (rowsCount == 0) {
                YT_LOG_INFO("Aborting removal transaction (TransactionId: %v, TableName: %v)",
                    transaction->GetId(),
                    table->GetName());
                YT_UNUSED_FUTURE(transaction->Abort());
            } else {
                YT_LOG_INFO("Removing rows (TableName: %v, Count: %v, TransactionId: %v)",
                    table->GetName(),
                    rowsCount,
                    transaction->GetId());

                const auto& tableKeyFields = table->GetKeyFields(/*filterEvaluatedFields*/ true);

                session->ScheduleStore(
                    [&] (IStoreContext* context) {
                        for (auto row : rowset->GetRows()) {
                            TObjectKey keyToRemove;
                            FromUnversionedRow(row, &keyToRemove, pendingRemovesKeyFields.size());
                            context->DeleteRow(&PendingRemovalsTable, keyToRemove);

                            auto objectKey = ParseObjectKey(std::get<TString>(keyToRemove[1]), tableKeyFields);
                            context->DeleteRow(table, objectKey);
                        }
                    });

                YT_LOG_INFO("Committing removal transaction (TransactionId: %v, TableName: %v)",
                    transaction->GetId(),
                    table->GetName());

                WaitFor(transaction->Commit())
                    .ThrowOnError();

                YT_LOG_INFO("Removal transaction committed (TransactionId: %v, TableName: %v)",
                    transaction->GetId(),
                    table->GetName());

                counter.RowCount.Increment(rowsCount);
            }
        }
    }

    static IListNodePtr GetObjectTypes()
    {
        std::vector<TObjectTypeName> names;
        for (const auto& type : GetGlobalObjectTypeRegistry()->GetTypes()) {
            if (type.Value != TObjectTypeValues::Null) {
                names.push_back(type.Name);
            }
        }
        return ConvertTo<IListNodePtr>(names);
    }

    void BuildOrchid(NYson::IYsonConsumer* consumer)
    {
        std::vector<TFuture<std::vector<TTabletInfo>>> futures;
        futures.reserve(TypeHandlers_.size());
        for (const auto& typeHandler : TypeHandlers_) {
            for (const auto& watchLog : typeHandler.second->GetWatchLogs()) {
                futures.push_back(Bootstrap_->GetWatchManager()
                    ->GetTabletInfos(typeHandler.first, watchLog.Name, /*tablets*/ {}));
            }
        }
        auto allTabletInfos = WaitForUnique(AllSucceeded(std::move(futures)))
            .ValueOrThrow();

        int index = 0;
        auto config = Bootstrap_->GetWatchManager()->GetConfig();
        NYTree::BuildYsonFluently(consumer)
            .BeginMap()
                .Item("types")
                .BeginList()
                    .Items(GetObjectTypes())
                .EndList()
                .Item("type_handlers")
                .DoMapFor(TypeHandlers_,
                    [&] (auto fluentTypeHandlersMap, const auto& typeAndHandler)
                {
                    const auto& [type, typeHandler] = typeAndHandler;
                    const auto& objectTypeName = GetGlobalObjectTypeRegistry()
                        ->GetTypeNameByValueOrCrash(type);
                    fluentTypeHandlersMap
                        .Item(objectTypeName)
                        .BeginMap()
                            .Item("watch_logs")
                            .DoMapFor(typeHandler->GetWatchLogs(),
                                [&] (auto fluentWatchLogsMap, const auto& watchLog)
                            {
                                const auto tabletCount = allTabletInfos[index++].size();
                                auto logState = config->GetLogState(objectTypeName, watchLog.Name);
                                fluentWatchLogsMap
                                    .Item(watchLog.Name)
                                    .BeginMap()
                                        .Item("name").Value(watchLog.Name)
                                        .Item("filter").Value(watchLog.Filter.Query)
                                        .Item("selector").Value(watchLog.Selector)
                                        .Item("tablet_count").Value(tabletCount)
                                        .Item("state").Value(logState)
                                        .Item("required_tags").Value(
                                            GetGlobalTagsRegistry()->MakeHumanReadableTagsList(watchLog.RequiredTags))
                                        .Item("excluded_tags").Value(
                                            GetGlobalTagsRegistry()->MakeHumanReadableTagsList(watchLog.ExcludedTags))
                                    .EndMap();
                            })
                            .Item("watch_log_changed_attributes")
                                .Value(config->GetChangedAttributesPaths(type))
                        .EndMap();
                })
            .EndMap();
    }
};

////////////////////////////////////////////////////////////////////////////////

TObjectManager::TObjectManager(
    IBootstrap* bootstrap,
    TObjectManagerConfigPtr initialConfig,
    std::vector<const TDBTable*> dataModelTables)
    : Impl_(New<TImpl>(
        this,
        bootstrap,
        std::move(initialConfig),
        std::move(dataModelTables)))
{ }

TObjectManager::~TObjectManager()
{ }

////////////////////////////////////////////////////////////////////////////////

void TObjectManager::Initialize()
{
    Impl_->Initialize();
}

////////////////////////////////////////////////////////////////////////////////

TObjectManagerConfigPtr TObjectManager::GetConfig() const
{
    return Impl_->GetConfig();
}

////////////////////////////////////////////////////////////////////////////////

IObjectTypeHandler* TObjectManager::GetTypeHandlerOrCrash(TObjectTypeValue type) const
{
    return Impl_->GetTypeHandlerOrCrash(type);
}

IObjectTypeHandler* TObjectManager::GetTypeHandlerOrThrow(TObjectTypeValue type) const
{
    return Impl_->GetTypeHandlerOrThrow(type);
}

IObjectTypeHandler* TObjectManager::FindTypeHandler(TObjectTypeValue type) const
{
    return Impl_->FindTypeHandler(type);
}

////////////////////////////////////////////////////////////////////////////////

bool TObjectManager::IsHistoryEnabled() const
{
    return Impl_->IsHistoryEnabled();
}

bool TObjectManager::IsHistoryDisabledForType(TObjectTypeValue type) const
{
    return Impl_->IsHistoryDisabledForType(type);
}

bool TObjectManager::IsHistoryIndexAttributeStoreEnabled(
    TObjectTypeValue type, const NYPath::TYPath& attributePath) const
{
    return Impl_->IsHistoryIndexAttributeStoreEnabled(type, attributePath);
}

bool TObjectManager::IsHistoryIndexAttributeQueryEnabled(
    TObjectTypeValue type, const NYPath::TYPath& attributePath) const
{
    return Impl_->IsHistoryIndexAttributeQueryEnabled(type, attributePath);
}

////////////////////////////////////////////////////////////////////////////////

NYTree::IYPathServicePtr TObjectManager::CreateOrchidService()
{
    return Impl_->CreateOrchidService();
}

NQueryClient::IColumnEvaluatorCachePtr TObjectManager::GetColumnEvaluatorCache() const
{
    return Impl_->GetColumnEvaluatorCache();
}

////////////////////////////////////////////////////////////////////////////////

EAttributesExtensibilityMode TObjectManager::GetAttributesExtensibilityMode() const
{
    return Impl_->GetAttributesExtensibilityMode();
}

std::vector<NYPath::TYPath> TObjectManager::GetAllowedExtensibleAttributePaths(TObjectTypeValue objectType)
{
    return Impl_->GetAllowedExtensibleAttributePaths(objectType);
}

////////////////////////////////////////////////////////////////////////////////

DELEGATE_SIGNAL(TObjectManager, void(const TObjectManagerConfigPtr&), ConfigUpdate, *Impl_);

////////////////////////////////////////////////////////////////////////////////

void TObjectManager::RegisterTypeHandler(std::unique_ptr<IObjectTypeHandler> handler)
{
    Impl_->RegisterTypeHandler(std::move(handler));
}

IBootstrap* TObjectManager::GetBootstrap() const
{
    return Impl_->GetBootstrap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
