#include "transaction.h"

#include "account.h"
#include "attribute_schema.h"
#include "config.h"
#include "db_schema.h"
#include "dns_record_set.h"
#include "geometric_2d_set_cover.h"
#include "group.h"
#include "helpers.h"
#include "internet_address.h"
#include "ip4_address_pool.h"
#include "network_project.h"
#include "node.h"
#include "node_segment.h"
#include "object_manager.h"
#include "pod.h"
#include "pod_disruption_budget.h"
#include "pod_set.h"
#include "private.h"
#include "resource.h"
#include "schema.h"
#include "type_handler.h"
#include "user.h"
#include "virtual_service.h"
#include "watch_manager.h"

#include <yp/server/master/bootstrap.h>
#include <yp/server/master/yt_connector.h>

#include <yp/server/nodes/node_tracker.h>

#include <yp/server/net/internet_address_manager.h>
#include <yp/server/net/net_manager.h>

#include <yp/server/scheduler/resource_manager.h>

#include <yp/server/access_control/access_control_manager.h>

#include <yp/server/accounting/accounting_manager.h>

#include <yp/server/lib/objects/object_filter.h>
#include <yp/server/lib/objects/type_info.h>

#include <yt/client/api/transaction.h>
#include <yt/client/api/client.h>
#include <yt/client/api/rowset.h>

#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/row_buffer.h>

#include <yt/ytlib/query_client/ast.h>
#include <yt/ytlib/query_client/query_preparer.h>

#include <yt/core/ytree/node.h>
#include <yt/core/ytree/ypath_resolver.h>

#include <yt/core/ypath/tokenizer.h>

#include <yt/core/misc/collection_helpers.h>

#include <array>

namespace NYP::NServer::NObjects {

using namespace NAccessControl;

using namespace NYT::NApi;
using namespace NYT::NYPath;
using namespace NYT::NYTree;
using namespace NYT::NTableClient;
using namespace NYT::NConcurrency;
using namespace NYT::NQueryClient::NAst;
using namespace NYT::NNet;
using namespace NYT::NYson;

using NYT::NQueryClient::TSourceLocation;
using NYT::NQueryClient::EBinaryOp;

////////////////////////////////////////////////////////////////////////////////

static const TString PrimaryTableAlias("p");
static const TString AnnotationsTableAliasPrefix("c");

////////////////////////////////////////////////////////////////////////////////

void FromProto(
    TRemoveUpdateRequest* request,
    const NClient::NApi::NProto::TRemoveUpdate& protoRequest)
{
    request->Path = protoRequest.path();
}

void FromProto(
    TAttributeTimestampPrerequisite* prerequisite,
    const NClient::NApi::NProto::TAttributeTimestampPrerequisite& protoPrerequisite)
{
    prerequisite->Path = protoPrerequisite.path();
    prerequisite->Timestamp = protoPrerequisite.timestamp();
}

void FromProto(
    TGetQueryOptions* options,
    const NClient::NApi::NProto::TGetObjectOptions& protoOptions)
{
    options->IgnoreNonexistent = protoOptions.ignore_nonexistent();
    options->FetchValues = protoOptions.fetch_values();
    options->FetchTimestamps = protoOptions.fetch_timestamps();
}

void FromProto(
    TSelectQueryOptions* options,
    const NClient::NApi::NProto::TSelectObjectsOptions& protoOptions)
{
    if (protoOptions.has_offset()) {
        options->Offset = protoOptions.offset();
    }
    if (protoOptions.has_limit()) {
        options->Limit = protoOptions.limit();
    }
    options->FetchValues = protoOptions.fetch_values();
    options->FetchTimestamps = protoOptions.fetch_timestamps();
}

void FromProto(
    TTimeInterval* timeInterval,
    const NClient::NApi::NProto::TTimeInterval& protoTimeInterval)
{
    // TODO(gritukan): Remove it after YP-1254.
    auto protoTimestampToInstant = [] (const google::protobuf::Timestamp& timestamp) {
        return TInstant::Seconds(timestamp.seconds()) + TDuration::MicroSeconds(timestamp.nanos() / 1000);
    };

    if (protoTimeInterval.has_begin()) {
        timeInterval->Begin = protoTimestampToInstant(protoTimeInterval.begin());
    }
    if (protoTimeInterval.has_end()) {
        timeInterval->End = protoTimestampToInstant(protoTimeInterval.end());
    }
}

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TAttributeSelector& selector)
{
    return Format("{Paths: %v}", selector.Paths);
}

////////////////////////////////////////////////////////////////////////////////

struct TChildrenAttributeHelper
{
    static void Add(TChildrenAttributeBase* attribute, TObject* child)
    {
        attribute->DoAdd(child);
    }

    static void Remove(TChildrenAttributeBase* attribute, TObject* child)
    {
        attribute->DoRemove(child);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TQueryContext
    : public IQueryContext
{
public:
    TQueryContext(
        NMaster::TBootstrap* bootstrap,
        EObjectType objectType,
        TQuery* query)
        : Bootstrap_(bootstrap)
        , ObjectType_(objectType)
        , Query_(query)
    { }

    virtual IObjectTypeHandler* GetTypeHandler() override
    {
        return Bootstrap_->GetObjectManager()->GetTypeHandler(ObjectType_);
    }

    virtual TExpressionPtr GetFieldExpression(const TDBField* field) override
    {
        auto it = FieldToExpression_.find(field);
        if (it == FieldToExpression_.end()) {
            auto expr = New<TReferenceExpression>(TSourceLocation(), field->Name, PrimaryTableAlias);
            it = FieldToExpression_.emplace(field, std::move(expr)).first;
        }
        return it->second;
    }

    virtual TExpressionPtr GetAnnotationExpression(const TString& name) override
    {
        auto it = AnnotationNameToExpression_.find(name);
        if (it == AnnotationNameToExpression_.end()) {
            auto foreignTableAlias = AnnotationsTableAliasPrefix + ToString(AnnotationNameToExpression_.size());
            const auto& ytConnector = Bootstrap_->GetYTConnector();
            auto annotationsPath = ytConnector->GetTablePath(&AnnotationsTable);
            Query_->Joins.emplace_back(
                true,
                TTableDescriptor(annotationsPath, foreignTableAlias),
                TExpressionList{
                    New<TReferenceExpression>(TSourceLocation(), ObjectsTable.Fields.Meta_Id.Name, PrimaryTableAlias),
                    New<TLiteralExpression>(TSourceLocation(), static_cast<i64>(ObjectType_)),
                    New<TLiteralExpression>(TSourceLocation(), name)
                },
                TExpressionList{
                    New<TReferenceExpression>(TSourceLocation(), AnnotationsTable.Fields.ObjectId.Name, foreignTableAlias),
                    New<TReferenceExpression>(TSourceLocation(), AnnotationsTable.Fields.ObjectType.Name, foreignTableAlias),
                    New<TReferenceExpression>(TSourceLocation(), AnnotationsTable.Fields.Name.Name, foreignTableAlias)
                },
                std::nullopt);

            auto expr = New<TReferenceExpression>(TSourceLocation(), AnnotationsTable.Fields.Value.Name, foreignTableAlias);
            it = AnnotationNameToExpression_.emplace(name, std::move(expr)).first;
        }
        return it->second;
    }

private:
    NMaster::TBootstrap* const Bootstrap_;
    const EObjectType ObjectType_;
    TQuery* const Query_;

    THashMap<const TDBField*, TExpressionPtr> FieldToExpression_;
    THashMap<TString, TExpressionPtr> AnnotationNameToExpression_;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETransactionTableLookupSessionState,
    (Initialized)
    (Requested)
    (ParsedResults)
);

////////////////////////////////////////////////////////////////////////////////

class TTransaction::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TTransaction* owner,
        NMaster::TBootstrap* bootstrap,
        TTransactionManagerConfigPtr config,
        const TTransactionId& id,
        TTimestamp startTimestamp,
        IClientPtr client,
        ITransactionPtr underlyingTransaction)
        : Owner_(owner)
        , Bootstrap_(bootstrap)
        , Config_(std::move(config))
        , Id_(id)
        , StartTimestamp_(startTimestamp)
        , Client_(std::move(client))
        , UnderlyingTransaction_(std::move(underlyingTransaction))
        , Logger(NYT::NLogging::TLogger(NObjects::Logger)
            .AddTag("TransactionId: %v", Id_))
        , Session_(this)
    {
        SingleVersionRetentionConfig_->MinDataTtl = TDuration::Zero();
        SingleVersionRetentionConfig_->MinDataVersions = 1;
        SingleVersionRetentionConfig_->MaxDataVersions = 1;
        // XXX(babenko): YP-777
        SingleVersionRetentionConfig_->IgnoreMajorTimestamp = true;
    }

    ETransactionState GetState() const
    {
        return State_;
    }

    const TTransactionId& GetId() const
    {
        return Id_;
    }

    TTimestamp GetStartTimestamp() const
    {
        return StartTimestamp_;
    }

    ISession* GetSession()
    {
        return &Session_;
    }


    std::unique_ptr<IUpdateContext> CreateUpdateContext()
    {
        EnsureReadWrite();
        return std::make_unique<TUpdateContext>(this);
    }


    TObject* CreateObject(EObjectType type, const IMapNodePtr& attributes)
    {
        EnsureReadWrite();
        auto context = CreateUpdateContext();
        auto* object = CreateObject(type, attributes, context.get());
        context->Commit();
        return object;
    }

    TObject* CreateObject(EObjectType type, const IMapNodePtr& attributes, IUpdateContext* context)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->GetTypeHandlerOrThrow(type);

        auto* schema = GetSchema(type);
        const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
        accessControlManager->ValidatePermission(schema, EAccessControlPermission::Create);

        EnsureReadWrite();
        return AbortOnException(
            [&] {
                return DoCreateObject(type, attributes, context);
            });
    }


    void UpdateObject(
        TObject* object,
        const std::vector<TUpdateRequest>& requests,
        const std::vector<TAttributeTimestampPrerequisite>& prerequisites)
    {
        EnsureReadWrite();
        auto context = CreateUpdateContext();
        UpdateObject(object, requests, prerequisites, context.get());
        context->Commit();
    }

    void UpdateObject(
        TObject* object,
        const std::vector<TUpdateRequest>& requests,
        const std::vector<TAttributeTimestampPrerequisite>& prerequisites,
        IUpdateContext* context)
    {
        const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
        accessControlManager->ValidatePermission(object, EAccessControlPermission::Write);

        EnsureReadWrite();
        AbortOnException(
            [&] {
                DoUpdateObject(
                    object,
                    requests,
                    prerequisites,
                    context);
            });
    }


    void RemoveObject(TObject* object)
    {
        EnsureReadWrite();
        auto context = CreateUpdateContext();
        RemoveObject(object, context.get());
        context->Commit();
    }

    void RemoveObject(TObject* object, IUpdateContext* context)
    {
        const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
        accessControlManager->ValidatePermission(object, EAccessControlPermission::Write);

        EnsureReadWrite();
        AbortOnException(
            [&] {
                DoRemoveObject(object, context);
            });
    }


    TGetQueryResult ExecuteGetQuery(
        EObjectType type,
        const std::vector<TObjectId>& ids,
        const TAttributeSelector& selector,
        const TGetQueryOptions& options)
    {
        TGetQueryResult result;
        result.Objects.resize(ids.size());

        std::vector<TObject*> requestedObjects;
        requestedObjects.reserve(ids.size());
        for (const auto& id : ids) {
            requestedObjects.push_back(GetObject(type, id));
        }

        if (!options.IgnoreNonexistent) {
            for (auto* object : requestedObjects) {
                object->ValidateExists();
            }
        }

        THashMap<TObjectId, std::vector<size_t>> objectIdToIndexes;
        for (size_t index = 0; index < ids.size(); ++index) {
            objectIdToIndexes[ids[index]].push_back(index);
        }
        auto getIndexesForId = [&] (const auto& id) {
            auto it = objectIdToIndexes.find(id);
            YT_VERIFY(it != objectIdToIndexes.end() && it->second.size() >= 1);
            return it->second;
        };
        auto copyResultsForDuplicateIds = [&] (const auto& indexes) {
            for (size_t i = 1; i < indexes.size(); ++i) {
                YT_VERIFY(!result.Objects[indexes[i]]);
                result.Objects[indexes[i]] = *result.Objects[indexes.front()];
            }
        };

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto* typeHandler = objectManager->GetTypeHandlerOrThrow(type);
        const auto* idField = typeHandler->GetIdField();
        const auto* parentIdField = typeHandler->GetParentIdField();

        TExpressionList keyColumnsExpr;
        if (parentIdField) {
            keyColumnsExpr.emplace_back(New<TReferenceExpression>(TSourceLocation(), parentIdField->Name, PrimaryTableAlias));
        }
        keyColumnsExpr.emplace_back(New<TReferenceExpression>(TSourceLocation(), idField->Name, PrimaryTableAlias));

        TLiteralValueTupleList keyTupleList;
        for (auto* object : requestedObjects) {
            if (!object->DoesExist()) {
                continue;
            }
            auto& tuple = keyTupleList.emplace_back();
            if (parentIdField) {
                tuple.emplace_back(object->GetParentId());
            }
            tuple.emplace_back(object->GetId());
        }

        if (keyTupleList.empty()) {
            return result;
        }

        auto inExpression = New<TInExpression>(
            TSourceLocation(),
            std::move(keyColumnsExpr),
            std::move(keyTupleList));

        auto query = MakeQuery(typeHandler);

        query->WherePredicate = TExpressionList{std::move(inExpression)};

        TQueryContext queryContext(
            Bootstrap_,
            type,
            query.get());
        TAttributeFetcherContext fetcherContext(&queryContext);

        TResolvePermissions permissions;
        auto resolveResults = ResolveAttributes(
            &queryContext,
            selector,
            &permissions);

        auto fetchers = BuildAttributeFetchers(
            Owner_,
            query.get(),
            &fetcherContext,
            &queryContext,
            resolveResults);
        auto queryString = FormatQuery(*query);

        YT_LOG_DEBUG("Getting objects (ObjectIds: %v, Query: %v)",
            ids,
            queryString);

        auto rowset = RunSelect(queryString);
        auto rows = rowset->GetRows();

        PrefetchAttributeValues(rows, fetchers);

        auto rowObjects = fetcherContext.GetObjects(Owner_, rows);

        ValidateObjectPermissions(rowObjects, permissions);

        if (options.FetchTimestamps) {
            PrefetchAttributeTimestamps(rowObjects, resolveResults);
        }

        for (size_t rowIndex = 0; rowIndex < rows.size(); ++rowIndex) {
            auto row = rows[rowIndex];
            auto* object = rowObjects[rowIndex];

            const auto& indexes = getIndexesForId(fetcherContext.GetObjectId(row));
            auto& valueList = result.Objects[indexes.front()];
            YT_VERIFY(!valueList);
            valueList.emplace();

            if (options.FetchValues) {
                FillAttributeValues(&*valueList, row, fetchers);
            }

            if (options.FetchTimestamps) {
                FillAttributeTimestamps(&*valueList, object, resolveResults);
            }

            copyResultsForDuplicateIds(indexes);
        }

        return result;
    }

    TSelectQueryResult ExecuteSelectQuery(
        EObjectType type,
        const std::optional<TObjectFilter>& filter,
        const TAttributeSelector& selector,
        const TSelectQueryOptions& options)
    {
        auto limit = options.Limit;
        if (limit && *limit < 0) {
            THROW_ERROR_EXCEPTION("Negative limit value");
        }

        auto offset = options.Offset;
        if (offset && *offset < 0) {
            THROW_ERROR_EXCEPTION("Negative offset value");
        }

        if (offset) {
            if (limit) {
                limit = *limit + *offset;
            } else {
                limit = Config_->InputRowLimit;
            }
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto* typeHandler = objectManager->GetTypeHandlerOrThrow(type);

        auto query = MakeQuery(typeHandler);

        TQueryContext queryContext(
            Bootstrap_,
            type,
            query.get());

        auto resolveResults = ResolveAttributes(
            &queryContext,
            selector);

        TAttributeFetcherContext fetcherContext(&queryContext);
        auto fetchers = BuildAttributeFetchers(
            Owner_,
            query.get(),
            &fetcherContext,
            &queryContext,
            resolveResults);

        auto predicateExpr = BuildAndExpression(
            filter
            ? BuildFilterExpression(&queryContext, *filter)
            : nullptr,
            BuildObjectFilterByRemovalTime());
        query->WherePredicate = {std::move(predicateExpr)};
        query->Limit = limit;

        auto queryString = FormatQuery(*query);

        YT_LOG_DEBUG("Selecting objects (Type: %v, Query: %v)",
            type,
            queryString);

        auto rowset = RunSelect(queryString);

        // TODO(babenko): YP-921; use QL offset feature
        auto rows = rowset->GetRows();
        if (offset) {
            if (offset > rows.Size()) {
                rows = TRange<TUnversionedRow>();
            } else {
                rows = rows.Slice(*offset, rows.Size());
            }
        }

        // NB: Avoid instantiating objects unless needed.
        std::vector<TObject*> rowObjects;
        if (options.FetchTimestamps) {
            rowObjects = fetcherContext.GetObjects(Owner_, rows);
        }

        YT_LOG_DEBUG("Prefetching results");

        if (options.FetchValues) {
            PrefetchAttributeValues(rows, fetchers);
        }

        if (options.FetchTimestamps) {
            PrefetchAttributeTimestamps(rowObjects, resolveResults);
        }

        YT_LOG_DEBUG("Fetching results");

        TSelectQueryResult result;
        for (size_t rowIndex = 0; rowIndex < rows.size(); ++rowIndex) {
            auto& valueList = result.Objects.emplace_back();

            if (options.FetchValues) {
                FillAttributeValues(&valueList, rows[rowIndex], fetchers);
            }

            if (options.FetchTimestamps) {
                FillAttributeTimestamps(&valueList, rowObjects[rowIndex], resolveResults);
            }
        }

        return result;
    }


    TSelectObjectHistoryResult ExecuteSelectObjectHistoryQuery(
        EObjectType objectType,
        const TObjectId& objectId,
        const TAttributeSelector& attributeSelector,
        TSelectObjectHistoryOptions options)
    {
        constexpr i64 MaximumEventsLimit = 100'000;

        if (options.Limit && *options.Limit > MaximumEventsLimit) {
            THROW_ERROR_EXCEPTION("The number of requested events exceeds the limit: %v > %v",
                *options.Limit,
                MaximumEventsLimit);
        }
        if (!options.Limit) {
            options.Limit = MaximumEventsLimit + 1;
        }

        auto query = GetObjectHistorySelectionQuery(objectType, objectId, options);
        YT_LOG_DEBUG("Selecting object history (Query: %v)",
            query);

        auto rowset = RunSelect(query);
        auto rows = rowset->GetRows();

        if (rows.size() > MaximumEventsLimit) {
            THROW_ERROR_EXCEPTION("Too many events selected, limit: %v", MaximumEventsLimit);
        }

        TSelectObjectHistoryResult result;
        for (auto row : rows) {
            auto& event = result.Events.emplace_back();
            TYsonString objectValue, historyEnabledAttributePaths;

            FromUnversionedRow(
                row,
                &event.Time,
                &event.EventType,
                &event.User,
                &objectValue,
                &historyEnabledAttributePaths);

            event.HistoryEnabledAttributes = ConvertTo<TVector<TYPath>>(historyEnabledAttributePaths);

            for (const auto& attributePath : attributeSelector.Paths) {
                auto attributeValue = TryGetAny(objectValue.GetData(), attributePath);
                if (attributeValue) {
                    event.Attributes.Values.emplace_back(*attributeValue);
                } else {
                    event.Attributes.Values.emplace_back(ConvertToYsonString(GetEphemeralNodeFactory()->CreateEntity()));
                }
            }
        }

        return result;
    }

    IUnversionedRowsetPtr SelectFields(
        EObjectType type,
        const std::vector<const TDBField*>& fields)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto* typeHandler = objectManager->GetTypeHandlerOrThrow(type);
        auto query = MakeQuery(typeHandler);

        TQueryContext queryContext(
            Bootstrap_,
            type,
            query.get());

        for (const auto* field : fields) {
            auto fieldExpression = queryContext.GetFieldExpression(field);
            query->SelectExprs->push_back(std::move(fieldExpression));
        }

        EnsureNonEmptySelectExpressions(query.get());

        query->WherePredicate = {BuildObjectFilterByRemovalTime()};

        auto queryString = FormatQuery(*query);

        YT_LOG_DEBUG("Selecting objects (Type: %v, Query: %v)",
            type,
            queryString);

        return RunSelect(queryString);
    }


    TObject* GetObject(EObjectType type, const TObjectId& id, const TObjectId& parentId = {})
    {
        return Session_.GetObject(type, id, parentId);
    }

    TSchema* GetSchema(EObjectType type)
    {
        static const auto typeToSchemaIdMap = BuildTypeToSchemaIdMap();
        const auto& id = typeToSchemaIdMap[type];
        return GetTypedObject<TSchema>(id);
    }


    TNode* GetNode(const TObjectId& id)
    {
        return GetTypedObject<TNode>(id);
    }

    TNode* CreateNode(const TObjectId& id = {})
    {
        EnsureReadWrite();
        return CreateTypedObject<TNode>(id, {});
    }


    TNodeSegment* GetNodeSegment(const TObjectId& id)
    {
        return GetTypedObject<TNodeSegment>(id);
    }


    TPod* GetPod(const TObjectId& id)
    {
        return GetTypedObject<TPod>(id);
    }


    TPodSet* GetPodSet(const TObjectId& id)
    {
        return GetTypedObject<TPodSet>(id);
    }


    TPodSet* CreatePodSet(const TObjectId& id = {})
    {
        return CreateTypedObject<TPodSet>(id, {});
    }


    TResource* GetResource(const TObjectId& id)
    {
        return GetTypedObject<TResource>(id);
    }


    TNetworkProject* GetNetworkProject(const TObjectId& id)
    {
        return GetTypedObject<TNetworkProject>(id);
    }


    TVirtualService* GetVirtualService(const TObjectId& id)
    {
        return GetTypedObject<TVirtualService>(id);
    }


    TDnsRecordSet* GetDnsRecordSet(const TObjectId& id)
    {
        return GetTypedObject<TDnsRecordSet>(id);
    }


    TDnsRecordSet* CreateDnsRecordSet(const TObjectId& id)
    {
        return CreateTypedObject<TDnsRecordSet>(id, {});
    }


    TInternetAddress* GetInternetAddress(const TObjectId& id)
    {
        return GetTypedObject<TInternetAddress>(id);
    }


    TAccount* GetAccount(const TObjectId& id)
    {
        return GetTypedObject<TAccount>(id);
    }


    TUser* GetUser(const TObjectId& id)
    {
        return GetTypedObject<TUser>(id);
    }


    TGroup* GetGroup(const TObjectId& id)
    {
        return GetTypedObject<TGroup>(id);
    }

    TPodDisruptionBudget* GetPodDisruptionBudget(const TObjectId& id)
    {
        return GetTypedObject<TPodDisruptionBudget>(id);
    }

    TIP4AddressPool* GetIP4AddressPool(const TObjectId& id)
    {
        return GetTypedObject<TIP4AddressPool>(id);
    }


    TFuture<TTransactionCommitResult> Commit()
    {
        EnsureReadWrite();
        State_ = ETransactionState::Committing;

        for (const auto& validator : Validators_) {
            validator();
        }

        const auto& resourceManager = Bootstrap_->GetResourceManager();
        const auto& netManager = Bootstrap_->GetNetManager();

        // TODO(avitella): Do it via scheduler.
        NNet::TInternetAddressManager internetAddressManager;
        NScheduler::TResourceManagerContext resourceManagerContext{
            netManager.Get(),
            &internetAddressManager,
        };

        for (auto* node : NodesAwaitingResourceValidation_) {
            if (node->DoesExist()) {
                resourceManager->ValidateNodeResource(node);
            }
        }

        for (auto* pod : PodsAwaitingSpecUpdate_) {
            if (pod->DoesExist()) {
                resourceManager->UpdatePodSpec(Owner_, pod);
            }
        }

        const auto& accountingManager = Bootstrap_->GetAccountingManager();
        accountingManager->ValidateAccounting(std::vector<TPod*>(
            PodsAwaitingAccountingValidation_.begin(),
            PodsAwaitingAccountingValidation_.end()));

        for (auto* pod : PodsAwaitingResourceAllocation_) {
            if (pod->DoesExist()) {
                resourceManager->ReallocatePodResources(Owner_, &resourceManagerContext, pod);
            }
        }

        Session_.FlushTransaction();

        return UnderlyingTransaction_->Commit()
            .Apply(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<NApi::TTransactionCommitResult>& underlyingResultOrError) {
                if (!underlyingResultOrError.IsOK()) {
                    State_ = ETransactionState::Failed;

                    THROW_ERROR_EXCEPTION("Error committing DB transaction")
                        << underlyingResultOrError;
                }

                const auto& underlyingResult = underlyingResultOrError.Value();
                YT_ASSERT(underlyingResult.CommitTimestamps.Timestamps.size() <= 1);
                auto timestamp = underlyingResult.CommitTimestamps.Timestamps.empty()
                    ? GetStartTimestamp()
                    : underlyingResult.CommitTimestamps.Timestamps[0].second;

                State_ = ETransactionState::Committed;

                YT_LOG_DEBUG("Transaction committed (CommitTimestamp: %llx)",
                    timestamp);

                const auto& nodeTracker = Bootstrap_->GetNodeTracker();
                for (auto* node : AgentsAwaitingNotifcation_) {
                    nodeTracker->NotifyAgent(node);
                }

                return TTransactionCommitResult{
                    timestamp
                };
            }));
    }

    void Abort()
    {
        EnsureReadWrite();
        YT_LOG_DEBUG("Transaction aborted");
        State_ = ETransactionState::Aborted;
        UnderlyingTransaction_->Abort();
    }


    void ScheduleNotifyAgent(TNode* node)
    {
        EnsureReadWrite();
        node->Status().AgentAddress().ScheduleLoad();
        if (AgentsAwaitingNotifcation_.insert(node).second) {
            YT_LOG_DEBUG("Agent notification scheduled (NodeId: %v)",
                node->GetId());
        }
    }

    void ScheduleAllocateResources(TPod* pod)
    {
        EnsureReadWrite();
        if (PodsAwaitingResourceAllocation_.insert(pod).second) {
            YT_LOG_DEBUG("Pod resource allocation scheduled (PodId: %v)",
                pod->GetId());
        }
    }

    void ScheduleValidateNodeResources(TNode* node)
    {
        EnsureReadWrite();
        if (NodesAwaitingResourceValidation_.insert(node).second) {
            YT_LOG_DEBUG("Node resource validation scheduled (NodeId: %v)",
                node->GetId());
        }
    }

    void ScheduleUpdatePodSpec(TPod* pod)
    {
        EnsureReadWrite();
        if (PodsAwaitingSpecUpdate_.insert(pod).second) {
            const auto& resourceManager = Bootstrap_->GetResourceManager();
            resourceManager->PrepareUpdatePodSpec(Owner_, pod);

            YT_LOG_DEBUG("Pod spec update scheduled (PodId: %v)",
                pod->GetId());
        }
    }

    void ScheduleValidateAccounting(TPod* pod)
    {
        EnsureReadWrite();
        if (PodsAwaitingAccountingValidation_.insert(pod).second) {
            const auto& accountingManager = Bootstrap_->GetAccountingManager();
            accountingManager->PrepareValidateAccounting(pod);

            YT_LOG_DEBUG("Pod accounting validation scheduled (PodId: %v)",
                pod->GetId());
        }
    }


    TAsyncSemaphoreGuard AcquireLock()
    {
        EnsureReadWrite();
        auto guardHolder = std::make_shared<TAsyncSemaphoreGuard>();
        auto promise = NewPromise<void>();
        Semaphore_->AsyncAcquire(
            BIND([=] (TAsyncSemaphoreGuard guard) mutable {
                *guardHolder = std::move(guard);
                promise.Set();
            }),
            GetSyncInvoker(),
            1);
        WaitFor(promise.ToFuture())
            .ThrowOnError();
        return std::move(*guardHolder);
    }

private:
    class TUpdateContext
        : public IUpdateContext
    {
    public:
        explicit TUpdateContext(TTransaction::TImplPtr transaction)
            : Transaction_(std::move(transaction))
        { }

        virtual void AddSetter(std::function<void()> setter) override
        {
            Setters_.push_back(std::move(setter));
        }

        virtual void AddFinalizer(std::function<void()> finalizer) override
        {
            Finalizers_.push_back(std::move(finalizer));
        }

        void Commit()
        {
            Transaction_->AbortOnException([&] {
                for (const auto& setter : Setters_) {
                    setter();
                }
                for (const auto& finalizer : Finalizers_) {
                    finalizer();
                }
            });
        }

    private:
        const TTransaction::TImplPtr Transaction_;

        std::vector<std::function<void()>> Setters_;
        std::vector<std::function<void()>> Finalizers_;
    };

    class TPersistenceContextBase
    {
    protected:
        TTransaction::TImpl* const Transaction_;

        struct TRowBufferTag { };
        const TRowBufferPtr RowBuffer_ = New<TRowBuffer>(TRowBufferTag());


        explicit TPersistenceContextBase(TTransaction::TImpl* transaction)
            : Transaction_(transaction)
        { }

        TKey CaptureKey(TRange<TUnversionedValue> key)
        {
            auto capturedKey = RowBuffer_->Capture(key.Begin(), key.Size());
            for (size_t index = 0; index < key.Size(); ++index) {
                capturedKey[index].Id = index;
            }
            return capturedKey;
        }
    };

    class TLoadContext
        : public TPersistenceContextBase
        , public ILoadContext
    {
    public:
        explicit TLoadContext(TTransaction::TImpl* transaction)
            : TPersistenceContextBase(transaction)
        { }

        virtual const TRowBufferPtr& GetRowBuffer() override
        {
            return RowBuffer_;
        }

        virtual TString GetTablePath(const TDBTable* table) override
        {
            const auto& ytConnector = Transaction_->Bootstrap_->GetYTConnector();
            return ytConnector->GetTablePath(table);
        }

        virtual void ScheduleLookup(
            const TDBTable* table,
            TRange<TUnversionedValue> key,
            TRange<const TDBField*> fields,
            std::function<void(const std::optional<TRange<NYT::NTableClient::TVersionedValue>>&)> handler) override
        {
            LookupRequestsPerTable_[table].push_back(TLookupRequest{
                CaptureKey(key),
                SmallVector<const TDBField*, TypicalFieldCountPerLookupRequest>(
                    fields.begin(),
                    fields.end()),
                std::move(handler)});
        }

        virtual void ScheduleSelect(
            const TString& query,
            std::function<void(const NYT::NApi::IUnversionedRowsetPtr&)> handler) override
        {
            TSelectRequest request;
            request.Query = query;
            request.Handler = std::move(handler);
            SelectRequests_.emplace_back(std::move(request));
        }

        void RunReads()
        {
            if (SelectRequests_.empty() && LookupRequestsPerTable_.empty()) {
                return;
            }

            const auto& Logger = Transaction_->Logger;

            YT_LOG_DEBUG("Running reads");

            std::vector<TFuture<void>> asyncResults;

            for (auto& request : SelectRequests_) {
                request.Tag = Format("Query: %v",
                    request.Query);

                YT_LOG_DEBUG("Executing select (%v)",
                    request.Tag);

                TSelectRowsOptions options;
                options.Timestamp = Transaction_->StartTimestamp_;
                options.InputRowLimit = Transaction_->Config_->InputRowLimit;
                options.OutputRowLimit = Transaction_->Config_->OutputRowLimit;

                auto asyncResult = Transaction_->Client_->SelectRows(request.Query, options)
                    .Apply(BIND([] (const TErrorOr<TSelectRowsResult>& resultOrError) {
                        THROW_ERROR_EXCEPTION_IF_FAILED(resultOrError, "Error fetching data from DB");
                        return resultOrError.Value().Rowset;
                    }));
                request.AsyncResult = asyncResult;
                asyncResults.push_back(asyncResult.As<void>());
            }

            THashMap<const TDBTable*, TTableLookupSession> tableLookupSessions;

            for (const auto& [table, lookupRequests] : LookupRequestsPerTable_) {
                auto [it, emplaced] = tableLookupSessions.emplace(
                    table,
                    TTableLookupSession{this, table, Logger});
                YT_VERIFY(emplaced);
                auto& tableLookupSession = it->second;
                asyncResults.push_back(tableLookupSession.ExecuteRequests(lookupRequests));
            }

            WaitFor(Combine(asyncResults))
                .ThrowOnError();

            YT_LOG_DEBUG("Reads complete; parsing results");

            std::vector<TError> errors;
            auto guardedRun = [&] (auto f) {
                try {
                    f();
                } catch (const std::exception& ex) {
                    errors.emplace_back(ex);
                }
            };

            for (const auto& request : SelectRequests_) {
                guardedRun([&] {
                    const auto& rowset = request.AsyncResult.Get().Value();
                    YT_LOG_DEBUG("Got select results (%v, RowCount: %v)",
                        request.Tag,
                        rowset->GetRows().Size());
                    request.Handler(rowset);
                });
            }

            for (auto& [table, lookupSession] : tableLookupSessions) {
                lookupSession.ParseResults();
            }

            for (const auto& [table, lookupRequests] : LookupRequestsPerTable_) {
                auto it = tableLookupSessions.find(table);
                YT_VERIFY(it != tableLookupSessions.end());
                const auto& lookupSession = it->second;

                for (const auto& lookupRequest : lookupRequests) {
                    auto optionalHandlerValues = lookupSession.GetResult(lookupRequest);
                    guardedRun([&] {
                        if (optionalHandlerValues) {
                            lookupRequest.Handler(MakeRange(*optionalHandlerValues));
                        } else {
                            lookupRequest.Handler(std::nullopt);
                        }
                    });
                }
            }

            YT_LOG_DEBUG("Results parsed");

            if (!errors.empty()) {
                THROW_ERROR_EXCEPTION("Error parsing database results")
                    << std::move(errors);
            }
        }

    private:
        static constexpr int TypicalFieldCountPerLookupRequest = 4;

        struct TSelectRequest
        {
            TString Query;
            std::function<void(const NYT::NApi::IUnversionedRowsetPtr&)> Handler;
            TFuture<IUnversionedRowsetPtr> AsyncResult;
            TString Tag;
        };

        std::vector<TSelectRequest> SelectRequests_;

        struct TLookupRequest
        {
            TKey Key;
            SmallVector<const TDBField*, TypicalFieldCountPerLookupRequest> Fields;
            std::function<void(const std::optional<TRange<NYT::NTableClient::TVersionedValue>>&)> Handler;
        };

        THashMap<const TDBTable*, std::vector<TLookupRequest>> LookupRequestsPerTable_;

        class TTableLookupSession
        {
        public:
            TTableLookupSession(
                TLoadContext* owner,
                const TDBTable* table,
                const NYT::NLogging::TLogger& logger)
                : Owner_(owner)
                , Table_(table)
                , TablePath_(Owner_->GetTablePath(Table_))
                , Logger(logger)
            { }

            TFuture<void> ExecuteRequests(const std::vector<TLookupRequest>& requests)
            {
                YT_VERIFY(State_ == ETransactionTableLookupSessionState::Initialized);
                State_ = ETransactionTableLookupSessionState::Requested;

                auto transaction = Owner_->Transaction_;

                YT_LOG_DEBUG("Building rectangle lookup requests (Path: %v)", TablePath_);

                FieldNameTable_ = transaction->BuildNameTable(Table_);

                std::vector<NGeometric2DSetCover::TRectangle<TKey>> rectangles;
                {
                    std::vector<NGeometric2DSetCover::TPoint<TKey>> points;
                    for (const auto& request : requests) {
                        for (const auto* field : request.Fields) {
                            auto fieldId = FieldNameTable_->GetIdOrRegisterName(field->Name);
                            points.emplace_back(request.Key, fieldId);
                        }
                    }
                    rectangles = NGeometric2DSetCover::BuildPerColumnSetCovering(
                        points,
                        transaction->Config_->MaxKeysPerLookupRequest);
                }

                RectangleRequests_.reserve(rectangles.size());
                for (auto& rectangle : rectangles) {
                    auto& rectangleRequest = RectangleRequests_.emplace_back();
                    rectangleRequest.Keys = std::move(rectangle.Rows);
                    rectangleRequest.FieldIds = std::move(rectangle.Columns);
                }

                std::vector<TFuture<void>> asyncResults;
                for (auto& rectangleRequest : RectangleRequests_) {
                    // Versioned lookup is needed for the TTimestampAttribute support.
                    // For the sake of convenience we use versioned lookup for all attributes.
                    TVersionedLookupRowsOptions options;
                    options.Timestamp = transaction->StartTimestamp_;
                    options.KeepMissingRows = true;
                    options.RetentionConfig = transaction->SingleVersionRetentionConfig_;
                    options.ColumnFilter = TColumnFilter(rectangleRequest.FieldIds);

                    auto keysRange = MakeSharedRange(rectangleRequest.Keys, Owner_->GetRowBuffer());

                    YT_LOG_DEBUG("Executing rectangle lookup request (%v)",
                        FormatRectangleRequest(rectangleRequest));

                    auto asyncResult = transaction->Client_->VersionedLookupRows(
                        TablePath_,
                        FieldNameTable_,
                        std::move(keysRange),
                        options);

                    rectangleRequest.AsyncResult = asyncResult.Apply(
                        BIND([] (const TErrorOr<IVersionedRowsetPtr>& rowsetOrError) {
                            THROW_ERROR_EXCEPTION_IF_FAILED(rowsetOrError, "Error fetching data from DB");
                            return rowsetOrError.Value();
                        }));

                    asyncResults.push_back(rectangleRequest.AsyncResult.As<void>());
                }

                return Combine(std::move(asyncResults));
            }

            void ParseResults()
            {
                YT_VERIFY(State_ == ETransactionTableLookupSessionState::Requested);
                State_ = ETransactionTableLookupSessionState::ParsedResults;

                YT_LOG_DEBUG("Parsing lookup session results (Path: %v)", TablePath_);

                for (const auto& rectangleRequest : RectangleRequests_) {
                    YT_VERIFY(rectangleRequest.AsyncResult.IsSet());
                    const auto& result = rectangleRequest.AsyncResult.Get().Value();

                    auto rows = result->GetRows();
                    YT_VERIFY(rows.Size() == rectangleRequest.Keys.size());

                    // Value.Id is actually a position in the rectangle request column filter.
                    auto getFieldIdByValueId = [&rectangleRequest] (int valueId) {
                        YT_VERIFY(0 <= valueId &&
                            valueId < static_cast<int>(rectangleRequest.FieldIds.size()));
                        return rectangleRequest.FieldIds[valueId];
                    };

                    for (size_t keyIndex = 0;
                        keyIndex < rectangleRequest.Keys.size();
                        ++keyIndex)
                    {
                        auto key = rectangleRequest.Keys[keyIndex];
                        auto tag = FormatRectangleRequestForKey(rectangleRequest, key);

                        auto row = rows[keyIndex];
                        if (!row) {
                            YT_LOG_DEBUG("Got missing lookup row (%v)", tag);
                            continue;
                        }

                        auto maxWriteTimestamp =
                            (row.BeginWriteTimestamps() == row.EndWriteTimestamps())
                            ? MinTimestamp
                            : row.BeginWriteTimestamps()[0];

                        auto maxDeleteTimestamp =
                            (row.BeginDeleteTimestamps() == row.EndDeleteTimestamps())
                            ? MinTimestamp
                            : row.BeginDeleteTimestamps()[0];

                        if (maxWriteTimestamp <= maxDeleteTimestamp) {
                            YT_LOG_DEBUG("Got dead lookup row (%v, Row: %v)", tag, row);
                            continue;
                        }

                        auto& rowResult = GetOrEmplaceRowResult(key);

                        for (const auto* key = row.BeginKeys(); key != row.EndKeys(); ++key) {
                            TVersionedValue value;
                            static_cast<TUnversionedValue&>(value) = *key;
                            auto fieldId = getFieldIdByValueId(value.Id);
                            // NB! Reinitialization is possible even for the key field because
                            //     one row can be contained in the several rectangles.
                            if (!rowResult[fieldId]) {
                                rowResult[fieldId] = value;
                            }
                        }

                        for (const auto* value = row.BeginValues();
                            value != row.EndValues();
                            ++value)
                        {
                            auto fieldId = getFieldIdByValueId(value->Id);
                            // NB! Only initialize result with last written value.
                            if (!rowResult[fieldId]) {
                                rowResult[fieldId] = *value;
                            }
                        }

                        YT_LOG_DEBUG("Got lookup row (%v, Row: %v)", tag, row);
                    }
                }
            }

            std::optional<SmallVector<TVersionedValue, TypicalFieldCountPerLookupRequest>> GetResult(
                const TLookupRequest& request) const
            {
                YT_VERIFY(State_ == ETransactionTableLookupSessionState::ParsedResults);

                auto resultIt = Results_.find(request.Key);
                if (resultIt == Results_.end()) {
                    return std::nullopt;
                }

                SmallVector<TVersionedValue, TypicalFieldCountPerLookupRequest> result;
                result.reserve(request.Fields.size());

                const auto& rowResult = resultIt->second;
                for (const auto* field : request.Fields) {
                    auto fieldId = FieldNameTable_->GetId(field->Name);
                    const auto& optionalValue = rowResult[fieldId];
                    if (optionalValue) {
                        result.push_back(*optionalValue);
                    } else {
                        result.push_back(MakeVersionedSentinelValue(
                            EValueType::Null, NullTimestamp));
                    }
                }

                return result;
            }

        private:
            TLoadContext* const Owner_;
            const TDBTable* const Table_;
            const TString TablePath_;
            const NYT::NLogging::TLogger& Logger;

            ETransactionTableLookupSessionState State_
                = ETransactionTableLookupSessionState::Initialized;

            TNameTablePtr FieldNameTable_;

            struct TRectangleRequest
            {
                std::vector<TKey> Keys;
                std::vector<int> FieldIds;
                TFuture<IVersionedRowsetPtr> AsyncResult;
            };

            std::vector<TRectangleRequest> RectangleRequests_;

            // The following matrix is indexed by (key, fieldId).
            // NB! It stores non-owning versioned values:
            //     holders are stored in the rectangle requests async results.
            using TRowResult = SmallVector<
                std::optional<TVersionedValue>,
                TypicalColumnCountPerDBTable
            >;
            THashMap<TKey, TRowResult> Results_;

            TRowResult& GetOrEmplaceRowResult(TKey key)
            {
                auto it = Results_.find(key);
                if (it == Results_.end()) {
                    it = Results_.emplace(key, TRowResult()).first;
                    auto& rowResult = it->second;
                    rowResult.assign(FieldNameTable_->GetSize(), std::nullopt);
                }
                return it->second;
            }

            TString FormatRectangleRequestForKey(
                const TRectangleRequest& request,
                TKey key) const
            {
                return Format("Path: %v, Columns: %v, Key: %v",
                    TablePath_,
                    MakeFormattableView(
                        request.FieldIds,
                        [this] (auto* builder, int fieldId) {
                            FormatValue(
                                builder,
                                FieldNameTable_->GetName(fieldId),
                                TStringBuf());
                        }),
                    key);
            }

            TString FormatRectangleRequest(const TRectangleRequest& request) const
            {
                return Format("Path: %v, Columns: %v, KeyCount: %v, Keys: %v",
                    TablePath_,
                    MakeFormattableView(
                        request.FieldIds,
                        [this] (auto* builder, int fieldId) {
                            FormatValue(
                                builder,
                                FieldNameTable_->GetName(fieldId),
                                TStringBuf());
                        }),
                    request.Keys.size(),
                    request.Keys);
            }
        };
    };

    class TStoreContext
        : public TPersistenceContextBase
        , public IStoreContext
    {
    public:
        explicit TStoreContext(TTransaction::TImpl* transaction)
            : TPersistenceContextBase(transaction)
        { }

        virtual const TRowBufferPtr& GetRowBuffer() override
        {
            return RowBuffer_;
        }

        virtual void WriteRow(
            const TDBTable* table,
            TRange<TUnversionedValue> key,
            TRange<const TDBField*> fields,
            TRange<TUnversionedValue> values) override
        {
            YT_ASSERT(key.Size() == table->Key.size());
            YT_ASSERT(fields.Size() == values.Size());
            WriteRequests_[table].push_back(TWriteRequest{
                CaptureKey(key),
                SmallVector<const TDBField*, 4>(fields.begin(), fields.end()),
                SmallVector<TUnversionedValue, 4>(values.begin(), values.end())
            });
        }

        virtual void DeleteRow(
            const TDBTable* table,
            TRange<TUnversionedValue> key) override
        {
            YT_ASSERT(key.Size() == table->Key.size());
            DeleteRequests_[table].push_back(TDeleteRequest{
                CaptureKey(key)
            });
        }

        void FillTransaction(const ITransactionPtr& transaction)
        {
            const auto& Logger = Transaction_->Logger;

            for (const auto& pair : WriteRequests_) {
                const auto* table = pair.first;

                auto path = GetTablePath(table);

                auto nameTable = BuildNameTable(table);

                THashMap<const TDBField*, int> fieldToId;
                SmallVector<const TDBField*, 64> idToField;

                const auto& requests = pair.second;

                for (const auto& request : requests) {
                    for (const auto* field : request.Fields) {
                        auto it = fieldToId.find(field);
                        if (it == fieldToId.end()) {
                            YT_VERIFY(fieldToId.emplace(field, nameTable->RegisterName(field->Name)).second);
                            idToField.push_back(field);
                        }
                    }
                }

                std::vector<TUnversionedRow> rows;
                rows.reserve(requests.size());

                for (const auto& request : requests) {
                    auto row = RowBuffer_->AllocateUnversioned(table->Key.size() + request.Fields.size());
                    for (size_t index = 0; index < table->Key.size(); ++index) {
                        row[index] = request.Key[index];
                        row[index].Id = index;
                    }
                    for (size_t index = 0; index < request.Fields.size(); ++index) {
                        auto& value = row[index + table->Key.size()];
                        value = request.Values[index];
                        value.Id = fieldToId[request.Fields[index]];
                    }
                    rows.push_back(row);
                    YT_LOG_DEBUG("Executing write (Path: %v, Columns: %v, Row: %v)",
                        path,
                        MakeFormattableView(MakeRange(row.Begin() + table->Key.size(), row.End()), [&] (TStringBuilderBase* builder, const auto& value) {
                            FormatValue(builder, idToField[value.Id - table->Key.size()]->Name, TStringBuf());
                        }),
                        row);
                }

                transaction->WriteRows(
                    path,
                    std::move(nameTable),
                    MakeSharedRange(std::move(rows), RowBuffer_));
            }

            for (const auto& pair : DeleteRequests_) {
                const auto* table = pair.first;

                auto path = GetTablePath(table);

                auto nameTable = BuildNameTable(table);

                const auto& requests = pair.second;

                std::vector<TKey> keys;
                keys.reserve(requests.size());

                for (const auto& request : requests) {
                    auto key = RowBuffer_->AllocateUnversioned(table->Key.size());
                    for (size_t index = 0; index < table->Key.size(); ++index) {
                        key[index] = request.Key[index];
                        key[index].Id = index;
                    }
                    keys.push_back(key);
                    YT_LOG_DEBUG("Executing delete (Path: %v, Key: %v)",
                        path,
                        key);
                }

                transaction->DeleteRows(
                    path,
                    std::move(nameTable),
                    MakeSharedRange(std::move(keys), RowBuffer_));
            }
        }

    private:
        struct TWriteRequest
        {
            TKey Key;
            SmallVector<const TDBField*, 4> Fields;
            SmallVector<TUnversionedValue, 4> Values;
        };

        THashMap<const TDBTable*, std::vector<TWriteRequest>> WriteRequests_;

        struct TDeleteRequest
        {
            TKey Key;
        };

        THashMap<const TDBTable*, std::vector<TDeleteRequest>> DeleteRequests_;


        TYPath GetTablePath(const TDBTable* table)
        {
            const auto& ytConnector = Transaction_->Bootstrap_->GetYTConnector();
            return ytConnector->GetTablePath(table);
        }
    };


    class TSession
        : public ISession
    {
    public:
        explicit TSession(TImpl* owner)
            : Owner_(owner)
            , Logger(Owner_->Logger)
        { }

        void FlushTransaction()
        {
            FlushObjectsDeletion();
            ValidateCreatedObjects();
            FlushObjectsCreation();
            if (Owner_->Bootstrap_->GetObjectManager()->IsHistoryEnabled()) {
                FlushHistoryEvents();
            }
            FlushWatchLogObjectsUpdates();
            std::vector<TError> errors;
            while (HasPendingLoads() || HasPendingStores()) {
                FlushLoadsOnce(&errors);
                FlushStoresOnce(&errors);
            }
            if (!errors.empty()) {
                THROW_ERROR_EXCEPTION("Persistence failure")
                    << errors;
            }
        }

        // ISession implementation.
        virtual IObjectTypeHandler* GetTypeHandler(EObjectType type) override
        {
            const auto& objectManager = Owner_->Bootstrap_->GetObjectManager();
            return objectManager->GetTypeHandler(type);
        }

        virtual TObject* CreateObject(
            EObjectType type,
            const TObjectId& id,
            const TObjectId& parentId) override
        {
            Owner_->EnsureReadWrite();

            auto actualId = GenerateId(id);
            ValidateObjectId(type, actualId);

            auto key = std::make_pair(type, actualId);
            auto it = InstantiatedObjects_.find(key);
            if (it != InstantiatedObjects_.end()) {
                auto* existingObject = it->second.get();
                auto existingObjectState = existingObject->GetState();
                if (existingObjectState != EObjectState::Removing &&
                    existingObjectState != EObjectState::Removed)
                {
                    THROW_ERROR_EXCEPTION(
                        NClient::NApi::EErrorCode::InvalidObjectState,
                        "%v %Qv is already in %Qlv state",
                        GetCapitalizedHumanReadableTypeName(type),
                        actualId,
                        existingObject->GetState());
                }
            }

            const auto& objectManager = Owner_->Bootstrap_->GetObjectManager();
            auto* typeHandler = objectManager->GetTypeHandlerOrThrow(type);
            auto parentType = typeHandler->GetParentType();
            if (parentType != EObjectType::Null && !parentId) {
                THROW_ERROR_EXCEPTION("Objects of type %Qlv require explicit parent of type %Qlv",
                    type,
                    parentType);
            }
            if (parentType == EObjectType::Null && parentId) {
                THROW_ERROR_EXCEPTION("Objects of type %Qlv do not require explicit parent",
                    type);
            }

            auto objectHolder = typeHandler->InstantiateObject(actualId, parentId, this);
            auto* object = objectHolder.get();

            YT_VERIFY(InstantiatedObjects_.emplace(key, std::move(objectHolder)).second);
            object->InitializeCreating();

            YT_VERIFY(CreatedObjects_.emplace(key, object).second);

            typeHandler->BeforeObjectCreated(Owner_->Owner_, object);

            if (parentType != EObjectType::Null) {
                auto* parent = GetObject(parentType, parentId);

                const auto& accessControlManager = Owner_->Bootstrap_->GetAccessControlManager();
                accessControlManager->ValidatePermission(parent, EAccessControlPermission::Write);

                auto* attribute = typeHandler->GetParentChildrenAttribute(parent);
                TChildrenAttributeHelper::Add(attribute, object);
            }

            YT_LOG_DEBUG("Object created (ObjectId: %v, ParentId: %v, Type: %v)",
                actualId,
                parentId,
                type);

            return object;
        }

        virtual TObject* GetObject(EObjectType type, const TObjectId& id, const TObjectId& parentId = {}) override
        {
            if (!id) {
                THROW_ERROR_EXCEPTION(
                    NClient::NApi::EErrorCode::InvalidObjectId,
                    "%v id cannot be empty",
                    GetCapitalizedHumanReadableTypeName(type));
            }

            auto key = std::make_pair(type, id);
            auto instantiatedIt = InstantiatedObjects_.find(key);
            if (instantiatedIt == InstantiatedObjects_.end()) {
                auto removedIt = RemovedObjects_[type].find(id);
                if (removedIt != RemovedObjects_[type].end()) {
                    return removedIt->second;
                }
                const auto& objectManager = Owner_->Bootstrap_->GetObjectManager();
                auto* typeHandler = objectManager->GetTypeHandlerOrThrow(type);
                auto objectHolder = typeHandler->InstantiateObject(id, parentId, this);
                auto* object = objectHolder.get();
                instantiatedIt = InstantiatedObjects_.emplace(key, std::move(objectHolder)).first;
                object->InitializeInstantiated();

                YT_LOG_DEBUG("Object instantiated (ObjectId: %v, ParentId: %v, Type: %v)",
                    id,
                    typeHandler->GetParentType() == EObjectType::Null
                        ? "<None>"
                        : (parentId ? parentId.c_str() : "<Unknown>"),
                    type);
            }

            return instantiatedIt->second.get();
        }

        virtual void RemoveObject(TObject* object) override
        {
            Owner_->EnsureReadWrite();

            auto state = object->GetState();
            YT_VERIFY(state != EObjectState::Creating);
            if (state == EObjectState::Removing ||
                state == EObjectState::Removed ||
                state == EObjectState::CreatedRemoving ||
                state == EObjectState::CreatedRemoved)
            {
                return;
            }

            object->GetTypeHandler()->BeforeObjectRemoved(Owner_->Owner_, object);

            if (state == EObjectState::Created) {
                object->SetState(EObjectState::CreatedRemoving);
            } else {
                object->SetState(EObjectState::Removing);
            }

            YT_LOG_DEBUG("Object removed (ObjectId: %v, Type: %v)",
                object->GetId(),
                object->GetType());

            object->GetTypeHandler()->AfterObjectRemoved(Owner_->Owner_, object);

            for (auto* attribute : object->Attributes()) {
                attribute->OnObjectRemoved();
            }

            auto key = std::make_pair(object->GetType(), object->GetId());
            if (state == EObjectState::Created) {
                object->SetState(EObjectState::CreatedRemoved);
            } else {
                RemovedObjects_[object->GetType()][object->GetId()] = object;
                object->SetState(EObjectState::Removed);
            }

            {
                auto it = InstantiatedObjects_.find(key);
                if (it != InstantiatedObjects_.end()) {
                    RemovedObjectsHolders_.emplace_back(std::move(it->second));
                    InstantiatedObjects_.erase(it);
                }
            }
            {
                auto it = CreatedObjects_.find(key);
                if (it != CreatedObjects_.end()) {
                    CreatedObjects_.erase(it);
                }
            }

            auto* typeHandler = object->GetTypeHandler();
            auto parentType = typeHandler->GetParentType();
            if (parentType != EObjectType::Null) {
                auto* parent = GetObject(parentType, object->GetParentId());
                auto* attribute = typeHandler->GetParentChildrenAttribute(parent);
                TChildrenAttributeHelper::Remove(attribute, object);
            }

            const auto& objectManager = Owner_->Bootstrap_->GetObjectManager();
            for (auto childrenType : TEnumTraits<EObjectType>::GetDomainValues()) {
                auto* childrenTypeHandler = objectManager->FindTypeHandler(childrenType);
                if (!childrenTypeHandler) {
                    continue;
                }
                if (childrenTypeHandler->GetParentType() != object->GetType()) {
                    continue;
                }
                auto* attribute = childrenTypeHandler->GetParentChildrenAttribute(object);
                // NB: Make a copy of children.
                auto children = attribute->UntypedLoad();
                for (auto* child : children) {
                    RemoveObject(child);
                }
            }
        }

        virtual void ScheduleLoad(TLoadCallback callback, int priority = ISession::DefaultLoadPriority) override
        {
            YT_ASSERT(priority >= 0 && priority < LoadPriorityCount);
            ScheduledLoads_[priority].push_back(std::move(callback));
        }

        virtual void ScheduleStore(TStoreCallback callback) override
        {
            Owner_->EnsureReadWrite();
            ScheduledStores_.push_back(std::move(callback));
        }

        virtual void FlushLoads() override
        {
            std::vector<TError> errors;
            while (HasPendingLoads()) {
                FlushLoadsOnce(&errors);
            }
            if (!errors.empty()) {
                THROW_ERROR_EXCEPTION("Persistence failure")
                    << errors;
            }
        }

    private:
        TImpl* const Owner_;
        const NLogging::TLogger& Logger;

        THashMap<std::pair<EObjectType, TObjectId>, std::unique_ptr<TObject>> InstantiatedObjects_;
        TVector<std::unique_ptr<TObject>> RemovedObjectsHolders_;

        THashMap<std::pair<EObjectType, TObjectId>, TObject*> CreatedObjects_;
        TEnumIndexedVector<EObjectType, THashMap<TObjectId, TObject*>> RemovedObjects_;

        std::array<std::vector<TLoadCallback>, LoadPriorityCount> ScheduledLoads_;
        std::vector<TStoreCallback> ScheduledStores_;

        static TObjectId GenerateId(const TObjectId& id)
        {
            if (id) {
                return id;
            }

            TStringBuilder builder;
            static const TString AvailableChars = "0123456789abcdefghijklmnopqrstuvwxyz";
            for (int index = 0; index < 16; ++index) {
                builder.AppendChar(AvailableChars[RandomNumber<size_t>(AvailableChars.size())]);
            }
            return builder.Flush();
        }

        bool HasPendingLoads()
        {
            for (const auto& loads : ScheduledLoads_) {
                if (!loads.empty()) {
                    return true;
                }
            }
            return false;
        }

        bool HasPendingStores()
        {
            return !ScheduledStores_.empty();
        }

        void ValidateCreatedObjects()
        {
            YT_LOG_DEBUG("Started validating created object");

            std::vector<std::unique_ptr<TObjectExistenceChecker>> checkers;
            std::vector<std::pair<TObject*, TObject*>> objectParentPairs;
            for (const auto& item : CreatedObjects_) {
                const auto& key = item.first;
                auto* object = item.second;

                if (object->GetState() != EObjectState::Created) {
                    continue;
                }

                if (RemovedObjects_[key.first].find(key.second) != RemovedObjects_[key.first].end()) {
                    continue;
                }

                auto checker = std::make_unique<TObjectExistenceChecker>(object);
                checker->ScheduleCheck();
                checkers.push_back(std::move(checker));

                auto* typeHandler = object->GetTypeHandler();
                auto parentType = typeHandler->GetParentType();
                if (parentType != EObjectType::Null) {
                    const auto& parentId = object->GetParentId();
                    auto* parent = Owner_->Session_.GetObject(parentType, parentId);
                    objectParentPairs.emplace_back(object, parent);
                }

                typeHandler->AfterObjectCreated(Owner_->Owner_, object);
            }

            FlushLoads();

            for (const auto& checker : checkers) {
                if (checker->Check()) {
                    auto* object = checker->GetObject();
                    THROW_ERROR_EXCEPTION(
                        NClient::NApi::EErrorCode::DuplicateObjectId,
                        "%v %v of already exists",
                        GetCapitalizedHumanReadableTypeName(object->GetType()),
                        GetObjectDisplayName(object));
                }
            }

            for (const auto& pair : objectParentPairs) {
                if (!pair.second->DoesExist()) {
                    THROW_ERROR_EXCEPTION(
                        NClient::NApi::EErrorCode::NoSuchObject,
                        "Parent %v %v of %v %v does not exist",
                        GetHumanReadableTypeName(pair.second->GetType()),
                        GetObjectDisplayName(pair.second),
                        GetHumanReadableTypeName(pair.first->GetType()),
                        GetObjectDisplayName(pair.first));
                }
            }

            YT_LOG_DEBUG("Finished validating created objects");
        }

        void FlushObjectsCreation()
        {
            YT_LOG_DEBUG("Started preparing objects creation");
            TStoreContext context(Owner_);

            const auto& watchManager = Owner_->Bootstrap_->GetWatchManager();

            for (const auto& item : CreatedObjects_) {
                const auto* object = item.second;

                if (object->GetState() != EObjectState::Created) {
                    continue;
                }

                auto* typeHandler = object->GetTypeHandler();

                // Delete previous incarnation, of any.
                context.DeleteRow(
                    typeHandler->GetTable(),
                    CaptureCompositeObjectKey(object, context.GetRowBuffer()));

                if (watchManager->Enabled()) {
                    context.WriteRow(
                        watchManager->GetWatchLogTable(object->GetType()),
                        {},
                        MakeArray(&WatchLogSchema.Fields.ObjectId, &WatchLogSchema.Fields.EventType),
                        ToUnversionedValues(context.GetRowBuffer(), object->GetId(), EEventType::ObjectCreated));
                }

                if (typeHandler->GetParentType() != EObjectType::Null) {
                    auto parentId = object->GetParentId();
                    YT_VERIFY(parentId);

                    context.WriteRow(
                        &ParentsTable,
                        ToUnversionedValues(
                            context.GetRowBuffer(),
                            object->GetId(),
                            object->GetType()),
                        MakeArray(&ParentsTable.Fields.ParentId),
                        ToUnversionedValues(
                            context.GetRowBuffer(),
                            parentId));
                }
            }

            context.FillTransaction(Owner_->UnderlyingTransaction_);
            YT_LOG_DEBUG("Finished preparing objects creation");
        }

        void FlushWatchLogObjectsUpdates()
        {
            YT_LOG_DEBUG("Started watch log objects updation");
            TStoreContext context(Owner_);

            const auto& watchManager = Owner_->Bootstrap_->GetWatchManager();

            if (watchManager->Enabled()) {
                for (const auto& [key, object] : InstantiatedObjects_) {
                    if (!object->IsStoreScheduled() || object->GetState() != EObjectState::Instantiated) {
                        continue;
                    }

                    context.WriteRow(
                        watchManager->GetWatchLogTable(object->GetType()),
                        {},
                        MakeArray(&WatchLogSchema.Fields.ObjectId, &WatchLogSchema.Fields.EventType),
                        ToUnversionedValues(context.GetRowBuffer(), object->GetId(), EEventType::ObjectUpdated));
                }
            }

            context.FillTransaction(Owner_->UnderlyingTransaction_);
            YT_LOG_DEBUG("Finished watch log objects updation");
        }

        void FlushObjectsDeletion()
        {
            auto now = TInstant::Now();

            YT_LOG_DEBUG("Started preparing objects deletion");
            TStoreContext context(Owner_);

            const auto& objectManager = Owner_->Bootstrap_->GetObjectManager();
            const auto& watchManager = Owner_->Bootstrap_->GetWatchManager();

            for (auto type : TEnumTraits<EObjectType>::GetDomainValues()) {
                auto* typeHandler = objectManager->FindTypeHandler(type);
                if (!typeHandler) {
                    continue;
                }

                auto parentType = typeHandler->GetParentType();
                const auto* table = typeHandler->GetTable();

                const auto& objects = RemovedObjects_[type];
                for (const auto& item : objects) {
                    const auto* object = item.second;

                    if (watchManager->Enabled()) {
                        context.WriteRow(
                            watchManager->GetWatchLogTable(object->GetType()),
                            {},
                            MakeArray(&WatchLogSchema.Fields.ObjectId, &WatchLogSchema.Fields.EventType),
                            ToUnversionedValues(context.GetRowBuffer(), object->GetId(), EEventType::ObjectRemoved));
                    }

                    context.WriteRow(
                        table,
                        CaptureCompositeObjectKey(object, context.GetRowBuffer()),
                        MakeArray(&ObjectsTable.Fields.Meta_RemovalTime),
                        ToUnversionedValues(
                            context.GetRowBuffer(),
                            now));
                    context.WriteRow(
                        &TombstonesTable,
                        ToUnversionedValues(
                            context.GetRowBuffer(),
                            object->GetId(),
                            object->GetType()),
                        MakeArray(&TombstonesTable.Fields.RemovalTime),
                        ToUnversionedValues(
                            context.GetRowBuffer(),
                            now));
                    if (parentType != EObjectType::Null) {
                        context.DeleteRow(
                            &ParentsTable,
                            ToUnversionedValues(
                                context.GetRowBuffer(),
                                object->GetId(),
                                type));
                    }
                }
            }

            context.FillTransaction(Owner_->UnderlyingTransaction_);
            YT_LOG_DEBUG("Finished preparing objects deletion");
        }

        void WriteHistoryEvent(
            TStoreContext& storeContext,
            TObject* object,
            EEventType eventType,
            TInstant time)
        {
            TYsonString historyEnabledAttributes;
            if (eventType == EEventType::ObjectRemoved) {
                // TODO (gritukan@): simpler?
                historyEnabledAttributes = TYsonString("{}");
            } else {
                historyEnabledAttributes = object->GetHistoryEnabledAttributes();
            }
            storeContext.WriteRow(
                &HistoryEventsTable,
                ToUnversionedValues(
                    storeContext.GetRowBuffer(),
                    object->GetType(),
                    object->GetId(),
                    object->MetaEtc().Load().uuid(),
                    time,
                    ToString(Owner_->GetId())),
                MakeArray(
                    &HistoryEventsTable.Fields.EventType,
                    &HistoryEventsTable.Fields.User,
                    &HistoryEventsTable.Fields.Value,
                    &HistoryEventsTable.Fields.HistoryEnabledAttributes),
                ToUnversionedValues(
                    storeContext.GetRowBuffer(),
                    eventType,
                    Owner_->Bootstrap_->GetAccessControlManager()->GetAuthenticatedUser(),
                    historyEnabledAttributes,
                    object->GetTypeHandler()->GetHistoryEnabledAttributePaths())
            );
        }

        void FlushHistoryEvents()
        {
            YT_LOG_DEBUG("Started writing history events");

            const auto time = TInstant::Now();

            TStoreContext storeContext(Owner_);
            const auto& objectManager = Owner_->Bootstrap_->GetObjectManager();
            for (auto type : TEnumTraits<EObjectType>::GetDomainValues()) {
                auto* typeHandler = objectManager->FindTypeHandler(type);
                if (!typeHandler || !typeHandler->HasHistoryEnabledAttributes()) {
                    continue;
                }

                for (const auto& [objectType, object] : RemovedObjects_[type]) {
                    WriteHistoryEvent(storeContext, object, EEventType::ObjectRemoved, time);
                }
            }

            for (const auto& [objectType, object] : InstantiatedObjects_) {
                if (!object->IsStoreScheduled() || object->GetState() != EObjectState::Instantiated) {
                    continue;
                }

                auto* typeHandler = object->GetTypeHandler();
                if (typeHandler->HasStoreScheduledHistoryEnabledAttributes(object.get())) {
                    WriteHistoryEvent(storeContext, object.get(), EEventType::ObjectUpdated, time);
                }
            }

            for (const auto& [objectType, object] : CreatedObjects_) {
                auto* typeHandler = object->GetTypeHandler();
                if (typeHandler->HasHistoryEnabledAttributes()) {
                    WriteHistoryEvent(storeContext, object, EEventType::ObjectCreated, time);
                }
            }

            storeContext.FillTransaction(Owner_->UnderlyingTransaction_);
            YT_LOG_DEBUG("Finished writing history events");
        }

        void FlushLoadsOnce(std::vector<TError>* errors)
        {
            for (int priority = 0; priority < LoadPriorityCount; ++priority) {
                auto& scheduledLoads = ScheduledLoads_[priority];
                if (scheduledLoads.empty()) {
                    continue;
                }

                YT_LOG_DEBUG("Started preparing reads (Priority: %v, Count: %v)",
                    priority,
                    scheduledLoads.size());

                TLoadContext context(Owner_);

                std::decay<decltype(scheduledLoads)>::type swappedLoads;
                std::swap(scheduledLoads, swappedLoads);
                for (const auto& callback: swappedLoads) {
                    try {
                        callback(&context);
                    } catch (const std::exception& ex) {
                        errors->push_back(ex);
                    }
                }

                YT_LOG_DEBUG("Finished preparing reads");

                context.RunReads();
            }
        }

        void FlushStoresOnce(std::vector<TError>* errors)
        {
            if (ScheduledStores_.empty()) {
                return;
            }

            YT_LOG_DEBUG("Started preparing writes (Count: %v)",
                ScheduledStores_.size());

            TStoreContext context(Owner_);

            decltype(ScheduledStores_) swappedStores;
            std::swap(ScheduledStores_, swappedStores);
            for (const auto& callback : swappedStores) {
                try {
                    callback(&context);
                } catch (const std::exception& ex) {
                    errors->push_back(ex);
                }
            }

            context.FillTransaction(Owner_->UnderlyingTransaction_);

            YT_LOG_DEBUG("Finished preparing writes");
        }
    };


    TTransaction* const Owner_;
    NMaster::TBootstrap* const Bootstrap_;
    const TTransactionManagerConfigPtr Config_;
    const TTransactionId Id_;
    const TTimestamp StartTimestamp_;
    const IClientPtr Client_;
    const ITransactionPtr UnderlyingTransaction_;

    const NYT::NLogging::TLogger Logger;

    const TRetentionConfigPtr SingleVersionRetentionConfig_ = New<TRetentionConfig>();

    const NYT::NConcurrency::TAsyncSemaphorePtr Semaphore_ = New<TAsyncSemaphore>(1);

    ETransactionState State_ = ETransactionState::Active;

    TSession Session_;

    std::vector<std::function<void()>> Validators_;

    THashSet<TNode*> AgentsAwaitingNotifcation_;
    THashSet<TPod*> PodsAwaitingResourceAllocation_;
    THashSet<TNode*> NodesAwaitingResourceValidation_;
    THashSet<TPod*> PodsAwaitingSpecUpdate_;
    THashSet<TPod*> PodsAwaitingAccountingValidation_;


    static TEnumIndexedVector<EObjectType, TObjectId> BuildTypeToSchemaIdMap()
    {
        TEnumIndexedVector<EObjectType, TObjectId> result;
        for (auto type : TEnumTraits<EObjectType>::GetDomainValues()) {
            result[type] = FormatEnum(type);
        }
        return result;
    }


    template <class T>
    T* GetTypedObject(const TObjectId& id)
    {
        auto* object = GetObject(T::Type, id);
        return object ? object->template As<T>() : nullptr;
    }

    template <class T>
    T* CreateTypedObject(const TObjectId& id, const TObjectId& parentId)
    {
        auto* object = Session_.CreateObject(T::Type, id, parentId)->template As<T>();
        object->SetState(EObjectState::Created);
        return object;
    }


    template <class F>
    auto AbortOnException(F func) -> decltype(func())
    {
        try {
            return func();
        } catch (const std::exception& ex) {
            Abort();
            THROW_ERROR_EXCEPTION("Error executing transactional request; transaction aborted")
                << ex;
        }
    }

    static TNameTablePtr BuildNameTable(const TDBTable* table)
    {
        auto nameTable = New<TNameTable>();
        for (int index = 0; index < static_cast<int>(table->Key.size()); ++index) {
            YT_VERIFY(nameTable->RegisterName(table->Key[index]->Name) == index);
        }
        return nameTable;
    }

    void EnsureReadWrite()
    {
        YT_ASSERT(UnderlyingTransaction_);
    }


    TObject* DoCreateObject(
        EObjectType type,
        const IMapNodePtr& attributes,
        IUpdateContext* context)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto* typeHandler = objectManager->GetTypeHandlerOrThrow(type);

        class TAttributeMatcher
        {
        public:
            TAttributeMatcher(
                TAttributeSchema* rootSchema,
                TAttributeSchema* idSchema,
                TAttributeSchema* parentIdSchema)
                : RootSchema_(rootSchema)
                , IdSchema_(idSchema)
                , ParentIdSchema_(parentIdSchema)
            { }

            void Run(const IMapNodePtr& map)
            {
                DoBuildUnmatchedMandatory(RootSchema_);
                DoMatch(map, RootSchema_);
            }

            const std::vector<TAttributeUpdateMatch>& Matches() const
            {
                return Matches_;
            }

            const THashSet<TAttributeSchema*>& PendingInitializerAttributes() const
            {
                return PendingInitializerAttributes_;
            }

            const TObjectId& GetId() const
            {
                return Id_;
            }

            const TObjectId& GetParentId() const
            {
                return ParentId_;
            }

            const THashSet<TAttributeSchema*>& UnmatchedMandatoryAttributes() const
            {
                return UnmatchedMandatoryAttributes_;
            }

        private:
            TAttributeSchema* const RootSchema_;
            TAttributeSchema* const IdSchema_;
            TAttributeSchema* const ParentIdSchema_;

            std::vector<TAttributeUpdateMatch> Matches_;
            THashSet<TAttributeSchema*> UnmatchedMandatoryAttributes_;
            THashSet<TAttributeSchema*> PendingInitializerAttributes_;
            TObjectId Id_;
            TObjectId ParentId_;


            void DoBuildUnmatchedMandatory(TAttributeSchema* schema)
            {
                if (schema->IsComposite()) {
                    for (const auto& pair : schema->KeyToChild()) {
                        DoBuildUnmatchedMandatory(pair.second);
                    }
                } else {
                    if (schema->GetMandatory()) {
                        YT_VERIFY(UnmatchedMandatoryAttributes_.insert(schema).second);
                    }
                    if (schema->HasInitializer()) {
                        YT_VERIFY(PendingInitializerAttributes_.insert(schema).second);
                    }
                }
            }

            void DoMatch(const INodePtr& node, TAttributeSchema* schema)
            {
                if (schema->IsComposite()) {
                    if (node->GetType() != ENodeType::Map) {
                        THROW_ERROR_EXCEPTION("Attribute %v is composite and cannot be parsed from %Qlv node",
                            schema->GetPath(),
                            node->GetType());
                    }
                    auto mapNode = node->AsMap();
                    auto* etcChild = schema->FindEtcChild();
                    for (const auto& [key, value] : mapNode->GetChildren()) {
                        auto* child = schema->FindChild(key);
                        if (child) {
                            DoMatch(value, child);
                        } else if (etcChild) {
                            AddMatch({
                                etcChild,
                                TSetUpdateRequest{"/" + ToYPathLiteral(key), value}
                            });
                        } else {
                            THROW_ERROR_EXCEPTION("Attribute %v has no child with key %Qv",
                                schema->GetPath(),
                                key);
                        }
                    }
                } else {
                    if (schema == IdSchema_) {
                        if (node->GetType() != ENodeType::String) {
                            THROW_ERROR_EXCEPTION("Attribute %v must be %Qlv",
                                schema->GetPath(),
                                ENodeType::String);
                        }
                        Id_ = node->GetValue<TString>();
                    } else if (schema == ParentIdSchema_) {
                        if (node->GetType() != ENodeType::String) {
                            THROW_ERROR_EXCEPTION("Attribute %v must be %Qlv",
                                schema->GetPath(),
                                ENodeType::String);
                        }
                        ParentId_ = node->GetValue<TString>();
                    } else {
                        if (!schema->HasValueSetter()) {
                            THROW_ERROR_EXCEPTION("Attribute %v cannot be set",
                                schema->GetPath());
                        }
                        AddMatch({
                            schema,
                            TSetUpdateRequest{TYPath(), node}
                        });
                    }
                    if (schema->GetMandatory()) {
                        YT_VERIFY(UnmatchedMandatoryAttributes_.erase(schema) == 1);
                    }
                }
            }

            void AddMatch(TAttributeUpdateMatch match)
            {
                PendingInitializerAttributes_.erase(match.Schema);
                Matches_.emplace_back(std::move(match));
            }
        } matcher(
            typeHandler->GetRootAttributeSchema(),
            typeHandler->GetIdAttributeSchema(),
            typeHandler->GetParentIdAttributeSchema());
        matcher.Run(attributes);

        const auto& unmatchedMandatory = matcher.UnmatchedMandatoryAttributes();
        if (!unmatchedMandatory.empty()) {
            THROW_ERROR_EXCEPTION("Missing mandatory attribute %v",
                (*unmatchedMandatory.begin())->GetPath());
        }

        auto* object = Session_.CreateObject(type, matcher.GetId(), matcher.GetParentId());

        for (const auto& match : matcher.Matches()) {
            PreloadAttribute(Owner_, object, match);
        }

        THashSet<TAttributeSchema*> updatedAttributes;
        for (const auto& match : matcher.Matches()) {
            auto* schema = match.Schema;
            const auto& request = std::get<TSetUpdateRequest>(match.Request);
            context->AddSetter([=] {
                try {
                    schema->RunValueSetter(
                        Owner_,
                        object,
                        request.Path,
                        request.Value,
                        request.Recursive);
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Error setting attribute %v",
                        schema->GetPath())
                        << ex;
                }
            });

            auto* current = schema;
            while (current && updatedAttributes.insert(current).second) {
                current = current->GetParent();
            }
        }

        for (auto* schema : matcher.PendingInitializerAttributes()) {
            schema->RunInitializer(Owner_, object);
        }

        OnAttributesUpdated(object, updatedAttributes, context);

        context->AddFinalizer([=] {
            object->SetState(EObjectState::Created);
        });

        return object;
    }

    void DoUpdateObject(
        TObject* object,
        const std::vector<TUpdateRequest>& requests,
        const std::vector<TAttributeTimestampPrerequisite>& prerequisites,
        IUpdateContext* context)
    {
        object->ValidateExists();

        std::vector<TAttributeUpdateMatch> matches;
        matches.reserve(requests.size());
        for (const auto& request : requests) {
            matches.push_back(MatchAttributeUpdate(object, request));
        }

        for (const auto& match : matches) {
            PreloadAttribute(Owner_, object, match);
        }

        for (const auto& prerequisite : prerequisites) {
            auto resolveResult = ResolveAttribute(object->GetTypeHandler(), prerequisite.Path);
            PregetAttributeTimestamp(resolveResult, Owner_, object);
            context->AddSetter([=] {
                auto actualTimestamp = GetAttributeTimestamp(resolveResult, Owner_, object);
                if (actualTimestamp > prerequisite.Timestamp) {
                    THROW_ERROR_EXCEPTION(NClient::NApi::EErrorCode::PrerequisiteCheckFailure,
                        "Prerequisite timestamp check failed for attribute %v of %v %Qv: expected <=%v, actual %v",
                        resolveResult.Attribute->GetPath(),
                        GetHumanReadableTypeName(object->GetType()),
                        object->GetId(),
                        prerequisite.Timestamp,
                        actualTimestamp);
                }
            });
        }

        THashSet<TAttributeSchema*> updatedAttributes;
        for (const auto& match : matches) {
            context->AddSetter([=] {
                ApplyAttributeUpdate(Owner_, object, match);
            });

            auto* current = match.Schema;
            while (current && updatedAttributes.insert(current).second) {
                current = current->GetParent();
            }
        }

        OnAttributesUpdated(object, updatedAttributes, context);
    }

    void DoRemoveObject(TObject* object, IUpdateContext* /*context*/)
    {
        object->ValidateExists();
        Session_.RemoveObject(object);
    }

    void OnAttributesUpdated(
        TObject* object,
        const THashSet<TAttributeSchema*>& attributes,
        IUpdateContext* context)
    {
        for (auto* schema : attributes) {
            schema->RunUpdatePrehandlers(Owner_, object);

            context->AddFinalizer([=] {
                schema->RunUpdateHandlers(Owner_, object);
            });

            Validators_.push_back([=] {
                schema->RunValidators(Owner_, object);
            });
        }
    }

    struct TAttributeUpdateMatch
    {
        TAttributeSchema* Schema;
        TUpdateRequest Request;
    };

    static TYPath GetRequestPath(const TUpdateRequest& request)
    {
        return Visit(request,
            [&] (const TSetUpdateRequest& typedRequest) {
                return typedRequest.Path;
            },
            [&] (const TRemoveUpdateRequest& typedRequest) {
                return typedRequest.Path;
            });
    }


    static TUpdateRequest PatchRequestPath(
        const TUpdateRequest& request,
        const TYPath& path)
    {
        return Visit(request,
            [&] (const TSetUpdateRequest& typedRequest) -> TUpdateRequest {
                return TSetUpdateRequest{path, typedRequest.Value, typedRequest.Recursive};
            },
            [&] (const TRemoveUpdateRequest&) -> TUpdateRequest {
                return TRemoveUpdateRequest{path};
            });
    }

    TAttributeUpdateMatch MatchAttributeUpdate(
        TObject* object,
        const TUpdateRequest& request)
    {
        TResolvePermissions permissions;
        auto resolveResult = ResolveAttribute(
            object->GetTypeHandler(),
            GetRequestPath(request),
            &permissions);

        if (!resolveResult.Attribute->GetUpdatable()) {
            THROW_ERROR_EXCEPTION("Attribute %v does not support updates",
                resolveResult.Attribute->GetPath());
        }

        if (!resolveResult.SuffixPath.empty() && !permissions.ReadPermissions.empty()) {
            const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
            for (auto permission : permissions.ReadPermissions) {
                accessControlManager->ValidatePermission(object, permission);
            }
        }

        return {resolveResult.Attribute, PatchRequestPath(request, resolveResult.SuffixPath)};
    }

    void PreloadAttribute(
        TTransaction* transaction,
        TObject* object,
        const TAttributeUpdateMatch& match)
    {
        if (GetRequestPath(match.Request).empty()) {
            return;
        }

        if (!match.Schema->HasPreupdater()) {
            return;
        }

        YT_LOG_DEBUG("Scheduling attribute load (ObjectId: %v, Attribute: %v)",
            object->GetId(),
            match.Schema->GetPath());

        match.Schema->RunPreupdater(transaction, object, match.Request);
    }

    void ApplyAttributeUpdate(
        TTransaction* transaction,
        TObject* object,
        const TAttributeUpdateMatch& match)
    {
        const auto& request = match.Request;

        try {
            Visit(request,
                [&] (const TSetUpdateRequest& typedRequest) {
                    ApplyAttributeSetUpdate(
                        transaction,
                        object,
                        match.Schema,
                        typedRequest);
                },
                [&] (const TRemoveUpdateRequest& typedRequest) {
                    ApplyAttributeRemoveUpdate(
                        transaction,
                        object,
                        match.Schema,
                        typedRequest);
                });
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error updating attribute %v of %v %v",
                match.Schema->GetPath(),
                GetHumanReadableTypeName(object->GetType()),
                GetObjectDisplayName(object))
                << ex;
        }
    }

    void ApplyCompositeAttributeSetUpdate(
        TTransaction* transaction,
        TObject* object,
        TAttributeSchema* schema,
        const TSetUpdateRequest& request)
    {
        YT_VERIFY(request.Path.Empty());

        if (request.Value->GetType() != ENodeType::Map) {
            THROW_ERROR_EXCEPTION("Attribute %v cannot be updated from %Qlv values",
                schema->GetPath(),
                request.Value->GetType());
        }

        auto mapValue = request.Value->AsMap();
        auto* etcChild = schema->FindEtcChild();
        IMapNodePtr etcMapValue;
        for (const auto& [key, childValue] : mapValue->GetChildren()) {
            auto* child = schema->FindChild(key);
            if (child) {
                ApplyAttributeSetUpdate(
                    transaction,
                    object,
                    child,
                    TSetUpdateRequest{TYPath(), childValue});
            } else if (etcChild) {
                if (!etcMapValue) {
                    etcMapValue = GetEphemeralNodeFactory()->CreateMap();
                }
                etcMapValue->AddChild(key, CloneNode(childValue));
            } else {
                THROW_ERROR_EXCEPTION("Attribute %v has no child with key %Qv",
                    schema->GetPath(),
                    key);
            }
        }

        if (etcMapValue) {
            ApplyAttributeSetUpdate(
                transaction,
                object,
                etcChild,
                TSetUpdateRequest{TYPath(), etcMapValue});
        }
    }

    void ApplyAttributeSetUpdate(
        TTransaction* transaction,
        TObject* object,
        TAttributeSchema* schema,
        const TSetUpdateRequest& request)
    {
        if (schema->IsComposite()) {
            ApplyCompositeAttributeSetUpdate(transaction, object, schema, request);
            return;
        }

        if (!schema->HasValueSetter()) {
            THROW_ERROR_EXCEPTION("Attribute %v does not support set updates",
                schema->GetPath());
        }

        YT_LOG_DEBUG("Applying set update (ObjectId: %v, Attribute: %v, Path: %v, Value: %v)",
            object->GetId(),
            schema->GetPath(),
            request.Path,
            ConvertToYsonString(request.Value, NYson::EYsonFormat::Text));

        schema->RunValueSetter(transaction, object, request.Path, request.Value, request.Recursive);
    }

    void ApplyAttributeRemoveUpdate(
        TTransaction* transaction,
        TObject* object,
        TAttributeSchema* schema,
        const TRemoveUpdateRequest& request)
    {
        if (!schema->HasRemover()) {
            THROW_ERROR_EXCEPTION("Attribute %v does not support remove updates",
                schema->GetPath());
        }

        YT_LOG_DEBUG("Applying remove update (ObjectId: %v, Attribute: %v, Path: %v)",
            object->GetId(),
            schema->GetPath(),
            request.Path);

        schema->RunRemover(transaction, object, request.Path);
    }


    static TExpressionPtr BuildObjectFilterByRemovalTime()
    {
        return New<TFunctionExpression>(
            TSourceLocation(),
            "is_null",
            TExpressionList{
                New<TReferenceExpression>(
                    TSourceLocation(),
                    TReference(ObjectsTable.Fields.Meta_RemovalTime.Name, PrimaryTableAlias)
                )
            });
    }

    static void EnsureNonEmptySelectExpressions(TQuery* query)
    {
        if (query->SelectExprs->empty()) {
            static const auto DummyExpr = New<TLiteralExpression>(
                TSourceLocation(),
                TLiteralValue(false));
            query->SelectExprs->push_back(DummyExpr);
        }
    }

    std::unique_ptr<TQuery> MakeQuery(IObjectTypeHandler* typeHandler)
    {
        auto query = std::make_unique<TQuery>();
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        const auto* table = typeHandler->GetTable();
        query->Table = TTableDescriptor(ytConnector->GetTablePath(table),  PrimaryTableAlias);
        query->SelectExprs.emplace();
        return query;
    }

    static std::vector<TResolveResult> ResolveAttributes(
        IQueryContext* queryContext,
        const TAttributeSelector& selector,
        TResolvePermissions* permissions = nullptr)
    {
        auto* typeHandler = queryContext->GetTypeHandler();
        std::vector<TResolveResult> results;
        results.reserve(selector.Paths.size());
        for (const auto& path : selector.Paths) {
            results.push_back(ResolveAttribute(typeHandler, path, permissions));
        }
        return results;
    }

    static std::vector<TAttributeFetcher> BuildAttributeFetchers(
        TTransaction* transaction,
        TQuery* query,
        TAttributeFetcherContext* fetcherContext,
        IQueryContext* queryContext,
        const std::vector<TResolveResult>& resolveResults)
    {
        std::vector<TAttributeFetcher> fetchers;
        for (const auto& resolveResult : resolveResults) {
            fetchers.emplace_back(resolveResult, transaction, fetcherContext, queryContext);
        }

        query->SelectExprs = fetcherContext->GetSelectExpressions();
        EnsureNonEmptySelectExpressions(query);

        return fetchers;
    }

    static void PregetAttributeTimestamp(
        const TResolveResult& resolveResult,
        TTransaction* transaction,
        TObject* object)
    {
        if (resolveResult.Attribute->IsComposite()) {
            YT_VERIFY(resolveResult.SuffixPath.empty());
            auto considerChild = [&] (auto* child) {
                PregetAttributeTimestamp(TResolveResult{child, TYPath()}, transaction, object);
            };
            for (const auto& [key, child] : resolveResult.Attribute->KeyToChild()) {
                considerChild(child);
            }
            if (auto* etcChild = resolveResult.Attribute->FindEtcChild()) {
                considerChild(etcChild);
            }
        } else if (resolveResult.Attribute->HasTimestampPregetter()) {
            resolveResult.Attribute->RunTimestampPregetter(transaction, object, resolveResult.SuffixPath);
        }
    }

    static TTimestamp GetAttributeTimestamp(
        const TResolveResult& resolveResult,
        TTransaction* transaction,
        TObject* object)
    {
        if (resolveResult.Attribute->IsComposite()) {
            YT_VERIFY(resolveResult.SuffixPath.empty());
            auto result = NullTimestamp;
            auto considerChild = [&] (auto* child) {
                result = std::max(result, GetAttributeTimestamp(TResolveResult{child, TYPath()}, transaction, object));
            };
            for (const auto& [key, child] : resolveResult.Attribute->KeyToChild()) {
                considerChild(child);
            }
            if (auto* etcChild = resolveResult.Attribute->FindEtcChild()) {
                considerChild(etcChild);
            }
            return result;
        } else if (resolveResult.Attribute->HasTimestampGetter()) {
            return resolveResult.Attribute->RunTimestampGetter(transaction, object, resolveResult.SuffixPath);
        } else {
            return NullTimestamp;
        }
    }

    void PrefetchAttributeValues(
        TRange<TUnversionedRow> rows,
        std::vector<TAttributeFetcher>& fetchers)
    {
        for (auto row : rows) {
            for (auto& fetcher : fetchers) {
                fetcher.Prefetch(row);
            }
        }
    }

    void PrefetchAttributeTimestamps(
        const std::vector<TObject*>& objects,
        const std::vector<TResolveResult>& resolveResults)
    {
        for (auto* object : objects) {
            for (const auto& resolveResult : resolveResults) {
                PregetAttributeTimestamp(
                    resolveResult,
                    Owner_,
                    object);
            }
        }
    }

    void ValidateObjectPermissions(
        const std::vector<TObject*>& objects,
        const TResolvePermissions& permissions)
    {
        for (auto* object : objects) {
            for (auto permission : permissions.ReadPermissions) {
                const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
                accessControlManager->ValidatePermission(object, permission);
            }
        }
    }

    void FillAttributeValues(
        TAttributeValueList* valueList,
        TUnversionedRow row,
        std::vector<TAttributeFetcher>& fetchers)
    {
        for (auto& fetcher : fetchers) {
            valueList->Values.push_back(fetcher.Fetch(row));
        }
    }

    void FillAttributeTimestamps(
        TAttributeValueList* valueList,
        TObject* object,
        const std::vector<TResolveResult>& resolveResults)
    {
        for (const auto& resolveResult : resolveResults) {
            valueList->Timestamps.push_back(GetAttributeTimestamp(
                resolveResult,
                Owner_,
                object));
        }
    }


    IUnversionedRowsetPtr RunSelect(const TString& queryString)
    {
        IUnversionedRowsetPtr rowset;
        Session_.ScheduleLoad(
            [&] (ILoadContext* context) {
                context->ScheduleSelect(
                    queryString,
                    [&] (const IUnversionedRowsetPtr& selectedRowset) {
                        rowset = selectedRowset;
                    });
            });
        Session_.FlushLoads();
        return rowset;
    }

    TString GetObjectHistorySelectionQuery(
        EObjectType objectType,
        const TObjectId& objectId,
        const TSelectObjectHistoryOptions& options)
    {
        TStringBuilder queryBuilder;
        queryBuilder.AppendFormat("[%v], [%v], [%v], [%v], [%v]",
            HistoryEventsTable.Fields.Time.Name,
            HistoryEventsTable.Fields.EventType.Name,
            HistoryEventsTable.Fields.User.Name,
            HistoryEventsTable.Fields.Value.Name,
            HistoryEventsTable.Fields.HistoryEnabledAttributes.Name);

        queryBuilder.AppendFormat(" from [%v]", Bootstrap_->GetYTConnector()->GetTablePath(&HistoryEventsTable));
        queryBuilder.AppendFormat(" where [%v] = %v and [%v] = %Qv",
            HistoryEventsTable.Fields.ObjectType.Name,
            static_cast<i64>(objectType),
            HistoryEventsTable.Fields.ObjectId.Name,
            objectId);

        if (options.Uuid) {
            queryBuilder.AppendFormat(" and [%v] = %Qv",
                HistoryEventsTable.Fields.Uuid.Name,
                *options.Uuid);
        }
        if (options.TimeInterval.Begin) {
            queryBuilder.AppendFormat(" and [%v] >= %v",
                HistoryEventsTable.Fields.Time.Name,
                options.TimeInterval.Begin->MicroSeconds());
        }
        if (options.TimeInterval.End) {
            queryBuilder.AppendFormat(" and [%v] < %v",
                HistoryEventsTable.Fields.Time.Name,
                options.TimeInterval.End->MicroSeconds());
        }

        queryBuilder.AppendFormat(" order by [%v]",
            HistoryEventsTable.Fields.Time.Name);

        if (options.Offset) {
            queryBuilder.AppendFormat(" offset %v",
                *options.Offset);
        }

        // YT requires limit for queries with ORDER BY
        // so options.Limit equals to default value if user didn't set it.
        YT_VERIFY(options.Limit);
        ui64 eventsLimit = *options.Limit;

        queryBuilder.AppendFormat(" limit %v",
            eventsLimit);

        return queryBuilder.Flush();
    }
};

////////////////////////////////////////////////////////////////////////////////

TTransaction::TTransaction(
    NMaster::TBootstrap* bootstrap,
    TTransactionManagerConfigPtr config,
    const TTransactionId& id,
    TTimestamp startTimestamp,
    IClientPtr client,
    ITransactionPtr underlyingTransaction)
    : Impl_(New<TImpl>(
        this,
        bootstrap,
        std::move(config),
        id,
        startTimestamp,
        std::move(client),
        std::move(underlyingTransaction)))
{ }

ETransactionState TTransaction::GetState() const
{
    return Impl_->GetState();
}

const TTransactionId& TTransaction::GetId() const
{
    return Impl_->GetId();
}

TTimestamp TTransaction::GetStartTimestamp() const
{
    return Impl_->GetStartTimestamp();
}

ISession* TTransaction::GetSession()
{
    return Impl_->GetSession();
}

std::unique_ptr<IUpdateContext> TTransaction::CreateUpdateContext()
{
    return Impl_->CreateUpdateContext();
}

TObject* TTransaction::CreateObject(EObjectType type, const IMapNodePtr& attributes)
{
    return Impl_->CreateObject(type, attributes);
}

TObject* TTransaction::CreateObject(EObjectType type, const IMapNodePtr& attributes, IUpdateContext* context)
{
    return Impl_->CreateObject(type, attributes, context);
}

void TTransaction::RemoveObject(TObject* object)
{
    return Impl_->RemoveObject(object);
}

void TTransaction::RemoveObject(TObject* object, IUpdateContext* context)
{
    return Impl_->RemoveObject(object, context);
}

void TTransaction::UpdateObject(
    TObject* object,
    const std::vector<TUpdateRequest>& requests,
    const std::vector<TAttributeTimestampPrerequisite>& prerequisites)
{
    return Impl_->UpdateObject(
        object,
        requests,
        prerequisites);
}

void TTransaction::UpdateObject(
    TObject* object,
    const std::vector<TUpdateRequest>& requests,
    const std::vector<TAttributeTimestampPrerequisite>& prerequisites,
    IUpdateContext* context)
{
    return Impl_->UpdateObject(
        object,
        requests,
        prerequisites,
        context);
}

TObject* TTransaction::GetObject(EObjectType type, const TObjectId& id, const TObjectId& parentId)
{
    return Impl_->GetObject(type, id, parentId);
}

TSchema* TTransaction::GetSchema(EObjectType type)
{
    return Impl_->GetSchema(type);
}

TGetQueryResult TTransaction::ExecuteGetQuery(
    EObjectType type,
    const std::vector<TObjectId>& ids,
    const TAttributeSelector& selector,
    const TGetQueryOptions& options)
{
    return Impl_->ExecuteGetQuery(
        type,
        ids,
        selector,
        options);
}

TSelectQueryResult TTransaction::ExecuteSelectQuery(
    EObjectType type,
    const std::optional<TObjectFilter>& filter,
    const TAttributeSelector& selector,
    const TSelectQueryOptions& options)
{
    return Impl_->ExecuteSelectQuery(
        type,
        filter,
        selector,
        options);
}

TSelectObjectHistoryResult TTransaction::ExecuteSelectObjectHistoryQuery(
    EObjectType objectType,
    const TObjectId& objectId,
    const TAttributeSelector& attributeSelector,
    const TSelectObjectHistoryOptions& options)
{
    return Impl_->ExecuteSelectObjectHistoryQuery(
        objectType,
        objectId,
        attributeSelector,
        options);
}

IUnversionedRowsetPtr TTransaction::SelectFields(
    EObjectType type,
    const std::vector<const TDBField*>& fields)
{
    return Impl_->SelectFields(type, fields);
}

TNode* TTransaction::GetNode(const TObjectId& id)
{
    return Impl_->GetNode(id);
}

TNode* TTransaction::CreateNode(const TObjectId& id)
{
    return Impl_->CreateNode(id);
}

TNodeSegment* TTransaction::GetNodeSegment(const TObjectId& id)
{
    return Impl_->GetNodeSegment(id);
}

TPod* TTransaction::GetPod(const TObjectId& id)
{
    return Impl_->GetPod(id);
}

TPodSet* TTransaction::GetPodSet(const TObjectId& id)
{
    return Impl_->GetPodSet(id);
}

TResource* TTransaction::GetResource(const TObjectId& id)
{
    return Impl_->GetResource(id);
}

TNetworkProject* TTransaction::GetNetworkProject(const TObjectId& id)
{
    return Impl_->GetNetworkProject(id);
}

TVirtualService* TTransaction::GetVirtualService(const TObjectId& id)
{
    return Impl_->GetVirtualService(id);
}

TDnsRecordSet* TTransaction::GetDnsRecordSet(const TObjectId& id)
{
    return Impl_->GetDnsRecordSet(id);
}

TDnsRecordSet* TTransaction::CreateDnsRecordSet(const TObjectId& id)
{
    return Impl_->CreateDnsRecordSet(id);
}

TInternetAddress* TTransaction::GetInternetAddress(const TObjectId& id)
{
    return Impl_->GetInternetAddress(id);
}

TAccount* TTransaction::GetAccount(const TObjectId& id)
{
    return Impl_->GetAccount(id);
}

TUser* TTransaction::GetUser(const TObjectId& id)
{
    return Impl_->GetUser(id);
}

TGroup* TTransaction::GetGroup(const TObjectId& id)
{
    return Impl_->GetGroup(id);
}

TPodDisruptionBudget* TTransaction::GetPodDisruptionBudget(const TObjectId& id)
{
    return Impl_->GetPodDisruptionBudget(id);
}

TFuture<TTransactionCommitResult> TTransaction::Commit()
{
    return Impl_->Commit();
}

void TTransaction::Abort()
{
    Impl_->Abort();
}

void TTransaction::ScheduleNotifyAgent(TNode* node)
{
    Impl_->ScheduleNotifyAgent(node);
}

void TTransaction::ScheduleAllocateResources(TPod* pod)
{
    Impl_->ScheduleAllocateResources(pod);
}

void TTransaction::ScheduleValidateNodeResources(TNode* node)
{
    Impl_->ScheduleValidateNodeResources(node);
}

void TTransaction::ScheduleUpdatePodSpec(TPod* pod)
{
    Impl_->ScheduleUpdatePodSpec(pod);
}

void TTransaction::ScheduleValidateAccounting(TPod* pod)
{
    Impl_->ScheduleValidateAccounting(pod);
}

TAsyncSemaphoreGuard TTransaction::AcquireLock()
{
    return Impl_->AcquireLock();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

