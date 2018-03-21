#include "transaction.h"
#include "object_manager.h"
#include "node.h"
#include "pod.h"
#include "pod_set.h"
#include "resource.h"
#include "network_project.h"
#include "config.h"
#include "db_schema.h"
#include "type_handler.h"
#include "private.h"
#include "attribute_schema.h"
#include "helpers.h"

#include <yp/server/master/bootstrap.h>
#include <yp/server/master/yt_connector.h>

#include <yp/server/nodes/node_tracker.h>

#include <yp/server/net/net_manager.h>

#include <yp/server/scheduler/resource_manager.h>

#include <yt/ytlib/api/transaction.h>
#include <yt/ytlib/api/client.h>
#include <yt/ytlib/api/rowset.h>

#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/row_buffer.h>

#include <yt/ytlib/query_client/ast.h>
#include <yt/ytlib/query_client/query_preparer.h>

#include <yt/core/ytree/public.h>

#include <yt/core/ypath/tokenizer.h>

#include <yt/core/misc/collection_helpers.h>

#include <array>

namespace NYP {
namespace NServer {
namespace NObjects {

using namespace NYT::NApi;
using namespace NYT::NYPath;
using namespace NYT::NYTree;
using namespace NYT::NTableClient;
using namespace NYT::NConcurrency;
using namespace NYT::NQueryClient::NAst;

using NYT::NQueryClient::TSourceLocation;
using NYT::NQueryClient::EBinaryOp;

using namespace NYT::NNet;

////////////////////////////////////////////////////////////////////////////////

static const TString PrimaryTableAlias("p");
static const TString AnnotationsTableAliasPrefix("c");

////////////////////////////////////////////////////////////////////////////////

void FromProto(TSetUpdateRequest* request, const NClient::NApi::NProto::TSetUpdate& protoRequest)
{
    request->Path = protoRequest.path();
    request->Value = ConvertToNode(NYson::TYsonString(protoRequest.value()));
    request->Recursive = protoRequest.recursive();
}

void FromProto(TRemoveUpdateRequest* request, const NClient::NApi::NProto::TRemoveUpdate& protoRequest)
{
    request->Path = protoRequest.path();
}

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TAttributeSelector& selector)
{
    return Format("{Paths: %v}", selector.Paths);
}

TString ToString(const TObjectFilter& filter)
{
    return Format("{Query: %v}", filter.Query);
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

    virtual TExpressionPtr GetFieldExpression(const TDbField* field) override
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
                Null);

            auto expr = New<TReferenceExpression>(TSourceLocation(), AnnotationsTable.Fields.Value.Name, foreignTableAlias);
            it = AnnotationNameToExpression_.emplace(name, std::move(expr)).first;
        }
        return it->second;
    }

private:
    NMaster::TBootstrap* const Bootstrap_;
    const EObjectType ObjectType_;
    TQuery* const Query_;

    THashMap<const TDbField*, TExpressionPtr> FieldToExpression_;
    THashMap<TString, TExpressionPtr> AnnotationNameToExpression_;
};

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
        EnsureReadWrite();
        return AbortOnException(
            [&] {
                return DoCreateObject(type, attributes, context);
            });
    }


    void UpdateObject(TObject* object, const std::vector<TUpdateRequest>& requests)
    {
        EnsureReadWrite();
        auto context = CreateUpdateContext();
        UpdateObject(object, requests, context.get());
        context->Commit();
    }

    void UpdateObject(TObject* object, const std::vector<TUpdateRequest>& requests, IUpdateContext* context)
    {
        EnsureReadWrite();
        AbortOnException(
            [&] {
                DoUpdateObject(object, requests, context);
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
        EnsureReadWrite();
        AbortOnException(
            [&] {
                DoRemoveObject(object, context);
            });
    }


    TGetQueryResult ExecuteGetQuery(
        EObjectType type,
        const TObjectId& id,
        const TAttributeSelector& selector)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto* typeHandler = objectManager->GetTypeHandler(type);

        auto* object = GetObject(type, id);
        object->ValidateExists();

        const auto* idField = typeHandler->GetIdField();
        auto matcherExpr = New<TBinaryOpExpression>(
            TSourceLocation(),
            EBinaryOp::Equal,
            TExpressionList{
                New<TReferenceExpression>(TSourceLocation(), idField->Name, PrimaryTableAlias)
            },
            TExpressionList{
                New<TLiteralExpression>(TSourceLocation(), id)
            });

        const auto* parentIdField = typeHandler->GetParentIdField();
        if (parentIdField) {
            matcherExpr = New<TBinaryOpExpression>(
                TSourceLocation(),
                EBinaryOp::And,
                TExpressionList{
                    std::move(matcherExpr)
                },
                TExpressionList{
                    New<TBinaryOpExpression>(
                        TSourceLocation(),
                        EBinaryOp::Equal,
                        TExpressionList{
                            New<TReferenceExpression>(TSourceLocation(), parentIdField->Name, PrimaryTableAlias)
                        },
                        TExpressionList{
                            New<TLiteralExpression>(TSourceLocation(), object->GetParentId())
                        })
                });
        }

        auto query = MakeQuery(typeHandler);

        query->WherePredicate = TExpressionList{matcherExpr};

        TQueryContext queryContext(Bootstrap_, type, query.get());
        TAttributeFetcherContext fetcherContext;
        auto fetchers = BuildAttributeFetchers(
            typeHandler,
            MakeStrong(Owner_),
            query.get(),
            &fetcherContext,
            &queryContext,
            selector);
        auto queryString = FormatQuery(*query);

        LOG_DEBUG("Getting object (ObjectId: %v, Query: %v)",
            id,
            queryString);

        auto rowset = RunSelect(queryString);
        auto rows = rowset->GetRows();
        YCHECK(rows.Size() <= 1);
        if (rows.Empty()) {
            return TGetQueryResult();
        }
        auto row = rows[0];

        TGetQueryResult result;
        result.Object.Emplace();
        for (auto& fetcher : fetchers) {
            fetcher.Prefetch(row);
        }
        for (auto& fetcher : fetchers) {
            result.Object->Values.push_back(fetcher.Fetch(row));
        }
        return result;
    }

    TSelectQueryResult ExecuteSelectQuery(
        EObjectType type,
        const TNullable<TObjectFilter>& filter,
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
        auto* typeHandler = objectManager->GetTypeHandler(type);

        auto query = MakeQuery(typeHandler);

        TQueryContext queryContext(Bootstrap_, type, query.get());
        TAttributeFetcherContext fetcherContext;
        auto fetchers = BuildAttributeFetchers(
            typeHandler,
            MakeStrong(Owner_),
            query.get(),
            &fetcherContext,
            &queryContext,
            selector);

        auto predicateExpr = BuildAndExpression(
            filter
            ? BuildFilterExpression(
                typeHandler,
                &queryContext,
                *filter)
            : nullptr,
            New<TFunctionExpression>(
                TSourceLocation(),
                "is_null",
                TExpressionList{
                    New<TReferenceExpression>(TSourceLocation(), TReference(ObjectsTable.Fields.Meta_RemovalTime.Name, PrimaryTableAlias))
                }));
        query->WherePredicate = {std::move(predicateExpr)};

        query->Limit = limit;

        auto queryString = FormatQuery(*query);

        LOG_DEBUG("Selecting objects (Type: %v, Query: %v)",
            type,
            queryString);

        auto rowset = RunSelect(queryString);
        auto rows = rowset->GetRows();

        auto forAllRows = [&] (auto func) {
            auto rowsToSkip = offset.Get(0);
            for (auto row : rows) {
                if (rowsToSkip > 0) {
                    --rowsToSkip;
                    continue;
                }
                func(row);
            }
        };

        LOG_DEBUG("Prefetching results");

        forAllRows([&] (auto row) {
            for (auto& fetcher : fetchers) {
                fetcher.Prefetch(row);
            }
        });

        LOG_DEBUG("Fetching results");

        TSelectQueryResult result;
        forAllRows([&] (auto row) {
            result.Objects.emplace_back();
            for (auto& fetcher : fetchers) {
                result.Objects.back().Values.push_back(fetcher.Fetch(row));
            }
        });

        return result;
    }

    TObject* GetObject(EObjectType type, const TObjectId& id, const TObjectId& parentId = {})
    {
        return Session_.GetObject(type, id, parentId);
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


    TFuture<TTransactionCommitResult> Commit()
    {
        EnsureReadWrite();
        State_ = ETransactionState::Committing;

        for (const auto& validator : Validators_) {
            validator();
        }

        const auto& resourceManager = Bootstrap_->GetResourceManager();
        const auto& netManager = Bootstrap_->GetNetManager();

        for (auto* node : NodesAwaitingResourceValidation_) {
            if (node->Exists()) {
                resourceManager->ValidateNodeResource(node);
            }
        }

        for (auto* pod : PodsAwaitingAddressesUpdate_) {
            if (pod->Exists()) {
                netManager->UpdatePodAddresses(Owner_, pod);
            }
        }

        for (auto* pod : PodsAwaitingSpecUpdate_) {
            if (pod->Exists()) {
                resourceManager->UpdatePodSpec(Owner_, pod);
            }
        }

        for (auto* pod : PodsAwaitingResourceAllocation_) {
            if (pod->Exists()) {
                resourceManager->ReallocatePodResources(Owner_, pod);
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
                Y_ASSERT(underlyingResult.CommitTimestamps.Timestamps.size() <= 1);
                auto timestamp = underlyingResult.CommitTimestamps.Timestamps.empty()
                    ? GetStartTimestamp()
                    : underlyingResult.CommitTimestamps.Timestamps[0].second;

                State_ = ETransactionState::Committed;

                LOG_DEBUG("Transaction committed (CommitTimestamp: %v)",
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
        LOG_DEBUG("Transaction aborted");
        State_ = ETransactionState::Aborted;
        UnderlyingTransaction_->Abort();
    }


    void ScheduleNotifyAgent(TNode* node)
    {
        EnsureReadWrite();
        if (AgentsAwaitingNotifcation_.insert(node).second) {
            LOG_DEBUG("Agent notification scheduled (NodeId: %v)",
                node->GetId());
        }
    }

    void ScheduleAllocateResources(TPod* pod)
    {
        EnsureReadWrite();
        if (PodsAwaitingResourceAllocation_.insert(pod).second) {
            LOG_DEBUG("Pod resource allocation scheduled (PodId: %v)",
                pod->GetId());
        }
    }

    void ScheduleValidateNodeResources(TNode* node)
    {
        EnsureReadWrite();
        if (NodesAwaitingResourceValidation_.insert(node).second) {
            LOG_DEBUG("Node resource validation scheduled (NodeId: %v)",
                node->GetId());
        }
    }

    void ScheduleUpdatePodAddresses(TPod* pod)
    {
        EnsureReadWrite();
        if (PodsAwaitingAddressesUpdate_.insert(pod).second) {
            const auto& netManager = Bootstrap_->GetNetManager();
            netManager->PrepareUpdatePodAddresses(pod);

            LOG_DEBUG("Pod addresses assignment scheduled (PodId: %v)",
                pod->GetId());
        }
    }

    void ScheduleUpdatePodSpec(TPod* pod)
    {
        EnsureReadWrite();
        if (PodsAwaitingSpecUpdate_.insert(pod).second) {
            const auto& resourceManager = Bootstrap_->GetResourceManager();
            resourceManager->PrepareUpdatePodSpec(MakeStrong(Owner_), pod);

            LOG_DEBUG("Pod spec update scheduled (PodId: %v)",
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

        const TRowBufferPtr RowBuffer_ = New<TRowBuffer>();


        explicit TPersistenceContextBase(TTransaction::TImpl* transaction)
            : Transaction_(transaction)
        { }

        TKey CaptureKey(const TRange<TUnversionedValue>& key)
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

        virtual TString GetTablePath(const TDbTable* table) override
        {
            const auto& ytConnector = Transaction_->Bootstrap_->GetYTConnector();
            return ytConnector->GetTablePath(table);
        }

        virtual void ScheduleLookup(
            const TDbTable* table,
            const TRange<TUnversionedValue>& key,
            const TRange<const TDbField*>& fields,
            std::function<void(const TNullable<TRange<NYT::NTableClient::TVersionedValue>>&)> handler) override
        {
            LookupRequests_[std::make_pair(table, CaptureKey(key))].Subrequests.push_back(TLookupSubrequest{
                SmallVector<const TDbField*, 2>(fields.begin(), fields.end()),
                SmallVector<int, 2>(),
                handler
            });
        }

        virtual void ScheduleSelect(
            const TString& query,
            std::function<void(const NYT::NApi::IUnversionedRowsetPtr&)> handler) override
        {
            SelectRequests_.push_back(TSelectRequest{
                query,
                handler
            });
        }

        void RunReads(const IClientBasePtr& client)
        {
            if (SelectRequests_.empty() && LookupRequests_.empty()) {
                return;
            }

            const auto& Logger = Transaction_->Logger;

            LOG_DEBUG("Running reads");

            std::vector<TFuture<void>> asyncResults;
            std::vector<TFuture<IVersionedRowsetPtr>> asyncLookupResults;

            for (auto& request : SelectRequests_) {
                request.Tag = Format("Query: %v",
                    request.Query);

                LOG_DEBUG("Executing select (%v)",
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

            THashMap<const TDbField*, int> fieldToId;
            SmallVector<const TDbField*, 64> idToField;

            for (auto& pair : LookupRequests_) {
                const auto* table = pair.first.first;
                auto& request = pair.second;

                auto key = pair.first.second;
                auto keys = MakeSharedRange(std::vector<TKey>{key}, RowBuffer_);

                auto path = GetTablePath(table);

                auto nameTable = BuildNameTable(table);

                TVersionedLookupRowsOptions options;
                options.Timestamp = Transaction_->StartTimestamp_;
                options.KeepMissingRows = false;
                options.ColumnFilter.All = false;
                options.RetentionConfig = Transaction_->SingleVersionRetentionConfig_;
                auto& filterIndexes = options.ColumnFilter.Indexes;

                idToField.clear();
                fieldToId.clear();
                int currentFieldId = 0;
                for (auto& subrequest : request.Subrequests) {
                    for (const auto* field : subrequest.Fields) {
                        int resultId;
                        auto it = fieldToId.find(field);
                        if (it == fieldToId.end()) {
                            int filterId = nameTable->RegisterName(field->Name);
                            filterIndexes.push_back(filterId);
                            idToField.push_back(field);
                            resultId = currentFieldId++;
                            YCHECK(fieldToId.emplace(field, resultId).second);
                        } else {
                            resultId = it->second;
                        }
                        subrequest.ResultColumnIds.push_back(resultId);
                    }
                }

                request.Tag = Format("Path: %v, Columns: %v, Keys: %v",
                    path,
                    MakeFormattableRange(idToField, [] (TStringBuilder* builder, const auto* field) {
                        FormatValue(builder, field->Name, TStringBuf());
                    }),
                    keys);

                LOG_DEBUG("Executing lookup (%v)",
                    request.Tag);

                auto asyncResult = Transaction_->Client_->VersionedLookupRows(
                    path,
                    nameTable,
                    keys,
                    options);

                request.AsyncResult = asyncResult.Apply(BIND([] (const TErrorOr<IVersionedRowsetPtr>& rowsetOrError) {
                    THROW_ERROR_EXCEPTION_IF_FAILED(rowsetOrError, "Error fetching data from DB");
                    return rowsetOrError.Value();
                }));
                asyncResults.push_back(asyncResult.As<void>());
            }

            WaitFor(Combine(asyncResults))
                .ThrowOnError();

            LOG_DEBUG("Reads complete; parsing results");

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
                    LOG_DEBUG("Got select results (%v, RowCount: %v)",
                        request.Tag,
                        rowset->GetRows().Size());
                    request.Handler(rowset);
                });
            }

            SmallVector<TVersionedValue, 16> lookupRowValues;
            SmallVector<TVersionedValue, 16> lookupHandlerValues;
            for (const auto& pair : LookupRequests_) {
                const auto& request = pair.second;
                const auto& result = request.AsyncResult.Get().Value();
                auto rows = result->GetRows();
                Y_ASSERT(rows.Size() <= 1);

                auto invokeHandlersWithNull = [&] () {
                    for (const auto& subrequest : request.Subrequests) {
                        subrequest.Handler(Null);
                    }
                };

                auto invokeHandlersWithRows = [&] () {
                    for (const auto& subrequest : request.Subrequests) {
                        lookupHandlerValues.clear();
                        for (auto id : subrequest.ResultColumnIds) {
                            lookupHandlerValues.push_back(lookupRowValues[id]);
                        }
                        guardedRun([&] {
                            subrequest.Handler(MakeRange(lookupHandlerValues));
                        });
                    }
                };

                if (rows.Empty()) {
                    LOG_DEBUG("No rows found (%v)",
                        request.Tag);
                    invokeHandlersWithNull();
                    continue;
                }

                auto row = rows[0];

                auto maxWriteTimestamp = (row.BeginWriteTimestamps() == row.EndWriteTimestamps())
                    ? MinTimestamp
                    : row.BeginWriteTimestamps()[0];
                auto maxDeleteTimestamp = (row.BeginDeleteTimestamps() == row.EndDeleteTimestamps())
                    ? MinTimestamp
                    : row.BeginDeleteTimestamps()[0];
                if (maxWriteTimestamp <= maxDeleteTimestamp) {
                    LOG_DEBUG("Got dead lookup row (%v, Row: %v)",
                        request.Tag,
                        row);
                    invokeHandlersWithNull();
                    continue;
                }

                int maxId = -1;
                for (const auto& subrequest : request.Subrequests) {
                    for (auto id : subrequest.ResultColumnIds) {
                        maxId = std::max(maxId, id);
                    }
                }

                for (int index = 0; index < maxId + 1; ++index) {
                    lookupRowValues.push_back(MakeVersionedSentinelValue(EValueType::Null, NullTimestamp));
                }

                for (const auto* key = row.BeginKeys(); key != row.EndKeys(); ++key) {
                    TVersionedValue value;
                    static_cast<TUnversionedValue&>(value) = *key;
                    lookupRowValues[value.Id] = value;
                }

                // TODO(babenko)
                THashSet<int> seenIds;
                for (const auto* value = row.BeginValues(); value != row.EndValues(); ++value) {
                    if (seenIds.insert(value->Id).second) {
                        lookupRowValues[value->Id] = *value;
                    }
                }

                LOG_DEBUG("Got lookup row (%v, Row: %v)",
                    request.Tag,
                    row);
                invokeHandlersWithRows();
            }

            LOG_DEBUG("Results parsed");

            if (!errors.empty()) {
                THROW_ERROR_EXCEPTION("Error parsing database results")
                    << std::move(errors);
            }
        }

    private:
        struct TSelectRequest
        {
            TString Query;
            std::function<void(const NYT::NApi::IUnversionedRowsetPtr&)> Handler;
            TFuture<IUnversionedRowsetPtr> AsyncResult;
            TString Tag;
        };

        std::vector<TSelectRequest> SelectRequests_;

        struct TLookupSubrequest
        {
            SmallVector<const TDbField*, 4> Fields;
            SmallVector<int, 4> ResultColumnIds;
            std::function<void(const TNullable<TRange<NYT::NTableClient::TVersionedValue>>&)> Handler;
        };

        struct TLookupRequest
        {
            std::vector<TLookupSubrequest> Subrequests;
            TFuture<IVersionedRowsetPtr> AsyncResult;
            TString Tag;
        };

        THashMap<std::pair<const TDbTable*, TKey>, TLookupRequest> LookupRequests_;
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
            const TDbTable* table,
            const TRange<TUnversionedValue>& key,
            const TRange<const TDbField*>& fields,
            const TRange<TUnversionedValue>& values) override
        {
            Y_ASSERT(key.Size() == table->Key.size());
            Y_ASSERT(fields.Size() == values.Size());
            WriteRequests_[table].push_back(TWriteRequest{
                CaptureKey(key),
                SmallVector<const TDbField*, 4>(fields.begin(), fields.end()),
                SmallVector<TUnversionedValue, 4>(values.begin(), values.end())
            });
        }

        virtual void DeleteRow(
            const TDbTable* table,
            const TRange<TUnversionedValue>& key) override
        {
            Y_ASSERT(key.Size() == table->Key.size());
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

                THashMap<const TDbField*, int> fieldToId;
                SmallVector<const TDbField*, 64> idToField;

                const auto& requests = pair.second;

                for (const auto& request : requests) {
                    for (const auto* field : request.Fields) {
                        auto it = fieldToId.find(field);
                        if (it == fieldToId.end()) {
                            YCHECK(fieldToId.emplace(field, nameTable->RegisterName(field->Name)).second);
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
                    LOG_DEBUG("Executing write (Path: %v, Columns: %v, Row: %v)",
                        path,
                        MakeFormattableRange(MakeRange(row.Begin() + table->Key.size(), row.End()), [&] (TStringBuilder* builder, const auto& value) {
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
                    LOG_DEBUG("Executing delete (Path: %v, Key: %v)",
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
            SmallVector<const TDbField*, 4> Fields;
            SmallVector<TUnversionedValue, 4> Values;
        };

        THashMap<const TDbTable*, std::vector<TWriteRequest>> WriteRequests_;

        struct TDeleteRequest
        {
            TKey Key;
        };

        THashMap<const TDbTable*, std::vector<TDeleteRequest>> DeleteRequests_;


        TYPath GetTablePath(const TDbTable* table)
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
            ValidateCreatedObjects();
            FlushObjectsCreation();
            FlushObjectsDeletion();
            while (HasPendingLoads() || HasPendingStores()) {
                FlushLoadsOnce();
                FlushStoresOnce();
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
                THROW_ERROR_EXCEPTION("%v %Qv is already in %Qlv state",
                    GetCapitalizedHumanReadableTypeName(type),
                    actualId,
                    existingObject->GetState());
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

            YCHECK(InstantiatedObjects_.emplace(key, std::move(objectHolder)).second);

            object->SetState(EObjectState::Creating);
            CreatedObjects_.push_back(object);

            typeHandler->BeforeObjectCreated(MakeStrong(Owner_->Owner_), object);

            if (parentType != EObjectType::Null) {
                auto* parent = GetObject(parentType, parentId);
                auto* attribute = typeHandler->GetParentChildrenAttribute(parent);
                TChildrenAttributeHelper::Add(attribute, object);
            }

            LOG_DEBUG("Object created (ObjectId: %v, ParentId: %v, Type: %v)",
                actualId,
                parentId,
                type);

            return object;
        }

        virtual TObject* GetObject(EObjectType type, const TObjectId& id, const TObjectId& parentId = {}) override
        {
            if (!id) {
                THROW_ERROR_EXCEPTION("%v id cannot be empty",
                    GetCapitalizedHumanReadableTypeName(type));
            }

            auto key = std::make_pair(type, id);
            auto it = InstantiatedObjects_.find(key);
            if (it == InstantiatedObjects_.end()) {
                const auto& objectManager = Owner_->Bootstrap_->GetObjectManager();
                auto* typeHandler = objectManager->GetTypeHandlerOrThrow(type);
                auto objectHolder = typeHandler->InstantiateObject(id, parentId, this);
                auto* object = objectHolder.get();
                it = InstantiatedObjects_.emplace(key, std::move(objectHolder)).first;
                object->ScheduleExists();

                LOG_DEBUG("Object instantiated (ObjectId: %v, ParentId: %v, Type: %v)",
                    id,
                    typeHandler->GetParentType() == EObjectType::Null
                        ? "<None>"
                        : (parentId ? parentId.c_str() : "<Unknown>"),
                    type);
            }

            return it->second.get();
        }

        virtual void RemoveObject(TObject* object) override
        {
            Owner_->EnsureReadWrite();

            auto state = object->GetState();
            YCHECK(state != EObjectState::Creating);
            if (state == EObjectState::Removing ||
                state == EObjectState::Removed ||
                state == EObjectState::CreatedRemoving ||
                state == EObjectState::CreatedRemoved ||
                state == EObjectState::Missing)
            {
                return;
            }

            object->GetTypeHandler()->BeforeObjectRemoved(MakeStrong(Owner_->Owner_), object);

            if (state == EObjectState::Created) {
                object->SetState(EObjectState::CreatedRemoving);
            } else {
                object->SetState(EObjectState::Removing);
            }

            LOG_DEBUG("Object removed (ObjectId: %v)",
                object->GetId());

            object->GetTypeHandler()->AfterObjectRemoved(MakeStrong(Owner_->Owner_), object);

            if (state == EObjectState::Created) {
                object->SetState(EObjectState::CreatedRemoved);
            } else {
                RemovedObjects_[object->GetType()].push_back(object);
                object->SetState(EObjectState::Removed);
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
            Y_ASSERT(priority >= 0 && priority < LoadPriorityCount);
            ScheduledLoads_[priority].push_back(std::move(callback));
        }

        virtual void ScheduleStore(TStoreCallback callback) override
        {
            Owner_->EnsureReadWrite();
            ScheduledStores_.push_back(std::move(callback));
        }

        virtual void FlushLoads() override
        {
            while (HasPendingLoads()) {
                FlushLoadsOnce();
            }
        }

    private:
        TImpl* const Owner_;
        const NLogging::TLogger& Logger;

        THashMap<std::pair<EObjectType, TObjectId>, std::unique_ptr<TObject>> InstantiatedObjects_;
        std::vector<TObject*> CreatedObjects_;
        TEnumIndexedVector<std::vector<TObject*>, EObjectType> RemovedObjects_;

        std::array<std::vector<TLoadCallback>, LoadPriorityCount> ScheduledLoads_;
        std::vector<TStoreCallback> ScheduledStores_;


        static TObjectId GenerateId(const TObjectId& id)
        {
            if (id) {
                return id;
            }

            TStringBuilder builder;
            static const char HexDigits[] = "0123456789abcdef";
            for (int index = 0; index < 8; ++index) {
                builder.AppendChar(HexDigits[RandomNumber<size_t>(16)]);
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
            LOG_DEBUG("Started validating created object");

            auto owner = MakeStrong(Owner_->Owner_);

            std::vector<std::unique_ptr<TObjectExistenceChecker>> checkers;
            std::vector<std::pair<TObject*, TObject*>> objectParentPairs;
            for (auto* object : CreatedObjects_) {
                if (object->GetState() != EObjectState::Created) {
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
                    parent->ScheduleExists();
                    objectParentPairs.emplace_back(object, parent);
                }

                typeHandler->AfterObjectCreated(owner, object);
            }

            FlushLoads();

            for (const auto& checker : checkers) {
                if (checker->Check()) {
                    auto* object = checker->GetObject();
                    THROW_ERROR_EXCEPTION(
                        NClient::NApi::EErrorCode::DuplicateObjectId,
                        "Object %Qv of type %Qlv already exists",
                        object->GetId(),
                        object->GetType());
                }
            }

            for (const auto& pair : objectParentPairs) {
                if (!pair.second->Exists()) {
                    THROW_ERROR_EXCEPTION("Parent %v %Qv of %v %Qv does not exist",
                        GetLowercaseHumanReadableTypeName(pair.second->GetType()),
                        pair.second->GetId(),
                        GetLowercaseHumanReadableTypeName(pair.first->GetType()),
                        pair.first->GetId());
                }
            }

            LOG_DEBUG("Finished validating created objects");
        }

        void FlushObjectsCreation()
        {
            LOG_DEBUG("Started preparing objects creation");
            TStoreContext context(Owner_);

            for (const auto* object : CreatedObjects_) {
                if (object->GetState() != EObjectState::Created) {
                    continue;
                }

                auto* typeHandler = object->GetTypeHandler();

                // Delete previous incarnation, of any.
                context.DeleteRow(
                    typeHandler->GetTable(),
                    CaptureCompositeObjectKey(object, context.GetRowBuffer()));

                if (typeHandler->GetParentType() != EObjectType::Null) {
                    auto parentId = object->GetParentId();
                    YCHECK(parentId);

                    context.WriteRow(
                        &ParentsTable,
                        ToDbValues(
                            context.GetRowBuffer(),
                            object->GetId(),
                            object->GetType()),
                        MakeArray(&ParentsTable.Fields.ParentId),
                        ToDbValues(
                            context.GetRowBuffer(),
                            parentId));
                }
            }

            context.FillTransaction(Owner_->UnderlyingTransaction_);
            LOG_DEBUG("Finished preparing objects creation");
        }

        void FlushObjectsDeletion()
        {
            auto now = TInstant::Now();

            LOG_DEBUG("Started preparing objects deletion");
            TStoreContext context(Owner_);

            const auto& objectManager = Owner_->Bootstrap_->GetObjectManager();
            for (auto type : TEnumTraits<EObjectType>::GetDomainValues()) {
                if (type == EObjectType::Null) {
                    continue;
                }

                auto* typeHandler = objectManager->GetTypeHandler(type);
                auto parentType = typeHandler->GetParentType();
                const auto* table = typeHandler->GetTable();

                const auto& objects = RemovedObjects_[type];
                for (const auto* object : objects) {
                    context.WriteRow(
                        table,
                        CaptureCompositeObjectKey(object, context.GetRowBuffer()),
                        MakeArray(&ObjectsTable.Fields.Meta_RemovalTime),
                        ToDbValues(
                            context.GetRowBuffer(),
                            now));
                    if (parentType != EObjectType::Null) {
                        context.DeleteRow(
                            &ParentsTable,
                            ToDbValues(
                                context.GetRowBuffer(),
                                object->GetId(),
                                type));
                    }
                }
            }

            context.FillTransaction(Owner_->UnderlyingTransaction_);
            LOG_DEBUG("Finished preparing objects deletion");
        }

        void FlushLoadsOnce()
        {
            for (int priority = 0; priority < LoadPriorityCount; ++priority) {
                auto& scheduledLoads = ScheduledLoads_[priority];
                if (scheduledLoads.empty()) {
                    continue;
                }

                LOG_DEBUG("Started preparing reads (Priority: %v, Count: %v)",
                    priority,
                    scheduledLoads.size());

                TLoadContext context(Owner_);

                std::decay<decltype(scheduledLoads)>::type swappedLoads;
                std::swap(scheduledLoads, swappedLoads);
                for (const auto& callback: swappedLoads) {
                    callback(&context);
                }

                LOG_DEBUG("Finished preparing reads");

                context.RunReads(Owner_->UnderlyingTransaction_);
            }
        }

        void FlushStoresOnce()
        {
            if (ScheduledStores_.empty()) {
                return;
            }

            LOG_DEBUG("Started preparing writes (Count: %v)",
                ScheduledStores_.size());

            TStoreContext context(Owner_);

            decltype(ScheduledStores_) swappedStores;
            std::swap(ScheduledStores_, swappedStores);
            for (const auto& callback : swappedStores) {
                callback(&context);
            }

            context.FillTransaction(Owner_->UnderlyingTransaction_);

            LOG_DEBUG("Finished preparing writes");
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
    THashSet<TPod*> PodsAwaitingAddressesUpdate_;
    THashSet<TPod*> PodsAwaitingSpecUpdate_;


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

    static TNameTablePtr BuildNameTable(const TDbTable* table)
    {
        auto nameTable = New<TNameTable>();
        for (size_t index = 0; index < table->Key.size(); ++index) {
            YCHECK(nameTable->RegisterName(table->Key[index]->Name) == index);
        }
        return nameTable;
    }

    void EnsureReadWrite()
    {
        Y_ASSERT(UnderlyingTransaction_);
    }


    TObject* DoCreateObject(
        EObjectType type,
        const IMapNodePtr& attributes,
        IUpdateContext* context)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto* typeHandler = objectManager->GetTypeHandler(type);

        auto transaction = MakeStrong(Owner_);

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
                        YCHECK(UnmatchedMandatoryAttributes_.insert(schema).second);
                    }
                    if (schema->HasInitializer()) {
                        YCHECK(PendingInitializerAttributes_.insert(schema).second);
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
                    auto* fallbackChild = schema->FindFallbackChild();
                    for (const auto& pair : mapNode->GetChildren()) {
                        const auto& key = pair.first;
                        const auto& value = pair.second;
                        auto* child = schema->FindChild(key);
                        if (child) {
                            DoMatch(value, child);
                        } else if (fallbackChild) {
                            AddMatch({
                                fallbackChild,
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
                        if (!schema->HasSetter()) {
                            THROW_ERROR_EXCEPTION("Attribute %v cannot be set",
                                schema->GetPath());
                        }
                        AddMatch({
                            schema,
                            TSetUpdateRequest{TYPath(), node}
                        });
                    }
                    if (schema->GetMandatory()) {
                        YCHECK(UnmatchedMandatoryAttributes_.erase(schema) == 1);
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
            PreloadAttribute(transaction, object, match);
        }

        THashSet<TAttributeSchema*> updatedAttributes;
        for (const auto& match : matcher.Matches()) {
            auto* schema = match.Schema;
            const auto& request = match.Request.As<TSetUpdateRequest>();
            context->AddSetter([=] {
                try {
                    schema->RunSetter(
                        transaction,
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
            schema->RunInitializer(transaction, object);
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
        IUpdateContext* context)
    {
        object->ValidateExists();

        auto* typeHandler = object->GetTypeHandler();
        auto transaction = MakeStrong(Owner_);

        std::vector<TAttributeUpdateMatch> matches;
        matches.reserve(requests.size());
        for (const auto& request : requests) {
            matches.push_back(MatchAttributeUpdate(typeHandler, request));
        }

        for (const auto& match : matches) {
            PreloadAttribute(transaction, object, match);
        }

        THashSet<TAttributeSchema*> updatedAttributes;
        for (const auto& match : matches) {
            context->AddSetter([=] {
                ApplyAttributeUpdate(transaction, object, match);
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
        auto transaction = MakeStrong(Owner_);
        for (auto* schema : attributes) {
            context->AddFinalizer([=] {
                schema->RunUpdateHandlers(transaction, object);
            });
            Validators_.push_back([=] {
                schema->RunValidators(transaction, object);
            });
        }
    }

    struct TAttributeUpdateMatch
    {
        TAttributeSchema* Schema;
        TUpdateRequest Request;
    };

    static const TYPath& GetRequestPath(const TUpdateRequest& request)
    {
        switch (request.Tag()) {
            case TUpdateRequest::TagOf<TSetUpdateRequest>():
                return request.As<TSetUpdateRequest>().Path;
            case TUpdateRequest::TagOf<TRemoveUpdateRequest>():
                return request.As<TRemoveUpdateRequest>().Path;
            default:
                Y_UNREACHABLE();
        }
    }


    static TUpdateRequest PatchRequestPath(
        const TUpdateRequest& request,
        const TYPath& path)
    {
        switch (request.Tag()) {
            case TUpdateRequest::TagOf<TSetUpdateRequest>(): {
                const auto& updateRequest = request.As<TSetUpdateRequest>();
                return TSetUpdateRequest{path, updateRequest.Value, updateRequest.Recursive};
            }
            case TUpdateRequest::TagOf<TRemoveUpdateRequest>():
                return TRemoveUpdateRequest{path};
            default:
                Y_UNREACHABLE();
        }
    }

    TAttributeUpdateMatch MatchAttributeUpdate(
        IObjectTypeHandler* typeHandler,
        const TUpdateRequest& request)
    {
        auto resolveResult = ResolveAttribute(typeHandler, GetRequestPath(request));
        if (!resolveResult.Attribute->GetUpdatable()) {
            THROW_ERROR_EXCEPTION("Attribute %v does not support updates",
                resolveResult.Attribute->GetPath());
        }
        return {resolveResult.Attribute, PatchRequestPath(request, resolveResult.SuffixPath)};
    }

    void PreloadAttribute(
        const TTransactionPtr& transaction,
        TObject* object,
        const TAttributeUpdateMatch& match)
    {
        if (GetRequestPath(match.Request).empty()) {
            return;
        }

        if (!match.Schema->HasPreloader()) {
            return;
        }

        LOG_DEBUG("Scheduling attribute load (ObjectId: %v, Attribute: %v)",
            object->GetId(),
            match.Schema->GetPath());

        match.Schema->RunPreloader(transaction, object, match.Request);
    }

    void ApplyAttributeUpdate(
        const TTransactionPtr& transaction,
        TObject* object,
        const TAttributeUpdateMatch& match)
    {
        const auto& request = match.Request;

        try {
            switch (request.Tag()) {
                case TUpdateRequest::TagOf<TSetUpdateRequest>():
                    ApplyAttributeSetUpdate(
                        transaction,
                        object,
                        match.Schema,
                        request.As<TSetUpdateRequest>());
                    break;
                case TUpdateRequest::TagOf<TRemoveUpdateRequest>():
                    ApplyAttributeRemoveUpdate(
                        transaction,
                        object,
                        match.Schema,
                        request.As<TRemoveUpdateRequest>());
                    break;
                default:
                    Y_UNREACHABLE();
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error updating attribute %v of %v %Qv",
                match.Schema->GetPath(),
                GetLowercaseHumanReadableTypeName(object->GetType()),
                object->GetId())
                << ex;
        }
    }

    void ApplyAttributeSetUpdate(
        const TTransactionPtr& transaction,
        TObject* object,
        TAttributeSchema* schema,
        const TSetUpdateRequest& request)
    {
        LOG_DEBUG("Applying set update (ObjectId: %v, Attribute: %v, Path: %v, Value: %v)",
            object->GetId(),
            schema->GetPath(),
            request.Path,
            ConvertToYsonString(request.Value, NYson::EYsonFormat::Text));

        if (!schema->HasSetter()) {
            THROW_ERROR_EXCEPTION("Attribute %v does not support set updates",
                schema->GetPath());
        }

        schema->RunSetter(transaction, object, request.Path, request.Value, request.Recursive);
    }

    void ApplyAttributeRemoveUpdate(
        const TTransactionPtr& transaction,
        TObject* object,
        TAttributeSchema* schema,
        const TRemoveUpdateRequest& request)
    {
        LOG_DEBUG("Applying remove update (ObjectId: %v, Attribute: %v, Path: %v)",
            object->GetId(),
            schema->GetPath(),
            request.Path);

        if (!schema->HasRemover()) {
            THROW_ERROR_EXCEPTION("Attribute %v does not support remove updates",
                schema->GetPath());
        }

        schema->RunRemover(transaction, object, request.Path);
    }


    std::unique_ptr<TQuery> MakeQuery(IObjectTypeHandler* typeHandler)
    {
        auto query = std::make_unique<TQuery>();
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        const auto* table = typeHandler->GetTable();
        query->Table = TTableDescriptor(ytConnector->GetTablePath(table),  PrimaryTableAlias);
        query->SelectExprs.Emplace();
        return query;
    }

    static std::vector<TAttributeFetcher> BuildAttributeFetchers(
        IObjectTypeHandler* typeHandler,
        const TTransactionPtr& transaction,
        TQuery* query,
        TAttributeFetcherContext* fetcherContext,
        IQueryContext* queryContext,
        const TAttributeSelector& selector)
    {
        if (selector.Paths.empty()) {
            static const auto DummyExpr = New<TLiteralExpression>(TSourceLocation(), TLiteralValue(false));
            query->SelectExprs->push_back(std::move(DummyExpr));
            return {};
        }

        std::vector<TAttributeFetcher> fetchers;
        for (const auto& path : selector.Paths) {
            auto resolveResult = ResolveAttribute(typeHandler, path);
            fetchers.emplace_back(typeHandler, resolveResult, transaction, fetcherContext, queryContext);
        }
        query->SelectExprs = fetcherContext->SelectExprs;
        return fetchers;
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

void TTransaction::UpdateObject(TObject* object, const std::vector<TUpdateRequest>& requests)
{
    return Impl_->UpdateObject(object, requests);
}

void TTransaction::UpdateObject(TObject* object, const std::vector<TUpdateRequest>& requests, IUpdateContext* context)
{
    return Impl_->UpdateObject(object, requests, context);
}

TObject* TTransaction::GetObject(EObjectType type, const TObjectId& id, const TObjectId& parentId)
{
    return Impl_->GetObject(type, id, parentId);
}

TGetQueryResult TTransaction::ExecuteGetQuery(
    EObjectType type,
    const TObjectId& id,
    const TAttributeSelector& selector)
{
    return Impl_->ExecuteGetQuery(
        type,
        id,
        selector);
}

TSelectQueryResult TTransaction::ExecuteSelectQuery(
    EObjectType type,
    const TNullable<TObjectFilter>& filter,
    const TAttributeSelector& selector,
    const TSelectQueryOptions& options)
{
    return Impl_->ExecuteSelectQuery(
        type,
        filter,
        selector,
        options);
}

TNode* TTransaction::GetNode(const TObjectId& id)
{
    return Impl_->GetNode(id);
}

TNode* TTransaction::CreateNode(const TObjectId& id)
{
    return Impl_->CreateNode(id);
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

void TTransaction::ScheduleUpdatePodAddresses(TPod* pod)
{
    Impl_->ScheduleUpdatePodAddresses(pod);
}

void TTransaction::ScheduleUpdatePodSpec(TPod* pod)
{
    Impl_->ScheduleUpdatePodSpec(pod);
}

TAsyncSemaphoreGuard TTransaction::AcquireLock()
{
    return Impl_->AcquireLock();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP

