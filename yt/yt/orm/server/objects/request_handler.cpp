#include "request_handler.h"

#include "aggregate_query_executor.h"
#include "db_config.h"
#include "helpers.h"
#include "object.h"
#include "select_object_history_executor.h"
#include "transaction_wrapper.h"
#include "watch_manager.h"

#include <yt/yt/orm/server/master/bootstrap.h>
#include <yt/yt/orm/server/master/service_detail.h>

#include <yt/yt/orm/server/access_control/access_control_manager.h>

#include <yt/yt/orm/client/objects/registry.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NYT::NTracing;

namespace {

auto BuildTransactionCallContext(const NRpc::IServiceContextPtr& context)
{
    return TTransactionCallContext{context->GetEndpointDescription()};
}

auto FlushPerformanceStatistics(const auto& transaction, bool need)
{
    return need
        ? std::make_optional(transaction->FlushPerformanceStatistics())
        : std::nullopt;
}

TCommonWriteResult GetCommonWriteResult(
    const TTransactionPtr& transaction,
    bool collectStatistics,
    auto commitTimestamp)
{
    return {
        .TransactionId = transaction->GetId(),
        .CommitTimestamp = commitTimestamp,
        .PerformanceStatistics = FlushPerformanceStatistics(transaction, collectStatistics)
    };
}

TCommonReadResult GetCommonReadResult(const TTransactionPtr& transaction, bool collectStatistics)
{
    return {
        .StartTimestamp = transaction->GetStartTimestamp(),
        .PerformanceStatistics = FlushPerformanceStatistics(transaction, collectStatistics)
    };
}

std::vector<TObjectInfo> GetCreatedObjectInfos(
    const TTransactionPtr& transaction,
    const TCreateObjectsRequest& request,
    const std::vector<TObject*>& objects)
{
    TTraceContextGuard guard(CreateTraceContextFromCurrent("NYT::NOrm::GetCreatedObjectInfos"));

    std::vector<TObjectInfo> objectInfos;
    objectInfos.reserve(objects.size());
    for (auto* object : objects) {
        objectInfos.push_back({
            .Key = object->GetKey(),
            .Type = object->GetType(),
            .Fqid = object->GetFqid(),
        });
        if (request.NeedIdentityMeta) {
            objectInfos.back().IdentityMeta = GetObjectIdentityMeta(transaction.Get(), object);
        }
    }
    return objectInfos;
}

std::vector<TObject*> GetRequestObjects(const TTransactionPtr& transaction, const TUpdateObjectsRequest& request)
{
    TTraceContextGuard guard(CreateTraceContextFromCurrent("NYT::NOrm::GetRequestObjects"));

    std::vector<TObject*> objects;
    objects.reserve(request.Subrequests.size());

    for (auto& subrequest : request.Subrequests) {
        auto* object = transaction->GetObject(
            subrequest.Type,
            subrequest.Match.Key,
            subrequest.Match.ParentKey);
        object->PrepareUuidValidation(subrequest.Match.Uuid);
        objects.push_back(object);
    }

    return objects;
}

std::vector<TObjectUpdateRequest> PrepareObjectUpdates(
    const TUpdateObjectsRequest& request,
    const std::vector<TObject*>& objects)
{
    TTraceContextGuard guard(CreateTraceContextFromCurrent("NYT::NOrm::PrepareObjectUpdates"));

    std::vector<TObjectUpdateRequest> objectUpdates;
    objectUpdates.reserve(request.Subrequests.size());

    for (size_t i = 0; i < request.Subrequests.size(); ++i) {
        auto* object = objects[i];
        auto& subrequest = request.Subrequests[i];
        if (object->DoesExist()) {
            object->ValidateUuid(subrequest.Match.Uuid);
            objectUpdates.push_back(TObjectUpdateRequest{
                .Object = object,
                .Updates = std::move(subrequest.Updates),
                .Prerequisites = std::move(subrequest.Prerequisites),
                .Consumer = subrequest.Consumer
            });
        } else if (!request.IgnoreNonexistent) {
            // Throws NoSuchObject error.
            object->ValidateExists();
        }
    }

    return objectUpdates;
}

std::vector<std::optional<TObjectInfo>> GetUpdatedObjectInfos(
    const TTransactionPtr& transaction,
    const TUpdateObjectsRequest& request,
    const std::vector<TObject*>& objects)
{
    TTraceContextGuard guard(CreateTraceContextFromCurrent("NYT::NOrm::GetUpdatedObjectInfos"));

    std::vector<std::optional<TObjectInfo>> objectInfos;
    objectInfos.reserve(objects.size());
    for (const auto& object : objects) {
        auto& optionalObjectInfo = objectInfos.emplace_back();
        if (object->DoesExist()) {
            optionalObjectInfo.emplace(TObjectInfo{
                .Key = object->GetKey(),
                .Type = object->GetType(),
            });
            if (request.NeedIdentityMeta) {
                optionalObjectInfo->IdentityMeta = GetObjectIdentityMeta(transaction.Get(), object);
            }
        }
    }

    return objectInfos;
}

std::vector<TObject*> GetRemoveRequestObjects(
    const TTransactionPtr& transaction,
    const TRemoveObjectsRequest& request)
{
    TTraceContextGuard guard(CreateTraceContextFromCurrent("NYT::NOrm::GetRemoveRequestObjects"));

    std::vector<TObject*> objects;
    objects.reserve(request.Subrequests.size());
    for (auto& subrequest : request.Subrequests) {
        auto* object = transaction->GetObject(
            subrequest.Type,
            subrequest.Match.Key,
            subrequest.Match.ParentKey);
        object->PrepareUuidValidation(subrequest.Match.Uuid);
        objects.push_back(object);
    }
    return objects;
}

std::vector<TObject*> CollectObjectsToRemove(
    const TTransactionPtr& /*transaction*/,
    const TRemoveObjectsRequest& request,
    const std::vector<TObject*>& objects)
{
    TTraceContextGuard guard(CreateTraceContextFromCurrent("NYT::NOrm::CollectObjectsToRemove"));

    std::vector<TObject*> objectsToRemove;
    objectsToRemove.reserve(request.Subrequests.size());
    for (size_t i = 0; i < request.Subrequests.size(); ++i) {
        TObject* object = objects[i];
        auto& subrequest = request.Subrequests[i];
        if (object->DoesExist()) {
            object->ValidateUuid(subrequest.Match.Uuid);
            objectsToRemove.push_back(object);
        } else if (!request.IgnoreNonexistent) {
            // Throws NoSuchObject error.
            object->ValidateExists();
        }
    }
    return objectsToRemove;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    NYT::TStringBuilderBase* builder,
    const TCommonOptions& options,
    TStringBuf /*format*/)
{
    builder->AppendFormat("{"
        "AllowFullScan: %v, "
        "CollectStatistics: %v}",
        options.AllowFullScan,
        options.CollectStatistics);
}

////////////////////////////////////////////////////////////////////////////////

TRequestHandler::TRequestHandler(NMaster::IBootstrap* bootstrap, bool enforceReadPermissions)
    : Bootstrap_(bootstrap)
    , EnforceReadPermissions_(enforceReadPermissions)
{}

TTimestamp TRequestHandler::GenerateTimestamps(
    const NRpc::IServiceContextPtr& context,
    int count) const
{
    const auto authUserGuard = NAccessControl::MakeAuthenticatedUserGuard(
        context,
        Bootstrap_->GetAccessControlManager());
    const auto& transactionManager = Bootstrap_->GetTransactionManager();
    const auto timestamp = WaitFor(transactionManager->GenerateTimestamps(count))
        .ValueOrThrow();
    return timestamp;
}

TStartTransactionResponse TRequestHandler::StartTransaction(
    const NRpc::IServiceContextPtr& context,
    TStartReadWriteTransactionOptions&& options) const
{
    const auto authUserGuard = NAccessControl::MakeAuthenticatedUserGuardAndThrottle(
        context,
        Bootstrap_->GetAccessControlManager(),
        /*requestWeight*/ 1);

    const auto& transactionManager = Bootstrap_->GetTransactionManager();
    const auto transaction = WaitFor(transactionManager->StartReadWriteTransaction(std::move(options)))
        .ValueOrThrow();
    if (options.ReadingTransactionOptions.AllowFullScan) {
        transaction->AllowFullScan(*options.ReadingTransactionOptions.AllowFullScan);
    }

    return {
        .TransactionId = transaction->GetId(),
        .StartTimestamp = transaction->GetStartTimestamp(),
        .StartTime = transaction->GetStartTime(),
    };
}

TCommitTransactionResponse TRequestHandler::CommitTransaction(
    const NRpc::IServiceContextPtr& context,
    TTransactionId transactionId,
    TCommitTransactionOptions&& options) const
{
    const auto authUserGuard = NAccessControl::MakeAuthenticatedUserGuardAndThrottle(
        context,
        Bootstrap_->GetAccessControlManager(),
        /*requestWeight*/ 1);

    TTransactionWrapper transactionWrapper(
        Bootstrap_->GetTransactionManager(),
        transactionId,
        context->GetTimeout(),
        /*allowFullScan*/ std::nullopt,
        /*mustOwn*/ true);
    const auto& transaction = transactionWrapper.Unwrap();

    auto result = WaitFor(transaction->Commit(std::move(options.TransactionContext)))
        .ValueOrThrow();

    auto response = TCommitTransactionResponse{
        .CommitResult = std::move(result),
        .PerformanceStatistics = FlushPerformanceStatistics(transaction, options.CollectStatistics),
        .TotalPerformanceStatistics = transaction->GetTotalPerformanceStatistics(),
    };
    return response;
}

void TRequestHandler::AbortTransaction(const NRpc::IServiceContextPtr& context, TTransactionId transactionId) const
{
    const auto authUserGuard = NAccessControl::MakeAuthenticatedUserGuardAndThrottle(
        context,
        Bootstrap_->GetAccessControlManager(),
        /*requestWeight*/ 1);

    TTransactionWrapper transactionWrapper(
        Bootstrap_->GetTransactionManager(),
        transactionId,
        context->GetTimeout(),
        /*allowFullScan*/ std::nullopt,
        /*mustOwn*/ false);
    const auto& transaction = transactionWrapper.Unwrap();

    // Fire-and-forget.
    YT_UNUSED_FUTURE(transaction->Abort());
}

TCreateObjectsResponse TRequestHandler::DoCreateObjects(
    const NRpc::IServiceContextPtr& context,
    TTransactionId transactionId,
    TCreateObjectsRequest&& request,
    bool dryRun) const
{
    const auto authUserGuard = NAccessControl::MakeAuthenticatedUserGuardAndThrottle(
        context,
        Bootstrap_->GetAccessControlManager(),
        /*requestWeight*/ request.Subrequests.size());

    TTransactionWrapper transactionWrapper(
        Bootstrap_->GetTransactionManager(),
        transactionId,
        context->GetTimeout(),
        request.CommonOptions.AllowFullScan,
        /*mustOwn*/ true);
    const auto& transaction = transactionWrapper.Unwrap();

    const auto transactionCallContext = BuildTransactionCallContext(context);
    const auto objects = transactionWrapper.Unwrap()->CreateObjects(
        std::move(request.Subrequests),
        transactionCallContext);

    transaction->PerformAttributeMigrations();

    if (request.NeedIdentityMeta) {
        for (auto* object : objects) {
            object->ScheduleMetaResponseLoad();
        }
    }

    if (dryRun) {
        transaction->RunPrecommitActions({});
        return {};
    }

    const auto commitTimestamp = transactionWrapper.CommitIfOwned();

    auto objectInfos = GetCreatedObjectInfos(transaction, request, objects);

    return {
        .CommonWriteResult = GetCommonWriteResult(
            transactionWrapper.Unwrap(),
            request.CommonOptions.CollectStatistics,
            commitTimestamp),
        .ObjectInfos = std::move(objectInfos)
    };
}

TCreateObjectsResponse TRequestHandler::CreateObjects(
    const NRpc::IServiceContextPtr& context,
    TTransactionId transactionId,
    TCreateObjectsRequest&& request) const
{
    return DoCreateObjects(
        context,
        transactionId,
        std::move(request));
}

TRemoveObjectsResponse TRequestHandler::DoRemoveObjects(
    const NRpc::IServiceContextPtr& context,
    TTransactionId transactionId,
    TRemoveObjectsRequest&& request,
    bool dryRun) const
{
    const auto authUserGuard = NAccessControl::MakeAuthenticatedUserGuardAndThrottle(
        context,
        Bootstrap_->GetAccessControlManager(),
        /*requestWeight*/ request.Subrequests.size());

    TTransactionWrapper transactionWrapper(
        Bootstrap_->GetTransactionManager(),
        transactionId,
        context->GetTimeout(),
        request.CommonOptions.AllowFullScan,
        /*mustOwn*/ true);
    const auto& transaction = transactionWrapper.Unwrap();

    auto objects = GetRemoveRequestObjects(transaction, request);
    auto objectsToRemove = CollectObjectsToRemove(transaction, request, objects);

    if (request.AllowRemovalWithNonEmptyReferences) {
        transaction->AllowRemovalWithNonEmptyReferences(*request.AllowRemovalWithNonEmptyReferences);
    }
    if (request.NeedIdentityMeta) {
        for (const auto& object : objects) {
            object->ScheduleMetaResponseLoad();
        }
    }

    transaction->RemoveObjects(std::move(objectsToRemove));

    std::vector<TRemoveObjectsResponse::TSubresponse> subresponses;
    subresponses.reserve(request.Subrequests.size());
    for (const auto& object : objects) {
        auto& subresponse = subresponses.emplace_back();
        if (object->DoesExist() && Bootstrap_->GetDBConfig().EnableFinalizers) {
            subresponse.FinalizationStartTime = TInstant::MicroSeconds(object->FinalizationStartTime());
        }
        if (object->DidExist()) {
            subresponse.ObjectInfo.emplace(TObjectInfo{
                .Key = object->GetKey(),
                .Type = object->GetType(),
            });
            if (request.NeedIdentityMeta) {
                subresponse.ObjectInfo->IdentityMeta = GetObjectIdentityMeta(transaction.Get(), object);
            }
        }
    }

    if (dryRun) {
        transaction->RunPrecommitActions({});
        return {};
    }

    const auto commitTimestamp = transactionWrapper.CommitIfOwned();

    return {
        .CommonWriteResult = GetCommonWriteResult(
            transactionWrapper.Unwrap(),
            request.CommonOptions.CollectStatistics,
            commitTimestamp),
        .Subresponses = std::move(subresponses),
    };
}

TRemoveObjectsResponse TRequestHandler::RemoveObjects(
    const NRpc::IServiceContextPtr& context,
    TTransactionId transactionId,
    TRemoveObjectsRequest&& request) const
{
    return DoRemoveObjects(
        context,
        transactionId,
        std::move(request));
}

TUpdateObjectsResponse TRequestHandler::DoUpdateObjects(
    const NRpc::IServiceContextPtr& context,
    TTransactionId transactionId,
    TUpdateObjectsRequest&& request,
    bool dryRun) const
{
    const auto authUserGuard = NAccessControl::MakeAuthenticatedUserGuardAndThrottle(
        context,
        Bootstrap_->GetAccessControlManager(),
        /*requestWeight*/ request.Subrequests.size());

    TTransactionWrapper transactionWrapper(
        Bootstrap_->GetTransactionManager(),
        transactionId,
        context->GetTimeout(),
        request.CommonOptions.AllowFullScan,
        /*mustOwn*/ true);
    const auto& transaction = transactionWrapper.Unwrap();

    const auto transactionCallContext = BuildTransactionCallContext(context);

    auto objects = GetRequestObjects(transaction, request);
    auto objectUpdates = PrepareObjectUpdates(request, objects);

    if (request.NeedIdentityMeta) {
        for (const auto& object : objects) {
            object->ScheduleMetaResponseLoad();
        }
    }

    const auto updateContext = transaction->CreateUpdateContext();
    transaction->UpdateObjects(std::move(objectUpdates), updateContext.get(), transactionCallContext);
    updateContext->Commit();

    transaction->PerformAttributeMigrations();

    auto objectInfos = GetUpdatedObjectInfos(transaction, request, objects);

    if (dryRun) {
        transaction->RunPrecommitActions({});
        return {};
    }

    const auto commitTimestamp = transactionWrapper.CommitIfOwned();

    return {
        .CommonWriteResult = GetCommonWriteResult(
            transactionWrapper.Unwrap(),
            request.CommonOptions.CollectStatistics,
            commitTimestamp),
        .ObjectInfos = std::move(objectInfos),
    };
}

TUpdateObjectsResponse TRequestHandler::UpdateObjects(
    const NRpc::IServiceContextPtr& context,
    TTransactionId transactionId,
    TUpdateObjectsRequest&& request) const
{
    return DoUpdateObjects(
        context,
        transactionId,
        std::move(request));
}

TGetObjectsResponse TRequestHandler::GetObjects(
    const NRpc::IServiceContextPtr& context,
    const std::vector<TKeyAttributeMatches>& matches,
    IAttributeValuesConsumerGroup* consumerGroup,
    TGetObjectsRequest&& request) const
{
    const auto authUserGuard = NAccessControl::MakeAuthenticatedUserGuardAndThrottle(
        context,
        Bootstrap_->GetAccessControlManager(),
        /*requestWeight*/ matches.size());

    TTransactionPtr transaction;
    if (request.QueryOptions.ReadUncommittedChanges) {
        transaction = TTransactionWrapper(
            Bootstrap_->GetTransactionManager(),
            std::get<TTransactionId>(request.TimestampOrTransactionId),
            context->GetTimeout(),
            request.CommonOptions.AllowFullScan,
            /*mustOwn*/ true).Unwrap();
    } else {
        transaction = TReadOnlyTransactionWrapper(
            Bootstrap_->GetTransactionManager(),
            request.TimestampOrTransactionId,
            context->GetTimeout(),
            request.CommonOptions.AllowFullScan).Unwrap();
    }

    request.QueryOptions.CheckReadPermissions = !IsSchemaReadAllowed(request.ObjectType);

    auto getObjectsExecutor = MakeGetQueryExecutor(
        transaction,
        request.ObjectType,
        request.Selector,
        matches,
        request.QueryOptions);
    std::move(*getObjectsExecutor).ExecuteConsuming(consumerGroup);

    return {
        .CommonReadResult = GetCommonReadResult(transaction, request.CommonOptions.CollectStatistics),
    };
}

TWatchObjectsResponse TRequestHandler::WatchObjects(
    const NRpc::IServiceContextPtr& context,
    TWatchObjectsRequest&& request) const
{
    const auto authUserGuard = NAccessControl::MakeAuthenticatedUserGuardAndThrottle(
        context,
        Bootstrap_->GetAccessControlManager(),
        /*requestWeight*/ 1);

    if (!IsSchemaReadAllowed(request.QueryOptions.ObjectType)) {
        THROW_ERROR_EXCEPTION(NClient::EErrorCode::AuthorizationError,
            "Current user does not have permission to read objects of type %v",
            NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(request.QueryOptions.ObjectType));
    }

    const auto& transactionManager = Bootstrap_->GetTransactionManager();

    auto startTimestamp = std::get_if<TTimestamp>(&request.QueryOptions.InitialOffsets);
    if (startTimestamp &&
        Bootstrap_->GetWatchManager()->GetConfig()->ValidateStartTimestampDataCompleteness)
    {
        transactionManager->ValidateDataCompleteness(*startTimestamp);
    }

    const auto transaction = TReadOnlyTransactionWrapper(
        transactionManager,
        {NullTimestamp},
        context->GetTimeout(),
        request.CommonOptions.AllowFullScan)
        .Unwrap();

    const auto executor = MakeWatchQueryExecutor(transaction);
    auto result = executor->ExecuteWatchQuery(std::move(request.QueryOptions));

    return TWatchObjectsResponse{
        .QueryResult = std::move(result),
        .PerformanceStatistics = FlushPerformanceStatistics(transaction, request.CommonOptions.CollectStatistics)
    };
}

TSelectObjectsResponse TRequestHandler::SelectObjects(
    const NRpc::IServiceContextPtr& context,
    IAttributeValuesConsumerGroup* consumerGroup,
    TSelectObjectsRequest&& request) const
{
    const auto authUserGuard = NAccessControl::MakeAuthenticatedUserGuardAndThrottle(
        context,
        Bootstrap_->GetAccessControlManager(),
        /*requestWeight*/ 1);

    const auto transaction = TReadOnlyTransactionWrapper(
        Bootstrap_->GetTransactionManager(),
        request.TimestampOrTransactionId,
        context->GetTimeout(),
        request.CommonOptions.AllowFullScan)
        .Unwrap();

    request.QueryOptions.CheckReadPermissions = !IsSchemaReadAllowed(request.ObjectType);

    auto selectQueryExecutor = MakeSelectQueryExecutor(
        transaction,
        request.ObjectType,
        request.Filter,
        request.Selector,
        request.OrderBy,
        request.QueryOptions,
        request.Index);
    auto result = selectQueryExecutor->ExecuteConsuming(consumerGroup);

    if (request.QueryOptions.Limit) {
        // This may happen with master-side filtering. Currently, only limited read does this.
        // Limited read performance is somewhat suboptimal.
        while (!result.LimitSatisfied && consumerGroup->TotalConsumers() < *request.QueryOptions.Limit) {
            // We can't set |Limit| to the remaining space: if the filtered rows are sparse, we'd
            // end up with a full scan row by single row. So, scan original |Limit| batches, but
            // trim the results (and set up the continuation token) to this |MasterSideLimit|.
            selectQueryExecutor->PatchOptions(
                /*continuationToken*/ result.ContinuationToken,
                /*masterSideLimit*/ *request.QueryOptions.Limit - consumerGroup->TotalConsumers());

            auto subresult = selectQueryExecutor->ExecuteConsuming(consumerGroup);

            result.ContinuationToken = subresult.ContinuationToken;
            result.LimitSatisfied = subresult.LimitSatisfied;

            if (request.QueryOptions.EnablePartialResult && selectQueryExecutor->CheckRequestRemainingTimeoutExpired()) {
                break;
            }
        }
    }

    return {
        .CommonReadResult = GetCommonReadResult(transaction, request.CommonOptions.CollectStatistics),
        .QueryResult = std::move(result),
    };
}

TAggregateObjectsResponse TRequestHandler::AggregateObjects(
    const NRpc::IServiceContextPtr& context,
    TAggregateObjectsRequest&& request) const
{
    const auto authUserGuard = NAccessControl::MakeAuthenticatedUserGuardAndThrottle(
        context,
        Bootstrap_->GetAccessControlManager(),
        /*requestWeight*/ 1);

    if (!IsSchemaReadAllowed(request.ObjectType)) {
        THROW_ERROR_EXCEPTION(NClient::EErrorCode::AuthorizationError,
            "Current user does not have permission to read objects of type %v",
            NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(request.ObjectType));
    }

    const auto transaction = TReadOnlyTransactionWrapper(
        Bootstrap_->GetTransactionManager(),
        request.TimestampOrTransactionId,
        context->GetTimeout(),
        request.CommonOptions.AllowFullScan)
        .Unwrap();

    auto aggregateQueryExecutor = MakeAggregateQueryExecutor(transaction);
    auto result = aggregateQueryExecutor->ExecuteAggregateQuery(
        request.ObjectType,
        request.Filter,
        request.AggregateExpressions,
        request.GroupByExpressions,
        request.FetchFinalizingObjects);

    TAttributeSelector selector;
    for (const auto& attribute : request.GroupByExpressions.Expressions) {
        selector.Paths.push_back(attribute);
    }
    for (const auto& attribute : request.AggregateExpressions.Expressions) {
        selector.Paths.push_back(attribute);
    }

    return {
        .CommonReadResult = GetCommonReadResult(transaction, request.CommonOptions.CollectStatistics),
        .QueryResult = std::move(result),
        .Selector = std::move(selector),
    };
}

TSelectObjectHistoryResponse TRequestHandler::SelectObjectHistory(
    const NRpc::IServiceContextPtr& context,
    const TAttributeSelector& selector,
    const std::optional<TAttributeSelector>& distinctBy,
    TSelectObjectHistoryRequest&& request) const
{
    const auto authUserGuard = NAccessControl::MakeAuthenticatedUserGuardAndThrottle(
        context,
        Bootstrap_->GetAccessControlManager(),
        /*requestWeight*/ 1);

    const auto transaction = TReadOnlyTransactionWrapper(
        Bootstrap_->GetTransactionManager(),
        {NullTimestamp},
        context->GetTimeout(),
        request.CommonOptions.AllowFullScan)
        .Unwrap();

    EnforceObjectHistoryReadPermission(transaction, selector, request);

    auto selectObjectHistoryExecutor = MakeSelectObjectHistoryExecutor(
        transaction,
        request.ObjectType,
        request.Match.Key,
        selector,
        distinctBy,
        std::move(request.ContinuationToken),
        request.SelectOptions);
    auto result = std::move(*selectObjectHistoryExecutor).Execute();
    return {
        .CommonReadResult = GetCommonReadResult(transaction, request.CommonOptions.CollectStatistics),
        .QueryResult = std::move(result),
    };
}

TWatchLogInfoResponse TRequestHandler::GetWatchLogInfo(
    const NRpc::IServiceContextPtr& context,
    TObjectTypeValue objectType,
    const TString& logName) const
{
    const auto authUserGuard = MakeAuthenticatedUserGuardAndThrottle(
        context,
        Bootstrap_->GetAccessControlManager(),
        /*requestWeight*/ 1);

    int tabletCount = WaitFor(Bootstrap_->GetWatchManager()->GetTabletCount(objectType, logName))
        .ValueOrThrow();

    return {
        .TabletCount = tabletCount,
    };
}

void TRequestHandler::AdvanceWatchLogConsumer(
    const NRpc::IServiceContextPtr& context,
    TObjectTypeValue objectType,
    const TString& consumer,
    const TWatchObjectsContinuationToken& continuationToken) const
{
    const auto authUserGuard = NAccessControl::MakeAuthenticatedUserGuardAndThrottle(
        context,
        Bootstrap_->GetAccessControlManager(),
        /*requestWeight*/ 1);

    const auto& watchManager = Bootstrap_->GetWatchManager();
    const auto* watchLog = watchManager->GetLogTableOrThrow(objectType, continuationToken.WatchLog);
    const auto transaction = WaitFor(Bootstrap_->GetTransactionManager()->StartReadWriteTransaction())
        .ValueOrThrow();
    {
        auto storeContext = transaction->CreateStoreContext();
        for (const auto& offset : continuationToken.EventOffsets) {
            storeContext->AdvanceConsumer(consumer,
                watchLog,
                watchManager->GetConfig()->QueuesCluster,
                offset.Tablet,
                /*oldOffset*/ std::nullopt,
                offset.Row);
        }
        storeContext->FillTransaction();
    }
    WaitFor(transaction->Commit())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

bool TRequestHandler::IsSchemaReadAllowed(TObjectTypeValue type) const
{
    if (!EnforceReadPermissions_) {
        return true;
    }

    // Schemas themselves are readable. The object schema schema does not exist. So meta.
    if (type == TObjectTypeValues::Schema) {
        return true;
    }

    const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
    const auto result = accessControlManager->CheckCachedPermission(
        NAccessControl::GetAuthenticatedUserIdentity().User,
        TObjectTypeValues::Schema,
        TObjectKey(NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrThrow(type)),
        NAccessControl::TAccessControlPermissionValues::Read);

    return result.Action == NAccessControl::EAccessControlAction::Allow;
}

bool TRequestHandler::IsSchemaLimitedReadAllowed(TObjectTypeValue type) const
{
    // This checks specifically for limited read.
    // The object schema schema does not exist and has no limited read permissions.
    if (type == TObjectTypeValues::Schema) {
        return false;
    }

    const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
    const auto result = accessControlManager->CheckCachedPermission(
        NAccessControl::GetAuthenticatedUserIdentity().User,
        TObjectTypeValues::Schema,
        TObjectKey(NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrThrow(type)),
        NAccessControl::TAccessControlPermissionValues::LimitedRead);

    return result.Action == NAccessControl::EAccessControlAction::Allow;
}

void TRequestHandler::EnforceObjectHistoryReadPermission(
    const TTransactionPtr& transaction,
    const TAttributeSelector& attributeSelector,
    TSelectObjectHistoryRequest& request) const
{
    if (IsSchemaReadAllowed(request.ObjectType)) { // Quick accept and config check.
        return;
    }

    // The user does not have permission to read the object type. The history of the currently
    // existing object may be examined if the proper UUID is provided and there is a suitable
    // object-level permission. If there is a limited read permission, a missing UUID will be
    // substituted automatically.
    auto* object = transaction->GetObject(request.ObjectType, request.Match.Key);
    object->ScheduleUuidLoad();
    object->ScheduleAccessControlLoad();

    auto& uuid = request.SelectOptions.Uuid;
    bool uuidMatched = false;
    if (uuid.empty()) {
        if (IsSchemaLimitedReadAllowed(request.ObjectType)) {
            uuid = object->GetUuid();
            uuidMatched = true;
        }
    } else if (object->GetUuid() == uuid) {
        uuidMatched = true;
    }

    THROW_ERROR_EXCEPTION_UNLESS(uuidMatched,
        NClient::EErrorCode::AuthorizationError,
        "Current user does not have the broad permission to read %Qv objects; "
        "try the request with a |/meta/uuid| of an existing object where you have a read permissions",
        NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(request.ObjectType));

    for (const auto& path : attributeSelector.Paths) {
        Bootstrap_->GetAccessControlManager()->ValidatePermission(
            object,
            NAccessControl::TAccessControlPermissionValues::Read,
            path);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
