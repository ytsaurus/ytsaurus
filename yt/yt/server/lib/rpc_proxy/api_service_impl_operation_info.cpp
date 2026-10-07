#include "api_service_impl.h"

#include <yt/yt/client/scheduler/operation_id_or_alias.h>

#include <library/cpp/yt/misc/cast.h>

namespace NYT::NRpcProxy {

using namespace NApi::NRpcProxy;
using namespace NApi;
using namespace NConcurrency;
using namespace NRpc;
using namespace NScheduler;
using namespace NSecurityClient;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void TApiService::RegisterOperationInfoMethods(TMultiproxyMethodList* methodList)
{
    auto registerMethod = [&] (EMultiproxyMethodKind methodKind, TMethodDescriptor&& descriptor) {
        RegisterMethodForMultiproxy(methodList, methodKind, descriptor);
    };

    registerMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(GetOperation));
    registerMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(ListOperations));
    registerMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(ListOperationEvents));
    registerMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(CheckOperationPermission));
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TApiService, GetOperation)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

    TGetOperationOptions options;
    SetTimeoutOptions(&options, context.Get());

    if (request->has_master_read_options()) {
        FromProto(&options, request->master_read_options());
    }
    if (request->has_attributes()) {
        options.Attributes.emplace();
        NYT::CheckedHashSetFromProto(&(*options.Attributes), request->attributes().keys());
    } else if (request->legacy_attributes_size() != 0) {
        // COMPAT(max42): remove when no clients older than Aug22 are there.
        options.Attributes.emplace();
        NYT::CheckedHashSetFromProto(&(*options.Attributes), request->legacy_attributes());
    }
    options.IncludeRuntime = request->include_runtime();
    if (request->has_maximum_cypress_progress_age()) {
        options.MaximumCypressProgressAge = FromProto<TDuration>(request->maximum_cypress_progress_age());
    }

    context->AnnotateRequest()
        .With("OperationId", operationIdOrAlias)
        .With("IncludeRuntime", options.IncludeRuntime)
        .With("Attributes", options.Attributes);

    ExecuteCall(
        context,
        [=] {
            return client->GetOperation(operationIdOrAlias, options);
        },
        [] (const auto& context, const auto& operation) {
            auto* response = &context->Response();
            response->set_meta(ToProto(ConvertToYsonString(operation)));
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, ListOperationEvents)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);

    TListOperationEventsOptions options;
    SetTimeoutOptions(&options, context.Get());

    if (request->has_event_type()) {
        options.EventType = NApi::NRpcProxy::NProto::ConvertOperationEventTypeFromProto(request->event_type());
    }

    options.Limit = request->limit();

    context->AnnotateRequest()
        .With("OperationIdOrAlias", operationIdOrAlias);

    ExecuteCall(
        context,
        [=] {
            return client->ListOperationEvents(operationIdOrAlias, options);
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            ToProto(response->mutable_events(), result);

            context->AnnotateResponse()
                .With("EventCount", result.size());
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, ListOperations)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    TListOperationsOptions options;
    SetTimeoutOptions(&options, context.Get());

    if (request->has_master_read_options()) {
        FromProto(&options, request->master_read_options());
    }

    if (request->has_from_time()) {
        options.FromTime = FromProto<TInstant>(request->from_time());
    }
    if (request->has_to_time()) {
        options.ToTime = FromProto<TInstant>(request->to_time());
    }
    if (request->has_cursor_time()) {
        options.CursorTime = FromProto<TInstant>(request->cursor_time());
    }
    options.CursorDirection = FromProto<EOperationSortDirection>(request->cursor_direction());
    if (request->has_user_filter()) {
        options.UserFilter = request->user_filter();
    }

    if (request->has_access_filter()) {
        options.AccessFilter = ConvertTo<TListOperationsAccessFilterPtr>(TYsonString(request->access_filter()));
    }

    if (request->has_state_filter()) {
        options.StateFilter = NYT::NApi::NRpcProxy::NProto::ConvertOperationStateFromProto(
            request->state_filter());
    }
    if (request->has_type_filter()) {
        options.TypeFilter = NYT::NApi::NRpcProxy::NProto::ConvertOperationTypeFromProto(
            request->type_filter());
    }
    if (request->has_substr_filter()) {
        options.SubstrFilter = request->substr_filter();
    }
    if (request->has_pool()) {
        options.Pool = request->pool();
    }
    if (request->has_pool_tree()) {
        options.PoolTree = request->pool_tree();
    }
    if (request->has_with_failed_jobs()) {
        options.WithFailedJobs = request->with_failed_jobs();
    }

    options.ArchiveFetchingTimeout = FromProto<TDuration>(request->archive_fetching_timeout());

    options.IncludeArchive = request->include_archive();
    options.IncludeCounters = request->include_counters();
    options.Limit = request->limit();

    if (request->has_attributes()) {
        options.Attributes.emplace();
        FromProto(&(*options.Attributes), request->attributes().keys());
    } else if (request->has_legacy_attributes() && !request->legacy_attributes().all()) {
        // COMPAT(max42): remove when no clients older than Aug22 are there.
        options.Attributes.emplace();
        FromProto(&(*options.Attributes), request->legacy_attributes().keys());
    }

    options.EnableUIMode = request->enable_ui_mode();

    context->AnnotateRequest()
        .With("IncludeArchive", options.IncludeArchive)
        .With("FromTime", options.FromTime)
        .With("ToTime", options.ToTime)
        .With("CursorTime", options.CursorTime)
        .With("UserFilter", options.UserFilter)
        .With("AccessFilter", ConvertToYsonString(options.AccessFilter, EYsonFormat::Text))
        .With("StateFilter", options.StateFilter)
        .With("TypeFilter", options.TypeFilter)
        .With("SubstrFilter", options.SubstrFilter)
        .With("Attributes", options.Attributes);

    ExecuteCall(
        context,
        [=] {
            return client->ListOperations(options);
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            ToProto(response->mutable_result(), result);

            context->AnnotateResponse()
                .With("OperationsCount", result.Operations.size())
                .With("FailedJobsCount", result.FailedJobsCount)
                .With("Incomplete", result.Incomplete);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, CheckOperationPermission)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto user = request->user();
    auto operationIdOrAlias = FromProto<TOperationIdOrAlias>(*request);
    auto permission = CheckedEnumCast<NYTree::EPermission>(request->permission());

    TCheckOperationPermissionOptions options;
    SetTimeoutOptions(&options, context.Get());

    context->AnnotateRequest()
        .With("User", user)
        .With("OperationIdOrAlias", operationIdOrAlias)
        .With("Permission", permission);

    ExecuteCall(
        context,
        [=] {
            return client->CheckOperationPermission(user, operationIdOrAlias, permission, options);
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            ToProto(response->mutable_result(), result);

            context->AnnotateResponse()
                .With("Action", result.Action);
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
