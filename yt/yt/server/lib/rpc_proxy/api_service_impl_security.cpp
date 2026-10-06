#include "api_service_impl.h"

namespace NYT::NRpcProxy {

using namespace NApi::NRpcProxy;
using namespace NApi;
using namespace NConcurrency;
using namespace NRpc;
using namespace NSecurityClient;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void TApiService::RegisterSecurityMethods(TMultiproxyMethodList* methodList)
{
    auto registerMethod = [&] (EMultiproxyMethodKind methodKind, TMethodDescriptor&& descriptor) {
        RegisterMethodForMultiproxy(methodList, methodKind, descriptor);
    };

    registerMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(GetCurrentUser));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(AddMember));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(RemoveMember));
    registerMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(CheckPermission));
    registerMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(CheckPermissionByAcl));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(TransferAccountResources));
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TApiService, GetCurrentUser)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    context->AnnotateRequest();

    ExecuteCall(
        context,
        [=] {
            return client->GetCurrentUser();
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            response->set_user(result.User);

            context->AnnotateResponse()
                .With("User", result.User);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, AddMember)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto group = request->group();
    auto member = request->member();

    TAddMemberOptions options;
    SetTimeoutOptions(&options, context.Get());
    SetMutatingOptions(&options, request, context.Get());
    if (request->has_prerequisite_options()) {
        FromProto(&options, request->prerequisite_options());
    }

    context->AnnotateRequest()
        .With("Group", group)
        .With("Member", member)
        .With("MutationId", options.MutationId)
        .With("Retry", options.Retry);

    ExecuteCall(
        context,
        [=] {
            return client->AddMember(group, member, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, RemoveMember)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto group = request->group();
    auto member = request->member();

    TRemoveMemberOptions options;
    SetTimeoutOptions(&options, context.Get());
    SetMutatingOptions(&options, request, context.Get());
    if (request->has_prerequisite_options()) {
        FromProto(&options, request->prerequisite_options());
    }

    context->AnnotateRequest()
        .With("Group", group)
        .With("Member", member)
        .With("MutationId", options.MutationId)
        .With("Retry", options.Retry);

    ExecuteCall(
        context,
        [=] {
            return client->RemoveMember(group, member, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, CheckPermission)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& user = request->user();
    const auto& path = request->path();
    auto permission = FromProto<EPermission>(request->permission());

    TCheckPermissionOptions options;
    if (request->has_columns()) {
        options.Columns = FromProto<std::vector<std::string>>(request->columns().items());
    }
    if (request->has_vital()) {
        options.Vital = request->vital();
    }
    SetTimeoutOptions(&options, context.Get());
    if (request->has_master_read_options()) {
        FromProto(&options, request->master_read_options());
    }
    if (request->has_transactional_options()) {
        FromProto(&options, request->transactional_options());
    }
    if (request->has_prerequisite_options()) {
        FromProto(&options, request->prerequisite_options());
    }

    context->AnnotateRequest()
        .With("User", user)
        .With("Path", path)
        .With("Permission", FormatPermissions(permission));

    ExecuteCall(
        context,
        [=] {
            return client->CheckPermission(user, path, permission, options);
        },
        [] (const auto& context, const auto& checkResponse) {
            auto* response = &context->Response();
            ToProto(response->mutable_result(), checkResponse);
            if (checkResponse.Columns) {
                ToProto(response->mutable_columns()->mutable_items(), *checkResponse.Columns);
            }

            context->AnnotateResponse()
                .With("Action", checkResponse.Action);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, CheckPermissionByAcl)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    std::optional<std::string> user;
    if (request->has_user()) {
        user = request->user();
    }
    auto permission = FromProto<EPermission>(request->permission());
    auto acl = ConvertToNode(TYsonString(request->acl()));

    TCheckPermissionByAclOptions options;
    SetTimeoutOptions(&options, context.Get());
    if (request->has_master_read_options()) {
        FromProto(&options, request->master_read_options());
    }
    if (request->has_prerequisite_options()) {
        FromProto(&options, request->prerequisite_options());
    }

    options.IgnoreMissingSubjects = request->ignore_missing_subjects();

    context->AnnotateRequest()
        .With("User", user)
        .With("Permission", FormatPermissions(permission));

    ExecuteCall(
        context,
        [=] {
            return client->CheckPermissionByAcl(user, permission, acl, options);
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            ToProto(response->mutable_result(), result);

            context->AnnotateResponse()
                .With("Action", result.Action);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, TransferAccountResources)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto srcAccount = request->src_account();
    auto dstAccount = request->dst_account();
    auto resourceDelta = ConvertToNode(TYsonString(request->resource_delta()));

    TTransferAccountResourcesOptions options;
    SetTimeoutOptions(&options, context.Get());
    SetMutatingOptions(&options, request, context.Get());

    context->AnnotateRequest()
        .With("SrcAccount", srcAccount)
        .With("DstAccount", dstAccount);

    ExecuteCall(
        context,
        [=] {
            return client->TransferAccountResources(srcAccount, dstAccount, resourceDelta, options);
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
