#include "client_impl.h"

#include "ypath_helpers.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/security_client/helpers.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>
#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/security_client/account_ypath_proxy.h>
#include <yt/yt/ytlib/security_client/group_ypath_proxy.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/client/security_client/acl.h>

#include <yt/yt/core/ypath/tokenizer.h>

namespace NYT::NApi::NNative {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NTabletClient;
using namespace NSecurityClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TCheckPermissionByAclResult TClient::DoCheckPermissionByAcl(
    const std::optional<TString>& user,
    EPermission permission,
    const INodePtr& acl,
    const TCheckPermissionByAclOptions& options)
{
    auto proxy = CreateObjectServiceReadProxy(options);
    auto batchReq = proxy->ExecuteBatch();
    SetBalancingHeader(batchReq, options);
    batchReq->SetSuppressTransactionCoordinatorSync(true);

    auto req = TMasterYPathProxy::CheckPermissionByAcl();
    if (user) {
        req->set_user(*user);
    }
    req->set_permission(static_cast<int>(permission));
    req->set_acl(ConvertToYsonString(acl).ToString());
    req->set_ignore_missing_subjects(options.IgnoreMissingSubjects);
    SetCachingHeader(req, options);

    batchReq->AddRequest(req);

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();
    auto rsp = batchRsp->GetResponse<TMasterYPathProxy::TRspCheckPermissionByAcl>(0)
        .ValueOrThrow();

    TCheckPermissionByAclResult result;
    result.Action = ESecurityAction(rsp->action());
    result.SubjectId = FromProto<TSubjectId>(rsp->subject_id());
    result.SubjectName = rsp->has_subject_name() ? std::make_optional(rsp->subject_name()) : std::nullopt;
    result.MissingSubjects = FromProto<std::vector<TString>>(rsp->missing_subjects());
    return result;
}

void TClient::DoAddMember(
    const TString& group,
    const TString& member,
    const TAddMemberOptions& options)
{
    auto proxy = CreateObjectServiceWriteProxy();
    auto batchReq = proxy->ExecuteBatch();
    SetPrerequisites(batchReq, options);

    auto req = TGroupYPathProxy::AddMember(GetGroupPath(group));
    req->set_name(member);
    SetMutationId(req, options);

    batchReq->AddRequest(req);

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();
    batchRsp->GetResponse<TGroupYPathProxy::TRspAddMember>(0)
        .ThrowOnError();
}

void TClient::DoRemoveMember(
    const TString& group,
    const TString& member,
    const TRemoveMemberOptions& options)
{
    auto proxy = CreateObjectServiceWriteProxy();
    auto batchReq = proxy->ExecuteBatch();
    SetPrerequisites(batchReq, options);

    auto req = TGroupYPathProxy::RemoveMember(GetGroupPath(group));
    req->set_name(member);
    SetMutationId(req, options);

    batchReq->AddRequest(req);

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();
    batchRsp->GetResponse<TGroupYPathProxy::TRspRemoveMember>(0)
        .ThrowOnError();
}

TCheckPermissionResponse TClient::DoCheckPermission(
    const TString& user,
    const TYPath& path,
    EPermission permission,
    const TCheckPermissionOptions& options)
{
    auto proxy = CreateObjectServiceReadProxy(options);
    auto batchReq = proxy->ExecuteBatch();
    batchReq->SetSuppressTransactionCoordinatorSync(options.SuppressTransactionCoordinatorSync);
    SetBalancingHeader(batchReq, options);

    auto req = TObjectYPathProxy::CheckPermission(path);
    req->set_user(user);
    req->set_permission(static_cast<int>(permission));
    if (options.Columns) {
        ToProto(req->mutable_columns()->mutable_items(), *options.Columns);
    }
    if (options.Vital) {
        req->set_vital(*options.Vital);
    }
    SetTransactionId(req, options, true);
    SetCachingHeader(req, options);
    NCypressClient::SetSuppressAccessTracking(req, true);
    NCypressClient::SetSuppressExpirationTimeoutRenewal(req, true);
    batchReq->AddRequest(req);

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();
    auto rsp = batchRsp->GetResponse<TObjectYPathProxy::TRspCheckPermission>(0)
        .ValueOrThrow();

    auto fillResult = [] (auto* result, const auto& protoResult) {
        result->Action = CheckedEnumCast<ESecurityAction>(protoResult.action());
        result->ObjectId = FromProto<TObjectId>(protoResult.object_id());
        result->ObjectName = protoResult.has_object_name() ? std::make_optional(protoResult.object_name()) : std::nullopt;
        result->SubjectId = FromProto<TSubjectId>(protoResult.subject_id());
        result->SubjectName = protoResult.has_subject_name() ? std::make_optional(protoResult.subject_name()) : std::nullopt;
    };

    TCheckPermissionResponse response;
    fillResult(&response, *rsp);
    if (rsp->has_columns()) {
        response.Columns.emplace();
        response.Columns->reserve(static_cast<size_t>(rsp->columns().items_size()));
        for (const auto& protoResult : rsp->columns().items()) {
            fillResult(&response.Columns->emplace_back(), protoResult);
        }
    }

    return response;
}

TCheckPermissionResult TClient::CheckPermissionImpl(
    const TYPath& path,
    EPermission permission,
    const TCheckPermissionOptions& options)
{
    // TODO(babenko): consider passing proper timeout
    const auto& user = Options_.GetAuthenticatedUser();
    return DoCheckPermission(user, path, permission, options);
}

void TClient::ValidatePermissionImpl(
    const TYPath& path,
    EPermission permission,
    const TCheckPermissionOptions& options)
{
    // TODO(babenko): consider passing proper timeout
    const auto& user = Options_.GetAuthenticatedUser();
    DoCheckPermission(user, path, permission, options)
        .ToError(user, permission)
        .ThrowOnError();
}

void TClient::MaybeValidateExternalObjectPermission(
    const TYPath& path,
    EPermission permission,
    const TCheckPermissionOptions& options)
{
    TObjectId objectId;
    if (!TryParseObjectId(path, &objectId)) {
        return;
    }

    switch (TypeFromId(objectId)) {
        case EObjectType::TableReplica:
            ValidateTableReplicaPermission(objectId, permission, options);
            break;

        default:
            break;
    }
}

TYPath TClient::GetReplicaTablePath(TTableReplicaId replicaId)
{
    auto cellTag = CellTagFromId(replicaId);
    auto proxy = CreateObjectServiceReadProxy({}, cellTag);
    auto batchReq = proxy->ExecuteBatch();

    auto req = TYPathProxy::Get(FromObjectId(replicaId) + "/@table_path");
    NCypressClient::SetSuppressAccessTracking(req, true);
    NCypressClient::SetSuppressExpirationTimeoutRenewal(req, true);
    batchReq->AddRequest(req);

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();
    auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>(0)
        .ValueOrThrow();

    return ConvertTo<TYPath>(TYsonString(rsp->value()));
}

void TClient::ValidateTableReplicaPermission(
    TTableReplicaId replicaId,
    EPermission permission,
    const TCheckPermissionOptions& options)
{
    // TODO(babenko): consider passing proper timeout
    auto tablePath = GetReplicaTablePath(replicaId);
    ValidatePermissionImpl(tablePath, permission, options);
}

void TClient::DoTransferAccountResources(
    const TString& srcAccount,
    const TString& dstAccount,
    NYTree::INodePtr resourceDelta,
    const TTransferAccountResourcesOptions& options)
{
    auto proxy = CreateObjectServiceWriteProxy();
    auto batchReq = proxy->ExecuteBatch();

    auto req = TAccountYPathProxy::TransferAccountResources(GetAccountPath(dstAccount));
    req->set_src_account(srcAccount);
    req->set_resource_delta(ConvertToYsonString(resourceDelta).ToString());
    SetMutationId(req, options);

    batchReq->AddRequest(req);

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();
    batchRsp->GetResponse<TAccountYPathProxy::TRspTransferAccountResources>(0)
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
