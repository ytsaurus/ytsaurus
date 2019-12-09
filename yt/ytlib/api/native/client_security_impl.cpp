#include "client_impl.h"

#include <yt/client/object_client/helpers.h>

#include <yt/client/security_client/helpers.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/ytlib/security_client/group_ypath_proxy.h>
#include <yt/client/security_client/acl.h>

#include <yt/ytlib/scheduler/helpers.h>

#include <yt/ytlib/scheduler/proto/job.pb.h>

namespace NYT::NApi::NNative {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NObjectClient;
using namespace NTabletClient;
using namespace NSecurityClient;

////////////////////////////////////////////////////////////////////////////////

TCheckPermissionByAclResult TClient::DoCheckPermissionByAcl(
    const std::optional<TString>& user,
    EPermission permission,
    const INodePtr& acl,
    const TCheckPermissionByAclOptions& options)
{
    auto proxy = CreateReadProxy<TObjectServiceProxy>(options);
    auto batchReq = proxy->ExecuteBatch();
    SetBalancingHeader(batchReq, options);

    auto req = TMasterYPathProxy::CheckPermissionByAcl();
    if (user) {
        req->set_user(*user);
    }
    req->set_permission(static_cast<int>(permission));
    req->set_acl(ConvertToYsonString(acl).GetData());
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
    auto proxy = CreateWriteProxy<TObjectServiceProxy>();
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
    auto proxy = CreateWriteProxy<TObjectServiceProxy>();
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
    auto proxy = CreateReadProxy<TObjectServiceProxy>(options);
    auto batchReq = proxy->ExecuteBatch();
    SetBalancingHeader(batchReq, options);

    auto req = TObjectYPathProxy::CheckPermission(path);
    req->set_user(user);
    req->set_permission(static_cast<int>(permission));
    if (options.Columns) {
        ToProto(req->mutable_columns()->mutable_items(), *options.Columns);
    }
    SetTransactionId(req, options, true);
    SetCachingHeader(req, options);
    NCypressClient::SetSuppressAccessTracking(req, true);
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

TCheckPermissionResult TClient::InternalCheckPermission(
    const TYPath& path,
    EPermission permission,
    const TCheckPermissionOptions& options)
{
    // TODO(babenko): consider passing proper timeout
    const auto& user = Options_.GetUser();
    return DoCheckPermission(user, path, permission, options);
}

void TClient::InternalValidatePermission(
    const TYPath& path,
    EPermission permission,
    const TCheckPermissionOptions& options)
{
    // TODO(babenko): consider passing proper timeout
    const auto& user = Options_.GetUser();
    DoCheckPermission(user, path, permission, options)
        .ToError(user, permission)
        .ThrowOnError();
}

void TClient::InternalValidateTableReplicaPermission(
    TTableReplicaId replicaId,
    EPermission permission,
    const TCheckPermissionOptions& options)
{
    // TODO(babenko): consider passing proper timeout
    auto tablePathYson = WaitFor(GetNode(FromObjectId(replicaId) + "/@table_path", {}))
        .ValueOrThrow();
    auto tablePath = ConvertTo<TYPath>(tablePathYson);
    InternalValidatePermission(tablePath, permission, options);
}

void TClient::ValidateOperationAccess(
    TJobId jobId,
    const NJobTrackerClient::NProto::TJobSpec& jobSpec,
    EPermissionSet permissions)
{
    const auto extensionId = NScheduler::NProto::TSchedulerJobSpecExt::scheduler_job_spec_ext;
    TSerializableAccessControlList acl;
    if (jobSpec.HasExtension(extensionId) && jobSpec.GetExtension(extensionId).has_acl()) {
        TYsonString aclYson(jobSpec.GetExtension(extensionId).acl());
        acl = ConvertTo<TSerializableAccessControlList>(aclYson);
    } else {
        // We check against an empty ACL to allow only "superusers" and "root" access.
        YT_LOG_WARNING(
            "Job spec has no sheduler_job_spec_ext or the extension has no ACL, "
            "validating against empty ACL (JobId: %v)",
            jobId);
    }

    NScheduler::ValidateOperationAccess(
        /* user */ std::nullopt,
        TOperationId(),
        jobId,
        permissions,
        acl,
        this,
        Logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
