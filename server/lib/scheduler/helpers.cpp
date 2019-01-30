#include "helpers.h"

#include <yt/ytlib/api/native/client.h>

#include <yt/client/object_client/helpers.h>

namespace NYT::NScheduler {

using namespace NSecurityClient;
using namespace NObjectClient;
using namespace NNodeTrackerClient;
using namespace NConcurrency;
using namespace NYTree;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

TJobId GenerateJobId(TCellTag tag, TNodeId nodeId)
{
    return MakeId(
        EObjectType::SchedulerJob,
        tag,
        RandomNumber<ui64>(),
        nodeId);
}

TNodeId NodeIdFromJobId(TJobId jobId)
{
    return jobId.Parts32[0];
}

////////////////////////////////////////////////////////////////////////////////

namespace {

EPermission GetPermission(EAccessType accessType)
{
    // Logic of transforming access type to permissions should not break the existing behavior.
    switch (accessType) {
        case EAccessType::Ownership:
            return EPermission::Write;
        case EAccessType::IntermediateData:
            return EPermission::Read;
        default:
            Y_UNREACHABLE();
    }
}

} // namespace

void ValidateOperationAccess(
    const TString& user,
    TOperationId operationId,
    EAccessType accessType,
    const INodePtr& acl,
    const NApi::NNative::IClientPtr& client,
    const NLogging::TLogger& logger)
{
    const auto& Logger = logger;
    TCheckPermissionByAclOptions options;
    options.IgnoreMissingSubjects = true;
    auto asyncResult = client->CheckPermissionByAcl(
        user,
        GetPermission(accessType),
        acl,
        options);
    auto result = WaitFor(asyncResult)
        .ValueOrThrow();

    if (!result.MissingSubjects.empty()) {
        YT_LOG_DEBUG("Operation has missing subjects in ACL (OperationId: %v, MissingSubjects: %v)",
            operationId,
            result.MissingSubjects);
    }

    if (result.Action == ESecurityAction::Allow) {
        YT_LOG_DEBUG("Operation access successfully validated (OperationId: %v, User: %v, AccessType: %v)",
            operationId,
            user,
            accessType);
    } else {
        THROW_ERROR_EXCEPTION(
            NSecurityClient::EErrorCode::AuthorizationError,
            "Access is denied")
            << TErrorAttribute("user", user)
            << TErrorAttribute("access_type", accessType)
            << TErrorAttribute("operation_id", operationId);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
