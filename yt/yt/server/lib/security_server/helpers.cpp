#include "helpers.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/logging/fluent_log.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NSecurityServer {

using namespace NLogging;
using namespace NObjectClient;
using namespace NSecurityClient;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

std::string FormatPermissionValidationTargetName(
    const TPermissionCheckTarget& target,
    const std::string& objectName)
{
    if (target.Column) {
        return Format("column %Qv of %v",
            *target.Column,
            objectName);
    } else {
        return objectName;
    }
}

void LogAndThrowAuthorizationError(
    const TLogger& logger,
    const TPermissionCheckTarget& target,
    const TPermissionCheckResult& result,
    EPermission permission,
    const std::string& userName,
    const std::string& targetObjectName,
    const TYPath& targetObjectPath,
    const std::string& resultObjectName,
    const std::string& resultSubjectName)
{
    YT_ASSERT(result.Action == ESecurityAction::Deny);

    TError error;

    auto event = LogStructuredEventFluently(logger, ELogLevel::Info)
        .Item("event").Value(EAccessControlEvent::AccessDenied)
        .Item("user").Value(userName)
        .Item("permission").Value(permission)
        .Item("object_name").Value(targetObjectName);

    if (target.Column) {
        event = event
            .Item("object_column").Value(*target.Column);
    }

    if (result.ObjectId && result.SubjectId) {
        error = TError(
            NSecurityClient::EErrorCode::AuthorizationError,
            "Access denied for user %Qv: %Qlv permission for %v is denied for %Qv by ACE at %v",
            userName,
            permission,
            targetObjectName,
            resultSubjectName,
            resultObjectName)
            << TErrorAttribute("denied_by", result.ObjectId)
            << TErrorAttribute("denied_for", result.SubjectId);

        event
            .Item("reason").Value(EAccessDenialReason::DeniedByAce)
            .Item("denied_for").Value(resultSubjectName)
            .Item("denied_by").Value(resultObjectName);
    } else {
        error = TError(
            NSecurityClient::EErrorCode::AuthorizationError,
            "Access denied for user %Qv: %Qlv permission for %v is not allowed by any matching ACE",
            userName,
            permission,
            targetObjectName);

        event
            .Item("reason").Value(EAccessDenialReason::NoAllowingAce);
    }

    if (!targetObjectPath.empty()) {
        error = error << TErrorAttribute("path", targetObjectPath);
    }

    ThrowAuthorizationError(std::move(error), target, permission, userName);
}

void ThrowAuthorizationError(
    TError error,
    const TPermissionCheckTarget &target,
    NYTree::EPermission permission,
    const std::string &userName)
{
    error <<= TErrorAttribute("permission", permission);
    error <<= TErrorAttribute("user", userName);
    error <<= TErrorAttribute("object_id", target.ObjectId);
    if (target.Column) {
        error <<= TErrorAttribute("object_column", target.Column);
    }
    error <<= TErrorAttribute("object_type", TypeFromId(target.ObjectId));

    THROW_ERROR(error);
}

std::optional<EAceInheritanceMode> GetInheritedInheritanceMode(EAceInheritanceMode mode, int depth)
{
    auto nothing = std::optional<EAceInheritanceMode>();
    switch (mode) {
        case EAceInheritanceMode::ObjectAndDescendants:
            return EAceInheritanceMode::ObjectAndDescendants;
        case EAceInheritanceMode::ObjectOnly:
            return (depth == 0 ? EAceInheritanceMode::ObjectOnly : nothing);
        case EAceInheritanceMode::DescendantsOnly:
            return (depth > 0 ? EAceInheritanceMode::ObjectAndDescendants : nothing);
        case EAceInheritanceMode::ImmediateDescendantsOnly:
            return (depth == 1 ? EAceInheritanceMode::ObjectOnly : nothing);
        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
