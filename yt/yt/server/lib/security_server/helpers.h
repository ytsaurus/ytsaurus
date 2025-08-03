#pragma once

#include "public.h"

#include <yt/yt/core/ytree/permission.h>

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

std::string FormatPermissionValidationTargetName(
    const TPermissionCheckTarget& target,
    const std::string& objectName);

// Logs an authorization denial event and throws an appropriate error.
void LogAndThrowAuthorizationError(
    const NLogging::TLogger& logger,
    const TPermissionCheckTarget& target,
    const TPermissionCheckResult& result,
    NYTree::EPermission permission,
    const std::string& userName,
    const std::string& targetObjectName,
    const NYPath::TYPath& targetObjectPath,
    const std::string& resultObjectName,
    const std::string& resultSubjectName);

// Enriches and throws an authorization error.
void ThrowAuthorizationError(
    TError error,
    const TPermissionCheckTarget &target,
    NYTree::EPermission permission,
    const std::string &userName);

std::optional<NSecurityClient::EAceInheritanceMode> GetInheritedInheritanceMode(
    NSecurityClient::EAceInheritanceMode mode,
    int depth);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
