#include "helpers.h"

#include "access_control_manager.h"
#include "private.h"
#include "subject_cluster.h"

#include <yt/yt/orm/client/objects/registry.h>

namespace NYT::NOrm::NServer::NAccessControl {

////////////////////////////////////////////////////////////////////////////////

bool ContainsPermission(
    const TAccessControlEntry& ace,
    TAccessControlPermissionValue permission)
{
    return std::find(
        ace.Permissions.begin(),
        ace.Permissions.end(),
        permission) != ace.Permissions.end();
}

////////////////////////////////////////////////////////////////////////////////

std::optional<std::string> GetUserExecutionPool(const TClusterSubjectSnapshotPtr& snapshot)
{
    YT_VERIFY(snapshot);

    auto userId = TryGetAuthenticatedUserIdentity().value_or(NRpc::GetRootAuthenticationIdentity()).User;
    auto userSnapshot = snapshot->FindSubject(userId);

    if (!userSnapshot) {
        THROW_ERROR_EXCEPTION(NClient::EErrorCode::AuthenticationError,
            "Authenticated user %Qv is not registered",
            userId);
    }
    if (userSnapshot->GetType() != TObjectTypeValues::User) {
        THROW_ERROR_EXCEPTION(NClient::EErrorCode::AuthenticationError,
            "Authenticated user %Qv is registered as %Qv",
            userId,
            NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(userSnapshot->GetType()));
    }
    const auto* user = userSnapshot->AsUser();
    YT_VERIFY(user);
    if (user->GetBanned()) {
        THROW_ERROR_EXCEPTION(NClient::EErrorCode::UserBanned,
            "Authenticated user %Qv is banned",
            userId);
    }

    return user->GetExecutionPool();
}

std::optional<std::string> SelectExecutionPoolTag(
    const NAccessControl::TAccessControlManagerPtr& accessControlManager,
    const std::string& identityTag,
    bool userTagInsteadOfPool)
{
    const auto snapshot = accessControlManager->TryGetClusterSubjectSnapshot();
    if (!snapshot) {
        if (userTagInsteadOfPool) {
            YT_LOG_DEBUG("Request execution pool is selected (ExecutionPool: %v, PoolSource: IdentityTag)", identityTag);
            return identityTag;
        }
        YT_LOG_DEBUG("Request execution pool is empty");
        return std::nullopt;
    }
    auto userExecutionPool = GetUserExecutionPool(snapshot);
    if (userExecutionPool) {
        YT_LOG_DEBUG("Request execution pool is selected (ExecutionPool: %v, PoolSource: UserExecutionPool)", *userExecutionPool);
        return userExecutionPool;
    } else if (userTagInsteadOfPool) {
        YT_LOG_DEBUG("Request execution pool is selected (ExecutionPool: %v, PoolSource: IdentityTag)", identityTag);
        return identityTag;
    }
    YT_LOG_DEBUG("Request execution pool is empty");
    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NAccessControl
