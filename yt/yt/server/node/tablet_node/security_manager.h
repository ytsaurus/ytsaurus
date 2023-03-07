#pragma once

#include "public.h"

#include <yt/server/node/cluster_node/public.h>

#include <yt/server/lib/security_server/security_manager.h>

#include <yt/core/actions/future.h>

#include <yt/core/misc/optional.h>

#include <yt/core/ytree/permission.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! A simple RAII guard for setting the authenticated user.
/*!
 *  \see #TSecurityManager::SetAuthenticatedUser
 *  \see #TSecurityManager::ResetAuthenticatedUser
 */
class TAuthenticatedUserGuard
    : public NSecurityServer::TAuthenticatedUserGuardBase
{
public:
    TAuthenticatedUserGuard(TSecurityManagerPtr securityManager, const std::optional<TString>& optionalUser);
};

////////////////////////////////////////////////////////////////////////////////

class TSecurityManager
    : public NSecurityServer::ISecurityManager
    , public NSecurityServer::IResourceLimitsManager
{
public:
    TSecurityManager(
        TSecurityManagerConfigPtr config,
        NClusterNode::TBootstrap* bootstrap);
    ~TSecurityManager();

    virtual void SetAuthenticatedUserByNameOrThrow(const TString& user) override;
    virtual void ResetAuthenticatedUser() override;
    virtual std::optional<TString> GetAuthenticatedUserName() override;

    TFuture<void> CheckResourceLimits(
        const TString& account,
        const TString& mediumName,
        NTabletClient::EInMemoryMode inMemoryMode);

    virtual void ValidateResourceLimits(
        const TString& account,
        const TString& mediumName,
        NTabletClient::EInMemoryMode inMemoryMode) override;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TSecurityManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
