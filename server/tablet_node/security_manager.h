#pragma once

#include "public.h"
#include "config.h"

#include <yt/server/cell_node/public.h>

#include <yt/server/security_server/security_manager_base.h>

#include <yt/core/actions/future.h>

#include <yt/core/misc/nullable.h>

#include <yt/core/ytree/permission.h>

namespace NYT {
namespace NTabletNode {

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
    TAuthenticatedUserGuard(TSecurityManagerPtr securityManager, const TNullable<TString>& maybeUser);
};

////////////////////////////////////////////////////////////////////////////////

class TSecurityManager
    : public NSecurityServer::ISecurityManager
{
public:
    TSecurityManager(
        TSecurityManagerConfigPtr config,
        NCellNode::TBootstrap* bootstrap);
    ~TSecurityManager();

    virtual void SetAuthenticatedUserByNameOrThrow(const TString& user) override;
    virtual void ResetAuthenticatedUser() override;
    virtual TNullable<TString> GetAuthenticatedUserName() override;

    TFuture<void> CheckPermission(
        const TTabletSnapshotPtr& tabletSnapshot,
        NYTree::EPermission permission);

    void ValidatePermission(
        const TTabletSnapshotPtr& tabletSnapshot,
        NYTree::EPermission permission);

    TFuture<void> CheckResourceLimits(
        const TString& account,
        const TString& mediumName,
        NTabletClient::EInMemoryMode inMemoryMode);

    void ValidateResourceLimits(
        const TString& account,
        const TString& mediumName,
        NTabletClient::EInMemoryMode inMemoryMode);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TSecurityManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
