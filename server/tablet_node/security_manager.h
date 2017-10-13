#pragma once

#include "public.h"
#include "config.h"

#include <yt/server/cell_node/public.h>

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
    : private TNonCopyable
{
public:
    TAuthenticatedUserGuard(TSecurityManagerPtr securityManager, const TNullable<TString>& maybeUser);
    ~TAuthenticatedUserGuard();

private:
    const TSecurityManagerPtr SecurityManager_;
    const bool IsNull_;

};

////////////////////////////////////////////////////////////////////////////////

class TSecurityManager
    : public TRefCounted
{
public:
    TSecurityManager(
        TSecurityManagerConfigPtr config,
        NCellNode::TBootstrap* bootstrap);
    ~TSecurityManager();

    void SetAuthenticatedUser(const TString& user);
    void ResetAuthenticatedUser();
    TNullable<TString> GetAuthenticatedUser();

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
