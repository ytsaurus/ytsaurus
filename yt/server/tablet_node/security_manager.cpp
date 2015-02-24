#include "stdafx.h"
#include "security_manager.h"
#include "tablet.h"
#include "config.h"
#include "private.h"

#include <core/concurrency/scheduler.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NTabletNode {

using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TAuthenticatedUserGuard::TAuthenticatedUserGuard(
    TSecurityManagerPtr securityManager,
    const Stroka& user)
    : SecurityManager_(std::move(securityManager))
{
    SecurityManager_->SetAuthenticatedUser(user);
}

TAuthenticatedUserGuard::~TAuthenticatedUserGuard()
{
    SecurityManager_->ResetAuthenticatedUser();
}

////////////////////////////////////////////////////////////////////////////////

class TSecurityManager::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TSecurityManagerConfigPtr config,
        NCellNode::TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
    { }

    void SetAuthenticatedUser(const Stroka& user)
    {
        YASSERT(!AuthenticatedUser_);
        AuthenticatedUser_ = user;
    }

    void ResetAuthenticatedUser()
    {
        YASSERT(AuthenticatedUser_);
        AuthenticatedUser_.Reset();
    }

    TFuture<void> CheckPermission(
        TTabletSnapshotPtr tabletSnapshot,
        EPermission permission)
    {
        if (!AuthenticatedUser_) {
            return VoidFuture;
        }

        const auto& user = *AuthenticatedUser_;

        LOG_DEBUG("Checking permission (TabletId: %v, TableId: %v, User: %v, Permission: %v)",
            tabletSnapshot->TabletId,
            tabletSnapshot->TableId,
            user,
            permission);

        return VoidFuture;
    }

    void ValidatePermission(
        TTabletSnapshotPtr tabletSnapshot,
        EPermission permission)
    {
        auto asyncResult = CheckPermission(std::move(tabletSnapshot), permission);
        auto maybeResult = asyncResult.TryGet();
        auto result = maybeResult ? *maybeResult : WaitFor(asyncResult);
        result.ThrowOnError();
    }

private:
    const TSecurityManagerConfigPtr Config_;
    NCellNode::TBootstrap const* Bootstrap_;

    TNullable<Stroka> AuthenticatedUser_;

};

////////////////////////////////////////////////////////////////////////////////

TSecurityManager::TSecurityManager(
    TSecurityManagerConfigPtr config,
    NCellNode::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

TSecurityManager::~TSecurityManager()
{ }

void TSecurityManager::SetAuthenticatedUser(const Stroka& user)
{
    Impl_->SetAuthenticatedUser(user);
}

void TSecurityManager::ResetAuthenticatedUser()
{
    Impl_->ResetAuthenticatedUser();
}

TFuture<void> TSecurityManager::CheckPermission(
    TTabletSnapshotPtr tabletSnapshot,
    EPermission permission)
{
    return Impl_->CheckPermission(std::move(tabletSnapshot), permission);
}

void TSecurityManager::ValidatePermission(
    TTabletSnapshotPtr tabletSnapshot,
    EPermission permission)
{
    Impl_->ValidatePermission(std::move(tabletSnapshot), permission);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
