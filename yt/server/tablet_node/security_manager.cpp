#include "stdafx.h"
#include "security_manager.h"
#include "tablet.h"
#include "config.h"
#include "private.h"

#include <core/misc/expiring_cache.h>

#include <core/concurrency/scheduler.h>
#include <core/concurrency/fls.h>

#include <ytlib/api/client.h>

#include <ytlib/object_client/helpers.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NTabletNode {

using namespace NYTree;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NApi;
using namespace NSecurityClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TAuthenticatedUserGuard::TAuthenticatedUserGuard(
    TSecurityManagerPtr securityManager,
    const TNullable<Stroka>& maybeUser)
    : SecurityManager_(std::move(securityManager))
    , IsNull_(!maybeUser)
{
    if (!IsNull_) {
        SecurityManager_->SetAuthenticatedUser(*maybeUser);
    }
}

TAuthenticatedUserGuard::~TAuthenticatedUserGuard()
{
    if (!IsNull_) {
        SecurityManager_->ResetAuthenticatedUser();
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TTablePermissionKey
{
    TObjectId TableId;
    Stroka User;
    EPermission Permission;

    // Hasher.
    operator size_t() const
    {
        size_t result = 0;
        result = HashCombine(result, TableId);
        result = HashCombine(result, User);
        result = HashCombine(result, Permission);
        return result;
    }

    // Comparer.
    bool operator == (const TTablePermissionKey& other) const
    {
        return
            TableId == other.TableId &&
            User == other.User &&
            Permission == other.Permission;
    }

    // Formatter.
    friend Stroka ToString(const TTablePermissionKey& key)
    {
        return Format("%v:%v:%v",
            key.TableId,
            key.User,
            key.Permission);
    }
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTablePermissionCache)

class TTablePermissionCache
    : public TExpiringCache<TTablePermissionKey, void>
{
public:
    TTablePermissionCache(
        TExpiringCacheConfigPtr config,
        NCellNode::TBootstrap* bootstrap)
        : TExpiringCache(config)
        , Bootstrap_(bootstrap)
    { }

private:
    NCellNode::TBootstrap* const Bootstrap_;


    virtual TFuture<void> DoGet(const TTablePermissionKey& key) override
    {
        LOG_DEBUG("Table permission check started (Key: {%v})",
            key);

        auto client = Bootstrap_->GetMasterClient();
        return client->CheckPermission(key.User, FromObjectId(key.TableId), key.Permission).Apply(
            BIND([=, this_ = MakeStrong(this)] (const TErrorOr<TCheckPermissionResult>& resultOrError) {
                if (!resultOrError.IsOK()) {
                    auto wrappedError = TError("Error checking permission for table %v",
                        key.TableId)
                        << resultOrError;
                    LOG_WARNING(wrappedError);
                    THROW_ERROR wrappedError;
                }

                const auto& result = resultOrError.Value();

                LOG_DEBUG("Table permission check complete (Key: {%v}, Action: %v)",
                    key,
                    result.Action);

                auto error = result.ToError(key.User, key.Permission);
                if (!error.IsOK()) {
                    THROW_ERROR error << TErrorAttribute("object", key.TableId);
                }
            }));
    }
};

DEFINE_REFCOUNTED_TYPE(TTablePermissionCache)

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
        , TablePermissionCache_(New<TTablePermissionCache>(Config_->TablePermissionCache, Bootstrap_))
    { }

    void SetAuthenticatedUser(const Stroka& user)
    {
        YASSERT(!*AuthenticatedUser_);
        *AuthenticatedUser_ = user;
    }

    void ResetAuthenticatedUser()
    {
        YASSERT(*AuthenticatedUser_);
        AuthenticatedUser_->Reset();
    }

    TNullable<Stroka> GetAuthenticatedUser()
    {
        return *AuthenticatedUser_;
    }

    TFuture<void> CheckPermission(
        TTabletSnapshotPtr tabletSnapshot,
        EPermission permission)
    {
        auto maybeUser = GetAuthenticatedUser();
        if (!maybeUser) {
            return VoidFuture;
        }

        // COMPAT(babenko)
        if (!tabletSnapshot->TableId) {
            return VoidFuture;
        }

        TTablePermissionKey key{tabletSnapshot->TableId, *maybeUser, permission};
        return TablePermissionCache_->Get(key);
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
    NCellNode::TBootstrap* const Bootstrap_;

    const TTablePermissionCachePtr TablePermissionCache_;

    TFls<TNullable<Stroka>> AuthenticatedUser_;

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

TNullable<Stroka> TSecurityManager::GetAuthenticatedUser()
{
    return Impl_->GetAuthenticatedUser();
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
