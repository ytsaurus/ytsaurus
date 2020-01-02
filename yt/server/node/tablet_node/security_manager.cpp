#include "security_manager.h"
#include "private.h"
#include "tablet.h"

#include <yt/server/node/cell_node/bootstrap.h>

#include <yt/server/lib/tablet_node/config.h>

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/security_client/permission_cache.h>

#include <yt/core/concurrency/fls.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/async_expiring_cache.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NConcurrency;
using namespace NSecurityClient;
using namespace NTabletClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TAuthenticatedUserGuard::TAuthenticatedUserGuard(
    TSecurityManagerPtr securityManager,
    const std::optional<TString>& optionalUser)
    : TAuthenticatedUserGuardBase(std::move(securityManager), optionalUser)
{ }

////////////////////////////////////////////////////////////////////////////////

struct TResourceLimitsKey
{
    TString Account;
    TString MediumName;
    EInMemoryMode InMemoryMode;

    // Hasher.
    operator size_t() const
    {
        size_t result = 0;
        HashCombine(result, Account);
        HashCombine(result, MediumName);
        HashCombine(result, InMemoryMode);
        return result;
    }

    // Comparer.
    bool operator == (const TResourceLimitsKey& other) const
    {
        return
            Account == other.Account &&
            MediumName == other.MediumName &&
            InMemoryMode == other.InMemoryMode;
    }

    // Formatter.
    friend TString ToString(const TResourceLimitsKey& key)
    {
        return Format("%v:%v:%v",
            key.Account,
            key.MediumName,
            key.InMemoryMode);
    }
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TResourceLimitsCache)

class TResourceLimitsCache
    : public TAsyncExpiringCache<TResourceLimitsKey, void>
{
public:
    TResourceLimitsCache(
        TAsyncExpiringCacheConfigPtr config,
        NCellNode::TBootstrap* bootstrap)
        : TAsyncExpiringCache(std::move(config))
        , Bootstrap_(bootstrap)
    { }

private:
    NCellNode::TBootstrap* const Bootstrap_;

    virtual TFuture<void> DoGet(const TResourceLimitsKey& key) override
    {
        YT_LOG_DEBUG("Resource limits violation check started (Key: %v)",
            key);

        auto client = Bootstrap_->GetMasterClient();
        auto options = TGetNodeOptions();
        options.ReadFrom = EMasterChannelKind::Cache;
        options.ExpireAfterSuccessfulUpdateTime = Config_->ExpireAfterSuccessfulUpdateTime;
        options.ExpireAfterFailedUpdateTime = Config_->ExpireAfterFailedUpdateTime;

        return client->GetNode("//sys/accounts/" + ToYPathLiteral(key.Account) + "/@violated_resource_limits", options).Apply(
            BIND([=, this_ = MakeStrong(this)] (const TErrorOr<TYsonString>& resultOrError) {
                if (!resultOrError.IsOK()) {
                    auto wrappedError = TError("Error getting resource limits for account %Qv",
                        key.Account)
                        << resultOrError;
                    YT_LOG_WARNING(wrappedError);
                    THROW_ERROR wrappedError;
                }

                const auto& node = ConvertToNode(resultOrError.Value());

                YT_LOG_DEBUG("Resource limits violation check completed (Key: %v, Result: %v)",
                    key.Account,
                    ConvertToYsonString(node, EYsonFormat::Text));

                if (node->AsMap()->GetChild("chunk_count")->GetValue<bool>()) {
                    THROW_ERROR_EXCEPTION("Account %Qv violates chunk count limit",
                        key.Account);
                }

                if (key.InMemoryMode != EInMemoryMode::None) {
                    if (node->AsMap()->GetChild("tablet_static_memory")->GetValue<bool>()) {
                        THROW_ERROR_EXCEPTION("Account %Qv violates tablet static memory limit",
                            key.Account);
                    }
                }

                const auto& mediumLimit = node->AsMap()->GetChild("disk_space_per_medium")->AsMap()->FindChild(key.MediumName);

                if (!mediumLimit) {
                    THROW_ERROR_EXCEPTION("Unknown medium %Qv",
                        key.MediumName);
                }
                if (mediumLimit->GetValue<bool>()) {
                    THROW_ERROR_EXCEPTION("Account %Qv violates disk space limit for medium %Qv",
                        key.Account,
                        key.MediumName);
                }
            }));
    }
};

DEFINE_REFCOUNTED_TYPE(TResourceLimitsCache)

////////////////////////////////////////////////////////////////////////////////

class TSecurityManager::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TSecurityManagerConfigPtr config,
        NCellNode::TBootstrap* bootstrap)
        : Config_(std::move(config))
        , Bootstrap_(bootstrap)
        , PermissionCache_(New<TPermissionCache>(Config_->PermissionCache, Bootstrap_->GetMasterClient()))
        , ResourceLimitsCache_(New<TResourceLimitsCache>(Config_->ResourceLimitsCache, Bootstrap_))
    { }

    void SetAuthenticatedUserByNameOrThrow(const TString& user)
    {
        YT_ASSERT(!*AuthenticatedUser_);
        *AuthenticatedUser_ = user;
    }

    void ResetAuthenticatedUser()
    {
        YT_ASSERT(*AuthenticatedUser_);
        AuthenticatedUser_->reset();
    }

    std::optional<TString> GetAuthenticatedUserName()
    {
        return *AuthenticatedUser_;
    }

    TFuture<void> CheckPermission(
        const TString& path,
        EPermission permission)
    {
        auto optionalUser = GetAuthenticatedUserName();
        if (!optionalUser) {
            return VoidFuture;
        }

        TPermissionKey key{path, *optionalUser, permission};
        return PermissionCache_->Get(key, /* GetInfo */ nullptr);
    }

    void ValidatePermission(
        const TString& path,
        EPermission permission)
    {
        auto asyncResult = CheckPermission(path, permission);
        auto optionalResult = asyncResult.TryGet();
        TError result;
        if (optionalResult) {
            result = *optionalResult;
        } else {
            YT_LOG_DEBUG("Started waiting for persmission cache result");
            result = WaitFor(asyncResult);
            YT_LOG_DEBUG("Finished waiting for persmission cache result");
        }
        result.ThrowOnError();
    }

    TFuture<void> CheckResourceLimits(
        const TString& account,
        const TString& mediumName,
        EInMemoryMode inMemoryMode)
    {
        return ResourceLimitsCache_->Get(TResourceLimitsKey{account, mediumName, inMemoryMode});
    }

    void ValidateResourceLimits(
        const TString& account,
        const TString& mediumName,
        EInMemoryMode inMemoryMode)
    {
        auto asyncResult = CheckResourceLimits(account, mediumName, inMemoryMode);
        auto optionalResult = asyncResult.TryGet();
        auto result = optionalResult ? *optionalResult : WaitFor(asyncResult);
        result.ThrowOnError();
    }

private:
    const TSecurityManagerConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;

    const TPermissionCachePtr PermissionCache_;
    const TResourceLimitsCachePtr ResourceLimitsCache_;

    TFls<std::optional<TString>> AuthenticatedUser_;

};

////////////////////////////////////////////////////////////////////////////////

TSecurityManager::TSecurityManager(
    TSecurityManagerConfigPtr config,
    NCellNode::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(
        std::move(config),
        bootstrap))
{ }

TSecurityManager::~TSecurityManager() = default;

void TSecurityManager::SetAuthenticatedUserByNameOrThrow(const TString& user)
{
    Impl_->SetAuthenticatedUserByNameOrThrow(user);
}

void TSecurityManager::ResetAuthenticatedUser()
{
    Impl_->ResetAuthenticatedUser();
}

std::optional<TString> TSecurityManager::GetAuthenticatedUserName()
{
    return Impl_->GetAuthenticatedUserName();
}

TFuture<void> TSecurityManager::CheckPermission(
    const TString& path,
    EPermission permission)
{
    return Impl_->CheckPermission(path, permission);
}

void TSecurityManager::ValidatePermission(
    const TString& path,
    EPermission permission)
{
    Impl_->ValidatePermission(path, permission);
}

TFuture<void> TSecurityManager::CheckResourceLimits(
    const TString& account,
    const TString& mediumName,
    EInMemoryMode inMemoryMode)
{
    return Impl_->CheckResourceLimits(account, mediumName, inMemoryMode);
}

void TSecurityManager::ValidateResourceLimits(
    const TString& account,
    const TString& mediumName,
    EInMemoryMode inMemoryMode)
{
    Impl_->ValidateResourceLimits(account, mediumName, inMemoryMode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
