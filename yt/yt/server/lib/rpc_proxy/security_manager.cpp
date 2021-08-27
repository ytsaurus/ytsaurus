#include "security_manager.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/core/misc/async_expiring_cache.h>

namespace NYT::NRpcProxy {

using namespace NApi;
using namespace NConcurrency;
using namespace NLogging;
using namespace NSecurityClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TUserCache)

class TUserCache
    : public TAsyncExpiringCache<TString, void>
{
public:
    TUserCache(
        TAsyncExpiringCacheConfigPtr config,
        IBootstrap* bootstrap,
        TLogger logger)
        : TAsyncExpiringCache(std::move(config), logger.WithTag("Cache: User"))
        , Bootstrap_(bootstrap)
        , Logger(std::move(logger))
    { }

private:
    IBootstrap* const Bootstrap_;
    const TLogger Logger;

    TFuture<void> DoGet(const TString& user, bool /*isPeriodicUpdate*/) noexcept override
    {
        YT_LOG_DEBUG("Getting user ban flag (User: %v)",
            user);

        auto client = Bootstrap_->GetNativeClient();
        auto options = TGetNodeOptions();
        options.ReadFrom = EMasterChannelKind::Cache;
        return client->GetNode("//sys/users/" + ToYPathLiteral(user) + "/@banned", options).Apply(
            BIND([=, this_ = MakeStrong(this)] (const TErrorOr<TYsonString>& resultOrError) {
                if (!resultOrError.IsOK()) {
                    auto wrappedError = TError("Error getting user info for user %Qv",
                        user)
                        << resultOrError;
                    YT_LOG_WARNING(wrappedError);
                    THROW_ERROR wrappedError;
                }

                auto banned = ConvertTo<bool>(resultOrError.Value());

                YT_LOG_DEBUG("Got user ban flag (User: %v, Banned: %v)",
                    user,
                    banned);

                if (banned) {
                    THROW_ERROR_EXCEPTION("User %Qv is banned",
                        user);
                }
            }));
    }
};

DEFINE_REFCOUNTED_TYPE(TUserCache)

////////////////////////////////////////////////////////////////////////////////

class TSecurityManager::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TSecurityManagerConfigPtr config,
        IBootstrap* bootstrap,
        TLogger logger)
        : Config_(std::move(config))
        , UserCache_(New<TUserCache>(
            Config_->UserCache,
            bootstrap,
            std::move(logger)))
    { }

    void ValidateUser(const TString& user)
    {
        WaitFor(UserCache_->Get(user))
            .ThrowOnError();
    }

private:
    const TSecurityManagerConfigPtr Config_;
    const TUserCachePtr UserCache_;
};

////////////////////////////////////////////////////////////////////////////////

TSecurityManager::TSecurityManager(
    TSecurityManagerConfigPtr config,
    IBootstrap* bootstrap,
    TLogger logger)
    : Impl_(New<TImpl>(
        std::move(config),
        bootstrap,
        std::move(logger)))
{ }

TSecurityManager::~TSecurityManager() = default;

void TSecurityManager::ValidateUser(
    const TString& user)
{
    Impl_->ValidateUser(user);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
