#include "security_manager.h"

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
        IConnectionPtr connection,
        TLogger logger)
        : TAsyncExpiringCache(std::move(config), logger.WithTag("Cache: User"))
        , Connection_(std::move(connection))
        , Logger(std::move(logger))
        , Client_(Connection_->CreateClient(TClientOptions{.User = NRpc::RootUserName}))
    { }

private:
    const IConnectionPtr Connection_;
    const TLogger Logger;

    const IClientPtr Client_;

    TFuture<void> DoGet(const TString& user, bool /*isPeriodicUpdate*/) noexcept override
    {
        YT_LOG_DEBUG("Getting user ban flag (User: %v)",
            user);

        auto options = TGetNodeOptions();
        options.ReadFrom = EMasterChannelKind::Cache;
        return Client_->GetNode("//sys/users/" + ToYPathLiteral(user) + "/@banned", options).Apply(
            BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<TYsonString>& resultOrError) {
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

class TSecurityManager
    : public ISecurityManager
{
public:
    TSecurityManager(
        TSecurityManagerDynamicConfigPtr config,
        IConnectionPtr connection,
        TLogger logger)
        : UserCache_(New<TUserCache>(
            config->UserCache,
            std::move(connection),
            std::move(logger)))
    { }

    void ValidateUser(const TString& user) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        WaitForFast(UserCache_->Get(user))
            .ThrowOnError();
    }

    void Reconfigure(const TSecurityManagerDynamicConfigPtr& config) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        UserCache_->Reconfigure(config->UserCache);
    }

private:
    const TUserCachePtr UserCache_;
};

////////////////////////////////////////////////////////////////////////////////

ISecurityManagerPtr CreateSecurityManager(
    TSecurityManagerDynamicConfigPtr config,
    IConnectionPtr connection,
    NLogging::TLogger logger)
{
    return New<TSecurityManager>(
        std::move(config),
        std::move(connection),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
