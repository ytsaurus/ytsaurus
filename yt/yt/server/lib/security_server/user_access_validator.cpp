#include "user_access_validator.h"

#include "config.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/core/misc/async_expiring_cache.h>

namespace NYT::NSecurityServer {

using namespace NApi;
using namespace NConcurrency;
using namespace NLogging;
using namespace NSecurityClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TUserBanCache)

class TUserBanCache
    : public TAsyncExpiringCache<std::string, void>
{
public:
    TUserBanCache(
        TAsyncExpiringCacheConfigPtr config,
        IConnectionPtr connection,
        TLogger logger)
        : TAsyncExpiringCache(std::move(config), logger.WithTag("Cache: UserBan"))
        , Connection_(std::move(connection))
        , Logger(std::move(logger))
        , Client_(Connection_->CreateClient(TClientOptions::Root()))
    { }

private:
    const IConnectionPtr Connection_;
    const TLogger Logger;
    const IClientPtr Client_;

    TFuture<void> DoGet(const std::string& user, bool /*isPeriodicUpdate*/) noexcept override
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

DEFINE_REFCOUNTED_TYPE(TUserBanCache)

////////////////////////////////////////////////////////////////////////////////

class TUserAccessValidator
    : public IUserAccessValidator
{
public:
    TUserAccessValidator(
        TUserAccessValidatorDynamicConfigPtr config,
        IConnectionPtr connection,
        TLogger logger)
        : UserCache_(New<TUserBanCache>(
            config->BanCache,
            std::move(connection),
            std::move(logger)))
    { }

    void ValidateUser(const std::string& user) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        WaitForFast(UserCache_->Get(user))
            .ThrowOnError();
    }

    void Reconfigure(const TUserAccessValidatorDynamicConfigPtr& config) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        UserCache_->Reconfigure(config->BanCache);
    }

private:
    const TUserBanCachePtr UserCache_;
};

////////////////////////////////////////////////////////////////////////////////

IUserAccessValidatorPtr CreateUserAccessValidator(
    TUserAccessValidatorDynamicConfigPtr config,
    IConnectionPtr connection,
    NLogging::TLogger logger)
{
    return New<TUserAccessValidator>(
        std::move(config),
        std::move(connection),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
