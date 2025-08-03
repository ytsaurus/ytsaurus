#include "cypress_user_manager.h"

#include "private.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/library/auth_server/config.h>
#include <yt/yt/library/auth_server/helpers.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/async_expiring_cache.h>
#include <yt/yt/core/misc/cache_config.h>
#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NAuth {

using namespace NYPath;
using namespace NYTree;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

class TCypressUserManager
    : public ICypressUserManager
{
public:
    TCypressUserManager(
        TCypressUserManagerConfigPtr config,
        NApi::IClientPtr client)
        : Config_(std::move(config))
        , Client_(std::move(client))
    { }

    TFuture<bool> CheckUserExists(const std::string& name) override
    {
        return Client_->NodeExists("//sys/users/" + ToYPathLiteral(name));
    }

    TFuture<void> CreateUser(const std::string& name, const std::vector<std::string>& tags) override
    {
        NApi::TCreateObjectOptions options;
        options.IgnoreExisting = true;

        auto attributes = CreateEphemeralAttributes();
        attributes->Set("name", name);
        attributes->Set("tags", tags);
        options.Attributes = std::move(attributes);

        return Client_->CreateObject(
            EObjectType::User,
            options).AsVoid();
    }

private:
    const TCypressUserManagerConfigPtr Config_;
    const NApi::IClientPtr Client_;
};

////////////////////////////////////////////////////////////////////////////////

ICypressUserManagerPtr CreateCypressUserManager(
    TCypressUserManagerConfigPtr config,
    NApi::IClientPtr client)
{
    return New<TCypressUserManager>(
        std::move(config),
        std::move(client));
}

////////////////////////////////////////////////////////////////////////////////

class TNullCypressUserManager
    : public ICypressUserManager
{
public:
    TFuture<bool> CheckUserExists(const std::string& /*name*/) override
    {
        return MakeFuture(true);
    }

protected:
    TFuture<void> CreateUser(const std::string& /*name*/, const std::vector<std::string>& /*tags*/) override
    {
        YT_UNIMPLEMENTED();
    }
};

////////////////////////////////////////////////////////////////////////////////

ICypressUserManagerPtr CreateNullCypressUserManager()
{
    return New<TNullCypressUserManager>();
}

////////////////////////////////////////////////////////////////////////////////

class TCypressUserManagerCheckUserExistsCache
    : public TAsyncExpiringCache<std::string, bool>
{
public:
    TCypressUserManagerCheckUserExistsCache(
        TAsyncExpiringCacheConfigPtr config,
        ICypressUserManagerPtr cypressUserManager,
        NProfiling::TProfiler profiler)
        : TAsyncExpiringCache(
            std::move(config),
            /*logger*/ {},
            std::move(profiler))
        , CypressUserManager_(std::move(cypressUserManager))
    { }

private:
    const ICypressUserManagerPtr CypressUserManager_;

    TFuture<bool> DoGet(
        const std::string& name,
        bool /*isPeriodicUpdate*/) noexcept override
    {
        return CypressUserManager_->CheckUserExists(name);
    }

    bool CanCacheError(const TError& /*error*/) noexcept override
    {
        return false;
    }
};

DECLARE_REFCOUNTED_CLASS(TCypressUserManagerCheckUserExistsCache)
DEFINE_REFCOUNTED_TYPE(TCypressUserManagerCheckUserExistsCache)

////////////////////////////////////////////////////////////////////////////////

class TCachingCypressUserManager
    : public ICypressUserManager
{
public:
    TCachingCypressUserManager(
        TCachingCypressUserManagerConfigPtr config,
        ICypressUserManagerPtr cypressUserManager,
        NProfiling::TProfiler profiler)
        : UserManager_(std::move(cypressUserManager))
        , CheckUserExistsCache_(New<TCypressUserManagerCheckUserExistsCache>(
            config->Cache->ToAsyncExpiringCacheConfig(),
            UserManager_,
            profiler.WithPrefix("/user_exists")))
    { }

    TFuture<bool> CheckUserExists(const std::string& name) override
    {
        return CheckUserExistsCache_->Get(name);
    }

    TFuture<void> CreateUser(const std::string& name, const std::vector<std::string>& tags) override
    {
        // If we cached the fact that user didn't exist, we should invalidate it.
        CheckUserExistsCache_->InvalidateValue(name, false);
        return UserManager_->CreateUser(name, tags);
    }

private:
    const ICypressUserManagerPtr UserManager_;
    const TCypressUserManagerCheckUserExistsCachePtr CheckUserExistsCache_;
};

////////////////////////////////////////////////////////////////////////////////

ICypressUserManagerPtr CreateCachingCypressUserManager(
    TCachingCypressUserManagerConfigPtr config,
    ICypressUserManagerPtr userManager,
    NProfiling::TProfiler profiler)
{
    return New<TCachingCypressUserManager>(
        std::move(config),
        std::move(userManager),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
