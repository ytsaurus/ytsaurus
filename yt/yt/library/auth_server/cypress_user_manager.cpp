#include "cypress_user_manager.h"

#include "auth_cache.h"
#include "private.h"

#include <yt/yt/client/api/client.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NAuth {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = AuthLogger;

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

    TFuture<NObjectClient::TObjectId> CreateUser(const TString& name) override
    {
        YT_LOG_DEBUG("Creating user object (Name: %v)", name);
        NApi::TCreateObjectOptions options;
        options.IgnoreExisting = true;

        auto attributes = CreateEphemeralAttributes();
        attributes->Set("name", name);
        options.Attributes = std::move(attributes);

        return Client_->CreateObject(
            NObjectClient::EObjectType::User,
            options);
    }

    TFuture<bool> CheckUserExists(const TString& name) override
    {
        YT_LOG_DEBUG("Checking if user exists (Name: %v)", name);

        return Client_->NodeExists("//sys/users/" + name);
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
    TFuture<TObjectId> CreateUser(const TString& /*name*/) override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<bool> CheckUserExists(const TString& /*name*/) override
    {
        return MakeFuture(true);
    }
};

////////////////////////////////////////////////////////////////////////////////

ICypressUserManagerPtr CreateNullCypressUserManager()
{
    return New<TNullCypressUserManager>();
}

////////////////////////////////////////////////////////////////////////////////

class TCypressUserManagerCreateUserCache
    : public TAuthCache<TString, NObjectClient::TObjectId, std::monostate>
{
public:
    TCypressUserManagerCreateUserCache(
        TAuthCacheConfigPtr config,
        ICypressUserManagerPtr cypressUserManager,
        NProfiling::TProfiler profiler)
        : TAuthCache(std::move(config), std::move(profiler))
        , CypressUserManager_(std::move(cypressUserManager))
    { }

private:
    ICypressUserManagerPtr CypressUserManager_;

    TFuture<NObjectClient::TObjectId> DoGet(
        const TString& name,
        const std::monostate& /*context*/) noexcept override
    {
        return CypressUserManager_->CreateUser(name);
    }
};

DECLARE_REFCOUNTED_CLASS(TCypressUserManagerCreateUserCache)
DEFINE_REFCOUNTED_TYPE(TCypressUserManagerCreateUserCache)

////////////////////////////////////////////////////////////////////////////////

class TCypressUserManagerCheckUserExistsCache
    : public TAuthCache<TString, bool, std::monostate>
{
public:
    TCypressUserManagerCheckUserExistsCache(
        TAuthCacheConfigPtr config,
        ICypressUserManagerPtr cypressUserManager,
        NProfiling::TProfiler profiler)
        : TAuthCache(std::move(config), std::move(profiler))
        , CypressUserManager_(std::move(cypressUserManager))
    { }

private:
    ICypressUserManagerPtr CypressUserManager_;

    TFuture<bool> DoGet(
        const TString& name,
        const std::monostate& /*context*/) noexcept override
    {
        return CypressUserManager_->CheckUserExists(name);
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
        : CreateUserCache_(New<TCypressUserManagerCreateUserCache>(config->Cache, cypressUserManager, profiler.WithPrefix("/create_user")))
        , CheckUserExistsCache_(New<TCypressUserManagerCheckUserExistsCache>(config->Cache, cypressUserManager, profiler.WithPrefix("/user_exists")))
    { }

    TFuture<NObjectClient::TObjectId> CreateUser(const TString& name) override
    {
        return CreateUserCache_->Get(name, std::monostate{});
    }

    TFuture<bool> CheckUserExists(const TString& name) override
    {
        return CheckUserExistsCache_->Get(name, std::monostate{});
    }

private:
    TCypressUserManagerCreateUserCachePtr CreateUserCache_;
    TCypressUserManagerCheckUserExistsCachePtr CheckUserExistsCache_;
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
