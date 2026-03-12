#include "cypress_user_manager.h"

#include "private.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/library/auth_server/config.h>
#include <yt/yt/library/auth_server/helpers.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/async_expiring_cache.h>
#include <yt/yt/core/misc/cache_config.h>
#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NAuth {

using namespace NYPath;
using namespace NYTree;
using namespace NObjectClient;
using namespace NYson;

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

    TFuture<std::vector<std::string>> GetUserGroups(const std::string& name) override
    {
        return Client_->GetNode("//sys/users/" + ToYPathLiteral(name) + "/@member_of")
            .Apply(BIND([] (const TYsonString& rawGroupsList) {
                return ConvertTo<std::vector<std::string>>(rawGroupsList);
            }));
    }

    TFuture<void> AddUserToGroup(const std::string& name, const std::string& group) override
    {
        return Client_->AddMember(group, name)
            .Apply(BIND([] (const TErrorOr<void>& result) {
                if (result.IsOK() || result.GetCode() == NSecurityClient::EErrorCode::AlreadyPresentInGroup) {
                    return;
                }
                result.ThrowOnError();
            }));
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

    TFuture<std::vector<std::string>> GetUserGroups(const std::string& /*name*/) override
    {
        return MakeFuture(std::vector<std::string>());
    }

protected:
    TFuture<void> CreateUser(const std::string& /*name*/, const std::vector<std::string>& /*tags*/) override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<void> AddUserToGroup(const std::string& /*name*/, const std::string& /*group*/) override
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

class TInMemoryCypressUserManager
    : public ICypressUserManager
{
public:
    TFuture<bool> CheckUserExists(const std::string& name) override
    {
        return MakeFuture(UserGroups_.contains(name));
    }

    TFuture<std::vector<std::string>> GetUserGroups(const std::string& name) override
    {
        if (!UserGroups_.contains(name)) {
            return MakeFuture(std::vector<std::string>());
        }

        const auto& groups = UserGroups_.at(name);
        return MakeFuture(std::vector<std::string>(groups.cbegin(), groups.cend()));
    }

    TFuture<void> CreateUser(const std::string& name, const std::vector<std::string>& /*tags*/) override
    {
        if (!UserGroups_.contains(name)) {
            UserGroups_[name] = {};
        }

        return OKFuture;
    }

    TFuture<void> AddUserToGroup(const std::string& name, const std::string& group) override
    {
        if (!UserGroups_.contains(name)) {
            return MakeFuture(TError("User does not exist (Name: %v)", name));
        }

        UserGroups_[name].insert(group);

        return OKFuture;
    }

private:
    std::map<std::string, std::set<std::string>> UserGroups_;
};

////////////////////////////////////////////////////////////////////////////////

ICypressUserManagerPtr CreateInMemoryCypressUserManager()
{
    return New<TInMemoryCypressUserManager>();
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
            NYT::NRpc::TDispatcher::Get()->GetHeavyInvoker(),
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

class TCypressUserManagerUserGroupsCache
    : public TAsyncExpiringCache<std::string, std::vector<std::string>>
{
public:
    TCypressUserManagerUserGroupsCache(
        TAsyncExpiringCacheConfigPtr config,
        ICypressUserManagerPtr cypressUserManager,
        NProfiling::TProfiler profiler)
        : TAsyncExpiringCache(
            std::move(config),
            NRpc::TDispatcher::Get()->GetHeavyInvoker(),
            /*logger*/ {},
            std::move(profiler))
        , CypressUserManager_(std::move(cypressUserManager))
    { }

private:
    const ICypressUserManagerPtr CypressUserManager_;

    TFuture<std::vector<std::string>> DoGet(
        const std::string& name,
        bool /*isPeriodicUpdate*/) noexcept override
    {
        return CypressUserManager_->GetUserGroups(name);
    }

    bool CanCacheError(const TError& /*error*/) noexcept override
    {
        return false;
    }
};

DECLARE_REFCOUNTED_CLASS(TCypressUserManagerUserGroupsCache)
DEFINE_REFCOUNTED_TYPE(TCypressUserManagerUserGroupsCache)

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
        , UserGroupsCache_(New<TCypressUserManagerUserGroupsCache>(
            config->Cache->ToAsyncExpiringCacheConfig(),
            UserManager_,
            profiler.WithPrefix("/user_groups")))
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

    TFuture<std::vector<std::string>> GetUserGroups(const std::string& name) override
    {
        return UserGroupsCache_->Get(name);
    }

    TFuture<void> AddUserToGroup(const std::string& name, const std::string& group) override
    {
        // If we cached groups for user, we should invalidate all of them.
        UserGroupsCache_->InvalidateActive(name);
        return UserManager_->AddUserToGroup(name, group);
    }

private:
    const ICypressUserManagerPtr UserManager_;
    const TCypressUserManagerCheckUserExistsCachePtr CheckUserExistsCache_;
    const TCypressUserManagerUserGroupsCachePtr UserGroupsCache_;
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
