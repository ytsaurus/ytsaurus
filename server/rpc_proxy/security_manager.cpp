#include "security_manager.h"
#include "private.h"
#include "bootstrap.h"

#include <yt/server/lib/tablet_node/config.h>

#include <yt/ytlib/api/native/client.h>

#include <yt/core/concurrency/fls.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/async_expiring_cache.h>

namespace NYT::NRpcProxy {

using namespace NApi;
using namespace NConcurrency;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NSecurityClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = RpcProxyLogger;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TUserCache)

class TUserCache
    : public TAsyncExpiringCache<TString, void>
{
public:
    TUserCache(
        TAsyncExpiringCacheConfigPtr config,
        const TBootstrap* bootstrap)
        : TAsyncExpiringCache(std::move(config))
        , Bootstrap_(bootstrap)
    { }

private:
    const TBootstrap* const Bootstrap_;

    virtual TFuture<void> DoGet(const TString& user, bool isPeriodicUpdate) noexcept override
    {
        YT_LOG_DEBUG("User check started (User: %v)",
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

                const auto& node = ConvertToNode(resultOrError.Value());

                YT_LOG_DEBUG("Got banned info for user %Qv: %Qv",
                    user,
                    ConvertToYsonString(node, EYsonFormat::Text));

                if (node->AsBoolean()->GetValue()) {
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
        const TBootstrap* bootstrap)
        : Config_(std::move(config))
        , Bootstrap_(bootstrap)
        , UserCache_(New<TUserCache>(Config_->UserCache, Bootstrap_))
    { }

    void ValidateUser(const TString& user)
    {
        WaitFor(UserCache_->Get(user))
            .ThrowOnError();
    }

private:
    const TSecurityManagerConfigPtr Config_;
    const TBootstrap* const Bootstrap_;
    const TUserCachePtr UserCache_;
};

////////////////////////////////////////////////////////////////////////////////

TSecurityManager::TSecurityManager(
    TSecurityManagerConfigPtr config,
    const TBootstrap* bootstrap)
    : Impl_(New<TImpl>(
        std::move(config),
        bootstrap))
{ }

TSecurityManager::~TSecurityManager() = default;

void TSecurityManager::ValidateUser(
    const TString& user)
{
    Impl_->ValidateUser(user);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
