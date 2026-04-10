#include "user_access_validator.h"

#include "config.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/core/misc/async_expiring_cache.h>

#include <yt/yt/core/rpc/dispatcher.h>

namespace NYT::NSecurityServer {

using namespace NApi;
using namespace NConcurrency;
using namespace NLogging;
using namespace NSecurityClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

using TUserBanCacheKey = std::pair<std::string, std::optional<std::string>>;

DECLARE_REFCOUNTED_CLASS(TUserBanCache)

class TUserBanCache
    : public TAsyncExpiringCache<TUserBanCacheKey, void>
{
public:
    TUserBanCache(
        TAsyncExpiringCacheConfigPtr config,
        NNative::IConnectionPtr connection,
        TLogger logger)
        : TAsyncExpiringCache(std::move(config), GetInvoker(), logger.WithTag("Cache: UserBan"))
        , Connection_(std::move(connection))
        , Logger(std::move(logger))
        , Client_(Connection_->CreateNativeClient(NNative::TClientOptions::Root()))
    { }

private:
    const NNative::IConnectionPtr Connection_;
    const TLogger Logger;
    const NNative::IClientPtr Client_;

    static IInvokerPtr GetInvoker()
    {
        return NRpc::TDispatcher::Get()->GetHeavyInvoker();
    }

    TFuture<void> DoGet(const TUserBanCacheKey& cacheKey, bool /*isPeriodicUpdate*/) noexcept override
    {
        const auto& [user, cluster] = cacheKey;
        YT_LOG_DEBUG("Getting user ban flag (User: %v, Cluster: %v)",
            user,
            cluster);

        if (!cluster) {
            return CheckUser(Client_, user);
        }

        std::vector<TFuture<void>> futures;
        // Check user on local cluster first.
        futures.push_back(Get(TUserBanCacheKey(user, std::nullopt)));

        auto multiproxyClientFuture = NNative::InsistentGetRemoteConnection(
            Connection_,
            *cluster,
            NNative::EInsistentGetRemoteConnectionMode::WaitFirstSuccessfulSync)
            .Apply(BIND([cluster=*cluster] (const TErrorOr<NNative::IConnectionPtr>& connectionOrError) {
                if (connectionOrError.IsOK()) {
                    auto connection = connectionOrError.Value();
                    auto client = connection->CreateNativeClient(NNative::TClientOptions::Root());
                    return client;
                } else {
                    THROW_ERROR_EXCEPTION("Cannot resolve multiproxy target cluster %Qv", cluster)
                        << connectionOrError;
                }
            }));

        auto remoteBan = multiproxyClientFuture.Apply(BIND([this_ = MakeStrong(this), user=user] (const NNative::IClientPtr& client) {
            return this_->CheckUser(client, user);
        }));

        futures.push_back(std::move(remoteBan));

        return AllSucceeded(std::move(futures));
    }

    TFuture<void> CheckUser(const NNative::IClientPtr& client, const std::string& user) noexcept
    {
        auto options = TGetNodeOptions();
        options.ReadFrom = EMasterChannelKind::Cache;
        options.SuppressUpstreamSync = true;
        options.SuppressTransactionCoordinatorSync = true;
        options.SuppressStronglyOrderedTransactionBarrier = true;
        auto clusterName = client->GetNativeConnection()->GetStaticConfig()->ClusterName;
        return client->GetNode("//sys/users/" + ToYPathLiteral(user) + "/@banned", options).Apply(
            BIND([user, clusterName, this, this_ = MakeStrong(this)] (const TErrorOr<TYsonString>& resultOrError) {
                if (!resultOrError.IsOK()) {
                    TError wrappedError;
                    if (resultOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                        wrappedError = TError("No such user %Qv",
                            user);
                    } else {
                        wrappedError = TError("Error getting user info for user %Qv",
                            user);
                    }
                    wrappedError <<= resultOrError;
                    YT_LOG_WARNING(wrappedError);
                    THROW_ERROR wrappedError;
                }

                auto banned = ConvertTo<bool>(resultOrError.Value());

                YT_LOG_DEBUG("Got user ban flag (User: %v, Banned: %v)",
                    user,
                    banned);

                if (banned) {
                    THROW_ERROR_EXCEPTION("User %Qv is banned on cluster %Qv",
                        user,
                        clusterName.value_or("unknown"));
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
        NNative::IConnectionPtr connection,
        TLogger logger)
        : UserCache_(New<TUserBanCache>(
            config->BanCache,
            std::move(connection),
            std::move(logger)))
    { }

    void ValidateUser(const std::string& user, const std::optional<std::string>& cluster) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto result = UserCache_->Find({user, cluster});
        if (result) {
            result->ThrowOnError();
        }

        WaitForFast(UserCache_->Get({user, cluster}))
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
    NNative::IConnectionPtr connection,
    NLogging::TLogger logger)
{
    return New<TUserAccessValidator>(
        std::move(config),
        std::move(connection),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
