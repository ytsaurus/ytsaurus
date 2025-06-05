#include "user_access_validator.h"

#include "config.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

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
        : TAsyncExpiringCache(std::move(config), logger.WithTag("Cache: UserBan"))
        , Connection_(std::move(connection))
        , Logger(std::move(logger))
        , Client_(Connection_->CreateNativeClient(TClientOptions::Root()))
    { }

private:
    const NNative::IConnectionPtr Connection_;
    const TLogger Logger;
    const NNative::IClientPtr Client_;

    THashMap<std::string, NNative::IClientPtr> RemoteClientMap_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, RemoteClientMapLock_);

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

        // If remote multiproxy target specified check its ban status as well.

        NNative::IClientPtr remoteClient;
        try {
            auto guard = Guard(RemoteClientMapLock_);

            const auto& [it, inserted] = RemoteClientMap_.emplace(*cluster, nullptr);
            if (inserted) {
                auto remoteClusterConnection = Connection_->GetClusterDirectory()->GetConnectionOrThrow(*cluster);
                it->second = remoteClusterConnection->CreateNativeClient(TClientOptions::Root());
            }
            remoteClient = it->second;
        } catch (const std::exception& ex) {
            return MakeFuture<void>(ex);
        }
        futures.push_back(CheckUser(remoteClient, user));

        return AllSucceeded(std::move(futures));
    }

    TFuture<void> CheckUser(const NNative::IClientPtr& client, const std::string& user) noexcept
    {
        auto options = TGetNodeOptions();
        options.ReadFrom = EMasterChannelKind::Cache;
        options.SuppressUpstreamSync = true;
        options.SuppressTransactionCoordinatorSync = true;
        auto clusterName = client->GetNativeConnection()->GetStaticConfig()->ClusterName;
        return client->GetNode("//sys/users/" + ToYPathLiteral(user) + "/@banned", options).Apply(
            BIND([user, clusterName, this, this_ = MakeStrong(this)] (const TErrorOr<TYsonString>& resultOrError) {
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
