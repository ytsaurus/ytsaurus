#include "ban_service.h"

#include "private.h"

#include "bootstrap.h"
#include "dynamic_config_manager.h"

#include <yt/yt/server/lib/cross_cluster_replicated_state/cross_cluster_replicated_state.h>
#include <yt/yt/server/lib/cross_cluster_replicated_state/cross_cluster_replicated_value.h>
#include <yt/yt/server/lib/cross_cluster_replicated_state/cross_cluster_replica_lock_waiter.h>
#include <yt/yt/server/lib/cross_cluster_replicated_state/cross_cluster_replica_version.h>

#include <yt/yt/ytlib/ban_client/ban_service_proxy.h>

#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/core/rpc/service_detail.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/net/local_address.h>

namespace NYT::NCypressProxy {

using namespace NConcurrency;
using namespace NCrossClusterReplicatedState;
using namespace NThreading;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

class TBanService
    : public IBanService
    , public NRpc::TServiceBase
{
public:
    explicit TBanService(IBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetInvoker("BanService"),
            GetDescriptor(),
            CypressProxyLogger().WithTag("BanService"),
            NRpc::TServiceOptions{
                .Authenticator = bootstrap->GetNativeAuthenticator(),
            })
        , Bootstrap_(bootstrap)
        , Config_(Bootstrap_->GetDynamicConfigManager()->GetConfig()->BanService)
        , BanCache_(std::make_shared<TBanCache>())
        , BanCacheRefreshExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TBanService::OnRefreshCache, MakeWeak(this))))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SetUserBanned));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetUserBanned));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ListBannedUsers));
    }

    bool IsBanned(const std::string& user) override
    {
        if (!IsRunning()) {
            return false;
        }

        auto banCache = BanCache_.Load();
        auto cacheEntry = banCache->find(user);
        if (cacheEntry == banCache->end()) {
            return false;
        }
        return cacheEntry->second.IsBanned;
    }

    NRpc::IServicePtr GetService() override
    {
        return MakeStrong(this);
    }

    void Reconfigure(const TBanServiceDynamicConfigPtr& config) override
    {
        auto oldEnable = Config_->Enable;
        Config_ = config;

        UseInObjectService_.store(config->UseInObjectService);
        if (CrossClusterReplicaLockWaiter_) {
            CrossClusterReplicaLockWaiter_->SetLockCheckPeriod(config->CrossClusterReplicatedState->LockCheckPeriod);
        }

        if (oldEnable == config->Enable) {
            return;
        }

        if (config->Enable) {
            try {
                Enable();
            } catch (const TErrorException& error) {
                YT_LOG_ALERT(error, "Failed to enable ban service");
            }
        } else {
            try {
                Disable();
            } catch (const TErrorException& error) {
                YT_LOG_ERROR(error, "Failed to disable ban service");
            }
        }
    }

private:
    IBootstrap* Bootstrap_;
    TBanServiceDynamicConfigPtr Config_;

    ICrossClusterReplicaLockWaiterPtr CrossClusterReplicaLockWaiter_;
    ICrossClusterReplicatedStatePtr CrossClusterReplicatedState_;

    struct TBannedUserEntry {
        std::string CrossClusterReplicaVersionAttribute;
        bool IsBanned;
    };
    using TBanCache = THashMap<std::string, TBannedUserEntry>;
    using TBanCachePtr = std::shared_ptr<const TBanCache>;
    TAtomicObject<TBanCachePtr> BanCache_;
    TPeriodicExecutorPtr BanCacheRefreshExecutor_;

    std::atomic<bool> IsEnabled_{false};
    std::atomic<bool> UseInObjectService_{false};

    DECLARE_RPC_SERVICE_METHOD(NBanClient::NProto, SetUserBanned)
    {
        auto targetUser = std::string(request->user_name());
        auto banned = request->is_banned();
        context->SetRequestInfo("TargetUser: %v, IsBanned: %v", targetUser, banned);

        ThrowIfDisabled();

        SetBanned(targetUser, banned);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NBanClient::NProto, GetUserBanned)
    {
        auto targetUser = std::string(request->user_name());
        context->SetRequestInfo("TargetUser: %v", targetUser);

        ThrowIfDisabled();

        auto isBanned = GetBannedConsistently(targetUser);
        response->set_is_banned(isBanned);

        context->SetResponseInfo("IsBanned: %v", isBanned);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NBanClient::NProto, ListBannedUsers)
    {
        context->SetRequestInfo();
        ThrowIfDisabled();

        for (const auto& bannedUser : ListBanned()) {
            auto* user = response->add_user_names();
            *user = bannedUser;
        }
        context->Reply();
    }

    void OnRefreshCache()
    {
        YT_ASSERT_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        YT_LOG_DEBUG("Starting ban cache refresh iteration");
        auto crossClusterReplicatedState = CrossClusterReplicatedState_;
        YT_ASSERT(crossClusterReplicatedState);
        auto fetchedVersions = WaitFor(crossClusterReplicatedState->FetchVersions());
        if (!fetchedVersions.IsOK()) {
            YT_LOG_ALERT(fetchedVersions, "Cannot fetch versions");
            return;
        }

        std::vector<std::string> freshUsers;
        auto banCache = BanCache_.Load();

        for (const auto& [user, version] : fetchedVersions.Value()) {
            auto oldUser = banCache->find(user);
            if (oldUser == banCache->end()) {
                freshUsers.push_back(user);
                continue;
            }
            if (oldUser->second.CrossClusterReplicaVersionAttribute < std::string(version)) {
                freshUsers.push_back(user);
            }
        }

        auto newCache = *banCache;
        for (const auto& user : freshUsers) {
            auto userValue = CrossClusterReplicatedState_->Value(GetTag(), TYPath(user));
            auto userNode = WaitFor(userValue->Load()).ValueOrThrow()->AsMap();
            TReplicaVersion version;
            try {
                version = ExtractVersion(userNode);
            } catch (const TErrorException& error) {
                YT_LOG_ALERT(error, "Failed to extract version of user %qv entry", user);
                continue;
            }
            auto isBanned = userNode->GetChildValueOrDefault("is_banned", false);
            newCache[user] = {MakeVersionAttributeValue(version), isBanned};
        }

        BanCache_.Store(std::make_shared<const TBanCache>(std::move(newCache)));
    }

    bool GetBannedConsistently(const std::string& user)
    {
        auto userValue = CrossClusterReplicatedState_->Value(GetTag(), TYPath(user));
        auto userNode = WaitFor(userValue->Load()).ValueOrThrow();
        if (!userNode) {
            return false;
        }
        return userNode->AsMap()->GetChildValueOrDefault("is_banned", false);
    }

    void SetBanned(const std::string& user, bool isBanned)
    {
        auto userValue = CrossClusterReplicatedState_->Value(GetTag(), TYPath(user));
        auto nodeFactory = NYTree::CreateEphemeralNodeFactory();
        auto node = nodeFactory->CreateMap();
        node->AddChild("is_banned", NYTree::ConvertToNode(isBanned));
        WaitFor(userValue->Store(node)).ThrowOnError();
    }

    std::vector<std::string> ListBanned()
    {
        TBanCachePtr banCache = BanCache_.Load();

        std::vector<std::string> bannedUsers;
        for (const auto& [user, entry] : *banCache) {
            if (entry.IsBanned) {
                bannedUsers.push_back(user);
            }
        }
        std::ranges::sort(bannedUsers);
        return bannedUsers;
    }

    void Enable()
    {
        YT_ASSERT_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        if (IsEnabled_.load()) {
            THROW_ERROR_EXCEPTION("Attempt to enable running ban service");
        }
        YT_LOG_DEBUG("Enabling ban service");

        WaitFor(Bootstrap_->GetNativeConnection()->GetClusterDirectorySynchronizer()->GetFirstSuccessfulSyncFuture())
            .ThrowOnError();
        auto replicatedStateConfig = GetDynamicConfig()->CrossClusterReplicatedState;
        CrossClusterReplicaLockWaiter_ = CreateCrossClusterReplicaLockWaiter(
            Bootstrap_->GetControlInvoker(),
            replicatedStateConfig,
            CypressProxyLogger().WithTag("BanService"));

        CrossClusterReplicatedState_ = CreateCrossClusterReplicatedState(
            CreateCrossClusterClient(
                Bootstrap_->GetNativeConnection(),
                replicatedStateConfig,
                NApi::NNative::TClientOptions::FromUser(replicatedStateConfig->User)),
            CrossClusterReplicaLockWaiter_,
            replicatedStateConfig, CypressProxyLogger().WithTag("BanService"));

        WaitFor(CrossClusterReplicatedState_->ValidateStateDirectories())
            .ThrowOnError();

        CrossClusterReplicaLockWaiter_->Start();

        BanCacheRefreshExecutor_->SetPeriod(GetDynamicConfig()->CacheRefreshPeriod);
        auto result = BanCacheRefreshExecutor_->StartAndGetFirstExecutedEvent();
        BanCacheRefreshExecutor_->ScheduleOutOfBand();
        WaitFor(std::move(result))
            .ThrowOnError();

        IsEnabled_.store(true);
    }

    void Disable()
    {
        YT_ASSERT_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        if (!IsEnabled_.load()) {
            THROW_ERROR_EXCEPTION("Attempt to disable a non-running ban service");
        }
        YT_LOG_DEBUG("Disabling ban service");

        IsEnabled_.store(false);
        WaitFor(BanCacheRefreshExecutor_->Stop())
            .ThrowOnError();
        WaitFor(CrossClusterReplicaLockWaiter_->Stop())
            .ThrowOnError();
        BanCache_.Store(TBanCachePtr());
    }

    bool IsRunning() const
    {
        return IsEnabled_.load() && UseInObjectService_.load();
    }

    void ThrowIfDisabled() const
    {
        if (!IsEnabled_.load()) {
            THROW_ERROR_EXCEPTION("Ban service is disabled");
        }
    }

    const TBanServiceDynamicConfigPtr& GetDynamicConfig() const
    {
        return Config_;
    }

    NRpc::TServiceDescriptor GetDescriptor() const
    {
        return NRpc::TServiceDescriptor("BanService")
            .SetProtocolVersion(0);
    }

    std::string GetTag() const
    {
        auto identity = NRpc::GetCurrentAuthenticationIdentity();
        return Format("%v;%v;%v", identity.User, NNet::GetLocalHostName(), GetCurrentFiberId());
    }
};

IBanServicePtr CreateBanService(IBootstrap* bootstrap)
{
    return New<TBanService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // NYT::NCypressProxy
