#include "alien_cluster_client_cache.h"

#include "alien_cluster_client_cache_base.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

namespace NYT::NTabletNode {

using namespace NYT::NApi::NNative;

////////////////////////////////////////////////////////////////////////////////

class TAlienClusterClientCache
    : public IAlienClusterClientCache
    , public TAlienClusterClientCacheBase

{
public:
    TAlienClusterClientCache(
        NApi::NNative::IConnectionPtr localConnection,
        NYT::NApi::TClientOptions clientOptions,
        TDuration evictionPeriod)
    : TAlienClusterClientCacheBase(evictionPeriod)
    , LocalConnection_(std::move(localConnection))
    , LocalClient_(LocalConnection_->CreateNativeClient(clientOptions))
    , ClientOptions_(std::move(clientOptions))
    { }

    NApi::NNative::IClientPtr GetClient(const std::string& clusterName) override
    {
        auto alienConnection = LocalConnection_->GetClusterDirectory()->FindConnection(clusterName);
        if (!alienConnection) {
            return nullptr;
        }

        auto now = Now();
        auto guard = Guard(CachedClientsLock_);
        CheckAndRemoveExpired(now, false);

        auto it = CachedClients_.try_emplace(clusterName, nullptr);
        if (it.second || it.first->second->GetConnection()->IsTerminated()) {
            it.first->second = alienConnection->CreateNativeClient(ClientOptions_);
        }

        return it.first->second;
    }

    void ForceRemoveExpired() override
    {
        auto now = Now();
        auto guard = Guard(CachedClientsLock_);
        CheckAndRemoveExpired(now, true);
    }

    const NApi::NNative::IClientPtr& GetLocalClient() const override
    {
        return LocalClient_;
    }

    TDuration GetEvictionPeriod() const override
    {
        return TAlienClusterClientCacheBase::GetEvictionPeriod();
    }

private:
    const NApi::NNative::IConnectionPtr LocalConnection_;
    const NApi::NNative::IClientPtr LocalClient_;
    const NApi::TClientOptions ClientOptions_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, CachedClientsLock_);
};

////////////////////////////////////////////////////////////////////////////////

IAlienClusterClientCachePtr CreateAlienClusterClientCache(
    IConnectionPtr localConnection,
    NApi::TClientOptions clientOptions,
    TDuration evictionPeriod)
{
    return New<TAlienClusterClientCache>(
        std::move(localConnection),
        std::move(clientOptions),
        evictionPeriod);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
