#include "alien_cluster_client_cache.h"

#include "alien_cluster_client_cache_base.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

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
        IConnectionPtr localConnection,
        TClientOptions clientOptions,
        TDuration evictionPeriod)
        : TAlienClusterClientCacheBase(evictionPeriod)
        , LocalConnection_(std::move(localConnection))
        , LocalClient_(LocalConnection_->CreateNativeClient(clientOptions))
        , ClientOptions_(std::move(clientOptions))
    { }

    IClientPtr GetClient(const std::string& clusterName) override
    {
        auto alienConnection = LocalConnection_->GetClusterDirectory()->FindConnection(clusterName);
        if (!alienConnection) {
            return nullptr;
        }

        auto now = Now();
        auto guard = Guard(CachedClientsLock_);
        CheckAndRemoveExpired(now, false);

        auto [it, inserted] = CachedClients_.try_emplace(clusterName, nullptr);
        if (inserted || it->second->GetConnection()->IsTerminated()) {
            it->second = alienConnection->CreateNativeClient(ClientOptions_);
        }

        return it->second;
    }

    void ForceRemoveExpired() override
    {
        auto now = Now();
        auto guard = Guard(CachedClientsLock_);
        CheckAndRemoveExpired(now, true);
    }

    const IClientPtr& GetLocalClient() const override
    {
        return LocalClient_;
    }

    TDuration GetEvictionPeriod() const override
    {
        return TAlienClusterClientCacheBase::GetEvictionPeriod();
    }

private:
    const IConnectionPtr LocalConnection_;
    const IClientPtr LocalClient_;
    const TClientOptions ClientOptions_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, CachedClientsLock_);
};

////////////////////////////////////////////////////////////////////////////////

IAlienClusterClientCachePtr CreateAlienClusterClientCache(
    IConnectionPtr localConnection,
    TClientOptions clientOptions,
    TDuration evictionPeriod)
{
    return New<TAlienClusterClientCache>(
        std::move(localConnection),
        std::move(clientOptions),
        evictionPeriod);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
