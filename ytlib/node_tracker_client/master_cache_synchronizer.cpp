#include "master_cache_synchronizer.h"

#include "public.h"
#include "config.h"

#include <yt/ytlib/node_tracker_client/master_cache_synchronizer.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/client/api/connection.h>
#include <yt/client/api/client.h>

#include <yt/client/node_tracker_client/private.h>

#include <yt/core/rpc/dispatcher.h>
#include <yt/core/rpc/roaming_channel.h>
#include <yt/core/rpc/reconfigurable_roaming_channel_provider.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/scheduler.h>

namespace NYT::NNodeTrackerClient {

using namespace NConcurrency;
using namespace NApi;
using namespace NYTree;
using namespace NObjectClient;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = NodeTrackerClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TMasterCacheSynchronizer::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TDuration syncPeriod,
        TWeakPtr<IConnection> connection)
        : Connection_(std::move(connection))
        , SyncExecutor_(New<TPeriodicExecutor>(
            NRpc::TDispatcher::Get()->GetLightInvoker(),
            BIND(&TImpl::OnSync, MakeWeak(this)),
            syncPeriod))
    { }

    void Start()
    {
        SyncExecutor_->Start();
    }

    TFuture<void> Stop()
    {
        return SyncExecutor_->Stop();
    }

    std::vector<TString> GetAddresses()
    {
        auto guard = Guard(SpinLock_);
        return Addresses_;
    }

    DEFINE_SIGNAL(void(const std::vector<TString>&), MasterCacheNodeAddressesUpdated);

private:
    const TWeakPtr<IConnection> Connection_;
    const TPeriodicExecutorPtr SyncExecutor_;

    TSpinLock SpinLock_;

    std::vector<TString> Addresses_;

    void OnSync()
    {
        try {
            auto connection = Connection_.Lock();
            if (!connection) {
                return;
            }

            auto client = connection->CreateClient(TClientOptions(NSecurityClient::RootUserName));

            YT_LOG_DEBUG("Started synchronizing master cache node list");

            TGetClusterMetaOptions options;
            options.ReadFrom = EMasterChannelKind::SecondLevelCache;
            options.PopulateMasterCacheNodeAddresses = true;

            auto meta = WaitFor(client->GetClusterMeta(options).ToImmediatelyCancelable())
                .ValueOrThrow();

            YT_LOG_DEBUG("Received master cache node list (Addresses: %v)", meta.MasterCacheNodeAddresses);

            {
                auto guard = Guard(SpinLock_);
                if (Addresses_ == meta.MasterCacheNodeAddresses) {
                    YT_LOG_DEBUG("Master cache node list has not changed");
                    return;
                }

                YT_LOG_INFO("Master cache node list has been changed (OldAddresses: %v, NewAddresses: %v)",
                    Addresses_,
                    meta.MasterCacheNodeAddresses);

                Addresses_ = std::move(meta.MasterCacheNodeAddresses);
            }

            MasterCacheNodeAddressesUpdated_.Fire(GetAddresses());

            YT_LOG_DEBUG("Finished synchronizing master cache node list");
        } catch (const std::exception& ex) {
            YT_LOG_INFO(ex, "Failed synchronizing master cache node list");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TMasterCacheSynchronizer::TMasterCacheSynchronizer(
    const TDuration& syncPeriod,
    const TWeakPtr<IConnection>& connection)
    : Impl_(New<TImpl>(syncPeriod, connection))
{ }

void TMasterCacheSynchronizer::Start()
{
    Impl_->Start();
}

TFuture<void> TMasterCacheSynchronizer::Stop()
{
    return Impl_->Stop();
}

std::vector<TString> TMasterCacheSynchronizer::GetAddresses()
{
    return Impl_->GetAddresses();
}

DELEGATE_SIGNAL(TMasterCacheSynchronizer, void(const std::vector<TString>&), MasterCacheNodeAddressesUpdated, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient
