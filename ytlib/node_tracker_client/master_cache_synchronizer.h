#pragma once

#include "public.h"

#include <yt/client/api/public.h>

#include <yt/core/rpc/roaming_channel.h>
#include <yt/core/rpc/public.h>

#include <yt/core/actions/public.h>
#include <yt/core/actions/signal.h>

namespace NYT::NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

class TMasterCacheSynchronizer
    : public TRefCounted
{
public:
    TMasterCacheSynchronizer(
        const TDuration& syncPeriod,
        const TWeakPtr<NApi::IConnection>& connection);

    void Start();
    TFuture<void> Stop();

    std::vector<TString> GetAddresses();

    DECLARE_SIGNAL(void(const std::vector<TString>&), MasterCacheNodeAddressesUpdated);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TMasterCacheSynchronizer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient
