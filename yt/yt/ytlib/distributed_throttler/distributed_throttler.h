#pragma once

#include "public.h"

#include <yt/yt/core/actions/invoker.h>

#include <yt/yt/core/rpc/channel.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/ytlib/discovery_client/public.h>

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

using TThrottlerId = TString;

////////////////////////////////////////////////////////////////////////////////

struct TThrottlerUsage
{
    double Rate = 0.0;
    double Limit = 0.0;
    i64 QueueTotalAmount = 0;
    TDuration MaxEstimatedOverdraftDuration = TDuration::Zero();
    TDuration MinEstimatedOverdraftDuration = TDuration::Max();
};

////////////////////////////////////////////////////////////////////////////////

struct IDistributedThrottlerFactory
    : public virtual TRefCounted
{
    virtual NConcurrency::IReconfigurableThroughputThrottlerPtr GetOrCreateThrottler(
        const TString& throttlerId,
        NConcurrency::TThroughputThrottlerConfigPtr throttlerConfig,
        TDuration throttleRpcTimeout = DefaultThrottleRpcTimeout) = 0;

    virtual void Reconfigure(TDistributedThrottlerConfigPtr config) = 0;

    //! Only the leader has non-empty throttler usages collected over all members.
    virtual std::shared_ptr<const THashMap<TThrottlerId, TThrottlerUsage>> TryGetThrottlerToGlobalUsage() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IDistributedThrottlerFactory)

IDistributedThrottlerFactoryPtr CreateDistributedThrottlerFactory(
    TDistributedThrottlerConfigPtr config,
    NRpc::IChannelFactoryPtr channelFactory,
    NApi::NNative::IConnectionPtr connection,
    IInvokerPtr invoker,
    NDiscoveryClient::TGroupId groupId,
    NDiscoveryClient::TMemberId memberId,
    NRpc::IServerPtr rpcServer,
    TString address,
    NLogging::TLogger logger,
    NRpc::IAuthenticatorPtr authenticator,
    NProfiling::TProfiler profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedThrottler
