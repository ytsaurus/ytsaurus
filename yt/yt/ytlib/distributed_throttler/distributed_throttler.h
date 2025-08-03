#pragma once

#include "public.h"

#include <yt/yt/core/actions/invoker.h>

#include <yt/yt/core/rpc/channel.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/ytlib/discovery_client/public.h>

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NDistributedThrottler {

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

DECLARE_REFCOUNTED_CLASS(TThrottlerToGlobalUsage)

class TThrottlerToGlobalUsage final
    : public THashMap<TThrottlerId, TThrottlerUsage>
{ };

DEFINE_REFCOUNTED_TYPE(TThrottlerToGlobalUsage)

////////////////////////////////////////////////////////////////////////////////

struct IDistributedThrottlerFactory
    : public virtual TRefCounted
{
    virtual NConcurrency::IReconfigurableThroughputThrottlerPtr GetOrCreateThrottler(
        const std::string& throttlerId,
        NConcurrency::TThroughputThrottlerConfigPtr throttlerConfig,
        TDuration throttleRpcTimeout = DefaultThrottleRpcTimeout) = 0;

    virtual void Reconfigure(TDistributedThrottlerConfigPtr config) = 0;

    //! Only the leader has non-empty throttler usages collected over all members.
    virtual TIntrusivePtr<const TThrottlerToGlobalUsage> TryGetThrottlerToGlobalUsage() const = 0;
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
    std::string address,
    NLogging::TLogger logger,
    NRpc::IAuthenticatorPtr authenticator,
    NProfiling::TProfiler profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedThrottler
