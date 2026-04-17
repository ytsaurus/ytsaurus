#pragma once

#include "client.h"

#include <yt/yt/core/concurrency/two_level_fair_share_thread_pool.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(ICypressPoolWeightProvider)

struct ICypressPoolWeightProvider
    : public NConcurrency::IPoolWeightProvider
{
    virtual void SetOverrides(THashMap<std::string, double> weights) = 0;

    virtual void SetClient(TWeakPtr<IClient> client) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICypressPoolWeightProvider)

////////////////////////////////////////////////////////////////////////////////

ICypressPoolWeightProviderPtr CreateCachingCypressPoolWeightProvider(
    TAsyncExpiringCacheConfigPtr config,
    TWeakPtr<IClient> client,
    IInvokerPtr invoker,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
