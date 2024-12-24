#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/api/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IAlienClusterClientCache
    : public TRefCounted
{
    virtual NApi::NNative::IClientPtr GetClient(const std::string& clusterName) = 0;
    virtual void ForceRemoveExpired() = 0;
    virtual TDuration GetEvictionPeriod() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IAlienClusterClientCache)

IAlienClusterClientCachePtr CreateAlienClusterClientCache(
    NApi::NNative::IConnectionPtr localConnection,
    NYT::NApi::TClientOptions clientOptions,
    TDuration evictionPeriod
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
