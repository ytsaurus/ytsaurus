#pragma once

#include "public.h"

#include "config.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/client/misc/public.h>

#include <yt/core/misc/async_expiring_cache.h>

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

class TObjectAttributeCache
    : public TAsyncExpiringCache<NYPath::TYPath, TAttributeMap>
{
public:
    TObjectAttributeCache(
        TObjectAttributeCacheConfigPtr config,
        std::vector<TString> attributes,
        NApi::NNative::IClientPtr client,
        IInvokerPtr invoker,
        const NLogging::TLogger& logger,
        NProfiling::TProfiler profiler = {});

protected:
    virtual TFuture<TAttributeMap> DoGet(const NYPath::TYPath& path) override;
    virtual TFuture<std::vector<TErrorOr<TAttributeMap>>> DoGetMany(
        const std::vector<NYPath::TYPath>& paths) override;

private:
    const TObjectAttributeCacheConfigPtr Config_;
    const std::vector<TString> Attributes_;
    NApi::NNative::IClientPtr Client_;
    IInvokerPtr Invoker_;
    NLogging::TLogger Logger;
};

DEFINE_REFCOUNTED_TYPE(TObjectAttributeCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
