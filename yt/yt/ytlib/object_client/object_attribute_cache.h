#pragma once

#include "public.h"

#include "config.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/client/api/client.h>

#include <yt/client/misc/public.h>

#include <yt/core/misc/async_expiring_cache.h>

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

class TObjectAttributeCache
    : public TAsyncExpiringCache<NYPath::TYPath, NYTree::IAttributeDictionaryPtr>
{
public:
    DEFINE_BYREF_RO_PROPERTY(std::vector<TString>, AttributeNames);

public:
    TObjectAttributeCache(
        TObjectAttributeCacheConfigPtr config,
        std::vector<TString> attributeNames,
        NApi::NNative::IClientPtr client,
        IInvokerPtr invoker,
        NLogging::TLogger logger = {},
        NProfiling::TProfiler profiler = {});

    // Method with signature similar to GetMany which goes directly to master.
    static TFuture<std::vector<TErrorOr<NYTree::IAttributeDictionaryPtr>>> GetFromClient(
        const std::vector<NYPath::TYPath>& paths,
        const NApi::NNative::IClientPtr& client,
        const std::vector<TString>& attributeNames,
        const NApi::TMasterReadOptions& options = NApi::TMasterReadOptions{});

protected:
    virtual TFuture<NYTree::IAttributeDictionaryPtr> DoGet(
        const NYPath::TYPath& path,
        bool isPeriodicUpdate) noexcept override;
    virtual TFuture<std::vector<TErrorOr<NYTree::IAttributeDictionaryPtr>>> DoGetMany(
        const std::vector<NYPath::TYPath>& paths,
        bool isPeriodicUpdate) noexcept override;

private:
    const TObjectAttributeCacheConfigPtr Config_;
    const NLogging::TLogger Logger;

    NApi::NNative::IClientPtr Client_;
    IInvokerPtr Invoker_;
};

DEFINE_REFCOUNTED_TYPE(TObjectAttributeCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
