#pragma once

#include "yt_client_factory.h"

#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/client/cache/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TYTClientFactory
    : public IYTClientFactory
{
public:
    YT_FLOW_EXTEND_PARAMETERS(NClient::NCache::TClientsCacheConfig);

    TYTClientFactory(TResourceContextPtr context, TDynamicResourceContextPtr dynamicContext);

    NApi::IClientPtr GetClient(TStringBuf clusterUrl) override;

private:
    NClient::NCache::IClientsCachePtr ClientsCache_;
};

DEFINE_REFCOUNTED_TYPE(TYTClientFactory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
