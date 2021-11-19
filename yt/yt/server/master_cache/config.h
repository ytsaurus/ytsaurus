#pragma once

#include <yt/yt/server/lib/chaos_cache/public.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/core/bus/tcp/public.h>

namespace NYT::NMasterCache {

////////////////////////////////////////////////////////////////////////////////

class TMasterCacheConfig
    : public TDeprecatedServerConfig
{
public:
    bool AbortOnUnrecognizedOptions;

    NBus::TTcpBusConfigPtr BusClient;

    NApi::NNative::TConnectionConfigPtr ClusterConnection;

    NObjectClient::TCachingObjectServiceConfigPtr CachingObjectService;

    NChaosCache::TChaosCacheConfigPtr ChaosCache;

    TMasterCacheConfig();
};

DEFINE_REFCOUNTED_TYPE(TMasterCacheConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMasterCache
