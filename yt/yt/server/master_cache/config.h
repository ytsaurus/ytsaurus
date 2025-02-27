#pragma once

#include <yt/yt/server/lib/chaos_cache/public.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/server/lib/cypress_registrar/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/core/bus/tcp/public.h>

#include <yt/yt/library/dynamic_config/public.h>

#include <yt/yt/library/server_program/config.h>

namespace NYT::NMasterCache {

////////////////////////////////////////////////////////////////////////////////

struct TMasterCacheBootstrapConfig
    : public NServer::TNativeServerBootstrapConfig
{
    bool AbortOnUnrecognizedOptions;

    NBus::TBusConfigPtr BusClient;

    NObjectClient::TCachingObjectServiceConfigPtr CachingObjectService;

    NChaosCache::TChaosCacheConfigPtr ChaosCache;

    TCypressRegistrarConfigPtr CypressRegistrar;

    NDynamicConfig::TDynamicConfigManagerConfigPtr DynamicConfigManager;

    REGISTER_YSON_STRUCT(TMasterCacheBootstrapConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterCacheBootstrapConfig)

////////////////////////////////////////////////////////////////////////////////

struct TMasterCacheProgramConfig
    : public TMasterCacheBootstrapConfig
    , public TServerProgramConfig
{
    REGISTER_YSON_STRUCT(TMasterCacheProgramConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterCacheProgramConfig)

////////////////////////////////////////////////////////////////////////////////

struct TMasterCacheDynamicConfig
    : public TSingletonsDynamicConfig
{
    NObjectClient::TCachingObjectServiceDynamicConfigPtr CachingObjectService;

    REGISTER_YSON_STRUCT(TMasterCacheDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterCacheDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMasterCache
