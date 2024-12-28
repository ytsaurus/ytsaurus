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

class TMasterCacheBootstrapConfig
    : public NServer::TNativeServerBootstrapConfig
{
public:
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

class TMasterCacheProgramConfig
    : public TMasterCacheBootstrapConfig
    , public TServerProgramConfig
{
public:
    REGISTER_YSON_STRUCT(TMasterCacheProgramConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterCacheProgramConfig)

////////////////////////////////////////////////////////////////////////////////

class TMasterCacheDynamicConfig
    : public TSingletonsDynamicConfig
{
public:
    NObjectClient::TCachingObjectServiceDynamicConfigPtr CachingObjectService;

    REGISTER_YSON_STRUCT(TMasterCacheDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterCacheDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMasterCache
