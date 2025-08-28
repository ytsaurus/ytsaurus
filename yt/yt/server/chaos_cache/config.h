#pragma once

#include <yt/yt/server/lib/chaos_cache/public.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/server/lib/cypress_registrar/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/bus/tcp/public.h>

#include <yt/yt/library/dynamic_config/public.h>

#include <yt/yt/library/server_program/config.h>

namespace NYT::NChaosCache {

////////////////////////////////////////////////////////////////////////////////

struct TChaosCacheBootstrapConfig
    : public NServer::TNativeServerBootstrapConfig
{
    bool AbortOnUnrecognizedOptions;

    NBus::TBusConfigPtr BusClient;

    NChaosCache::TChaosCacheConfigPtr ChaosCache;

    TCypressRegistrarConfigPtr CypressRegistrar;

    NDynamicConfig::TDynamicConfigManagerConfigPtr DynamicConfigManager;

    REGISTER_YSON_STRUCT(TChaosCacheBootstrapConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChaosCacheBootstrapConfig)

////////////////////////////////////////////////////////////////////////////////

struct TChaosCacheProgramConfig
    : public TChaosCacheBootstrapConfig
    , public TServerProgramConfig
{
    REGISTER_YSON_STRUCT(TChaosCacheProgramConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChaosCacheProgramConfig)

////////////////////////////////////////////////////////////////////////////////

struct TChaosCacheDynamicConfig
    : public TSingletonsDynamicConfig
{
    REGISTER_YSON_STRUCT(TChaosCacheDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChaosCacheDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosCache
