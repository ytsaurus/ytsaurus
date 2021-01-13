#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

#include <yt/core/ytalloc/config.h>

#include <yt/core/net/config.h>

#include <yt/core/rpc/config.h>

#include <yt/core/logging/config.h>

#include <yt/core/tracing/config.h>

#include <yt/core/profiling/config.h>

#include <yt/core/service_discovery/yp/config.h>

#include <yt/ytlib/chunk_client/config.h>

#include <yt/yt/library/profiling/solomon/exporter.h>

#include <yt/yt/library/tracing/jaeger/tracer.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TSingletonsConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TDuration SpinlockHiccupThreshold;
    NYTAlloc::TYTAllocConfigPtr YTAlloc;
    THashMap<TString, int> FiberStackPoolSizes;
    NNet::TAddressResolverConfigPtr AddressResolver;
    NRpc::TDispatcherConfigPtr RpcDispatcher;
    NServiceDiscovery::NYP::TServiceDiscoveryConfigPtr YPServiceDiscovery;
    NChunkClient::TDispatcherConfigPtr ChunkClientDispatcher;
    NProfiling::TProfileManagerConfigPtr ProfileManager;
    NProfiling::TSolomonExporterConfigPtr SolomonExporter;
    NLogging::TLogManagerConfigPtr Logging;
    NTracing::TJaegerTracerConfigPtr Jaeger;

    TSingletonsConfig()
    {
        RegisterParameter("spinlock_hiccup_threshold", SpinlockHiccupThreshold)
            .Default(TDuration::MicroSeconds(100));
        RegisterParameter("yt_alloc", YTAlloc)
            .DefaultNew();
        RegisterParameter("fiber_stack_pool_sizes", FiberStackPoolSizes)
            .Default({});
        RegisterParameter("address_resolver", AddressResolver)
            .DefaultNew();
        RegisterParameter("rpc_dispatcher", RpcDispatcher)
            .DefaultNew();
        RegisterParameter("yp_service_discovery", YPServiceDiscovery)
            .DefaultNew();
        RegisterParameter("chunk_client_dispatcher", ChunkClientDispatcher)
            .DefaultNew();
        RegisterParameter("profile_manager", ProfileManager)
            .DefaultNew();
        RegisterParameter("solomon_exporter", SolomonExporter)
            .DefaultNew();
        RegisterParameter("logging", Logging)
            .Default(NLogging::TLogManagerConfig::CreateDefault());
        RegisterParameter("jaeger", Jaeger)
            .DefaultNew();

        // COMPAT(prime@): backward compatible config for CHYT
        RegisterPostprocessor([this] {
            if (!ProfileManager->GlobalTags.empty()) {
                SolomonExporter->Host = "";
                SolomonExporter->InstanceTags = ProfileManager->GlobalTags;
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TSingletonsConfig)

////////////////////////////////////////////////////////////////////////////////

class TDiagnosticDumpConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    std::optional<TDuration> YTAllocDumpPeriod;
    std::optional<TDuration> RefCountedTrackerDumpPeriod;

    TDiagnosticDumpConfig()
    {
        RegisterParameter("yt_alloc_dump_period", YTAllocDumpPeriod)
            .Default();
        RegisterParameter("ref_counted_tracker_dump_period", RefCountedTrackerDumpPeriod)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TDiagnosticDumpConfig)

////////////////////////////////////////////////////////////////////////////////

// NB: These functions should not be called from bootstrap
// config validator since logger is not set up yet.
void WarnForUnrecognizedOptions(
    const NLogging::TLogger& logger,
    const NYTree::TYsonSerializablePtr& config);

void AbortOnUnrecognizedOptions(
    const NLogging::TLogger& logger,
    const NYTree::TYsonSerializablePtr& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
