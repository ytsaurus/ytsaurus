#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_serializable.h>

#include <yt/yt/core/ytalloc/config.h>

#include <yt/yt/core/net/config.h>

#include <yt/yt/core/rpc/config.h>

#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/core/logging/config.h>

#include <yt/yt/core/profiling/config.h>

#include <yt/yt/core/tracing/config.h>

#include <yt/yt/core/service_discovery/yp/config.h>

#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt/library/profiling/solomon/exporter.h>

#include <yt/yt/library/tracing/jaeger/tracer.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TRpcConfig
    : public NYTree::TYsonSerializable
{
public:
    NTracing::TTracingConfigPtr Tracing;

    TRpcConfig();
};

DEFINE_REFCOUNTED_TYPE(TRpcConfig)

////////////////////////////////////////////////////////////////////////////////

class TSingletonsConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TDuration SpinlockHiccupThreshold;
    NYTAlloc::TYTAllocConfigPtr YTAlloc;
    THashMap<TString, int> FiberStackPoolSizes;
    NNet::TAddressResolverConfigPtr AddressResolver;
    NBus::TTcpDispatcherConfigPtr TcpDispatcher;
    NRpc::TDispatcherConfigPtr RpcDispatcher;
    NServiceDiscovery::NYP::TServiceDiscoveryConfigPtr YPServiceDiscovery;
    NChunkClient::TDispatcherConfigPtr ChunkClientDispatcher;
    NProfiling::TProfileManagerConfigPtr ProfileManager;
    NProfiling::TSolomonExporterConfigPtr SolomonExporter;
    NLogging::TLogManagerConfigPtr Logging;
    NTracing::TJaegerTracerConfigPtr Jaeger;
    TRpcConfigPtr Rpc;

    TSingletonsConfig();
};

DEFINE_REFCOUNTED_TYPE(TSingletonsConfig)

////////////////////////////////////////////////////////////////////////////////

class TSingletonsDynamicConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    std::optional<TDuration> SpinlockHiccupThreshold;
    NYTAlloc::TYTAllocConfigPtr YTAlloc;
    NBus::TTcpDispatcherDynamicConfigPtr TcpDispatcher;
    NRpc::TDispatcherDynamicConfigPtr RpcDispatcher;
    NChunkClient::TDispatcherDynamicConfigPtr ChunkClientDispatcher;
    NProfiling::TProfileManagerDynamicConfigPtr ProfileManager;
    NLogging::TLogManagerDynamicConfigPtr Logging;
    NTracing::TJaegerTracerDynamicConfigPtr Jaeger;
    TRpcConfigPtr Rpc;

    TSingletonsDynamicConfig();
};

DEFINE_REFCOUNTED_TYPE(TSingletonsDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TDiagnosticDumpConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    std::optional<TDuration> YTAllocDumpPeriod;
    std::optional<TDuration> RefCountedTrackerDumpPeriod;

    TDiagnosticDumpConfig();
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
