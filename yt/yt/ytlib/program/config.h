#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_serializable.h>
#include <yt/yt/core/ytree/yson_struct.h>

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

class TTCMallocConfig
    : public virtual NYTree::TYsonStruct
{
public:
    i64 BackgroundReleaseRate;
    int MaxPerCpuCacheSize;

    REGISTER_YSON_STRUCT(TTCMallocConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTCMallocConfig)

////////////////////////////////////////////////////////////////////////////////

// YsonStruct version.
class TSingletonsConfig
    : public virtual NYTree::TYsonStruct
{
public:
    TDuration SpinWaitSlowPathLoggingThreshold;
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
    TTCMallocConfigPtr TCMalloc;

    REGISTER_YSON_STRUCT(TSingletonsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSingletonsConfig)

// YsonSerializable version. Keep it in sync with verstion above. Expected to die quite soon.
class TDeprecatedSingletonsConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TDuration SpinWaitSlowPathLoggingThreshold;
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
    TTCMallocConfigPtr TCMalloc;

    TDeprecatedSingletonsConfig();
};

DEFINE_REFCOUNTED_TYPE(TDeprecatedSingletonsConfig)

////////////////////////////////////////////////////////////////////////////////

// YsonStruct version.
class TSingletonsDynamicConfig
    : public virtual NYTree::TYsonStruct
{
public:
    std::optional<TDuration> SpinWaitSlowPathLoggingThreshold;
    NYTAlloc::TYTAllocConfigPtr YTAlloc;
    NBus::TTcpDispatcherDynamicConfigPtr TcpDispatcher;
    NRpc::TDispatcherDynamicConfigPtr RpcDispatcher;
    NChunkClient::TDispatcherDynamicConfigPtr ChunkClientDispatcher;
    NLogging::TLogManagerDynamicConfigPtr Logging;
    NTracing::TJaegerTracerDynamicConfigPtr Jaeger;
    TRpcConfigPtr Rpc;
    TTCMallocConfigPtr TCMalloc;

    REGISTER_YSON_STRUCT(TSingletonsDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSingletonsDynamicConfig)

// YsonSerializable version. Keep it in sync with verstion above. Expected to die quite soon.
class TDeprecatedSingletonsDynamicConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    std::optional<TDuration> SpinWaitSlowPathLoggingThreshold;
    NYTAlloc::TYTAllocConfigPtr YTAlloc;
    NBus::TTcpDispatcherDynamicConfigPtr TcpDispatcher;
    NRpc::TDispatcherDynamicConfigPtr RpcDispatcher;
    NChunkClient::TDispatcherDynamicConfigPtr ChunkClientDispatcher;
    NLogging::TLogManagerDynamicConfigPtr Logging;
    NTracing::TJaegerTracerDynamicConfigPtr Jaeger;
    TRpcConfigPtr Rpc;
    TTCMallocConfigPtr TCMalloc;

    TDeprecatedSingletonsDynamicConfig();
};

DEFINE_REFCOUNTED_TYPE(TDeprecatedSingletonsDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

// YsonStruct version.
class TDiagnosticDumpConfig
    : public virtual NYTree::TYsonStruct
{
public:
    std::optional<TDuration> YTAllocDumpPeriod;
    std::optional<TDuration> RefCountedTrackerDumpPeriod;

    REGISTER_YSON_STRUCT(TDiagnosticDumpConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiagnosticDumpConfig)

// YsonSerializable version. Keep it in sync with verstion above. Expected to die quite soon.
class TDeprecatedDiagnosticDumpConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    std::optional<TDuration> YTAllocDumpPeriod;
    std::optional<TDuration> RefCountedTrackerDumpPeriod;

    TDeprecatedDiagnosticDumpConfig();
};

DEFINE_REFCOUNTED_TYPE(TDeprecatedDiagnosticDumpConfig)

////////////////////////////////////////////////////////////////////////////////

// NB: These functions should not be called from bootstrap
// config validator since logger is not set up yet.
void WarnForUnrecognizedOptions(
    const NLogging::TLogger& logger,
    const NYTree::TYsonStructPtr& config);

void WarnForUnrecognizedOptions(
    const NLogging::TLogger& logger,
    const NYTree::TYsonSerializablePtr& config);

void AbortOnUnrecognizedOptions(
    const NLogging::TLogger& logger,
    const NYTree::TYsonStructPtr& config);

void AbortOnUnrecognizedOptions(
    const NLogging::TLogger& logger,
    const NYTree::TYsonSerializablePtr& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
