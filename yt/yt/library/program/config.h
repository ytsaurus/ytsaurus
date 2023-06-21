#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_serializable.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/ytalloc/config.h>

#include <yt/yt/core/net/config.h>

#include <yt/yt/core/rpc/config.h>
#include <yt/yt/core/rpc/grpc/config.h>

#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/core/logging/config.h>

#include <yt/yt/core/tracing/config.h>

#include <yt/yt/core/service_discovery/yp/config.h>

#include <yt/yt/library/profiling/solomon/exporter.h>

#include <yt/yt/library/containers/config.h>

#include <yt/yt/library/tracing/jaeger/tracer.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TRpcConfig
    : public NYTree::TYsonStruct
{
public:
    NTracing::TTracingConfigPtr Tracing;

    REGISTER_YSON_STRUCT(TRpcConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRpcConfig)

////////////////////////////////////////////////////////////////////////////////

class TTCMallocConfig
    : public virtual NYTree::TYsonStruct
{
public:
    i64 BackgroundReleaseRate;
    int MaxPerCpuCacheSize;

    i64 AggressiveReleaseThreshold;
    i64 AggressiveReleaseSize;
    TDuration AggressiveReleasePeriod;

    //! Approximately 1/#GuardedSamplingRate of all allocations of
    //! size <= 256 KiB will be under GWP-ASAN.
    std::optional<i64> GuardedSamplingRate;

    REGISTER_YSON_STRUCT(TTCMallocConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTCMallocConfig)

////////////////////////////////////////////////////////////////////////////////

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
    NRpc::NGrpc::TDispatcherConfigPtr GrpcDispatcher;
    NServiceDiscovery::NYP::TServiceDiscoveryConfigPtr YPServiceDiscovery;
    NProfiling::TSolomonExporterConfigPtr SolomonExporter;
    NLogging::TLogManagerConfigPtr Logging;
    NTracing::TJaegerTracerConfigPtr Jaeger;
    TRpcConfigPtr Rpc;
    TTCMallocConfigPtr TCMalloc;
    bool EnableRefCountedTrackerProfiling;
    bool EnableResourceTracker;
    bool EnablePortoResourceTracker;
    std::optional<double> ResourceTrackerVCpuFactor;
    NContainers::TPodSpecConfigPtr PodSpec;

    REGISTER_YSON_STRUCT(TSingletonsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSingletonsConfig)

////////////////////////////////////////////////////////////////////////////////

class TSingletonsDynamicConfig
    : public virtual NYTree::TYsonStruct
{
public:
    std::optional<TDuration> SpinWaitSlowPathLoggingThreshold;
    NYTAlloc::TYTAllocConfigPtr YTAlloc;
    NBus::TTcpDispatcherDynamicConfigPtr TcpDispatcher;
    NRpc::TDispatcherDynamicConfigPtr RpcDispatcher;
    NLogging::TLogManagerDynamicConfigPtr Logging;
    NTracing::TJaegerTracerDynamicConfigPtr Jaeger;
    TRpcConfigPtr Rpc;
    TTCMallocConfigPtr TCMalloc;

    REGISTER_YSON_STRUCT(TSingletonsDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSingletonsDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

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
