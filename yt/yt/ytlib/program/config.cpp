#include "config.h"

namespace NYT {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TSingletonsConfig::TSingletonsConfig()
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

////////////////////////////////////////////////////////////////////////////////

TSingletonsDynamicConfig::TSingletonsDynamicConfig()
{
    RegisterParameter("spinlock_hiccup_threshold", SpinlockHiccupThreshold)
        .Optional();
    RegisterParameter("yt_alloc", YTAlloc)
        .Optional();
    RegisterParameter("rpc_dispatcher", RpcDispatcher)
        .DefaultNew();
    RegisterParameter("chunk_client_dispatcher", ChunkClientDispatcher)
        .DefaultNew();
    RegisterParameter("profile_manager", ProfileManager)
        .Default();
    RegisterParameter("logging", Logging)
        .DefaultNew();
    RegisterParameter("jaeger", Jaeger)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

TDiagnosticDumpConfig::TDiagnosticDumpConfig()
{
    RegisterParameter("yt_alloc_dump_period", YTAllocDumpPeriod)
        .Default();
    RegisterParameter("ref_counted_tracker_dump_period", RefCountedTrackerDumpPeriod)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void WarnForUnrecognizedOptions(
    const NLogging::TLogger& logger,
    const NYTree::TYsonSerializablePtr& config)
{
    const auto& Logger = logger;
    auto unrecognized = config->GetUnrecognizedRecursively();
    if (unrecognized && unrecognized->GetChildCount() > 0) {
        YT_LOG_WARNING("Bootstrap config contains unrecognized options (Unrecognized: %v)",
            ConvertToYsonString(unrecognized, NYson::EYsonFormat::Text));
    }
}

void AbortOnUnrecognizedOptions(
    const NLogging::TLogger& logger,
    const NYTree::TYsonSerializablePtr& config)
{
    const auto& Logger = logger;
    auto unrecognized = config->GetUnrecognizedRecursively();
    if (unrecognized && unrecognized->GetChildCount() > 0) {
        YT_LOG_ERROR("Bootstrap config contains unrecognized options, terminating (Unrecognized: %v)",
            ConvertToYsonString(unrecognized, NYson::EYsonFormat::Text));
        YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

