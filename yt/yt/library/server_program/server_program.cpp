#include "server_program.h"

#include <yt/yt/library/fusion/service_directory.h>

#include <yt/yt/library/containers/porto_resource_tracker.h>

#include <yt/yt/library/disk_manager/hotswap_manager.h>

#include <yt/yt/library/coredumper/coredumper.h>

#include <yt/yt/library/disk_manager/hotswap_manager.h>

#include <yt/yt/library/profiling/solomon/exporter.h>

#include <yt/yt/library/profiling/perf/event_counter_profiler.h>

#include <yt/yt/library/program/helpers.h>

#include <yt/yt/library/fusion/service_directory.h>

#include <yt/yt/core/misc/ref_counted_tracker_profiler.h>

#include <library/cpp/yt/phdr_cache/phdr_cache.h>

#include <library/cpp/yt/mlock/mlock.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TServerProgramBase::TServerProgramBase()
    : ServiceDirectory_(NFusion::CreateServiceDirectory())
{ }

void TServerProgramBase::SetMainThreadName(const std::string& name)
{
    MainThreadName_ = name;
}

const std::string& TServerProgramBase::GetMainThreadName() const
{
    return MainThreadName_;
}

void TServerProgramBase::ValidateOpts()
{ }

void TServerProgramBase::TweakConfig()
{ }

void TServerProgramBase::SleepForever()
{
    Sleep(TDuration::Max());
    YT_ABORT();
}

NFusion::IServiceLocatorPtr TServerProgramBase::GetServiceLocator() const
{
    return ServiceDirectory_;
}

NFusion::IServiceDirectoryPtr TServerProgramBase::GetServiceDirectory() const
{
    return ServiceDirectory_;
}

void TServerProgramBase::Configure(const TServerProgramConfigPtr& config)
{
    ConfigureUids();

    ConfigureIgnoreSigpipe();

    ConfigureCrashHandler();

    ConfigureExitZeroOnSigterm();

    EnablePhdrCache();

    ConfigureAllocator();

    MlockFileMappings();

    ConfigureSingletons(config);

    NProfiling::EnablePerfEventCounterProfiling();

    if (config->EnablePortoResourceTracker) {
        NContainers::EnablePortoResourceTracker(config->PodSpec);
    }

    if (config->EnableRefCountedTrackerProfiling) {
        EnableRefCountedTrackerProfiling();
    }

    auto serviceDirectory = GetServiceDirectory();

    if (config->CoreDumper) {
        serviceDirectory->RegisterService(NCoreDump::CreateCoreDumper(config->CoreDumper));
    }

    {
        auto solomonExporter = New<NProfiling::TSolomonExporter>(config->SolomonExporter);
        solomonExporter->Start();
        serviceDirectory->RegisterService(std::move(solomonExporter));
    }

    if (config->HotswapManager) {
        serviceDirectory->RegisterService(NDiskManager::CreateHotswapManager(config->HotswapManager));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
