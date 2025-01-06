#pragma once

#include <yt/yt/library/containers/config.h>

#include <yt/yt/library/disk_manager/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/misc/configurable_singleton_def.h>

#include <yt/yt/library/coredumper/config.h>

#include <yt/yt/library/profiling/solomon/config.h>

#include <yt/yt/library/disk_manager/config.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TServerProgramConfig
    : public TSingletonsConfig
{
    bool EnablePortoResourceTracker;
    NContainers::TPodSpecConfigPtr PodSpec;

    bool EnableRefCountedTrackerProfiling;

    NCoreDump::TCoreDumperConfigPtr CoreDumper;

    NProfiling::TSolomonExporterConfigPtr SolomonExporter;

    NDiskManager::THotswapManagerConfigPtr HotswapManager;

    REGISTER_YSON_STRUCT(TServerProgramConfig)

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TServerProgramConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
