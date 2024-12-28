#include "config.h"

#include <yt/yt/library/tcmalloc/config.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TServerProgramConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_porto_resource_tracker", &TThis::EnablePortoResourceTracker)
        .Default(false);
    registrar.Parameter("pod_spec", &TThis::PodSpec)
        .DefaultNew();

    registrar.Parameter("enable_ref_counted_tracker_profiling", &TThis::EnableRefCountedTrackerProfiling)
        .Default(true);

    registrar.Parameter("core_dumper", &TThis::CoreDumper)
        .Default();

    registrar.Parameter("solomon_exporter", &TThis::SolomonExporter)
        .DefaultNew();

    registrar.Postprocessor([] (TThis* config) {
        // TODO(babenko): consider configuring memory_profile_dump_path in ytcfgen
        auto tcmallocConfig = config->GetSingletonConfig<NTCMalloc::TTCMallocConfig>();
        if (!tcmallocConfig->HeapSizeLimit->MemoryProfileDumpPath && config->CoreDumper) {
            tcmallocConfig->HeapSizeLimit->MemoryProfileDumpPath = config->CoreDumper->Path;
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
